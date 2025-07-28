import csv
from itertools import islice
import json, os, time, requests
from pathlib import Path
from dotenv import load_dotenv
import sys

load_dotenv()

CLIENT_ID       = os.getenv("ZOHO_CLIENT_ID")
CLIENT_SECRET   = os.getenv("ZOHO_CLIENT_SECRET")
REFRESH_TOKEN   = os.getenv("ZOHO_REFRESH_TOKEN")
BASE_URL        = os.getenv("ZOHO_BASE_URL",  "https://www.zohoapis.com")
ORG_ID          = os.getenv("ZOHO_ORG_ID", "648703833")
API_DOMAIN      = os.getenv("ZOHO_API_DOMAIN", BASE_URL)

CACHE_FILE      = Path(__file__).with_suffix(".tokencache.json")
SKEW            = 30           # segundos de colchón para refrescar antes de que caduque
SESSION         = requests.Session()

def _load_cached_token():
    """Lee token y expiración del disco (si existe y sigue vivo)."""
    try:
        data = json.loads(CACHE_FILE.read_text())
        if data["expires_at"] - time.time() > SKEW:
            return data["access_token"]
    except Exception:
        pass                       # archivo corrupto o inexistente
    return None

def _save_cached_token(token, expires_in):
    CACHE_FILE.write_text(json.dumps({
        "access_token": token,
        "expires_at": time.time() + expires_in
    }))

def get_access_token(force_refresh=False):
    """
    Devuelve un access_token válido.
    * Lo lee de cache si aún está fresco.
    * Lo renueva con el refresh_token cuando sea necesario.
    """
    if not force_refresh:
        cached = _load_cached_token()
        if cached:
            return cached

    resp = SESSION.post(
        "https://accounts.zoho.com/oauth/v2/token",
        params=dict(
            refresh_token = REFRESH_TOKEN,
            client_id     = CLIENT_ID,
            client_secret = CLIENT_SECRET,
            grant_type    = "refresh_token"
        ),
        timeout=30
    )
    resp.raise_for_status()
    data = resp.json()
    token       = data["access_token"]
    expires_in  = int(data.get("expires_in", 3600))
    _save_cached_token(token, expires_in)
    return token

def api_request(method, endpoint, **kwargs):
    """
    Envuelve SESSION.request añadiendo el header Authorization
    y refrescando el token automáticamente si Zoho responde 401.
    """
    url = f"{API_DOMAIN}{endpoint}"
    for attempt in (0, 1):                       # máx. 1 reintento
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Zoho-oauthtoken {get_access_token()}"
        resp = SESSION.request(method, url, headers=headers, **kwargs, timeout=60)

        if resp.status_code != 401:
            resp.raise_for_status()
            # algunas APIs V8 devuelven datos envueltos en "data"
            return resp.json()

        # 401 → token caducó o inválido ⇒ fuerzo refresh y reintento
        print("Token expirado, renovando…")
        get_access_token(force_refresh=True)
    resp.raise_for_status()          # si llega aquí es un 401 persistente


def list_fields(module_name: str):
    res = api_request("GET", "/crm/v8/settings/fields", params={"module": module_name})
    for f in res.get("fields", []):
        print(f"{f['api_name']:<30} - {f.get('data_type')}")

def create_bulk_read_job(module_name: str, name: str = None):
    payload = {
        "query": {
            "module": {"api_name": module_name},
            "fields": ["id"], 
            "criteria": {
                "field":      { "api_name": "Due_Date" },
                "comparator": "less_than",  
                                            
                "value":      "2025-05-01" 
            }
        }
    }
    res = api_request("POST", "/crm/bulk/v8/read", json=payload)
    JOB_IDS_HISTORY = Path(__file__).with_suffix(".jobshistory.json")
    dataToSave = {
        name: name,
        "id": res["data"][0]["details"]["id"],
        "module": module_name,
    }
    if JOB_IDS_HISTORY.exists():
        history = json.loads(JOB_IDS_HISTORY.read_text())
        history.append(dataToSave)
    else:
        history = [dataToSave]
    JOB_IDS_HISTORY.write_text(json.dumps(history, indent=2))
    return res["data"][0]["details"]["id"]

def check_job_status(job_id: str):
    while True:
        res = api_request("GET", f"/crm/bulk/v8/read/{job_id}")
        job = res["data"][0]
        state = job["state"]  # aquí ya no hay "details"

        if state == "COMPLETED":
            return job["result"]  # aquí también sin "details"

        if state in ("FAILURE", "FAILED"):
            raise RuntimeError("Bulk read job falló")

        time.sleep(5)


def download_all_pages(job_id, out_prefix):
    # Obtener info inicial
    res = api_request("GET", f"/crm/bulk/v8/read/{job_id}")
    job = res["data"][0]
    result = job["result"]

    print("Información inicial:", result)

    page = result["page"]
    next_token = result.get("next_page_token")
    more = result.get("more_records", False)

    # Header de autorización
    token = get_access_token()
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}

    # Descargar primera página
    url = result["download_url"]
    r = SESSION.get(f"{API_DOMAIN}{url}", headers=headers, stream=True, timeout=60)
    zip_name = f"{out_prefix}_page_{page}.zip"
    with open(zip_name, "wb") as f:
        for chunk in r.iter_content(8192):
            f.write(chunk)
    print(f"Descargada página {page} → {zip_name}")
    
def delete_batch_from_file(module, file_path):
    """
    Lee un archivo con una columna 'id' y borra en lotes de 100 usando el endpoint:
    DELETE /crm/v8/{module}?ids=ID1,ID2,...
    """
    # Leer los IDs
    ids = []
    with open(file_path, newline='', encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        if "Id" not in reader.fieldnames:
            raise RuntimeError("El archivo debe tener una columna llamada 'id'")
        for row in reader:
            ids.append(row["Id"])

    print(f"Se leyeron {len(ids)} IDs desde {file_path}")

    def chunked(iterable, size=100):
        it = iter(iterable)
        for first in it:
            yield [first] + list(islice(it, size - 1))

    token = get_access_token()
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}

    total_deleted = 0
    for batch in chunked(ids, 100):
        ids_str = ",".join(batch)
        url = f"{API_DOMAIN}/crm/v8/{module}?ids={ids_str}&wf_trigger=true"
        resp = SESSION.delete(url, headers=headers, timeout=60)
        print(f"DELETE {len(batch)} → {resp.status_code}")
        try:
            print(resp.json())
        except Exception:
            print(resp.text)
        resp.raise_for_status()
        total_deleted += len(batch)

    print(f"Eliminación completada: {total_deleted} registros borrados.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python zoho_bulk.py [create|status|download|delete_batch] [args...]")
        sys.exit(1)

    action = sys.argv[1]

    if action == "create":
        module = sys.argv[2] if len(sys.argv) > 2 else "Tasks"
        if(len(sys.argv) < 4):
            print("Debes especificar el nombre del bulk read job")
            sys.exit(1)
        job_id = create_bulk_read_job(module, sys.argv[3])
        print(f"Nuevo job creado: {job_id}")

    elif action == "status":
        if len(sys.argv) < 3:
            print("Falta job_id")
            sys.exit(1)
        job_id = sys.argv[2]
        result = check_job_status(job_id)
        print(f"Resultado: {result}")

    elif action == "download":
        if len(sys.argv) < 4:
            print("Uso: download JOB_ID prefijo_salida")
            sys.exit(1)

        job_id = sys.argv[2]
        out_prefix = sys.argv[3]

        # Consultar el estado del job
        res = api_request("GET", f"/crm/bulk/v8/read/{job_id}")
        job = res["data"][0]

        if job["state"] != "COMPLETED":
            print(f"El job todavía no está listo. Estado actual: {job['state']}")
            sys.exit(1)

        print(f"Descargando todas las páginas del job {job_id}...")
        download_all_pages(job_id, out_prefix)
    
    elif action == "list_fields":
        if len(sys.argv) < 3:
            print("Uso: list_fields NOMBRE_DEL_MÓDULO")
            sys.exit(1)
        module_name = sys.argv[2]
        list_fields(module_name)
        
    elif action == "delete_batch":
        if len(sys.argv) < 4:
            print("Uso: delete_batch MODULE ARCHIVO_IDS.csv")
            sys.exit(1)

        module_name = sys.argv[2]
        file_path = sys.argv[3]
        delete_batch_from_file(module_name, file_path)
    else:
        print("Acción desconocida")
