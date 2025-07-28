import requests



url = "https://accounts.zoho.com/oauth/v2/token"
params = {
    "grant_type": "authorization_code",
    "client_id": client_id,
    "client_secret": client_secret,
    "redirect_uri": redirect_uri,
    "code": grant_token
}

response = requests.post(url, params=params)
print(response.json())