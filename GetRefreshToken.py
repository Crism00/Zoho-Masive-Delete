import requests

client_id = "1000.3Q482NNC9DZEMD925R5W0INKG72VKD"
client_secret = "9b8676607b7eb0aabe9b7769fc4d3195cdbdc0bdbc"
grant_token = "1000.dd46d16b5707d934d8f8de86716021af.676b36f6359e1315a4630ca6f16e9c5f"
redirect_uri = "http://localhost"

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