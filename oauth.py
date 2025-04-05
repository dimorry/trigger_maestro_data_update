import json
import base64
import requests

# import os
# print("Current working directory:", os.getcwd())

# Read user_id and password from a JSON file
with open('credentials.json', 'r') as cred_file:
    credentials = json.load(cred_file)
    user_id = credentials['user_id']
    password = credentials['password']

# Generate the Base64-encoded authorization value
auth_string = f"{user_id}:{password}"
auth_base64 = base64.b64encode(auth_string.encode()).decode()

# Function to get headers
def get_headers(access_token=None):
    if access_token:
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}"
        }
    else:
        return {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {auth_base64}"
        }

# Function to get a new access token
def get_access_token(base_url):
    url_token = f"{base_url}/oauth2/token"
    payload = "grant_type=client_credentials"
    headers_token = get_headers()
    response_token = requests.post(url_token, data=payload, headers=headers_token)
    token_data = response_token.json()
    return token_data.get("access_token")