import json
import base64
import requests


class OAuthClient:
    def __init__(self, base_url, user_id, password):
        """
        Initialize the OAuthClient with user credentials.
        """
        self.base_url = base_url
        self.user_id = user_id
        self.password = password

        # Generate the Base64-encoded authorization value
        auth_string = f"{self.user_id}:{self.password}"
        self.auth_base64 = base64.b64encode(auth_string.encode()).decode()

    def get_access_token(self):
        """
        Request a new access token from the OAuth server.
        """
        url_token = f"{self.base_url}/oauth2/token"
        payload = "grant_type=client_credentials"
        headers_token = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": f"Basic {self.auth_base64}"
            }
        response_token = requests.post(url_token, data=payload, headers=headers_token)
        response_token.raise_for_status()  # Raise an exception for HTTP errors
        token_data = response_token.json()
        return token_data.get("access_token")

