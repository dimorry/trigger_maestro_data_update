import requests
import logging
import time
from oauth import get_access_token

def make_request(method, url, headers, payload=None):
    retries = 3
    for attempt in range(retries):
        try:
            if method == "POST":
                response = requests.post(url, json=payload, headers=headers)
            elif method == "GET":
                response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:  # Token expired
                logging.info("Token expired. Refreshing token...")
                headers["Authorization"] = f"Bearer {get_access_token(url)}"
                continue  # Retry with refreshed token
            raise  # Bubble up the HTTPError
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                logging.warning(f"Request failed (Attempt {attempt + 1}/{retries}): {e}. Retrying...")
                time.sleep(2)  # Wait before retrying
            else:
                raise  # Bubble up the exception after retries are exhausted


def trigger_data_update_request(base_url, data_source, access_token):
    """
    Function to trigger the data update request.
    """
    url_trigger = f"{base_url}/integration/V1/dataupdate/trigger"
    payload_trigger = {
        "KeepExtract": "False",
        "DisableExtractDoneCheck": "False",
        "IntegrationScenario": data_source,
        "OnlyProcessDataSources": data_source,
        "RequiredDataSources": data_source
    }
    headers_trigger = {"Authorization": f"Bearer {access_token}"}
    response = make_request("POST", url_trigger, headers_trigger, payload_trigger)
    if response:
        return response.json().get("StatusKey")
    else:
        logging.error("Failed to trigger data update.")
        return None


def get_data_update_status_response(base_url, status_key, access_token):
    """
    Function to make the GET request for checking data update status.
    """
    url_status = f"{base_url}/integration/V1/dataupdate/{status_key}"
    headers_status = {"Authorization": f"Bearer {access_token}"}
    return make_request("GET", url_status, headers_status)