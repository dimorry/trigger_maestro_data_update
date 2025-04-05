import requests
import time
import logging
import json
import argparse  # Import argparse for command-line arguments
from datetime import datetime, timedelta
from urllib.parse import urljoin
from oauth import get_headers, get_access_token
import logging.handlers  # Import for rotating file handler

# Configure logging to both console and rolling file logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Log to console
        logging.handlers.RotatingFileHandler(
            "trigger_data_update.log", maxBytes=10 * 1024 * 1024, backupCount=5  # 10 MB, keep last 5 files
        )
    ]
)

# Load configuration
with open('config.json', 'r') as config_file:
    config = json.load(config_file)

HOST = config["HOST"]
COMPANY_ID = config["COMPANY_ID"]

BASE_URL = urljoin(HOST, COMPANY_ID)

FILE_XFER_WAIT_TIME_MINUTES = config["FILE_XFER_WAIT_TIME_MINUTES"]  # Extracted from config.json
DATA_UPDATE_WAIT_TIME_MINUTES = config["DATA_UPDATE_WAIT_TIME_MINUTES"]  # Extracted from config.json
MAX_FILE_TRANSFER_TIME_MINUTES = config["MAX_FILE_TRANSFER_TIME_MINUTES"]


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
                headers["Authorization"] = f"Bearer {get_access_token(BASE_URL)}"
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
    headers_trigger = get_headers(access_token)
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
    headers_status = get_headers(access_token)
    return make_request("GET", url_status, headers_status)

def trigger_data_update(base_url, data_source):
    start_time = datetime.now()

    while True:  # Data Transfer loop
        try:
            logging.info(f"Step 1: Waiting {FILE_XFER_WAIT_TIME_MINUTES} minutes for file transfer to complete.")
            time.sleep(FILE_XFER_WAIT_TIME_MINUTES * 60)  # Convert minutes to seconds for sleep
            
            if datetime.now() - start_time > timedelta(minutes=MAX_FILE_TRANSFER_TIME_MINUTES):
                raise TimeoutError(f"File transfer cancelled or exceeded maximum allowed time of {MAX_FILE_TRANSFER_TIME_MINUTES} minutes. Exiting.")

            logging.info("Step 2: Triggering data update.")
            access_token = get_access_token(base_url)
            access_key = trigger_data_update_request(base_url, data_source, access_token)
            if not access_key:
                raise Exception("Failed to trigger data update.")

            logging.info("Step 3: Entering inner loop to check for data update ID.")
            while True:
                if datetime.now() - start_time > timedelta(minutes=120):
                    raise TimeoutError(f"Safety timeout reached after 120 minutes. Exiting.")
            
                response_status = get_data_update_status_response(base_url, access_key, access_token)
                status_data = response_status.json()
                data_update_id = status_data.get("DataUpdateId")
                status = status_data.get("Status")  # Extract the status from the response

                if not data_update_id:
                    logging.info(f"Data update is still in progress. Waiting {DATA_UPDATE_WAIT_TIME_MINUTES} minutes...")
                    time.sleep(DATA_UPDATE_WAIT_TIME_MINUTES * 60)  # Convert minutes to seconds for sleep
                    continue

                logging.info("Step 4: Checking data update status.")
                if status == "No new extract data":
                    logging.info("No new extract data. Restarting the process.")
                    break  # Restart data transfer loop
                else:
                    logging.info(f"Data Update ID: {data_update_id}")
                    logging.info(f"Status: {status}")
                    return status
        except TimeoutError as e:
            logging.error(e)
            exit(2)
        except Exception as e:
            raise  # Bubble up the exception to the main function

def main(DATA_SOURCE):
    try:
        program_start_time = time.time()
        logging.info(f"Program started. Processing {DATA_SOURCE} data source on {BASE_URL}.")

        # Trigger data update
        status = trigger_data_update(BASE_URL, DATA_SOURCE)
        if not status:
            logging.error("Failed to complete data update.")
            exit(1)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        exit(1)

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Trigger a data update process.")
    parser.add_argument("--data-source", required=True, help="The data source to use for the data update.")
    args = parser.parse_args()

    DATA_SOURCE = args.data_source  # Get DATA_SOURCE from command-line arguments
    main(DATA_SOURCE)
    exit(0)
# End of the script
# This script is designed to be run as a standalone program.