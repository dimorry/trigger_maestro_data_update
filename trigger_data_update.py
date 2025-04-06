import logging
import time
import requests
from datetime import datetime, timedelta
from oauth import OAuthClient



class TriggerDataUpdate:
    def __init__(self, data_source, base_url, file_xfer_wait_time, max_file_transfer_time, 
                 data_update_wait_time, safety_timeout_minutes, user_id, password):

        self.data_source = data_source
        self.base_url = base_url
        self.file_xfer_wait_time = file_xfer_wait_time
        self.max_file_transfer_time = max_file_transfer_time
        self.data_update_wait_time = data_update_wait_time
        self.safety_timeout_minutes = safety_timeout_minutes
        self.user_id = user_id
        self.password = password
        self.access_token = None


    def run(self, data_source):
        start_time = datetime.now()

        self.get_token()

        while True:  # Data Transfer loop
            try:
                logging.info(f"Step 1: Waiting {self.file_xfer_wait_time} minutes for file transfer to complete.")
                time.sleep(self.file_xfer_wait_time * 60)  # Convert minutes to seconds for sleep

                if datetime.now() - start_time > timedelta(minutes=self.max_file_transfer_time):
                    raise TimeoutError(
                        f"File transfer cancelled or exceeded maximum allowed time of {self.max_file_transfer_time} minutes. Exiting."
                    )

                logging.info("Step 2: Triggering data update.")

                access_key = self.make_request("trigger_data_update")
                if not access_key:
                    raise Exception("Failed to trigger data update.")

                logging.info("Step 3: Entering inner loop to check for data update ID.")
                while True:
                    if datetime.now() - start_time > timedelta(minutes=self.safety_timeout_minutes):
                        raise TimeoutError(f"Safety timeout reached after {self.safety_timeout_minutes} minutes. Exiting.")

                    response_status = self.make_request("get_data_update_status", access_key)
                    status_data = response_status.json()
                    data_update_id = status_data.get("DataUpdateId")
                    status = status_data.get("Status")  # Extract the status from the response

                    if not data_update_id:
                        logging.info(f"Data update is still in progress. Waiting {self.data_update_wait_time} minutes...")
                        time.sleep(self.data_update_wait_time * 60)  # Convert minutes to seconds for sleep
                        continue

                    logging.info("Step 4: Checking data update status.")

                    if status == "No new extract data" or status == "Canceled": # Maestro sometimes logs as cancelled when files are still being transferred
                        logging.info(f"Data Update Id {data_update_id}: Status: {status}. Restarting the process.")
                        break  # Restart data transfer loop
                    else:
                        logging.info(f"Data Update ID: {data_update_id}, Status: {status}")
                        return status
            except TimeoutError as e:
                exit(2)
            except Exception as e:
                raise

    def get_token(self):
        oauth_client = OAuthClient(self.base_url, self.user_id, self.password)
        self.access_token = oauth_client.get_access_token()
        if not self.access_token:
            logging.error("Failed to obtain access token.")
            raise Exception("Access token is not available.")
        
        return self.access_token


    def make_request(self, call, status_key=None):
        retries = 3        

        for attempt in range(retries):
            try:
                if call == "trigger_data_update":
                    return self.trigger_data_update_request()
                elif call == "get_data_update_status":
                    return self.get_data_update_status_response(status_key)
                else:
                    raise ValueError(f"Invalid call: {call}")
            except requests.exceptions.HTTPError as e:
                if attempt < retries - 1:
                    logging.warning(f"Request failed (Attempt {attempt + 1}/{retries}): {e}. Retrying...")
                    time.sleep(2)
                else:
                    logging.error(f"Request failed after {retries} attempts: {e}")
                    raise

                if e.response.status_code == 401:  # Token expired
                    logging.info("Token expired. Refreshing token...")
                    self.get_token()
                    continue  # Retry with refreshed token
            except requests.exceptions.RequestException as e:
                if attempt < retries - 1:
                    logging.warning(f"Request failed (Attempt {attempt + 1}/{retries}): {e}. Retrying...")
                    time.sleep(2)
                else:
                    logging.error(f"Request failed after {retries} attempts: {e}")
                    raise


    def trigger_data_update_request(self):
        logging.info(f"Calling Trigger Data Update")
        url_trigger = f"{self.base_url}/integration/V1/dataupdate/trigger"
        payload_trigger = {
            "KeepExtract": "False",
            "DisableExtractDoneCheck": "False",
            "IntegrationScenario": self.data_source,
            "OnlyProcessDataSources": self.data_source,
            "RequiredDataSources": self.data_source
        }
        headers_trigger = {"Authorization": f"Bearer {self.access_token}"}
        response = self.send_request("POST", url_trigger, headers_trigger, payload_trigger)
        if response:
            return response.json().get("StatusKey")
        else:
            logging.error("Failed to trigger data update.")
            return None


    def get_data_update_status_response(self, status_key):
        logging.info(f"Calling Get Data Update Status")
        url_status = f"{self.base_url}/integration/V1/dataupdate/{status_key}"
        headers_status = {"Authorization": f"Bearer {self.access_token}"}
        return self.send_request("GET", url_status, headers_status)


    def send_request(self, method, url, headers, payload=None):
        if method == "POST":
            response = requests.post(url, json=payload, headers=headers)
        elif method == "GET":
            response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response