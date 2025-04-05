import logging
import time
from datetime import datetime, timedelta
from oauth import get_access_token
from request import trigger_data_update_request, get_data_update_status_response
import json
import os

# Load configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'config.json')
with open(CONFIG_PATH, 'r') as config_file:
    config = json.load(config_file)


class TriggerDataUpdate:
    def __init__(self, base_url, file_xfer_wait_time, max_file_transfer_time, data_update_wait_time, safety_timeout_minutes):
        self.base_url = base_url
        self.file_xfer_wait_time = file_xfer_wait_time
        self.max_file_transfer_time = max_file_transfer_time
        self.data_update_wait_time = data_update_wait_time
        self.safety_timeout_minutes = safety_timeout_minutes

    def run(self, data_source):
        start_time = datetime.now()

        while True:  # Data Transfer loop
            try:
                logging.info(f"Step 1: Waiting {self.file_xfer_wait_time} minutes for file transfer to complete.")
                time.sleep(self.file_xfer_wait_time * 60)  # Convert minutes to seconds for sleep

                if datetime.now() - start_time > timedelta(minutes=self.max_file_transfer_time):
                    raise TimeoutError(
                        f"File transfer cancelled or exceeded maximum allowed time of {self.max_file_transfer_time} minutes. Exiting."
                    )

                logging.info("Step 2: Triggering data update.")
                access_token = get_access_token(self.base_url)
                access_key = trigger_data_update_request(self.base_url, data_source, access_token)
                if not access_key:
                    raise Exception("Failed to trigger data update.")

                logging.info("Step 3: Entering inner loop to check for data update ID.")
                while True:
                    if datetime.now() - start_time > timedelta(minutes=self.safety_timeout_minutes):
                        raise TimeoutError(f"Safety timeout reached after {self.safety_timeout_minutes} minutes. Exiting.")

                    response_status = get_data_update_status_response(self.base_url, access_key, access_token)
                    status_data = response_status.json()
                    data_update_id = status_data.get("DataUpdateId")
                    status = status_data.get("Status")  # Extract the status from the response

                    if not data_update_id:
                        logging.info(f"Data update is still in progress. Waiting {self.data_update_wait_time} minutes...")
                        time.sleep(self.data_update_wait_time * 60)  # Convert minutes to seconds for sleep
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
                exit(2)
            except Exception as e:
                raise