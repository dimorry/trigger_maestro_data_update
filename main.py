import argparse
import json
import logging
import logging.handlers
import os
from urllib.parse import urljoin
from trigger_data_update import TriggerDataUpdate

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'config.json')
CREDS_PATH = os.path.join(BASE_DIR, 'credentials.json')
LOG_PATH = os.path.join(BASE_DIR, 'logs/trigger_data_update.log')

# Update logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Log to console
        logging.handlers.RotatingFileHandler(
            LOG_PATH, maxBytes=10 * 1024 * 1024, backupCount=5  # 10 MB, keep last 5 files
        )
    ]
)

with open(CONFIG_PATH, 'r') as config_file:
    config = json.load(config_file)

with open(CREDS_PATH, 'r') as credentials_file:
    credentials = json.load(credentials_file)

# Load configuration from environment variables default to config file if missing for local testing
HOST = os.getenv("HOST", config["HOST"])
COMPANY_ID = os.getenv("COMPANY_ID", config["COMPANY_ID"])
FILE_XFER_WAIT_TIME_MINUTES = int(os.getenv("FILE_XFER_WAIT_TIME_MINUTES", config["FILE_XFER_WAIT_TIME_MINUTES"]))
DATA_UPDATE_WAIT_TIME_MINUTES = int(os.getenv("DATA_UPDATE_WAIT_TIME_MINUTES", config["DATA_UPDATE_WAIT_TIME_MINUTES"]))
MAX_FILE_TRANSFER_TIME_MINUTES = int(os.getenv("MAX_FILE_TRANSFER_TIME_MINUTES", config["MAX_FILE_TRANSFER_TIME_MINUTES"]))
SAFETY_TIMEOUT_MINUTES = int(os.getenv("SAFETY_TIMEOUT_MINUTES", config["SAFETY_TIMEOUT_MINUTES"]))

USER_ID = os.getenv("USER_ID", credentials["USER_ID"])
PASSWORD = os.getenv("PASSWORD", credentials["PASSWORD"])

BASE_URL = urljoin(HOST, COMPANY_ID)


def main():
    parser = argparse.ArgumentParser(description="Trigger a data update process.")
    parser.add_argument("--data-source", required=True, help="The data source to use for the data update.")
    args = parser.parse_args()

    data_source = args.data_source

    try:
        logging.info(f"Program started. Processing {data_source} data source on {BASE_URL}.")
        trigger_data_update = TriggerDataUpdate(
            base_url=BASE_URL,
            file_xfer_wait_time=FILE_XFER_WAIT_TIME_MINUTES,
            max_file_transfer_time=MAX_FILE_TRANSFER_TIME_MINUTES,
            data_update_wait_time=DATA_UPDATE_WAIT_TIME_MINUTES,
            safety_timeout_minutes=SAFETY_TIMEOUT_MINUTES,
            user_id=USER_ID,
            password=PASSWORD,
            data_source=data_source
        )
        status = trigger_data_update.run(data_source)
        if status!= "Completed":
            logging.error("Data update process was not completed successfully. Exiting with code 1.")
            exit(1)

        exit(0)
    except TimeoutError as e:
        logging.error(f"A timeout error occurred: {e}. Exiting with code 2.")
        exit(2)
    except Exception as e:
        logging.error(f"An error occurred: {e}. Exiting with code 1.")
        exit(1)


if __name__ == "__main__":
    main()