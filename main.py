import argparse
import json
import logging
import logging.handlers
import os
from urllib.parse import urljoin
from trigger_data_update import TriggerDataUpdate

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'config.json')
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

# Load configuration
with open(CONFIG_PATH, 'r') as config_file:
    config = json.load(config_file)

HOST = config["HOST"]
COMPANY_ID = config["COMPANY_ID"]

BASE_URL = urljoin(HOST, COMPANY_ID)

FILE_XFER_WAIT_TIME_MINUTES = config["FILE_XFER_WAIT_TIME_MINUTES"]
DATA_UPDATE_WAIT_TIME_MINUTES = config["DATA_UPDATE_WAIT_TIME_MINUTES"]
MAX_FILE_TRANSFER_TIME_MINUTES = config["MAX_FILE_TRANSFER_TIME_MINUTES"]


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
        )
        status = trigger_data_update.run(data_source)
        if not status:
            logging.error("Failed to complete data update.")
            exit(1)
    except TimeoutError as e:
        logging.error(e)
        exit(2)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        exit(1)


if __name__ == "__main__":
    main()