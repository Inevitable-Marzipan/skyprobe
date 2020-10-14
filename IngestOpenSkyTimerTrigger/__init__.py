import datetime
import logging
import os
import json

import azure.functions as func
import pandas as pd

from .openskynetwork import OpenSkyNetworkApi
from azure.storage.blob import BlobServiceClient

USERNAME = os.environ["OSN_USERNAME"]
PASSWORD = os.environ["OSN_PASSWORD"]
ACCOUNT_URL = os.environ["BLOB_ACCOUNT_URL"]
CREDENTIAL = os.environ["BLOB_CREDENTIAL"]
CONTAINER_NAME = os.environ["BLOB_CONTAINER_NAME"]

with open("config.json", "r") as f:
    config = json.load(f)

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    api = OpenSkyNetworkApi(USERNAME, PASSWORD)
    service = BlobServiceClient(ACCOUNT_URL, CREDENTIAL)
    containerService = service.get_container_client(CONTAINER_NAME)

    now = datetime.datetime.now()
    now_ts = int(now.timestamp())
    today = now.date()
    today_dt = datetime.datetime.combine(today, datetime.datetime.min.time())
    yesterday = today - datetime.timedelta(days=1)
    yesterday_dt = datetime.datetime.combine(yesterday, datetime.datetime.min.time())
    yesterdayStart = int(yesterday_dt.timestamp())
    yesterdayEnd = int(today_dt.timestamp())

    for aircraft_name in config["aircraft"].keys():
        aircraft_icao24 = config["aircraft"][aircraft_name]["icao24"]
        flights = api.get_aircraft(aircraft_icao24, yesterdayStart, yesterdayEnd)
        logging.info(f"Number of flights for aircraft {aircraft_name}: {len(flights)}")

        if len(flights) > 0:
            logging.info("Saving data to blob storage")
            
            data = pd.DataFrame(flights)
            blob_name = f"{aircraft_icao24}/{yesterday.year}/{yesterday_dt.month:02}/{yesterday_dt.day:02}/{now_ts}.csv"
            theblob = containerService.get_blob_client(blob=blob_name)
            output = data.to_csv()
            theblob.upload_blob(output)
            logging.info("Completed data upload to blob storage")