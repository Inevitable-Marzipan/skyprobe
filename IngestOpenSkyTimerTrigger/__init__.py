import datetime
import logging
import os

import azure.functions as func
import pandas as pd

from .openskynetwork import OpenSkyNetworkApi
from azure.storage.blob import BlobServiceClient

USERNAME = os.environ["OSN_USERNAME"]
PASSWORD = os.environ["OSN_PASSWORD"]
ACCOUNT_URL = os.environ["BLOB_ACCOUNT_URL"]
CREDENTIAL = os.environ["BLOB_CREDENTIAL"]
CONTAINER_NAME = os.environ["BLOB_CONTAINER_NAME"]


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    api = OpenSkyNetworkApi(USERNAME, PASSWORD)

    now = datetime.datetime.now()
    today = now.date()
    today_dt = datetime.datetime.combine(today, datetime.datetime.min.time())
    yesterday = today - datetime.timedelta(days=1)
    yesterday_dt = datetime.datetime.combine(yesterday, datetime.datetime.min.time())
    yesterdayStart = int(yesterday_dt.timestamp())
    yesterdayEnd = int(today_dt.timestamp())
    aircraft_map = {"D-FLOH": "3d67cf",
                "D-FBPS": "3d4d8c"}
    aircraft = "D-FBPS"
    aircraft_icao24 = aircraft_map[aircraft]
    flights = api.get_aircraft(aircraft_icao24, yesterdayStart, yesterdayEnd)
    logging.info(f"Number of flights for aircraft {aircraft}: {len(flights)}")
    if len(flights) > 0:
        logging.info("Saving data to blob storage")
        data = pd.DataFrame(flights)

        service = BlobServiceClient(ACCOUNT_URL, CREDENTIAL)
        containerService = service.get_container_client(CONTAINER_NAME)
        now_ts = int(now.timestamp())
        blob_name = f"{aircraft_icao24}/{yesterday.year}/{yesterday_dt.month:02}/{yesterday_dt.day:02}/{now_ts}.csv"
        theblob = containerService.get_blob_client(blob=blob_name)
        output = data.to_csv()
        theblob.upload_blob(output)
        logging.info("Completed data upload to blob storage")