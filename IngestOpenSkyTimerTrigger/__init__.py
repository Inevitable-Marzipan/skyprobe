import datetime
import logging
import os

import azure.functions as func

from .openskynetwork import OpenSkyNetworkApi

USERNAME = os.environ["OSN_USERNAME"]
PASSWORD = os.environ["OSN_PASSWORD"]


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    api = OpenSkyNetworkApi(USERNAME, PASSWORD)

    now = int(datetime.datetime.now().timestamp())
    offset = now % (60 * 60 * 24)
    dayStart = now - offset - (60 * 60 * 24)
    dayEnd = now - offset
    aircraft_map = {"D-FLOH": "3d67cf",
                "D-FBPS": "3d4d8c"}
    aircraft = "D-FBPS"
    aircraft_icao24 = aircraft_map[aircraft]
    flights = api.get_aircraft(aircraft_icao24, dayStart, now)
    logging.info(f"Number of flights for aircraft {aircraft}: {len(flights)}")

