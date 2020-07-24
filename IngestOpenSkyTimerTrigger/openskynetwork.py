import requests

class OpenSkyNetworkApi:
    aircraft_flight_url = "https://{0}:{1}@opensky-network.org/api/flights/aircraft?icao24={2}&begin={3}&end={4}"
    def __init__(self, username, password):
        self.username = username
        self.password = password
    def get_aircraft(self, aircraft, begin, end):
        url = self.aircraft_flight_url.format(self.username, self.password, aircraft, begin, end)
        response = requests.get(url)
        data = response.json()
        return data