import requests
import json
from datetime import datetime, timezone, timedelta
import base64

class WeatherApiClient:
    def __init__(self, client_id, client_secret, latitude, longitude):
        self.client_id = client_id
        self.client_secret = client_secret
        self.latitude = latitude
        self.longitude = longitude
        self.token_url = 'https://api.srgssr.ch/oauth/v1/accesstoken?grant_type=client_credentials'
        self.base_url = 'https://api.srgssr.ch/srf-meteo/v2/forecastpoint'
        self.access_token = None
        self.headers = {
            'Cache-Control': 'no-cache',
            'Content-Length': '0'
        }
        self.get_access_token()

    def get_access_token(self):
        credentials = f"{self.client_id}:{self.client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        headers = {
            'Authorization': f'Basic {encoded_credentials}',
            'Cache-Control': 'no-cache',
            'Content-Length': '0'
        }
        response = requests.post(self.token_url, headers=headers)
        if response.status_code == 200:
            self.access_token = response.json()['access_token']
            print("Successfully fetched access token.")
        else:
            print(f'Failed to get access token: {response.status_code} - {response.text}')
            self.access_token = None

    def fetch_temperature_data(self):
        if not self.access_token:
            self.get_access_token()
            if not self.access_token:
                return None

        request_url = f'{self.base_url}/{self.latitude},{self.longitude}'
        headers = {
            'accept': 'application/json',
            'Authorization': f'Bearer {self.access_token}'
        }
        response = requests.get(request_url, headers=headers)
        if response.status_code == 401:
            error_data = response.json()
            if error_data.get("code") == "401.02.002":
                print("Access token has expired. Fetching a new one.")
                self.get_access_token()
                return self.fetch_temperature_data()
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f'Failed to get data: {response.status_code} - {response.text}')
            return None

    def get_temperature(self):
        data = self.fetch_temperature_data()
        if not data:
            return None, None

        current_time = datetime.now(timezone.utc)
        print(f"current time: {current_time}")

        def parse_datetime(date_str):
            return datetime.fromisoformat(date_str.replace("Z", "+00:00")).astimezone(timezone.utc)

        temperature = None
        timestamp_used = None
        smallest_diff = timedelta.max

        for hour in data.get("hours", []):
            hour_time = parse_datetime(hour["date_time"])
            time_diff = current_time - hour_time
            if timedelta(0) <= time_diff < smallest_diff:
                smallest_diff = time_diff
                temperature = hour["TTT_C"]
                timestamp_used = hour_time

        for three_hour in data.get("three_hours", []):
            three_hour_time = parse_datetime(three_hour["date_time"])
            time_diff = current_time - three_hour_time
            if timedelta(0) <= time_diff < smallest_diff:
                smallest_diff = time_diff
                temperature = three_hour["TTT_C"]
                timestamp_used = three_hour_time
        
        print(f"outside temp: {temperature}")

        return temperature, timestamp_used

# client_id = 'uH8XLtnrmlkwrN4SioaVLcpgSSZsGRfO'
# client_secret = 'QEpsjLGUnbNWaigW'
# latitude = 47.4625
# longitude = 9.0411

# weather_client = WeatherApiClient(client_id, client_secret, latitude, longitude)
# temperature, timestamp = weather_client.get_temperature()

# if temperature is not None and timestamp is not None:
#     print(f"Current temperature (closest match): {temperature}°C")
#     print(f"Timestamp used: {timestamp.isoformat()}")
# else:
#     print("No matching temperature found for the current time in the provided data.")