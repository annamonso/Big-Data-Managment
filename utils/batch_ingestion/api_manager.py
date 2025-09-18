# Description: This file contains several classes which are used to interact with several APIs. 
# The classes are used to fetch data from the APIs and return it in a structured format to the pipeline.


###########
# Imports #
###########
import pandas as pd
import json
########################
# Open Meteo API class #
########################
import openmeteo_requests
import requests_cache
from retry_requests import retry

class open_meteo_api:

    def __init__(self):
        #obtain the api key from the environment
        self.historical_temperatures_url = "https://archive-api.open-meteo.com/v1/archive"
        self.current_weather_url = ""
        self.weather_forecast_url = "https://api.open-meteo.com/v1/forecast"
        self.climate_change_url = ""
        self.cities_path = "./utils/cities.json"

        # Initialize OpenMeteo API
        self.cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
        self.retry_session = retry(self.cache_session, retries = 5, backoff_factor = 0.2)
        self.openmeteo = openmeteo_requests.Client(session = self.retry_session)



    def get_historical_temperatures(self, city:str, start_date:str,end_date:str = None,
                                    lat:str=None, long:str=None) -> pd.DataFrame:
        """
        Fetch historical temperature data for a city between two dates (daily)
        Parameters:
        city (str): The name of the city.
        start_date (str): The start date in the format "YYYY-MM-DD".
        end_date (str): The end date in the format "YYYY-MM-DD".
        lat (str): The latitude of the city.
        long (str): The longitude of the city.
        Returns:
        pd.DataFrame: A DataFrame containing the historical temperature data.
        """
        #Check dates correctness
        try:
            start_dt = pd.to_datetime(start_date)
            if end_date:
                end_dt = pd.to_datetime(end_date)
        except Exception as e:
            raise ValueError("Dates must be in the format 'YYYY-MM-DD'.") from e

        threshold_date = pd.to_datetime("today") - pd.Timedelta(days=2)

        if start_dt > threshold_date:
            raise ValueError("Start date must be at least 2 days in the past.")
        if end_date and end_dt > threshold_date:
            raise ValueError("End date must be at least 2 days in the past.")
        if end_date and end_dt < start_dt:
            raise ValueError("End date must be after start date.")
        
        if not end_date:
            end_date = start_date
        
        with open(self.cities_path) as f:
            cities = json.load(f)
        
        if city in cities:
            latitude = cities[city]["latitude"]
            longitude = cities[city]["longitude"]
        
        params = {
            "latitude": latitude,
	        "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "daily": "temperature_2m_mean",
            "timezone": "UTC"
        }

        responses = self.openmeteo.weather_api(self.historical_temperatures_url, params)
        response = responses[0]
        daily = response.Daily()

        dates = pd.date_range(start=start_date, end=end_date, freq='D')

        daily_data = {
            "date": dates,
            "temperature_2m_mean": daily.Variables(0).ValuesAsNumpy()
        }

        daily_dataframe = pd.DataFrame(data=daily_data)

        daily_dataframe["city"] = city if city else f"{latitude}_{longitude}"
        daily_dataframe["latitude"] = latitude
        daily_dataframe["longitude"] = longitude

        return daily_dataframe

########################
# Open Meteo API class #
########################
import requests

class air_quality_api:

    def __init__(self):
        #obtain the api key from the environment
        self.api_key = 'fe4918b51d0ed064ddd68ad0844f214f79e1315c'

    def get_air_quality_data(self, city:str):
        """
        Fetch air quality data for a city.
        Parameters:
        city (str): The name of the city.
        Returns:
        dict: A dictionary containing the air quality data.
        """
        
        # get air quality
        air_quality_api_url = f"http://api.waqi.info/feed/{city}/?token={self.api_key}"
        response = requests.get(air_quality_api_url)

        if response.status_code == 200:
            data = response.json()
            if data['status'] == 'ok':
                aqi = data['data']['aqi']
                pm25 = data['data']['iaqi']['pm25']['v'] if 'pm25' in data['data']['iaqi'] else None
                pm10 = data['data']['iaqi']['pm10']['v'] if 'pm10' in data['data']['iaqi'] else None
                no2 = data['data']['iaqi']['no2']['v'] if 'no2' in data['data']['iaqi'] else None

        current_date = pd.to_datetime("today").date()
        air_quality_data = {
            "city": city,
            "date": current_date,
            "aqi": aqi,
            "pm25": pm25,
            "pm10": pm10,
            "no2": no2
        }

        return air_quality_data

