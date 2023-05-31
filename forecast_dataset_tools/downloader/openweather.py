import json
import logging
from datetime import datetime

import requests

from .base import WeatherService

_log = logging.getLogger(__name__)


class OpenWeatherService(WeatherService):
    base_url = "http://api.openweathermap.org/data/2.5/"
    _cfg_key = "OpenWeather"

    def __init__(self, api_key, data_dir="OpenWeather", file_prefix=""):
        super().__init__(data_dir, file_prefix)
        self.api_key = api_key

    def get_data(self, location):
        api_call = "onecall"
        payload = {
            "appid": self.api_key,
            "lat": location.lat,
            "lon": location.lon,
            "exclude": "minutely,alerts",
            "units": "metric",
        }
        data = requests.get(self.base_url + api_call, params=payload).json()

        _log.debug("JSON response:")
        for l in json.dumps(data, indent=4).splitlines():
            _log.debug(l)
        """
        Example response when API key is invalid:
        {
            "cod": 401,
            "message": "Invalid API key. Please see https://openweathermap.org/faq#error401 for more info."
        }
        """
        if "cod" in data and data["cod"] == 401:
            _log.error(
                f"Unable to retrieve OpenWeather data for {location.name}."
                " Error: " + data["message"]
            )
            data = None

        return data

    def get_rows(self, location):
        """Gets data from API, extracts the useful information in a list of dicts
        suitable to be converted to a dataframe."""
        rtn = []
        data = self.get_data(location)
        if data is None:
            return None
        current_dt = datetime.fromtimestamp(data["current"]["dt"])
        # TODO: Building the dict all at once means that if any item fails for
        #  whatever reason, the whole row will be lost. Maybe better to build it
        #  one item at a time and gracefully handle missing data.
        # Current weather
        rtn.append(
            {
                "location": location.name,
                "lat": data["lat"],
                "lon": data["lon"],
                "type": 0,
                "current_dt": current_dt,
                "dt": current_dt,
                "temp": data["current"]["temp"],
                "clouds": data["current"]["clouds"],
                "humidity": data["current"]["humidity"],
                "wind_speed": data["current"]["wind_speed"],
                "wind_direction": data["current"]["wind_deg"],
                # Preciptation is not included unless it is non-zero.
                "precipitation": data["current"].get("rain", {"1h": 0})["1h"],
                "precipitation_period": 60,  # Number of minutes for rain accumulation
            }
        )
        # Hourly forecasts
        for h in data["hourly"]:
            rtn.append(
                {
                    "location": location.name,
                    "lat": data["lat"],
                    "lon": data["lon"],
                    "type": "hourly",
                    "current_dt": current_dt,
                    "dt": datetime.fromtimestamp(h["dt"]),
                    "temp": h["temp"],
                    "clouds": h["clouds"],
                    "humidity": h["humidity"],
                    "wind_speed": h["wind_speed"],
                    "wind_direction": h["wind_deg"],
                    "probability_of_preciptitation": h["pop"],
                    # Preciptation is not included unless it is non-zero.
                    # TODO: Need to confirm the key is right
                    "precipitation": h.get("rain", {"1h": 0})["1h"],
                    "precipitation_period": 60,
                }
            )
        # Daily forecasts
        for d in data["daily"]:
            rtn.append(
                {
                    "location": location.name,
                    "lat": data["lat"],
                    "lon": data["lon"],
                    "type": "daily",
                    "current_dt": current_dt,
                    "dt": datetime.fromtimestamp(d["dt"]),
                    "temp_high": d["temp"]["max"],
                    "temp_low": d["temp"]["min"],
                    "clouds": d["clouds"],
                    "humidity": d["humidity"],
                    "wind_speed": d["wind_speed"],
                    "wind_direction": d["wind_deg"],
                    "probability_of_preciptitation": d["pop"],
                    # Preciptation is not included unless it is non-zero.
                    "precipitation": d.get("rain", 0),
                    "precipitation_period": 60 * 24,
                }
            )
        return rtn
