import json
import logging
from datetime import datetime

import pandas as pd
import requests

from .base import WeatherService

_log = logging.getLogger(__name__)


class MGMHavaDurumu(WeatherService):
    base_url = "https://servis.mgm.gov.tr/web/"
    _cfg_key = "MGM Hava Durumu"
    _missing = -9999

    def __init__(self, data_dir="MGM/HavaDurumu", file_prefix=""):
        super().__init__(data_dir, file_prefix)

    def get_data(self, location):
        # If site id is not set, no request can be done
        site_il = location.il
        site_ilce = location.ilce
        if pd.isna(site_il) or pd.isna(site_ilce):
            return None

        session = requests.session()  # Reuse the session object for efficiency

        forecast_ids = self._get_forecast_ids(site_il, site_ilce, session)
        if forecast_ids is None:
            _log.error(
                "Failed to retrieve MGM forecast due to failure to get location ids."
            )
            return None
        id_current = forecast_ids["merkezId"]
        id_hourly_forecast = forecast_ids["saatlikTahminIstNo"]
        id_daily_forecast = forecast_ids["gunlukTahminIstNo"]

        headers = {"Origin": "https://www.mgm.gov.tr"}
        data = dict()

        data["lat"] = forecast_ids["enlem"]
        data["lon"] = forecast_ids["boylam"]

        api_call = "sondurumlar"
        payload = {"merkezid": id_current}
        response = session.get(
            self.base_url + "/" + api_call, params=payload, headers=headers
        ).json()
        _log.debug(f"MGM Hava Durumu {api_call} json response:")
        for l in json.dumps(response, indent=4).splitlines():
            _log.debug(l)
        if "error" in response:
            _log.error(
                f"Failed to get {api_call} from MGM for {site_il=}, {site_ilce=}."
                "Error: " + response["error"] + "/" + response["message"]
            )
            data["current"] = None
        else:
            data["current"] = response[0]

        api_call = "tahminler/saatlik"
        payload = {"istno": id_hourly_forecast}
        response = session.get(
            self.base_url + "/" + api_call, params=payload, headers=headers
        ).json()
        _log.debug(f"MGM Hava Durumu {api_call} json response:")
        for l in json.dumps(response, indent=4).splitlines():
            _log.debug(l)
        if "error" in response:
            _log.error(
                f"Failed to get {api_call} from MGM for {site_il=}, {site_ilce=}."
                "Error: " + response["error"] + "/" + response["message"]
            )
            data["hourly"] = None
        else:
            data["hourly"] = response[0]

        api_call = "tahminler/gunluk"
        payload = {"istno": id_daily_forecast}
        response = session.get(
            self.base_url + "/" + api_call, params=payload, headers=headers
        ).json()
        _log.debug(f"MGM Hava Durumu {api_call} json response:")
        for l in json.dumps(response, indent=4).splitlines():
            _log.debug(l)
        if "error" in response:
            _log.error(
                f"Failed to get {api_call} from MGM for {site_il=}, {site_ilce=}."
                "Error: " + response["error"] + "/" + response["message"]
            )
            data["daily"] = None
        else:
            data["daily"] = response[0]
        return data

    def get_rows(self, location):
        """Gets data from API, extracts the useful information in a list of dicts
        suitable to be converted to a dataframe."""
        rtn = []
        data = self.get_data(location)
        if data is None:
            return []

        if data["current"] and data["current"]["veriZamani"]:
            current_dt = self._fix_time(data["current"]["veriZamani"])
        else:
            current_dt = datetime.utcnow()
        # TODO: Building the dict all at once means that if any item fails for
        #  whatever reason, the whole row will be lost. Maybe better to build it
        #  one item at a time and gracefully handle missing data.
        # Current weather
        if data["current"] is not None:
            rtn.append(
                {
                    "location": location.name,
                    "lat": data["lat"],
                    "lon": data["lon"],
                    "type": 0,
                    "current_dt": current_dt,
                    "dt": current_dt,
                    "temp": self._g(data["current"], "sicaklik"),
                    "clouds": self._g(
                        data["current"], "kapalilik"
                    ),  # Is this ever filled??
                    "condition": self._g(data["current"], "hadiseKodu"),
                    "humidity": self._g(data["current"], "nem"),
                    "wind_speed": self._g(data["current"], "ruzgarHiz"),
                    "wind_direction": self._g(data["current"], "ruzgarYon"),
                    # Preciptation comes in the following:
                    # yagis00Now, yagis10Dk, yagis1Saat, yagis6Saat, yagis12Saat, yagis24Saat
                    "precipitation": self._g(data["current"], "yagis1Saat"),
                    "precipitation_period": 60,  # Number of minutes for rain accumulation
                }
            )
        # Hourly forecasts
        if data["hourly"] is not None:
            for h in data["hourly"]["tahmin"]:
                rtn.append(
                    {
                        "location": location.name,
                        "lat": data["lat"],
                        "lon": data["lon"],
                        "type": "hourly",
                        "current_dt": self._fix_time(data["hourly"]["baslangicZamani"]),
                        "dt": self._fix_time(self._g(h, "tarih")),
                        "temp": self._g(h, "sicaklik"),
                        "condition": self._g(h, "hadise"),
                        "humidity": self._g(h, "nem"),
                        "wind_speed": self._g(h, "ruzgarHizi"),
                        "wind_direction": self._g(h, "ruzgarYonu"),
                    }
                )
        # Daily forecasts
        d = data["daily"]
        if d is not None:
            for dn in "12345":
                rtn.append(
                    {
                        "location": location.name,
                        "lat": data["lat"],
                        "lon": data["lon"],
                        "type": "daily",
                        "current_dt": current_dt,
                        "dt": self._fix_time(self._g(d, "tarihGun" + dn)),
                        "temp_high": self._g(d, "enYuksekGun" + dn),
                        "temp_low": self._g(d, "enDusukGun" + dn),
                        "humidity_low": self._g(d, "enDusukNemGun" + dn),
                        "humidity_high": self._g(d, "enYuksekNemGun" + dn),
                        "wind_speed": self._g(d, "ruzgarHizGun" + dn),
                        "wind_direction": self._g(d, "ruzgarYonGun" + dn),
                        "condition": self._g(d, "hadiseGun" + dn),
                    }
                )
        return rtn

    @classmethod
    def _get_forecast_ids(cls, site_il, site_ilce, session=None):
        if session is None:
            session = requests.session()
        payload = {"il": site_il}
        headers = {"Origin": "https://www.mgm.gov.tr"}
        api_call = "merkezler/ililcesi"
        # TODO: Add context manager to allow to disable caching'
        data = requests.get(
            cls.base_url + "/" + api_call, params=payload, headers=headers
        ).json()
        _log.debug("MGM Hava Durumu merkezler/ililcesi json response:")
        for l in json.dumps(data, indent=4).splitlines():
            _log.debug(l)

        if "error" in data:
            _log.error(
                f"Failed to get forecast ids from MGM for {site_il=}, {site_ilce=}."
                "Error: " + data["error"] + "/" + data["message"]
            )
            return None

        ilce_df = pd.DataFrame(data)
        rtn = ilce_df[ilce_df["ilce"] == site_ilce]
        if len(rtn) == 1:
            rtn = rtn.iloc[0]
        else:
            return None
        # Sometimes the system has a sondurumIstNo but not a saatlikTahminIstNo.
        if (
            pd.isna(rtn["saatlikTahminIstNo"])
            and (~pd.isna(ilce_df["saatlikTahminIstNo"])).any()
        ):
            # Find a different station that does have a forecast.
            rtn["saatlikTahminIstNo"] = ilce_df[
                ~pd.isna(ilce_df["saatlikTahminIstNo"])
            ]["saatlikTahminIstNo"][0]
        return rtn

    @classmethod
    def _fix_time(cls, dt):
        """Convert text timestamp from service to pandas datetime"""
        # Text returned by MGM service is labeled as localized to UTC,
        # and it actually appears to be UTC.
        dt = pd.to_datetime(dt)
        dt = dt.tz_localize(None)
        # dt = dt.tz_localize('Europe/Istanbul')
        # dt = dt.tz_convert('UTC')
        # dt = dt.tz_localize(None)
        return dt

    _weather_condition_map_tr = {
        "A": "Açık",
        "AB": "Az Bulutlu",
        "PB": "Parçalı Bulutlu",
        "CB": "Çok Bulutlu",
        "HY": "Hafif Yağmurlu",
        "Y": "Yağmurlu",
        "KY": "Kuvvetli Yağmurlu",
        "KKY": "Karla Karışık Yağmurlu",
        "HKY": "Hafif Kar Yağışlı",
        "K": "Kar Yağışlı",
        "KYK": "Yoğun Kar Yağışlı",
        "HSY": "Hafif Sağanak Yağışlı",
        "SY": "Sağanak Yağışlı",
        "KSY": "Kuvvetli Sağanak Yağışlı",
        "MSY": "Mevzi Sağanak Yağışlı",
        "DY": "Dolu",
        "GSY": "Gökgürültülü Sağanak Yağışlı",
        "KGSY": "Kuvvetli Gökgürültülü Sağanak Yağışlı",
        "SIS": "Sisli",
        "PUS": "Puslu",
        "DNM": "Dumanlı",
        "KF": "Toz veya Kum Fırtınası",
        "R": "Rüzgarlı",
        "GKR": "Güneyli Kuvvetli Rüzgar",
        "KKR": "Kuzeyli Kuvvetli Rüzgar",
        "SCK": "Sıcak",
        "SGK": "Soğuk",
        "HHY": "Yağışlı",
    }
