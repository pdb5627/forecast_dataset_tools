import json
import logging
import re
from datetime import timedelta

import pandas as pd
import requests

from .base import WeatherService

_log = logging.getLogger(__name__)

# Regular expression to parse ISO duration strings.
# Regex copied from from
# https://github.com/gweis/isodate/blob/master/src/isodate/isoduration.py
ISO8601_PERIOD_REGEX = re.compile(
    r"^(?P<sign>[+-])?"
    r"P(?!\b)"
    r"(?P<years>[0-9]+([,.][0-9]+)?Y)?"
    r"(?P<months>[0-9]+([,.][0-9]+)?M)?"
    r"(?P<weeks>[0-9]+([,.][0-9]+)?W)?"
    r"(?P<days>[0-9]+([,.][0-9]+)?D)?"
    r"((?P<separator>T)(?P<hours>[0-9]+([,.][0-9]+)?H)?"
    r"(?P<minutes>[0-9]+([,.][0-9]+)?M)?"
    r"(?P<seconds>[0-9]+([,.][0-9]+)?S)?)?$"
)


def _parse_period(s):
    m = ISO8601_PERIOD_REGEX.match(s)
    if not m:
        return None
    groups = m.groupdict()
    for key, val in groups.items():
        if key not in ("separator", "sign"):
            if val is None:
                groups[key] = 0
            else:
                groups[key] = float(groups[key][:-1].replace(",", "."))
    ret = timedelta(
        days=groups["days"],
        hours=groups["hours"],
        minutes=groups["minutes"],
        seconds=groups["seconds"],
        weeks=groups["weeks"],
    )
    if groups["sign"] == "-":
        ret = -1 * ret
    return ret


class SolcastService(WeatherService):
    base_url = "https://api.solcast.com.au/"
    _cfg_key = "SolCast Weather"

    def __init__(self, api_key, data_dir="SolCast", file_prefix=""):
        super().__init__(data_dir, file_prefix)
        self.api_key = api_key

    def get_data(self, location):
        # If site id is not set, no request can be done
        site_id = location.solcast_site_id
        if pd.isna(site_id):
            return None
        api_call = "weather_sites"
        data = dict()
        for data_request in ["forecasts", "estimated_actuals"]:
            payload = {"api_key": self.api_key, "format": "json"}
            data.update(
                requests.get(
                    self.base_url
                    + api_call
                    + "/"
                    + location.solcast_site_id
                    + "/"
                    + data_request,
                    params=payload,
                ).json()
            )

        _log.debug("JSON response:")
        for l in json.dumps(data, indent=4).splitlines():
            _log.debug(l)
        """
        Example response when the API key is invalid:

        {
            "response_status": {
                "error_code": "NotFound",
                "message": "ApiKey does not exist",
                "errors": []
            }
        }
        """
        if "response_status" in data and "error_code" in data["response_status"]:
            _log.error(
                f"Unable to retrieve SolCast data for {location.name}."
                "Error: " + data["response_status"]["message"]
            )
            data = None

        return data

    def get_rows(self, location):
        """Gets data from API, extracts the useful information in a list of dicts
        suitable to be converted to a dataframe."""
        rtn = []
        data = self.get_data(location)
        if data is None:
            return []
        f = data["forecasts"][0]
        current_dt = pd.to_datetime(f["period_end"]).tz_localize(None) - _parse_period(
            f["period"]
        )
        rtn = []
        for f in data["estimated_actuals"]:
            rtn.append(
                {
                    "location": location.name,
                    "lat": location.lat,
                    "lon": location.lon,
                    "type": 0,
                    "current_dt": current_dt,
                    "dt": pd.to_datetime(f["period_end"]).tz_localize(None)
                    - _parse_period(f["period"]),
                    "clouds": f["cloud_opacity"],
                    "ghi": f["ghi"],
                    "ebh": f["ebh"],
                    "dni": f["dni"],
                    "dhi": f["dhi"],
                }
            )
        for f in data["forecasts"]:
            rtn.append(
                {
                    "location": location.name,
                    "lat": location.lat,
                    "lon": location.lon,
                    "type": f["period"],
                    "current_dt": current_dt,
                    "dt": pd.to_datetime(f["period_end"]).tz_localize(None)
                    - _parse_period(f["period"]),
                    "temp": f["air_temp"],
                    "clouds": f["cloud_opacity"],
                    "ghi": f["ghi"],
                    "ghi90": f["ghi90"],
                    "ghi10": f["ghi10"],
                    "ebh": f["ebh"],
                    "dni": f["dni"],
                    "dni10": f["dni10"],
                    "dni90": f["dni90"],
                    "dhi": f["dhi"],
                }
            )
        return rtn
