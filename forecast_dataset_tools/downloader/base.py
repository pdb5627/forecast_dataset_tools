import logging
import os
from datetime import datetime, timezone
from math import nan

import pandas as pd

_no_flag = object()

logger = logging.getLogger(__name__)


class WeatherService:
    _cfg_key = "Set a value for this service's group in config files."
    _missing = _no_flag

    def __init__(self, data_dir="weather_service", file_prefix=""):
        self.data_dir = data_dir
        self.file_prefix = file_prefix

    def get_rows(self, location):
        """Override this function in subclasses. The important thing is
        for the column labels to be consistent. Here are some notes:
        current_dt: The datetime (UTC) when the data was provided from the service.
        dt: The datetime (UTC) corresponding to the weather observation or forecast.
        type: The type of data.
            0 for actual current or historical data
            1 for hourly forecast
            2 for daily forecast
        """
        pass

    def get_df(self, location):
        rows = self.get_rows(location)
        # Convert list of dicts to DataFrame
        df = pd.DataFrame(rows)
        return df

    def _g(cls, d, k, default=nan):
        """Accesses a member of a dict and either returns the value. If access fails
        for any reason, a default value is set. Some services may use a flag for
        missing data, in which case the passed missing parameter is replaced with nan.
        """
        try:
            val = d[k]
        except KeyError:
            val = default
            print(f'WARNING: ({cls.__class__}) Missing key "{k}"')

        if cls._missing is not _no_flag and (
            val == cls._missing or val == str(cls._missing)
        ):
            val = default
        return val

    def save_to_csv(self, df):
        """Save a DataFrame to csv in the directory and filenameing structure for the class."""
        dt = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
        fn = self.file_prefix
        if len(fn) > 0:
            fn += "_"
        fn += dt + ".csv"
        save_path = self.data_dir
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        df.to_csv(os.path.join(save_path, fn), index=False)
