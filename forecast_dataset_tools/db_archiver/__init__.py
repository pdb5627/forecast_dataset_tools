""" Package to read in the various datasets that I have collected and return a
consistent dataframe. The dataframe will have the following index and columns:
index: DateTimeIndex is a naive timestamp in UTC time.
'P_out': PV output power, normalized to nominal output, if available, or maximum
         output, if the nominal output is not available.
Additional columns may be present, but are not guaranteed. (Column names may be
standardized in the future.)

The data will be resampled to an hourly basis using a mean.

"""
from .abb_inverter_logger import ABBInverterDataSet
from .meteogram_forecast import MeteogramForecast
from .solcast_weather import SolCastWeather

# from .clearsky_model import ClearskyModel  # Need to sort out pvlib and ems.solar_model dependency
