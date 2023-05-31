""" Package to read in the various datasets that I have collected and return a
consistent dataframe. The dataframe will have the following index and columns:
index: DateTimeIndex is a naive timestamp in UTC time.

The data will be resampled to an hourly basis using a mean.

"""
from .abb_inverter_logger import ABBInverterDataSet
from .meteogram_forecast import MeteogramForecast
from .solcast_weather import SolCastWeather

# from .clearsky_model import ClearskyModel  # Need to sort out pvlib and ems.solar_model dependency
