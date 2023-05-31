from .. import logging_config  # isort:skip

import inspect
import logging
import shutil

import click
import sqlalchemy

import forecast_dataset_tools.config as fdt_config

from .abb_inverter_logger import ABBInverterDataSet
from .meteogram_forecast import MeteogramForecast
from .solcast_weather import SolCastWeather

_log = logging.getLogger(__name__)

cls_list = [ABBInverterDataSet, MeteogramForecast, SolCastWeather]
cfg_cls_map = {cls._cfg_key: cls for cls in cls_list}

context_settings = {"max_content_width": shutil.get_terminal_size().columns - 0}


@click.command(context_settings=context_settings)
@fdt_config.config_file_option
@logging_config.log_level_option
@click.option(
    "-r",
    "--reset-db",
    is_flag=True,
    help="Reset the database by dropping any existing data before importing data. "
    "The database will also be reset if reset_db is set to true in the config file. "
    "Otherwise, only new data is imported.",
)
def archive(config_filename, log_level, reset_db):
    """Import data files into sqlite data archive using parameters from a
    TOML-format configuration file."""
    logging.getLogger("forecast_dataset_tools").setLevel(log_level)

    cfg = fdt_config.find_and_load(config_filename)

    dataset_db_file = cfg["dataset_db_file"]
    engine = sqlalchemy.create_engine(
        f"sqlite+pysqlite:///{dataset_db_file}", echo=False
    )

    # Import results for any dataset classes mapped to groups in the config file
    for cfg_key in cfg:
        if cfg_key not in cfg_cls_map:
            continue

        _log.info(f"Processing {cfg_key}")

        cls = cfg_cls_map[cfg_key]
        ds_cfg = cfg[cfg_key]
        ds_init_args = inspect.signature(cls.__init__).parameters
        ds_args = {p: ds_cfg[p] for p in ds_init_args if p in ds_cfg}
        ds = cls(db_engine=engine, **ds_args)

        if reset_db or cfg.get("reset_db", False):
            ds.import_all_data()
        else:
            ds.import_new_data()


if __name__ == "__main__":
    archive()
