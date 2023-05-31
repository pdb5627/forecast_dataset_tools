from .. import logging_config  # isort:skip

import inspect
import logging

import click
import pandas as pd

import forecast_dataset_tools.config as fdt_config

from .mgm_havadurumu import MGMHavaDurumu
from .openweather import OpenWeatherService
from .solcast import SolcastService

_log = logging.getLogger(__name__)

cls_list = [OpenWeatherService, SolcastService, MGMHavaDurumu]
cfg_cls_map = {cls._cfg_key: cls for cls in cls_list}


@click.command()
@fdt_config.config_file_option
def download(config_filename):
    """Download data using parameters from a TOML-format configuration file, and
    then export the data to csv files in the configured data directories."""
    click.echo("Starting to download....")
    cfg = fdt_config.load(config_filename)

    locations = pd.read_csv(cfg["locations_file"])

    # Retrieve results for any downloader classes mapped to groups in the config file
    for cfg_key in cfg:
        if cfg_key not in cfg_cls_map:
            continue
        _log.info(f"Processing {cfg_key}")

        results_list = []

        cls = cfg_cls_map[cfg_key]
        svc_cfg = cfg[cfg_key]
        svc_init_args = inspect.signature(cls.__init__).parameters
        svc_args = {p: svc_cfg[p] for p in svc_init_args if p in svc_cfg}
        svc = cls(**svc_args)

        for l in locations.itertuples():
            _log.debug(
                f"Preparing to download from service={type(svc)} for location={l.name}"
            )
            try:
                df_l = svc.get_df(l)
            except Exception as e:
                if isinstance(e, SystemExit):
                    raise e
                _log.error(
                    f"Exception while processing service={type(svc)}, location={l.name}",
                    exc_info=True,
                )
            else:
                if len(df_l) > 0:
                    results_list.append(df_l)

        if results_list:
            df = pd.concat(results_list)
            svc.save_to_csv(df)

    return None


if __name__ == "__main__":
    download()
