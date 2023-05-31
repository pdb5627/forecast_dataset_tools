import logging
from pathlib import Path

import click
import platformdirs

try:
    import tomllib  # type: ignore
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore

_log = logging.getLogger(__name__)

default_config_fname = "config.toml"
default_config_path = platformdirs.user_config_path("forecast_dataset_tools")
config_fpath1 = Path.cwd() / default_config_fname
config_fpath2 = default_config_path / default_config_fname

config_location_help = f"""Configuration filename. If this option is not provided,
    the program will look for a config file in the following locations in this order:

    \b
    {config_fpath1} (current directory)
    {config_fpath2} (application configuration directory)
    """

config_file_option = click.option(
    "-c",
    "--config/",
    "config_filename",
    type=click.Path(exists=True, dir_okay=False),
    help=config_location_help,
)

def find_and_load(fname=None, do_convert_paths=True):
    """Attempts to load config from fname. If fname is None, then attempts to
    load from the current working directory, then the application configuration
    directory."""
    if fname is not None:
        return load(fname, do_convert_paths)

    for fname in [config_fpath1, config_fpath2]:
        try:
            cfg = load(fname, do_convert_paths)
            _log.info(f"Configuration loaded from {fname}.")
            return cfg
        except FileNotFoundError:
            _log.debug(f"No configuration file found at {fname}.")
    else:
        raise FileNotFoundError("No configuration file could be found.")


def load(fname, do_convert_paths=True):
    with open(fname, "rb") as f:
        cfg = tomllib.load(f)
    if do_convert_paths:
        convert_paths(cfg)
    return cfg


def convert_paths(cfg):
    cfg["main_data_dir"] = Path(cfg["main_data_dir"]).expanduser().resolve()

    for subdir in ["dataset_db_file", "locations_file"]:
        cfg[subdir] = cfg["main_data_dir"] / cfg[subdir]

    for group in cfg.keys():
        if isinstance(cfg[group], dict) and "data_dir" in cfg[group]:
            cfg[group]["data_dir"] = cfg["main_data_dir"] / cfg[group]["data_dir"]
