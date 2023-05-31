"""Console script for forecast_dataset_tools."""
from . import logging_config  # isort:skip

import click

from .db_archiver.cli import archive
from .downloader.cli import download


@click.group()
def cli(args=None):
    pass

cli.add_command(download)
cli.add_command(archive)


if __name__ == "__main__":
    cli()
