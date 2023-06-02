# Forecast Dataset Tools

Retrieve, save, process, and load time-series datasets, especially for PV power
forecasting. There are two sub-modules within the package:

The `downloader` sub-module includes classes for downloading data from the
weather forecast sources and saving it in csv files.

The `db_archiver` sub-module includes classes for reading downloaded data from
csv files, storing the raw data in a database (currently sqlite), pre-processing
the data to conform it to a desired frequency (e.g. hourly), and retrieving the
data pandas DataFrames. Data files that have already been read are logged in the
database for efficient identification of new data files.

Configuration is provided through a toml-format file. See the example
configuration file (`example_config.toml`) for available options.

The currently implemented datasets are as follows:

-   SolCast (download & database archival)
-   Turkish Meteorology Department (MGM) meteogram forecast (database archival only)
-   Turkish Meteorology Department (MGM) current weather and forecast (download only)
-   OpenWeather (download only)
-   ABB PV inverter output logs (database archival only)

The information in this file provides some information to assist new users to
get started, but for more details, users are directed to inspect the source
code.

If you are using this code and have questions about how to use it or need some
additional feature or dataset included, open an issue on GitHub.

## Installation

Currently this package is only distributed via its GitHub repository and is not
on PyPI.

To install using pip:

```
pip install git+https://github.com/pdb5627/forecast_dataset_tools/
```

Dependencies are listed in `environment.yml`, which can be used to install the
dependencies in a conda environment using conda. (Add the `--name` option to
specify a different name than the one in the environment file.)

For a new environment:
```
conda env create -f environment.yml
```

To add dependencies to an existing environment:
```
conda activate myenv
conda env update --file environment.yml  --name myenv
```

## Usage

This package can be used as a library and embedded into other Python code, and
it can be used as standalone command-line tools.

### CLI usage

When installed using pip, new scripts named `forecast_dataset_tools`,
`data_downloader`, and `db_archiver` are installed. Two sub-commands, accessible
either by their respective scripts directly or as sub-commands of the main CLI
interface, are then used to download or interact with the database data archive.
Their usage is as follows:

To download data:
```
forecast_dataset_tools download [OPTIONS] CONFIG_FILENAME

  Download data using parameters from CONFIG_FILENAME, a TOML-format
  configuration file, and then export the data to csv files in the configured
  data directories.

Options:
  --help  Show this message and exit.
```
or
```
data_downloader [OPTIONS] CONFIG_FILENAME
```

To add data to the archival database:
```
forecast_dataset_tools archive [OPTIONS] CONFIG_FILENAME

  Import csv data files into sqlite data archive using parameters from
  CONFIG_FILENAME, a TOML-format configuration file.

Options:
  --reset-db  Reset the database by dropping any existing data before
              importing data. The database will also be reset if reset_db is
              set to true in the config file. Otherwise, only new data is
              imported.
  --help      Show this message and exit.
```
or
```
db_archiver [OPTIONS] CONFIG_FILENAME
```

The cli interface uses configuration files to load the necessary data regarding
data sources and forecast locations. Information about data sources and local
file locations is loaded from a TOML configuration file. An example of such a
configuration file is included with the package as the file
`example_config.toml`. Since API keys may be specified in this file, users
should be careful not to commit their production configuration file to a public
code repository.

Information about locations at which forecasts are to be retrieved and stored is
loaded from a csv-formatted file. An example of such a file is provided with the
package as `example_locations.csv`.

For now the format of these files is not documented beyond the provided examples.

### Library usage

The `downloader` module includes classes for downloading data from various data
sources. These classes implement the following public methods:

-   `get_rows`: Retrieves and returns data as a list of dicts.
-   `get_df`: Retrieves and returns data as a pandas `DataFrame`.
-   `save_to_csv`: Saves the given DataFrame to file with naming based on the configured
    data directory, file prefix, and the current localized time.

The available downloader classes are `SolcastService`, `OpenWeatherService`, and
`MGMHavaDurumu`.

The `db_archiver` module includes classes for archiving downloaded data in an
sqlite database, applying some pre-processing, and retrieving data as a pandas
`DataFrame`. These classes implement the following public methods:

-   `import_all_data`: Import all available data into the database. Any existing
    data is dropped from the database first.
-   `import_new_data`: Import available data that has not already been imported.
    Existing data is kept.
-   `get_data_by_date`: Returns data beginning on start and going to end (not
    inclusive). Start and end may be dates or datetimes or strings that pandas
    can convert.

The `db_archiver` module also includes the function `interpolate_to_index`,
which uses linear interpolation to change a `DataFrame` from one datetime or
numerical index to another. This function is used internally in the
pre-processing step of the classes in the module, but it may be of use to users
as well.

## MGM server issues with SSL

The MGM meteograms can be downloaded using a tool such as curl from URLs of the
form `https://www.mgm.gov.tr//FTPDATA/sht/wrf/<il>_<ilçe>.png`. (MGM may change
this at any time.) The datagrams can then be digitized using this meteogram
extraction tool: https://github.com/pdb5627/meteogram_extract

As of May 2023, the MGM server is not able to negotiate an ssl connection that
openssl's default settings will allow. This can be worked around by using an
alternative openssl.cnf configuration, or by using an older version of openssl
(v1.1.1). A working openssl.cnf is available at
https://github.com/pdb5627/forecast_dataset_tools/blob/master/openssl.cnf. It
can be used while running a script by setting the OPENSSL_CONF envionment
variable to its location on disk. On a Linux shell, the command would look like
this:

```
>>> OPENSSL_CONF=~/openssl.cnf python ...
```
