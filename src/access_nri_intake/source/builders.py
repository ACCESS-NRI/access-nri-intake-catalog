# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Builders for generating Intake-ESM datastores"""

import multiprocessing
import re
import traceback
from pathlib import Path

import xarray as xr
from ecgtools.builder import INVALID_ASSET, TRACEBACK, Builder

from ..utils import validate_against_schema
from . import ESM_JSONSCHEMA, PATH_COLUMN, VARIABLE_COLUMN
from .utils import (
    EmptyFileError,
    _AccessNCFileInfo,
    _VarInfo,
    get_timeinfo,
)

# Frequency translations
FREQUENCIES: dict[str, tuple[int, str]] = {
    "daily": (1, "day"),
    "_dai$": (1, "day"),
    "month": (1, "mon"),
    "_mon$": (1, "mon"),
    "1mon": (1, "mon"),
    "yearly": (1, "yr"),
    "_ann$": (1, "yr"),
}

# ACCESS output file patterns
PATTERNS_HELPERS = {
    "not_multi_digit": "(?:\\d(?!\\d)|[^\\d](?=\\d)|[^\\d](?!\\d))",
    "om3_components": "(?:cice|mom6|ww3)",
    "ymds": "\\d{4}[_,\\-]\\d{2}[_,\\-]\\d{2}[_,\\-]\\d{5}",
    "ymd": "\\d{4}[_,\\-]\\d{2}[_,\\-]\\d{2}",
    "ym": "\\d{4}[_,\\-]\\d{2}",
    "y": "\\d{4}",
}


class ParserError(Exception):
    pass


class BaseBuilder(Builder):
    """
    Base class for creating Intake-ESM datastore builders. Not intended for direct use.
    This builds on the ecgtools.Builder class.
    """

    # Base class carries an empty set
    PATTERNS: list = []

    def __init__(
        self,
        path: str | list[str],
        depth: int = 0,
        exclude_patterns: list[str] | None = None,
        include_patterns: list[str] | None = None,
        data_format: str = "netcdf",
        groupby_attrs: list[str] | None = None,
        aggregations: list[dict] | None = None,
        storage_options: dict | None = None,
        joblib_parallel_kwargs: dict = {"n_jobs": multiprocessing.cpu_count()},
    ):
        """
        This method should be overwritten. The expection is that some of these arguments
        will be hardcoded in sub classes of this class.

        Parameters
        ----------
        path: str or list of str
            Path or list of path to crawl for assets/files.
        depth: int, optional
            Maximum depth to crawl for assets. Default is 0.
        exclude_patterns: list of str, optional
            List of glob patterns to exclude from crawling.
        include_patterns: list of str, optional
            List of glob patterns to include from crawling.
        data_format: str
            The data format. Valid values are 'netcdf', 'reference' and 'zarr'.
        groupby_attrs: List[str]
            Column names (attributes) that define data sets that can be aggegrated.
        aggregations: List[dict]
            List of aggregations to apply to query results, default None
        storage_options: dict, optional
            Parameters passed to the backend file-system such as Google Cloud Storage,
            Amazon Web Service S3
        joblib_parallel_kwargs: dict, optional
            Parameters passed to joblib.Parallel. Default is {}.
        """

        if isinstance(path, str):
            path = [path]

        self.paths = path
        self.depth = depth
        self.exclude_patterns = exclude_patterns
        self.include_patterns = include_patterns
        self.data_format = data_format
        self.groupby_attrs = groupby_attrs
        self.aggregations = aggregations
        self.storage_options = storage_options
        self.joblib_parallel_kwargs = joblib_parallel_kwargs

        super().__post_init__()

    def _parse(self):
        super().parse(parsing_func=self.parser)

    def parse(self):
        """
        Parse metadata from assets.
        """
        self._parse()
        return self

    def _save(self, name: str, description: str, directory: str | None):
        super().save(
            name=name,
            path_column_name=PATH_COLUMN,
            variable_column_name=VARIABLE_COLUMN,
            data_format=self.data_format,
            groupby_attrs=self.groupby_attrs,
            aggregations=self.aggregations,
            esmcat_version="0.0.1",
            description=description,
            directory=directory,
            catalog_type="file",
            to_csv_kwargs={"compression": "gzip"},
        )

    def save(self, name: str, description: str, directory: str | None = None) -> None:
        """
        Save datastore contents to a file.

        Parameters
        ----------
        name: str
            The name of the file to save the datastore to.
        description : str
            Detailed multi-line description of the collection.
        directory: str, optional
            The directory to save the datastore to. If None, use the current directory.
        """

        if self.df.empty:
            raise ValueError(
                "Intake-ESM datastore has not yet been built. Please run `.build()` first"
            )

        self._save(name, description, directory)

    def validate_parser(self):
        """
        Run the parser on a single file and check the schema of the info being parsed
        """

        if not self.assets:
            raise ValueError(
                "asset list provided is None. Please run `.get_assets()` first"
            )

        for asset in self.assets:
            info = self.parser(asset)
            if INVALID_ASSET not in info:
                validate_against_schema(info, ESM_JSONSCHEMA)
                return self

        raise ParserError(
            "Parser returns no valid assets. Try parsing a single file with Builder.parser(file)"
        )

    def build(self):
        """
        Builds a datastore from a list of netCDF files or zarr stores.
        """

        self.get_assets().validate_parser().parse().clean_dataframe()

        return self

    @property
    def columns_with_iterables(self):
        """
        Return a set of the columns that have iterables
        """
        # Stolen from intake-esm.cat.ESMCatalogModel
        if self.df.empty:
            return set()
        has_iterables = (
            self.df.sample(20, replace=True)
            .map(type)
            .isin([list, tuple, set])
            .any()
            .to_dict()
        )
        return {column for column, check in has_iterables.items() if check}

    @staticmethod
    def parser(file):
        """
        Parse info from a file asset

        Parameters
        ----------
        file: str
            The path to the file
        """
        # This method should be overwritten
        raise NotImplementedError

    @classmethod
    def parse_access_filename(
        cls,
        filename: str,
        patterns: list[str] | None = None,
        frequencies: dict = FREQUENCIES,
        redaction_fill: str = "X",
    ) -> tuple[str, str | None, str | None]:
        """
        Parse an ACCESS model filename and return a file id and any time information

        Parameters
        ----------
        filename: str
            The filename to parse with the extension removed
        patterns: list of str, optional
            A list of regex patterns to match against the filename. If None, use the class PATTERNS
        frequencies: dict, optional
            A dictionary of regex patterns to match against the filename to determine the frequency
        redaction_fill: str, optional
            The character to replace time information with. Defaults to "X"

        Returns
        -------
        file_id: str
            The file id constructed by redacting time information and replacing non-python characters
            with underscores
        timestamp: str | None
            A string of the redacted time information (e.g. "1990-01") if available, otherwise None
        frequency: str | None
            The frequency of the file if available in the filename, otherwise None
        """
        if patterns is None:
            patterns = cls.PATTERNS

        # Try to determine frequency
        frequency = None
        for pattern, freq in frequencies.items():
            if re.search(pattern, filename):
                frequency = freq
                break

        # Parse file id
        file_id = filename
        timestamp = None
        for pattern in patterns:
            match = re.match(pattern, file_id)
            if match:
                timestamp = match.group(1)
                redaction = re.sub(r"\d", redaction_fill, timestamp)
                file_id = (
                    file_id[: match.start(1)] + redaction + file_id[match.end(1) :]
                )
                break

        # Remove non-python characters from file ids
        file_id = re.sub(r"[-.]", "_", file_id)
        file_id = re.sub(r"_+", "_", file_id).strip("_")

        return file_id, timestamp, frequency

    @classmethod
    def parse_access_ncfile(
        cls, file: str, time_dim: str = "time"
    ) -> _AccessNCFileInfo:
        """
        Get Intake-ESM datastore entry info from an ACCESS netcdf file

        Parameters
        ----------
        fname: str
            The path to the netcdf file
        time_dim: str
            The name of the time dimension

        Returns
        -------
        output_nc_info: _AccessNCFileInfo
            A dataclass containing the information parsed from the file

        Raises
        ------
        EmptyFileError: If the file contains no variables
        """

        file_path = Path(file)

        file_id, filename_timestamp, filename_frequency = cls.parse_access_filename(
            file_path.stem
        )

        with xr.open_dataset(
            file,
            chunks={},
            decode_cf=False,
            decode_times=False,
            decode_coords=False,
        ) as ds:
            dvars = _VarInfo()

            for var in ds.variables:
                attrs = ds[var].attrs
                dvars.append_attrs(var, attrs)  # type: ignore

            start_date, end_date, frequency = get_timeinfo(
                ds, filename_frequency, time_dim
            )

        if not dvars.variable_list:
            raise EmptyFileError("This file contains no variables")

        output_ncfile = _AccessNCFileInfo(
            filename=file_path.name,
            path=file,
            file_id=file_id,
            filename_timestamp=filename_timestamp,
            frequency=frequency,
            start_date=start_date,
            end_date=end_date,
            **dvars.to_var_info_dict(),
        )

        return output_ncfile


class AccessOm2Builder(BaseBuilder):
    """Intake-ESM datastore builder for ACCESS-OM2 COSIMA datasets"""

    PATTERNS = [
        rf"^iceh.*\.({PATTERNS_HELPERS['ymd']}|{PATTERNS_HELPERS['ym']})$",  # ACCESS-ESM1.5/OM2/CM2 ice
        rf"^iceh.*\.(\d{{3}})-{PATTERNS_HELPERS['not_multi_digit']}.*",  # ACCESS-OM2 ice
        rf"^ocean.*[_,-](?:ymd|ym|y)_({PATTERNS_HELPERS['ymd']}|{PATTERNS_HELPERS['ym']}|{PATTERNS_HELPERS['y']})(?:$|[_,-]{PATTERNS_HELPERS['not_multi_digit']}.*)",  # ACCESS-OM2 ocean
        r"^ocean.*[^\d]_(\d{2})$",  # A few wierd files in ACCESS-OM2 01deg_jra55v13_ryf9091
    ]

    def __init__(self, path):
        """
        Initialise a AccessOm2Builder

        Parameters
        ----------
        path : str or list of str
            Path or list of paths to crawl for assets/files.
        """

        kwargs = dict(
            path=path,
            depth=3,
            exclude_patterns=["*restart*", "*o2i.nc"],
            include_patterns=["*.nc"],
            data_format="netcdf",
            groupby_attrs=["file_id", "frequency"],
            aggregations=[
                {
                    "type": "join_existing",
                    "attribute_name": "start_date",
                    "options": {
                        "dim": "time",
                        "combine": "by_coords",
                    },
                },
            ],
        )

        super().__init__(**kwargs)

    @classmethod
    def parser(cls, file) -> dict:
        try:
            matches = re.match(r".*/output\d+/([^/]*)/.*\.nc", file)
            if matches:
                realm = matches.groups()[0]

            if realm == "ice":
                realm = "seaIce"

            nc_info = cls.parse_access_ncfile(file)
            ncinfo_dict = nc_info.to_dict()

            ncinfo_dict["realm"] = realm

            return ncinfo_dict

        except Exception:
            return {INVALID_ASSET: file, TRACEBACK: traceback.format_exc()}


class AccessOm3Builder(BaseBuilder):
    """Intake-ESM datastore builder for ACCESS-OM3 COSIMA datasets"""

    PATTERNS = [
        rf"[^\.]*\.{PATTERNS_HELPERS['om3_components']}\..*?({PATTERNS_HELPERS['ymds']}|{PATTERNS_HELPERS['ymd']}|{PATTERNS_HELPERS['ym']}|{PATTERNS_HELPERS['y']})(?:$|{PATTERNS_HELPERS['not_multi_digit']})",  # ACCESS-OM3
    ]

    def __init__(self, path):
        """
        Initialise a AccessOm3Builder

        Parameters
        ----------
        path : str or list of str
            Path or list of paths to crawl for assets/files.
        """

        kwargs = dict(
            path=path,
            depth=2,
            exclude_patterns=[
                "*restart*",
                "*MOM_IC.nc",
                "*ocean_geometry.nc",
                "*ocean.stats.nc",
                "*Vertical_coordinate.nc",
            ],
            include_patterns=["*.nc"],
            data_format="netcdf",
            groupby_attrs=["file_id", "frequency"],
            aggregations=[
                {
                    "type": "join_existing",
                    "attribute_name": "start_date",
                    "options": {
                        "dim": "time",
                        "combine": "by_coords",
                    },
                },
            ],
        )

        super().__init__(**kwargs)

    @classmethod
    def parser(cls, file) -> dict:
        try:
            output_nc_info = cls.parse_access_ncfile(file)
            ncinfo_dict = output_nc_info.to_dict()

            if "mom6" in ncinfo_dict["filename"]:
                realm = "ocean"
            elif "ww3" in ncinfo_dict["filename"]:
                realm = "wave"
            elif "cice" in ncinfo_dict["filename"]:
                realm = "seaIce"
            else:
                raise ParserError(f"Cannot determine realm for file {file}")
            ncinfo_dict["realm"] = realm

            return ncinfo_dict

        except Exception:
            return {INVALID_ASSET: file, TRACEBACK: traceback.format_exc()}


class AccessEsm15Builder(BaseBuilder):
    """Intake-ESM datastore builder for ACCESS-ESM1.5 datasets"""

    PATTERNS = [
        rf"^iceh.*\.({PATTERNS_HELPERS['ymd']}|{PATTERNS_HELPERS['ym']})$",  # ACCESS-ESM1.5/OM2/CM2 ice
        r"^.*\.p.-(\d{6})_.*",  # ACCESS-ESM1.5 atmosphere
    ]

    def __init__(self, path, ensemble):
        """
        Initialise a AccessEsm15Builder

        Parameters
        ----------
        path: str or list of str
            Path or list of paths to crawl for assets/files.
        ensemble: boolean
            Whether to treat each path as a separate member of an ensemble to join
            along a new member dimension
        """

        kwargs = dict(
            path=path,
            depth=3,
            exclude_patterns=["*restart*"],
            include_patterns=["*.nc*"],
            data_format="netcdf",
            groupby_attrs=["file_id", "frequency"],
            aggregations=[
                {
                    "type": "join_existing",
                    "attribute_name": "start_date",
                    "options": {
                        "dim": "time",
                        "combine": "by_coords",
                    },
                },
            ],
        )

        if ensemble:
            kwargs["aggregations"] += [
                {
                    "type": "join_new",
                    "attribute_name": "member",
                },
            ]

        super().__init__(**kwargs)

    @classmethod
    def parser(cls, file):
        try:
            match_groups = re.match(r".*/([^/]*)/history/([^/]*)/.*\.nc", file).groups()
            exp_id = match_groups[0]
            realm = match_groups[1]

            realm_mapping = {"atm": "atmos", "ocn": "ocean", "ice": "seaIce"}

            nc_info = cls.parse_access_ncfile(file)
            ncinfo_dict = nc_info.to_dict()

            # Remove exp_id from file id so that members can be part of the same dataset
            ncinfo_dict["file_id"] = re.sub(
                exp_id,
                "",
                ncinfo_dict["file_id"],
            ).strip("_")
            ncinfo_dict["realm"] = realm_mapping[realm]
            ncinfo_dict["member"] = exp_id

            return ncinfo_dict

        except Exception:
            return {INVALID_ASSET: file, TRACEBACK: traceback.format_exc()}


# Include this so it is in the documentation
class AccessCm2Builder(AccessEsm15Builder):
    """Intake-ESM datastore builder for ACCESS-CM2 datasets"""

    PATTERNS = [
        rf"^iceh.*\.({PATTERNS_HELPERS['ymd']}|{PATTERNS_HELPERS['ym']})$",  # ACCESS-ESM1.5/OM2/CM2 ice
        rf"^iceh.*\.({PATTERNS_HELPERS['ym']})-{PATTERNS_HELPERS['not_multi_digit']}.*",  # ACCESS-CM2 ice
        r"^.*\.p.(\d{6})_.*",  # ACCESS-CM2 atmosphere
    ]
