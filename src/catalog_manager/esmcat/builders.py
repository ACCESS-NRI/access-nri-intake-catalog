# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" Builders for generating intake-esm catalogs """

import re
import traceback
import multiprocessing
from pathlib import Path

import jsonschema

import xarray as xr

from ecgtools.builder import Builder, INVALID_ASSET, TRACEBACK

from . import schema
from .utils import get_timeinfo, strip_pattern_rh


class ParserError(Exception):
    pass


class BaseBuilder(Builder):
    """
    Base class for creating intake-esm catalog builders. Not intended for direct use.
    This builds on the ecgtools.Builder class.
    """

    def __init__(
        self,
        path,
        depth=0,
        exclude_patterns=None,
        include_patterns=None,
        data_format="netcdf4",
        groupby_attrs=None,
        aggregations=None,
        storage_options=None,
        joblib_parallel_kwargs={"n_jobs": multiprocessing.cpu_count()},
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

        super().__post_init_post_parse__()

    def _parse(self):
        super().parse(parsing_func=self.parser)

    def parse(self):
        """
        Parse metadata from assets.
        """
        self._parse()
        return self

    def _save(self, name, description, directory):
        super().save(
            name=name,
            path_column_name=schema["path_column"],
            variable_column_name=schema["variable_column"],
            data_format=self.data_format,
            groupby_attrs=self.groupby_attrs,
            aggregations=self.aggregations,
            esmcat_version="0.0.1",
            description=description,
            directory=directory,
            catalog_type="file",
            to_csv_kwargs={"compression": "gzip"},
        )

    def save(self, name, description, directory=None):
        """
        Save catalog contents to a file.

        Parameters
        ----------
        name: str
            The name of the file to save the catalog to.
        description : str
            Detailed multi-line description of the collection.
        directory: str, optional
            The directory to save the catalog to. If None, use the current directory.
        """

        if self.df.empty:
            raise ValueError(
                "intake-esm catalog has not yet been built. Please run `.build()` first"
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
                jsonschema.validate(info, schema["jsonschema"])
                return self

        raise ParserError(
            "Parser returns no valid assets. Try parsing a single file with Builder.parser(file)"
        )

    def build(self):
        """
        Builds a catalog from a list of netCDF files or zarr stores.
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
            .applymap(type)
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
        pass


class AccessOm2Builder(BaseBuilder):
    """Intake-esm catalog builder for ACCESS-OM2 COSIMA datasets"""

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
                }
            ],
        )

        super().__init__(**kwargs)

    @staticmethod
    def parser(file):
        try:
            filename = Path(file).stem

            # Get file id without any dates
            # - ocean-3d-v-1-monthly-pow02-ym_1958_04.nc
            # - iceh.057-daily.nc
            # - oceanbgc-3d-caco3-1-yearly-mean-y_2015.nc
            file_id = strip_pattern_rh(
                [r"\d{4}[-_]\d{2}", r"\d{4}", r"\d{3}"], filename
            )

            match_groups = re.match(
                r".*/([^/]*)/([^/]*)/output\d+/([^/]*)/.*\.nc", file
            ).groups()
            # configuration = match_groups[0]
            # experiment = match_groups[1]
            realm = match_groups[2]

            with xr.open_dataset(file, chunks={}, decode_times=False) as ds:
                variable_list = [var for var in ds if "long_name" in ds[var].attrs]

            info = {
                "path": str(file),
                "realm": realm,
                "variable": variable_list,
                "filename": filename,
                "file_id": file_id,
            }

            info["start_date"], info["end_date"], info["frequency"] = get_timeinfo(ds)

            return info

        except Exception:
            return {INVALID_ASSET: file, TRACEBACK: traceback.format_exc()}


class AccessEsm15Builder(BaseBuilder):
    """Intake-esm catalog builder for ACCESS-ESM1.5 datasets"""

    def __init__(self, path):
        """
        Initialise a AccessEsm15Builder

        Parameters
        ----------
        path : str or list of str
            Path or list of paths to crawl for assets/files.
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
                }
            ],
        )

        super().__init__(**kwargs)

    @staticmethod
    def parser(file):
        try:
            filename = Path(file).stem

            # Get file id without any dates
            # - iceh_m.2014-06.nc
            # - bz687a.pm107912_mon.nc
            # - bz687a.p7107912_mon.nc
            # - PI-GWL-B2035.pe-109904_dai.nc
            # - PI-1pct-02.pe-011802_dai.nc_dai.nc
            # - ocean_daily.nc-02531231
            file_id = strip_pattern_rh(
                [r"\d{4}[-_]\d{2}", r"\d{8}", r"\d{6}"], filename
            )

            match_groups = re.match(r".*/([^/]*)/history/([^/]*)/.*\.nc", file).groups()
            # experiment = match_groups[0]
            realm = match_groups[1]
            if realm == "atm":
                realm = "atmos"
            elif realm == "ocn":
                realm = "ocean"
            elif realm != "ice":
                raise ParserError(f"Could not translate {realm} to a realm")

            with xr.open_dataset(file, chunks={}, decode_times=False) as ds:
                variable_list = [var for var in ds if "long_name" in ds[var].attrs]

            info = {
                "path": str(file),
                "realm": realm,
                "variable": variable_list,
                "filename": filename,
                "file_id": file_id,
            }

            info["start_date"], info["end_date"], info["frequency"] = get_timeinfo(ds)

            return info

        except Exception:
            return {INVALID_ASSET: file, TRACEBACK: traceback.format_exc()}


# Include this so it is in the documentation
class AccessCm2Builder(AccessEsm15Builder):
    """Intake-esm catalog builder for ACCESS-CM2 datasets"""

    pass
