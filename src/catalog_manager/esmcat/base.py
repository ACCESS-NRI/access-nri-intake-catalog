# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" Base builder for generating an intake-esm catalog """

import multiprocessing

from ecgtools.builder import Builder, INVALID_ASSET

from ..metadata import CoreESMMetadata


class ParserError(Exception):
    pass


class BaseBuilder(Builder):
    """
    Base class for creating intake-esm catalog builders. Not intended for direct use.
    This builds on the ecgtools.Builder class.
    """

    def __init__(
        self,
        paths,
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
        paths : list of str
            List of paths to crawl for assets/files.
        depth : int, optional
            Maximum depth to crawl for assets. Default is 0.
        exclude_patterns : list of str, optional
            List of glob patterns to exclude from crawling.
        include_patterns : list of str, optional
            List of glob patterns to include from crawling.
        data_format : str
            The data format. Valid values are netcdf, reference and zarr.
        groupby_attrs: List[str]
            Column names (attributes) that define data sets that can be aggegrated.
        aggregations : List[dict]
            List of aggregations to apply to query results, default None
        storage_options : dict, optional
            Parameters passed to the backend file-system such as Google Cloud Storage,
            Amazon Web Service S3
        joblib_parallel_kwargs : dict, optional
            Parameters passed to joblib.Parallel. Default is {}.
        """

        self.paths = paths
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
            path_column_name=CoreESMMetadata.path_column_name,
            variable_column_name=CoreESMMetadata.variable_column_name,
            data_format=self.data_format,
            groupby_attrs=self.groupby_attrs,
            aggregations=self.aggregations,
            esmcat_version="0.0.1",
            description=description,
            directory=directory,
            catalog_type="file",
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
                CoreESMMetadata.validate(info)
                return self

        raise ParserError(
            f"Parser returns no valid assets. Try parsing a single file with {self.parser}(file)"
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
        """This method should be overwritten"""
        pass
