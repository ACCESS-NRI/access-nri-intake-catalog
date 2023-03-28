# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

""" Tools for managing intake catalogues """

import os

import yaml

# import jsonschema
import pandas as pd
from intake_esm.cat import (
    Assets,
    Attribute,
    AggregationControl,
)

from . import parsers
from .cat import MetaCatalogModel


# config_schema = {
#         'type': 'object',
#         'properties': {
#             'model': {'type': 'string'},
#             'catalogs': {
#                 'type': 'object',
#                 'properties': {
#                     'catalog_names': {
#                         'type': 'array',
#                         'items': {'type': 'string'},
#                     },
#                 },
#             },
#             'parser': {'type': 'string'},
#             'search': {
#                 'type': 'object',
#                 'properties': {
#                     'depth': {'type': 'integer'},
#                     'exclude_patterns': {
#                         'type': 'array',
#                         'items': {'type': 'string'},
#                     },
#                     'include_patterns': {
#                         'type': 'array',
#                         'items': {'type': 'string'},
#                     },
#                 },
#             },
#         },
#         'required': ['id','catalogs','parser','search'],
#     }


class CatalogExistsError(Exception):
    "Exception for trying to write catalog that already exists"
    pass


class CatalogBuilder:
    """
    Build intake-esm catalog(s) base on a provided config file
    """

    def __init__(self, config, metacatalog):
        """
        Initialise a CatalogBuilder

        Parameters
        ----------
        config: str
            Path to the config yaml file describing the catalog to build/add
        metacatalog: str
            Path to the metacatalog
        """

        with open(config) as f:
            config = yaml.safe_load(f)

        # jsonschema.validate(config, config_schema)

        self.model = config.get("model")
        self.catalogs = config.get("catalogs")
        self.parser = getattr(parsers, config.get("parser"))
        self.build_kwargs = config.get("search")
        self.groupby_attrs = config.get("aggregation_control")["groupby_attrs"]
        self.aggregations = config.get("aggregation_control")["aggregations"]

        self.metacatalog = metacatalog

    def build(self, catalogs_dir, add_to_metacatalog=True, overwrite=False):
        """
        Build the intake-esm catalog(s)

        Parameters
        ----------
        catalogs_dir: str
            Where to output catalog(s)
        add_to_metacatalog: boolean, optional
            Whether or not to add the catalog(s) to the metacatalog
        overwrite: boolean, optional
            Whether to overwrite any existing catalog(s) with the same name
        """

        import multiprocessing
        from ecgtools import Builder

        esmcat_version = "0.0.1"

        ncpu = multiprocessing.cpu_count()

        for cat_name, cat_contents in self.catalogs.items():

            root_dir = cat_contents["root_dirs"]
            description = cat_contents["description"]

            json_file = os.path.abspath(f"{os.path.join(catalogs_dir, cat_name)}.json")
            if os.path.isfile(json_file):
                if not overwrite:
                    raise CatalogExistsError(
                        f"A catalog already exists for {cat_name}. To overwrite, "
                        "pass `overwrite=True` to CatalogBuilder.build"
                    )

            builder = Builder(
                root_dir,
                **self.build_kwargs,
                joblib_parallel_kwargs={"n_jobs": ncpu},
            ).build(parsing_func=self.parser)

            builder.save(
                name=cat_name,
                path_column_name="path",
                variable_column_name="variable",
                data_format="netcdf",
                groupby_attrs=self.groupby_attrs,
                aggregations=self.aggregations,
                esmcat_version=esmcat_version,
                description=description,
                directory=catalogs_dir,
                catalog_type="file",
            )

            if add_to_metacatalog:
                self._add_to_metacatalog(
                    cat_name,
                    builder.df,
                    json_file,
                )

    def _add_to_metacatalog(self, name, df, json_file):
        """
        Add an intake-esm catalogue to the metacatalog

        Parameters
        ----------
        name: str
            The catalog name
        df: pandas Dataframe
            Dataframe for the intake-esm catalog being added
        json_file: str
            The path to the intake-esm obj json file
        """

        def _get_variables_union(df, variable_column_name="variable"):
            """Get the union of all variables in a dataframe"""
            variable_sets = df[variable_column_name].apply(set)
            return sorted(list(set.union(*variable_sets.to_list())))

        cat_df = pd.DataFrame(
            [
                {
                    "model": self.model,
                    "experiment": name,
                    "realm": sorted(list(set(df["realm"].to_list()))),
                    "variable": _get_variables_union(df),
                    "frequency": sorted(list(set(df["frequency"].to_list()))),
                    "dataset_catalog": json_file,
                }
            ]
        )

        if os.path.isfile(self.metacatalog):
            meta = MetaCatalogModel.load(self.metacatalog)

            if name in list(meta.df.experiment):
                meta._df.loc[meta.df["experiment"] == name] = cat_df
            else:
                meta._df = pd.concat([meta._df, cat_df], ignore_index=True)
        else:
            attributes = [
                Attribute(column_name=column, vocabulary="")
                for column in cat_df.columns
            ]
            _aggregation_control = AggregationControl(
                variable_column_name="variable",
                groupby_attrs=["model", "experiment"],
                aggregations=[],
            )
            meta = MetaCatalogModel(
                esmcat_version="0.0.1",
                description="A test meta-esm catalog",
                attributes=attributes,
                aggregation_control=_aggregation_control,
                assets=Assets(column_name="dataset_catalog", format="netcdf"),
            )
            meta._df = cat_df

        meta.save(name="meta", catalog_type="file")
