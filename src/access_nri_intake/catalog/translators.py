# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""
Tools for translating metadata in an intake source into a metadata table to use in an intake-dataframe-catalog
like the ACCESS-NRI catalog
"""

from dataclasses import dataclass
from functools import partial
from typing import Callable

import pandas as pd
import tlz
from intake import DataSource

from . import COLUMNS_WITH_ITERABLES
from .utils import _to_tuple, trace_failure, tuplify_series

__all__ = [
    "Cmip6Translator",
    "Cmip5Translator",
    "BarpaTranslator",
    "CordexTranslator",
    "Era5Translator",
    "CcamTranslator",
    "NarclimTranslator",
]

FREQUENCY_TRANSLATIONS = {
    "monthly-averaged-by-hour": "1hr",
    "monthly-averaged-by-day": "1hr",
    "3hrPt": "3hr",
    "6hrPt": "6hr",
    "daily": "1day",
    "day": "1day",
    "mon": "1mon",
    "monthly-averaged": "1mon",
    "monC": "1mon",
    "monClim": "1mon",
    "monPt": "1mon",
    "sem": "3mon",
    "subhrPt": "subhr",
    "yr": "1yr",
    "yrPt": "1yr",
}


class TranslatorError(Exception):
    "Generic Exception for the Translator classes"

    pass


class DefaultTranslator:
    """
    Default Translator for translating metadata in an intake datastore into a :py:class:`~pandas.DataFrame`
    of metadata for use in an intake-dataframe-catalog.
    """

    def __init__(self, source: DataSource, columns: list[str]):
        """
        Initialise a DefaultTranslator. This Translator works as follows:

        - If the input source is an intake-esm datastore, the translator will first look for the column in the
          esmcat.df attribute, casting iterable columns to tuples. If the source is not an intake-esm datastore,
          this step is skipped.
        - If that fails, the translator will then look for the column name as an attribute on the source itself
        - If that fails, the translator will then look for the column name in the metadata attribute of the source

        Parameters
        ----------
        source: :py:class:`~intake.DataSource`
            The source to translate from
        columns: list of str
            The columns to translate to (these are the core columns in the intake-dataframe-catalog)
        """

        self.source = source
        self.columns = columns
        self._dispatch: dict[str, Callable[[], pd.Series]] = {
            column: partial(self._default_translator, column=column)
            for column in columns
        }
        self._dispatch_keys = _DispatchKeys()

    def _default_translator(self, column: str) -> pd.Series:
        """
        Try to translate a column from a source using the default translator. This translator works as follows:
        - If the input source is an intake-esm datastore, the translator will first look for the column in the
             esmcat.df attribute, casting iterable columns to tuples. If the source is not an intake-esm datastore,
             this step is skipped.
        - If that fails, the translator will then look for the column name as an attribute on the source itself
        - If that fails, the translator will then look for the column name in the metadata attribute of the source

        Parameters
        ----------
        column: str
            The column to translate, e.g. "frequency"
        """
        if hasattr(self.source, "esmcat"):
            try:
                series = self.source.df[column]

                # Cast to tuples
                if column in self.source.esmcat.columns_with_iterables:
                    return series.apply(tuple)
                elif column in COLUMNS_WITH_ITERABLES:
                    return _to_tuple(series)
                else:
                    return series
            except KeyError:
                len_df = len(self.source.df)
        else:
            len_df = 1

        if hasattr(self.source, column):
            val = getattr(self.source, column)
        elif column in self.source.metadata:
            val = self.source.metadata[column]
            # Some metadata fields can be a value _or_ array
            if isinstance(val, (list, tuple, set)):
                val = tuple(val)
            elif column in COLUMNS_WITH_ITERABLES:
                val = (val,)
        else:
            raise TranslatorError(
                f"Could not translate '{column}' from {self.source.name} using {self.__class__.__name__}"
            )

        return pd.Series([val] * len_df)

    def translate(self, groupby: list[str] | None = None) -> pd.DataFrame:
        """
        Return the translated :py:class:`~pandas.DataFrame` of metadata and merge into set of
        set of rows with unique values of the columns specified.

        Parameters
        ----------
        groupby: list of str, optional
            Core metadata columns to group by before merging metadata across remaining core columns.
        """

        def _unique_values(series):
            """
            Return unique values in a series
            """

            values = series.dropna()
            iterable_entries = series.name in COLUMNS_WITH_ITERABLES

            if iterable_entries:
                type_ = type(values.iloc[0])
                values = tlz.concat(values)
                return type_(set(values))
            else:
                series_array = series.to_numpy()
                if all(series_array[0] == series_array):
                    return series_array[0]
                else:
                    raise TranslatorError(
                        f"Column '{series.name}' contains multiple values within a merged group. In order to be able "
                        f"to merge, the entries in column '{series.name}' must be of iterable type list, tuple or set."
                    )

        df = pd.concat(
            {col: func() for col, func in self._dispatch.items()}, axis="columns"
        )

        if groupby:
            ungrouped_columns = list(set(self.columns) - set(groupby))
            df = (
                df.groupby(groupby)
                .agg({col: _unique_values for col in ungrouped_columns})
                .reset_index()
            )

        return df[self.columns]  # Preserve ordering

    def set_dispatch(
        self, core_colname: str, func: Callable, input_name: str | None = None
    ):
        """
        Set a dispatch function for a column. Typically only required when either:
            1. `core_colname != input_name`
            2. A custom translation function (`func`) is required.

        Parameters
        ----------
        core_colname: str
            The core column name to translate to
        input_name: str, optional
            The name of the column in the source. If not provided, this defaults
            to none, and no translation will occur
        func: callable
            The function to translate the column
        """
        if core_colname not in ["model", "realm", "frequency", "variable"]:
            raise TranslatorError(
                f"'core_colname' must be one of 'model', 'realm', 'frequency', 'variable', not {core_colname}"
            )
        self._dispatch[core_colname] = func
        setattr(self._dispatch_keys, core_colname, input_name)

    @trace_failure
    def _realm_translator(self) -> pd.Series:
        """
        Return realm, fixing a few issues
        """
        return _cmip_realm_translator(self.source.df[self._dispatch_keys.realm])

    @trace_failure
    @tuplify_series
    def _model_translator(self) -> pd.Series:
        """
        Return model from dispatch_keys.model
        """
        return self.source.df[self._dispatch_keys.model]

    @trace_failure
    @tuplify_series
    def _frequency_translator(self) -> pd.Series:
        """
        Return frequency, fixing a few issues
        """
        return self.source.df[self._dispatch_keys.frequency].apply(
            lambda x: FREQUENCY_TRANSLATIONS.get(x, x)
        )

    @trace_failure
    @tuplify_series
    def _variable_translator(self) -> pd.Series:
        """
        Return variable as a tuple
        """
        return self.source.df[self._dispatch_keys.variable]


class Cmip6Translator(DefaultTranslator):
    """
    CMIP6 Translator for translating metadata from the NCI CMIP6 intake datastores.
    """

    def __init__(self, source: DataSource, columns: list[str]):
        """
        Initialise a Cmip6Translator

        Parameters
        ----------
        source: :py:class:`~intake.DataSource`
            The NCI CMIP6 intake-esm datastore
        columns: list of str
            The columns to translate to (these are the core columns in the intake-dataframe-catalog)
        """

        super().__init__(source, columns)
        self.set_dispatch(
            input_name="source_id", core_colname="model", func=super()._model_translator
        )
        self.set_dispatch(
            input_name="realm", core_colname="realm", func=super()._realm_translator
        )
        self.set_dispatch(
            input_name="frequency",
            core_colname="frequency",
            func=super()._frequency_translator,
        )
        self.set_dispatch(
            input_name="variable_id",
            core_colname="variable",
            func=super()._variable_translator,
        )


class Cmip5Translator(DefaultTranslator):
    """
    CMIP5 Translator for translating metadata from the NCI CMIP5 intake datastores.
    """

    def __init__(self, source: DataSource, columns: list[str]):
        """
        Initialise a Cmip5Translator

        Parameters
        ----------
        source: :py:class:`~intake.DataSource`
            The NCI CMIP5 intake-esm datastore
        columns: list of str
            The columns to translate to (these are the core columns in the intake-dataframe-catalog)
        """

        super().__init__(source, columns)
        self.set_dispatch(
            input_name="model", core_colname="model", func=super()._model_translator
        )
        self.set_dispatch(
            input_name="realm", core_colname="realm", func=super()._realm_translator
        )
        self.set_dispatch(
            input_name="frequency",
            core_colname="frequency",
            func=super()._frequency_translator,
        )
        self.set_dispatch(
            input_name="variable",
            core_colname="variable",
            func=super()._variable_translator,
        )


class BarpaTranslator(DefaultTranslator):
    """
    Barpa Translator for translating metadata from the NCI BARPA intake datastores.
    """

    def __init__(self, source, columns):
        """
        Initialise a BarpaTranslator

        Parameters
        ----------
        source: :py:class:`~intake.DataSource`
            The NCI BARPA intake-esm datastore
        columns: list of str
            The columns to translate to (these are the core columns in the intake-dataframe-catalog)
        """

        super().__init__(source, columns)
        self.set_dispatch(
            input_name="source_id", core_colname="model", func=super()._model_translator
        )
        self.set_dispatch(
            input_name="realm", core_colname="realm", func=self._realm_translator
        )
        self.set_dispatch(
            input_name="freq",
            core_colname="frequency",
            func=super()._frequency_translator,
        )
        self.set_dispatch(
            input_name="variable_id",
            core_colname="variable",
            func=super()._variable_translator,
        )

    def _realm_translator(self):
        """
        Return realm, fixing a few issues
        """
        return self.source.df.apply(lambda x: ("none",), 1)


class CordexTranslator(DefaultTranslator):
    """
    Cordex Translator for translating metadata from the NCI CORDEX intake datastores.
    """

    def __init__(self, source, columns):
        """
        Initialise a CordexTranslator

        Parameters
        ----------
        source: :py:class:`~intake.DataSource`
            The NCI CORDEX intake-esm datastore
        columns: list of str
            The columns to translate to (these are the core columns in the intake-dataframe-catalog)
        """

        super().__init__(source, columns)
        self.set_dispatch(
            input_name="project_id",
            core_colname="model",
            func=super()._model_translator,
        )
        self.set_dispatch(
            input_name="variable_id",
            core_colname="variable",
            func=super()._variable_translator,
        )
        self.set_dispatch(
            input_name="realm", core_colname="realm", func=self._realm_translator
        )

    def _realm_translator(self):
        """
        Return realm, fixing a few issues
        """
        return self.source.df.apply(lambda x: ("none",), 1)


class Era5Translator(DefaultTranslator):
    """
    Era5 Translator for translating metadata from the NCI ERA5 intake datastores.
    """

    def __init__(self, source, columns):
        """
        Initialise a Era5Translator

        Parameters
        ----------
        source: :py:class:`~intake.DataSource`
            The NCI ERA5 intake-esm datastore
        columns: list of str
            The columns to translate to (these are the core columns in the intake-dataframe-catalog)
        """

        super().__init__(source, columns)
        self.set_dispatch(
            input_name="variable",
            core_colname="variable",
            func=super()._variable_translator,
        )
        self.set_dispatch(
            input_name="stream", core_colname="realm", func=self._realm_translator
        )
        self.set_dispatch(
            input_name="path", core_colname="frequency", func=self._frequency_translator
        )
        self.set_dispatch(
            input_name="path", core_colname="model", func=self._model_translator
        )

    @tuplify_series
    @trace_failure
    def _model_translator(self):
        """
        Get the model from the path. This is a slightly hacky approach, using the
        following logic:
        - Dir structure follows the form : `'/g/data/rt52/$MODEL/...`
        where model is one of 'era5', 'era5t', 'era5-preliminary', 'era5-1',
        'era5-derived'.
        """
        return self.source.df["path"].str.split("/").str[4]

    def _realm_translator(self):
        """
        Return realm. Not clear how we can extract this from the ERA5 data, so
        we'll just return 'none' for now.
        """
        return self.source.df.apply(lambda x: ("none",), 1)

    @tuplify_series
    @trace_failure
    def _frequency_translator(self):
        """
        Get the frequency from the path
        """
        config_str = self.source.df["path"].str.split("/").str[6].copy()
        """
        ERA5 contains some datasets where the frequency isn't readily identifiable:
        - 'reanalysis' is at 1hour frequency
        - 'v3-1' is at 1day frequency
        - 'v4-0' is at 1day frequency
        - 'v1-1' is at 1hour frequency

        These are going to get preprocessed here so that we don't make the
        FREQUENCIES dictionary large and confusing.
        """
        ERA5_FREQUENCY_TRANSLATIONS = {
            "reanalysis": "1hr",
            "v3-1": "1day",
            "v4-0": "1day",
            "v1-1": "1hr",
        }

        preproc_config_str = config_str.apply(
            lambda x: ERA5_FREQUENCY_TRANSLATIONS.get(x, x)
        )

        return preproc_config_str.apply(lambda x: FREQUENCY_TRANSLATIONS.get(x, x))


class CcamTranslator(DefaultTranslator):
    """
    Ccam Translator for translating metadata from the NCI CCAM intake datastores.
    """

    def __init__(self, source, columns):
        """
        Initialise a CcamTranslator

        Parameters
        ----------
        source: :py:class:`~intake.DataSource`
            The NCI CCAM intake-esm datastore
        columns: list of str
            The columns to translate to (these are the core columns in the intake-dataframe-catalog)
        """

        super().__init__(source, columns)
        self.set_dispatch(
            input_name="project_id",
            core_colname="model",
            func=super()._model_translator,
        )
        self.set_dispatch(
            input_name="variable_id",
            core_colname="variable",
            func=super()._variable_translator,
        )
        self.set_dispatch(
            input_name="realm",
            core_colname="realm",
            func=self._realm_translator,
        )
        self.set_dispatch(
            input_name="frequency",
            core_colname="frequency",
            func=super()._frequency_translator,
        )

    def _realm_translator(self):
        """
        Realm is not available in the CCAM metadata, so we'll just return
        ('none',) for now.
        """
        return self.source.df.apply(lambda x: ("none",), 1)


class NarclimTranslator(DefaultTranslator):
    def __init__(self, source, columns):
        """
        Initialise a BarpaTranslator

        Parameters
        ----------
        source: :py:class:`~intake.DataSource`
            The NCI BARPA intake-esm datastore
        columns: list of str
            The columns to translate to (these are the core columns in the intake-dataframe-catalog)
        """

        super().__init__(source, columns)
        self.set_dispatch(
            input_name="source_id",
            core_colname="model",
            func=super()._model_translator,
        )
        self.set_dispatch(
            input_name="realm",
            core_colname="realm",
            func=self._realm_translator,
        )
        self.set_dispatch(
            input_name="frequency",
            core_colname="frequency",
            func=super()._frequency_translator,
        )
        self.set_dispatch(
            input_name="variable_id",
            core_colname="variable",
            func=super()._variable_translator,
        )

    def _realm_translator(self):
        """
        Return realm, fixing a few issues
        """
        return self.source.df.apply(lambda x: ("atmos",), 1)


@dataclass
class _DispatchKeys:
    """
    Data class to store the keys for the dispatch dictionary in the Translator classes
    """

    model: str | None = None
    realm: str | None = None
    frequency: str | None = None
    variable: str | None = None


def _cmip_realm_translator(series) -> pd.Series:
    """
    Return realm from CMIP realm metadata, fixing some issues. This function takes
    a series of strings and returns a series of tuples as there are sometimes multiple
    realms per cmip asset
    """

    def _translate(string: str) -> tuple[str, ...]:
        translations = {
            "na": "none",
            "landonly": "land",
            "ocnBgChem": "ocnBgchem",
            "seaice": "seaIce",
        }

        raw_realms = string.split(" ")
        realms = set()
        for realm in raw_realms:
            realm = translations.get(realm, realm)
            realms |= {realm}
        return tuple(realms)

    return series.apply(lambda string: _translate(string))
