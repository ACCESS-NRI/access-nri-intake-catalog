# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""
Tools for translating metadata in an intake source into a metadata table to use in an intake-dataframe-catalog
like the ACCESS-NRI catalog
"""

from dataclasses import dataclass
from functools import partial
from typing import Callable, Optional

import pandas as pd
import tlz
from intake import DataSource

from . import COLUMNS_WITH_ITERABLES

FREQUENCY_TRANSLATIONS = {
    "3hrPt": "3hr",
    "6hrPt": "6hr",
    "daily": "1day",
    "day": "1day",
    "mon": "1mon",
    "monC": "1mon",
    "monClim": "1mon",
    "monPt": "1mon",
    "sem": "3mon",
    "subhrPt": "subhr",
    "yr": "1yr",
    "yrPt": "1yr",
}


def _to_tuple(series: pd.Series) -> pd.Series:
    """
    Make each entry in the provided series a tuple

    Parameters
    ----------
    series: :py:class:`~pandas.Series`
        A pandas Series or another object with an `apply` method
    """
    return series.apply(lambda x: (x,))


def tuplify_series(func: Callable) -> Callable:
    """
    Decorator that wraps a function that returns a pandas Series and converts
    each entry in the series to a tuple
    """

    def wrapper(*args, **kwargs):
        # Check if the first argument is 'self'
        if len(args) > 0 and hasattr(args[0], "__class__"):
            self = args[0]
            series = func(self, *args[1:], **kwargs)
        else:
            series = func(*args, **kwargs)
        return _to_tuple(series)

    return wrapper


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

    def translate(self, groupby: Optional[list[str]] = None) -> pd.DataFrame:
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
        self, core_colname: str, func: Callable, input_name: Optional[str] = None
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

    def _realm_translator(self) -> pd.Series:
        """
        Return realm, fixing a few issues
        """
        return _cmip_realm_translator(self.source.df[self._dispatch_keys.realm])

    @tuplify_series
    def _model_translator(self) -> pd.Series:
        """
        Return model from dispatch_keys.model
        """
        return self.source.df[self._dispatch_keys.model]

    @tuplify_series
    def _frequency_translator(self) -> pd.Series:
        """
        Return frequency, fixing a few issues
        """
        return self.source.df[self._dispatch_keys.frequency].apply(
            lambda x: FREQUENCY_TRANSLATIONS.get(x, x)
        )

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


class EraiTranslator(DefaultTranslator):
    """
    ERAI Translator for translating metadata from the NCI ERA-Interim intake datastore.
    """

    def __init__(self, source: DataSource, columns: list[str]):
        """
        Initialise a EraiTranslator

        Parameters
        ----------
        source: :py:class:`~intake.DataSource`
            The NCI ERA-Interim intake-esm datastore
        columns: list of str
            The columns to translate to (these are the core columns in the intake-dataframe-catalog)
        """

        super().__init__(source, columns)

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
            input_name="source_id", core_colname="model", func=super()._model_translator
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


@dataclass
class _DispatchKeys:
    """
    Data class to store the keys for the dispatch dictionary in the Translator classes
    """

    model: Optional[str] = None
    realm: Optional[str] = None
    frequency: Optional[str] = None
    variable: Optional[str] = None


class Mom6Translator(DefaultTranslator):
    """
    MOM6 Translator for translating metadata from the NCI MOM6 intake datastores.
    """

    def __init__(self, source, columns):
        """
        Initialise a MOM6

        Parameters
        ----------
        source: :py:class:`~intake.DataSource`
            The NCI MOM6 intake-esm datastore
        columns: list of str
            The columns to translate to (these are the core columns in the intake-dataframe-catalog)
        """
        super().__init__(source, columns)

    #     self._dispatch["model"] = self._model_translator

    # def _model_translator(self):
    #     """
    #     Return model (hard-coded)
    #     """
    #     return pd.Series(
    #         [
    #             "MOM6",
    #         ]
    #     )


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
