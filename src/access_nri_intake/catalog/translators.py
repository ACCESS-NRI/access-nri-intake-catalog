# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""
Tools for translating metadata in an intake source into a metadata table to use in an intake-dataframe-catalog
like the ACCESS-NRI catalog
"""

import re
from functools import partial

import pandas as pd
import tlz

from . import COLUMNS_WITH_ITERABLES


class TranslatorError(Exception):
    "Generic Exception for the Translator classes"
    pass


class DefaultTranslator:
    """
    Default Translator for translating metadata in an intake datastore into a :py:class:`~pandas.DataFrame`
    of metadata for use in an intake-dataframe-catalog.
    """

    def __init__(self, source, columns):
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
        self._dispatch = {
            column: partial(self._default_translator, column=column)
            for column in columns
        }

    def _default_translator(self, column):
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
                    return to_tuple(series)
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
            if isinstance(val, list):
                val = tuple(val)
        else:
            raise TranslatorError(
                f"Could not translate '{column}' from {self.source.name} using {self.__class__.__name__}"
            )

        return pd.Series([val] * len_df)

    def translate(self, groupby):
        """
        Return the translated :py:class:`~pandas.DataFrame` of metadata and merge into set of
        set of rows with unique values of the columns specified.

        Parameters
        ----------
        groupby: list of str
            Core metadata columns to group by before merging metadata across remaining core columns.
        """

        def _find_unique(series):
            """
            Return a set of unique values in a series
            """

            values = series.dropna()
            iterable_entries = series.name in COLUMNS_WITH_ITERABLES
            type_ = type(series.iloc[0])

            if iterable_entries:
                values = tlz.concat(values)

            uniques = tuple(set(values))

            return (
                uniques[0]
                if (len(uniques) == 1) & (not iterable_entries)
                else type_(uniques)
            )

        df = pd.concat(
            {col: func() for col, func in self._dispatch.items()}, axis="columns"
        )

        ungrouped_columns = list(set(self.columns) - set(groupby))
        df_grouped = (
            df.groupby(groupby)
            .agg({col: _find_unique for col in ungrouped_columns})
            .reset_index()
        )

        return df_grouped[self.columns]  # Preserve ordering


class Cmip6Translator(DefaultTranslator):
    """
    CMIP6 Translator for translating metadata from the NCI CMIP6 intake datastores.
    """

    def __init__(self, source, columns):
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
        self._dispatch["model"] = self._model_translator
        self._dispatch["realm"] = self._realm_translator
        self._dispatch["frequency"] = self._frequency_translator
        self._dispatch["variable"] = self._variable_translator

    def _model_translator(self):
        """
        Return model from source_id
        """
        return to_tuple(self.source.df["source_id"])

    def _realm_translator(self):
        """
        Return realm, fixing a few issues
        """
        return _cmip_realm_translator(self.source.df)

    def _frequency_translator(self):
        """
        Return frequency, fixing a few issues
        """
        return _cmip_frequency_translator(self.source.df)

    def _variable_translator(self):
        """
        Return variable as a tuple
        """
        return to_tuple(self.source.df["variable_id"])


class Cmip5Translator(DefaultTranslator):
    """
    CMIP5 Translator for translating metadata from the NCI CMIP5 intake datastores.
    """

    def __init__(self, source, columns):
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
        self._dispatch["model"] = self._model_translator
        self._dispatch["realm"] = self._realm_translator
        self._dispatch["frequency"] = self._frequency_translator
        self._dispatch["variable"] = self._variable_translator

    def _model_translator(self):
        """
        Return variable as a tuple
        """
        return to_tuple(self.source.df["model"])

    def _realm_translator(self):
        """
        Return realm, fixing a few issues
        """
        return _cmip_realm_translator(self.source.df)

    def _frequency_translator(self):
        """
        Return frequency, fixing a few issues
        """
        return _cmip_frequency_translator(self.source.df)

    def _variable_translator(self):
        """
        Return variable as a tuple
        """
        return to_tuple(self.source.df["variable"])


class EraiTranslator(DefaultTranslator):
    """
    ERAI Translator for translating metadata from the NCI ERA-Interim intake datastore.
    """

    def __init__(self, source, columns):
        """
        Initialise a EraiTranslator

        Parameters
        ----------
        source: :py:class:`~intake.DataSource`
            The NCI ERA-Interim intake-esm datastore
        columns: list of str
            The columns to translate (these are the core columns in the intake-dataframe-catalog)
        """

        super().__init__(source, columns)
        self._dispatch["variable"] = self._variable_translator

    def _variable_translator(self):
        """
        Return variable as a tuple
        """
        return to_tuple(self.source.df["variable"])


def _cmip_frequency_translator(df):
    """
    Return frequency from CMIP frequency metadata
    """

    def _parse(string):
        for remove in ["Pt", "C.*"]:  # Remove Pt, C, and Clim
            string = re.sub(remove, "", string)
        string = string.replace("daily", "day")  # Some incorrect metadata
        string = string.replace("sem", "3mon")  # CORDEX for seasonal mean
        return (f"1{string}",) if string[0] in ["m", "d", "y"] else (string,)

    return df["frequency"].apply(lambda string: _parse(string))


def _cmip_realm_translator(df):
    """
    Return realm from CMIP realm metadata, fixing some issues
    """

    def _parse(string):
        raw_realms = string.split(" ")
        realms = []
        for realm in raw_realms:
            if re.match("na", realm, flags=re.I):
                realms.append("none")
            elif re.match("seaIce", realm, flags=re.I):
                realms.append("seaIce")
            elif re.match("landIce", realm, flags=re.I):
                realms.append("landIce")
            elif re.match("ocnBgchem", realm, flags=re.I):
                realms.append("ocnBgchem")
            elif re.match("atmos", realm, flags=re.I):
                realms.append("atmos")
            elif re.match("atmosChem", realm, flags=re.I):
                realms.append("atmosChem")
            elif re.match("aerosol", realm, flags=re.I):
                realms.append("aerosol")
            elif re.match("land", realm, flags=re.I):
                realms.append("land")
            elif re.match("ocean", realm, flags=re.I):
                realms.append("ocean")
            else:
                realms.append("unknown")
        return tuple(set(realms))

    return df["realm"].apply(lambda string: _parse(string))


def to_tuple(series):
    """
    Make entries in the provided series a tuple

    Parameters
    ----------
    series: :py:class:`~pandas.Series`
        A pandas Series or another object with an `apply` method
    """
    return series.apply(lambda x: (x,))


def match_substrings(series, substrings):
    """
    Search for a list of substrings in each entry, ignoring case, and return the
    one that's found.

    Parameters
    ----------
    series: :py:class:`~pandas.Series`
        A pandas Series or another object with an `apply` method
    substrings: list of str
        A list of substrings to try and match on each entry in series
    """

    def _parse(s):
        for substring in substrings:
            match = re.match(rf".*{substring}.*", s, flags=re.IGNORECASE)
            if match:
                return substring
        raise ValueError(f"Could not match {s} to any substring")

    return series.apply(lambda s: _parse(s))
