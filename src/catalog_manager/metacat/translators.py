# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""
Tools for translating metadata in an intake subcatalog into a metadata table to use in an intake-dataframe-catalog
"""

import re
from functools import partial

import pandas as pd


class TranslatorError(Exception):
    pass


class DefaultTranslator:
    """
    Default Translator for translating metadata in an intake catalog into a :py:class:`~pandas.DataFrame`
    of metadata for use in an intake-dataframe-catalog.
    """

    def __init__(self, cat, columns):
        """
        Initialise a DefaultTranslator. This Translator works as follows:

        - If the input catalog is an intake-esm catalog, the translator will first look for the column in the
             esmcat.df attribute. If the catalog is not an intake-esm catalog, this step is skipped.
        - If that fails, the translator will then look for the column name as an attribute on the catalog iteself
        - If that fails, the translator will then look for the column name in the metadata attribute of the catalog

        Parameters
        ----------
        cat: :py:class:`~intake.DataSource`
            The catalog to use to do the translations
        columns: list of str
            The columns to translate
        """

        self.cat = cat
        self.columns = columns
        self._dispatch = {
            column: partial(self._default_translator, column=column)
            for column in columns
        }

    def _default_translator(self, column):
        """
        Try to translate a column from a catalog using the default translator. This translator works as follows:
        - If the input catalog is an intake-esm catalog, the translator will first look for the column in the
             esmcat.df attribute. If the catalog is not an intake-esm catalog, this step is skipped.
        - If that fails, the translator will then look for the column name as an attribute on the catalog iteself
        - If that fails, the translator will then look for the column name in the metadata attribute of the catalog

        Parameters
        ----------
        column: str
            The column to translate, e.g. "frequency"
        """
        if hasattr(self.cat, "esmcat"):
            try:
                return self.cat.df[column]
            except KeyError:
                len_df = len(self.cat.df)
        else:
            len_df = 1

        if hasattr(self.cat, column):
            val = getattr(self.cat, column)
        elif column in self.cat.metadata:
            val = self.cat.metadata[column]
        else:
            raise TranslatorError(
                f"Could not translate '{column}' from {self.cat.name} using {self.__class__.__name__}"
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

        def _list_unique(series):
            # TODO: This could be made more robust
            iterable_entries = isinstance(series.iloc[0], (list, tuple, set))
            uniques = sorted(
                set(
                    series.drop_duplicates()
                    .apply(lambda x: x if iterable_entries else [x])
                    .sum()
                )
            )
            return (
                uniques[0] if (len(uniques) == 1) & (not iterable_entries) else uniques
            )

        df = pd.concat(
            {col: func() for col, func in self._dispatch.items()}, axis="columns"
        )

        ungrouped_columns = list(set(self.columns) - set(groupby))
        df_grouped = (
            df.groupby(groupby)
            .agg({col: _list_unique for col in ungrouped_columns})
            .reset_index()
        )

        return df_grouped[self.columns]  # Preserve ordering


class Cmip6Translator(DefaultTranslator):
    """
    CMIP6 Translator for translating metadata from the NCI CMIP6 intake catalogs.
    """

    def __init__(self, cat, columns):
        """
        Initialise a Cmip6Translator

        Parameters
        ----------
        cat: :py:class:`~intake.DataSource`
            The NCI CMIP6 intake-esm catalog
        columns: list of str
            The columns to translate
        """

        super().__init__(cat, columns)
        self._dispatch["model"] = self._model_translator
        self._dispatch["realm"] = self._realm_translator
        self._dispatch["frequency"] = self._frequency_translator
        self._dispatch["variable"] = self._variable_translator

    def _model_translator(self):
        """
        Return model from source_id
        """
        return self.cat.df["source_id"]

    def _realm_translator(self):
        """
        Return realm, fixing a few issues
        """
        return _cmip_realm_translator(self.cat.df)

    def _frequency_translator(self):
        """
        Return frequency, fixing a few issues
        """
        return _cmip_frequency_translator(self.cat.df)

    def _variable_translator(self):
        """
        Return variable as a list
        """
        return to_list(self.cat.df["variable_id"])


class Cmip5Translator(DefaultTranslator):
    """
    CMIP5 Translator for translating metadata from the NCI CMIP5 intake catalogs.
    """

    def __init__(self, cat, columns):
        """
        Initialise a Cmip5Translator

        Parameters
        ----------
        cat: :py:class:`~intake.DataSource`
            The NCI CMIP5 intake-esm catalog
        columns: list of str
            The columns to translate
        """

        super().__init__(cat, columns)
        self._dispatch["realm"] = self._realm_translator
        self._dispatch["frequency"] = self._frequency_translator
        self._dispatch["variable"] = self._variable_translator

    def _realm_translator(self):
        """
        Return realm, fixing a few issues
        """
        return _cmip_realm_translator(self.cat.df)

    def _frequency_translator(self):
        """
        Return frequency, fixing a few issues
        """
        return _cmip_frequency_translator(self.cat.df)

    def _variable_translator(self):
        """
        Return variable as a list
        """
        return to_list(self.cat.df["variable"])


class EraiTranslator(DefaultTranslator):
    """
    ERAI Translator for translating metadata from the NCI ERA-Interim intake catalogs.
    """

    def __init__(self, cat, columns):
        """
        Initialise a EraiTranslator

        Parameters
        ----------
        cat: :py:class:`~intake.DataSource`
            The NCI ERA-Interim intake-esm catalog
        columns: list of str
            The columns to translate
        """

        super().__init__(cat, columns)
        self._dispatch["variable"] = self._variable_translator

    def _variable_translator(self):
        """
        Return variable as a list
        """
        return to_list(self.cat.df["variable"])


def _cmip_frequency_translator(df):
    """
    Return frequency from CMIP frequency metadata
    """

    def _parse(string):
        for remove in ["Pt", "C.*"]:  # Remove Pt, C, and Clim
            string = re.sub(remove, "", string)
        string = string.replace("daily", "day")  # Some incorrect metadata
        string = string.replace("sem", "3mon")  # CORDEX for seasonal mean
        return f"1{string}" if string[0] in ["m", "d", "y"] else string

    return df["frequency"].apply(lambda string: _parse(string))


def _cmip_realm_translator(df):
    """
    Return realm from CMIP realm metadata, fixing some issues
    """

    def _parse(string):
        if re.match("seaIce", string, flags=re.I):
            return "seaIce"
        elif re.match("landIce", string, flags=re.I):
            return "landIce"
        elif re.match("ocnBgchem", string, flags=re.I):
            return "ocnBgchem"
        elif re.match("atmos", string, flags=re.I):
            return "atmos"
        elif re.match("atmosChem", string, flags=re.I):
            return "atmosChem"
        elif re.match("aerosol", string, flags=re.I):
            return "aerosol"
        elif re.match("land", string, flags=re.I):
            return "land"
        elif re.match("ocean", string, flags=re.I):
            return "ocean"
        else:
            return "unknown"

    return df["realm"].apply(lambda string: _parse(string))


def to_list(series):
    """
    Make entries in the provided series a list

    Parameters
    ----------
    series: :py:class:`~pandas.Series`
        A pandas Series or another object with an `apply` method
    """
    return series.apply(lambda x: [x])


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
