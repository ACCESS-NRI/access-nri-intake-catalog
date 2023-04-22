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
            print(val)
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
        self._dispatch["realm"] = partial(self._realm_frequency_translator, get="realm")
        self._dispatch["frequency"] = partial(
            self._realm_frequency_translator, get="frequency"
        )
        self._dispatch["variable"] = self._variable_translator

    def _model_translator(self):
        """
        Return model from source_id
        """
        return self.cat.df["source_id"]

    def _realm_frequency_translator(self, get):
        """
        Return realm and frequency from table_id (as best as possible)

        TODO: should read these directly from 'realm' and 'frequency' columns
        once these are added to the intake catalog
        """
        table_id = self.cat.df["table_id"]
        mapping = {
            "3hr": ("unknown", "3hr"),  # "atmos", "land", "ocean"
            "6hrLev": ("atmos", "6hr"),
            "6hrPlev": ("atmos", "6hr"),
            "6hrPlevPt": ("atmos", "6hr"),
            "AERday": ("atmos", "1day"),
            "AERhr": ("atmos", "1hr"),
            "AERmon": ("atmos", "1mon"),
            "AERmonZ": ("atmos", "1mon"),
            "Amon": ("atmos", "1mon"),
            "CF3hr": ("atmos", "3hr"),
            "CFday": ("atmos", "1day"),
            "CFmon": ("atmos", "1mon"),
            "CFsubhr": ("atmos", "subhr"),
            "E1hr": ("atmos", "1hr"),
            "E1hrClimMon": ("atmos", "1hr"),
            "E3hr": ("unknown", "3hr"),  # "atmos", "land"
            "E3hrPt": ("unknown", "3hr"),  # "atmos", "land"
            "E6hrZ": ("atmos", "6hr"),
            "Eday": ("unknown", "1day"),  # "atmos", "ice", "land", "ocean"
            "EdayZ": ("atmos", "1day"),
            "Efx": ("unknown", "fx"),  # "atmos", "ice", "land"
            "Emon": ("unknown", "1mon"),  # "atmos", "land", "ocean"
            "EmonZ": ("unknown", "1mon"),  # "atmos", "ocean"
            "Esubhr": ("atmos", "subhr"),
            "Eyr": ("unknown", "1yr"),  # "land", "ocean"
            "IfxAnt": ("landIce", "fx"),
            "IfxGre": ("landIce", "fx"),
            "ImonAnt": ("landIce", "1mon"),
            "ImonGre": ("landIce", "1mon"),
            "IyrAnt": ("landIce", "1yr"),
            "IyrGre": ("landIce", "1yr"),
            "LImon": ("unknown", "1mon"),  # "ice", "land"
            "Lmon": ("land", "1mon"),
            "Oclim": ("ocean", "1mon"),
            "Oday": ("ocean", "1day"),
            "Odec": ("ocean", "1dec"),
            "Ofx": ("ocean", "fx"),
            "Omon": ("ocean", "1mon"),
            "Oyr": ("ocean", "1yr"),
            "SIday": ("seaIce", "1day"),
            "SImon": ("seaIce", "1mon"),
            "day": ("unknown", "1day"),  # "atmos", "ocean"
            "fx": ("unknown", "fx"),  # "atmos", "ice", "land"
        }
        ind = {"realm": 0, "frequency": 1}[get]
        return table_id.map({k: v[ind] for k, v in mapping.items()})

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
        self._dispatch["frequency"] = self._frequency_translator
        self._dispatch["variable"] = self._variable_translator

    def _frequency_translator(self):
        """
        Return frequency from time_frequency
        """

        def _parse(s):
            s = re.sub("clim", "", s, flags=re.IGNORECASE)
            return f"1{s}" if s[0] in ["m", "d", "y"] else s

        return self.cat.df["time_frequency"].apply(lambda s: _parse(s))

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
