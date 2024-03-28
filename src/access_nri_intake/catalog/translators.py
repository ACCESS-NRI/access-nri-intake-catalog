# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""
Tools for translating metadata in an intake source into a metadata table to use in an intake-dataframe-catalog
like the ACCESS-NRI catalog
"""

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

    def translate(self, groupby=None):
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
        return _to_tuple(self.source.df["source_id"])

    def _realm_translator(self):
        """
        Return realm, fixing a few issues
        """
        return _cmip_realm_translator(self.source.df["realm"])

    def _frequency_translator(self):
        """
        Return frequency, fixing a few issues
        """
        return _to_tuple(_cmip_frequency_translator(self.source.df["frequency"]))

    def _variable_translator(self):
        """
        Return variable as a tuple
        """
        return _to_tuple(self.source.df["variable_id"])


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
        return _to_tuple(self.source.df["model"])

    def _realm_translator(self):
        """
        Return realm, fixing a few issues
        """
        return _cmip_realm_translator(self.source.df["realm"])

    def _frequency_translator(self):
        """
        Return frequency, fixing a few issues
        """
        return _to_tuple(_cmip_frequency_translator(self.source.df["frequency"]))

    def _variable_translator(self):
        """
        Return variable as a tuple
        """
        return _to_tuple(self.source.df["variable"])


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
            The columns to translate to (these are the core columns in the intake-dataframe-catalog)
        """

        super().__init__(source, columns)
        self._dispatch["variable"] = self._variable_translator

    def _variable_translator(self):
        """
        Return variable as a tuple
        """
        return _to_tuple(self.source.df["variable"])


def _cmip_frequency_translator(series):
    """
    Return frequency from CMIP frequency metadata
    """

    def _translate(string):
        translations = {
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

        try:
            return translations[string]
        except KeyError:
            return string

    return series.apply(lambda string: _translate(string))


def _cmip_realm_translator(series):
    """
    Return realm from CMIP realm metadata, fixing some issues. This function returns
    a tuple as there are sometimes multiple realms per cmip asset
    """

    def _translate(string):
        translations = {
            "na": "none",
            "landonly": "land",
            "ocnBgChem": "ocnBgchem",
            "seaice": "seaIce",
        }

        raw_realms = string.split(" ")
        realms = []
        for realm in raw_realms:
            try:
                realm = translations[realm]
            except KeyError:
                pass
            if realm not in realms:
                realms.append(realm)
        return tuple(realms)

    return series.apply(lambda string: _translate(string))


def _to_tuple(series):
    """
    Make each entry in the provided series a tuple

    Parameters
    ----------
    series: :py:class:`~pandas.Series`
        A pandas Series or another object with an `apply` method
    """
    return series.apply(lambda x: (x,))
