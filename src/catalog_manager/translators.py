import re

import pandas as pd

from .metadata import CoreDFMetadata, _ALLOWABLE_FREQS


class MetadataTranslatorError(Exception):
    pass


class MetadataTranslator:
    """
    Base class for translating intake-esm dataframe columns into intake-dataframe-catalog core columns
    """

    def __init__(self, translations):
        """
        Initialise a MetadataTranslator

        Parameters
        ----------
        translations: dict, optional
            Dictionary with keys corresponding to core metadata columns in the intake-dataframe-catalog
            (see catalog_manager.CoreDFMetadata) and values corresponding to functions that translate
            the intake-esm catalog to a :py:class:`~pandas:Series` or :py:class:`~pandas:DataFrame`
            of metadata for the intake-dataframe-catalog column. If a key has a value of None, it is
            assumed that this key exists as a column in the intake-esm dataframe. If values are not not
            callable they are input directly as metadata entries in the intake-esm dataframe for that
            key (column).
        """
        self.translations = translations
        self.validate()

    def validate(self):
        """
        Check that all core intake-dataframe-catalog columns have translators provided
        """
        if set(self.translations) - set(CoreDFMetadata.columns):
            raise MetadataTranslatorError(
                f"The input translations should be a dictionary with the keys: {', '.join(CoreDFMetadata.columns)}"
            )

    def translate(self, cat):
        """
        Translate a pandas dataframe

        Parameters
        ----------
        cat: :py:class:`~intake-esm.esm_datastore`
            The intake-esm catalog to translate

        Returns
        -------
        translated: :py:class:`~pandas.DataFrame`
            A dataframe of metadata translated from the input catalog
        """

        translated = {}
        for col, translation in self.translations.items():
            if translation:
                if callable(translation):
                    translated[col] = translation(cat)
                else:
                    translated[col] = pd.Series([translation] * len(cat.df))
            else:
                translated[col] = cat.df[col]

        return pd.concat(translated, axis="columns")


def _make_list(series, column):
    """
    Make entries in the provided column a list
    """
    return [series[column]]


def _get_cmip6_freq(series):
    """
    Parse frequency from CMIP6 metadata
    """
    table_id = series["table_id"]
    if table_id == "Oclim":
        # An annoying edge case
        return "1mon"
    for pattern in [s for s in _ALLOWABLE_FREQS]:
        get_int = False
        if r"\d+" in pattern:
            get_int = True
            pattern = pattern.replace(r"\d+", "")
        match = re.match(f".*({pattern})", table_id)
        if match:
            freq = match.groups()[0]
            if get_int:
                if match.start(1) > 0:
                    try:
                        n = int(table_id[match.start(1) - 1])
                        return f"{n}{freq}"
                    except ValueError:
                        pass
                return f"1{freq}"
            else:
                return freq


def _get_cmip6_realm(series):
    """
    Parse realm from CMIP6 metadata
    """
    table_id = series["table_id"]
    if any(re.match(f".*{pattern}.*", table_id) for pattern in ["Ant", "Gre", "SI"]):
        return "ice"
    if table_id == "3hr" or any(
        re.match(f".*{pattern}.*", table_id)
        for pattern in ["Lev", "Plev", "A", "CF", "E.*hr", "Z"]
    ):
        return "atmos"
    if any(re.match(f".*{pattern}.*", table_id) for pattern in ["O"]):
        return "ocean"
    if any(re.match(f".*{pattern}.*", table_id) for pattern in ["L"]):
        return "land"
    return "unknown"


SimpleMetadataTranslator = MetadataTranslator(
    {col: None for col in CoreDFMetadata.columns}
)

CosimaMetadataTranslator = MetadataTranslator(
    {
        "model": "ACCESS-OM2",
        "experiment": lambda cat: pd.Series([cat.name] * len(cat.df)),
        "description": lambda cat: pd.Series([cat.description] * len(cat.df)),
        "realm": None,
        "variable": None,
        "frequency": None,
    }
)

Cmip6MetadataTranslator = MetadataTranslator(
    {
        "model": "CMIP6",
        "experiment": "CMIP6_CMS",
        "description": "CLEX CMS indexed replicas of CMIP6 available on Gadi.",
        "realm": lambda cat: cat.df.apply(_get_cmip6_realm, axis="columns"),
        "variable": lambda cat: cat.df.apply(
            _make_list, column="variable_id", axis="columns"
        ),
        "frequency": lambda cat: cat.df.apply(_get_cmip6_freq, axis="columns"),
    }
)
