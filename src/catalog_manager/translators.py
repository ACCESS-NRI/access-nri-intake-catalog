import re
import pandas as pd

from .metadata import CoreDFMetadata


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


def _to_list(df, column):
    """
    Make entries in the provided column a list
    """
    return df[column].apply(lambda x: [x])


def _get_cmip6_realm_freq(df, get):
    """
    Parse realm and frequency from CMIP6 metadata
    """
    table_id = df["table_id"]
    mapping = {
        "3hr": ("multi", "3hr"),  # "atmos", "land", "ocean"
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
        "E3hr": ("multi", "3hr"),  # "atmos", "land"
        "E3hrPt": ("multi", "3hr"),  # "atmos", "land"
        "E6hrZ": ("atmos", "6hr"),
        "Eday": ("multi", "1day"),  # "atmos", "ice", "land", "ocean"
        "EdayZ": ("atmos", "1day"),
        "Efx": ("multi", "fx"),  # "atmos", "ice", "land"
        "Emon": ("multi", "1mon"),  # "atmos", "land", "ocean"
        "EmonZ": ("multi", "1mon"),  # "atmos", "ocean"
        "Esubhr": ("atmos", "subhr"),
        "Eyr": ("multi", "1yr"),  # "land", "ocean"
        "IfxAnt": ("ice", "fx"),
        "IfxGre": ("ice", "fx"),
        "ImonAnt": ("ice", "1mon"),
        "ImonGre": ("ice", "1mon"),
        "IyrAnt": ("ice", "1yr"),
        "IyrGre": ("ice", "1yr"),
        "LImon": ("multi", "1mon"),  # "ice", "land"
        "Lmon": ("land", "1mon"),
        "Oclim": ("ocean", "1mon"),
        "Oday": ("ocean", "1day"),
        "Odec": ("ocean", "1dec"),
        "Ofx": ("ocean", "fx"),
        "Omon": ("ocean", "1mon"),
        "Oyr": ("ocean", "1yr"),
        "SIday": ("ice", "1day"),
        "SImon": ("ice", "1mon"),
        "day": ("multi", "1day"),  # "atmos", "ocean"
        "fx": ("multi", "fx"),  # "atmos", "ice", "land"
    }
    ind = {"realm": 0, "freq": 1}[get]
    return table_id.map({k: v[ind] for k, v in mapping.items()})


def _get_cmip5_freq(df):
    """
    Parse frequency from CMIP5 metadata
    """

    def _parse(s):
        s = re.sub("clim", "", s, flags=re.IGNORECASE)
        return f"1{s}" if s[0] in ["m", "d", "y"] else s

    return df["time_frequency"].apply(lambda s: _parse(s))


def _extract_realm(df, column):
    """
    Search for substrings matching allowing realms, ignoring case
    """

    def _parse(s):
        for realm in ["atmos", "ocean", "ice", "land"]:
            match = re.match(r".*" + realm + r".*", s, flags=re.IGNORECASE)
            if match:
                return realm
        raise ValueError(f"Could not match {s} to any realm")

    return df[column].apply(lambda s: _parse(s))


DefaultTranslator = MetadataTranslator(
    {
        "subcatalog": lambda cat: pd.Series([cat.name] * len(cat.df)),
        "description": lambda cat: pd.Series([cat.description] * len(cat.df)),
        "model": lambda cat: pd.Series([cat.metadata["model"]] * len(cat.df)),
        "realm": None,
        "frequency": None,
        "variable": None,
    }
)

Cmip6Translator = MetadataTranslator(
    {
        "subcatalog": lambda cat: pd.Series([cat.name] * len(cat.df)),
        "description": lambda cat: pd.Series([cat.description] * len(cat.df)),
        "model": lambda cat: cat.df["source_id"],
        "realm": lambda cat: _get_cmip6_realm_freq(cat.df, get="realm"),
        "frequency": lambda cat: _get_cmip6_realm_freq(cat.df, get="freq"),
        "variable": lambda cat: _to_list(cat.df, column="variable_id"),
    }
)

Cmip5Translator = MetadataTranslator(
    {
        "subcatalog": lambda cat: pd.Series([cat.name] * len(cat.df)),
        "description": lambda cat: pd.Series([cat.description] * len(cat.df)),
        "model": None,
        "realm": None,
        "frequency": lambda cat: _get_cmip5_freq(cat.df),
        "variable": lambda cat: _to_list(cat.df, column="variable"),
    }
)

EraiTranslator = MetadataTranslator(
    {
        "subcatalog": lambda cat: pd.Series([cat.name] * len(cat.df)),
        "description": lambda cat: pd.Series([cat.description] * len(cat.df)),
        "model": "ERA-Interim",
        "realm": lambda cat: _extract_realm(cat.df, column="realm"),
        "frequency": None,
        "variable": lambda cat: _to_list(cat.df, column="variable"),
    }
)
