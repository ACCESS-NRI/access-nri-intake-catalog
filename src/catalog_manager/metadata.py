import re


_ALLOWABLE_FREQS = ["fx", "subhr", r"\d+hr", r"\d+day", r"\d+mon", r"\d+yr", r"\d+dec"]


class CoreMetadataError(Exception):
    pass


class CoreMetadataBase:
    """
    Base class for keeping track of core metadata columns anf formats and validating against them.
    Not intended to be used directly
    """

    def __init__(self):
        """
        Override this. Initialise self._metadata_columns with dictionary of  {"column_descriptor":
        ("column_name", "description of type of column entries", "function for checking columns entries"}
        """
        self._metadata_columns = {}

    @classmethod
    def validate(cls, metadata):
        """
        Validate a dictionary of metadata relative to core columns

        Parameters
        ----------
        metadata: dict
            Dictionary with keys corresponding to metadata columns and values corresponding to metadata
            entries
        """
        core = cls()
        _validate_metadata(core._metadata_columns, metadata)

    @classmethod
    @property
    def columns(cls):
        return [val[0] for val in cls()._metadata_columns.values()]


class CoreESMMetadata(CoreMetadataBase):
    """
    Core intake-esm catalog metadata columns and formats. Not really intended to be used by users.
    """

    def __init__(self):
        self._metadata_columns = {
            "path_column": ("path", "strings", lambda x: isinstance(x, str)),
            "realm_column": ("realm", "strings", lambda x: isinstance(x, str)),
            "variable_column": (
                "variable",
                "lists of strings",
                lambda x: isinstance(x, list) and all(isinstance(s, str) for s in x),
            ),
            "frequency_column": (
                "frequency",
                f"one of {', '.join(_ALLOWABLE_FREQS)}",
                lambda x: any(
                    re.match(pattern, x)
                    for pattern in [freq for freq in _ALLOWABLE_FREQS]
                ),
            ),
            "start_date_column": (
                "start_date",
                "date strings with the format %Y-%m-%d, %H:%M:%S",
                lambda x: True
                if re.match(r"\d\d\d\d-\d\d-\d\d,\s\d\d:\d\d:\d\d", x)
                else False,
            ),
            "end_date_column": (
                "end_date",
                "date strings with the format %Y-%m-%d, %H:%M:%S",
                lambda x: True
                if re.match(r"\d\d\d\d-\d\d-\d\d,\s\d\d:\d\d:\d\d", x)
                else False,
            ),
        }

    @classmethod
    @property
    def path_column_name(cls):
        return cls()._metadata_columns["path_column"][0]

    @classmethod
    @property
    def variable_column_name(cls):
        return cls()._metadata_columns["variable_column"][0]


class CoreDFMetadata(CoreMetadataBase):
    """
    Core intake-dataframe-catalog catalog metadata columns and formats. Not really intended to be used
    by users.
    """

    def __init__(self):
        self._metadata_columns = {
            "model_column": ("model", "strings", lambda x: isinstance(x, str)),
            "experiment_column": (
                "experiment",
                "strings",
                lambda x: isinstance(x, str),
            ),
            "description_column": (
                "description",
                "strings",
                lambda x: isinstance(x, str),
            ),
            "realm_column": ("realm", "strings", lambda x: isinstance(x, str)),
            "variable_column": (
                "variable",
                "lists of strings",
                lambda x: isinstance(x, list) and all(isinstance(s, str) for s in x),
            ),
            "frequency_column": (
                "frequency",
                f"one of {', '.join(_ALLOWABLE_FREQS)}",
                lambda x: any(
                    re.match(pattern, x)
                    for pattern in [freq for freq in _ALLOWABLE_FREQS]
                ),
            ),
        }

        # Columns to groupby when creating intake-dataframe-datalog metadata from intake-esm metadata
        # (see catalog_manager.CatalogManager.parse_esm_metadata)
        self.groupby_columns = ["realm", "frequency"]

        # Column name to use as yaml_column in intake-dataframe-datalog
        self._yaml_column = "yaml"

        # Column name to use as name_column in intake-dataframe-datalog
        self._name_column = self._metadata_columns["experiment_column"][0]

    @classmethod
    @property
    def groupby_columns(cls):
        return cls().groupby_columns

    @classmethod
    @property
    def yaml_column(cls):
        return cls()._yaml_column

    @classmethod
    @property
    def name_column(cls):
        return cls()._name_column


def _validate_metadata(template, metadata):
    """
    Validate a dictionary of metadata relative to core intake-esm columns

    Parameters
    ----------
    template: dict
        Template of core metadata columns, their names, descriptions and functions to evaluate if metadata
        entries are valid
    metadata: dict
        Dictionary with keys corresponding to metadata columns and values corresponding to metadata
        entries
    """
    for col, (name, descr, func) in template.items():
        if not ((name in metadata) and (func(metadata[name]))):
            raise CoreMetadataError(
                f"The catalog must include core column '{name}' with entries that are {descr}"
            )
