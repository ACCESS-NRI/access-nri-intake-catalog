import re


_ALLOWABLE_FREQS = ["fx", "subhr", r"\d+hr", r"\d+day", r"\d+mon", r"\d+yr", r"\d+dec"]


class CoreMetadataError(Exception):
    pass


class CoreMetadataBase:
    @classmethod
    def validate(cls, metadata):
        """
        Validate a dictionary of metadata relative to core intake-esm columns

        metadata: dict
            Dictionary with keys corresponding to metadata columns and values corresponding to metadata
            entries
        """
        core = cls()
        _validate_metadata(core._columns, metadata)

    @classmethod
    @property
    def columns(cls):
        return [val[0] for val in cls()._columns.values()]


class CoreESMMetadata(CoreMetadataBase):
    def __init__(self):
        self._columns = {
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
        return cls()._columns["path_column"][0]

    @classmethod
    @property
    def variable_column_name(cls):
        return cls()._columns["variable_column"][0]


class CoreDFMetadata(CoreMetadataBase):
    def __init__(self):
        self._columns = {
            # name_column is the name of the subcatalog
            "name_column": ("experiment", "strings", lambda x: isinstance(x, str)),
            "model_column": ("model", "strings", lambda x: isinstance(x, str)),
            "realm_column": ("strings", lambda x: isinstance(x, str)),
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


def _validate_metadata(template, metadata):
    """
    Validate a dictionary of metadata relative to core intake-esm columns

    template: dict
        Template of core metadata keys, descriptions and functions to evaluate if metadata entries
        are valid
    metadata: dict
        Dictionary with keys corresponding to metadata columns and values corresponding to metadata
        entries
    """
    for col, (name, descr, func) in template.items():
        if not ((name in metadata) and (func(metadata[name]))):
            raise CoreMetadataError(
                f"The catalog must include core column '{name}' with entries that are {descr}"
            )
