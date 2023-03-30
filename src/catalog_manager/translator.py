import re

from . import _ALLOWABLE_FREQS


def _get_cmip6_freq(table_id):
    """
    Parse frequency from CMIP6 table_id
    """
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


def _get_cmip6_realm(table_id):
    """
    Parse realm from CMIP6 table_id
    """
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


cmip6 = {
    "model": "CMIP6",
    "experiment": "CMIP6",
    "realm": lambda x: _get_cmip6_realm(x["table_id"]),
    "variable": lambda x: [x["variable_id"]],
    "frequency": lambda x: _get_cmip6_freq(x["table_id"]),
}
