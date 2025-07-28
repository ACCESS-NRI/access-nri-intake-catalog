# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""General utility functions for access-nri-intake"""

import ast
import json
import re
from importlib import resources as rsr
from pathlib import Path
from typing import Any
from warnings import warn

import jsonschema
import yaml
from IPython.core.getipython import get_ipython
from IPython.core.interactiveshell import ExecutionInfo

from . import CATALOG_LOCATION, USER_CATALOG_LOCATION


def get_jsonschema(metadata_file: str, required: list) -> tuple[dict, dict]:
    """
    Read in the required JSON schema, and annotate it with "required" fields.

    Parameters
    ----------
    required: list
        A list of the properties to include in the "required" key
    """

    schema_file = rsr.files("access_nri_intake").joinpath(metadata_file)
    with schema_file.open(mode="r") as fpath:  # type: ignore
        schema = json.load(fpath)

    schema_required = schema.copy()
    req = []
    for col in required:
        if col not in schema_required["properties"]:
            warn(
                f"Required column {col} does not exist in schema. Entries in this column will not be validated"
            )
        else:
            req.append(col)

    schema_required["required"] = req

    return schema, schema_required


def load_metadata_yaml(path: str | Path, jsonschema: dict) -> dict:
    """
    Load a metadata.yaml file, leaving dates as strings, and validate against a jsonschema,
    allowing for tuples as arrays

    Parameters
    ----------
    paths: str
        The path to the metadata.yaml
    """

    class NoDatesSafeLoader(yaml.SafeLoader):
        @classmethod
        def remove_implicit_resolver(cls, tag_to_remove):
            """
            Remove implicit resolvers for a particular tag

            See https://stackoverflow.com/questions/34667108/ignore-dates-and-times-while-parsing-yaml
            """
            if "yaml_implicit_resolvers" not in cls.__dict__:
                cls.yaml_implicit_resolvers = cls.yaml_implicit_resolvers.copy()

            for first_letter, mappings in cls.yaml_implicit_resolvers.items():
                cls.yaml_implicit_resolvers[first_letter] = [
                    (tag, regexp) for tag, regexp in mappings if tag != tag_to_remove
                ]

    NoDatesSafeLoader.remove_implicit_resolver("tag:yaml.org,2002:timestamp")

    with open(path) as fpath:
        metadata = yaml.load(fpath, Loader=NoDatesSafeLoader)

    validate_against_schema(metadata, jsonschema)

    return metadata


def validate_against_schema(instance: dict, schema: dict) -> None:
    """
    Validate a dictionary against a jsonschema, allowing for tuples as arrays

    Parameters
    ----------
    instance: dict
        The instance to validate
    schema: dict
        The jsonschema

    Raises
    ------
    jsonschema.exceptions.ValidationError
        If the instance does not match the schema
    """

    Validator = jsonschema.validators.validator_for(schema)
    type_checker = Validator.TYPE_CHECKER.redefine(
        "array", lambda checker, instance: isinstance(instance, (list, tuple))
    )
    TupleAllowingValidator = jsonschema.validators.extend(
        Validator, type_checker=type_checker
    )

    issues = list(TupleAllowingValidator(schema).iter_errors(instance))

    if len(issues) > 0:
        issue_str = ""
        for i, issue in enumerate(issues, start=1):
            try:
                issue_str += f"\n{i:02d} | {issue.absolute_path[0]} : {issue.message}"
            except IndexError:  # Must be a missing keyword, not a bad type/value
                issue_str += f"\n{i:02d} | (missing) : {issue.message}"
        raise jsonschema.ValidationError(issue_str)

    return


def _can_be_array(field):
    """
    Does the schema allow the provided field to be an array?
    """

    def _is_array(field):
        try:
            return field["type"] == "array"
        except KeyError:
            return False

    is_array = _is_array(field)
    if (not is_array) and ("oneOf" in field):
        for nfield in field["oneOf"]:
            is_array = is_array or _is_array(nfield)
    return is_array


def get_catalog_fp(basepath=None):
    if basepath is not None:
        if not isinstance(basepath, Path):
            basepath = Path(basepath)
        return basepath / "catalog.yaml"
    if Path(USER_CATALOG_LOCATION).is_file():
        warn(
            (
                "User defined catalog found in `$HOME/.access_nri_intake_catalog`. "
                "Remove this file to use default catalog."
            ),
            RuntimeWarning,
            stacklevel=2,
        )
        return USER_CATALOG_LOCATION
    return CATALOG_LOCATION


def check_permissions() -> None:
    """
    Use an IPython cell magic to listen for calls to `.to_dask`, `.to_dataset_dict()` or `to_datatree`, and
    inspect the list of paths attached to the associated esm_datastore. If we find any paths that we don't
    have the relevant permissions for, then emit a warning
    """
    pass


def strip_magic(code: str) -> str:
    """
    Parse the provided code into an AST (Abstract Syntax Tree).

    Parameters
    ----------

    code : str
        The code to parse.
    Returns
    -------
    str
        The code without IPython magic commands.

    """

    IPYTHON_MAGIC_PATTERN = r"^\s*[%!?]{1,2}|^.*\?{1,2}$"

    code = "\n".join(
        line for line in code.splitlines() if not re.match(IPYTHON_MAGIC_PATTERN, line)
    )

    return code


def capture_registered_calls(info: ExecutionInfo) -> None:
    """
    Use the AST module to parse the code that we are executing & check for attempts
    to access directories that we haven't set storage flags for.

    Fail silently if we can't parse the code.

    Parameters
    ----------
    info : IPython.core.interactiveshell.ExecutionInfo
        An object containing information about the code being executed.

    Returns
    -------
    None
    """
    code = info.raw_cell

    if code is None:
        return None

    code = strip_magic(code)

    try:
        tree = ast.parse(code)
    except (SyntaxError, IndentationError):
        return None

    user_namespace: dict[str, Any] = get_ipython().user_ns  # type: ignore

    try:
        visitor = CallListener(user_namespace)
        visitor.visit(tree)
    except Exception:
        return None

    return None


class CallListener(ast.NodeVisitor):
    def __init__(
        self,
        user_namespace: dict[str, Any],
    ):
        self.user_namespace = user_namespace
        self._caught_calls: set[str] = set()  # Mostly for debugging

    def _get_full_name(self, node: ast.AST) -> str | None:
        """Recursively get the full name of a function or method call."""
        if isinstance(node, ast.Attribute):
            return f"{self._get_full_name(node.value)}.{node.attr}"
        elif isinstance(node, ast.Name):
            return node.id
        return None

    def safe_eval(self, node: ast.AST) -> Any:
        """Try to evaluate a node, or return the unparsed node if that fails.

        Might be unnecessary.
        """
        try:
            return ast.literal_eval(node)
        except (ValueError, SyntaxError):
            return ast.unparse(node)

    def visit_Call(self, node: ast.Call) -> None:
        """
        Listen for calls that match anything of the form `esm_datastore.to_dask()`, `esm_datastore.to_dataset_dict()`, or
        `esm_datastore.to_datatree()`. Annoyingly, we also need to be able to identify stuff that looks like this too:
        `esm_datastore.search(variable='xyz').to_dask()`
        """

        full_name = self._get_full_name(node.func)
        func_name = None
        if full_name:
            parts = full_name.split(".")
            if len(parts) == 1:
                # Regular function call, not interested, return
                return None
            else:
                # Check if the first part is in the user namespace
                instance = self.user_namespace.get(parts[0])
                if instance is None:
                    self.generic_visit(node)
                    return None

                class_name = type(instance).__name__
                if class_name != "module":
                    func_name = f"{class_name}.{'.'.join(parts[1:])}"
                else:
                    func_name = f"{instance.__name__}.{'.'.join(parts[1:])}"

        if func_name is None:
            return None

        if func_name.startswith("esm_datastore"):
            check_permissions()

        self.generic_visit(node)
