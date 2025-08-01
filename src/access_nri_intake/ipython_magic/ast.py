# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Shared utilities for writing Intake-ESM builders and their parsers"""

import re
import warnings
from typing import Any

import libcst as cst
import libcst.matchers as m
from IPython.core.getipython import get_ipython
from IPython.core.interactiveshell import InteractiveShell
from IPython.core.magic import cell_magic
from libcst._exceptions import ParserSyntaxError

from access_nri_intake.cli import _confirm_project_access, _get_project_code


class MissingStorageWarning(UserWarning):
    """Warning raised when a storage flag is missing for a project code."""

    pass


class MissingStorageError(Exception):
    """Error raised when a storage flag is missing for a project code."""

    pass


def check_permissions(esm_datastore, err: Exception | None = None) -> None:
    """
    Use an IPython cell magic to listen for calls to `.to_dask`, `.to_dataset_dict()` or `to_datatree`, and
    inspect the list of paths attached to the associated esm_datastore. If we find any paths that we don't
    have the relevant permissions for, then emit a warning
    """
    project_codes = set(esm_datastore.df["path"].map(_get_project_code))

    access_valid, error_msg = _confirm_project_access(project_codes)

    error_msg = f"{error_msg}\n\tThis is likely the source of any ESMDataSourceErrors or missing data"

    if not access_valid:
        if err is None:
            warnings.warn("\n".join(error_msg), category=MissingStorageWarning)
        else:
            # We don't raise from err here, as it's already propagated out to our
            # stack trace in the `result = ip.run_cell(cell); _err = result.error_in_exec if result.error_in_exec else None`
            # `check_load_calls` magic. So we just raise a new error
            raise MissingStorageError(error_msg)

    return None


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


@cell_magic
def check_storage_enabled(line, cell) -> None:
    """
    Use the AST module to parse the code that we are executing & check for attempts
    to access directories that we haven't set storage flags for.

    Fail silently if we can't parse the code.

    Parameters
    ----------
    line:   str
        The line of the cell magic (not used here, but required by IPython).

    cell : str
        The code to parse.

    Returns
    -------
    None
    """
    code = cell

    code = strip_magic(code)

    try:
        tree = cst.parse_module(code)
    except (SyntaxError, ParserSyntaxError, IndentationError):
        return None

    ip = get_ipython()  # type: ignore

    result = ip.run_cell(cell)
    _err = result.error_in_exec if result.error_in_exec else None

    user_namespace: dict[str, Any] = get_ipython().user_ns  # type: ignore

    reducer = ChainSimplifier(user_namespace)
    reduced_tree = tree.visit(reducer)
    visitor = CallListener(user_namespace, _err)
    reduced_tree.visit(visitor)

    return None


class CallListener(cst.CSTVisitor):
    def __init__(self, user_namespace: dict[str, Any], _err: Exception | None = None):
        self.user_namespace = user_namespace
        self._caught_calls: set[str] = set()  # Mostly for debugging
        self._err = _err

    def visit_Call(self, node: cst.Call) -> None:
        """
        Listen for calls that match anything of the form `esm_datastore.to_dask()`, `esm_datastore.to_dataset_dict()`, or
        `esm_datastore.to_datatree()`.
        """
        if isinstance(node.func, cst.Attribute) and isinstance(
            node.func.value, cst.Name
        ):
            obj_name = node.func.value.value
            method_name = node.func.attr.value

            # Check if object exists in user namespace
            instance = self.user_namespace.get(obj_name)
            if instance is None:
                return

            # Check if it's an esm_datastore with a load method
            class_name = type(instance).__name__
            if class_name == "esm_datastore" and self._is_load_call(method_name):
                check_permissions(instance, self._err)

    def _is_load_call(self, func_name: str) -> bool:
        """
        Check if the function name is one of the load calls we are interested in.
        """
        return (
            func_name.endswith("to_dask")
            or func_name.endswith("to_dataset_dict")
            or func_name.endswith("to_datatree")
        )


class ChainSimplifier(cst.CSTTransformer):
    """
    Transform chained calls by removing intermediate method calls
    Example: ds.search(...).search(...).to_dataset_dict()
    becomes: ds.to_dataset_dict()
    """

    def __init__(self, user_namespace: dict[str, Any] | None = None):
        self.user_namespace = user_namespace or {}

    def leave_Call(self, original_node: cst.Call, updated_node: cst.Call) -> cst.Call:
        # Use matcher to identify the pattern: any_method(search_call(...))
        search_pattern = m.Call(
            func=m.Attribute(value=m.Call(func=m.Attribute(attr=m.Name("search"))))
        )

        if m.matches(updated_node, search_pattern):
            # Extract the method name and inner call
            inner_call = updated_node.func.value  # type: ignore[attr-defined]

            # Replace the value with the inner call's value to remove the `.search()` call
            new_func = updated_node.func.with_changes(value=inner_call.func.value)
            return updated_node.with_changes(func=new_func)

        return updated_node

    def leave_Subscript(
        self, original_node: cst.Subscript, updated_node: cst.Subscript
    ) -> cst.Name | cst.Subscript:
        """
        Handle subscript access to catalog items. This transforms a node, taking
        something like `cat["expt"]` and replacing it with it's return value. It
        also inserts the return value into the user namespace with the name
        `obj_<id(instance)>`, so that we can refer to it later in the code.
        """

        instance = self.user_namespace.get(original_node.value.value, None)  # type: ignore[attr-defined]
        if type(instance).__name__ == "esm_datastore":
            # Just replace it with the esm_datastore node
            _name = f"obj_{id(instance)}"
            self.user_namespace[_name] = instance
            return cst.Name(value=_name)

        return updated_node


def load_ipython_extension(ipython: InteractiveShell) -> None:
    """
    Load the IPython extension and register it to run before cells.
    """
    # ipython.events.register("pre_run_cell", capture_load_calls)  # type: ignore
    ipython.register_magic_function(
        check_storage_enabled, "cell", "check_storage_enabled"
    )
    return None


# Register the extension
ip = get_ipython()  # type: ignore
if ip:
    load_ipython_extension(ip)  # pragma: no cover
