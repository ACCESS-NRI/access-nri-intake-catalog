# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""Shared utilities for writing Intake-ESM builders and their parsers"""

import ast
import re
import warnings
from typing import Any

from IPython.core.getipython import get_ipython
from IPython.core.interactiveshell import ExecutionInfo, InteractiveShell
from IPython.core.magic import cell_magic

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
            # `check_load_calls` magic.
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


def capture_load_calls(info: ExecutionInfo) -> None:
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


@cell_magic
def check_load_calls(line, cell) -> None:
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
    code = cell

    if code is None:
        return None

    code = strip_magic(code)

    try:
        tree = ast.parse(code)
    except (SyntaxError, IndentationError):
        return None

    ip = get_ipython()  # type: ignore

    result = ip.run_cell(cell)
    _err = result.error_in_exec if result.error_in_exec else None

    user_namespace: dict[str, Any] = get_ipython().user_ns  # type: ignore

    visitor = CallListener(user_namespace, _err)
    visitor.visit(tree)

    return None


class CallListener(ast.NodeVisitor):
    def __init__(self, user_namespace: dict[str, Any], _err: Exception | None = None):
        self.user_namespace = user_namespace
        self._caught_calls: set[str] = set()  # Mostly for debugging
        self._err = _err

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

        if func_name.startswith("esm_datastore") and self._is_load_call(func_name):
            check_permissions(instance, self._err)

        self.generic_visit(node)

    def _is_load_call(self, func_name: str) -> bool:
        """
        Check if the function name is one of the load calls we are interested in.
        """
        return (
            func_name.endswith("to_dask")
            or func_name.endswith("to_dataset_dict")
            or func_name.endswith("to_datatree")
        )


def load_ipython_extension(ipython: InteractiveShell) -> None:
    """
    Load the IPython extension and register it to run before cells.
    """
    # ipython.events.register("pre_run_cell", capture_load_calls)  # type: ignore
    ipython.register_magic_function(check_load_calls, "cell", "check_load_calls")
    return None


# Register the extension
ip = get_ipython()  # type: ignore
if ip:
    load_ipython_extension(ip)
