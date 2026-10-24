# Copyright 2026 ACCESS-NRI and contributors.
# SPDX-License-Identifier: Apache-2.0

"""Optional MOPPy bridge for selected raw ACCESS-ESM1.6 catalog assets."""

from pathlib import Path
from typing import Any

from intake_esm.core import esm_datastore

_SUPPORTED_MODELS = {"ACCESS-ESM1.6", "ACCESS-ESM1-6"}
_SOURCE_ID = "ACCESS-ESM1-6"
_MODEL_ID = "ACCESS-ESM1.6"
_CMIP_VERSIONS = {"CMIP6", "CMIP6Plus", "CMIP7"}


def _check_gadi_projects(paths: list[str]) -> None:
    """Fail early when selected files live on an unmounted Gadi project."""
    projects = {
        parts[3]
        for path in paths
        if (parts := Path(path).parts)[:3] == ("/", "g", "data")
        and len(parts) > 4
    }
    missing = sorted(
        project for project in projects if not (Path("/g/data") / project).is_dir()
    )
    if missing:
        raise FileNotFoundError(
            f"Gadi projects {', '.join(missing)} are not accessible; "
            "check storage flags and memberships"
        )


def _validate_selection(
    source: esm_datastore, selected: esm_datastore, variant_label: str
) -> list[str]:
    metadata = source.metadata if isinstance(source.metadata, dict) else {}
    model = metadata.get("model")
    if isinstance(model, str):
        models = {model}
    elif isinstance(model, (list, tuple, set)):
        models = set(model)
    else:
        models = set()
    if not models or not models <= _SUPPORTED_MODELS:
        raise ValueError(
            "CMORisation currently supports only a source whose model "
            "metadata is ACCESS-ESM1.6"
        )

    if not isinstance(selected, esm_datastore):
        raise TypeError(
            "selected must be an Intake-ESM datastore returned by source.search()"
        )

    source_df = source.df
    selected_df = selected.df
    if not {"variable", "path", "realm"} <= set(source_df.columns):
        raise ValueError(
            "CMORisation requires a raw ACCESS-ESM1.6 datastore with "
            "path, realm and variable columns"
        )
    if "variable_id" in source_df or "table_id" in source_df:
        raise ValueError(
            "CMORisation requires raw model output, not an existing CMIP datastore"
        )
    if selected_df.empty:
        raise ValueError("The selected datastore contains no files to CMORise")
    if "path" not in selected_df:
        raise ValueError("The selected datastore has no path column")

    paths = selected_df["path"].tolist()
    if any(not isinstance(path, str) or not Path(path).is_absolute() for path in paths):
        raise ValueError("CMORisation requires absolute local file paths")
    if not set(paths) <= set(source_df["path"]):
        raise ValueError("The selected files do not belong to this catalog source")
    if selected.esmcat.id != source.esmcat.id:
        raise ValueError(
            "The selected datastore does not belong to this catalog source"
        )

    if "member" in selected_df:
        members = set(selected_df["member"].dropna().astype(str))
        if members and members != {variant_label}:
            raise ValueError(
                f"Selected member values {sorted(members)} do not match "
                f"variant_label={variant_label!r}"
            )

    asset_format = getattr(
        source.esmcat.assets.format, "value", source.esmcat.assets.format
    )
    if asset_format != "netcdf":
        raise ValueError("CMORisation currently supports only local NetCDF assets")

    unique_paths = sorted(set(paths))
    _check_gadi_projects(unique_paths)
    return unique_paths


def cmorise_selection(
    source: esm_datastore,
    selected: esm_datastore,
    *,
    compound_name: str,
    cmip_version: str,
    experiment_id: str,
    variant_label: str,
    activity_id: str,
    grid_label: str | None = None,
    parent_info: dict[str, Any] | None = None,
):
    """Return a lazy CMORised xarray Dataset for the selected source files."""
    if cmip_version not in _CMIP_VERSIONS:
        raise ValueError(f"cmip_version must be one of {sorted(_CMIP_VERSIONS)}")
    for name, value in (
        ("compound_name", compound_name),
        ("experiment_id", experiment_id),
        ("variant_label", variant_label),
        ("activity_id", activity_id),
    ):
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{name} must be a non-empty string")

    paths = _validate_selection(source, selected, variant_label)

    try:
        from access_moppy import ACCESS_ESM_CMORiser
    except ModuleNotFoundError as exc:
        if exc.name != "access_moppy":
            raise
        raise ImportError(
            "CMORisation requires ACCESS-MOPPy; install access_nri_intake[moppy]"
        ) from exc

    cmoriser = ACCESS_ESM_CMORiser(
        input_data=paths,
        compound_name=compound_name,
        cmip_version=cmip_version,
        experiment_id=experiment_id,
        source_id=_SOURCE_ID,
        variant_label=variant_label,
        activity_id=activity_id,
        grid_label=grid_label,
        parent_info=parent_info,
        model_id=_MODEL_ID,
    )
    cmoriser.run()
    return cmoriser.to_dataset()
