# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0


from unittest import mock

import pytest
from intake_dataframe_catalog.core import DfFileCatalogError

from access_nri_intake.catalog import EXP_JSONSCHEMA
from access_nri_intake.catalog.manager import CatalogManager, CatalogManagerError
from access_nri_intake.catalog.translators import (
    Cmip5Translator,
    Cmip6Translator,
)
from access_nri_intake.source.builders import (
    AccessCm2Builder,
    AccessEsm15Builder,
    AccessOm2Builder,
    AccessOm3Builder,
)
from access_nri_intake.utils import load_metadata_yaml


def test_CatalogManager_init(tmp_path):
    """Test that CatalogManager initialising correctly"""
    path = str(tmp_path / "cat.csv")

    cat = CatalogManager(path)
    assert cat.mode == "w"
    assert hasattr(cat, "dfcat")

    with pytest.raises(CatalogManagerError) as excinfo:
        cat._add()
    assert "first load or build the source" in str(excinfo.value)


@pytest.mark.parametrize(
    "builder, basedir, kwargs",
    [
        (AccessOm2Builder, "access-om2", {}),
        (AccessCm2Builder, "access-cm2/by578", {"ensemble": False}),
        (AccessEsm15Builder, "access-esm1-5", {"ensemble": False}),
        (AccessOm3Builder, "access-om3", {}),
    ],
)
def test_CatalogManager_build_esm(tmp_path, test_data, builder, basedir, kwargs):
    """Test building and adding an Intake-ESM datastore"""
    path = str(tmp_path / "cat.csv")
    cat = CatalogManager(path)

    metadata = load_metadata_yaml(
        str(test_data / basedir / "metadata.yaml"), EXP_JSONSCHEMA
    )
    args = dict(
        name="test",
        description="test",
        builder=builder,
        path=str(test_data / basedir),
        metadata=metadata,
        directory=str(tmp_path),
        **kwargs,
    )
    cat.build_esm(**args)

    # Try to rebuild without setting overwrite
    with pytest.raises(CatalogManagerError) as excinfo:
        cat.build_esm(**args)
    assert "An Intake-ESM datastore already exists" in str(excinfo.value)

    # Overwrite
    cat.build_esm(**args, overwrite=True)

    cat.save()
    cat = CatalogManager(path)
    assert cat.mode == "a"


@pytest.mark.parametrize(
    "translator, datastore, metadata",
    [
        (Cmip5Translator, "cmip5-al33.json", {}),
        (Cmip6Translator, "cmip6-oi10.json", {}),
    ],
)
def test_CatalogManager_load(tmp_path, test_data, translator, datastore, metadata):
    """Test loading and adding an Intake-ESM datastore"""
    path = str(tmp_path / "cat.csv")
    cat = CatalogManager(path)

    args = dict(
        name="test",
        description="test",
        path=str(test_data / f"esm_datastore/{datastore}"),
        translator=translator,
        metadata=metadata,
    )
    cat.load(**args)
    cat.save()

    cat = CatalogManager(path)
    assert cat.mode == "a"


def test_CatalogManager_load_error(tmp_path, test_data):
    """Test loading and adding an Intake-ESM datastore"""
    path = str(tmp_path / "cat.csv")
    cat = CatalogManager(path)

    # Test can load when path is len 1 list
    path = str(test_data / "esm_datastore/cmip5-al33.json")
    args = dict(
        name="test",
        description="test",
        translator=Cmip5Translator,
    )
    cat.load(**args, path=[path])

    # Test fails when len > 1
    with pytest.raises(CatalogManagerError) as excinfo:
        cat.load(**args, path=[path, path])
    assert "Only a single data source" in str(excinfo.value)


def test_CatalogManager_all(tmp_path, test_data):
    """Test adding multiple sources"""
    path = str(tmp_path / "cat.csv")
    cat = CatalogManager(path)

    # Load source
    load_args = dict(
        name="cmip5-al33",
        description="cmip5-al33",
        path=str(test_data / "esm_datastore/cmip5-al33.json"),
        translator=Cmip5Translator,
    )
    cat.load(
        **load_args,
    )
    assert len(cat.dfcat) == 1
    cat.save()
    assert len(CatalogManager(path).dfcat) == 1

    # Build sources
    models = {"access-om2": AccessOm2Builder, "access-om3": AccessOm3Builder}
    for model, builder in models.items():
        metadata = load_metadata_yaml(
            str(test_data / model / "metadata.yaml"), EXP_JSONSCHEMA
        )
        cat.build_esm(
            name=model,
            description=model,
            builder=builder,
            path=str(test_data / model),
            metadata=metadata,
            directory=str(tmp_path),
        )
    # Still only one entry on disk
    assert len(cat.dfcat) == len(models) + 1
    assert len(CatalogManager(path).dfcat) == 1

    # Check that entry with same name overwrites correctly
    cat.load(
        **load_args,
    )
    assert len(cat.dfcat) == len(models) + 1
    cat.save()
    assert len(CatalogManager(path).dfcat) == len(models) + 1


@pytest.mark.parametrize(
    "intake_dataframe_err_str, access_nri_err_str, cause_str",
    [
        (
            "Expected iterable metadata columns: ['model']. Unable to add entry with iterable metadata columns '[]' to dataframe catalog: columns ['model'] must be iterable to ensure metadata entries are consistent.",
            "Error adding source 'cmip5-al33' to the catalog",
            "Expected iterable metadata columns: ['model']",
        ),
        (
            "Generic Exception for the CatalogManager class",
            "Generic Exception for the CatalogManager class",
            "None",
        ),
    ],
)
def test_CatalogManager_load_invalid_model(
    tmp_path, test_data, intake_dataframe_err_str, access_nri_err_str, cause_str
):
    """Test loading and adding an Intake-ESM datastore"""
    path = str(tmp_path / "cat.csv")
    cat = CatalogManager(path)

    # Test can load when path is len 1 list
    path = test_data / "esm_datastore/cmip5-al33.json"
    # Load source
    load_args = dict(
        name="cmip5-al33",
        description="cmip5-al33",
        path=str(test_data / "esm_datastore/cmip5-al33.json"),
        translator=Cmip5Translator,
    )

    with mock.patch.object(
        cat.dfcat,
        "add",
        side_effect=DfFileCatalogError(intake_dataframe_err_str),
    ):
        with pytest.raises(CatalogManagerError) as excinfo:
            cat.load(**load_args)

    assert access_nri_err_str in str(excinfo.value)
    assert cause_str in str(excinfo.value.__cause__)
