# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import pytest

from access_nri_intake.catalog.manager import CatalogManager, CatalogManagerError
from access_nri_intake.catalog.translators import (
    Cmip5Translator,
    Cmip6Translator,
    EraiTranslator,
)
from access_nri_intake.source.builders import (
    AccessCm2Builder,
    AccessEsm15Builder,
    AccessOm2Builder,
)


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
    ],
)
def test_CatalogManager_build_esm(tmp_path, test_data, builder, basedir, kwargs):
    """Test building and adding an Intake-ESM datastore"""
    path = str(tmp_path / "cat.csv")
    cat = CatalogManager(path)

    args = dict(
        name="test",
        description="test",
        builder=builder,
        path=str(test_data / basedir),
        metadata=dict(
            model=[
                basedir,
            ]
        ),
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
        (EraiTranslator, "erai.json", {"model": ["ERA-Interim"]}),
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

    # Build source
    cat.build_esm(
        name="access-om2",
        description="access-om2",
        builder=AccessOm2Builder,
        path=str(test_data / "access-om2"),
        metadata=dict(
            model=[
                "ACCESS-OM2",
            ]
        ),
        directory=str(tmp_path),
    )
    # Still only one entry on disk
    assert len(cat.dfcat) == 2
    assert len(CatalogManager(path).dfcat) == 1

    # Check that entry with same name overwrites correctly
    cat.load(
        **load_args,
    )
    assert len(cat.dfcat) == 2
    cat.save()
    assert len(CatalogManager(path).dfcat) == 2
