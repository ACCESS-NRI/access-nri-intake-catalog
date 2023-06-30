# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import pytest

from access_nri_intake.source import builders


@pytest.mark.parametrize(
    "basedir, builder, kwargs",
    [
        ("access-om2", "AccessOm2Builder", {}),
        ("access-cm2", "AccessCm2Builder", {"ensemble": False}),
        ("access-cm2", "AccessCm2Builder", {"ensemble": True}),
        ("access-esm1-5", "AccessEsm15Builder", {"ensemble": False}),
        ("access-esm1-5", "AccessEsm15Builder", {"ensemble": True}),
    ],
)
def test_builder(test_data, basedir, builder, kwargs):
    Builder = getattr(builders, builder)
    path = str(test_data / basedir)
    builder = Builder(path, **kwargs)
    print(builder.get_assets())
