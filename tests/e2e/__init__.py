import pytest

e2e = pytest.mark.skipif(
    "not config.getoption('--e2e')",
)
