import pytest

import access_nri_intake.catalog.translators as translators

from . import e2e


@pytest.fixture
def translator_names():
    return [
        t
        for t in dir(translators)
        if t.endswith("Translator") and not t.startswith("Default")
    ]


@e2e
def test_alignment():
    raise AssertionError("This test should not run in CI")


def test_import_all():
    print([getattr(_) for _ in translators if hasattr(_, "translate")])


# def pytest_generate_tests(metafunc):
#     if "translator" in metafunc.fixturenames:
#         metafunc.parametrize(
#             "yamlfile", glob.glob(str(Path(metadata_sources() / "*" / "metadata.yaml")))
#         )


# def test_metadata_sources_valid(yamlfile, capsys):
#     try:
#         metadata_validate(
#             [
#                 yamlfile,
#             ]
#         )
#     except Exception:
#         assert False, f"Validation of {yamlfile} failed with uncaught exception"
#     output = capsys.readouterr()
#     assert "FAILED" not in output.out, f"Validation failed for {yamlfile}"
