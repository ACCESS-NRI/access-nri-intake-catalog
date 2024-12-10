import pytest

import access_nri_intake.catalog.translators as translators

from . import e2e


@e2e
@pytest.mark.parametrize(
    "translator_name",
    [
        t
        for t in dir(translators)
        if t.endswith("Translator") and not t.startswith("Default")
    ],
)
def test_alignment(translator_name, live_config_dir):
    translator = getattr(translators, translator_name)
    # Now live test the translator
    print(translator, live_config_dir)
    assert 1
    # build(
    #     [
    #         str(config_dir / "cmip5.yaml"),
    #         str(config_dir / "access-om2.yaml"),
    #         "--build_base_path",
    #         str(BASE_DIR),
    #         "--catalog_base_path",
    #         "./",
    #         "--catalog_file",
    #         "metacatalog.csv",
    #         "--version",
    #         v_num,
    #         "--no_update",
    #     ]
    # )


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
