import packaging.version


def test_intake_esm_fork():
    """
    Make sure intake_esm version > 2025.7.9, ~= we are on our bleeding edge fork
    """
    import intake_esm

    assert packaging.version.Version(
        intake_esm.__version__
    ) > packaging.version.Version("2025.9.10")
