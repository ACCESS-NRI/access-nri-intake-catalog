import packaging.version


def test_intake_esm_fork():
    """
    Make sure intake_esm version > 2025.7.9, ~= we are on our bleeding edge fork
    """
    import intake_esm

    assert packaging.version.Version(
        intake_esm.__version__
    ) > packaging.version.Version("2025.9.10")


def test_ecgtools_fork():
    """
    Make sure ecgtools-access version > 2024.6.0, ~= we are on our bleeding edge fork
    """
    import ecgtools

    assert packaging.version.Version(ecgtools.__version__) >= packaging.version.Version(
        "2025.9.19"
    )
