import argparse

import yaml


def build_cc_db(paths):
    """
    Build a cosima-cookbook database for a list of paths
    """
    import cosima_cookbook as cc

    db = "./out/cookbook.db"
    session = cc.database.create_session(db)
    cc.database.build_index(paths, session, force=False)


def build_nri_cat(paths):
    """
    Build an access-nri-intake catalog for a list of paths
    """
    from access_nri_intake.catalog.manager import CatalogManager
    from access_nri_intake.source.builders import AccessOm2Builder

    catalog = CatalogManager("./out/catalog.csv")

    for path in paths:
        name = path.split("/")[-1]
        catalog.build_esm(
            name=name,
            description=name,
            builder=AccessOm2Builder,
            path=path,
            metadata={"model": ["ACCESS-OM2"]},
            directory="./out/catalog_source/",
            overwrite=True,
        ).add()


def main():
    parser = argparse.ArgumentParser(
        description="Build a test catalog/database for benchmarking."
    )
    parser.add_argument(
        "--database",
        choices=["nri", "cc"],
        required=True,
        help="Whether to build an access-nri-intake catalog ('nri') or a cosima-cookbook database ('cc').",
    )

    with open("./config/access-om2.yaml") as fobj:
        config = yaml.safe_load(fobj)

    paths = [source["path"][0] for source in config["sources"]]

    args = parser.parse_args()
    database = args.database

    if database == "nri":
        build_nri_cat(paths)
    elif database == "cc":
        build_cc_db(paths)


if __name__ == "__main__":
    main()
