import argparse
import yaml
import glob
import jsonschema

schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "experiment_uuid": {
            "type": "string",
            "format": "uuid",
        },
        "short_description": {"type": "string"},
        "long_description": {"type": "string"},
        "model": {
            "oneOf": [
                {"type": ["string", "null"]},
                {
                    "type": "array",
                    "items": {"type": ["string", "null"]},
                },
            ]
        },
        "nominal_resolution": {
            "oneOf": [
                {"type": ["string", "null"]},
                {
                    "type": "array",
                    "items": {"type": ["string", "null"]},
                },
            ]
        },
        "version": {"type": ["number", "null"]},
        "contact": {"type": ["string", "null"]},
        "email": {"type": ["string", "null"]},
        "created": {"type": ["string", "null"]},
        "reference": {"type": ["string", "null"]},
        "url": {"type": ["string", "null"]},
        "parent_experiment": {"type": ["string", "null"]},
        "related_experiments": {
            "type": ["array", "null"],
            "items": {"type": ["string", "null"]},
        },
        "notes": {"type": ["string", "null"]},
        "keywords": {
            "type": ["array", "null"],
            "items": {"type": ["string", "null"]},
        },
    },
    "required": [
        "name",
        "experiment_uuid",
        "short_description",
        "long_description",
        "model",
        "nominal_resolution",
        "version",
        "contact",
        "email",
        "created",
        "reference",
        "url",
        "parent_experiment",
        "related_experiments",
        "notes",
        "keywords",
    ],
}


def main():

    parser = argparse.ArgumentParser(
        description="Validate the schema of a metadata.yaml file"
    )
    parser.add_argument(
        "file",
        type=str,
        help="The path to the metadata.yaml file (can include wildcards for multiple metadata.yaml)",
    )

    args = parser.parse_args()
    file = args.file

    class NoDatesSafeLoader(yaml.SafeLoader):
        @classmethod
        def remove_implicit_resolver(cls, tag_to_remove):
            """
            Remove implicit resolvers for a particular tag

            See https://stackoverflow.com/questions/34667108/ignore-dates-and-times-while-parsing-yaml
            """
            if "yaml_implicit_resolvers" not in cls.__dict__:
                cls.yaml_implicit_resolvers = cls.yaml_implicit_resolvers.copy()

            for first_letter, mappings in cls.yaml_implicit_resolvers.items():
                cls.yaml_implicit_resolvers[first_letter] = [
                    (tag, regexp) for tag, regexp in mappings if tag != tag_to_remove
                ]

    NoDatesSafeLoader.remove_implicit_resolver("tag:yaml.org,2002:timestamp")

    for f in glob.glob(file):
        with open(f) as fpath:
            instance = yaml.load(fpath, Loader=NoDatesSafeLoader)

        print(f"Validating {f}... ", end="")

        jsonschema.validate(instance, schema)

        print("success")


if __name__ == "__main__":
    main()
