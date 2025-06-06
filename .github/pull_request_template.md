<!-- Thanks for submitting a PR, your contribution is really appreciated! -->
<!-- Unless your change is trivial, please create an issue to discuss the change before creating a PR -->
<!-- Below are a few things we ask you kindly to self-check before getting a review. Remove checks that are not relevant.
-->

## Change Summary

<!-- Please give a short summary of the changes. -->

## Related issue number

<!-- Are there any issues opened that will be resolved by merging this change? -->
<!--
Please note any issues this fixes using [closing keywords]( https://help.github.com/articles/closing-issues-using-keywords/ ):
-->

## Checklist

- [ ] Unit tests for the changes exist
- [ ] Tests pass on CI
- [ ] Documentation reflects the changes where applicable

<!-- Delete this section unless you have added a new translator/datastore: -->
- [ ] The new translator has been added to `__all__` in `src/access_nri_intake/catalog/translators.py`
-  [ ] The new datastore has been added to the list of configs in `bin/build_all.sh` and `bin/test_end_to_end.sh`, and the storage flags in both scripts updated, if necessary.
- [ ] You have generated a valid `metadata.yaml` for the datastore & placed it in the correct location: eg. `config/metadata_sources/esgf-ref-qv56/metadata.yaml` for the `esgf-ref-qv56` datastore.
    - `<<REPLACE THIS TEXT WITH YOUR METADATA FILE IN THIS PACKAGE>>`
- [ ] The `metadata.yaml` has been copied to the correct location in `/g/data/xp65/admin/intake` -  for example, `/g/data/xp65/admin/intake/metadata/esgf-ref-qv56/metadata.yaml` for the `esgf-ref-qv56` datastore
    - `<<REPLACE THIS TEXT WITH THE PATH TO METADATA FILE IN ITS FINAL LOCATION ON DISK>>` 

## Notes

- In order to build a new catalog including this additional datastore, you will need to either release a new version of the `access-nri-intake` package, or run the `bin/build_all.sh` script to build a new catalog, using a virtual environment with the most recent (ie. including this PR) version of `access-nri-intake` installed. This can be done by activating the `venv` in `/g/data/xp65/admin/access-nri-intake-catalog/bin/build_all.sh` job.
<!-- End of Translator Change section  -->

<!--
Please add any other relevant info below:
-->