.. _cmorise:

CMORising ACCESS-ESM1.6 output
===============================

The optional MOPPy integration turns a searched set of raw ACCESS-ESM1.6 files
into a CMIP-style xarray Dataset. Searching and CMORisation are separate steps;
ordinary catalog searches keep their existing Intake-ESM return type.

Install ACCESS-MOPPy 1.8.4 or newer in a compatible Python environment
(Python 3.11–3.13)::

   python -m pip install 'access_nri_intake[moppy]'

For example, the published historical datastore contains raw ACCESS-ESM1.6
files. Select one monthly ``rsds`` file, then CMORise it::

   import intake

   source = intake.cat.access_nri["historical-r1i1p1f1-2026.06.12"]
   monthly = source.search(variable="rsds", frequency="1mon")
   selected = source.search(
       variable="rsds", frequency="1mon", path=monthly.df.path.iloc[0]
   )
   selected.df[["path", "variable", "start_date", "end_date"]]

   parent_info = {
       "parent_experiment_id": "piControl",
       "parent_activity_id": "CMIP",
       "parent_mip_era": "CMIP7",
       "parent_source_id": "ACCESS-ESM1-6",
       "parent_variant_label": "r1i1p1f1",
       "parent_time_units": "days since 0001-01-01",
       "branch_time_in_child": 0.0,
       "branch_time_in_parent": 73048.0,
       "branch_method": "standard",
   }

   ds = source.cmorise(
       selected,
       compound_name="atmos.rsds.tavg-u-hxy-u.mon.GLB",
       cmip_version="CMIP7",
       experiment_id="historical",
       variant_label="r1i1p1f1",
       activity_id="CMIP",
       grid_label="gn",
       parent_info=parent_info,
   )

These parent values are specific to this run. Its metadata links to
``piControl-2026.04.07`` and gives a parent branch date of ``0201-01-01``;
``73048`` is that date in the parent run's proleptic Gregorian time units.
MOPPy requires all nine ``parent_info`` fields for this historical CMIP7
experiment, even when returning an in-memory Dataset.

``ds`` is MOPPy's lazy CMORised xarray Dataset. The method does not write
NetCDF files. It passes the exact distinct paths in ``selected.df`` to MOPPy,
so a search that limits the time range also limits the inputs. For derived
variables, select every native file needed by the MOPPy mapping; a search for
just one raw field can omit required inputs. MOPPy's ``input_folder`` discovery
is not used because it could expand beyond the catalog selection.

Each call CMORises one target variable. Choose ``compound_name``,
``experiment_id`` and ``activity_id`` values supported by the selected MOPPy
controlled vocabulary and matching the source experiment.

The source must identify only ACCESS-ESM1.6 in its ``model`` metadata and must
contain raw local NetCDF assets. ``variant_label`` is checked against the
selected ``member`` column when that column has values. Other models and
already CMIP-processed datastores are rejected. MOPPy receives the fixed
``source_id="ACCESS-ESM1-6"`` and mapping ``model_id="ACCESS-ESM1-6"``.

For publication, verify the experiment's ``parent_info`` values and validate
written output using the MOPPy workflow. An in-memory Dataset alone has not
been checked for publication compliance.
