.. _cmorise:

CMORising ACCESS-ESM1.6 output
===============================

The optional MOPPy integration turns a searched set of raw ACCESS-ESM1.6 files
into a CMIP-style xarray Dataset. Searching and CMORisation are separate steps;
ordinary catalog searches keep their existing Intake-ESM return type.

Install ACCESS-MOPPy 1.8.4 or newer in a compatible Python environment
(Python 3.11–3.13)::

   python -m pip install 'access_nri_intake[moppy]'

Select files from one raw ACCESS-ESM1.6 source, inspect them, and then CMORise::

   import intake

   source = intake.cat.access_nri["<ACCESS-ESM1.6 piControl-spinup source>"]
   selected = source.search(variable="rsds", frequency="1mon")
   selected.df[["path", "variable", "start_date", "end_date"]]

   ds = source.cmorise(
       selected,
       compound_name="atmos.rsds.tavg-u-hxy-u.mon.GLB",
       cmip_version="CMIP7",
       experiment_id="piControl-spinup",
       variant_label="r1i1p1f1",
       activity_id="CMIP",
       grid_label="gn",
   )

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

For publication, supply the experiment's ``parent_info`` block and validate
written output using the MOPPy workflow. An in-memory Dataset alone has not
been checked for publication compliance.
