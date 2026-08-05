.. _aliasing:

Aliasing
========

The ACCESS-NRI catalog supports **aliasing** — the ability to search using alternative,
user-friendly names that are automatically mapped to the underlying canonical values stored
in the catalog. This is particularly useful for researchers familiar with
`CMIP <https://www.wcrp-climate.org/wgcm-cmip>`_ vocabularies who want to discover raw
ACCESS model data without needing to learn ACCESS-specific variable codes or column names.

For example, a user familiar with CMIP conventions can search for ``variable="tas"`` and the
catalog will find files stored under the raw ACCESS variable name ``fld_s03i236`` *as well as*
any files already labelled ``tas``. Similarly, typing ``frequency="daily"`` will match
entries catalogued as ``1day``.

.. seealso::

   :ref:`what` gives a high-level overview of the catalog structure.
   :ref:`how` explains prerequisites and how to start a session on Gadi.
   The :doc:`aliases_demo` notebook demonstrates all of the features described on this page.

----

.. _aliasing_architecture:

Two-level architecture
^^^^^^^^^^^^^^^^^^^^^^

Aliasing is applied at two levels, corresponding to the two layers of the catalog:

.. code-block:: text

   intake.cat.access_nri          ← AliasedDataframeCatalog
   │  search(source_id=...)           field aliasing  +  value aliasing
   │  search(model=...)
   │
   └─ cat["access-esm1-6"]        ← AliasedESMCatalog (per dataset)
         search(variable=...)         value aliasing only
                                      (ESM datastores use native field names)

The **top-level catalog** (``intake.cat.access_nri``) supports both
*field aliasing* (accepting alternative column names) and *value aliasing*
(accepting alternative search terms). The **per-dataset ESM datastores** you get
back from it support value aliasing only, since each datastore already has its own
fixed set of field names.

----

.. _field_aliasing:

Field aliasing
^^^^^^^^^^^^^^

Field aliasing applies at the top-level catalog. It lets you use CMIP-style column
names when calling :meth:`~intake_dataframe_catalog.core.DfFileCatalog.search`
on ``intake.cat.access_nri``.

**Example**

.. code-block:: python

   import intake

   cat = intake.cat.access_nri

   # Using the CMIP-style field name "source_id" instead of ACCESS-NRI's "model"
   results = cat.search(source_id="ACCESS-ESM1-5")

   # Using "variable_id" instead of "variable"
   results = cat.search(variable_id="tas")

When a field alias fires, the library emits a :class:`UserWarning` so you can see exactly
what mapping was applied. To suppress these warnings, see :ref:`alias_warnings` below.

.. dropdown:: Full field alias reference

   The following aliases are accepted at the **top-level catalog** only. These do **not**
   apply when searching inside an individual ESM datastore.

   =====================  ====================  =================================
   Alias you can use      Canonical field name  Notes
   =====================  ====================  =================================
   ``source_id``          ``model``             CMIP controlled vocabulary term
   ``variable_id``        ``variable``          CMIP controlled vocabulary term
   ``table_id``           ``realm``             CMIP controlled vocabulary term
   ``member_id``          ``ensemble``          CMIP controlled vocabulary term
   ``experiment_id``      ``experiment``        CMIP controlled vocabulary term
   ``source``             ``model``             Short alternative
   ``var``                ``variable``          Short alternative
   =====================  ====================  =================================

----

.. _value_aliasing:

Value aliasing
^^^^^^^^^^^^^^

Value aliasing applies at **both** the top-level catalog and inside individual ESM
datastores. When a value alias fires, the library expands your search to include
*both* your original term and the canonical value, so you never accidentally exclude
data that is already stored under the canonical name.

For example:

.. code-block:: python

   results = cat.search(frequency="daily")
   # Internally becomes: frequency=["1day", "daily"]

   results = cat.search(realm="atmosphere")
   # Internally becomes: realm=["atmos", "atmosphere"]

Value aliases are available for the fields documented below.

.. _frequency_aliases:

Frequency aliases
~~~~~~~~~~~~~~~~~

These aliases apply to the ``frequency`` field.

.. dropdown:: Frequency alias reference

   ============  ================
   Alias         Canonical value
   ============  ================
   ``daily``     ``1day``
   ``day``       ``1day``
   ``monthly``   ``1mon``
   ``month``     ``1mon``
   ``yearly``    ``1yr``
   ``annual``    ``1yr``
   ``year``      ``1yr``
   ``hourly``    ``1hr``
   ``hour``      ``1hr``
   ``3hourly``   ``3hr``
   ``6hourly``   ``6hr``
   ============  ================

.. _realm_aliases:

Realm aliases
~~~~~~~~~~~~~

These aliases apply to the ``realm`` field.

.. dropdown:: Realm alias reference

   ====================  ================
   Alias                 Canonical value
   ====================  ================
   ``atmosphere``        ``atmos``
   ``atm``               ``atmos``
   ``oceanic``           ``ocean``
   ``terrestrial``       ``land``
   ``ice``               ``seaIce``
   ``sea_ice``           ``seaIce``
   ``sea-ice``           ``seaIce``
   ====================  ================

.. _model_aliases:

Model aliases
~~~~~~~~~~~~~

These aliases apply to the ``model`` field (top-level catalog) and ``source_id``
(ESM datastores).

.. dropdown:: Model alias reference

   ====================  ====================
   Alias                 Canonical value
   ====================  ====================
   ``ACCESS-ESM1``       ``ACCESS-ESM1-5``
   ====================  ====================

.. _experiment_aliases:

Experiment aliases
~~~~~~~~~~~~~~~~~~

These aliases apply to the ``experiment_id`` field.

.. dropdown:: Experiment alias reference

   ===================  ================
   Alias                Canonical value
   ===================  ================
   ``hist``             ``historical``
   ``control``          ``piControl``
   ``pi-control``       ``piControl``
   ``pre-industrial``   ``piControl``
   ``rcp85``            ``ssp585``
   ``rcp45``            ``ssp245``
   ``rcp26``            ``ssp126``
   ``ssp5-85``          ``ssp585``
   ``ssp2-45``          ``ssp245``
   ``ssp1-26``          ``ssp126``
   ===================  ================

.. _variable_aliases:

Variable aliases — CMIP-to-ACCESS mappings
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The most powerful aliases are the **CMIP-to-ACCESS variable mappings**. These apply to
the ``variable`` field (at both catalog levels) and ``variable_id`` (inside ESM
datastores). They allow you to search using CMIP standard variable names and find raw
ACCESS model output stored under ACCESS field codes.

Mappings are loaded automatically from the bundled file
``access_nri_intake/data/mappings/access-esm1-6-cmip-mappings.json``, which covers
137 variables across the atmosphere, land, and ocean components of
**ACCESS-ESM1.6**.

.. code-block:: python

   # Retrieve an ESM datastore for ACCESS-ESM1-6 data
   ds = cat["access-esm1-6"]

   # Search using the CMIP name — returns files stored as "fld_s03i236"
   ds.search(variable="tas")

   # Pass a list of CMIP names
   ds.search(variable=["tas", "pr", "ci"])

.. dropdown:: Representative CMIP variable mapping examples (ACCESS-ESM1.6)

   The table below shows a selection of common CMIP variables. The full list of 137
   mappings is in
   ``src/access_nri_intake/data/mappings/access-esm1-6-cmip-mappings.json``.

   ============  =================  ==========================================  =============
   CMIP name     ACCESS field       CF standard name                            Component
   ============  =================  ==========================================  =============
   ``tas``       ``fld_s03i236``    air_temperature                             atmosphere
   ``pr``        ``fld_s05i216``    precipitation_flux                          atmosphere
   ``ps``        ``fld_s00i409``    surface_air_pressure                        atmosphere
   ``uas``       ``fld_s03i209``    eastward_wind                               atmosphere
   ``vas``       ``fld_s03i210``    northward_wind                              atmosphere
   ``ci``        ``fld_s05i269``    convection_time_fraction                    atmosphere
   ``clt``       ``fld_s02i204``    cloud_area_fraction                         atmosphere
   ``cl``        ``fld_s02i261``    cloud_area_fraction_in_atmosphere_layer     atmosphere
   ``cli``       ``fld_s02i309``    mass_fraction_of_cloud_ice_in_air           atmosphere
   ``hfls``      ``fld_s03i234``    surface_upward_latent_heat_flux             atmosphere
   ``hfss``      ``fld_s03i217``    surface_upward_sensible_heat_flux           atmosphere
   ``rldscs``    ``fld_s02i208``    surface_downwelling_longwave_flux_…         atmosphere
   ``sftlf``     ``fld_s03i395``    land_area_fraction                          land
   ``tos``       ``surface_temp``   —                                           ocean
   ============  =================  ==========================================  =============

----

.. _alias_warnings:

Controlling alias behaviour
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Alias warnings
~~~~~~~~~~~~~~

Every time an alias fires, the library emits a :class:`UserWarning` describing the
mapping that was applied:

.. code-block:: python

   import warnings
   import intake

   cat = intake.cat.access_nri
   cat.search(frequency="daily")
   # UserWarning: Value aliasing: frequency='daily' → frequency=['1day','daily']

This is intentional — it keeps searches transparent and reproducible. You can suppress
these warnings by passing ``show_warnings=False`` when constructing the catalog wrapper,
or by using standard Python warning filters:

.. code-block:: python

   # Suppress using Python's warnings module
   with warnings.catch_warnings():
       warnings.simplefilter("ignore", UserWarning)
       results = cat.search(frequency="daily")

Escaping aliasing with ``.unwrap()``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Both the top-level catalog wrapper and the per-dataset ESM datastore wrapper expose an
:meth:`unwrap` method that returns the underlying, unaliased catalog object:

.. code-block:: python

   # Get the raw DfFileCatalog (no aliasing)
   raw_cat = cat.unwrap()

   # Get the raw esm_datastore (no aliasing)
   ds = cat["access-esm1-6"]
   raw_ds = ds.unwrap()

This is useful when you want to call catalog methods that are not supported by the alias
wrapper, or when you need the exact type expected by another library.

.. _alias_regex:

Regex and other non-string values
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Value aliasing only applies to plain strings and lists/tuples/sets of strings. If you
pass a **regex pattern** (e.g. ``"ci|cl|tas"``), an integer, or any other non-string
type, it is passed through to the underlying catalog unchanged:

.. code-block:: python

   # Regex: passed through unchanged — no aliasing applied
   ds.search(variable="ci|cl|tas")

   # Plain string: aliases applied
   ds.search(variable="ci")  # → ["fld_s05i269", "ci"]
