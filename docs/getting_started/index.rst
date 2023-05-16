.. _Getting_started:

Getting Started
===============

Thanks for helping to test and provide feedback on the ACCESS-NRI Intake catalog.

The catalog is not yet available in one of the pre-built analysis environments on NCI, so for the time being, we'll have to install it into an environment ourselves. Here we'll use the `CLEX CMS managed conda <http://climate-cms.wikis.unsw.edu.au/Conda>`_ for convenience. To use this, you'll need to be a member of the hh5 project on Gadi. You'll also need to be a member of the tm70 project to access the ACCESS-NRI catalog files.

#. If you haven't already, create a file called :code:`~/.condarc` containing::

    auto_activate_base: false
    envs_dirs:
      - /scratch/$PROJECT/$USER/conda/envs
      - /g/data/hh5/public/apps/miniconda3/envs
    pkgs_dirs:
      - /scratch/$PROJECT/$USER/conda/pkgs
    conda-build:
      root-dir: /scratch/$PROJECT/$USER/conda/bld

#. Run the following to load the conda module and then deactivate it::

    $ module use /g/data/hh5/public/modules
    $ module load conda/analysis3
    $ conda deactivate

#. Create a new conda environment containing the access-nri-intake package and some additional useful packages::

    $ conda create -n access-nri-catalog -y \
    $     -c conda-forge -c accessnri \
    $     jupyterlab dask-labextension matplotlib access-nri-intake

#. |  Now start an ARE JupyterLab session, specifying the following Advanced options:

   * :code:`/g/data/hh5/public/modules` under "Module directories";
   * :code:`conda/analysis3` under "Modules";
   * :code:`/g/data3/hh5/public/apps/miniconda3` under "Python or Conda virtual environment base";
   * :code:`access-nri-catalog` under "Conda environment". 
   
   | From this session, you should be able to run the example notebooks in the `notebooks directory of this repo <https://github.com/ACCESS-NRI/access-nri-intake-catalog/tree/main/notebooks>`_. Note, static renderings of these notebooks are also included in the :ref:`How-to guides <How_tos>` section of this documentation.


