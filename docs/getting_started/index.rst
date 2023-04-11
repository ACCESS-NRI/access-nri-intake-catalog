.. _Getting_started:

Getting Started
===============

Thanks for helping to test and provide feedback on the ACCESS-NRI Intake catalog.

The catalog is not yet available in one of the pre-built analysis environments on NCI, so for the time being, we'll have to install it into an environment ourselves. Here we'll use the `CLEX CMS managed conda <http://climate-cms.wikis.unsw.edu.au/Conda>`_ for convenience. To use this, you'll need to be a member of the hh5 project on Gadi. You'll also need to be a member of the tm70 project to access the ACCESS-NRI catalog files.

#. If you haven't already, create a file called :code:`~/.condarc` containing:

.. code-block:: bash

    auto_activate_base: false
    envs_dirs:
      - /scratch/$PROJECT/$USER/conda/envs
      - /g/data/hh5/public/apps/miniconda3/envs
    pkgs_dirs:
      - /scratch/$PROJECT/$USER/conda/pkgs
    conda-build:
      root-dir: /scratch/$PROJECT/$USER/conda/bld

#. Run the following to load the conda module and then deactivate it:

.. code-block:: bash

    $ module use /g/data/hh5/public/modules
    $ module load conda/analysis3
    $ conda deactivate

#. Clone this repo and install the ACCESS-NRI catalog and some other packages:

.. code-block:: bash

    $ git clone git@github.com:ACCESS-NRI/nri_intake_catalog.git
    $ cd nri_intake_catalog
    $ conda env create --name nri-cat -f environment-dev.yaml
    $ conda activate nri-cat
    $ conda install -c conda-forge jupyterlab
    $ pip install --no-deps -e .

#. Now start an ARE JupyterLab session, specifying :code:`/g/data3/hh5/public/apps/miniconda3` under "Python or Conda virtual environment base" and :code:`nri-cat` under "Conda environment" (both are in Advanced options). From this session, you should be able to run the example notebooks in :code:`notebooks` of the current directory.

