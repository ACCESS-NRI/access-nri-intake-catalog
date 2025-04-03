.. _external:

Non-Gadi use
============

ACCESS-NRI Intake Catalog is designed for managing the collection Intake-ESM datastores on the 
`NCI Gadi <https://nci.org.au/our-systems/hpc-systems>`_ environment.
However, it is able to be modified to manage Intake-ESM datastores stored on other systems.

.. warning:: 
    Adapting ACCESS-NRI Intake Catalog to run on another computer system is an advanced task, that requires code
    modification, the ability to install this code on your target machine, and an intimate knowledge of the 
    file system on your target system. It is not recommended for the faint-hearted!

To adapt ACCESS-NRI Intake Catalog to run on a non-Gadi system, check out a copy of the source code and consider
making the following modifications:

1. The constants in the top level :code:`__init__.py` file describe where the input data are stored, the patterns
   and regular expressions used to match related file paths, and the location of the final catalog file. 
   These will need to be updated to reflect the target file system structure.

   a. Gadi stores data in a file system that is arrayed by projects. Projects are denoted by an alphanumeric code of
      one to two letters, followed by one to two digits, e.g., :code:`hh5`, :code:`io10`, etc. Data is then stored
      at targets like :code:`/g/data/<project code>/`.

      The :code:`catalog-build` command, which invokes the function :code:`cli.build`, has a sequence of calls to determine
      the projects that are involved in a catalog build, and checks that the build user has access to those project
      storage locations (this code section currently starts at line 473 of :code:`cli.py`). 
      The build will be aborted if these checks fail. Therefore, if your storage does not
      use a similar directory structure to Gadi (that is, a group of directories all situated at one root location), you may need
      to modify or remove these calls to achieve a successful catalog build.

2. The existing YAML files in :code:`config/` refer to the datastores/raw data stored on Gadi. You will need to 
   remove these, and replace them with similarly-structured YAML files denoting your own data setup. (Note that the 
   contents of :code:`config/metadata-sources` are archival copies of live experiment :ref:`metadata`; 
   you will not need to replace these on your system.)

3. The command-line scripts in :code:`bin/` contain PBS commands and file paths specific to Gadi. You will need 
   to modify these scripts to reflect your computing system.
