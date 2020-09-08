===============================
vivarium_csu_ltbi
===============================

Research repository for the vivarium_csu_ltbi project.

.. contents::
   :depth: 1

Model Documentation Resources
-----------------------------

**You should put links to the concept model documentation and any other**
**relevant documentation here.**

Installation
------------

These models require data from GBD databases. You'll need several internal
IHME packages and access to the IHME cluster.

To install the extra dependencies create a file called ~/.pip/pip.conf which
looks like this::

    [global]
    extra-index-url = http://pypi.services.ihme.washington.edu/simple
    trusted-host = pypi.services.ihme.washington.edu


To set up a new research environment, open up a terminal on the cluster and
run::

    $> conda create --name=vivarium_csu_ltbi python=3.6
    ...standard conda install stuff...
    $> conda activate vivarium_csu_ltbi
    (vivarium_csu_ltbi) $> conda install redis
    (vivarium_csu_ltbi) $> git clone git@github.com:ihmeuw/vivarium_csu_ltbi.git
    ...you may need to do username/password stuff here...
    (vivarium_csu_ltbi) $> cd vivarium_csu_ltbi
    (vivarium_csu_ltbi) $> pip install -e .


Usage
-----

You'll find five directories inside the main
``src/vivarium_csu_ltbi`` package directory:

- ``artifacts``

  This directory contains all input data used to run the simulations.
  You can open these files and examine the input data using the vivarium
  artifact tools.  A tutorial can be found at https://vivarium.readthedocs.io/en/latest/tutorials/artifact.html#reading-data

- ``components``

  This directory is for Python modules containing custom components for
  the vivarium_csu_ltbi project. You should work with the
  engineering staff to help scope out what you need and get them built.

- ``external_data``

  If you have **small scale** external data for use in your sim or in your
  results processing, it can live here. This is almost certainly not the right
  place for data, so make sure there's not a better place to put it first.

- ``model_specifications``

  This directory should hold all model specifications and branch files
  associated with the project.

- ``verification_and_validation``

  Any post-processing and analysis code or notebooks you write should be
  stored in this directory.


  Running Simulations
  -------------------

  With your conda environment active, the first step to running simulations
  is making the model specification files.  A model specification is a
  complete description of a vivarium model. The command to generate model
  specifications is installed with this repository and it can be run
  from any directory.::

    (vivarium-htn) $> make_specs
      2019-11-18 21:30:41.429 | INFO     | vivarium_csu_ltbi.cli:make_specs:69 - Writing model spec(s) to "/REPO_INSTALLATION_DIRECTORY/vivarium_csu_ltbi/src/vivarium_csu_ltbi/model_specifications"
      2019-11-18 21:30:41.429 | INFO     | vivarium_csu_ltbi.cli:make_specs:74 -    Writing china.yaml
      2019-11-18 21:30:41.430 | INFO     | vivarium_csu_ltbi.cli:make_specs:74 -    Writing italy.yaml

      2019-11-18 21:30:41.430 | INFO     | vivarium_csu_ltbi.cli:make_specs:74 -    Writing mexico.yaml
      2019-11-18 21:30:41.431 | INFO     | vivarium_csu_ltbi.cli:make_specs:74 -    Writing russian_federation.yaml
      2019-11-18 21:30:41.431 | INFO     | vivarium_csu_ltbi.cli:make_specs:74 -    Writing south_korea.yaml

  As the log message indicates, the model specifications will be written to
  the ``model_specifications`` subdirectory in this repository. You can then
  run simulations by, e.g.::

    (vivarium-htn) $> simulate run -v /<REPO_INSTALLATION_DIRECTORY>/vivarium_csu_ltbi/src/vivarium_csu_ltbi/model_specifications/china.yaml

   The ``-v`` flag will log verbosely, so you will get log messages every time
   step. For more ways to run simulations, see the tutorials at
   https://vivarium.readthedocs.io/en/latest/tutorials/running_a_simulation/index.html
   and https://vivarium.readthedocs.io/en/latest/tutorials/exploration.html

