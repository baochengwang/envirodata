.. Envirodata documentation master file, created by
   sphinx-quickstart on Sun Feb  4 20:31:57 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Envirodata
===========

.. toctree::
   :maxdepth: 2
   :caption: Contents:

API
###

.. module:: envirodata.geocoder

.. autoclass:: Geocoder
   :members:

.. module:: envirodata.environment

.. autoclass:: Environment
   :members:

.. autoclass:: Service
   :members:

Services
#########

.. module:: envirodata.services.wetterdienst

.. autoclass:: CacheDB
   :members:

.. autoclass:: Loader
   :members:

.. autoclass:: Getter
   :members:

.. module:: envirodata.services.cdsapi

.. autoclass:: Loader
   :members:

.. autoclass:: Getter
   :members:

Utilities
##########

.. automethod:: envirodata.utils.general.get_config
.. automethod:: envirodata.utils.general.load_object


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
