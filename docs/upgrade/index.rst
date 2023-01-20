=================
Upgrade ScyllaDB
=================

.. toctree::
   :titlesonly:
   :hidden:

   ScyllaDB Enterprise <upgrade-enterprise/index>
   ScyllaDB Open Source <upgrade-opensource/index>
   ScyllaDB Open Source to ScyllaDB Enterprise <upgrade-to-enterprise/index>
   ScyllaDB AMI <ami-upgrade>


ScyllaDB upgrade is a rolling procedure - it does not require a full cluster shutdown and is performed without any 
downtime or disruption of service.

To ensure a successful upgrade and avoid breaking anything, you should perform your upgrades consecutively - to each 
successive X.Y version. For example, to upgrade from version 4.4 to 5.0, you should first upgrade ScyllaDB to version 
4.5, next from 4.5 to 4.6, and finally, from 4.6 to 5.0, **without skipping any major version**.

.. raw:: html


   <div class="panel callout radius animated">
            <div class="row">
              <div class="medium-3 columns">
                <h5 id="getting-started">Procedures for upgrading ScyllaDB</h5>
              </div>
              <div class="medium-9 columns">


* :doc:`Upgrade ScyllaDB Enterprise <upgrade-enterprise/index>`

* :doc:`Upgrade ScyllaDB Open Source <upgrade-opensource/index>`

* :doc:`Upgrade from ScyllaDB Open Source to Scylla Enterprise <upgrade-to-enterprise/index>`

* :doc:`Upgrade ScyllaDB AMI <ami-upgrade>`


.. raw:: html

   </div>
   </div>
   </div>


