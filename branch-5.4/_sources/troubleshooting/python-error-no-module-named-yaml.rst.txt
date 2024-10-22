A change in EPEL broke ScyllaDB Python Script
===============================================

Phenomena
^^^^^^^^^
When upgrading CentOS on a Scylla node, Scylla setup script, like scylla_prepare or scylla_setup fails with the following error:

.. code-block:: python
                
  traceback (most recent call last):
    File "/sbin/scylla_setup", line 29, in <module>
      from scylla_util import *
    File "/usr/lib/scylla/scylla_util.py", line 31, in <module>
      import yaml
   ModuleNotFoundError: No module named 'yaml'

Problem
^^^^^^^

The source cause is a change in EPEL repository upgrade, breaking backward compatibility by moving from Python34 to Python36, and dropping PyYAML library in the process. Scylla uses PyYAML in a few of its Python scripts.


Bypass
^^^^^^^

Install the python36 version of PyYAML

.. code-block:: shell
                
  sudo yum install python36-PyYAML -y

Solution
^^^^^^^^

In future releases, we will provide a more robust solution by encapsulating Python as part of Scylla Installation. More on this in the blog post `The Complex Path for a Simple Portable Python Interpreter, or Snakes on a Data Plane <https://www.scylladb.com/2019/02/14/the-complex-path-for-a-simple-portable-python-interpreter-or-snakes-on-a-data-plane/>`_.







