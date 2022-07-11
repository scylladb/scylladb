OS Support by Platform and Version
==================================

The following matrix shows which Operating Systems, Platforms, and Containers / Instance Engines are supported with which versions of Scylla.

Scylla requires a fix to the XFS append introduced in kernel 3.15 (back-ported to 3.10 in RHEL/CentOS).
Scylla will not run with earlier kernel versions. Details in `Scylla issue 885 <https://github.com/scylladb/scylla/issues/885>`_.

.. note::

   Scylla Open Source supports x86_64 for all versions and aarch64 starting from Scylla 4.6 and nightly build. In particular, aarch64 support includes AWS EC2 Graviton.

   For Scylla Open Source **4.5** and later, the recommended OS and Scylla AMI/IMage OS is Ubuntu 20.04.4 LTS.


Scylla Open Source
-------------------

.. note:: For Enterprise versions **prior to** 4.6, the recommended OS and Scylla AMI/Image OS is CentOS 7.

   For Scylla Open Source versions **4.6 and later**, the recommended OS and Scylla AMI/Image OS is Ubuntu 20.04.



+--------------------------+----------------------------------+-----------------------------+-------------+
| Platform                 |       Ubuntu                     |    Debian                   | Centos/RHEL |
+--------------------------+------+------+------+------+------+------+------+-------+-------+------+------+
| Scylla Version / Version | 14.04| 16.04| 18.04|20.04 |22.04 | 8    | 9    |  10   |  11   | 7    | 8    |
+==========================+======+======+======+======+======+======+======+=======+=======+======+======+
|   5.0                    | |x|  | |x|  | |v|  | |v|  | |v|  | |x|  | |x|  | |v|   | |v|   | |v|  | |v|  |
+--------------------------+------+------+------+------+------+------+------+-------+-------+------+------+
|   4.6                    | |x|  | |v|  | |v|  | |v|  | |x|  | |x|  | |v|  | |v|   | |x|   | |v|  | |v|  |
+--------------------------+------+------+------+------+------+------+------+-------+-------+------+------+
|   4.5                    | |x|  | |v|  | |v|  | |v|  | |x|  | |x|  | |v|  | |v|   | |x|   | |v|  | |v|  |
+--------------------------+------+------+------+------+------+------+------+-------+-------+------+------+
|   4.4                    | |x|  | |v|  | |v|  | |v|  | |x|  | |x|  | |v|  | |v|   | |x|   | |v|  | |v|  |
+--------------------------+------+------+------+------+------+------+------+-------+-------+------+------+
|   4.3                    | |x|  | |v|  | |v|  | |v|  | |x|  | |x|  | |v|  | |v|   | |x|   | |v|  | |v|  |
+--------------------------+------+------+------+------+------+------+------+-------+-------+------+------+
|   4.2                    | |x|  | |v|  | |v|  | |x|  | |x|  | |x|  | |v|  | |v|   | |x|   | |v|  | |v|  |
+--------------------------+------+------+------+------+------+------+------+-------+-------+------+------+
|   4.1                    | |x|  | |v|  | |v|  | |x|  | |x|  | |x|  | |v|  | |v|   | |x|   | |v|  | |v|  |
+--------------------------+------+------+------+------+------+------+------+-------+-------+------+------+
|   4.0                    | |x|  | |v|  | |v|  | |x|  | |x|  | |x|  | |v|  | |x|   | |x|   | |v|  | |x|  |
+--------------------------+------+------+------+------+------+------+------+-------+-------+------+------+
|   3.x                    | |x|  | |v|  | |v|  | |x|  | |x|  | |x|  | |v|  | |x|   | |x|   | |v|  | |x|  |
+--------------------------+------+------+------+------+------+------+------+-------+-------+------+------+
|   2.3                    | |v|  | |v|  | |v|  | |x|  | |x|  | |v|  | |v|  | |x|   | |x|   | |v|  | |x|  |
+--------------------------+------+------+------+------+------+------+------+-------+-------+------+------+
|   2.2                    | |v|  | |v|  | |x|  | |x|  | |x|  | |v|  | |x|  | |x|   | |x|   | |v|  | |x|  |
+--------------------------+------+------+------+------+------+------+------+-------+-------+------+------+


All releases are available as a Docker container, EC2 AMI, and a GCP image (GCP image from version 4.3).


Scylla Enterprise
-----------------

.. note:: Enterprise versions **prior to** 2021.1, the recommended OS and Scylla AMI/IMage OS is CentOS 7.

   For Enterprise versions **2021.1 and later**, the recommended OS and Scylla AMI/IMage OS is Ubuntu 20.04.4 LTS.

   For Enterprise versions **2021.1 and later**, the recommended OS and Scylla AMI/Image OS is Ubuntu 20.04.

+--------------------------+---------------------------+--------------------+------------+
| Platform                 |  Ubuntu                   | Debian             | Centos/RHEL|
+--------------------------+------+------+------+------+------+------+------+------+-----+
| Scylla Version / Version | 14   | 16   |  18  |  20  | 8    | 9    | 10   |  7   | 8   |
+==========================+======+======+======+======+======+======+======+======+=====+
|   2021.1                 | |x|  | |v|  | |v|  | |v|  |  |x| |  |v| |  |v| | |v|  | |v| |
+--------------------------+------+------+------+------+------+------+------+------+-----+
|   2020.1                 | |x|  | |v|  | |v|  |  |x| |  |x| |  |v| |  |v| | |v|  | |v| |
+--------------------------+------+------+------+------+------+------+------+------+-----+
|   2019.1                 | |x|  | |v|  | |v|  |  |x| |  |x| |  |v| |  |x| | |v|  | |x| |
+--------------------------+------+------+------+------+------+------+------+------+-----+
|   2018.1                 | |v|  | |v|  | |x|  |  |x| | |v|  | |x|  |  |x| | |v|  | |x| |
+--------------------------+------+------+------+------+------+------+------+------+-----+


All releases are available as a Docker container, EC2 AMI, and a GCP image (GCP image from version 2021.1).
