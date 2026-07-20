============================
ScyllaDB Versioning
============================

ScyllaDB follows the ``MAJOR.MINOR.PATCH`` `semantic versioning <https://semver.org/>`_:

* ``MAJOR`` versions contain significant changes in the product and may introduce incompatible API changes.
* ``MINOR`` versions introduce new features and improvements in a backward-compatible manner.
* ``PATCH`` versions have backward-compatible bug fixes.

**Examples**

ScyllaDB Open Source:

* ``MAJOR`` versions: 4.y, 5.y
* ``MINOR`` versions: 5.2.z, 5.4.z
* ``PATCH`` versions: 5.2.1, 5.2.2


ScyllaDB Enterprise:

* ``MAJOR`` versions: 2021.y.x, 2022.y.z
* ``MINOR`` versions: 2022.1.z, 2022.2.z
* ``PATCH`` versions: 2022.1.1, 2022.1.2




.. only:: enterprise

    ScyllaDB Enterprise Version Support Policy
    ----------------------------------------------------

    ScyllaDB Enterprise supports two latest ``MAJOR`` versions and two latest ``MINOR`` versions. They are referred to as LTS (long-term support) and feature releass, respectively.

    **Example**

    Let's assume that the following versions are available as of today:
    2021.1, 2022.1, 2022.2, 2022.3, 2022.4

    The following versions would be supported:

    * 2021.1 and 2022.1 - two latest ``MAJOR`` versions (LTS)
    * 2022.3 and 2022.4 - two latest ``MINOR`` versions (feature releases)

    Version 2022.2 would not be supported.


    LTS vs. Feature Releases
    -----------------------------

    Long-Term Support (LTS) - Major Versions:
    
    * Released approximately once a year.


    Feature Releases - Minor Versions:
    
    * 3-4 releases per year
    * Closely follow ScyllaDB Open Source releases (see `ScyllaDB Enterprise vs. Open Source Matrix <https://enterprise.docs.scylladb.com/stable/reference/versions-matrix-enterprise-oss.html>`_)
    * Introduce features added in ScyllaDB Open Source, as well as Enterprise-only premium features