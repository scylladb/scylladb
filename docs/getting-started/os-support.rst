OS Support by Linux Distributions and Version
==============================================

The following matrix shows which Linux distributions, containers, and images
are :ref:`supported <os-support-definition>` with which versions of ScyllaDB.

Note that support for Ubuntu 20.04 is deprecated and will be removed in
a future release.

.. datatemplate:json:: /_static/data/os-support.json
  :template: platforms.tmpl

``*`` 2024.1.9 and later

All releases are available as a Docker container, EC2 AMI, GCP, and Azure images.

.. _os-support-definition:

By *supported*, it is meant that:

- A binary installation package is available to `download <https://www.scylladb.com/download/>`_.
- The download and install procedures are tested as part of the ScyllaDB release process for each version.
- An automated install is included from :doc:`ScyllaDB Web Installer for Linux tool </getting-started/installation-common/scylla-web-installer>` (for the latest versions).

You can `build ScyllaDB from source <https://github.com/scylladb/scylladb#build-prerequisites>`_
on other x86_64 or aarch64 platforms, without any guarantees.



