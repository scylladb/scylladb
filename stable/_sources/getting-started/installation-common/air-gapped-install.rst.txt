==============================
Air-gapped Server Installation
==============================

An air-gapped server is a server without any access to external repositories or connections to any network, including the internet.
To install ScyllaDB on an air-gapped server, you first need to download the relevant files from a server that is not air-gapped and then and move the files to the air-gapped servers to complete the installation.

There are two ways to install ScyllaDB on an air-gapped server:

- With root privileges (recommended): download the OS specific packages (rpms and debs) and install them with the package manager (dnf and apt). See `Install ScyllaDB on an Air-gapped Server Using the Packages (Option 2) <https://www.scylladb.com/download/?platform=tar>`_.
- Without root privileges: using the :doc:`ScyllaDB Unified Installer <unified-installer>`.
