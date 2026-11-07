==============================
Air-gapped Server Installation
==============================

An air-gapped server is a server without any access to external repositories or connections to any network, including the internet.
To install ScyllaDB on an air-gapped server, you first need to download the relevant files from a server that is not air-gapped and then move the files to the air-gapped servers to complete the installation.

There are two ways to install ScyllaDB on an air-gapped server:

- With root privileges (recommended): on a server with internet access, download the OS-specific packages (rpms and debs)
  from the ScyllaDB repository, move them to the air-gapped server, and install them with the package manager (dnf and apt).
  See :doc:`Install ScyllaDB Linux Packages </getting-started/install-scylla/install-on-linux>` for the repository
  configuration for your platform.
- Without root privileges: using the :doc:`ScyllaDB Unified Installer <unified-installer>`.
