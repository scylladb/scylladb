# OS Support by Linux Distributions and Version

The following matrix shows which Linux distributions, containers, and images are supported with which versions of ScyllaDB.

Where *supported* in this scope means:

- A binary installation package is available to [download](https://www.scylladb.com/download/).
- The download and install procedures are tested as part of ScyllaDB release process for each version.
- An automated install is included from [ScyllaDB Web Installer for Linux tool](https://opensource.docs.scylladb.com/branch-5.4/getting-started/installation-common/scylla-web-installer.md) (for latest versions)

You can [build ScyllaDB from source](https://github.com/scylladb/scylladb#build-prerequisites) on other x86_64 or aarch64 platforms, without any guarantees.

|                            |                                                           |                                                           | Linux Distributions                                       | Ubuntu                                                    | Debian                                                     | CentOS /<br/>RHEL                                         | Rocky /<br/>RHEL                                           |
|----------------------------|-----------------------------------------------------------|-----------------------------------------------------------|-----------------------------------------------------------|-----------------------------------------------------------|------------------------------------------------------------|-----------------------------------------------------------|------------------------------------------------------------|
| ScyllaDB Version / Version | 20.04                                                     | 22.04                                                     | 10                                                        | 11                                                        | 7                                                          | 8                                                         | 9                                                          |
| 5.4                        | <i class="inline-icon icon-check" aria-hidden="true"></i> | <i class="inline-icon icon-check" aria-hidden="true"></i> | <i class="inline-icon icon-check" aria-hidden="true"></i> | <i class="inline-icon icon-check" aria-hidden="true"></i> | <i class="inline-icon icon-cancel" aria-hidden="true"></i> | <i class="inline-icon icon-check" aria-hidden="true"></i> | <i class="inline-icon icon-check" aria-hidden="true"></i>  |
| 5.2                        | <i class="inline-icon icon-check" aria-hidden="true"></i> | <i class="inline-icon icon-check" aria-hidden="true"></i> | <i class="inline-icon icon-check" aria-hidden="true"></i> | <i class="inline-icon icon-check" aria-hidden="true"></i> | <i class="inline-icon icon-check" aria-hidden="true"></i>  | <i class="inline-icon icon-check" aria-hidden="true"></i> | <i class="inline-icon icon-cancel" aria-hidden="true"></i> |
* The recommended OS for ScyllaDB Open Source is Ubuntu 22.04.
* All releases are available as a Docker container and EC2 AMI, GCP, and Azure images.

## Supported Architecture

ScyllaDB Open Source supports x86_64 for all versions and AArch64 starting from ScyllaDB 4.6 and nightly build.
In particular, aarch64 support includes AWS EC2 Graviton.
