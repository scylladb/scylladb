Removing ScyllaDB with the "--autoremove" option on Ubuntu breaks system packages
======================================================================================

Problem
^^^^^^^

Running ``apt purge scylla --autoremove`` marks most system packages for
removal.

.. code::

   root@myserv:~# apt purge scylla --autoremove
   Reading package lists... Done
   Building dependency tree... Done
   Reading state information... Done
   The following packages will be REMOVED:
     apport-symptoms* bc* bcache-tools* bolt* btrfs-progs* byobu* cloud-guest-utils* cloud-init* cloud-initramfs-copymods* cloud-initramfs-dyn-netconf* cryptsetup* cryptsetup-initramfs* dmeventd* eatmydata* ethtool* fdisk* fonts-ubuntu-console* fwupd* fwupd-signed* gdisk* gir1.2-packagekitglib-1.0* git* git-man* kpartx* landscape-common* libaio1* libappstream4* libatasmart4* libblockdev-crypto2* libblockdev-fs2*
     libblockdev-loop2* libblockdev-part-err2* libblockdev-part2* libblockdev-swap2* libblockdev-utils2* libblockdev2* libdevmapper-event1.02.1* libeatmydata1* liberror-perl* libfdisk1* libfwupd2* libfwupdplugin5* libgcab-1.0-0* libgpgme11* libgstreamer1.0-0* libgusb2* libinih1* libintl-perl* libintl-xs-perl* libjcat1* libjson-glib-1.0-0* libjson-glib-1.0-common* liblvm2cmd2.03* libmbim-glib4* libmbim-proxy*
     libmm-glib0* libmodule-find-perl* libmodule-scandeps-perl* libmspack0* libpackagekit-glib2-18* libparted-fs-resize0* libproc-processtable-perl* libqmi-glib5* libqmi-proxy* libsgutils2-2* libsmbios-c2* libsort-naturally-perl* libstemmer0d* libtcl8.6* libterm-readkey-perl* libudisks2-0* liburcu8* libutempter0* libvolume-key1* libxmlb2* libxmlsec1* libxmlsec1-openssl* libxslt1.1* lvm2* lxd-agent-loader* mdadm*
     modemmanager* motd-news-config* multipath-tools* needrestart* open-vm-tools* overlayroot* packagekit* packagekit-tools* pastebinit* patch* pollinate* python3-apport* python3-certifi* python3-chardet* python3-configobj* python3-debconf* python3-debian* python3-json-pointer* python3-jsonpatch* python3-jsonschema* python3-magic* python3-newt* python3-packaging* python3-pexpect* python3-problem-report*
     python3-ptyprocess* python3-pyrsistent* python3-requests* python3-software-properties* python3-systemd* python3-xkit* run-one* sbsigntool* screen* scylla* scylla-conf* scylla-cqlsh* scylla-kernel-conf* scylla-node-exporter* scylla-python3* scylla-server* secureboot-db* sg3-utils* sg3-utils-udev* software-properties-common* sosreport* tcl* tcl8.6* thin-provisioning-tools* tmux* ubuntu-drivers-common* udisks2*
     unattended-upgrades* update-notifier-common* usb-modeswitch* usb-modeswitch-data* xfsprogs* zerofree*
   0 upgraded, 0 newly installed, 139 to remove and 0 not upgraded.

Cause
^^^^^^^

This problem may occur on Ubuntu 22.04 or earlier. It is caused by
the ``systemd-coredump`` package installed with the ``scylla_setup`` script.
Installing ``systemd-coredump`` results in removing ``apport`` and ``ubuntu-server``.
In turn, the ``--autoremove`` option marks for removal all packages installed
by ``ubuntu-server dependencies``.


Solution
^^^^^^^^^^

Do not run the ``--autoremove`` option when removing ScyllaDB.