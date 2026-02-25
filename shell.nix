# Copyright (C) 2021-present ScyllaDB
#

#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

args:

import ./default.nix (args // {
  shell = true;

  devInputs = { pkgs, llvm }: with pkgs; [
    # for impure building
    ccache
    distcc

    # for debugging
    binutils  # addr2line etc.
    elfutils

    gdbWithGreenThreadSupport

    llvm.llvm
    lz4       # coredumps on modern Systemd installations are lz4-compressed

    # etc
    diffutils
    colordiff
  ];
})
