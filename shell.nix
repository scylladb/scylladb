# Copyright (C) 2021-present ScyllaDB
#

#
# SPDX-License-Identifier: AGPL-3.0-or-later

{
  pkgs ? null,
  mode ? "dev",
  useCcache ? false
}:
import ./default.nix ({
  inherit mode useCcache;
  testInputsFrom = pkgs: with pkgs; [
    python3Packages.boto3
    python3Packages.colorama
    python3Packages.pytest
  ];
  gitPkg = pkgs: pkgs.gitFull;
} //
(if pkgs != null then { inherit pkgs; } else {}))
