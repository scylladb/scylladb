# Copyright (C) 2021-present ScyllaDB
#

#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.

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
