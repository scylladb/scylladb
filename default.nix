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
#

#
# * At present this is not very useful for nix-build, just for nix-shell
#
# * IMPORTANT: to avoid using up ungodly amounts of disk space under
#   /nix/store/, make sure the actual build directory is physically
#   outside this tree, and make ./build a symlink to it
#

{
  pkgs ? import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/4cb48cc25622334f17ec6b9bf56e83de0d521fb7.tar.gz") {},
  mode ? "release",
  verbose ? false,
  useCcache ? false, # can't get this to work, see https://github.com/NixOS/nixpkgs/issues/49894
  testInputsFrom ? (_: []),
  gitPkg ? (pkgs: pkgs.gitMinimal),
}:

with pkgs; let
  inherit (builtins)
    baseNameOf
    fetchurl
    match
    readFile
    toString
    trace;

  antlr3Patched = antlr3.overrideAttrs (_: {
    patches = [
      (fetchurl {
        url = "https://src.fedoraproject.org/rpms/antlr3/raw/f1bb8d639678047935e1761c3bf3c1c7da8d0f1d/f/0006-antlr3memory.hpp-fix-for-C-20-mode.patch";
      })
    ];
  });
  rapidjsonPatched = rapidjson.overrideAttrs (_: {
    patches = [
      (fetchurl {
        url = "https://src.fedoraproject.org/rpms/rapidjson/raw/48402da9f19d060ffcd40bf2b2e6987212c58b0c/f/rapidjson-1.1.0-c++20.patch";
      })
    ];
  });
  zstdStatic = zstd.overrideAttrs (_: {
    cmakeFlags = [
      "-DZSTD_BUILD_SHARED:BOOL=OFF"
      "-DZSTD_BUILD_STATIC:BOOL=ON"
      "-DZSTD_PROGRAMS_LINK_SHARED:BOOL=OFF"
      "-DZSTD_LEGACY_SUPPORT:BOOL=ON"
      "-DZSTD_BUILD_TESTS:BOOL=OFF"
    ];
  });

  llvmBundle = llvmPackages_11;

  stdenv =
    if useCcache
    then (overrideCC llvmBundle.stdenv (ccacheWrapper.override { cc = llvmBundle.clang; }))
    else llvmBundle.stdenv;

in stdenv.mkDerivation {
  name = "scylladb";
  nativeBuildInputs = [
    ant
    antlr3Patched
    boost17x.dev
    cmake
    gcc
    (gitPkg pkgs)
    libtool
    llvmBundle.lld
    maven
    ninja
    pkg-config
    python3
    ragel
    stow
  ];
  buildInputs = [
    antlr3Patched
    boost17x
    c-ares
    cryptopp
    fmt
    gmp
    gnutls
    hwloc
    icu
    jsoncpp
    libp11
    libsystemtap
    libtasn1
    libunistring
    libxfs
    libxml2
    libyamlcpp
    lksctp-tools
    lua53Packages.lua
    lz4
    nettle
    numactl
    openssl
    p11-kit
    protobuf
    python3Packages.cassandra-driver
    python3Packages.distro
    python3Packages.psutil
    python3Packages.pyparsing
    python3Packages.pyudev
    python3Packages.pyyaml
    python3Packages.requests
    python3Packages.setuptools
    python3Packages.urwid
    rapidjsonPatched
    snappy
    systemd
    thrift
    valgrind
    xorg.libpciaccess
    xxHash
    zlib
    zstdStatic
  ] ++ (testInputsFrom pkgs);

  src = lib.cleanSourceWith {
    filter = name: type:
      let baseName = baseNameOf (toString name); in
      !((type == "symlink" && baseName == "build") ||
        (type == "directory" &&
         (baseName == "build" ||
          baseName == ".cache" ||
          baseName == ".direnv" ||
          baseName == ".github" ||
          baseName == ".pytest_cache" ||
          baseName == "__pycache__")));
    src = ./.;
  };

  postPatch = ''
    patchShebangs ./configure.py
    patchShebangs ./merge-compdb.py
    patchShebangs ./seastar/scripts/seastar-json2code.py
    patchShebangs ./seastar/cooking.sh
  '';

  IMPLICIT_CFLAGS = ''
    ${readFile (llvmBundle.stdenv.cc + "/nix-support/libcxx-cxxflags")} ${readFile (llvmBundle.stdenv.cc + "/nix-support/libc-cflags")}
  '';

  configurePhase = ''
    ./configure.py ${if verbose then "--verbose " else ""}--mode=${mode}
  '';

  buildPhase = ''
    ${ninja}/bin/ninja build/${mode}/scylla
  '';

  installPhase = ''
    mkdir $out
    cp -r * $out/
  '';
}
