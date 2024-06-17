# Copyright (C) 2021-present ScyllaDB
#

#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

#
# * "nix build" is unsupported (or rather supported up to and not including
#   installPhase), so basically all this is just for "nix develop"
#
# * IMPORTANT: to avoid using up ungodly amounts of disk space under
#   /nix/store/ when you are not using flakes, make sure to move the
#   actual build directory outside this tree and make ./build a
#   symlink to it.  Or use flakes (seriously, just use flakes).
#

{ flake ? false
, shell ? false
, pkgs ? import <nixpkgs> { system = builtins.currentSystem; overlays = [ (import ./dist/nix/overlay.nix <nixpkgs>) ]; }
, srcPath ? builtins.path { path = ./.; name = "scylla"; }
, repl ? null
, mode ? "release"
, verbose ? false

# shell env will want to add stuff to the environment, and the way
# for it to do so is to pass us a function with this signatire:
, devInputs ? ({ pkgs, llvm }: [])
}:

let
  inherit (builtins)
    baseNameOf
    head
    match
    split
  ;

  inherit (import (builtins.fetchTarball {
    url = "https://github.com/hercules-ci/gitignore/archive/5b9e0ff9d3b551234b4f3eb3983744fa354b17f1.tar.gz";
    sha256 = "01l4phiqgw9xgaxr6jr456qmww6kzghqrnbc7aiiww3h6db5vw53";
  }) { inherit (pkgs) lib; })
    gitignoreSource;

  # all later Boost versions are problematic one way or another
  boost = pkgs.boost175;

  llvm = pkgs.llvmPackages_15;

  stdenvUnwrapped = llvm.stdenv;

  # define custom ccache- and distcc-aware wrappers for all relevant
  # compile drivers (used only in shell env)
  cc-wrappers = pkgs.callPackage ./dist/nix/pkg/custom/ccache-distcc-wrap {
    cc = stdenvUnwrapped.cc;
    clang = llvm.clang;
    inherit (pkgs) gcc;
  };

  stdenv = if shell then pkgs.overrideCC stdenvUnwrapped cc-wrappers
           else stdenvUnwrapped;

  noNix = path: type: type != "regular" || (match ".*\.nix" path) == null;
  src = builtins.filterSource noNix (if flake then srcPath
                                     else gitignoreSource srcPath);

  derive = if shell then pkgs.mkShell.override { inherit stdenv; }
           else stdenv.mkDerivation;

in derive ({
  name = "scylla";
  inherit src;

  # since Scylla build, as it exists, is not cross-capable, the
  # nativeBuildInputs/buildInputs distinction below ranges, depending
  # on how charitable one feels, from "pedantic" through
  # "aspirational" all the way to "cargo cult ritual" -- i.e. not
  # expected to be actually correct or verifiable.  but it's the
  # thought that counts!
  nativeBuildInputs = with pkgs; [
    ant
    antlr3
    boost
    cargo
    cmake
    cxx-rs
    gcc
    openjdk11_headless
    libtool
    llvm.bintools
    maven
    ninja
    pkg-config
    python2
    (python3.withPackages (ps: with ps; [
      aiohttp
      boto3
      colorama
      distro
      magic
      psutil
      pyparsing
      pytest
      pytest-asyncio
      pyudev
      pyyaml
      requests
      scylla-driver
      setuptools
      tabulate
      urwid
    ]))
    ragel
    rustc
    stow
  ] ++ (devInputs { inherit pkgs llvm; });

  buildInputs = with pkgs; [
    abseil-cpp
    antlr3
    boost
    c-ares
    cryptopp
    fmt
    gmp
    gnutls
    hwloc
    icu
    jsoncpp
    libdeflate
    libidn2
    libp11
    libsystemtap
    libtasn1
    libunistring
    liburing
    libxcrypt
    libxfs
    libxml2
    libyamlcpp
    llvm.compiler-rt
    lksctp-tools
    lua54Packages.lua
    lz4
    nettle
    numactl
    openssl
    p11-kit
    protobuf
    rapidjson
    snappy
    systemd
    valgrind
    xorg.libpciaccess
    xxHash
    zlib
    zstd
  ];

  JAVA8_HOME = "${pkgs.openjdk8_headless}/lib/openjdk";
  JAVA_HOME = "${pkgs.openjdk11_headless}/lib/openjdk";

}
// (if shell then {

  configurePhase = "./configure.py${if verbose then " --verbose" else ""} --disable-dpdk";

} else {

  # sha256 of the filtered source tree:
  SCYLLA_RELEASE = head (split "-" (baseNameOf src));

  postPatch = ''
    patchShebangs ./configure.py
    patchShebangs ./seastar/scripts/seastar-json2code.py
    patchShebangs ./seastar/cooking.sh
  '';

  configurePhase = "./configure.py${if verbose then " --verbose" else ""} --mode=${mode}";

  buildPhase = ''
    ${pkgs.ninja}/bin/ninja \
      build/${mode}/scylla \
      build/${mode}/iotune \

  '';
   #   build/${mode}/dist/tar/scylla-tools-package.tar.gz \
   #   build/${mode}/dist/tar/scylla-jmx-package.tar.gz \

  installPhase = ''
    echo not implemented 1>&2
    exit 1
  '';

})
// (if !shell || repl == null then {} else {

  REPL = repl;

})
)
