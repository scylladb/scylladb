{ pkgs
, lib

, cc ? pkgs.cc
, gcc ? pkgs.gcc
, clang ? pkgs.clang

, ... }:

let
  inherit (builtins)
    map
    match
  ;
  inherit (pkgs)
    ccache
    coreutils
    distcc
    runCommand
    stdenv
  ;

  wrap = pkg: driver: ''
    #! ${stdenv.shell} -e
    driver=${pkg}/bin/${driver}
    dist_driver="$driver${if ((match "clang.*" driver) != null) then " -Wno-error=unused-command-line-argument" else ""}"
    distcc=${distcc}/bin/distcc
    ccache=${ccache}/bin/ccache

    export DISTCC_IO_TIMEOUT=1200  # hello, repair/row_level.cc

    wrap=
    if [[ -z "$NODISTCC" ]]; then
      wrap=d
    fi
    if [[ -n "$CCACHE_DIR" ]]; then
      wrap+=c
    fi

    if [[ -z "$wrap" ]]; then
      exec $driver "$@"
    elif [[ "$wrap" == d ]]; then
      exec $distcc $dist_driver "$@"
    elif [[ "$wrap" == c ]]; then
      exec $ccache $driver "$@"
    elif [[ "$wrap" == dc ]]; then
      export CCACHE_PREFIX=$distcc
      exec $ccache $dist_driver "$@"
    else
      echo wrapper bug 1>&2
      exit 1
    fi
  '';
in runCommand "distcc-ccache-wrap" { } ''
  ${coreutils}/bin/mkdir -p $out/bin
  ${lib.concatStrings (map ({pkg, driver}: ''
    ${coreutils}/bin/echo ${lib.escapeShellArg (wrap pkg driver)} > $out/bin/${driver}
    ${coreutils}/bin/chmod +x $out/bin/${driver}
  '') [
    { pkg = gcc; driver = "gcc"; }
    { pkg = gcc; driver = "g++"; }
    { pkg = clang; driver = "clang"; }
    { pkg = clang; driver = "clang++"; }
    { pkg = cc; driver = "cc"; }
    { pkg = cc; driver = "c++"; }
  ])}
''
