#!/bin/sh

cat << EOF
usage: ninja [options] [targets...]

Run \`ninja -h\` for help on supported options.

Build targets:
  build             Build artifacts for all configured build modes.
  <mode>-build      Build artifacts for a specific build mode.
  <mode>-headers    Verify that all headers in given mode are self-sufficient.

  build/<mode>/scylla
                    Build scylla executable in given mode.

  build/<mode>/tools/scylla-sstable-index
  build/<mode>/tools/scylla-types
                    Build tool executables in given mode.

  build/<mode>/<path>/file.o
                    Build object file from <path>/file.cc.

  build/<mode>/<path>/file.hh.o
                    Verify that <path>/file.hh is self sufficient.

Test targets:
  test              Run tests for all configured build modes.
  <mode>-test       Run tests for a specific build mode.

  build/<mode>/test/<path>/test_executable
                    Build test executable in given mode.

  build/<mode>/test/<path>/test_executable_g
                    Build test executable with debug symbols in given mode.

Packaging targets:
  dist              Build distribution packages (.rpm, .deb) for all build modes.
  <mode>-dist       Build distribution packages (.rpm, .deb) for a specific build mode.
EOF
