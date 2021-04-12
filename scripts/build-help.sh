#!/bin/sh

cat << EOF
usage: ninja [options] [targets...]

Run \`ninja -h\` for help on supported options.

Build targets:
  build             Build artifacts for all configured build modes.
  <mode>-build      Build artifacts for a specific build mode.

Test targets:
  test              Run tests for all configured build modes.
  <mode>-test       Run tests for a specific build mode.

Packaging targets:
  dist              Build distribution packages (.rpm, .deb) for all build modes.
  <mode>-dist       Build distribution packages (.rpm, .deb) for a specific build mode.
EOF
