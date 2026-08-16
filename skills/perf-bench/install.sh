#!/bin/bash
# Install perf-bench skill for opencode
set -euo pipefail

SKILL_DIR="$HOME/.config/opencode/skills/perf-bench"
SOURCE_DIR="$(cd "$(dirname "$0")" && pwd)"

if [ -L "$SKILL_DIR" ]; then
  echo "Symlink already exists: $SKILL_DIR"
elif [ -e "$SKILL_DIR" ]; then
  echo "Error: $SKILL_DIR exists and is not a symlink" >&2
  exit 1
else
  ln -sf "$SOURCE_DIR" "$SKILL_DIR"
  echo "Installed: $SKILL_DIR -> $SOURCE_DIR"
fi
