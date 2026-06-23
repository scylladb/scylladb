#!/bin/bash
# Install perf-bench skill for opencode
set -euo pipefail

SKILL_DIR="$HOME/.config/opencode/skills/perf-bench"
SOURCE_DIR="$(cd "$(dirname "$0")" && pwd)"

if [ -e "$SKILL_DIR" ] && [ ! -L "$SKILL_DIR" ]; then
  echo "Error: $SKILL_DIR exists and is not a symlink" >&2
  exit 1
fi

CURRENT_TARGET="$(readlink "$SKILL_DIR" 2>/dev/null || true)"
if [ "$CURRENT_TARGET" = "$SOURCE_DIR" ]; then
  echo "Already installed: $SKILL_DIR -> $SOURCE_DIR"
elif [ -n "$CURRENT_TARGET" ]; then
  echo "Updating symlink: $SKILL_DIR was $CURRENT_TARGET, now $SOURCE_DIR"
  ln -sf "$SOURCE_DIR" "$SKILL_DIR"
else
  ln -sf "$SOURCE_DIR" "$SKILL_DIR"
  echo "Installed: $SKILL_DIR -> $SOURCE_DIR"
fi
