#!/bin/bash -e
#
# Update the cqlsh-rs submodule to a new version and commit with full diff details.
#
# Usage:
#   ./tools/cqlsh-rs-pkg/update-cqlsh-rs.sh                # update to latest main
#   ./tools/cqlsh-rs-pkg/update-cqlsh-rs.sh v0.2.0         # update to a tag
#   ./tools/cqlsh-rs-pkg/update-cqlsh-rs.sh abc1234         # update to a specific commit
#   ./tools/cqlsh-rs-pkg/update-cqlsh-rs.sh origin/main     # update to a remote branch tip
#
# The commit message will include:
#   - Old and new commit SHAs
#   - Full git log between old and new versions
#   - GitHub PR links extracted from commit messages
#   - GitHub release notes (when the target is a tagged release)
#   - Diffstat of changes in cqlsh-rs
#
# Environment variables:
#   GITHUB_TOKEN   Optional. Set to a personal access token to avoid GitHub
#                  API rate limits (60 req/h unauth vs 5000 req/h auth).
#

set -euo pipefail

# Defined up front so every GitHub API call can use it, whether or not a token
# is set.  Expanded as "${AUTH_HEADER[@]+...}" because set -u rejects expanding
# an empty array on older bash.
AUTH_HEADER=()
if [ -n "${GITHUB_TOKEN:-}" ]; then
    AUTH_HEADER=(-H "Authorization: Bearer $GITHUB_TOKEN")
fi

TOPLEVEL="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
cd "$TOPLEVEL"

SUBMODULE_PATH="tools/cqlsh-rs"
GITHUB_REPO="scylladb/cqlsh-rs"
GITHUB_BASE="https://github.com/$GITHUB_REPO"
TARGET="${1:-origin/main}"

# ---------- validate submodule ----------
if [ ! -f "$SUBMODULE_PATH/.git" ] && [ ! -d "$SUBMODULE_PATH/.git" ]; then
    echo "ERROR: Submodule not initialized at $SUBMODULE_PATH" >&2
    echo "  Run: git submodule update --init $SUBMODULE_PATH" >&2
    exit 1
fi

# ---------- record old state ----------
OLD_SHA=$(git -C "$SUBMODULE_PATH" rev-parse HEAD)
OLD_SHORT=$(git -C "$SUBMODULE_PATH" rev-parse --short HEAD)
OLD_DESC=$(git -C "$SUBMODULE_PATH" log -1 --format='%s' HEAD)

echo "=== Updating cqlsh-rs submodule ==="
echo "  Current: $OLD_SHORT ($OLD_DESC)"
echo "  Target:  $TARGET"
echo

# ---------- fetch and checkout target ----------
git -C "$SUBMODULE_PATH" fetch --all --tags --quiet

# Resolve target to a full SHA
if git -C "$SUBMODULE_PATH" rev-parse --verify "$TARGET^{commit}" >/dev/null 2>&1; then
    NEW_SHA=$(git -C "$SUBMODULE_PATH" rev-parse "$TARGET^{commit}")
else
    echo "ERROR: Cannot resolve '$TARGET' to a commit in $SUBMODULE_PATH" >&2
    echo "  Available tags:" >&2
    git -C "$SUBMODULE_PATH" tag -l | sed 's/^/    /' >&2
    exit 1
fi

NEW_SHORT=$(git -C "$SUBMODULE_PATH" rev-parse --short "$NEW_SHA")

if [ "$OLD_SHA" = "$NEW_SHA" ]; then
    echo "Already at $NEW_SHORT — nothing to update."
    exit 0
fi

git -C "$SUBMODULE_PATH" checkout "$NEW_SHA" --quiet
NEW_DESC=$(git -C "$SUBMODULE_PATH" log -1 --format='%s' HEAD)

echo "  New:     $NEW_SHORT ($NEW_DESC)"
echo

# ---------- collect diff data ----------
echo ">>> Collecting changes between $OLD_SHORT..$NEW_SHORT"

# Git log between old and new (include PR numbers in parentheses if present)
CHANGELOG=$(git -C "$SUBMODULE_PATH" log --oneline --no-merges "$OLD_SHA..$NEW_SHA")
COMMIT_COUNT=$(echo "$CHANGELOG" | grep -c . || true)

# Diffstat
DIFFSTAT=$(git -C "$SUBMODULE_PATH" diff --stat "$OLD_SHA..$NEW_SHA")

# Check for Cargo.toml version change
OLD_VERSION=$(git -C "$SUBMODULE_PATH" show "$OLD_SHA:Cargo.toml" 2>/dev/null | grep '^version' | head -1 | sed 's/.*"\(.*\)"/\1/' || echo "unknown")
NEW_VERSION=$(git -C "$SUBMODULE_PATH" show "$NEW_SHA:Cargo.toml" 2>/dev/null | grep '^version' | head -1 | sed 's/.*"\(.*\)"/\1/' || echo "unknown")

echo
echo "  Commits: $COMMIT_COUNT"
echo "  Version: $OLD_VERSION -> $NEW_VERSION"
echo

# ---------- extract PR links from commit messages ----------
# GitHub squash/merge commits include "(#NNN)" in the subject line.
PR_SECTION=""
PR_NUMS=$(git -C "$SUBMODULE_PATH" log --no-merges --format='%s %b' "$OLD_SHA..$NEW_SHA" \
          | grep -oE '#[0-9]+' | tr -d '#' | sort -un || true)

if [ -n "$PR_NUMS" ]; then
    echo ">>> Resolving PR links..."
    PR_LINES=""
    while IFS= read -r pr; do
        PR_LINES="${PR_LINES}  ${GITHUB_BASE}/pull/${pr}"$'\n'
    done <<< "$PR_NUMS"
    PR_SECTION="Pull requests:

$PR_LINES"
    echo "$PR_LINES"
fi

# ---------- fetch GitHub release notes if target is a tag ----------
RELEASE_SECTION=""

# Determine whether $TARGET (or the resolved SHA) corresponds to a tag
RESOLVED_TAG=$(git -C "$SUBMODULE_PATH" tag --points-at "$NEW_SHA" 2>/dev/null | head -1 || true)

if [ -n "$RESOLVED_TAG" ]; then
    echo ">>> Tag $RESOLVED_TAG detected — fetching GitHub release notes..."

    if ! command -v curl &>/dev/null; then
        echo "  (curl not found — skipping GitHub release notes)"
    else
        RELEASE_JSON=$(curl -sf \
            ${AUTH_HEADER[@]+"${AUTH_HEADER[@]}"} \
            "https://api.github.com/repos/$GITHUB_REPO/releases/tags/$RESOLVED_TAG" \
            2>/dev/null || true)

        if [ -n "$RELEASE_JSON" ]; then
            # Extract the body; handle both jq and python fallback
            if command -v jq &>/dev/null; then
                RELEASE_BODY=$(echo "$RELEASE_JSON" | jq -r '.body // empty')
            else
                # Pure-bash fallback: extract "body" field with python3
                RELEASE_BODY=$(echo "$RELEASE_JSON" | python3 -c \
                    "import sys,json; d=json.load(sys.stdin); print(d.get('body',''))" \
                    2>/dev/null || true)
            fi

            if [ -n "$RELEASE_BODY" ]; then
                RELEASE_URL="${GITHUB_BASE}/releases/tag/$RESOLVED_TAG"
                RELEASE_SECTION="GitHub release notes ($RESOLVED_TAG):
$RELEASE_URL

$RELEASE_BODY"
                echo "  Release notes fetched ($(echo "$RELEASE_BODY" | wc -l) lines)"
            else
                echo "  Release $RESOLVED_TAG has no body on GitHub."
            fi
        else
            echo "  No GitHub release found for tag $RESOLVED_TAG (may be draft or missing)."
        fi
    fi
fi

# ---------- update the packaged binary pin ----------
# The package ships a pre-built release rather than building the submodule, so
# bumping the submodule alone would leave the two disagreeing and every package
# build failing on build_reloc.sh's drift guard.  Move the pin along with it.
VERSION_FILE="tools/cqlsh-rs-pkg/cqlsh-rs.version"
PIN_SECTION=""

echo ">>> Updating $VERSION_FILE for v$NEW_VERSION"

PIN_JSON=$(curl -sf ${AUTH_HEADER[@]+"${AUTH_HEADER[@]}"} \
    "https://api.github.com/repos/$GITHUB_REPO/releases/tags/v$NEW_VERSION" 2>/dev/null || true)

asset_digest() {
    # $1: rust target triple.  Prints the sha256 of that release asset.
    python3 -c '
import json, sys
name = f"cqlsh-rs-{sys.argv[1]}-{sys.argv[2]}.tar.gz"
try:
    release = json.load(sys.stdin)
except ValueError:
    sys.exit(1)
for asset in release.get("assets", []):
    if asset["name"] == name:
        print((asset.get("digest") or "").removeprefix("sha256:"))
        break
' "$NEW_VERSION" "$1" <<<"$PIN_JSON"
}

SHA_X86_64=$(asset_digest x86_64-unknown-linux-musl || true)
SHA_AARCH64=$(asset_digest aarch64-unknown-linux-musl || true)

if [ -n "$SHA_X86_64" ] && [ -n "$SHA_AARCH64" ]; then
    sed -i \
        -e "s/^cqlsh_rs_version=.*/cqlsh_rs_version=$NEW_VERSION/" \
        -e "s/^cqlsh_rs_sha256_x86_64=.*/cqlsh_rs_sha256_x86_64=$SHA_X86_64/" \
        -e "s/^cqlsh_rs_sha256_aarch64=.*/cqlsh_rs_sha256_aarch64=$SHA_AARCH64/" \
        "$VERSION_FILE"
    PIN_SECTION="Packaged binary pin:

  version: $OLD_VERSION -> $NEW_VERSION
  x86_64:  $SHA_X86_64
  aarch64: $SHA_AARCH64"
    echo "  Pinned v$NEW_VERSION with the digests published for it."
else
    echo "  WARNING: no linux-musl release assets found for v$NEW_VERSION." >&2
    echo "  $VERSION_FILE still pins $(sed -n 's/^cqlsh_rs_version=//p' "$VERSION_FILE")," >&2
    echo "  so package builds will fail the version drift check until a release" >&2
    echo "  for v$NEW_VERSION is published and the pin is updated by hand." >&2
fi

echo

# ---------- build commit message ----------
COMMIT_MSG="submodule: update cqlsh-rs to $NEW_SHORT"

if [ "$OLD_VERSION" != "$NEW_VERSION" ]; then
    COMMIT_MSG="$COMMIT_MSG ($OLD_VERSION -> $NEW_VERSION)"
fi

# Assemble body sections — skip empty ones
BODY_PARTS=()
BODY_PARTS+=("Update cqlsh-rs submodule from $OLD_SHORT to $NEW_SHORT.

cqlsh-rs version: $OLD_VERSION -> $NEW_VERSION")

BODY_PARTS+=("Changes ($COMMIT_COUNT commits):

$CHANGELOG")

[ -n "$PIN_SECTION" ]     && BODY_PARTS+=("$PIN_SECTION")
[ -n "$PR_SECTION" ]      && BODY_PARTS+=("$PR_SECTION")
[ -n "$RELEASE_SECTION" ] && BODY_PARTS+=("$RELEASE_SECTION")

BODY_PARTS+=("Diffstat:

$DIFFSTAT")

# Join sections with a blank line between each
COMMIT_BODY=""
for part in "${BODY_PARTS[@]}"; do
    if [ -n "$COMMIT_BODY" ]; then
        COMMIT_BODY="${COMMIT_BODY}

${part}"
    else
        COMMIT_BODY="$part"
    fi
done

# ---------- stage and show what we'll commit ----------
git add "$SUBMODULE_PATH" "$VERSION_FILE"

echo "=== Commit Preview ==="
echo
echo "$COMMIT_MSG"
echo
echo "$COMMIT_BODY"
echo
echo "======================"
echo

# ---------- confirm and commit ----------
read -p "Commit this update? [Y/n] " -n 1 -r
echo
if [[ $REPLY =~ ^[Nn]$ ]]; then
    echo "Aborted. Submodule is updated but not committed."
    echo "  To revert: git -C $SUBMODULE_PATH checkout $OLD_SHA"
    exit 0
fi

git commit -m "$(cat <<EOF
$COMMIT_MSG

$COMMIT_BODY
EOF
)"

echo
echo "Done. Committed cqlsh-rs update $OLD_SHORT -> $NEW_SHORT"
