#!/bin/bash
#
# Copyright (C) 2026-present ScyllaDB
#
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Install antlr3 (the tool and the patched C++ runtime headers) into
# /usr/local. Fedora no longer ships the antlr3 C++ backend, so we take
# the tool's jar from Maven and the C++ runtime headers from the source
# tarball, with patches applied.
#
# This is run by install-dependencies.sh, but can also be run on its own
# (as root) by developers who want antlr3 without the rest of
# install-dependencies.sh.

set -euo pipefail

trap 'echo "error $? in $0 line $LINENO"' ERR

ANTLR3_VERSION=3.5.3
ANTLR3_JAR_URL="https://repo1.maven.org/maven2/org/antlr/antlr-complete/${ANTLR3_VERSION}/antlr-complete-${ANTLR3_VERSION}.jar"
ANTLR3_JAR_SHA256=e781de9b3e2cc1297dfdaf656da946a1fd22f449bd9e0ce1e12d488976887f83
ANTLR3_SOURCE_URL="https://github.com/antlr/antlr3/archive/${ANTLR3_VERSION}/antlr3-${ANTLR3_VERSION}.tar.gz"
ANTLR3_SOURCE_SHA256=a0892bcf164573d539b930e57a87ea45333141863a0dd3a49e5d8c919c8a58ab
# Patches from Fedora 43 (src.fedoraproject.org) that apply to the C++ headers
ANTLR3_PATCHES=(
    0006-antlr3memory.hpp-fix-for-C-20-mode.patch
    0008-unconst-cyclicdfa-gcc-14.patch
)

prefix=/usr/local
jardir="${prefix}/share/java"
bindir="${prefix}/bin"
includedir="${prefix}/include"

if [ -f "${jardir}/antlr-complete-${ANTLR3_VERSION}.jar" ] \
    && [ -f "${bindir}/antlr3" ] \
    && [ -f "${includedir}/antlr3.hpp" ]; then
    echo "antlr3 ${ANTLR3_VERSION} already installed, skipping"
    exit 0
fi

umask 0022

tmpdir=$(mktemp -d)

# Download and install the complete JAR
mkdir -p "${jardir}"
curl -fSL -o "${tmpdir}/antlr-complete-${ANTLR3_VERSION}.jar" "${ANTLR3_JAR_URL}"
echo "${ANTLR3_JAR_SHA256}  ${tmpdir}/antlr-complete-${ANTLR3_VERSION}.jar" | sha256sum --check
mv "${tmpdir}/antlr-complete-${ANTLR3_VERSION}.jar" "${jardir}/"

# Create the antlr3 wrapper script
mkdir -p "${bindir}"
cat > "${bindir}/antlr3" <<'WRAPPER'
#!/bin/bash
exec java -cp /usr/local/share/java/antlr-complete-ANTLR3_VERSION.jar org.antlr.Tool "$@"
WRAPPER
sed -i "s/ANTLR3_VERSION/${ANTLR3_VERSION}/" "${bindir}/antlr3"
chmod +x "${bindir}/antlr3"

# Download and extract the source for C++ headers
curl -fSL -o "${tmpdir}/antlr3-${ANTLR3_VERSION}.tar.gz" "${ANTLR3_SOURCE_URL}"
echo "${ANTLR3_SOURCE_SHA256}  ${tmpdir}/antlr3-${ANTLR3_VERSION}.tar.gz" | sha256sum --check
tar -xzf "${tmpdir}/antlr3-${ANTLR3_VERSION}.tar.gz" -C "${tmpdir}"

# Apply patches to C++ headers
srcdir="${tmpdir}/antlr3-${ANTLR3_VERSION}"
patchdir="$(dirname "$0")/../tools/antlr3-patches"
for patch in "${ANTLR3_PATCHES[@]}"; do
    patch -d "${srcdir}" -p1 < "${patchdir}/${patch}"
done

# Install C++ headers (header-only library)
mkdir -p "${includedir}"
install -m 644 "${srcdir}"/runtime/Cpp/include/* "${includedir}/"

rm -rf "${tmpdir}"
echo "antlr3 ${ANTLR3_VERSION} installed to ${prefix}"
