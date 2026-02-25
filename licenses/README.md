## Note on licenses directory

The files in this directory represent licenses that apply to portions of
the work. See each source file for applicable licenses.

The work in whole is licensed under the ScyllaDB-Source-Available-1.0 license.
the LICENSE-ScyllaDB-Source-Available.md file in the top-level directory.

Individual files contain the following tag:

  SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

Some files are derived from Apache projects. These are dual-licensed
with the Apache License (version 2) and ScyllaDB-Source-Available-1.0.
They contain the following tag:

  SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)

### `musl libc` files

`licenses/musl-license.txt` is obtained from:
  https://git.musl-libc.org/cgit/musl/tree/COPYRIGHT

`utils/crypt_sha512.cc` is obtained from:
  https://git.musl-libc.org/cgit/musl/tree/src/crypt/crypt_sha512.c

Both files are obtained from git.musl-libc.org.
Import commit:
  commit 1b76ff0767d01df72f692806ee5adee13c67ef88
  Author: Alex Rønne Petersen <alex@alexrp.com>
  Date:   Sun Oct 12 05:35:19 2025 +0200

  s390x: shuffle register usage in __tls_get_offset to avoid r0 as address

musl as a whole is licensed under the standard MIT license included in
`licenses/musl-license.txt`.
