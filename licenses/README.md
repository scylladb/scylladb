## Note on licenses directory

The files in this directory represent licenses that apply to portions of
the work. See each source file for applicable licenses.

The work in whole is licensed under the ScyllaDB-Source-Available-1.0 license.
the LICENSE-ScyllaDB-Source-Available.md file in the top-level directory.

Individual files contain the following tag:

  SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

Some files are derived from Apache projects. These are dual-licensed
with the Apache License (version 2) and ScyllaDB-Source-Available-1.0.
They contain the following tag:

  SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)

### AWS SDK for C++ service models

The service-specific entries in `utils/s3/aws_error_definitions.{hh,cc}` are
generated from the S3 and STS `c2j` API models in aws/aws-sdk-cpp:
  https://github.com/aws/aws-sdk-cpp/tree/main/tools/code-generation/api-descriptions

Only the AWS error names, wire codes and retryability flags are carried over.
`scripts/gen_aws_service_errors.py` refreshes those entries from the current
models; the AWS models themselves are not committed here.

aws-sdk-cpp as a whole is licensed under the Apache License (version 2),
included in `licenses/apache-license-2.0.txt`. The two files are therefore
dual-licensed and carry:

  SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)

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
