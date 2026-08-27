/*
 * Copyright (C) 2026-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/shared_ptr.hh>

namespace encryption {

class encryption_context;

// Wires Parquet Modular Encryption up to this module's key providers, so a `pq` table can name
// any of them -- local file, replicated, KMIP, AWS KMS, GCP, Azure -- and therefore so BYOK
// works. Installs a sstables::parquet::key_source for the whole process; see
// sstables/parquet/encryption_keys.hh for why that indirection exists (sstables cannot depend on
// this library, because this library depends on sstables).
//
// Called from register_extensions, which is the one place the context is known to exist and which
// runs on every path that can open an sstable, including cql_test_env and the offline tools --
// unlike main.cc, which the tools do not go through.
void register_parquet_key_source(seastar::shared_ptr<encryption_context>);

}
