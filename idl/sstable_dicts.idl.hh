/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

verb [[with_timeout, cancellable]] estimate_sstable_volume(table_id table) -> uint64_t;
verb [[with_timeout, cancellable]] sample_sstables(table_id table, uint64_t chunk_size, uint64_t n_chunks) -> utils::chunked_vector<temporary_buffer<char>>;
