/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/future.hh>
#include "compress.hh"
#include "gms/feature_service.hh"
#include "schema/schema_fwd.hh"

struct sstable_compressor_factory {
    virtual ~sstable_compressor_factory() {}
    virtual future<compressor_ptr> make_compressor_for_writing(schema_ptr) = 0;
    virtual future<compressor_ptr> make_compressor_for_reading(sstables::compression&) = 0;
    virtual future<> set_recommended_dict(table_id, std::span<const std::byte> dict) = 0;
};

std::unique_ptr<sstable_compressor_factory> make_sstable_compressor_factory(const gms::feature_service&);
