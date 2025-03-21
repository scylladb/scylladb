/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/future.hh>
#include "compress.hh"
#include "schema/schema_fwd.hh"
#include "utils/updateable_value.hh"

struct sstable_compressor_factory {
    virtual ~sstable_compressor_factory() {}
    virtual future<compressor_ptr> make_compressor_for_writing(schema_ptr) = 0;
    virtual future<compressor_ptr> make_compressor_for_reading(sstables::compression&) = 0;
    virtual future<> set_recommended_dict(table_id, std::span<const std::byte> dict) = 0;
    struct config {
        bool register_metrics = false;
        utils::updateable_value<bool> enable_writing_dictionaries{true};
        utils::updateable_value<float> memory_fraction_starting_at_which_we_stop_writing_dicts{1};
    };
};

std::unique_ptr<sstable_compressor_factory> make_sstable_compressor_factory(sstable_compressor_factory::config cfg = {});
