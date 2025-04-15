/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "replica/database.hh"
#include "service/raft/raft_group0_client.hh"

// Responsible for re-training the SSTable compression dicts
// (for dict-aware tables) periodically.
//
// As of this writing, it works like this:
// every $tick_period (15 minutes), if we are the current Raft leader,
// we check for dict-aware tables which have no dict, or a dict older
// than $retrain_period.
// For those tables, if they have enough data (>1GiB) for a training,
// we train a new dict and check if it's significantly better
// than the current one (provides ratio smaller than 95% of current ratio),
// and if so, we update the dict.
class sstable_dict_autotrainer {
public:
    struct config {
        utils::updateable_value<float> tick_period_in_seconds;
        utils::updateable_value<float> retrain_period_in_seconds;
        utils::updateable_value<uint64_t> min_dataset_bytes;
        utils::updateable_value<float> min_improvement_factor;
    };
private:
    service::storage_service& _ss;
    service::raft_group0_client& _group0_client;
    config _cfg;
    abort_source _as;
    future<> _fiber;

    future<> tick();
    future<> run();
public:
    // Must be constructed and run on shard 0.
    sstable_dict_autotrainer(service::storage_service&, service::raft_group0_client&, config);
    future<> stop();
};

// Computes the compression ratio of the given compressor
// (provided by the `factory` based on `initial_schema` with overwritten `params`),
// on the given set of samples.
future<float> try_one_compression_config(
    sstable_compressor_factory& factory,
    schema_ptr initial_schema,
    const compression_parameters& params,
    const utils::chunked_vector<temporary_buffer<char>>& validation_samples
);

// Computes the compression ratio of the given compressor
// (provided by the `factory` based on `initial_schema` with overwritten `params`,
// and with recommended dict set to `dict`),
// on the given set of samples.
future<float> try_one_compression_config(
    std::span<std::byte> dict,
    schema_ptr initial_schema,
    const compression_parameters& params,
    const utils::chunked_vector<temporary_buffer<char>>& validation_samples
);
