/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "utils/reservoir_sampling.hh"
#include "utils/updateable_value.hh"
#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/util/log.hh>

namespace utils {

extern seastar::logger dict_trainer_logger;

class alien_worker;

// A utility for training static compression dictionaries.
// 
// It's a combination of a reservoir sampler (utils::page_sampler) and a 3rd party
// training library (zstd).
class dict_sampler {
public:
    using page_type = std::vector<std::byte>;
    using dict_type = std::vector<std::byte>;
private:
    std::vector<page_type> _storage;
    size_t _bytes_remaining = 0;
    seastar::semaphore _min_bytes_satisifed{0};
    utils::page_sampler _page_sampler{0, 0, 0};
    bool _sampling = false;

    void reset() noexcept;
public:


    // Describes a config for sampling and training.
    struct request {
        // Sampling will last at least until this future resolves.
        seastar::future<> min_sampling_duration = seastar::make_ready_future<>();
        // Sampling will ingest at least this much before training.
        size_t min_sampling_bytes = 1024 * 1024 * 1024;
        // Shouldn't matter too much.
        size_t page_size = 8096;
        // A reasonable sample size is ~100x the dict size.
        size_t sample_size = 16 * 1024 * 1024;
    };

    // Begin sampling and training.
    // After enough time passes and enough data is input via `ingest()`,
    // the training library will be called on the gathered sample, and then the
    // returned future will resolve to the resulting dict.
    //
    // If the abort source is triggered before the min_sampling_bytes threshold is met,
    // the sampling will be canceled and the returned future will resolve to the aborting exception.
    // In reasonable use cases, min_sampling_duration should be abortable with the same abort source.
    seastar::future<std::vector<page_type>> sample(request, seastar::abort_source&);

    // When in the sampling phase, this will feed the data to the sampler.
    // Otherwise, it's a no-op.
    void ingest(std::span<const std::byte> x);

    bool is_sampling() { return _sampling; }
};

struct zdict_train_config {
    // 110 kiB is zstd's recommended default.
    size_t max_dict_size = 110 * 1024;
};
// Outside of tests, this should only be called on non-reactor threads. Calling it on a reactor thread will
// cause giant stalls.
dict_sampler::dict_type zdict_train(std::span<const dict_sampler::page_type> samples, zdict_train_config cfg);

class walltime_compressor_tracker;
class shared_dict;

class dict_training_loop {
    bool _paused = true;
    seastar::abort_source _cancelled;
    seastar::semaphore _pause{0};
    seastar::abort_source _pause_as;
public:
    struct when {
        enum class type {
            NEVER,
            WHEN_LEADER,
            ALWAYS,
            COUNT,
        };
        static constexpr std::string_view names[] = {
            "never",
            "when_leader",
            "always",
        };
        static_assert(std::size(names) == static_cast<size_t>(type::COUNT));
        // Implements enum_option.
        static std::unordered_map<std::string, type> map() {
            std::unordered_map<std::string, type> ret;
            for (size_t i = 0; i < std::size(names); ++i) {
                ret.insert({std::string(names[i]), type(i)});
            }
            return ret;
        }
    };
    void pause();
    void unpause();
    void cancel() noexcept;
    seastar::future<> start(
        dict_sampler&,
        std::function<seastar::future<>(dict_sampler::dict_type)> emit,
        utils::updateable_value<uint32_t> min_time_seconds,
        utils::updateable_value<uint64_t> min_bytes,
        utils::alien_worker&);
};

} // namespace utils
