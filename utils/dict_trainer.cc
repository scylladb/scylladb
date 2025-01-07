/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/defer.hh>
#include <seastar/core/sleep.hh>
#include "utils/dict_trainer.hh"
#include "utils/alien_worker.hh"
#include "utils/shared_dict.hh"
#include "utils/hashers.hh"
#include "raft/raft.hh"
#include <zdict.h>

using namespace seastar;


namespace utils {

seastar::logger dict_trainer_logger("dict_training");

future<std::vector<dict_sampler::page_type>> dict_sampler::sample(request req, abort_source& as) {
    auto ensure_reset = defer([this] {
        dict_trainer_logger.debug("Sampling finished.");
        reset();
    });

    assert(!_sampling);
    assert(req.page_size);

    _storage.reserve(req.sample_size / req.page_size);
    _page_sampler = page_sampler(req.page_size, req.sample_size / req.page_size, /* hardcoded random seed */ 0);
    _bytes_remaining = req.min_sampling_bytes;
    _min_bytes_satisifed.signal(_bytes_remaining == 0);
    _sampling = true;
    dict_trainer_logger.debug("Sampling until the requested amount of time passes...");
    co_await std::move(req.min_sampling_duration);
    dict_trainer_logger.debug("Sampling until sampled data size threshold is met...");
    co_await _min_bytes_satisifed.wait(as);
    co_return std::move(_storage);
}

void dict_sampler::reset() noexcept {
    *this = dict_sampler();
}

void dict_sampler::ingest(std::span<const std::byte> x) {
    if (!_sampling) {
        return;
    }
    const size_t sz = x.size();
    while (x.size()) {
        if (auto cmd = _page_sampler.ingest_some(x)) {
            if (cmd->slot >= _storage.size()) {
                _storage.push_back(page_type(cmd->data.begin(), cmd->data.end()));
            } else {
                _storage[cmd->slot].assign(cmd->data.begin(), cmd->data.end());
            }
        }
    }
    auto bytes_remaining_before = _bytes_remaining;
   _bytes_remaining -= std::min(sz, _bytes_remaining);
    if (_bytes_remaining == 0 && bytes_remaining_before != 0) {
        _min_bytes_satisifed.signal();
    }
}

dict_sampler::dict_type zdict_train(std::span<const dict_sampler::page_type> samples, zdict_train_config cfg) {
    auto sample_sizes = std::vector<size_t>();
    sample_sizes.reserve(samples.size());
    for (const auto& sample : samples) {
        sample_sizes.push_back(sample.size());
    }

    auto input = std::vector<std::byte>();
    input.reserve(std::accumulate(sample_sizes.begin(), sample_sizes.end(), 0));
    for (const auto& sample : samples) {
        input.insert(input.end(), sample.begin(), sample.end());
    }

    auto ret = dict_sampler::dict_type(cfg.max_dict_size);
    auto dictsize = ZDICT_trainFromBuffer(ret.data(), ret.size(), input.data(), sample_sizes.data(), sample_sizes.size());
    if (ZDICT_isError(dictsize)) {
        const char* errname = ZDICT_getErrorName(dictsize);
        dict_trainer_logger.error("ZDICT_trainFromBuffer: {}", errname);
        throw std::runtime_error(fmt::format("ZDICT_trainFromBuffer: {}", errname));
    }

    ret.resize(dictsize);
    return ret;
}

void dict_training_loop::pause() {
    dict_trainer_logger.debug("dict_training_loop::pause(), called");
    if (!std::exchange(_paused, true)) {
        dict_trainer_logger.debug("dict_training_loop::pause(), pausing");
        _pause.consume();
        _pause_as.request_abort();
    }
}

void dict_training_loop::unpause() {
    dict_trainer_logger.debug("dict_training_loop::unpause(), called");
    if (std::exchange(_paused, false)) {
        dict_trainer_logger.debug("dict_training_loop::unpause(), unpausing");
        _pause.signal();
    }
}

void dict_training_loop::cancel() noexcept {
    dict_trainer_logger.debug("dict_training_loop::cancel(), called");
    _cancelled.request_abort();
    _pause_as.request_abort();
}

seastar::future<> dict_training_loop::start(
    dict_sampler& ds,
    std::function<future<>(dict_sampler::dict_type)> emit,
    utils::updateable_value<uint32_t> min_time_seconds,
    utils::updateable_value<uint64_t> min_bytes,
    utils::alien_worker& worker
) {
    dict_trainer_logger.debug("dict_training_loop::start(), called");
    std::default_random_engine rng(0);
    while (!_cancelled.abort_requested()) {
        try {
            _pause_as = seastar::abort_source();
            dict_trainer_logger.debug("dict_training_loop: waiting on _pause...");
            auto units = co_await get_units(_pause, 1, _cancelled);
            dict_trainer_logger.debug("dict_training_loop: sampling...");
            auto sample = co_await ds.sample({
                .min_sampling_duration = seastar::sleep_abortable(std::chrono::seconds(min_time_seconds), _pause_as),
                .min_sampling_bytes = min_bytes,
            }, _pause_as);
            dict_trainer_logger.debug("dict_training_loop: training...");
            // The order of samples coming from dict_sampler is unspecified.
            // In particular, they could have a correlation with time.
            // 
            // But the zdict trainer silently expects samples to be shuffled,
            // because of how it does its train-test split.
            //
            // It shouldn't matter in practice, but can matter in a synthetic test
            // with a small amount of training data.
            std::shuffle(sample.begin(), sample.end(), rng);
            auto dict_data = co_await worker.submit<dict_sampler::dict_type>([sample = std::move(sample)] {
                return zdict_train(sample, {});
            });
            dict_trainer_logger.debug("dict_training_loop: publishing...");
            co_await emit(dict_data);
            dict_trainer_logger.debug("dict_training_loop: published...");
        } catch (const raft::request_aborted&) {
            dict_trainer_logger.debug("dict_training_loop: raft aborted while publishing");
        } catch (...) {
            if (_cancelled.abort_requested()) {
                dict_trainer_logger.debug("dict_training_loop: cancelled");
            } else if (_paused) {
                dict_trainer_logger.debug("dict_training_loop: paused");
            } else  {
                dict_trainer_logger.error("Failed to train a dictionary: {}.", std::current_exception());
            }
        }
    }
}

static sha256_type get_sha256(std::span<const std::byte> in) {
    sha256_hasher hasher;
    hasher.update(reinterpret_cast<const char*>(in.data()), in.size());
    auto b = hasher.finalize();
    auto out = sha256_type();
    assert(b.size() == out.size());
    std::memcpy(&out, b.data(), b.size());
    return out;
}

shared_dict::shared_dict(std::span<const std::byte> d, uint64_t timestamp, UUID origin_node, int zstd_compression_level)
    : id{
        .timestamp = timestamp,
        .origin_node = origin_node,
        .content_sha256 = get_sha256(d)
    }
    , data(d.begin(), d.end())
    , zstd_ddict(ZSTD_createDDict_byReference(data.data(), data.size()), ZSTD_freeDDict)
    , zstd_cdict(ZSTD_createCDict_byReference(data.data(), data.size(), zstd_compression_level), ZSTD_freeCDict)
    , lz4_cdict(LZ4_createStream(), LZ4_freeStream)
{
    size_t lz4_dict_size = std::min<size_t>(data.size(), max_lz4_dict_size);
    lz4_ddict = std::span(data).last(lz4_dict_size);
    LZ4_loadDict(lz4_cdict.get(), reinterpret_cast<const char*>(lz4_ddict.data()), lz4_ddict.size());
    // Note: zstd dictionary builder puts the most valuable (frequent) samples
    // at the end of the buffer (to minimize the size of backreference offsets),
    // and it puts entropy tables (useless for lz4) at the front.
    //
    // So for lz4, which can only use dictionaries of size at most 64 kiB
    // we should take the last 64 kiB.
    lz4_ddict = std::span(data).last(lz4_dict_size);
}

} // namespace utils

