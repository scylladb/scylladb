/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/condition-variable.hh>
#include <seastar/rpc/rpc_types.hh>
#include "utils/refcounted.hh"
#include "utils/updateable_value.hh"
#include "utils/enum_option.hh"

namespace utils {

class dict_sampler;
class lz4_cstream;
class lz4_dstream;
class zstd_cstream;
class zstd_dstream;
class stream_compressor;
class stream_decompressor;
class shared_dict;
using dict_ptr = lw_shared_ptr<foreign_ptr<lw_shared_ptr<shared_dict>>>;
class control_protocol_frame;

// An enum wrapper, describing supported RPC compression algorithms.
// Always contains a valid value —- the constructors won't allow
// an invalid/unknown enum variant to be constructed.
struct compression_algorithm {
    using underlying = uint8_t;
    enum class type : underlying {
        RAW,
        LZ4,
        ZSTD,
        COUNT,
    } _value;
    // Construct from an integer.
    // Used to deserialize the algorithm from the first byte of the frame.
    constexpr compression_algorithm(underlying x) {
        if (x < 0 || x >= static_cast<underlying>(type::COUNT)) {
            throw std::runtime_error(fmt::format("Invalid value {} for enum compression_algorithm", static_cast<int>(x)));
        }
        _value = static_cast<type>(x);
    }
    // Construct from `type`. Makes sure that `type` has a valid value.
    constexpr compression_algorithm(type x) : compression_algorithm(static_cast<underlying>(x)) {}

    // These names are used in multiple places:
    // RPC negotiation, in metric labels, and config.
    static constexpr std::string_view names[] = {
        "raw",
        "lz4",
        "zstd",
    };
    static_assert(std::size(names) == static_cast<int>(compression_algorithm::type::COUNT));

    // Implements enum_option.
    static auto map() {
        std::unordered_map<std::string, type> ret;
        for (size_t i = 0; i < std::size(names); ++i) {
            ret.insert(std::make_pair<std::string, type>(std::string(names[i]), compression_algorithm(i).get()));
        }
        return ret;
    }

    constexpr std::string_view name() const noexcept { return names[idx()]; }
    constexpr underlying idx() const noexcept { return static_cast<underlying>(_value); }
    constexpr type get() const noexcept { return _value; }
    constexpr static size_t count() { return static_cast<size_t>(type::COUNT); };
    bool operator<=>(const compression_algorithm &) const = default;
};


// Represents a set of compression algorithms.
// Backed by a bitset.
// Used for convenience during algorithm negotiations.
class compression_algorithm_set {
    uint8_t _bitset;
    static_assert(std::numeric_limits<decltype(_bitset)>::digits > compression_algorithm::count());
    constexpr compression_algorithm_set(uint8_t v) noexcept : _bitset(v) {}
public:
    // Returns a set containing the given algorithm and all algorithms weaker (smaller in the enum order)
    // than it.
    constexpr static compression_algorithm_set this_or_lighter(compression_algorithm algo) noexcept {
        auto x = 1 << (algo.idx());
        return {x + (x - 1)};
    }
    // Returns the strongest (greatest in the enum order) algorithm in the set.
    constexpr compression_algorithm heaviest() const {
        return {std::bit_width(_bitset) - 1};
    }
    // The usual set operations.
    constexpr static compression_algorithm_set singleton(compression_algorithm algo) noexcept {
        return {1 << algo.idx()};
    }
    constexpr compression_algorithm_set intersection(compression_algorithm_set o) const noexcept {
        return {_bitset & o._bitset};
    }
    constexpr compression_algorithm_set difference(compression_algorithm_set o) const noexcept {
        return {_bitset &~ o._bitset};
    }
    constexpr compression_algorithm_set sum(compression_algorithm_set o) const noexcept {
        return {_bitset | o._bitset};
    }
    constexpr bool contains(compression_algorithm algo) const noexcept {
        return _bitset & (1 << algo.idx());
    }
    constexpr bool operator==(const compression_algorithm_set&) const = default;
    // Returns the contained bitset. Used for serialization.
    constexpr uint8_t value() const noexcept {
        return _bitset;
    }
    // Reconstructs the set from the output of `value()`. Used for deserialization.
    constexpr static compression_algorithm_set from_value(uint8_t bitset) {
        compression_algorithm_set x = bitset;
        x.heaviest(); // This is a validation check. It will throw if the bitset contains some illegal/unknown bits.
        return x;
    }
};

using algo_config = std::vector<enum_option<compression_algorithm>>;

// See docs/dev/advanced_rpc_compression.md,
// section `Negotiation` for more information about the protocol.
struct control_protocol {
    // The sender increments its protocol epoch every time it proposes to commit to a different
    // algorithm.
    // The epoch is echoed back by the receiver to match proposals with accepts. 
    uint64_t _sender_protocol_epoch = 0;
    uint64_t _receiver_protocol_epoch = 0;

    // To send a control frame to the peer, we set one of these flags and signal _needs_progress.
    // This will cause at least one RPC message to be sent promptly. We prepend our frame to
    // the next RPC message.

    // These two flags are mutually exclusive.
    bool _sender_has_update = false;
    bool _sender_has_commit = false;
    // These two flags are mutually exclusive.
    bool _receiver_has_update = false;
    bool _receiver_has_commit = false;

    dict_ptr _sender_recent_dict = nullptr;
    dict_ptr _sender_committed_dict = nullptr;
    dict_ptr _sender_current_dict = nullptr;
    dict_ptr _receiver_recent_dict = nullptr;
    dict_ptr _receiver_committed_dict = nullptr;
    dict_ptr _receiver_current_dict = nullptr;
    compression_algorithm _sender_current_algo = compression_algorithm::type::RAW;
    compression_algorithm _sender_committed_algo = compression_algorithm::type::RAW;
    compression_algorithm_set _algos = compression_algorithm_set::singleton(compression_algorithm::type::RAW);

    // When signalled, an empty message will be sent over this connection soon.
    // Used to guarantee progress of algorithm negotiations.
    condition_variable& _needs_progress;
public:
    control_protocol(condition_variable&);
    // These functions handle the control (negotiation) protocol.
    std::optional<control_protocol_frame> produce_control_header();
    void consume_control_header(control_protocol_frame);
    void announce_dict(dict_ptr) noexcept;
    void set_supported_algos(compression_algorithm_set algos) noexcept;
    compression_algorithm sender_current_algorithm() const noexcept;
    const shared_dict& sender_current_dict() const noexcept; 
    const shared_dict& receiver_current_dict() const noexcept; 
};

class advanced_rpc_compressor final : public rpc::compressor {
public:
    class tracker;
    template <typename HighResClock, typename LowResClock> class tracker_with_clock;
private:
    // Pointer/reference to the tracker, which contains stats that we need to update,
    // and limits that we need to respect.
    //
    // The `refcounted` is just a precaution against a misuse of the APIs.
    refcounted::ref<tracker> _tracker;

    // Index of the compressor inside the tracker.
    // Used to unregister the compressor on destruction.
    size_t _idx = -1;

    // State of the negotiation protocol.
    control_protocol _control;

    // Used by _control to send its messages to other side of the connection.
    condition_variable _needs_progress;
    std::function<future<>()> _send_empty_frame;
    future<> _progress_fiber;

    // These return global compression contexts (for non-streaming compression modes), lazily initializing them.
    zstd_dstream& get_global_zstd_dstream();
    zstd_cstream& get_global_zstd_cstream();
    lz4_dstream& get_global_lz4_dstream();
    lz4_cstream& get_global_lz4_cstream();

    // Calls the appropriate get_*_cstream() function.
    stream_compressor& get_compressor(compression_algorithm);
    // Calls the appropriate get_*_dstream() function.
    stream_decompressor& get_decompressor(compression_algorithm);

    // Decides the algorithm used for the next message, based
    // on the state of the negotiation and the size of the message.
    compression_algorithm get_algo_for_next_msg(size_t msgsize);

    // Starts a worker fiber responsible for sending _control's messages.
    future<> start_progress_fiber();
public:
    advanced_rpc_compressor(
        tracker& fac,
        std::function<future<>()> send_empty_frame
    );
    ~advanced_rpc_compressor();

    // The public interface of rpc::compressor.
    rpc::snd_buf compress(size_t head_space, rpc::snd_buf data) override;
    rpc::rcv_buf decompress(rpc::rcv_buf data) override;
    sstring name() const override;
    future<> close() noexcept override;
};

// Tracker holds one of these for every compression mode/algorithm.
// They are used for displaying metrics, and for implementing CPU/memory usage limits.
struct per_algorithm_stats {
    uint64_t bytes_sent = 0;
    uint64_t compressed_bytes_sent = 0;
    uint64_t messages_sent = 0;
    uint64_t compression_cpu_nanos = 0;
    uint64_t bytes_received = 0;
    uint64_t compressed_bytes_received = 0;
    uint64_t messages_received = 0;
    uint64_t decompression_cpu_nanos = 0;
};

// The tracker contains everything which is shared between compressor instances:
// stats, metrics, limits, reusable non-streaming compressors.
//
// Class `tracker` itself contains clock-independent functionality.
// Clock-dependent functionality is split into `tracker_with_clock`, to minimize template pollution.
// Alternatively, we could wrap clocks into some virtual interface.
//
// Tracker is referenced by all compressors, so we inherit from `refcounted` to
// prevent a misuse of the API (dangling references).
class advanced_rpc_compressor::tracker : public refcounted {
public:
    using algo_config = algo_config;
    struct config {
        updateable_value<uint32_t> zstd_min_msg_size{0};
        updateable_value<uint32_t> zstd_max_msg_size{std::numeric_limits<uint32_t>::max()};
        updateable_value<float> zstd_quota_fraction{0};
        updateable_value<uint32_t> zstd_quota_refresh_ms{20};
        updateable_value<float> zstd_longterm_quota_fraction{1000};
        updateable_value<uint32_t> zstd_longterm_quota_refresh_ms{1000};
        updateable_value<algo_config> algo_config{{compression_algorithm::type::ZSTD, compression_algorithm::type::LZ4}};
        bool register_metrics = false;
        updateable_value<bool> checksumming{true};
    };
private:
    friend advanced_rpc_compressor;
    
    config _cfg;
    observer<algo_config> _algo_config_observer;

    std::array<per_algorithm_stats, compression_algorithm::count()> _stats;
    metrics::metric_groups _metrics;

    // Compression contexts for non-streaming compression modes.
    // They are shared by all compressors owned this tracker.
    std::unique_ptr<zstd_cstream> _global_zstd_cstream;
    std::unique_ptr<zstd_dstream> _global_zstd_dstream;
    std::unique_ptr<lz4_cstream> _global_lz4_cstream;
    std::unique_ptr<lz4_dstream> _global_lz4_dstream;
    std::vector<advanced_rpc_compressor*> _compressors;
    dict_ptr _most_recent_dict = nullptr;

    dict_sampler* _dict_sampler = nullptr;

    void register_metrics();
    void maybe_refresh_zstd_quota(uint64_t now) noexcept;
    bool cpu_limit_exceeded() const noexcept;
    uint64_t get_total_nanos_spent() const noexcept;

    zstd_dstream& get_global_zstd_dstream();
    zstd_cstream& get_global_zstd_cstream();
    lz4_dstream& get_global_lz4_dstream();
    lz4_cstream& get_global_lz4_cstream();

    void ingest(const rpc::snd_buf& data);
    void ingest(const rpc::rcv_buf& data);

    template <typename T>
    requires std::same_as<T, rpc::rcv_buf> || std::same_as<T, rpc::snd_buf>
    void ingest_generic(const T& data);

    size_t register_compressor(advanced_rpc_compressor*);
    void unregister_compressor(size_t);
public:
    tracker(config);
    virtual ~tracker();

    // Interface of rpc::compressor::factory.
    // `tracker` itself doesn't inherit from `factory` (just because this inheritance would have no users),
    // but a wrapper over `tracker` can use these to implement the interface.
    const sstring& supported() const;
    std::unique_ptr<advanced_rpc_compressor> negotiate(sstring feature, bool is_server, std::function<future<>()> send_empty_frame);
    std::span<const per_algorithm_stats, compression_algorithm::count()> get_stats() const noexcept;

    void announce_dict(dict_ptr);
    void attach_to_dict_sampler(dict_sampler*) noexcept;
    void set_supported_algos(compression_algorithm_set algos) noexcept;
protected:
    // These members are governed by `tracker_with_clock`.
    //
    // Why use nanos instead of Clock::duration?
    // Because that would require templating `factory_base` and `advanced_rpc_compressor` on `Clock`.
    // Forcing a common duration unit allows for encapsulation of clock-related details inside `tracker_with_clock`.
    virtual uint64_t get_steady_nanos() const = 0;

    // There are two CPU limit accounting periods: short period and long period.
    // Long period is multiple seconds and is meant to limit the throughput overhead.
    // Short period is a few several milliseconds and is meant to limit the latency ovehead.
    // Each period has a separate quota and we fall back to cheaper compression if any of
    // them is exceeded. 
    //
    // The long quota is periodically reset by a timer.
    // The short quota is periodically reset manually by the tracker, because the period is very short.
    // A timer with this period could generate unnecessary noise (e.g. keep waking up an otherwise-idle reactor).
    constexpr static std::chrono::nanoseconds long_period = std::chrono::seconds(10);
    uint64_t _short_period_start = 0;
    uint64_t _long_period_start = 0;
    uint64_t _nanos_used_before_this_short_period = 0;
    uint64_t _nanos_used_before_this_long_period = 0;
};

// Implements clock-dependent functionality for `tracker`.
template <typename HighResClock, typename LowResClock>
class advanced_rpc_compressor::tracker_with_clock : public advanced_rpc_compressor::tracker {
    virtual uint64_t get_steady_nanos() const override {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(HighResClock::now().time_since_epoch()).count();
    }
public:
    tracker_with_clock(config c)
        : advanced_rpc_compressor::tracker(std::move(c))
    {}
    // updateable_value must be created on the destination shard.
    // Since tracker is sharded, we can't copy the tracker::config (which contains updateable_value)
    // to all shards. But we can pass to all shards a function which will create the tracker::config. 
    tracker_with_clock(std::function<config()> f)
        : tracker_with_clock(f())
    {}
};

class walltime_compressor_tracker final : public utils::advanced_rpc_compressor::tracker_with_clock<std::chrono::steady_clock, lowres_clock> {
    using tracker_with_clock::tracker_with_clock;
};

// Helper for setting up the lw_shared_ptr<foreign_ptr<lw_shared_ptr<utils::shared_dict>>> tree
// used by the tracker to manage the lifetime of dicts.
future<> announce_dict_to_shards(seastar::sharded<walltime_compressor_tracker>&, utils::shared_dict);

} // namespace utils
