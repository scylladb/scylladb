/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/core/coroutine.hh>
#include <seastar/core/metrics.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>
#include <numeric>
#include "utils/small_vector.hh"
#include "utils/log.hh"
#include "bytes.hh"
#include "advanced_rpc_compressor.hh"
#include "advanced_rpc_compressor_protocol.hh"
#include "stream_compressor.hh"
#include "dict_trainer.hh"
#include <seastar/core/on_internal_error.hh>

namespace netw {

logging::logger arc_logger("advanced_rpc_compressor");

static const shared_dict null_dict;

control_protocol::control_protocol(condition_variable& cv)
    : _needs_progress(cv)
{
}

compression_algorithm control_protocol::sender_current_algorithm() const noexcept {
    return _sender_current_algo;
}

const shared_dict& control_protocol::sender_current_dict() const noexcept {
    return _sender_current_dict ? **_sender_current_dict : null_dict;
}

const shared_dict& control_protocol::receiver_current_dict() const noexcept {
    return _receiver_current_dict ? **_receiver_current_dict : null_dict;
}

dict_ptr control_protocol::sender_current_dict_ptr() const noexcept {
    return _sender_current_dict;
}

dict_ptr control_protocol::receiver_current_dict_ptr() const noexcept {
    return _receiver_current_dict;
}

static const shared_dict& deref_dict(const dict_ptr& d) noexcept {
    return d ? **d : null_dict;
}

static shared_dict::dict_id get_dict_id(dict_ptr d) {
    return d ? (**d).id : null_dict.id;
}

void control_protocol_frame::one_side::serialize(std::span<std::byte, serialized_size> out_span) {
    char* out = reinterpret_cast<char*>(out_span.data());
    seastar::write_le<uint8_t>(&out[0], header);
    seastar::write_le<uint64_t>(&out[1], epoch);
    seastar::write_le<uint8_t>(&out[9], algo.value());
    seastar::write_le<uint64_t>(&out[10], dict.origin_node.get_least_significant_bits());
    seastar::write_le<uint64_t>(&out[18], dict.origin_node.get_most_significant_bits());
    seastar::write_le<uint64_t>(&out[26], dict.timestamp);
    std::memcpy(&out[34], dict.content_sha256.data(), dict.content_sha256.size());
    static_assert(serialized_size == 66);
}

control_protocol_frame::one_side control_protocol_frame::one_side::deserialize(std::span<const std::byte, serialized_size> in_span) {
    const char* in = reinterpret_cast<const char*>(in_span.data());
    control_protocol_frame::one_side ret;
    ret.header = static_cast<header_enum>(seastar::read_le<uint8_t>(&in[0]));
    ret.epoch = seastar::read_le<uint64_t>(&in[1]);
    ret.algo = compression_algorithm_set::from_value(seastar::read_le<uint8_t>(&in[9]));
    ret.dict.origin_node = utils::UUID(seastar::read_le<uint64_t>(&in[18]), seastar::read_le<uint64_t>(&in[10]));
    ret.dict.timestamp = seastar::read_le<uint64_t>(&in[26]);
    std::memcpy(ret.dict.content_sha256.data(), &in[34], 32);
    static_assert(serialized_size == 66);
    return ret;
}

void control_protocol_frame::serialize(std::span<std::byte, serialized_size> out) {
    sender.serialize(out.subspan<0, one_side::serialized_size>());
    receiver.serialize(out.subspan<one_side::serialized_size, one_side::serialized_size>());
};

control_protocol_frame control_protocol_frame::deserialize(std::span<const std::byte, serialized_size> in) {
    control_protocol_frame pf;
    pf.sender = one_side::deserialize(in.subspan<0, one_side::serialized_size>());
    pf.receiver = one_side::deserialize(in.subspan<one_side::serialized_size, one_side::serialized_size>());
    return pf;
}

void control_protocol::announce_dict(dict_ptr d) noexcept {
    _sender_recent_dict = d;
    _sender_protocol_epoch += 1;
    _sender_has_update = true;
    _sender_has_commit = false;
    _receiver_recent_dict = d;
    _receiver_has_update = true;
    _receiver_has_commit = false;
    _needs_progress.signal();
}
void control_protocol::set_supported_algos(compression_algorithm_set algos) noexcept {
    _algos = algos;
    _sender_protocol_epoch += 1;
    _sender_has_update = true;
    _sender_has_commit = false;
    _receiver_has_update = true;
    _needs_progress.signal();
}

void control_protocol::consume_control_header(control_protocol_frame cpf) {
    if (cpf.receiver.header == control_protocol_frame::UPDATE) {
        _sender_protocol_epoch += 1;
        _sender_has_update = true;
        _sender_has_commit = false;
        _needs_progress.signal();
    } else if (cpf.receiver.header == control_protocol_frame::COMMIT && cpf.receiver.epoch == _sender_protocol_epoch) {
        _sender_has_commit = true;
        assert(!_sender_has_update);
        if (get_dict_id(_sender_committed_dict) != cpf.receiver.dict) {
            _sender_committed_dict = _sender_current_dict;
        }
        _sender_committed_algo = cpf.receiver.algo.intersection(_algos).heaviest();
        _needs_progress.signal();
    }
    if (cpf.sender.header == control_protocol_frame::UPDATE) {
        _receiver_has_commit = true;
        _receiver_has_update = false;
        if (cpf.sender.dict == get_dict_id(_receiver_recent_dict)) {
            _receiver_committed_dict = _receiver_recent_dict;
        }
        _receiver_protocol_epoch = cpf.sender.epoch;
        _needs_progress.signal();
    } else if (cpf.sender.header == control_protocol_frame::COMMIT) {
        if (cpf.sender.dict == get_dict_id(_receiver_committed_dict)) {
            _receiver_current_dict = _receiver_committed_dict;
        } else {
            assert(cpf.sender.dict == get_dict_id(_receiver_current_dict));
        }
    }
}

std::optional<control_protocol_frame> control_protocol::produce_control_header() {
    control_protocol_frame pf;
    if (!(_sender_has_commit || _sender_has_update || _receiver_has_commit || _receiver_has_update)) [[likely]] {
        return std::nullopt;
    }
    if (_sender_has_commit) {
        _sender_has_commit = false;
        assert(!_sender_has_update);
        _sender_current_dict = _sender_committed_dict;
        _sender_current_algo = _sender_committed_algo;
        pf.sender.header = control_protocol_frame::COMMIT;
        pf.sender.dict = get_dict_id(_sender_current_dict);
        pf.sender.algo = compression_algorithm_set::singleton(_sender_current_algo);
        pf.sender.epoch = _sender_protocol_epoch;
    } else if (_sender_has_update) {
        _sender_has_update = false;
        _sender_committed_dict = _sender_recent_dict;
        pf.sender.header = control_protocol_frame::UPDATE;
        pf.sender.dict = get_dict_id(_sender_recent_dict);
        pf.sender.algo = compression_algorithm_set::singleton(_sender_current_algo);
        pf.sender.epoch = _sender_protocol_epoch;
    }
    if (_receiver_has_commit) {
        _receiver_has_commit = false;
        pf.receiver.header = control_protocol_frame::COMMIT;
        pf.receiver.dict = get_dict_id(_receiver_committed_dict);
        pf.receiver.algo = _algos;
        pf.receiver.epoch = _receiver_protocol_epoch;
    } else if (_receiver_has_update) {
        _receiver_has_update = false;
        pf.receiver.header = control_protocol_frame::UPDATE;
        pf.receiver.dict = get_dict_id(_receiver_recent_dict);
        pf.receiver.algo = _algos;
        pf.receiver.epoch = _receiver_protocol_epoch;
    }
    return pf;
}

// Converting the list obtained from config.cc to a more workable form.
compression_algorithm_set algo_list_to_set(std::span<const enum_option<compression_algorithm>> v) {
    auto out = compression_algorithm_set::singleton(compression_algorithm::type::RAW);
    for (const auto& i : v) {
        out = out.sum(compression_algorithm_set::singleton(compression_algorithm(i)));
    }
    return out;
}

static raw_stream the_raw_stream;

advanced_rpc_compressor::advanced_rpc_compressor(
    tracker& fac,
    std::function<future<>()> send_empty_frame)
    : _tracker(fac)
    , _control(_needs_progress)
    , _send_empty_frame(std::move(send_empty_frame))
    , _progress_fiber(start_progress_fiber())
{
    _idx =_tracker->register_compressor(this);
}

future<> advanced_rpc_compressor::start_progress_fiber() {
    while (true) {
        co_await _needs_progress.when();
        co_await _send_empty_frame();
    }
}

future<> advanced_rpc_compressor::close() noexcept {
    _needs_progress.broken();
    return std::move(_progress_fiber).handle_exception([] (const auto& ep) {});
}

advanced_rpc_compressor::~advanced_rpc_compressor() {
    _tracker->unregister_compressor(_idx);
}

// Note: whenever a backwards-incompatible change to the compressor protocol/format
// is made, the COMPRESSOR_NAME has to change.
//
// SCYLLA_V4 is SCYLLA_V3 plus segmented messages. Only SCYLLA_V4 is supported,
// so connections to nodes which only support SCYLLA_V3 fall back to Seastar's LZ4_FRAGMENTED compressor.
const static sstring COMPRESSOR_NAME = "SCYLLA_V4";

compression_algorithm advanced_rpc_compressor::get_algo_for_next_segment(compression_algorithm negotiated, size_t msgsize) {
    auto algo = negotiated;
    if (algo == compression_algorithm::type::ZSTD
        && (_tracker->cpu_limit_exceeded()
            || msgsize < _tracker->_cfg.zstd_min_msg_size.get()
            || msgsize > _tracker->_cfg.zstd_max_msg_size.get())
    ) {
        algo = compression_algorithm::type::LZ4;
    }
    return algo;
}

sstring advanced_rpc_compressor::name() const {
    return COMPRESSOR_NAME;
}

const sstring& advanced_rpc_compressor::tracker::supported() const {
    return COMPRESSOR_NAME;
}

std::unique_ptr<advanced_rpc_compressor> advanced_rpc_compressor::tracker::negotiate(
    sstring feature,
    bool is_server,
    std::function<future<>()> send_empty_frame)
{
    if (feature != COMPRESSOR_NAME) {
        return nullptr;
    }
    auto c = std::make_unique<advanced_rpc_compressor>(*this, std::move(send_empty_frame));
    c->_control.set_supported_algos(algo_list_to_set(_cfg.algo_config.get()));
    c->_control.announce_dict(_most_recent_dict);
    return c;
}


advanced_rpc_compressor::tracker::tracker(config cfg)
    : _cfg(cfg)
    , _algo_config_observer(_cfg.algo_config.observe([this] (const auto& x) {
        set_supported_algos(algo_list_to_set(x));
    }))
{
    if (_cfg.register_metrics) {
        register_metrics();
    }
}

advanced_rpc_compressor::tracker::~tracker() {
}

void advanced_rpc_compressor::tracker::attach_to_dict_sampler(dict_sampler* dt) noexcept {
    _dict_sampler = dt;
}

void advanced_rpc_compressor::tracker::set_supported_algos(compression_algorithm_set algos) noexcept {
    for (const auto c : _compressors) {
        c->_control.set_supported_algos(algos);
    }
}

size_t advanced_rpc_compressor::tracker::register_compressor(advanced_rpc_compressor* c) {
    _compressors.push_back(c);
    c->_control.announce_dict(_most_recent_dict);
    return _compressors.size() - 1;
}

void advanced_rpc_compressor::tracker::unregister_compressor(size_t i) {
    assert(_compressors.size() && i < _compressors.size());
    std::swap(_compressors[i], _compressors.back());
    _compressors[i]->_idx = i;
    _compressors.pop_back();
}

void advanced_rpc_compressor::tracker::register_metrics() {
    namespace sm = seastar::metrics;
    sm::label algo_label("algorithm");
    for (int i = 0; i < static_cast<int>(compression_algorithm::type::COUNT); ++i) {
        auto stats = &_stats[i];
        auto label = algo_label(compression_algorithm(i).name());
        _metrics.add_group("rpc_compression", {
            sm::make_counter("bytes_sent", stats->bytes_sent, sm::description("bytes written to RPC connections, before compression"), {label}),
            sm::make_counter("compressed_bytes_sent", stats->compressed_bytes_sent, sm::description("bytes written to RPC connections, after compression"), {label}),
            sm::make_counter("compressed_bytes_received", stats->compressed_bytes_received, sm::description("bytes read from RPC connections, before decompression"), {label}),
            sm::make_counter("messages_received", stats->messages_received, sm::description("RPC messages received"), {label}),
            sm::make_counter("messages_sent", stats->messages_sent, sm::description("RPC messages sent"), {label}),
            sm::make_counter("bytes_received", stats->bytes_received, sm::description("bytes read from RPC connections, after decompression"), {label}),
            sm::make_counter("compression_cpu_nanos", stats->compression_cpu_nanos, sm::description("nanoseconds spent on compression"), {label}),
            sm::make_counter("decompression_cpu_nanos", stats->decompression_cpu_nanos, sm::description("nanoseconds spent on decompression"), {label}),
        });
    }
}

uint64_t advanced_rpc_compressor::tracker::get_total_nanos_spent() const noexcept {
    return _stats[static_cast<int>(compression_algorithm::type::ZSTD)].decompression_cpu_nanos
        + _stats[static_cast<int>(compression_algorithm::type::ZSTD)].compression_cpu_nanos
        + _stats[static_cast<int>(compression_algorithm::type::LZ4)].decompression_cpu_nanos
        + _stats[static_cast<int>(compression_algorithm::type::LZ4)].compression_cpu_nanos;
}

void advanced_rpc_compressor::tracker::maybe_refresh_zstd_quota(uint64_t now) noexcept {
    using std::chrono::nanoseconds, std::chrono::milliseconds;
    if (now >= _short_period_start + nanoseconds(milliseconds(_cfg.zstd_quota_refresh_ms)).count()) {
        _short_period_start = now;
        _nanos_used_before_this_short_period = get_total_nanos_spent();
    }
    if (now >= _long_period_start + nanoseconds(milliseconds(_cfg.zstd_longterm_quota_refresh_ms)).count()) {
        _long_period_start = now;
        _nanos_used_before_this_long_period = get_total_nanos_spent();
    }
}

bool advanced_rpc_compressor::tracker::cpu_limit_exceeded() const noexcept {
    using std::chrono::nanoseconds, std::chrono::milliseconds;
    uint64_t used_short = get_total_nanos_spent() - _nanos_used_before_this_short_period;
    uint64_t used_long = get_total_nanos_spent() - _nanos_used_before_this_long_period;
    uint64_t limit_short = nanoseconds(milliseconds(_cfg.zstd_quota_refresh_ms.get())).count() * _cfg.zstd_quota_fraction;
    uint64_t limit_long = nanoseconds(milliseconds(_cfg.zstd_longterm_quota_refresh_ms.get())).count() * _cfg.zstd_longterm_quota_fraction;
    return used_long >= limit_long || used_short >= limit_short;
}

std::span<const per_algorithm_stats, compression_algorithm::count()> advanced_rpc_compressor::tracker::get_stats() const noexcept {
    return _stats;
}

stream_compressor& advanced_rpc_compressor::get_compressor(compression_algorithm algo, const shared_dict& dict) {
    switch (algo.get()) {
    case compression_algorithm::type::LZ4: return get_global_lz4_cstream(dict);
    case compression_algorithm::type::ZSTD: return get_global_zstd_cstream(dict);
    case compression_algorithm::type::RAW: return the_raw_stream;
    default: __builtin_unreachable();
    }
}

stream_decompressor& advanced_rpc_compressor::get_decompressor(compression_algorithm algo, const shared_dict& dict) {
    switch (algo.get()) {
    case compression_algorithm::type::LZ4: return get_global_lz4_dstream(dict);
    case compression_algorithm::type::ZSTD: return get_global_zstd_dstream(dict);
    case compression_algorithm::type::RAW: return the_raw_stream;
    default: __builtin_unreachable();
    }
}

// The layout of the first byte of a segment.
// See docs/dev/advanced_rpc_compression.md, section `Wire protocol details`.
constexpr uint8_t segment_algo_mask = 0x1f;
constexpr uint8_t segment_flag_more = 0x20;
constexpr uint8_t segment_flag_checksum = 0x40;
constexpr uint8_t segment_flag_control_frame = 0x80;
static_assert(compression_algorithm::count() <= segment_algo_mask + 1);

struct advanced_rpc_compressor::outgoing_message {
    size_t size;
    // The control protocol only changes the sender's current algorithm and dict
    // in produce_control_header(), which is called once per message, before the first segment.
    // So all segments use the same values. (Except that the algorithm can be weakened
    // for some segments by get_algo_for_next_segment()).
    compression_algorithm negotiated_algo;
    dict_ptr dict;
    std::optional<control_protocol_frame> control_frame;
    bool checksumming;
};

struct advanced_rpc_compressor::incoming_message {
    // Set after the control frame (if any) of the first segment is consumed.
    dict_ptr dict;
};

advanced_rpc_compressor::outgoing_message advanced_rpc_compressor::start_outgoing_message(size_t msgsize) {
    // produce_control_header() can switch the sender to the newly committed dict and algorithm,
    // so they must be read after it.
    auto control_frame = _control.produce_control_header();
    return outgoing_message{
        .size = msgsize,
        .negotiated_algo = _control.sender_current_algorithm(),
        .dict = _control.sender_current_dict_ptr(),
        .control_frame = std::move(control_frame),
        .checksumming = _tracker->_cfg.checksumming.get(),
    };
}

rpc::snd_buf advanced_rpc_compressor::compress_segment(size_t head_space, snd_buf_cursor& in, outgoing_message& msg, bool first) {
    const size_t checksum_size = msg.checksumming ? sizeof(uint32_t) : 0;
    const bool has_control_frame = first && msg.control_frame;
    const size_t control_frame_size = has_control_frame ? control_protocol_frame::serialized_size : 0;
    // The length field is only present if more segments follow, and we only know that after the compression.
    // So we reserve space for it, and trim the space away if it turns out to be unneeded.
    const size_t length_size = sizeof(uint32_t);
    const size_t max_header_size = 1 + checksum_size + control_frame_size + length_size;

    auto now = _tracker->get_steady_nanos();
    _tracker->maybe_refresh_zstd_quota(now);

    const auto algo = get_algo_for_next_segment(msg.negotiated_algo, msg.size);
    auto& stats = _tracker->_stats[algo.idx()];

    const auto segment_start = in;
    rpc::snd_buf compressed;
    {
        auto update_time_stats = defer([&, nanos_before = now] noexcept {
            stats.compression_cpu_nanos += _tracker->get_steady_nanos() - nanos_before;
        });
        try {
            compressed = compress_impl(head_space + max_header_size, in, get_compressor(algo, deref_dict(msg.dict)), true, rpc::snd_buf::chunk_size, _tracker->_cfg.need_preempt);
        } catch (...) {
            arc_logger.error("Error during compression with algorithm {}: {:t}. ", algo.name(), std::current_exception());
            throw;
        }
        _tracker->ingest(segment_start, segment_start.remaining() - in.remaining());
    }
    const size_t segment_size = segment_start.remaining() - in.remaining();
    const bool more = in.remaining() > 0;
    const size_t header_size = max_header_size - (more ? 0 : length_size);

    // compress_impl guarantees that the head space (including our header) is in the first fragment,
    // so what we are doing below is legal.
    auto& front = compressed.front();
    front.trim_front(max_header_size - header_size);
    compressed.size -= max_header_size - header_size;

    // Write the header right after the external head_space.
    char* out = front.get_write() + head_space;
    *out++ = (algo.idx() & segment_algo_mask)
        | (more ? segment_flag_more : 0x00)
        | (checksum_size ? segment_flag_checksum : 0x00)
        | (has_control_frame ? segment_flag_control_frame : 0x00);
    if (checksum_size) {
        write_le<uint32_t>(out, crc_impl(segment_start, segment_size));
        out += sizeof(uint32_t);
    }
    if (has_control_frame) {
        constexpr size_t out_size = control_protocol_frame::serialized_size;
        msg.control_frame->serialize(std::span<std::byte, out_size>(reinterpret_cast<std::byte*>(out), out_size));
        out += out_size;
    }
    if (more) {
        write_le<uint32_t>(out, compressed.size - head_space - header_size);
    }

    stats.bytes_sent += segment_size;
    stats.compressed_bytes_sent += compressed.size - head_space;
    stats.messages_sent += first;
    return compressed;
}

// Concatenates the segments of a message, without copying them.
//
// Empty fragments are dropped. RPC reads the decompressed message through an input_stream,
// which treats an empty fragment as the end of the stream, so an empty fragment
// must not precede the next segment.
template <typename Buf>
requires std::same_as<Buf, rpc::rcv_buf> || std::same_as<Buf, rpc::snd_buf>
class segment_concatenator {
    std::vector<temporary_buffer<char>> _frags;
    size_t _size = 0;
    void append_fragment(temporary_buffer<char> frag) {
        if (!frag.empty()) {
            _frags.push_back(std::move(frag));
        }
    }
public:
    void append(Buf b) {
        _size += b.size;
        if (auto* single = std::get_if<temporary_buffer<char>>(&b.bufs)) {
            append_fragment(std::move(*single));
        } else {
            for (auto& frag : std::get<std::vector<temporary_buffer<char>>>(b.bufs)) {
                append_fragment(std::move(frag));
            }
        }
    }
    Buf finish() && {
        return Buf(std::move(_frags), _size);
    }
};

rpc::snd_buf advanced_rpc_compressor::compress(size_t head_space, rpc::snd_buf data) {
    auto msg = start_outgoing_message(data.size);
    auto in = snd_buf_cursor(data);
    auto first_segment = compress_segment(head_space, in, msg, true);
    if (in.remaining() == 0) {
        return first_segment;
    }
    segment_concatenator<rpc::snd_buf> out;
    out.append(std::move(first_segment));
    while (in.remaining() > 0) {
        out.append(compress_segment(0, in, msg, false));
    }
    return std::move(out).finish();
}

future<rpc::snd_buf> advanced_rpc_compressor::compress_async(size_t head_space, rpc::snd_buf data) {
    // Most messages fit in one segment, so the first segment is compressed
    // outside of a coroutine, to avoid allocating a coroutine frame for them.
    try {
        auto msg = start_outgoing_message(data.size);
        auto in = snd_buf_cursor(data);
        auto first_segment = compress_segment(head_space, in, msg, true);
        if (in.remaining() == 0) {
            return make_ready_future<rpc::snd_buf>(std::move(first_segment));
        }
        const size_t consumed = data.size - in.remaining();
        return compress_remaining_segments(std::move(data), consumed, std::move(msg), std::move(first_segment));
    } catch (...) {
        return current_exception_as_future<rpc::snd_buf>();
    }
}

future<rpc::snd_buf> advanced_rpc_compressor::compress_remaining_segments(rpc::snd_buf data, size_t consumed, outgoing_message msg, rpc::snd_buf first_segment) {
    // The cursor of the caller points into the moved-from `data`, so we need a new one.
    auto in = snd_buf_cursor(data);
    while (consumed > 0) {
        consumed -= in.next(consumed).size();
    }
    // Each segment is a complete frame, so no compression context has to be kept
    // across the preemption points. Other connections can use the shared contexts meanwhile.
    segment_concatenator<rpc::snd_buf> out;
    out.append(std::move(first_segment));
    while (in.remaining() > 0) {
        if (_tracker->_cfg.need_preempt()) {
            co_await yield();
        }
        out.append(compress_segment(0, in, msg, false));
    }
    co_return std::move(out).finish();
}

// Reads an rcv_buf from the front. Doesn't copy the payload.
class rcv_buf_reader {
    utils::small_vector<temporary_buffer<char>, 4> _frags;
    size_t _idx = 0;
    size_t _remaining;
public:
    explicit rcv_buf_reader(rpc::rcv_buf data) : _remaining(data.size) {
        if (auto* single = std::get_if<temporary_buffer<char>>(&data.bufs)) {
            _frags.push_back(std::move(*single));
        } else {
            for (auto& frag : std::get<std::vector<temporary_buffer<char>>>(data.bufs)) {
                _frags.push_back(std::move(frag));
            }
        }
    }

    size_t remaining() const noexcept {
        return _remaining;
    }

    template <typename T>
    requires std::is_trivially_copyable_v<T>
    T read() {
        if (_remaining < sizeof(T)) {
            throw std::runtime_error("Truncated compressed RPC frame");
        }
        std::array<T, 1> out;
        auto out_span = std::as_writable_bytes(std::span(out)).subspan(0);
        while (out_span.size()) {
            auto& frag = _frags[_idx];
            if (frag.empty()) {
                ++_idx;
                continue;
            }
            size_t n = std::min<size_t>(out_span.size(), frag.size());
            std::memcpy(static_cast<void*>(out_span.data()), frag.get(), n);
            out_span = out_span.subspan(n);
            frag.trim_front(n);
            _remaining -= n;
        }
        return out[0];
    }

    // Returns the next `n` bytes.
    rpc::rcv_buf read_buf(size_t n) {
        if (_remaining < n) {
            throw std::runtime_error("Truncated compressed RPC frame");
        }
        _remaining -= n;
        utils::small_vector<temporary_buffer<char>, 1> pieces;
        for (size_t left = n; left > 0;) {
            auto& frag = _frags[_idx];
            if (frag.size() <= left) {
                left -= frag.size();
                if (!frag.empty()) {
                    pieces.push_back(std::move(frag));
                }
                ++_idx;
            } else {
                pieces.push_back(frag.share(0, left));
                frag.trim_front(left);
                left = 0;
            }
        }
        if (pieces.size() == 0) {
            return rpc::rcv_buf();
        } else if (pieces.size() == 1) {
            return rpc::rcv_buf(std::move(pieces.front()));
        }
        return rpc::rcv_buf({std::make_move_iterator(pieces.begin()), std::make_move_iterator(pieces.end())}, n);
    }
};

static std::string format_dict_id(const shared_dict::dict_id& id) {
    return fmt::format("{{timestamp={}, origin_node={}, content_sha256={}}}", id.timestamp, id.origin_node, fmt_hex(id.content_sha256));
}

static std::string format_dict_ptr(const dict_ptr& d) {
    if (!d) {
        return "null";
    }
    const auto& id = (**d).id;
    return format_dict_id(id);
}

static std::string format_control_protocol(const control_protocol& cp) {
    return fmt::format(
        "{{sender_protocol_epoch={}, receiver_protocol_epoch={}"
        ", sender_has_update={}, sender_has_commit={}"
        ", receiver_has_update={}, receiver_has_commit={}"
        ", sender_recent_dict={}, sender_committed_dict={}, sender_current_dict={}"
        ", receiver_recent_dict={}, receiver_committed_dict={}, receiver_current_dict={}"
        ", sender_current_algo={}, sender_committed_algo={}, algos={:#04x}}}",
        cp._sender_protocol_epoch, cp._receiver_protocol_epoch,
        cp._sender_has_update, cp._sender_has_commit,
        cp._receiver_has_update, cp._receiver_has_commit,
        format_dict_ptr(cp._sender_recent_dict), format_dict_ptr(cp._sender_committed_dict), format_dict_ptr(cp._sender_current_dict),
        format_dict_ptr(cp._receiver_recent_dict), format_dict_ptr(cp._receiver_committed_dict), format_dict_ptr(cp._receiver_current_dict),
        cp._sender_current_algo.name(), cp._sender_committed_algo.name(), cp._algos.value());
}

std::pair<rpc::rcv_buf, bool> advanced_rpc_compressor::decompress_segment(rcv_buf_reader& in, incoming_message& msg, bool first) {
    const uint8_t header_byte = in.read<uint8_t>();
    const bool more = header_byte & segment_flag_more;
    const bool has_checksum = header_byte & segment_flag_checksum;
    const bool has_control_frame = header_byte & segment_flag_control_frame;

    uint32_t expected_crc = -1;
    if (has_checksum) {
        expected_crc = seastar::le_to_cpu(in.read<uint32_t>());
    }

    if (has_control_frame) {
        if (!first) {
            throw std::runtime_error("Control frame in a non-first segment of a compressed RPC frame");
        }
        auto control_protocol_frame_bytes = in.read<std::array<std::byte, control_protocol_frame::serialized_size>>();
        _control.consume_control_header(control_protocol_frame::deserialize(control_protocol_frame_bytes));
    }
    if (first) {
        // consume_control_header() can switch the receiver to a new dict, so this must happen after it.
        msg.dict = _control.receiver_current_dict_ptr();
    }

    const size_t compressed_size = more ? seastar::le_to_cpu(in.read<uint32_t>()) : in.remaining();
    const auto compressed = in.read_buf(compressed_size);

    // Will throw if the enum value is unknown.
    auto algo = compression_algorithm(header_byte & segment_algo_mask);

    auto& stats = _tracker->_stats[algo.idx()];
    auto update_time_stats = defer([&, nanos_before = _tracker->get_steady_nanos()] noexcept {
        stats.decompression_cpu_nanos += _tracker->get_steady_nanos() - nanos_before;
    });
    auto decompressed = std::invoke([&] {
        try {
            return decompress_impl(compressed, get_decompressor(algo, deref_dict(msg.dict)), true, rpc::snd_buf::chunk_size);
         } catch (...) {
            arc_logger.error("Error during decompression with algorithm {}: {:t}. ", algo.name(), std::current_exception());
            throw;
        }
    });
    if (has_checksum) {
        const uint32_t actual_crc = crc_impl(decompressed);
        if (expected_crc != actual_crc) [[unlikely]] {
            arc_logger.error(
                "RPC compression checksum error details:"
                " expected_crc={:#010x}, actual_crc={:#010x}"
                ", header_byte={:#04x}, first_segment={}"
                ", control_protocol={}"
                ", compressed_size={}, decompressed_size={}",
                expected_crc, actual_crc,
                header_byte, first,
                format_control_protocol(_control),
                compressed_size, decompressed.size);
            seastar::on_internal_error(arc_logger, fmt::format("RPC compression checksum error (expected: {:x}, got: {:x}). This indicates a bug. Set `internode_compression: none` and restart the nodes to regain stability, then report the bug.", expected_crc, actual_crc));
        }
    }
    _tracker->ingest(decompressed);
    stats.compressed_bytes_received += compressed_size;
    stats.bytes_received += decompressed.size;
    stats.messages_received += first;
    return {std::move(decompressed), more};
}

rpc::rcv_buf advanced_rpc_compressor::decompress(rpc::rcv_buf data) {
    auto in = rcv_buf_reader(std::move(data));
    auto msg = incoming_message();
    auto [first_segment, more] = decompress_segment(in, msg, true);
    if (!more) {
        return std::move(first_segment);
    }
    segment_concatenator<rpc::rcv_buf> out;
    out.append(std::move(first_segment));
    while (more) {
        rpc::rcv_buf segment;
        std::tie(segment, more) = decompress_segment(in, msg, false);
        out.append(std::move(segment));
    }
    return std::move(out).finish();
}

future<rpc::rcv_buf> advanced_rpc_compressor::decompress_async(rpc::rcv_buf data) {
    // Most messages fit in one segment, so the first segment is decompressed
    // outside of a coroutine, to avoid allocating a coroutine frame for them.
    try {
        auto in = rcv_buf_reader(std::move(data));
        auto msg = incoming_message();
        auto [first_segment, more] = decompress_segment(in, msg, true);
        if (!more) {
            return make_ready_future<rpc::rcv_buf>(std::move(first_segment));
        }
        return decompress_remaining_segments(std::move(in), std::move(msg), std::move(first_segment));
    } catch (...) {
        return current_exception_as_future<rpc::rcv_buf>();
    }
}

future<rpc::rcv_buf> advanced_rpc_compressor::decompress_remaining_segments(rcv_buf_reader in, incoming_message msg, rpc::rcv_buf first_segment) {
    segment_concatenator<rpc::rcv_buf> out;
    out.append(std::move(first_segment));
    bool more = true;
    while (more) {
        if (_tracker->_cfg.need_preempt()) {
            co_await yield();
        }
        rpc::rcv_buf segment;
        std::tie(segment, more) = decompress_segment(in, msg, false);
        out.append(std::move(segment));
    }
    co_return std::move(out).finish();
}

zstd_dstream& advanced_rpc_compressor::get_global_zstd_dstream(const shared_dict& dict) {
    auto& dstream = _tracker->get_global_zstd_dstream();
    dstream.set_dict(dict.zstd_ddict.get());
    return dstream;
}

zstd_cstream& advanced_rpc_compressor::get_global_zstd_cstream(const shared_dict& dict) {
    auto& cstream = _tracker->get_global_zstd_cstream();
    cstream.set_dict(dict.zstd_cdict.get());
    return cstream;
}

lz4_dstream& advanced_rpc_compressor::get_global_lz4_dstream(const shared_dict& dict) {
    auto& dstream = _tracker->get_global_lz4_dstream();
    dstream.set_dict(dict.lz4_ddict);
    return dstream;
}

lz4_cstream& advanced_rpc_compressor::get_global_lz4_cstream(const shared_dict& dict) {
    auto& cstream = _tracker->get_global_lz4_cstream();
    cstream.set_dict(dict.lz4_cdict.get());
    return cstream;
}

zstd_dstream& advanced_rpc_compressor::tracker::get_global_zstd_dstream() {
    if (!_global_zstd_dstream) {
        _global_zstd_dstream = std::make_unique<zstd_dstream>();
    }
    return *_global_zstd_dstream;
}

zstd_cstream& advanced_rpc_compressor::tracker::get_global_zstd_cstream() {
    if (!_global_zstd_cstream) {
        _global_zstd_cstream = std::make_unique<zstd_cstream>();
    }
    return *_global_zstd_cstream;
}

lz4_dstream& advanced_rpc_compressor::tracker::get_global_lz4_dstream() {
    if (!_global_lz4_dstream) {
        _global_lz4_dstream = std::make_unique<lz4_dstream>(); 
    }
    return *_global_lz4_dstream;
}

lz4_cstream& advanced_rpc_compressor::tracker::get_global_lz4_cstream() {
    if (!_global_lz4_cstream) {
        _global_lz4_cstream = std::make_unique<lz4_cstream>();
    }
    return *_global_lz4_cstream;
}

void advanced_rpc_compressor::tracker::ingest(snd_buf_cursor from, size_t size) {
    if (_dict_sampler && _dict_sampler->is_sampling()) {
        while (size > 0) {
            const auto piece = from.next(size);
            _dict_sampler->ingest(std::as_bytes(piece));
            size -= piece.size();
        }
    }
}

void advanced_rpc_compressor::tracker::ingest(const rpc::rcv_buf& data) {
    if (_dict_sampler && _dict_sampler->is_sampling()) {
        if (const auto* src = std::get_if<temporary_buffer<char>>(&data.bufs)) {
            _dict_sampler->ingest({reinterpret_cast<const std::byte*>(src->get()), src->size()});
        } else {
            const auto& frags = std::get<std::vector<temporary_buffer<char>>>(data.bufs);
            for (const auto& frag : frags) {
                _dict_sampler->ingest({reinterpret_cast<const std::byte*>(frag.get()), frag.size()});
            }
        }
    }
}

void advanced_rpc_compressor::tracker::announce_dict(dict_ptr d) {
    _most_recent_dict = d;
    for (const auto c : _compressors) {
        c->_control.announce_dict(_most_recent_dict);
    }
}

future<> announce_dict_to_shards(seastar::sharded<walltime_compressor_tracker>& sharded_tracker, shared_dict shared_dict) {
    arc_logger.debug("Announcing new dictionary: ts={}, origin={}", shared_dict.id.timestamp, shared_dict.id.origin_node);
    auto dict = make_lw_shared(std::move(shared_dict));
    auto foreign_ptrs = std::vector<foreign_ptr<decltype(dict)>>();
    for (size_t i = 0; i < this_smp_shard_count(); ++i) {
        foreign_ptrs.push_back(make_foreign(dict));
    }
    co_await sharded_tracker.invoke_on_all([&foreign_ptrs] (auto& tracker) {
        tracker.announce_dict(make_lw_shared(std::move(foreign_ptrs[this_shard_id()])));
    });
}

} // namespace netw
