/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/metrics.hh>
#include <seastar/util/defer.hh>
#include <numeric>
#include "log.hh"
#include "utils/advanced_rpc_compressor.hh"
#include "utils/advanced_rpc_compressor_protocol.hh"
#include "stream_compressor.hh"
#include "utils/dict_trainer.hh"
#include "seastar/core/on_internal_error.hh"

namespace utils {

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
    ret.dict.origin_node = UUID(seastar::read_le<uint64_t>(&in[18]), seastar::read_le<uint64_t>(&in[10]));
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
const static sstring COMPRESSOR_NAME = "SCYLLA_V3";

compression_algorithm advanced_rpc_compressor::get_algo_for_next_msg(size_t msgsize) {
    auto algo = _control.sender_current_algorithm();
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

stream_compressor& advanced_rpc_compressor::get_compressor(compression_algorithm algo) {
    switch (algo.get()) {
    case compression_algorithm::type::LZ4: return get_global_lz4_cstream();
    case compression_algorithm::type::ZSTD: return get_global_zstd_cstream();
    case compression_algorithm::type::RAW: return the_raw_stream;
    default: __builtin_unreachable();
    }
}

stream_decompressor& advanced_rpc_compressor::get_decompressor(compression_algorithm algo) {
    switch (algo.get()) {
    case compression_algorithm::type::LZ4: return get_global_lz4_dstream();
    case compression_algorithm::type::ZSTD: return get_global_zstd_dstream();
    case compression_algorithm::type::RAW: return the_raw_stream;
    default: __builtin_unreachable();
    }
}

rpc::snd_buf advanced_rpc_compressor::compress(size_t head_space, rpc::snd_buf data) {
    const size_t checksum_size = _tracker->_cfg.checksumming.get() ? sizeof(uint32_t) : 0;
    const uint32_t crc = checksum_size ? crc_impl(data) : -1;

    auto now = _tracker->get_steady_nanos();
    _tracker->maybe_refresh_zstd_quota(now);

    auto algo = get_algo_for_next_msg(data.size);

    auto& stats = _tracker->_stats[algo.idx()];
    auto update_time_stats = defer([&, nanos_before = now] {
        stats.compression_cpu_nanos += _tracker->get_steady_nanos() - nanos_before;
    });

    _tracker->ingest(data);

    auto protocol_header = _control.produce_control_header();
    const size_t protocol_header_size = protocol_header ? control_protocol_frame::serialized_size : 0;

    auto uncompressed_size = data.size;
    auto compressed = std::invoke([&] {
        try {
            return compress_impl(head_space + 1 + checksum_size + protocol_header_size, std::move(data), get_compressor(algo), true, rpc::snd_buf::chunk_size);
        } catch (...) {
            arc_logger.error("Error during decompression with algorithm {}: {}. ", algo.name(), std::current_exception());
            throw;
        }
    });

    // Write the algorithm type to the first byte after the external head_space.
    // Note: compress_impl guarantees that the head space (including our byte, as we passed head_space + 1) is in the first fragment,
    // so what we are doing below is legal.
    auto dst = std::get_if<temporary_buffer<char>>(&compressed.bufs);
    if (!dst) {
        dst = std::get<std::vector<temporary_buffer<char>>>(compressed.bufs).data();
    }
    static_assert(compression_algorithm::count() <= 0x3f); // We have 6 bits for algorithm ID, 2 bits for flags.
    dst->get_write()[head_space] = (algo.idx() & 0x3f) | (protocol_header ? 0x80 : 0x00) | (checksum_size ? 0x40 : 0x00);
    if (checksum_size) {
        write_le<uint32_t>(&dst->get_write()[head_space + 1], crc);
    }
    if (protocol_header) {
        auto out_data = reinterpret_cast<std::byte*>(dst->get_write() + head_space + 1 + checksum_size);
        constexpr size_t out_size = control_protocol_frame::serialized_size;
        auto out = std::span<std::byte, out_size>(out_data, out_size);
        protocol_header->serialize(out);
    }

    stats.bytes_sent += uncompressed_size;
    stats.compressed_bytes_sent += compressed.size - head_space;
    stats.messages_sent += 1;
    return compressed;
}

template <typename T>
requires std::is_trivially_copyable_v<T>
T read_from_rcv_buf(rpc::rcv_buf& data) {
    if (data.size < sizeof(T)) {
        throw std::runtime_error("Truncated compressed RPC frame");
    }
    auto it = std::get_if<temporary_buffer<char>>(&data.bufs);
    if (!it) {
        it = std::get<std::vector<temporary_buffer<char>>>(data.bufs).data();
    }
    std::array<T, 1> out;
    auto out_span = std::as_writable_bytes(std::span(out)).subspan(0);
    while (out_span.size()) {
        size_t n = std::min<size_t>(out_span.size(), it->size());
        // Make a special case for n==0, to avoid calling memcpy(src=..., it->get()=nullptr, n=0). The nullptr bothers UBSAN.
        if (n) {
            std::memcpy(static_cast<void*>(out_span.data()), it->get(), n);
            out_span = out_span.subspan(n);
            it->trim_front(n);
            data.size -= n;
        }
        ++it;
    }
    return out[0];
}

rpc::rcv_buf advanced_rpc_compressor::decompress(rpc::rcv_buf data) {
    const uint8_t header_byte = read_from_rcv_buf<uint8_t>(data);
    const bool has_checksum = header_byte & 0x40;
    const bool has_control_frame = header_byte & 0x80;

    uint32_t expected_crc = -1;
    if (has_checksum) {
        expected_crc = seastar::le_to_cpu(read_from_rcv_buf<uint32_t>(data));
    }

    if (has_control_frame) {
        auto control_protocol_frame_bytes = read_from_rcv_buf<std::array<std::byte, control_protocol_frame::serialized_size>>(data);
        _control.consume_control_header(control_protocol_frame::deserialize(control_protocol_frame_bytes));
    }

    // Will throw if the enum value is unknown.
    auto algo = compression_algorithm(header_byte & 0x3f);

    auto& stats = _tracker->_stats[algo.idx()];
    auto update_time_stats = defer([&, nanos_before = _tracker->get_steady_nanos()] {
        stats.decompression_cpu_nanos += _tracker->get_steady_nanos() - nanos_before;
    });
    auto compressed_size = data.size;
    auto decompressed = std::invoke([&] {
        try {
            return decompress_impl(data, get_decompressor(algo), true, rpc::snd_buf::chunk_size);
         } catch (...) {
            arc_logger.error("Error during compression with algorithm {}: {}. ", algo.name(), std::current_exception());
            throw;
        }
    });
    if (has_checksum) {
        const uint32_t actual_crc = crc_impl(decompressed);
        if (expected_crc != actual_crc) {
            seastar::on_internal_error(arc_logger, fmt::format("RPC compression checksum error (expected: {:x}, got: {:x}). This indicates a bug. Set `internode_compression: none` and restart the nodes to regain stability, then report the bug.", expected_crc, actual_crc));
        }
    }
    _tracker->ingest(decompressed);
    stats.compressed_bytes_received += compressed_size;
    stats.bytes_received += decompressed.size;
    stats.messages_received += 1;
    return decompressed;
}

zstd_dstream& advanced_rpc_compressor::get_global_zstd_dstream() {
    auto& dstream = _tracker->get_global_zstd_dstream();
    dstream.set_dict(_control.receiver_current_dict().zstd_ddict.get());
    return _tracker->get_global_zstd_dstream();
}

zstd_cstream& advanced_rpc_compressor::get_global_zstd_cstream() {
    auto& cstream = _tracker->get_global_zstd_cstream();
    cstream.set_dict(_control.sender_current_dict().zstd_cdict.get());
    return _tracker->get_global_zstd_cstream();
}

lz4_dstream& advanced_rpc_compressor::get_global_lz4_dstream() {
    auto& dstream = _tracker->get_global_lz4_dstream();
    dstream.set_dict(_control.receiver_current_dict().lz4_ddict);
    return dstream;
}

lz4_cstream& advanced_rpc_compressor::get_global_lz4_cstream() {
    auto& cstream = _tracker->get_global_lz4_cstream();
    cstream.set_dict(_control.sender_current_dict().lz4_cdict.get());
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

template <typename T>
requires std::same_as<T, rpc::rcv_buf> || std::same_as<T, rpc::snd_buf>
void advanced_rpc_compressor::tracker::ingest_generic(const T& data) {
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

void advanced_rpc_compressor::tracker::ingest(const rpc::snd_buf& data) {
    ingest_generic(data);
}

void advanced_rpc_compressor::tracker::ingest(const rpc::rcv_buf& data) {
    ingest_generic(data);
}

void advanced_rpc_compressor::tracker::announce_dict(dict_ptr d) {
    _most_recent_dict = d;
    for (const auto c : _compressors) {
        c->_control.announce_dict(_most_recent_dict);
    }
}

future<> announce_dict_to_shards(seastar::sharded<walltime_compressor_tracker>& sharded_tracker, utils::shared_dict shared_dict) {
    arc_logger.debug("Announcing new dictionary: ts={}, origin={}", shared_dict.id.timestamp, shared_dict.id.origin_node);
    auto dict = make_lw_shared(std::move(shared_dict));
    auto foreign_ptrs = std::vector<foreign_ptr<decltype(dict)>>();
    for (size_t i = 0; i < smp::count; ++i) {
        foreign_ptrs.push_back(make_foreign(dict));
    }
    co_await sharded_tracker.invoke_on_all([&foreign_ptrs] (auto& tracker) {
        tracker.announce_dict(make_lw_shared(std::move(foreign_ptrs[this_shard_id()])));
    });
}

} // namespace utils
