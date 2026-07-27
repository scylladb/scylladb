/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/core/metrics.hh>
#include <seastar/util/defer.hh>
#include <filesystem>
#include <seastar/util/tmp_file.hh>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <numeric>
#include <system_error>
#include "utils/log.hh"
#include "utils/assert.hh"
#include "utils/error_injection.hh"
#include "utils/hashers.hh"
#include "utils/xx_hasher.hh"
#include "bytes.hh"
#include "advanced_rpc_compressor.hh"
#include "advanced_rpc_compressor_protocol.hh"
#include "stream_compressor.hh"
#include "dict_trainer.hh"
#include <seastar/core/on_internal_error.hh>

namespace netw {

logging::logger arc_logger("advanced_rpc_compressor");

static const shared_dict null_dict;

std::array<std::byte, rpc_compression_fingerprint::serialized_size> rpc_compression_fingerprint::serialize() const noexcept {
    std::array<std::byte, serialized_size> out;
    auto* out_data = reinterpret_cast<char*>(out.data());
    seastar::write_be<uint32_t>(out_data, crc);
    seastar::write_be<uint64_t>(out_data + sizeof(uint32_t), compressed_xxh64);
    return out;
}

std::optional<rpc_compression_fingerprint> rpc_compression_fingerprint::deserialize(std::span<const std::byte> bytes) noexcept {
    if (bytes.size() != serialized_size) {
        return std::nullopt;
    }
    const auto* data = reinterpret_cast<const char*>(bytes.data());
    return rpc_compression_fingerprint{
        .crc = seastar::read_be<uint32_t>(data),
        .compressed_xxh64 = seastar::read_be<uint64_t>(data + sizeof(uint32_t)),
    };
}

std::optional<rpc_compression_fingerprint> rpc_compression_fingerprint::deserialize(std::string_view hex_string) noexcept {
    try {
        auto bytes = from_hex(hex_string);
        return deserialize(std::span<const std::byte>(reinterpret_cast<const std::byte*>(bytes.data()), bytes.size()));
    } catch (...) {
        return std::nullopt;
    }
}

static std::optional<rpc_compression_fingerprint> parse_dump_message_on_fingerprint(const sstring& value) {
    if (value.empty()) {
        return std::nullopt;
    }
    auto fingerprint = rpc_compression_fingerprint::deserialize(value);
    if (!fingerprint) {
        arc_logger.warn("Ignoring invalid internode_compression_dump_message_on_fingerprint value '{}'", value);
    }
    return fingerprint;
}

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
const static sstring COMPRESSOR_NAME = "SCYLLA_V4";

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
    , _dump_message_on_fingerprint(parse_dump_message_on_fingerprint(_cfg.dump_message_on_fingerprint.get()))
    , _dump_message_on_fingerprint_observer(_cfg.dump_message_on_fingerprint.observe([this] (const auto& x) {
        _dump_message_on_fingerprint = parse_dump_message_on_fingerprint(x);
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

template<class T>
concept RpcBuf = std::same_as<T, rpc::rcv_buf> || std::same_as<T, rpc::snd_buf>;

template <RpcBuf Buf, std::invocable<std::span<const std::byte>> Func>
static void for_each_rpc_buf_fragment(const Buf& data, size_t offset, Func&& func) {
    if (offset > data.size) {
        throw std::invalid_argument("RPC buffer fragment offset is past the end of the buffer");
    }
    auto payload_size = data.size - offset;
    auto it = std::get_if<temporary_buffer<char>>(&data.bufs);
    if (!it) {
        it = std::get<std::vector<temporary_buffer<char>>>(data.bufs).data();
    }

    while (payload_size > 0) {
        const auto fragment_size = it->size();
        if (offset > fragment_size) {
            offset -= fragment_size;
            ++it;
            continue;
        }
        const auto n = std::min<size_t>(fragment_size - offset, payload_size);
        std::invoke(func, std::as_bytes(std::span(it->get() + offset, n)));
        offset = 0;
        payload_size -= n;
        ++it;
    }
}

template <RpcBuf Buf>
static uint64_t xxh64_impl(const Buf& data, size_t offset = 0) noexcept {
    xx_hasher hasher;
    for_each_rpc_buf_fragment(data, offset, [&hasher] (std::span<const std::byte> fragment) {
        if (!fragment.empty()) {
            hasher.update(reinterpret_cast<const char*>(fragment.data()), fragment.size());
        }
    });
    return hasher.finalize_uint64();
}

template <RpcBuf Buf>
static rpc_compression_fingerprint get_compression_fingerprint(uint32_t crc, const Buf& compressed, size_t offset = 0) noexcept {
    return rpc_compression_fingerprint{
        .crc = crc,
        .compressed_xxh64 = xxh64_impl(compressed, offset),
    };
}

static rpc_compression_fingerprint get_compression_fingerprint(uint32_t crc, const rpc::snd_buf& compressed, size_t offset) noexcept {
    return get_compression_fingerprint<rpc::snd_buf>(crc, compressed, offset);
}

static void write_all(int fd, std::span<const std::byte> data) {
    while (!data.empty()) {
        const ssize_t n = ::write(fd, data.data(), data.size());
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw std::system_error(errno, std::system_category(), "write");
        }
        data = data.subspan(n);
    }
}

// Dumps a compressed frame together with the dictionary it used to a file in the
// temporary directory, for offline investigation.
//
// This is just a diagnostic aid, so any failure is logged and otherwise ignored.
// It must never bring the node down.
template <RpcBuf Buf>
static void dump_message(
    std::string_view path_filename,
    std::string_view description,
    uint8_t header_byte,
    uint32_t expected_crc,
    uint32_t actual_crc,
    const shared_dict& dict,
    const Buf& data,
    size_t payload_offset
) {
    const auto path = fmt::format("{}/{}.shard_{}.bin", seastar::default_tmpdir().native(), path_filename, this_shard_id());
    int fd = -1;
    auto close_fd = defer([&fd] noexcept {
        if (fd >= 0) {
            ::close(fd);
        }
    });
    try {
        // Recreate the file from scratch, so that we are sure about its permissions.
        if (::unlink(path.c_str()) < 0 && errno != ENOENT) {
            throw std::system_error(errno, std::system_category(), "unlink");
        }
        fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, S_IRUSR | S_IWUSR);
        if (fd < 0) {
            throw std::system_error(errno, std::system_category(), "open");
        }

        // The message can be arbitrarily big, so we stream it out fragment by fragment
        // instead of gluing it into one contiguous buffer.
        auto write_be = [&fd] <std::integral T> (T x) {
            std::array<char, sizeof(T)> buf;
            seastar::write_be<T>(buf.data(), x);
            write_all(fd, std::as_bytes(std::span(buf)));
        };

        write_be(header_byte);
        write_be(expected_crc);
        write_be(actual_crc);
        write_all(fd, dict.id.content_sha256);
        write_be(uint64_t(dict.data.size()));
        write_all(fd, dict.data);
        for_each_rpc_buf_fragment(data, payload_offset, [&] (std::span<const std::byte> fragment) {
            write_be(uint64_t(fragment.size()));
            write_all(fd, fragment);
        });

        if (::close(std::exchange(fd, -1)) < 0) {
            throw std::system_error(errno, std::system_category(), "close");
        }
        arc_logger.warn("Dumped {} to {}", description, path);
    } catch (...) {
        arc_logger.error("Failed to dump {} to {}: {}", description, path, std::current_exception());
    }
}

rpc::snd_buf advanced_rpc_compressor::compress(size_t head_space, rpc::snd_buf data) {
    const size_t checksum_size = _tracker->_cfg.checksumming.get() ? sizeof(uint32_t) + sizeof(std::byte) : 0;
    const uint32_t crc = checksum_size ? crc_impl(data) : -1;

    auto now = _tracker->get_steady_nanos();
    _tracker->maybe_refresh_zstd_quota(now);

    auto algo = get_algo_for_next_msg(data.size);

    auto& stats = _tracker->_stats[algo.idx()];
    auto update_time_stats = defer([&, nanos_before = now] noexcept {
        stats.compression_cpu_nanos += _tracker->get_steady_nanos() - nanos_before;
    });

    _tracker->ingest(data);

    auto protocol_header = _control.produce_control_header();
    const size_t protocol_header_size = protocol_header ? control_protocol_frame::serialized_size : 0;

    const std::byte dict_id = _control.sender_current_dict().id.content_sha256[0];

    auto uncompressed_size = data.size;
    auto compressed = std::invoke([&] {
        try {
            return compress_impl(head_space + 1 + checksum_size + protocol_header_size, data, get_compressor(algo), true, rpc::snd_buf::chunk_size);
        } catch (...) {
            arc_logger.error("Error during compression with algorithm {}: {}. ", algo.name(), std::current_exception());
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
    const uint8_t header_byte = (algo.idx() & 0x3f) | (protocol_header ? 0x80 : 0x00) | (checksum_size ? 0x40 : 0x00);
    dst->get_write()[head_space] = header_byte;
    if (checksum_size) {
        write_le<uint32_t>(&dst->get_write()[head_space + 1], crc);
        dst->get_write()[head_space + 1 + sizeof(uint32_t)] = static_cast<char>(dict_id);
    }
    if (protocol_header) {
        auto out_data = reinterpret_cast<std::byte*>(dst->get_write() + head_space + 1 + checksum_size);
        constexpr size_t out_size = control_protocol_frame::serialized_size;
        auto out = std::span<std::byte, out_size>(out_data, out_size);
        protocol_header->serialize(out);
    }

    // Emulates a corruption of the message somewhere between the compressor and the
    // decompressor, to let tests exercise the checksum validation path on the receiver.
    // An empty message carries no compressed payload, so there is nothing to corrupt in it.
    if (uncompressed_size > 0) {
        utils::get_local_injector().inject("advanced_rpc_compressor_corrupt_last_byte", [&] {
            auto* frags = std::get_if<temporary_buffer<char>>(&compressed.bufs);
            size_t n_frags = 1;
            if (!frags) {
                auto& vec = std::get<std::vector<temporary_buffer<char>>>(compressed.bufs);
                frags = vec.data();
                n_frags = vec.size();
            }
            // The last fragment can be empty, so we look for the last one which isn't.
            while (n_frags > 0 && frags[n_frags - 1].empty()) {
                --n_frags;
            }
            SCYLLA_ASSERT(n_frags > 0);
            auto& last = frags[n_frags - 1];
            last.get_write()[last.size() - 1] ^= 0xff;
        });
    }

    const auto& dump_message_on_fingerprint = _tracker->_dump_message_on_fingerprint;
    if (checksum_size && dump_message_on_fingerprint) {
        if (dump_message_on_fingerprint->crc == crc) [[unlikely]] {
            const size_t payload_offset = head_space + 1 + checksum_size + protocol_header_size;
            auto fingerprint = get_compression_fingerprint(crc, compressed, payload_offset);
            if (fingerprint == dump_message_on_fingerprint) {
                arc_logger.warn("Sender matched fingerprint {}", fmt_hex(fingerprint.serialize()));
                static thread_local logging::logger::rate_limit dump_rate_limit(std::chrono::minutes(1));
                if (!dump_rate_limit.rate_limited()) {
                    dump_message(
                        "scylladb_rpc_fingerprint_match_dump",
                        "the pre-compression message matching fingerprint",
                        header_byte,
                        crc,
                        crc,
                        _control.sender_current_dict(),
                        data,
                        0);
                }
            }
        }
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

rpc::rcv_buf advanced_rpc_compressor::decompress(rpc::rcv_buf data) {
    const uint8_t header_byte = read_from_rcv_buf<uint8_t>(data);
    const bool has_checksum = header_byte & 0x40;
    const bool has_control_frame = header_byte & 0x80;

    uint32_t expected_crc = -1;
    std::byte expected_dict_id = std::byte(0);
    if (has_checksum) {
        expected_crc = seastar::le_to_cpu(read_from_rcv_buf<uint32_t>(data));
        expected_dict_id = std::byte(read_from_rcv_buf<uint8_t>(data));
    }

    if (has_control_frame) {
        auto control_protocol_frame_bytes = read_from_rcv_buf<std::array<std::byte, control_protocol_frame::serialized_size>>(data);
        _control.consume_control_header(control_protocol_frame::deserialize(control_protocol_frame_bytes));
    }

    // Will throw if the enum value is unknown.
    auto algo = compression_algorithm(header_byte & 0x3f);

    auto& stats = _tracker->_stats[algo.idx()];
    auto update_time_stats = defer([&, nanos_before = _tracker->get_steady_nanos()] noexcept {
        stats.decompression_cpu_nanos += _tracker->get_steady_nanos() - nanos_before;
    });
    auto compressed_size = data.size;
    auto decompressed = std::invoke([&] {
        try {
            return decompress_impl(data, get_decompressor(algo), true, rpc::snd_buf::chunk_size);
         } catch (...) {
            arc_logger.error("Error during decompression with algorithm {}: {}. ", algo.name(), std::current_exception());
            throw;
        }
    });
    if (has_checksum) {
        const uint32_t actual_crc = crc_impl(decompressed);
        const auto& current_dict_id = _control.receiver_current_dict().id;
        if (expected_dict_id != current_dict_id.content_sha256[0]) [[unlikely]] {
            static thread_local logging::logger::rate_limit error_rate_limit(std::chrono::minutes(1));
            arc_logger.log(log_level::error, error_rate_limit, "RPC compression dict ID mismatch: expected={:#04x}, actual={}", static_cast<int>(expected_dict_id), format_dict_id(current_dict_id));
        }
        if (expected_crc != actual_crc) [[unlikely]] {
            const auto fingerprint = get_compression_fingerprint(expected_crc, data);
            // Gathering the details below (in particular hashing the dictionary) isn't free,
            // so we query the rate limit ourselves instead of passing it to the logger.
            arc_logger.error(
                "RPC compression checksum error details:"
                " expected_crc={:#010x}, actual_crc={:#010x}"
                ", header_byte={:#04x}"
                ", fingerprint={}"
                ", control_protocol={}"
                ", compressed_size={}, decompressed_size={}",
                expected_crc, actual_crc,
                header_byte,
                fmt_hex(fingerprint.serialize()),
                format_control_protocol(_control),
                compressed_size, decompressed.size);
            static thread_local logging::logger::rate_limit dump_rate_limit(std::chrono::minutes(1));
            if (!dump_rate_limit.rate_limited()) {
                arc_logger.error("sha256 recomputed from the contents of the dictionary used for decompression: {}", fmt_hex(get_sha256(_control.receiver_current_dict().data)));
                if (_tracker->_cfg.dump_message_on_checksum_error.get()) {
                    dump_message(
                        "scylladb_rpc_checksum_error_dump",
                        "the compressed message which failed checksum validation",
                        header_byte,
                        expected_crc,
                        actual_crc,
                        _control.receiver_current_dict(),
                        data,
                        0);
                }
            }
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
