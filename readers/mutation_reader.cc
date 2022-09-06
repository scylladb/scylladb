/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "readers/flat_mutation_reader_v2.hh"
#include "mutation_rebuilder.hh"
#include "mutation_fragment_stream_validator.hh"
#include "schema_upgrader.hh"

logging::logger mrlog("mutation_reader");

invalid_mutation_fragment_stream::invalid_mutation_fragment_stream(std::runtime_error e) : std::runtime_error(std::move(e)) {
}

static mutation_fragment_v2::kind to_mutation_fragment_kind_v2(mutation_fragment::kind k) {
    switch (k) {
        case mutation_fragment::kind::partition_start:
            return mutation_fragment_v2::kind::partition_start;
        case mutation_fragment::kind::static_row:
            return mutation_fragment_v2::kind::static_row;
        case mutation_fragment::kind::clustering_row:
            return mutation_fragment_v2::kind::clustering_row;
        case mutation_fragment::kind::range_tombstone:
            return mutation_fragment_v2::kind::range_tombstone_change;
        case mutation_fragment::kind::partition_end:
            return mutation_fragment_v2::kind::partition_end;
    }
    std::abort();
}

mutation_fragment_stream_validator::mutation_fragment_stream_validator(const ::schema& s)
    : _schema(s)
    , _prev_kind(mutation_fragment_v2::kind::partition_end)
    , _prev_pos(position_in_partition::end_of_partition_tag_t{})
    , _prev_partition_key(dht::minimum_token(), partition_key::make_empty()) {
}

bool mutation_fragment_stream_validator::operator()(const dht::decorated_key& dk) {
    if (_prev_partition_key.less_compare(_schema, dk)) {
        _prev_partition_key = dk;
        return true;
    }
    return false;
}

bool mutation_fragment_stream_validator::operator()(dht::token t) {
    if (_prev_partition_key.token() <= t) {
        _prev_partition_key._token = t;
        return true;
    }
    return false;
}

bool mutation_fragment_stream_validator::operator()(mutation_fragment_v2::kind kind, position_in_partition_view pos) {
    if (kind == mutation_fragment_v2::kind::partition_end && _current_tombstone) {
        return false;
    }
    if (_prev_kind == mutation_fragment_v2::kind::partition_end) {
        const bool valid = (kind == mutation_fragment_v2::kind::partition_start);
        if (valid) {
            _prev_kind = mutation_fragment_v2::kind::partition_start;
            _prev_pos = pos;
        }
        return valid;
    }
    auto cmp = position_in_partition::tri_compare(_schema);
    auto res = cmp(_prev_pos, pos);
    bool valid = true;
    if (_prev_kind == mutation_fragment_v2::kind::range_tombstone_change) {
        valid = res <= 0;
    } else {
        valid = res < 0;
    }
    if (valid) {
        _prev_kind = kind;
        _prev_pos = pos;
    }
    return valid;
}
bool mutation_fragment_stream_validator::operator()(mutation_fragment::kind kind, position_in_partition_view pos) {
    return (*this)(to_mutation_fragment_kind_v2(kind), pos);
}

bool mutation_fragment_stream_validator::operator()(const mutation_fragment_v2& mf) {
    const auto valid = (*this)(mf.mutation_fragment_kind(), mf.position());
    if (valid && mf.is_range_tombstone_change()) {
        _current_tombstone = mf.as_range_tombstone_change().tombstone();
    }
    return valid;
}
bool mutation_fragment_stream_validator::operator()(const mutation_fragment& mf) {
    return (*this)(to_mutation_fragment_kind_v2(mf.mutation_fragment_kind()), mf.position());
}

bool mutation_fragment_stream_validator::operator()(mutation_fragment_v2::kind kind) {
    bool valid = true;
    switch (_prev_kind) {
        case mutation_fragment_v2::kind::partition_start:
            valid = kind != mutation_fragment_v2::kind::partition_start;
            break;
        case mutation_fragment_v2::kind::static_row: // fall-through
        case mutation_fragment_v2::kind::clustering_row: // fall-through
        case mutation_fragment_v2::kind::range_tombstone_change:
            valid = kind != mutation_fragment_v2::kind::partition_start &&
                    kind != mutation_fragment_v2::kind::static_row;
            break;
        case mutation_fragment_v2::kind::partition_end:
            valid = kind == mutation_fragment_v2::kind::partition_start;
            break;
    }
    if (valid) {
        _prev_kind = kind;
    }
    return valid;
}
bool mutation_fragment_stream_validator::operator()(mutation_fragment::kind kind) {
    return (*this)(to_mutation_fragment_kind_v2(kind));
}

bool mutation_fragment_stream_validator::on_end_of_stream() {
    return _prev_kind == mutation_fragment_v2::kind::partition_end;
}

void mutation_fragment_stream_validator::reset(dht::decorated_key dk) {
    _prev_partition_key = dk;
    _prev_pos = position_in_partition::for_partition_start();
    _prev_kind = mutation_fragment_v2::kind::partition_start;
    _current_tombstone = {};
}

void mutation_fragment_stream_validator::reset(const mutation_fragment_v2& mf) {
    _prev_pos = mf.position();
    _prev_kind = mf.mutation_fragment_kind();
    if (mf.is_range_tombstone_change()) {
        _current_tombstone = mf.as_range_tombstone_change().tombstone();
    } else {
        _current_tombstone = {};
    }
}
void mutation_fragment_stream_validator::reset(const mutation_fragment& mf) {
    _prev_pos = mf.position();
    _prev_kind = to_mutation_fragment_kind_v2(mf.mutation_fragment_kind());
}

namespace {

[[noreturn]] void on_validation_error(seastar::logger& l, const seastar::sstring& reason) {
    try {
        on_internal_error(l, reason);
    } catch (std::runtime_error& e) {
        throw invalid_mutation_fragment_stream(e);
    }
}

}

bool mutation_fragment_stream_validating_filter::operator()(const dht::decorated_key& dk) {
    if (_validation_level < mutation_fragment_stream_validation_level::token) {
        return true;
    }
    if (_validation_level == mutation_fragment_stream_validation_level::token) {
        if (_validator(dk.token())) {
            return true;
        }
        on_validation_error(mrlog, format("[validator {} for {}] Unexpected token: previous {}, current {}",
                static_cast<void*>(this), _name, _validator.previous_token(), dk.token()));
    } else {
        if (_validator(dk)) {
            return true;
        }
        on_validation_error(mrlog, format("[validator {} for {}] Unexpected partition key: previous {}, current {}",
                static_cast<void*>(this), _name, _validator.previous_partition_key(), dk));
    }
}

mutation_fragment_stream_validating_filter::mutation_fragment_stream_validating_filter(sstring_view name, const schema& s,
        mutation_fragment_stream_validation_level level)
    : _validator(s)
    , _name(format("{} ({}.{} {})", name, s.ks_name(), s.cf_name(), s.id()))
    , _validation_level(level)
{
    if (mrlog.is_enabled(log_level::debug)) {
        std::string_view what;
        switch (_validation_level) {
            case mutation_fragment_stream_validation_level::partition_region:
                what = "partition region";
                break;
            case mutation_fragment_stream_validation_level::token:
                what = "partition region and token";
                break;
            case mutation_fragment_stream_validation_level::partition_key:
                what = "partition region and partition key";
                break;
            case mutation_fragment_stream_validation_level::clustering_key:
                what = "partition region, partition key and clustering key";
                break;
        }
        mrlog.debug("[validator {} for {}] Will validate {} monotonicity.", static_cast<void*>(this), _name, what);
    }
}

bool mutation_fragment_stream_validating_filter::operator()(mutation_fragment_v2::kind kind, position_in_partition_view pos) {
    bool valid = false;

    mrlog.debug("[validator {}] {}:{}", static_cast<void*>(this), kind, pos);

    if (kind == mutation_fragment_v2::kind::partition_end && _current_tombstone) {
        on_validation_error(mrlog, format("[validator {} for {}] Unexpected active tombstone at partition-end: partition key {}: tombstone {}",
                static_cast<void*>(this), _name, _validator.previous_partition_key(), _current_tombstone));
    }

    if (_validation_level >= mutation_fragment_stream_validation_level::clustering_key) {
        valid = _validator(kind, pos);
    } else {
        valid = _validator(kind);
    }

    if (__builtin_expect(!valid, false)) {
        if (_validation_level >= mutation_fragment_stream_validation_level::clustering_key) {
            on_validation_error(mrlog, format("[validator {} for {}] Unexpected mutation fragment: partition key {}: previous {}:{}, current {}:{}",
                    static_cast<void*>(this), _name, _validator.previous_partition_key(), _validator.previous_mutation_fragment_kind(), _validator.previous_position(), kind, pos));
        } else if (_validation_level >= mutation_fragment_stream_validation_level::partition_key) {
            on_validation_error(mrlog, format("[validator {} for {}] Unexpected mutation fragment: partition key {}: previous {}, current {}",
                    static_cast<void*>(this), _name, _validator.previous_partition_key(), _validator.previous_mutation_fragment_kind(), kind));
        } else {
            on_validation_error(mrlog, format("[validator {} for {}] Unexpected mutation fragment: previous {}, current {}",
                    static_cast<void*>(this), _name, _validator.previous_mutation_fragment_kind(), kind));
        }
    }

    return true;
}

bool mutation_fragment_stream_validating_filter::operator()(mutation_fragment::kind kind, position_in_partition_view pos) {
    return (*this)(to_mutation_fragment_kind_v2(kind), pos);
}

bool mutation_fragment_stream_validating_filter::operator()(const mutation_fragment_v2& mv) {
    auto valid = (*this)(mv.mutation_fragment_kind(), mv.position());
    if (valid && mv.is_range_tombstone_change()) {
        _current_tombstone = mv.as_range_tombstone_change().tombstone();
    }
    return valid;
}
bool mutation_fragment_stream_validating_filter::operator()(const mutation_fragment& mv) {
    return (*this)(to_mutation_fragment_kind_v2(mv.mutation_fragment_kind()), mv.position());
}

bool mutation_fragment_stream_validating_filter::on_end_of_partition() {
    return (*this)(mutation_fragment::kind::partition_end, position_in_partition_view(position_in_partition_view::end_of_partition_tag_t()));
}

void mutation_fragment_stream_validating_filter::on_end_of_stream() {
    mrlog.debug("[validator {}] EOS", static_cast<const void*>(this));
    if (!_validator.on_end_of_stream()) {
        on_validation_error(mrlog, format("[validator {} for {}] Stream ended with unclosed partition: {}", static_cast<const void*>(this), _name,
                _validator.previous_mutation_fragment_kind()));
    }
}

static size_t compute_buffer_size(const schema& s, const flat_mutation_reader_v2::tracked_buffer& buffer)
{
    return boost::accumulate(
        buffer
        | boost::adaptors::transformed([&s] (const mutation_fragment_v2& mf) {
            return mf.memory_usage();
        }), size_t(0)
    );
}

flat_mutation_reader_v2& flat_mutation_reader_v2::operator=(flat_mutation_reader_v2&& o) noexcept {
    if (_impl && _impl->is_close_required()) {
        impl* ip = _impl.get();
        // Abort to enforce calling close() before readers are closed
        // to prevent leaks and potential use-after-free due to background
        // tasks left behind.
        on_internal_error_noexcept(mrlog, format("{} [{}]: permit {}: was not closed before overwritten by move-assign", typeid(*ip).name(), fmt::ptr(ip), ip->_permit.description()));
        abort();
    }
    _impl = std::move(o._impl);
    return *this;
}

flat_mutation_reader_v2::~flat_mutation_reader_v2() {
    if (_impl && _impl->is_close_required()) {
        impl* ip = _impl.get();
        // Abort to enforce calling close() before readers are closed
        // to prevent leaks and potential use-after-free due to background
        // tasks left behind.
        on_internal_error_noexcept(mrlog, format("{} [{}]: permit {}: was not closed before destruction", typeid(*ip).name(), fmt::ptr(ip), ip->_permit.description()));
        abort();
    }
}

void flat_mutation_reader_v2::impl::forward_buffer_to(const position_in_partition& pos) {
    clear_buffer();
    _buffer_size = compute_buffer_size(*_schema, _buffer);
}

void flat_mutation_reader_v2::impl::clear_buffer_to_next_partition() {
    auto next_partition_start = std::find_if(_buffer.begin(), _buffer.end(), [] (const mutation_fragment_v2& mf) {
        return mf.is_partition_start();
    });
    _buffer.erase(_buffer.begin(), next_partition_start);

    _buffer_size = compute_buffer_size(*_schema, _buffer);
}

template<typename Source>
future<bool> flat_mutation_reader_v2::impl::fill_buffer_from(Source& source) {
    if (source.is_buffer_empty()) {
        if (source.is_end_of_stream()) {
            return make_ready_future<bool>(true);
        }
        return source.fill_buffer().then([this, &source] {
            return fill_buffer_from(source);
        });
    } else {
        while (!source.is_buffer_empty() && !is_buffer_full()) {
            push_mutation_fragment(source.pop_mutation_fragment());
        }
        return make_ready_future<bool>(source.is_end_of_stream() && source.is_buffer_empty());
    }
}

template future<bool> flat_mutation_reader_v2::impl::fill_buffer_from<flat_mutation_reader_v2>(flat_mutation_reader_v2&);

void flat_mutation_reader_v2::do_upgrade_schema(const schema_ptr& s) {
    *this = transform(std::move(*this), schema_upgrader_v2(s));
}

void flat_mutation_reader_v2::on_close_error(std::unique_ptr<impl> i, std::exception_ptr ep) noexcept {
    impl* ip = i.get();
    on_internal_error_noexcept(mrlog,
            format("Failed to close {} [{}]: permit {}: {}", typeid(*ip).name(), fmt::ptr(ip), ip->_permit.description(), ep));
}

future<mutation_opt> read_mutation_from_flat_mutation_reader(flat_mutation_reader_v2& r) {
    return r.consume(mutation_rebuilder_v2(r.schema()));
}
