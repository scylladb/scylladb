/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/util/lazy.hh>

#include "readers/flat_mutation_reader_v2.hh"
#include "mutation/mutation_rebuilder.hh"
#include "mutation/mutation_fragment_stream_validator.hh"
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

static sstring
format_partition_key(const schema& s, const dht::decorated_key& pkey, const char* prefix = "") {
    if (pkey.key().is_empty()) {
        return "";
    }
    return format("{}{} ({})", prefix, pkey.key().with_schema(s), pkey);
}

static mutation_fragment_stream_validator::validation_result
ooo_key_result(const schema& s, dht::token t, const partition_key* pkey, dht::decorated_key prev_key) {
    return mutation_fragment_stream_validator::validation_result::invalid(format("out-of-order {} {}, previous {} was {}",
            pkey ? "partition key" : "token",
            pkey ? format("{} ({{key: {}, token: {}}})", pkey->with_schema(s), *pkey, t) : format("{}", t),
            prev_key.key().is_empty() ? "token" : "partition key",
            prev_key.key().is_empty() ? format("{}", prev_key.token()) : format_partition_key(s, prev_key)));
}

mutation_fragment_stream_validator::validation_result
mutation_fragment_stream_validator::validate(dht::token t, const partition_key* pkey) {
    if (_prev_partition_key.token() > t) {
        return ooo_key_result(_schema, t, pkey, _prev_partition_key);
    }
    partition_key::tri_compare cmp(_schema);
    if (_prev_partition_key.token() == t && pkey && cmp(_prev_partition_key.key(), *pkey) >= 0) {
        return ooo_key_result(_schema, t, pkey, _prev_partition_key);
    }
    _prev_partition_key._token = t;
    if (pkey) {
        _prev_partition_key._key = *pkey;
    } else {
        // If new partition-key is not supplied, we reset it to empty one, which
        // will compare less than any other key, making sure we don't attempt to
        // compare partition-keys belonging to different tokens.
        if (!_prev_partition_key.key().is_empty()) {
            _prev_partition_key._key = partition_key::make_empty();
        }
    }
    return validation_result::valid();
}

mutation_fragment_stream_validator::validation_result
mutation_fragment_stream_validator::operator()(const dht::decorated_key& dk) {
    return validate(dk.token(), &dk.key());
}

mutation_fragment_stream_validator::validation_result
mutation_fragment_stream_validator::operator()(dht::token t) {
    return validate(t, nullptr);
}

mutation_fragment_stream_validator::validation_result
mutation_fragment_stream_validator::validate(mutation_fragment_v2::kind kind, std::optional<position_in_partition_view> pos,
    std::optional<tombstone> new_current_tombstone) {
    // Check for unclosed range tombstone on partition end
    if (kind == mutation_fragment_v2::kind::partition_end && _current_tombstone) {
        return validation_result::invalid(format("invalid partition-end, partition {} has an active range tombstone {}",
                    format_partition_key(_schema, _prev_partition_key), _current_tombstone));
    }

    auto valid = true;

    // Check fragment kind order
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
    if (!valid) {
        return validation_result::invalid(format("out-of-order mutation fragment {}{}, previous mutation fragment was {}",
                kind,
                format_partition_key(_schema, _prev_partition_key, " in partition "),
                _prev_kind));
    }

    if (pos && _prev_kind != mutation_fragment_v2::kind::partition_end) {
        auto cmp = position_in_partition::tri_compare(_schema);
        auto res = cmp(_prev_pos, *pos);
        if (_prev_kind == mutation_fragment_v2::kind::range_tombstone_change) {
            valid = res <= 0;
        } else {
            valid = res < 0;
        }
        if (!valid) {
            return validation_result::invalid(format("out-of-order {} at position {}{}, previous clustering element was {} at position {}",
                    kind,
                    *pos,
                    format_partition_key(_schema, _prev_partition_key, " in partition "),
                    _prev_pos,
                    _prev_kind));
        }
    }

    _prev_kind = kind;
    if (pos) {
        _prev_pos = *pos;
    } else {
        switch (kind) {
            case mutation_fragment_v2::kind::partition_start:
                _prev_pos = position_in_partition::for_partition_start();
                break;
            case mutation_fragment_v2::kind::static_row:
                _prev_pos = position_in_partition(position_in_partition::static_row_tag_t{});
                break;
            case mutation_fragment_v2::kind::clustering_row:
                 [[fallthrough]];
            case mutation_fragment_v2::kind::range_tombstone_change:
                if (_prev_pos.region() != partition_region::clustered) { // don't move pos if it is already a clustering one
                    _prev_pos = position_in_partition(position_in_partition::before_clustering_row_tag_t{}, clustering_key::make_empty());
                }
                break;
            case mutation_fragment_v2::kind::partition_end:
                _prev_pos = position_in_partition(position_in_partition::end_of_partition_tag_t{});
                break;
        }
    }
    if (new_current_tombstone) {
        _current_tombstone = *new_current_tombstone;
    }
    return validation_result::valid();
}

mutation_fragment_stream_validator::validation_result
mutation_fragment_stream_validator::operator()(mutation_fragment_v2::kind kind, position_in_partition_view pos,
        std::optional<tombstone> new_current_tombstone) {
    return validate(kind, pos, new_current_tombstone);
}
mutation_fragment_stream_validator::validation_result
mutation_fragment_stream_validator::operator()(mutation_fragment::kind kind, position_in_partition_view pos) {
    return validate(to_mutation_fragment_kind_v2(kind), pos, {});
}

mutation_fragment_stream_validator::validation_result
mutation_fragment_stream_validator::operator()(const mutation_fragment_v2& mf) {
    return validate(mf.mutation_fragment_kind(), mf.position(),
            mf.is_range_tombstone_change() ? std::optional(mf.as_range_tombstone_change().tombstone()) : std::nullopt);
}
mutation_fragment_stream_validator::validation_result
mutation_fragment_stream_validator::operator()(const mutation_fragment& mf) {
    return validate(to_mutation_fragment_kind_v2(mf.mutation_fragment_kind()), mf.position(), {});
}

mutation_fragment_stream_validator::validation_result
mutation_fragment_stream_validator::operator()(mutation_fragment_v2::kind kind, std::optional<tombstone> new_current_tombstone) {
    return validate(kind, {}, new_current_tombstone);
}
mutation_fragment_stream_validator::validation_result
mutation_fragment_stream_validator::operator()(mutation_fragment::kind kind) {
    return validate(to_mutation_fragment_kind_v2(kind), {}, {});
}

mutation_fragment_stream_validator::validation_result
mutation_fragment_stream_validator::on_end_of_stream() {
    if (_prev_kind == mutation_fragment_v2::kind::partition_end) {
        return validation_result::valid();
    }
    return validation_result::invalid(format("invalid end-of-stream, last partition{} was not closed, last fragment was {}",
            format_partition_key(_schema, _prev_partition_key, " "),
            _prev_kind));
}

void mutation_fragment_stream_validator::reset(dht::decorated_key dk) {
    _prev_partition_key = std::move(dk);
    _prev_pos = position_in_partition::for_partition_start();
    _prev_kind = mutation_fragment_v2::kind::partition_start;
    _current_tombstone = {};
}

void mutation_fragment_stream_validator::reset(mutation_fragment_v2::kind kind, position_in_partition_view pos, std::optional<tombstone> new_current_tombstone) {
    _prev_pos = pos;
    _prev_kind = kind;
    if (new_current_tombstone) {
        _current_tombstone = *new_current_tombstone;
    }
}
void mutation_fragment_stream_validator::reset(const mutation_fragment_v2& mf) {
    reset(mf.mutation_fragment_kind(), mf.position(), mf.is_range_tombstone_change() ? std::optional(mf.as_range_tombstone_change().tombstone()) : std::nullopt);
}
void mutation_fragment_stream_validator::reset(const mutation_fragment& mf) {
    reset(to_mutation_fragment_kind_v2(mf.mutation_fragment_kind()), mf.position(), std::nullopt);
}

namespace {

[[noreturn]] void on_validation_error(seastar::logger& l, const mutation_fragment_stream_validating_filter& zis, mutation_fragment_stream_validator::validation_result res) {
    try {
        on_internal_error(l, format("[validator {} for {}] {}", fmt::ptr(&zis), zis.full_name(), res.what()));
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
        if (auto res = _validator(dk.token()); !res) {
            on_validation_error(mrlog, *this, res);
        }
        return true;
    } else {
        if (auto res = _validator(dk); !res) {
            on_validation_error(mrlog, *this, res);
        }
        return true;
    }
}

sstring mutation_fragment_stream_validating_filter::full_name() const {
    const auto& s = _validator.schema();
    return format("{} ({}.{} {})", _name_view, s.ks_name(), s.cf_name(), s.id());
}

mutation_fragment_stream_validating_filter::mutation_fragment_stream_validating_filter(const char* name_literal, sstring name_value, const schema& s,
        mutation_fragment_stream_validation_level level)
    : _validator(s)
    , _name_storage(std::move(name_value))
    , _validation_level(level)
{
    if (name_literal) {
        _name_view = name_literal;
    } else {
        _name_view = _name_storage;
    }
    if (mrlog.is_enabled(log_level::debug)) {
        std::string_view what;
        switch (_validation_level) {
            case mutation_fragment_stream_validation_level::none:
                what = "no";
                break;
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
        mrlog.debug("[validator {} for {}] Will validate {} monotonicity.", static_cast<void*>(this), full_name(), what);
    }
}

mutation_fragment_stream_validating_filter::mutation_fragment_stream_validating_filter(sstring name, const schema& s,
        mutation_fragment_stream_validation_level level)
    : mutation_fragment_stream_validating_filter(nullptr, std::move(name), s, level)
{ }

mutation_fragment_stream_validating_filter::mutation_fragment_stream_validating_filter(const char* name, const schema& s,
        mutation_fragment_stream_validation_level level)
    : mutation_fragment_stream_validating_filter(name, {}, s, level)
{ }

bool mutation_fragment_stream_validating_filter::operator()(mutation_fragment_v2::kind kind, position_in_partition_view pos,
        std::optional<tombstone> new_current_tombstone) {
    std::optional<mutation_fragment_stream_validator::validation_result> res;

    mrlog.debug("[validator {}] {}:{} new_current_tombstone: {}", static_cast<void*>(this), kind, pos, new_current_tombstone);

    if (_validation_level >= mutation_fragment_stream_validation_level::clustering_key) {
        res = _validator(kind, pos, new_current_tombstone);
    } else {
        res = _validator(kind, new_current_tombstone);
    }

    if (__builtin_expect(!res->is_valid(), false)) {
        on_validation_error(mrlog, *this, *res);
    }

    return true;
}

bool mutation_fragment_stream_validating_filter::operator()(mutation_fragment::kind kind, position_in_partition_view pos) {
    return (*this)(to_mutation_fragment_kind_v2(kind), pos, {});
}

bool mutation_fragment_stream_validating_filter::operator()(const mutation_fragment_v2& mv) {
    return (*this)(mv.mutation_fragment_kind(), mv.position(),
            mv.is_range_tombstone_change() ? std::optional(mv.as_range_tombstone_change().tombstone()) : std::nullopt);
}
bool mutation_fragment_stream_validating_filter::operator()(const mutation_fragment& mv) {
    return (*this)(to_mutation_fragment_kind_v2(mv.mutation_fragment_kind()), mv.position(), {});
}

void mutation_fragment_stream_validating_filter::reset(mutation_fragment_v2::kind kind, position_in_partition_view pos,
        std::optional<tombstone> new_current_tombstone) {
    mrlog.debug("[validator {}] reset to {} @ {}{}", static_cast<const void*>(this), kind, pos, value_of([t = new_current_tombstone] () -> sstring {
        if (!t) {
            return "";
        }
        return format(" (new tombstone: {})", *t);
    }));
    _validator.reset(kind, pos, new_current_tombstone);
}
void mutation_fragment_stream_validating_filter::reset(const mutation_fragment_v2& mf) {
    mrlog.debug("[validator {}] reset to {} @ {}{}", static_cast<const void*>(this), mf.mutation_fragment_kind(), mf.position(), value_of([&mf] () -> sstring {
        if (!mf.is_range_tombstone_change()) {
            return "";
        }
        return format(" (new tombstone: {})", mf.as_range_tombstone_change().tombstone());
    }));
    _validator.reset(mf);
}

bool mutation_fragment_stream_validating_filter::on_end_of_partition() {
    return (*this)(mutation_fragment::kind::partition_end, position_in_partition_view(position_in_partition_view::end_of_partition_tag_t()));
}

void mutation_fragment_stream_validating_filter::on_end_of_stream() {
    if (_validation_level < mutation_fragment_stream_validation_level::partition_region) {
        return;
    }
    mrlog.debug("[validator {}] EOS", static_cast<const void*>(this));
    if (auto res = _validator.on_end_of_stream(); !res) {
        on_validation_error(mrlog, *this, res);
    }
}

static size_t compute_buffer_size(const schema& s, const flat_mutation_reader_v2::tracked_buffer& buffer)
{
    return boost::accumulate(
        buffer
        | boost::adaptors::transformed([] (const mutation_fragment_v2& mf) {
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
