/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <fmt/std.h>
#include <seastar/core/queue.hh>

#include "tools/json_mutation_stream_parser.hh"
#include "dht/i_partitioner.hh"
#include "utils/rjson.hh"

namespace tools {
namespace {

using reader = rapidjson::GenericReader<rjson::encoding, rjson::encoding, rjson::allocator>;
class stream {
public:
    using Ch = char;
private:
    input_stream<Ch> _is;
    temporary_buffer<Ch> _current;
    size_t _pos = 0;
    size_t _line = 1;
    size_t _last_lf_pos = 0;
private:
    void maybe_read_some() {
        if (!_current.empty()) {
            return;
        }
        _current = _is.read().get();
        // EOS is encoded as null char
        if (_current.empty()) {
            _current = temporary_buffer<Ch>("\0", 1);
        }
    }
public:
    stream(input_stream<Ch> is) : _is(std::move(is)) {
        maybe_read_some();
    }
    stream(stream&&) = default;
    ~stream() {
        _is.close().get();
    }
    Ch Peek() const {
        return *_current.get();
    }
    Ch Take() {
        auto c = Peek();
        if (c == '\n') {
            ++_line;
            ++_last_lf_pos = _pos;
        }
        ++_pos;
        _current.trim_front(1);
        maybe_read_some();
        return c;
    }
    size_t Tell() {
        return _pos;
    }
    // ostream methods, unused but need a definition
    Ch* PutBegin() { return nullptr; }
    void Put(Ch c) { }
    void Flush() { }
    size_t PutEnd(Ch* begin) { return 0; }
    // own methods
    size_t line() const {
        return _line;
    }
    size_t last_line_feed_pos() const {
        return _last_lf_pos;
    }
};
class handler {
public:
    using Ch = char;
private:
    enum class state {
        start,
        before_partition,
        in_partition,
        before_key,
        in_key,
        before_tombstone,
        in_tombstone,
        before_static_columns,
        before_clustering_elements,
        before_clustering_element,
        in_clustering_element,
        in_range_tombstone_change,
        in_clustering_row,
        before_marker,
        in_marker,
        before_clustering_columns,
        before_column_key,
        before_column,
        in_column,
        before_ignored_value,
        before_integer,
        before_string,
        before_bool,
    };
    struct column {
        const column_definition* def = nullptr;
        std::optional<bool> is_live;
        std::optional<api::timestamp_type> timestamp;
        std::optional<bytes> value;
        std::optional<gc_clock::time_point> deletion_time;

        explicit column(const column_definition* def) : def(def) { }
    };
    struct tombstone {
        std::optional<api::timestamp_type> timestamp;
        std::optional<gc_clock::time_point> deletion_time;
    };
private:
    schema_ptr _schema;
    reader_permit _permit;
    queue<mutation_fragment_v2_opt>& _queue;
    logger& _logger;
    circular_buffer<state> _state_stack;
    std::string _key; // last seen key
    bool _partition_start_emited = false;
    bool _is_shadowable = false; // currently processed tombstone is a shadowable one
    std::optional<bool> _bool;
    std::optional<int64_t> _integer;
    std::optional<std::string_view> _string;
    std::optional<partition_key> _pkey;
    std::optional<tombstone> _tombstone;
    std::optional<clustering_key> _ckey;
    std::optional<bound_weight> _bound_weight;
    std::optional<row_marker> _row_marker;
    std::optional<row_tombstone> _row_tombstone;
    std::optional<row> _row;
    std::optional<column> _column;
    std::optional<gc_clock::duration> _ttl;
    std::optional<gc_clock::time_point> _expiry;
private:
    static std::string_view to_string(state s) {
        switch (s) {
            case state::start: return "start";
            case state::before_partition: return "before_partition";
            case state::in_partition: return "in_partition";
            case state::before_key: return "before_key";
            case state::in_key: return "in_key";
            case state::before_tombstone: return "before_tombstone";
            case state::in_tombstone: return "in_tombstone";
            case state::before_static_columns: return "before_static_columns";
            case state::before_clustering_elements: return "before_clustering_elements";
            case state::before_clustering_element: return "before_clustering_element";
            case state::in_clustering_element: return "in_clustering_element";
            case state::in_range_tombstone_change: return "in_range_tombstone_change";
            case state::in_clustering_row: return "in_clustering_row";
            case state::before_marker: return "before_marker";
            case state::in_marker: return "in_marker";
            case state::before_clustering_columns: return "before_clustering_columns";
            case state::before_column_key: return "before_column_key";
            case state::before_column: return "before_column";
            case state::in_column: return "in_column";
            case state::before_ignored_value: return "before_ignored_value";
            case state::before_integer: return "before_integer";
            case state::before_string: return "before_string";
            case state::before_bool: return "before_bool";
        }
        std::abort();
    }

    std::string stack_to_string() const {
        return fmt::to_string(fmt::join(_state_stack | std::views::transform([] (state s) { return to_string(s); }), "|"));
    }

    template<typename... Args>
    bool error(fmt::format_string<Args...> fmt, Args&&... args) {
        auto parse_error = fmt::format(fmt, std::forward<Args>(args)...);
        _logger.trace("{}", parse_error);
        _queue.abort(std::make_exception_ptr(std::runtime_error(parse_error)));
        return false;
    }

    bool emit(mutation_fragment_v2 mf) {
        _logger.trace("emit({})", mf.mutation_fragment_kind());
        _queue.push_eventually(std::move(mf)).get();
        return true;
    }

    bool parse_partition_key() {
        try {
            auto raw = from_hex(*_string);
            _pkey.emplace(partition_key::from_bytes(raw));
        } catch (...) {
            return error("failed to parse partition key from raw string: {}", fmt::streamed(std::current_exception()));
        }
        return true;
    }

    bool parse_clustering_key() {
        try {
            auto raw = from_hex(*_string);
            _ckey.emplace(clustering_key::from_bytes(raw));
        } catch (...) {
            return error("failed to parse clustering key from raw string: {}", fmt::streamed(std::current_exception()));
        }
        return true;
    }

    bool parse_bound_weight() {
        switch (*_integer) {
            case -1:
                _bound_weight.emplace(bound_weight::before_all_prefixed);
                return true;
            case 0:
                _bound_weight.emplace(bound_weight::equal);
                return true;
            case 1:
                _bound_weight.emplace(bound_weight::after_all_prefixed);
                return true;
            default:
                return error("failed to parse bound weight: {} is not a valid bound weight value", *_integer);
        }
    }

    bool parse_deletion_time() {
        try {
            auto dt = gc_clock::time_point(gc_clock::duration(timestamp_from_string(*_string) / 1000));
            if (top(1) == state::in_column) {
                _column->deletion_time = dt;
            } else {
                _tombstone->deletion_time = dt;
            }
            return true;
        } catch (...) {
            return error("failed to parse deletion_time: {}", std::current_exception());
        }
    }

    bool parse_ttl() {
        auto e = _string->end();
        if (*std::prev(e) == 's') {
            --e;
        }
        uint64_t ttl;
        std::stringstream ss(std::string(_string->begin(), e));
        ss >> ttl;
        if (ss.fail()) {
            return error("failed to parse ttl value of {}", _string);
        }
        _ttl = gc_clock::duration(ttl);
        return true;
    }

    bool parse_expiry() {
        try {
            _expiry = gc_clock::time_point(gc_clock::duration(timestamp_from_string(*_string) / 1000));
        } catch (...) {
            return error("failed to parse expiry: {}", std::current_exception());
        }
        return true;
    }

    std::optional<::tombstone> get_tombstone() {
        if (bool(_tombstone->timestamp) != bool(_tombstone->deletion_time)) {
            error("incomplete tombstone: timestamp or deletion-time have to be either both present or missing");
            return {};
        }
        if (!_tombstone->timestamp) {
            _tombstone.reset();
            return ::tombstone{};
        }
        auto tomb = ::tombstone(*_tombstone->timestamp, *_tombstone->deletion_time);
        _tombstone.reset();
        return tomb;
    }

    bool finalize_partition_start(::tombstone tomb = {}) {
        auto pkey = std::exchange(_pkey, {});
        if (!pkey) {
            return error("failed to finalize partition start: no partition key");
        }
        partition_start ps(dht::decorate_key(*_schema, *pkey), tomb);
        _partition_start_emited = true;
        return emit(mutation_fragment_v2(*_schema, _permit, std::move(ps)));
    }

    bool finalize_static_row() {
        if (!_row) {
            return error("failed to finalize clustering row: row is not initialized yet");
        }
        auto row = std::exchange(_row, {});
        auto sr = static_row(std::move(*row));
        return emit(mutation_fragment_v2(*_schema, _permit, std::move(sr)));
    }

    bool finalize_range_tombstone_change() {
        if (!_bound_weight) {
            return error("failed to finalize range tombstone change: missing bound weight");
        }
        if (*_bound_weight == bound_weight::equal) {
            return error("failed to finalize range tombstone change: bound_weight::equal is not valid for range tombstones changes");
        }
        if (!_row_tombstone) {
            return error("failed to finalize range tombstone change: missing tombstone");
        }
        clustering_key ckey = clustering_key::make_empty();
        if (_ckey) {
            ckey = std::move(*std::exchange(_ckey, {}));
        }
        auto pos = position_in_partition(partition_region::clustered, *std::exchange(_bound_weight, {}), std::move(ckey));
        auto tomb = std::exchange(_row_tombstone, {})->tomb();
        auto rtc = range_tombstone_change(std::move(pos), std::move(tomb));
        return emit(mutation_fragment_v2(*_schema, _permit, std::move(rtc)));
    }

    bool finalize_row_marker() {
        if (!_row_marker) {
            return error("failed to finalize row marker: it has no timestamp");
        }
        if (bool(_expiry) != bool(_ttl)) {
            return error("failed to finalize row marker: ttl and expiry must either be both present or both missing");
        }
        if (!_expiry && !_ttl) {
            return true;
        }
        _row_marker->apply(row_marker(_row_marker->timestamp(), *std::exchange(_ttl, {}), *std::exchange(_expiry, {})));
        return true;
    }

    bool parse_column_value() {
        try {
            _column->value.emplace(_column->def->type->from_string(*_string));
        } catch (...) {
            return error("failed to parse cell value: {}", std::current_exception());
        }
        return true;
    }

    bool finalize_column() {
        if (!_row) {
            return error("failed to finalize cell: row not initialized yet");
        }
        if (!_column->is_live || !_column->timestamp) {
            return error("failed to finalize cell: required fields is_live and/or timestamp missing");
        }
        if (*_column->is_live && !_column->value) {
            return error("failed to finalize cell: live cell doesn't have data");
        }
        if (!*_column->is_live && !_column->deletion_time) {
            return error("failed to finalize cell: dead cell doesn't have deletion time");
        }
        if (bool(_expiry) != bool(_ttl)) {
            return error("failed to finalize cell: ttl and expiry must either be both present or both missing");
        }
        if (*_column->is_live) {
            if (_ttl) {
                _row->apply(*_column->def, ::atomic_cell::make_live(*_column->def->type, *_column->timestamp, *_column->value,
                        *std::exchange(_expiry, {}), *std::exchange(_ttl, {})));
            } else {
                _row->apply(*_column->def, ::atomic_cell::make_live(*_column->def->type, *_column->timestamp, *_column->value));
            }
        } else {
            _row->apply(*_column->def, ::atomic_cell::make_dead(*_column->timestamp, *_column->deletion_time));
        }
        _column.reset();
        return true;
    }

    bool finalize_clustering_row() {
        if (!_ckey) {
            return error("failed to finalize clustering row: missing clustering key");
        }
        if (!_row) {
            return error("failed to finalize clustering row: row is not initialized yet");
        }
        auto row = std::exchange(_row, {});
        auto tomb = std::exchange(_row_tombstone, {});
        auto marker = std::exchange(_row_marker, {});
        auto cr = clustering_row(
                std::move(*_ckey),
                tomb.value_or(row_tombstone{}),
                marker.value_or(row_marker{}),
                std::move(*row));
        return emit(mutation_fragment_v2(*_schema, _permit, std::move(cr)));
    }

    bool finalize_partition() {
        _partition_start_emited = false;
        return emit(mutation_fragment_v2(*_schema, _permit, partition_end{}));
    }

    struct retire_state_result {
        bool ok = true;
        unsigned pop_states = 1;
        std::optional<state> next_state;
    };
    retire_state_result handle_retire_state() {
        _logger.trace("handle_retire_state(): stack={}", stack_to_string());
        retire_state_result ret;
        switch (top()) {
            case state::before_partition:
                // EOS
                _queue.push_eventually({}).get();
                break;
            case state::in_partition:
                ret.ok = finalize_partition();
                break;
            case state::in_key:
                ret.pop_states = 2;
                break;
            case state::in_tombstone:
                ret.pop_states = 2;
                {
                    auto is_shadowable = std::exchange(_is_shadowable, false);
                    auto tomb = get_tombstone();
                    if (!tomb) {
                        ret.ok = false;
                        break;
                    }
                    if (top(2) == state::in_partition) {
                        ret.ok = finalize_partition_start(*tomb);
                    } else if (top(2) == state::in_range_tombstone_change) {
                        _row_tombstone.emplace(*tomb);
                    } else if (top(2) == state::in_clustering_row) {
                        if (is_shadowable) {
                            if (!_row_tombstone) {
                                ret.ok = error("cannot apply shadowable tombstone, row tombstone not initialized yet");
                                break;
                            }
                            _row_tombstone->apply(shadowable_tombstone(*tomb), {});
                        } else {
                            _row_tombstone.emplace(*tomb);
                        }
                    } else {
                        ret.ok = error("retiring in_tombstone state in invalid context: {}", stack_to_string());
                    }
                }
                break;
            case state::in_marker:
                ret.pop_states = 2;
                ret.ok = finalize_row_marker();
                break;
            case state::in_column:
                ret.pop_states = 2;
                ret.ok = finalize_column();
                break;
            case state::before_column_key:
                if (top(1) == state::before_static_columns) {
                    ret.ok = finalize_static_row();
                }
                ret.pop_states = 2;
                break;
            case state::before_clustering_element:
                ret.pop_states = 2;
                break;
            case state::in_range_tombstone_change:
                ret.pop_states = 2;
                ret.ok = finalize_range_tombstone_change();
                break;
            case state::in_clustering_row:
                ret.pop_states = 2;
                ret.ok = finalize_clustering_row();
                break;
            case state::before_ignored_value:
                break;
            case state::before_bool:
                if (top(1) == state::in_column) {
                    _column->is_live = _bool;
                }
                _bool.reset();
                break;
            case state::before_integer:
                if (top(1) == state::in_tombstone) {
                    _tombstone->timestamp = _integer.value();
                }
                if (top(1) == state::in_range_tombstone_change) {
                    ret.ok = parse_bound_weight();
                }
                if (top(1) == state::in_column) {
                    _column->timestamp = _integer;
                }
                if (top(1) == state::in_marker) {
                    _row_marker.emplace(_integer.value());
                }
                _integer.reset();
                break;
            case state::before_string:
                if (top(1) == state::in_key) {
                    if (top(3) == state::in_partition) {
                        ret.ok = parse_partition_key();
                    } else if (top(3) == state::in_clustering_row || top(3) == state::in_range_tombstone_change) {
                        ret.ok = parse_clustering_key();
                    }
                } else if (top(1) == state::in_tombstone) {
                    ret.ok = parse_deletion_time();
                } else if (top(1) == state::in_marker) {
                    if (_key == "ttl") {
                        ret.ok = parse_ttl();
                    } else {
                        ret.ok = parse_expiry();
                    }
                } else if (top(1) == state::in_clustering_element) {
                    if (*_string == "clustering-row") {
                        ret.next_state = state::in_clustering_row;
                    } else if (*_string == "range-tombstone-change") {
                        ret.next_state = state::in_range_tombstone_change;
                    } else {
                        ret.ok = error("invalid clustering element type: {}, expected clustering-row or range-tombstone-change", *_string);
                    }
                } else if (top(1) == state::in_column) {
                    if (_key == "type") {
                        if (*_string != "regular") {
                            ret.ok = error("unsupported cell type {}, currently only regular cells are supported", *_string);
                        } else {
                            ret.ok = true;
                        }
                    } else if (_key == "ttl") {
                        ret.ok = parse_ttl();
                    } else if (_key == "expiry") {
                        ret.ok = parse_expiry();
                    } else if (_key == "deletion_time") {
                        ret.ok = parse_deletion_time();
                    } else {
                        ret.ok = parse_column_value();
                    }
                }
                _string.reset();
                break;
            default:
                ret.ok =  error("attempted to retire unexpected state {} ({})", to_string(top()), stack_to_string());
                break;
        }
        return ret;
    }
    state top(size_t i = 0) const {
        return _state_stack[i];
    }
    bool push(state s) {
        _logger.trace("push({})", to_string(s));
        _state_stack.push_front(s);
        return true;
    }
    bool pop() {
        auto res = handle_retire_state();
        _logger.trace("pop({})", res.ok ? res.pop_states : 0);
        if (!res.ok) {
            return false;
        }
        while (res.pop_states--) {
            _state_stack.pop_front();
        }
        if (res.next_state) {
            push(*res.next_state);
        }
        return true;
    }
    bool unexpected(seastar::compat::source_location sl = seastar::compat::source_location::current()) {
        return error("unexpected json event {} in state {}", sl.function_name(), stack_to_string());
    }
    bool unexpected(std::string_view key, seastar::compat::source_location sl = seastar::compat::source_location::current()) {
        return error("unexpected json event {}({}) in state {}", sl.function_name(), key, stack_to_string());
    }
public:
    explicit handler(schema_ptr schema, reader_permit permit, queue<mutation_fragment_v2_opt>& queue, logger& logger)
        : _schema(std::move(schema))
        , _permit(std::move(permit))
        , _queue(queue)
        , _logger(logger)
    {
        push(state::start);
    }
    handler(handler&&) = default;
    bool Null() {
        _logger.trace("Null()");
        switch (top()) {
            case state::before_ignored_value:
                return pop();
            default:
                return unexpected();
        }
        return true;
    }
    bool Bool(bool b) {
        _logger.trace("Bool({})", b);
        switch (top()) {
            case state::before_bool:
                _bool.emplace(b);
                return pop();
            default:
                return unexpected();
        }
        return true;
    }
    bool Int(int i) {
        _logger.trace("Int({})", i);
        switch (top()) {
            case state::before_ignored_value:
                return pop();
            case state::before_integer:
                _integer.emplace(i);
                return pop();
            default:
                return unexpected();
        }
        return true;
    }
    bool Uint(unsigned i) {
        _logger.trace("Uint({})", i);
        switch (top()) {
            case state::before_ignored_value:
                return pop();
            case state::before_integer:
                _integer.emplace(i);
                return pop();
            default:
                return unexpected();
        }
        return true;
    }
    bool Int64(int64_t i) {
        _logger.trace("Int64({})", i);
        switch (top()) {
            case state::before_ignored_value:
                return pop();
            case state::before_integer:
                _integer.emplace(i);
                return pop();
            default:
                return unexpected();
        }
        return true;
    }
    bool Uint64(uint64_t i) {
        _logger.trace("Uint64({})", i);
        switch (top()) {
            case state::before_ignored_value:
                return pop();
            case state::before_integer:
                _integer.emplace(i);
                return pop();
            default:
                return unexpected();
        }
        return true;
    }
    bool Double(double d) {
        _logger.trace("Double({})", d);
        switch (top()) {
            case state::before_ignored_value:
                return pop();
            default:
                return unexpected();
        }
        return true;
    }
    bool RawNumber(const Ch* str, rapidjson::SizeType length, bool copy) {
        _logger.trace("RawNumber({})", std::string_view(str, length));
        return unexpected();
    }
    bool String(const Ch* str, rapidjson::SizeType length, bool copy) {
        _logger.trace("String({})", std::string_view(str, length));
        switch (top()) {
            case state::before_ignored_value:
                return pop();
            case state::before_string:
                _string.emplace(str, length);
                return pop();
            default:
                return unexpected();
        }
        return true;
    }
    bool StartObject() {
        _logger.trace("StartObject()");
        switch (top()) {
            case state::before_partition:
                return push(state::in_partition);
            case state::before_key:
                return push(state::in_key);
            case state::before_tombstone:
                _tombstone.emplace();
                return push(state::in_tombstone);
            case state::before_static_columns:
                _row.emplace();
                return push(state::before_column_key);
            case state::before_clustering_element:
                _row.emplace();
                return push(state::in_clustering_element);
            case state::before_marker:
                return push(state::in_marker);
            case state::before_clustering_columns:
                return push(state::before_column_key);
            case state::before_column:
                return push(state::in_column);
            default:
                return unexpected();
        }
    }
    bool Key(const Ch* str, rapidjson::SizeType length, bool copy) {
        _key = std::string(str, length);
        _logger.trace("Key({})", _key);
        switch (top()) {
            case state::in_partition:
                if (_key == "key") {
                    return push(state::before_key);
                }
                if (_key == "tombstone") {
                    return push(state::before_tombstone);
                }
                if (_key == "static_row" || _key == "clustering_elements") {
                    if (!_partition_start_emited && !finalize_partition_start()) {
                        return false;
                    }
                    if (_key == "static_row") {
                        return push(state::before_static_columns);
                    } else {
                        return push(state::before_clustering_elements);
                    }
                }
                return unexpected(_key);
            case state::in_key:
                if (_key == "value" || (top(2) == state::in_partition && _key == "token")) {
                    return push(state::before_ignored_value);
                }
                if (_key == "raw") {
                    return push(state::before_string);
                }
                return unexpected(_key);
            case state::in_tombstone:
                if (_key == "timestamp") {
                    return push(state::before_integer);
                }
                if (_key == "deletion_time") {
                    return push(state::before_string);
                }
                return unexpected(_key);
            case state::in_marker:
                if (_key == "timestamp") {
                    return push(state::before_integer);
                }
                if (_key == "ttl" || _key == "expiry") {
                    return push(state::before_string);
                }
                return unexpected(_key);
            case state::in_clustering_element:
                if (_key == "type") {
                    return push(state::before_string);
                }
                return unexpected(_key);
            case state::in_range_tombstone_change:
                if (_key == "key") {
                    return push(state::before_key);
                }
                if (_key == "weight") {
                    return push(state::before_integer);
                }
                if (_key == "tombstone") {
                    return push(state::before_tombstone);
                }
                return unexpected(_key);
            case state::in_clustering_row:
                if (_key == "key") {
                    return push(state::before_key);
                }
                if (_key == "marker") {
                    return push(state::before_marker);
                }
                if (_key == "tombstone") {
                    return push(state::before_tombstone);
                }
                if (_key == "shadowable_tombstone") {
                    _is_shadowable = true;
                    return push(state::before_tombstone);
                }
                if (_key == "columns") {
                    return push(state::before_clustering_columns);
                }
                return unexpected(_key);
            case state::before_column_key:
                _column.emplace(_schema->get_column_definition(bytes(reinterpret_cast<bytes::const_pointer>(_key.data()), _key.size())));
                if (!_column->def) {
                    return error("failed to look-up column name {}", _key);
                }
                if (top(1) == state::before_static_columns && _column->def->kind != column_kind::static_column) {
                    return error("cannot add column {} of kind {} to static row", _key, to_sstring(_column->def->kind));
                }
                if (top(1) == state::before_clustering_columns && _column->def->kind != column_kind::regular_column) {
                    return error("cannot add column {} of kind {} to regular row", _key, to_sstring(_column->def->kind));
                }
                if (!_column->def->is_atomic()) {
                    return error("failed to initialize column {}: non-atomic columns are not supported yet", _key);
                }
                return push(state::before_column);
            case state::in_column:
                if (_key == "is_live") {
                    return push(state::before_bool);
                }
                if (_key == "timestamp") {
                    return push(state::before_integer);
                }
                if (_key == "type" || _key == "ttl" || _key == "expiry" || _key == "value" || _key == "deletion_time") {
                    return push(state::before_string);
                }
                return unexpected(_key);
            default:
                return unexpected(_key);
        }
    }
    bool EndObject(rapidjson::SizeType memberCount) {
        _logger.trace("EndObject()");
        switch (top()) {
            case state::in_partition:
            case state::in_key:
            case state::in_tombstone:
            case state::in_range_tombstone_change:
            case state::in_clustering_row:
            case state::before_column_key:
            case state::in_marker:
            case state::in_column:
                return pop();
            default:
                return unexpected();
        }
    }
    bool StartArray() {
        _logger.trace("StartArray()");
        switch (top()) {
            case state::start:
                return push(state::before_partition);
            case state::before_clustering_elements:
                return push(state::before_clustering_element);
            default:
                return unexpected();
        }
    }
    bool EndArray(rapidjson::SizeType elementCount) {
        _logger.trace("EndArray({})", elementCount);
        switch (top()) {
            case state::before_clustering_element:
            case state::before_partition:
                return pop();
            default:
                return unexpected();
        }
    }
};

struct parsing_aborted : public std::exception { };

} // anonymous namespace

class json_mutation_stream_parser::impl {
    queue<mutation_fragment_v2_opt> _queue;
    stream _stream;
    logger& _logger;
    handler _handler;
    reader _reader;
    thread _thread;

public:
    impl(schema_ptr schema, reader_permit permit, input_stream<char> istream, logger& logger)
        : _queue(1)
        , _stream(std::move(istream))
        , _logger(logger)
        , _handler(std::move(schema), std::move(permit), _queue, _logger)
        , _thread([this] { _reader.Parse(_stream, _handler); })
    { }
    ~impl() {
        _queue.abort(std::make_exception_ptr(parsing_aborted{}));
        try {
            _thread.join().get();
        } catch (...) {
            _logger.warn("json_mutation_stream_parser: parser thread exited with exception: {}", std::current_exception());
        }
    }
    future<mutation_fragment_v2_opt> operator()() {
        return _queue.pop_eventually().handle_exception([this] (std::exception_ptr e) -> mutation_fragment_v2_opt {
            auto err_off = _reader.GetErrorOffset();
            throw std::runtime_error(fmt::format("parsing input failed at line {}, offset {}: {}", _stream.line(), err_off - _stream.last_line_feed_pos(), e));
        });
    }
};

json_mutation_stream_parser::json_mutation_stream_parser(schema_ptr schema, reader_permit permit, input_stream<char> istream, logger& logger)
    : _impl(std::make_unique<impl>(std::move(schema), std::move(permit), std::move(istream), logger))
{ }

json_mutation_stream_parser::json_mutation_stream_parser(json_mutation_stream_parser&&) noexcept = default;

json_mutation_stream_parser::~json_mutation_stream_parser() = default;

future<mutation_fragment_v2_opt> json_mutation_stream_parser::operator()() { return (*_impl)(); }

} // namespace tools
