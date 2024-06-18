/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "mutation.hh"
#include "query-result-writer.hh"
#include "mutation_rebuilder.hh"
#include "mutation/json.hh"
#include "types/collection.hh"
#include "types/tuple.hh"
#include "dht/i_partitioner.hh"
#include "reader_concurrency_semaphore.hh"
#include "readers/from_mutations_v2.hh"

logging::logger mlog("mutation");

mutation::data::data(dht::decorated_key&& key, schema_ptr&& schema)
    : _schema(std::move(schema))
    , _dk(std::move(key))
    , _p(*_schema)
{ }

mutation::data::data(partition_key&& key_, schema_ptr&& schema)
    : _schema(std::move(schema))
    , _dk(dht::decorate_key(*_schema, std::move(key_)))
    , _p(*_schema)
{ }

mutation::data::data(schema_ptr&& schema, dht::decorated_key&& key, const mutation_partition& mp)
    : _schema(schema)
    , _dk(std::move(key))
    , _p(*schema, mp)
{ }

mutation::data::data(schema_ptr&& schema, dht::decorated_key&& key, mutation_partition&& mp)
    : _schema(std::move(schema))
    , _dk(std::move(key))
    , _p(std::move(mp))
{ }

void mutation::set_static_cell(const column_definition& def, atomic_cell_or_collection&& value) {
    partition().static_row().apply(def, std::move(value));
}

void mutation::set_static_cell(const bytes& name, const data_value& value, api::timestamp_type timestamp, ttl_opt ttl) {
    auto column_def = schema()->get_column_definition(name);
    if (!column_def) {
        throw std::runtime_error(format("no column definition found for '{}'", name));
    }
    if (!column_def->is_static()) {
        throw std::runtime_error(format("column '{}' is not static", name));
    }
    partition().static_row().apply(*column_def, atomic_cell::make_live(*column_def->type, timestamp, column_def->type->decompose(value), ttl));
}

void mutation::set_clustered_cell(const clustering_key& key, const bytes& name, const data_value& value,
        api::timestamp_type timestamp, ttl_opt ttl) {
    auto column_def = schema()->get_column_definition(name);
    if (!column_def) {
        throw std::runtime_error(format("no column definition found for '{}'", name));
    }
    return set_clustered_cell(key, *column_def, atomic_cell::make_live(*column_def->type, timestamp, column_def->type->decompose(value), ttl));
}

void mutation::set_clustered_cell(const clustering_key& key, const column_definition& def, atomic_cell_or_collection&& value) {
    auto& row = partition().clustered_row(*schema(), key).cells();
    row.apply(def, std::move(value));
}

void mutation::set_cell(const clustering_key_prefix& prefix, const bytes& name, const data_value& value,
        api::timestamp_type timestamp, ttl_opt ttl) {
    auto column_def = schema()->get_column_definition(name);
    if (!column_def) {
        throw std::runtime_error(format("no column definition found for '{}'", name));
    }
    return set_cell(prefix, *column_def, atomic_cell::make_live(*column_def->type, timestamp, column_def->type->decompose(value), ttl));
}

void mutation::set_cell(const clustering_key_prefix& prefix, const column_definition& def, atomic_cell_or_collection&& value) {
    if (def.is_static()) {
        set_static_cell(def, std::move(value));
    } else if (def.is_regular()) {
        set_clustered_cell(prefix, def, std::move(value));
    } else {
        throw std::runtime_error("attempting to store into a key cell");
    }
}

bool mutation::operator==(const mutation& m) const {
    return decorated_key().equal(*schema(), m.decorated_key())
           && partition().equal(*schema(), m.partition(), *m.schema());
}

uint64_t
mutation::live_row_count(gc_clock::time_point query_time) const {
    return partition().live_row_count(*schema(), query_time);
}

bool
mutation_decorated_key_less_comparator::operator()(const mutation& m1, const mutation& m2) const {
    return m1.decorated_key().less_compare(*m1.schema(), m2.decorated_key());
}

boost::iterator_range<std::vector<mutation>::const_iterator>
slice(const std::vector<mutation>& partitions, const dht::partition_range& r) {
    struct cmp {
        bool operator()(const dht::ring_position& pos, const mutation& m) const {
            return m.decorated_key().tri_compare(*m.schema(), pos) > 0;
        };
        bool operator()(const mutation& m, const dht::ring_position& pos) const {
            return m.decorated_key().tri_compare(*m.schema(), pos) < 0;
        };
    };

    return boost::make_iterator_range(
        r.start()
            ? (r.start()->is_inclusive()
                ? std::lower_bound(partitions.begin(), partitions.end(), r.start()->value(), cmp())
                : std::upper_bound(partitions.begin(), partitions.end(), r.start()->value(), cmp()))
            : partitions.cbegin(),
        r.end()
            ? (r.end()->is_inclusive()
              ? std::upper_bound(partitions.begin(), partitions.end(), r.end()->value(), cmp())
              : std::lower_bound(partitions.begin(), partitions.end(), r.end()->value(), cmp()))
            : partitions.cend());
}

void
mutation::upgrade(const schema_ptr& new_schema) {
    if (_ptr->_schema != new_schema) {
        schema_ptr s = new_schema;
        partition().upgrade(*schema(), *new_schema);
        _ptr->_schema = std::move(s);
    }
}

void mutation::apply(mutation&& m) {
    mutation_application_stats app_stats;
    partition().apply(*schema(), std::move(m.partition()), *m.schema(), app_stats);
}

void mutation::apply(const mutation& m) {
    mutation_application_stats app_stats;
    partition().apply(*schema(), m.partition(), *m.schema(), app_stats);
}

void mutation::apply(const mutation_fragment& mf) {
    partition().apply(*schema(), mf);
}

mutation& mutation::operator=(const mutation& m) {
    return *this = mutation(m);
}

mutation mutation::operator+(const mutation& other) const {
    auto m = *this;
    m.apply(other);
    return m;
}

mutation& mutation::operator+=(const mutation& other) {
    apply(other);
    return *this;
}

mutation& mutation::operator+=(mutation&& other) {
    apply(std::move(other));
    return *this;
}

mutation mutation::sliced(const query::clustering_row_ranges& ranges) const {
    return mutation(schema(), decorated_key(), partition().sliced(*schema(), ranges));
}

mutation mutation::compacted() const {
    auto m = *this;
    m.partition().compact_for_compaction(*schema(), always_gc, m.decorated_key(), gc_clock::time_point::min(), tombstone_gc_state(nullptr));
    return m;
}

size_t mutation::memory_usage(const ::schema& s) const {
    auto res = sizeof(*this);
    if (_ptr) {
        res += sizeof(data);
        res += _ptr->_dk.external_memory_usage();
        res += _ptr->_p.external_memory_usage(s);
    }
    return res;
}

mutation reverse(mutation mut) {
    auto reverse_schema = mut.schema()->make_reversed();
    mutation_rebuilder_v2 reverse_rebuilder(reverse_schema);
    return *std::move(mut).consume(reverse_rebuilder, consume_in_reverse::yes).result;
}

namespace {
class mutation_by_size_splitter {
    struct partition_state {
        mutation_rebuilder_v2 builder;
        size_t empty_partition_size;
        size_t size = 0;
        explicit partition_state(schema_ptr schema)
            : builder(std::move(schema))
        {
        }
    };
    const schema_ptr _schema;
    std::vector<mutation>& _target;
    const size_t _max_size;
    std::optional<partition_state> _state;
    template <typename T>
    stop_iteration consume_fragment(T&& fragment) {
        const auto fragment_size = fragment.memory_usage(*_schema);
        if (_state->size && _state->size + _state->empty_partition_size + fragment_size > _max_size) {
            _target.emplace_back(_state->builder.flush());
            // We could end up with an empty mutation if we consumed a range_tombstone_change
            // and the next fragment exceeds the limit. The tombstone range may not have been
            // closed yet and range_tombstone will not be created.
            // This should be a rare case though, so just pop such mutation.
            if (_target.back().partition().empty()) {
                _target.pop_back();
            }
            _state->size = 0;
        }
        _state->size += fragment_size;
        _state->builder.consume(std::move(fragment));
        return stop_iteration::no;
    }
public:
    mutation_by_size_splitter(schema_ptr schema, std::vector<mutation>& target, size_t max_size)
        : _schema(std::move(schema))
        , _target(target)
        , _max_size(max_size)
    {
    }
    void consume_new_partition(const dht::decorated_key& dk) {
        _state.emplace(_schema);
        _state->empty_partition_size = _state->builder.consume_new_partition(dk).memory_usage(*_schema);
    }
    void consume(tombstone t) {
        _state->builder.consume(t);
    }
    stop_iteration consume(static_row&& sr) {
        return consume_fragment(std::move(sr));
    }
    stop_iteration consume(clustering_row&& cr) {
        return consume_fragment(std::move(cr));
    }
    stop_iteration consume(range_tombstone_change&& rtc) {
        return consume_fragment(std::move(rtc));
    }
    stop_iteration consume_end_of_partition() {
        _state->builder.consume_end_of_partition();
        if (auto mut_opt = _state->builder.consume_end_of_stream(); mut_opt) {
            // This final mutation could be empty if the last consumed fragment was a range_tombstone_change
            // with no timestamp (i.e. a closing rtc), but a range_tombstone ending at this position
            // was already emitted in the previous mutation (because the previous mutation was flushed
            // after consuming a clustering_row at that position).
            if (!mut_opt->partition().empty()) {
                _target.emplace_back(std::move(*mut_opt));
            }
        } else {
            on_internal_error(mlog, "consume_end_of_stream didn't return a mutation");
        }
        _state.reset();
        return stop_iteration::no;
    }
    stop_iteration consume_end_of_stream() {
        return stop_iteration::no;
    }
};
}

future<> split_mutation(mutation source, std::vector<mutation>& target, size_t max_size) {
    reader_concurrency_semaphore sem(reader_concurrency_semaphore::no_limits{}, "split_mutation",
        reader_concurrency_semaphore::register_metrics::no);
    {
        auto s = source.schema();
        auto reader = make_mutation_reader_from_mutations_v2(s,
            sem.make_tracking_only_permit(s, "split_mutation", db::no_timeout, {}),
            std::move(source));
        co_await reader.consume(mutation_by_size_splitter(s, target, max_size));
    }
    co_await sem.stop();
}

auto fmt::formatter<mutation>::format(const mutation& m, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    const ::schema& s = *m.schema();
    const auto& dk = m.decorated_key();

    auto out = ctx.out();
    out = fmt::format_to(out, "{{table: '{}.{}', key: {{", s.ks_name(), s.cf_name());

    auto type_iterator = dk._key.get_compound_type(s)->types().begin();
    auto column_iterator = s.partition_key_columns().begin();

    for (auto&& e : dk._key.components(s)) {
        fmt::format_to(out, "'{}': {}, ", column_iterator->name_as_text(), (*type_iterator)->to_string(to_bytes(e)));
        ++type_iterator;
        ++column_iterator;
    }

    return fmt::format_to(out, "token: {}}}, {}\n}}", dk._token, mutation_partition::printer(s, m.partition()));
}

namespace mutation_json {

void mutation_partition_json_writer::write_each_collection_cell(const collection_mutation_view_description& mv, data_type type,
        std::function<void(atomic_cell_view, data_type)> func) {
    std::function<void(size_t, bytes_view)> write_key;
    std::function<void(size_t, atomic_cell_view)> write_value;
    if (auto t = dynamic_cast<const collection_type_impl*>(type.get())) {
        write_key = [this, t = t->name_comparator()] (size_t, bytes_view k) { _writer.String(t->to_string(k)); };
        write_value = [t = t->value_comparator(), &func] (size_t, atomic_cell_view v) { func(v, t); };
    } else if (auto t = dynamic_cast<const tuple_type_impl*>(type.get())) {
        write_key = [this] (size_t i, bytes_view) { _writer.String(""); };
        write_value = [t, &func] (size_t i, atomic_cell_view v) { func(v, t->type(i)); };
    }

    if (write_key && write_value) {
        _writer.StartArray();
        for (size_t i = 0; i < mv.cells.size(); ++i) {
            _writer.StartObject();
            _writer.Key("key");
            write_key(i, mv.cells[i].first);
            _writer.Key("value");
            write_value(i, mv.cells[i].second);
            _writer.EndObject();
        }
        _writer.EndArray();
    } else {
        _writer.Null();
    }
}

sstring mutation_partition_json_writer::to_string(gc_clock::time_point tp) {
    return fmt::format("{:%F %T}z", fmt::gmtime(gc_clock::to_time_t(tp)));
}

void mutation_partition_json_writer::write_atomic_cell_value(const atomic_cell_view& cell, data_type type) {
    if (type->is_counter()) {
        if (cell.is_counter_update()) {
            _writer.Int64(cell.counter_update_value());
        } else {
            write(counter_cell_view(cell));
        }
    } else {
        _writer.String(type->to_string(cell.value().linearize()));
    }
}

void mutation_partition_json_writer::write_collection_value(const collection_mutation_view_description& mv, data_type type) {
    write_each_collection_cell(mv, type, [&] (atomic_cell_view v, data_type t) { write_atomic_cell_value(v, t); });
}

void mutation_partition_json_writer::write(gc_clock::duration ttl, gc_clock::time_point expiry) {
    _writer.Key("ttl");
    _writer.AsString(ttl);
    _writer.Key("expiry");
    _writer.String(to_string(expiry));
}

void mutation_partition_json_writer::write(const tombstone& t) {
    _writer.StartObject();
    if (t) {
        _writer.Key("timestamp");
        _writer.Int64(t.timestamp);
        _writer.Key("deletion_time");
        _writer.String(to_string(t.deletion_time));
    }
    _writer.EndObject();
}

void mutation_partition_json_writer::write(const row_marker& m) {
    _writer.StartObject();
    _writer.Key("timestamp");
    _writer.Int64(m.timestamp());
    if (m.is_live() && m.is_expiring()) {
        write(m.ttl(), m.expiry());
    }
    _writer.EndObject();
}

void mutation_partition_json_writer::write(counter_cell_view cv) {
    _writer.StartArray();
    for (const auto& shard : cv.shards()) {
        _writer.StartObject();
        _writer.Key("id");
        _writer.AsString(shard.id());
        _writer.Key("value");
        _writer.Int64(shard.value());
        _writer.Key("clock");
        _writer.Int64(shard.logical_clock());
        _writer.EndObject();
    }
    _writer.EndArray();
}

void mutation_partition_json_writer::write(const atomic_cell_view& cell, data_type type, bool include_value) {
    _writer.StartObject();
    _writer.Key("is_live");
    _writer.Bool(cell.is_live());
    _writer.Key("type");
    if (type->is_counter()) {
        if (cell.is_counter_update()) {
            _writer.String("counter-update");
        } else {
            _writer.String("counter-shards");
        }
    } else if (type->is_collection()) {
        _writer.String("frozen-collection");
    } else {
        _writer.String("regular");
    }
    _writer.Key("timestamp");
    _writer.Int64(cell.timestamp());
    if (!type->is_counter()) {
        if (cell.is_live_and_has_ttl()) {
            write(cell.ttl(), cell.expiry());
        }
        if (!cell.is_live()) {
            _writer.Key("deletion_time");
            _writer.String(to_string(cell.deletion_time()));
        }
    }
    if (include_value && (type->is_counter() || cell.is_live())) {
        _writer.Key("value");
        write_atomic_cell_value(cell, type);
    }
    _writer.EndObject();
}
void mutation_partition_json_writer::write(const collection_mutation_view_description& mv, data_type type, bool include_value) {
    _writer.StartObject();

    if (mv.tomb) {
        _writer.Key("tombstone");
        write(mv.tomb);
    }

    _writer.Key("cells");

    write_each_collection_cell(mv, type, [&] (atomic_cell_view v, data_type t) { write(v, t, include_value); });

    _writer.EndObject();
}

void mutation_partition_json_writer::write(const atomic_cell_or_collection& cell, const column_definition& cdef, bool include_value) {
    if (cdef.is_atomic()) {
        write(cell.as_atomic_cell(cdef), cdef.type, include_value);
    } else if (cdef.type->is_collection() || cdef.type->is_user_type()) {
        cell.as_collection_mutation().with_deserialized(*cdef.type, [&, this] (collection_mutation_view_description mv) {
            write(mv, cdef.type, include_value);
        });
    } else {
        _writer.Null();
    }
}

void mutation_partition_json_writer::write(const row& r, column_kind kind, bool include_value) {
    _writer.StartObject();
    r.for_each_cell([this, kind, include_value] (column_id id, const atomic_cell_or_collection& cell) {
        auto cdef = _schema.column_at(kind, id);
        _writer.Key(cdef.name_as_text());
        write(cell, cdef, include_value);
    });
    _writer.EndObject();
}

} // namespace mutation_json
