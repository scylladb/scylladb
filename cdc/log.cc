/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <utility>
#include <algorithm>

#include <boost/range/irange.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <seastar/core/thread.hh>
#include <seastar/core/metrics.hh>

#include "cdc/log.hh"
#include "cdc/generation.hh"
#include "cdc/split.hh"
#include "cdc/cdc_options.hh"
#include "cdc/change_visitor.hh"
#include "cdc/metadata.hh"
#include "bytes.hh"
#include "database.hh"
#include "db/schema_tables.hh"
#include "partition_slice_builder.hh"
#include "schema.hh"
#include "schema_builder.hh"
#include "service/migration_listener.hh"
#include "service/storage_proxy.hh"
#include "types/tuple.hh"
#include "cql3/statements/select_statement.hh"
#include "cql3/multi_column_relation.hh"
#include "cql3/tuples.hh"
#include "cql3/untyped_result_set.hh"
#include "log.hh"
#include "utils/rjson.hh"
#include "utils/UUID_gen.hh"
#include "utils/managed_bytes.hh"
#include "utils/fragment_range.hh"
#include "types.hh"
#include "concrete_types.hh"
#include "types/listlike_partial_deserializing_iterator.hh"
#include "tracing/trace_state.hh"
#include "stats.hh"
#include "compaction/compaction_strategy.hh"

namespace std {

template<> struct hash<std::pair<net::inet_address, unsigned int>> {
    std::size_t operator()(const std::pair<net::inet_address, unsigned int> &p) const {
        return std::hash<net::inet_address>{}(p.first) ^ std::hash<int>{}(p.second);
    }
};

}

using namespace std::chrono_literals;

logging::logger cdc_log("cdc");

namespace cdc {
static schema_ptr create_log_schema(const schema&, std::optional<utils::UUID> = {});
}

static constexpr auto cdc_group_name = "cdc";

void cdc::stats::parts_touched_stats::register_metrics(seastar::metrics::metric_groups& metrics, std::string_view suffix) {
    namespace sm = seastar::metrics;
    auto register_part = [&] (part_type part, sstring part_name) {
        metrics.add_group(cdc_group_name, {
                sm::make_total_operations(format("operations_on_{}_performed_{}", part_name, suffix), count[(size_t)part],
                        sm::description(format("number of {} CDC operations that processed a {}", suffix, part_name)),
                        {})
            });
    };

    register_part(part_type::STATIC_ROW, "static_row");
    register_part(part_type::CLUSTERING_ROW, "clustering_row");
    register_part(part_type::MAP, "map");
    register_part(part_type::SET, "set");
    register_part(part_type::LIST, "list");
    register_part(part_type::UDT, "udt");
    register_part(part_type::RANGE_TOMBSTONE, "range_tombstone");
    register_part(part_type::PARTITION_DELETE, "partition_delete");
    register_part(part_type::ROW_DELETE, "row_delete");
}

cdc::stats::stats() {
    namespace sm = seastar::metrics;

    auto register_counters = [this] (counters& counters, sstring kind) {
        const auto split_label = sm::label("split");
        _metrics.add_group(cdc_group_name, {
                sm::make_total_operations("operations_" + kind, counters.unsplit_count,
                        sm::description(format("number of {} CDC operations", kind)),
                        {split_label(false)}),

                sm::make_total_operations("operations_" + kind, counters.split_count,
                        sm::description(format("number of {} CDC operations", kind)),
                        {split_label(true)}),

                sm::make_total_operations("preimage_selects_" + kind, counters.preimage_selects,
                        sm::description(format("number of {} preimage queries performed", kind)),
                        {}),

                sm::make_total_operations("operations_with_preimage_" + kind, counters.with_preimage_count,
                        sm::description(format("number of {} operations that included preimage", kind)),
                        {}),

                sm::make_total_operations("operations_with_postimage_" + kind, counters.with_postimage_count,
                        sm::description(format("number of {} operations that included postimage", kind)),
                        {})
            });

        counters.touches.register_metrics(_metrics, kind);
    };
    register_counters(counters_total, "total");
    register_counters(counters_failed, "failed");
}

cdc::operation_result_tracker::~operation_result_tracker() {
    auto update_stats = [this] (stats::counters& counters) {
        if (_details.was_split) {
            counters.split_count++;
        } else {
            counters.unsplit_count++;
        }
        counters.touches.apply(_details.touched_parts);
        if (_details.had_preimage) {
            counters.with_preimage_count++;
        }
        if (_details.had_postimage) {
            counters.with_postimage_count++;
        }
    };

    update_stats(_stats.counters_total);
    if (_failed) {
        update_stats(_stats.counters_failed);
    }
}

class cdc::cdc_service::impl : service::migration_listener::empty_listener {
    friend cdc_service;
    db_context _ctxt;
    bool _stopped = false;
public:
    impl(db_context ctxt)
        : _ctxt(std::move(ctxt))
    {
        _ctxt._migration_notifier.register_listener(this);
    }
    ~impl() {
        assert(_stopped);
    }

    future<> stop() {
        return _ctxt._migration_notifier.unregister_listener(this).then([this] {
            _stopped = true;
        });
    }

    void on_before_create_column_family(const schema& schema, std::vector<mutation>& mutations, api::timestamp_type timestamp) override {
        if (schema.cdc_options().enabled()) {
            auto& db = _ctxt._proxy.get_db().local();
            auto logname = log_name(schema.cf_name());
            check_that_cdc_log_table_does_not_exist(db, schema, logname);
            ensure_that_table_has_no_counter_columns(schema);

            // in seastar thread
            auto log_schema = create_log_schema(schema);
            auto& keyspace = db.find_keyspace(schema.ks_name());

            auto log_mut = db::schema_tables::make_create_table_mutations(keyspace.metadata(), log_schema, timestamp);

            mutations.insert(mutations.end(), std::make_move_iterator(log_mut.begin()), std::make_move_iterator(log_mut.end()));
        }
    }

    void on_before_update_column_family(const schema& new_schema, const schema& old_schema, std::vector<mutation>& mutations, api::timestamp_type timestamp) override {
        bool is_cdc = new_schema.cdc_options().enabled();
        bool was_cdc = old_schema.cdc_options().enabled();

        // we need to create or modify the log & stream schemas iff either we changed cdc status (was != is)
        // or if cdc is on now unconditionally, since then any actual base schema changes will affect the column 
        // etc.
        if (was_cdc || is_cdc) {
            auto& db = _ctxt._proxy.get_db().local();

            if (!was_cdc) {
                check_that_cdc_log_table_does_not_exist(db, new_schema, log_name(new_schema.cf_name()));
            }
            if (is_cdc) {
                check_for_attempt_to_create_nested_cdc_log(db, new_schema);
                ensure_that_table_has_no_counter_columns(new_schema);
            }

            auto logname = log_name(old_schema.cf_name());
            auto& keyspace = db.find_keyspace(old_schema.ks_name());
            auto log_schema = was_cdc ? db.find_column_family(old_schema.ks_name(), logname).schema() : nullptr;

            if (!is_cdc) {
                auto log_mut = db::schema_tables::make_drop_table_mutations(keyspace.metadata(), log_schema, timestamp);

                mutations.insert(mutations.end(), std::make_move_iterator(log_mut.begin()), std::make_move_iterator(log_mut.end()));
                return;
            }

            auto new_log_schema = create_log_schema(new_schema, log_schema ? std::make_optional(log_schema->id()) : std::nullopt);

            auto log_mut = log_schema 
                ? db::schema_tables::make_update_table_mutations(db, keyspace.metadata(), log_schema, new_log_schema, timestamp, false)
                : db::schema_tables::make_create_table_mutations(keyspace.metadata(), new_log_schema, timestamp)
                ;

            mutations.insert(mutations.end(), std::make_move_iterator(log_mut.begin()), std::make_move_iterator(log_mut.end()));
        }
    }

    void on_before_drop_column_family(const schema& schema, std::vector<mutation>& mutations, api::timestamp_type timestamp) override {
        if (schema.cdc_options().enabled()) {
            auto logname = log_name(schema.cf_name());
            auto& db = _ctxt._proxy.get_db().local();
            auto& keyspace = db.find_keyspace(schema.ks_name());
            auto log_schema = db.find_column_family(schema.ks_name(), logname).schema();

            auto log_mut = db::schema_tables::make_drop_table_mutations(keyspace.metadata(), log_schema, timestamp);

            mutations.insert(mutations.end(), std::make_move_iterator(log_mut.begin()), std::make_move_iterator(log_mut.end()));
        }
    }

    future<std::tuple<std::vector<mutation>, lw_shared_ptr<cdc::operation_result_tracker>>> augment_mutation_call(
        lowres_clock::time_point timeout,
        std::vector<mutation>&& mutations,
        tracing::trace_state_ptr tr_state,
        db::consistency_level write_cl
    );

    template<typename Iter>
    future<> append_mutations(Iter i, Iter e, schema_ptr s, lowres_clock::time_point, std::vector<mutation>&);

private:
    static void check_for_attempt_to_create_nested_cdc_log(database& db, const schema& schema) {
        const auto& cf_name = schema.cf_name();
        const auto cf_name_view = std::string_view(cf_name.data(), cf_name.size());
        if (is_log_for_some_table(db, schema.ks_name(), cf_name_view)) {
            throw exceptions::invalid_request_exception(format("Cannot create a CDC log for a table {}.{}, because creating nested CDC logs is not allowed",
                    schema.ks_name(), schema.cf_name()));
        }
    }

    static void check_that_cdc_log_table_does_not_exist(database& db, const schema& schema, const sstring& logname) {
        if (db.has_schema(schema.ks_name(), logname)) {
            throw exceptions::invalid_request_exception(format("Cannot create CDC log table for table {}.{} because a table of name {}.{} already exists",
                    schema.ks_name(), schema.cf_name(),
                    schema.ks_name(), logname));
        }
    }

    static void ensure_that_table_has_no_counter_columns(const schema& schema) {
        if (schema.is_counter()) {
            throw exceptions::invalid_request_exception(format("Cannot create CDC log for table {}.{}. Counter support not implemented",
                    schema.ks_name(), schema.cf_name()));
        }
    }
};

cdc::cdc_service::cdc_service(service::storage_proxy& proxy, cdc::metadata& cdc_metadata, service::migration_notifier& notifier)
    : cdc_service(db_context(proxy, cdc_metadata, notifier))
{}

cdc::cdc_service::cdc_service(db_context ctxt)
    : _impl(std::make_unique<impl>(std::move(ctxt)))
{
    _impl->_ctxt._proxy.set_cdc_service(this);
}

future<> cdc::cdc_service::stop() {
    _impl->_ctxt._proxy.set_cdc_service(nullptr);
    return _impl->stop();
}

cdc::cdc_service::~cdc_service() = default;

namespace {
static const sstring delta_mode_string_keys = "keys";
static const sstring delta_mode_string_full = "full";

static const std::string_view image_mode_string_on  = "on";
static const std::string_view image_mode_string_off = "off";
static const std::string_view image_mode_string_full = delta_mode_string_full;

sstring to_string(cdc::delta_mode dm) {
    switch (dm) {
        case cdc::delta_mode::keys : return delta_mode_string_keys;
        case cdc::delta_mode::full : return delta_mode_string_full;
    }
    throw std::logic_error("Impossible value of cdc::delta_mode");
}

sstring to_string(cdc::image_mode m) {
    switch (m) {
        case cdc::image_mode::off : return "false";
        case cdc::image_mode::on : return "true";
        case cdc::image_mode::full : return sstring(image_mode_string_full);
    }
    throw std::logic_error("Impossible value of cdc::image_mode");
}

} // anon. namespace

std::ostream& cdc::operator<<(std::ostream& os, delta_mode m) {
    return os << to_string(m);
}

std::ostream& cdc::operator<<(std::ostream& os, image_mode m) {
    return os << to_string(m);
}

cdc::options::options(const std::map<sstring, sstring>& map) {
    if (!map.contains("enabled")) {
        return;
    }

    for (auto& p : map) {
        auto key = p.first;
        auto val = p.second;

        std::transform(key.begin(), key.end(), key.begin(), ::tolower);
        std::transform(val.begin(), val.end(), val.begin(), ::tolower);

        auto is_true = val == "true" || val == "1";
        auto is_false = val == "false" || val == "0";

        if (key == "enabled") {
            if (is_true || is_false) {
                _enabled = is_true;    
            } else {
                throw exceptions::configuration_exception("Invalid value for CDC option \"enabled\": " + p.second);
            }
        } else if (key == "preimage") {
            if (is_true || val == image_mode_string_on) {
                _preimage = image_mode::on;
            } else if (val == image_mode_string_full) {
                _preimage = image_mode::full;
            } else if (val == image_mode_string_off || is_false) {
                _preimage = image_mode::off;
            } else {
                throw exceptions::configuration_exception("Invalid value for CDC option \"preimage\": " + p.second);
            }
        } else if (key == "postimage") {
            if (is_true || is_false) {
                _postimage = is_true;    
            } else {
                throw exceptions::configuration_exception("Invalid value for CDC option \"postimage\": " + p.second);
            }
        } else if (key == "delta") {
            if (val == delta_mode_string_keys) {
                _delta_mode = delta_mode::keys;
            } else if (val != delta_mode_string_full) {
                throw exceptions::configuration_exception("Invalid value for CDC option \"delta\": " + p.second);
            }
        } else if (key == "ttl") {
            try {
                _ttl = std::stoi(p.second);
            } catch (std::invalid_argument& e) {
                throw exceptions::configuration_exception("Invalid value for CDC option \"ttl\": " + p.second);
            } catch (std::out_of_range& e) {
                throw exceptions::configuration_exception("Invalid CDC option: ttl too large");
            }
            if (_ttl < 0) {
                throw exceptions::configuration_exception("Invalid CDC option: ttl must be >= 0");
            }
        } else {
            throw exceptions::configuration_exception("Invalid CDC option: " + p.first);
        }
    }
}

std::map<sstring, sstring> cdc::options::to_map() const {
    if (!_enabled) {
        return {};
    }
    return {
        { "enabled", _enabled ? "true" : "false" },
        { "preimage", to_string(_preimage) },
        { "postimage", _postimage ? "true" : "false" },
        { "delta", to_string(_delta_mode) },
        { "ttl", std::to_string(_ttl) },
    };
}

sstring cdc::options::to_sstring() const {
    return rjson::print(rjson::from_string_map(to_map()));
}

bool cdc::options::operator==(const options& o) const {
    return _enabled == o._enabled && _preimage == o._preimage && _postimage == o._postimage && _ttl == o._ttl
            && _delta_mode == o._delta_mode;
}
bool cdc::options::operator!=(const options& o) const {
    return !(*this == o);
}

namespace cdc {

using operation_native_type = std::underlying_type_t<operation>;

static const sstring cdc_log_suffix = "_scylla_cdc_log";
static const sstring cdc_meta_column_prefix = "cdc$";
static const sstring cdc_deleted_column_prefix = cdc_meta_column_prefix + "deleted_";
static const sstring cdc_deleted_elements_column_prefix = cdc_meta_column_prefix + "deleted_elements_";

static bool is_log_name(const std::string_view& table_name) {
    return boost::ends_with(table_name, cdc_log_suffix);
}

bool is_cdc_metacolumn_name(const sstring& name) {
    return name.compare(0, cdc_meta_column_prefix.size(), cdc_meta_column_prefix) == 0;
}

bool is_log_for_some_table(const database& db, const sstring& ks_name, const std::string_view& table_name) {
    auto base_schema = get_base_table(db, ks_name, table_name);
    if (!base_schema) {
        return false;
    }
    return base_schema->cdc_options().enabled();
}

schema_ptr get_base_table(const database& db, const schema& s) {
    return get_base_table(db, s.ks_name(), s.cf_name());
}

schema_ptr get_base_table(const database& db, sstring_view ks_name,std::string_view table_name) {
    if (!is_log_name(table_name)) {
        return nullptr;
    }
    // Note: It is legal for a user to directly create a table with name
    // `X_scylla_cdc_log`. A table with name `X` might be present (but with
    // cdc log disabled), or not present at all when creating `X_scylla_cdc_log`.
    // Therefore, existence of `X_scylla_cdc_log` does not imply existence of `X`
    // and, in order not to throw, we explicitly need to check for existence of 'X'.
    const auto table_base_name = base_name(table_name);
    if (!db.has_schema(ks_name, table_base_name)) {
        return nullptr;
    }
    return db.find_schema(sstring(ks_name), table_base_name);
}

seastar::sstring base_name(std::string_view log_name) {
    assert(is_log_name(log_name));
    return sstring(log_name.data(), log_name.size() - cdc_log_suffix.size());
}

sstring log_name(std::string_view table_name) {
    return sstring(table_name) + cdc_log_suffix;
}

sstring log_data_column_name(std::string_view column_name) {
    return sstring(column_name);
}

seastar::sstring log_meta_column_name(std::string_view column_name) {
    return cdc_meta_column_prefix + sstring(column_name);
}

bytes log_data_column_name_bytes(const bytes& column_name) {
    return column_name;
}

bytes log_meta_column_name_bytes(const bytes& column_name) {
    return to_bytes(cdc_meta_column_prefix) + column_name;
}

seastar::sstring log_data_column_deleted_name(std::string_view column_name) {
    return cdc_deleted_column_prefix + sstring(column_name);
}

bytes log_data_column_deleted_name_bytes(const bytes& column_name) {
    return to_bytes(cdc_deleted_column_prefix) + column_name;
}

seastar::sstring log_data_column_deleted_elements_name(std::string_view column_name) {
    return cdc_deleted_elements_column_prefix + sstring(column_name);
}

bytes log_data_column_deleted_elements_name_bytes(const bytes& column_name) {
    return to_bytes(cdc_deleted_elements_column_prefix) + column_name;
}

static schema_ptr create_log_schema(const schema& s, std::optional<utils::UUID> uuid) {
    schema_builder b(*s.registry(), s.ks_name(), log_name(s.cf_name()));
    b.with_partitioner("com.scylladb.dht.CDCPartitioner");
    b.set_compaction_strategy(sstables::compaction_strategy_type::time_window);
    b.set_comment(fmt::format("CDC log for {}.{}", s.ks_name(), s.cf_name()));
    auto ttl_seconds = s.cdc_options().ttl();
    if (ttl_seconds > 0) {
        b.set_gc_grace_seconds(0);
        auto ceil = [] (int dividend, int divisor) {
            return dividend / divisor + (dividend % divisor == 0 ? 0 : 1);
        };
        auto seconds_to_minutes = [] (int seconds_value) {
            using namespace std::chrono;
            return std::chrono::ceil<minutes>(seconds(seconds_value)).count();
        };
        // What's the minimum window that won't create more than 24 sstables.
        auto window_seconds = ceil(ttl_seconds, 24);
        auto window_minutes = seconds_to_minutes(window_seconds);
        b.set_compaction_strategy_options({
                {"compaction_window_unit", "MINUTES"},
                {"compaction_window_size", std::to_string(window_minutes)},
                // A new SSTable will become fully expired every
                // `window_seconds` seconds so we shouldn't check for expired
                // sstables too often.
                {"expired_sstable_check_frequency_seconds",
                        std::to_string(std::max(1, window_seconds / 2))},
        });
    }
    b.with_column(log_meta_column_name_bytes("stream_id"), bytes_type, column_kind::partition_key);
    b.with_column(log_meta_column_name_bytes("time"), timeuuid_type, column_kind::clustering_key);
    b.with_column(log_meta_column_name_bytes("batch_seq_no"), int32_type, column_kind::clustering_key);
    b.with_column(log_meta_column_name_bytes("operation"), data_type_for<operation_native_type>());
    b.with_column(log_meta_column_name_bytes("ttl"), long_type);
    b.with_column(log_meta_column_name_bytes("end_of_batch"), boolean_type);
    b.set_caching_options(caching_options::get_disabled_caching_options());
    auto add_columns = [&] (const schema::const_iterator_range_type& columns, bool is_data_col = false) {
        for (const auto& column : columns) {
            auto type = column.type;
            if (type->get_kind() == abstract_type::kind::empty) {
                if (!(s.is_dense() && s.regular_columns_count() == 1)) {
                    on_internal_error(cdc_log, "Unexpected column with EMPTY type");
                }
                continue;
            }
            if (is_data_col && type->is_multi_cell()) {
                type = visit(*type, make_visitor(
                    // non-frozen lists are represented as map<timeuuid, value_type>. Otherwise we cannot express delta
                    [] (const list_type_impl& type) -> data_type {
                        return map_type_impl::get_instance(type.name_comparator(), type.value_comparator(), false);
                    },
                    // everything else is just frozen self
                    [] (const abstract_type& type) {
                        return type.freeze();
                    }
                ));
            }
            b.with_column(log_data_column_name_bytes(column.name()), type);
            if (is_data_col) {
                b.with_column(log_data_column_deleted_name_bytes(column.name()), boolean_type);
            }
            if (column.type->is_multi_cell()) {
                auto dtype = visit(*type, make_visitor(
                    // all collection deletes are set<key_type> (i.e. timeuuid for lists)
                    [] (const collection_type_impl& type) -> data_type {
                        return set_type_impl::get_instance(type.name_comparator(), false);
                    },
                    // user types deletes are a set of the indices removed
                    [] (const user_type_impl& type) -> data_type {
                        return set_type_impl::get_instance(short_type, false);
                    },
                    [] (const abstract_type& type) -> data_type {
                        throw std::invalid_argument("Should not reach");
                    }
                ));
                b.with_column(log_data_column_deleted_elements_name_bytes(column.name()), dtype);
            }
        }
    };
    add_columns(s.partition_key_columns());
    add_columns(s.clustering_key_columns());
    add_columns(s.static_columns(), true);
    add_columns(s.regular_columns(), true);

    if (uuid) {
        b.set_uuid(*uuid);
    }

    return b.build();
}

// iterators for collection merge
template<typename T>
class collection_iterator {
public:
    using iterator_category = std::input_iterator_tag;
    using value_type = const T;
    using difference_type = std::ptrdiff_t;
    using pointer = const T*;
    using reference = const T&;
private:
    managed_bytes_view _v, _next;
    size_t _rem = 0;
    T _current;
public:
    collection_iterator(managed_bytes_view_opt v = {})
        : _v(v.value_or(managed_bytes_view{}))
        , _rem(_v.empty() ? 0 : read_collection_size(_v, cql_serialization_format::internal()))
    {
        if (_rem != 0) {
            parse();
        }
    }
    collection_iterator(const collection_iterator&) = default;
    const T& operator*() const {
        return _current;
    }
    const T* operator->() const {
        return &_current;
    }
    collection_iterator& operator++() {
        next();
        if (_rem != 0) {
            parse();
        } else {
            _current = {};
        }
        return *this;
    }
    collection_iterator operator++(int) {
        auto v = *this;
        ++(*this);
        return v;
    }
    bool operator==(const collection_iterator& x) const {
        return _v == x._v;
    }
    bool operator!=(const collection_iterator& x) const {
        return !(*this == x);
    }
private:
    void next() {
        --_rem;
        _v = _next;
    }
    void parse();
};

template<>
void collection_iterator<std::pair<managed_bytes_view, managed_bytes_view>>::parse() {
    assert(_rem > 0);
    _next = _v;
    auto k = read_collection_value(_next, cql_serialization_format::internal());
    auto v = read_collection_value(_next, cql_serialization_format::internal());
    _current = std::make_pair(k, v);
}

template<>
void collection_iterator<managed_bytes_view>::parse() {
    assert(_rem > 0);
    _next = _v;
    auto k = read_collection_value(_next, cql_serialization_format::internal());
    _current = k;
}

template<typename Container, typename T>
class maybe_back_insert_iterator : public std::back_insert_iterator<Container> {
    const abstract_type& _type;
    collection_iterator<T> _s, _e;
public:
    using value_type = typename Container::value_type;
    maybe_back_insert_iterator(Container& c, const abstract_type& type, collection_iterator<T> s)
        : std::back_insert_iterator<Container>(c)
        , _type(type)
        , _s(s)
    {}
    maybe_back_insert_iterator& operator*() {
        return *this;
    }
    maybe_back_insert_iterator& operator=(const value_type& v) {
        if (!find(v)) {
            std::back_insert_iterator<Container>::operator=(v);
        }
        return *this;
    }
    maybe_back_insert_iterator& operator=(value_type&& v) {
        if (!find(v)) {
            std::back_insert_iterator<Container>::operator=(std::move(v));
        }
        return *this;
    }
private:
    bool find(const value_type& v) {
       // cheating - reducing search span, because we know we only append unique values (see below).
       while (_s != _e) {
           auto n = compare(*_s, v);
           if (n <= 0) {
               ++_s;
           }
           if (n == 0) {
               return true;
           }
           if (n > 0) {
               break;
           }
       }
       return false;
    }
    std::strong_ordering compare(const T&, const value_type& v);
};

template<>
std::strong_ordering maybe_back_insert_iterator<std::vector<std::pair<managed_bytes_view, managed_bytes_view>>, managed_bytes_view>::compare(
        const managed_bytes_view& t, const value_type& v) {
    return _type.compare(t, v.first);
}

template<>
std::strong_ordering maybe_back_insert_iterator<std::vector<managed_bytes_view>, managed_bytes_view>::compare(const managed_bytes_view& t, const value_type& v) {
    return _type.compare(t, v);
}

template<typename Container, typename T>
auto make_maybe_back_inserter(Container& c, const abstract_type& type, collection_iterator<T> s) {
    return maybe_back_insert_iterator<Container, T>(c, type, s);
}

static size_t collection_size(const managed_bytes_opt& bo) {
    if (bo) {
        managed_bytes_view mbv(*bo);
        return read_collection_size(mbv, cql_serialization_format::internal());
    }
    return 0;
}
template<typename Func>
static void udt_for_each(const managed_bytes_opt& bo, Func&& f) {
    if (bo) {
        managed_bytes_view mbv(*bo);
        std::for_each(tuple_deserializing_iterator::start(mbv), tuple_deserializing_iterator::finish(mbv), std::forward<Func>(f));
    }
}
static managed_bytes merge(const collection_type_impl& ctype, const managed_bytes_opt& prev, const managed_bytes_opt& next, const managed_bytes_opt& deleted) {
    std::vector<std::pair<managed_bytes_view, managed_bytes_view>> res;
    res.reserve(collection_size(prev) + collection_size(next));
    auto type = ctype.name_comparator();
    auto cmp = [&type = *type](const std::pair<managed_bytes_view, managed_bytes_view>& p1, const std::pair<managed_bytes_view, managed_bytes_view>& p2) {
        return type.compare(p1.first, p2.first) < 0;
    };
    collection_iterator<std::pair<managed_bytes_view, managed_bytes_view>> e, i(prev), j(next);
    // note order: set_union, when finding doubles, use value from first1 (j here). So
    // since this is next, it has prio
    std::set_union(j, e, i, e, make_maybe_back_inserter(res, *type, collection_iterator<managed_bytes_view>(deleted)), cmp);
    return map_type_impl::serialize_partially_deserialized_form_fragmented(res, cql_serialization_format::internal());
}
static managed_bytes merge(const set_type_impl& ctype, const managed_bytes_opt& prev, const managed_bytes_opt& next, const managed_bytes_opt& deleted) {
    std::vector<managed_bytes_view> res;
    res.reserve(collection_size(prev) + collection_size(next));
    auto type = ctype.name_comparator();
    auto cmp = [&type = *type](managed_bytes_view k1, managed_bytes_view k2) {
        return type.compare(k1, k2) < 0;
    };
    collection_iterator<managed_bytes_view> e, i(prev), j(next), d(deleted);
    std::set_union(j, e, i, e, make_maybe_back_inserter(res, *type, d), cmp);
    return set_type_impl::serialize_partially_deserialized_form_fragmented(res, cql_serialization_format::internal());
}
static managed_bytes merge(const user_type_impl& type, const managed_bytes_opt& prev, const managed_bytes_opt& next, const managed_bytes_opt& deleted) {
    std::vector<managed_bytes_view_opt> res(type.size());
    udt_for_each(prev, [&res, i = res.begin()](managed_bytes_view_opt k) mutable {
        *i++ = k;
    });
    udt_for_each(next, [&res, i = res.begin()](managed_bytes_view_opt k) mutable {
        if (k) {
            *i = k;
        }
        ++i;
    });
    collection_iterator<managed_bytes_view> e, d(deleted);
    std::for_each(d, e, [&res](managed_bytes_view k) {
        auto index = deserialize_field_index(k);
        res[index] = std::nullopt;
    });
    return type.build_value_fragmented(res);
}
static managed_bytes merge(const abstract_type& type, const managed_bytes_opt& prev, const managed_bytes_opt& next, const managed_bytes_opt& deleted) {
    throw std::runtime_error(format("cdc merge: unknown type {}", type.name()));
}

using cell_map = std::unordered_map<const column_definition*, managed_bytes_opt>;
using row_states_map = std::unordered_map<clustering_key, cell_map, clustering_key::hashing, clustering_key::equality>;

static managed_bytes_opt get_col_from_row_state(const cell_map* state, const column_definition& cdef) {
    if (state) {
        if (auto it = state->find(&cdef); it != state->end()) {
            return it->second;
        }
    }
    return std::nullopt;
}

static cell_map* get_row_state(row_states_map& row_states, const clustering_key& ck) {
    auto it = row_states.find(ck);
    return it == row_states.end() ? nullptr : &it->second;
}

static managed_bytes_opt get_preimage_col_value(const column_definition& cdef, const cql3::untyped_result_set_row *pirow) {
    if (!pirow || !pirow->has(cdef.name_as_text())) {
        return std::nullopt;
    }
    return cdef.is_atomic()
        ? pirow->get_blob_fragmented(cdef.name_as_text())
        : visit(*cdef.type, make_visitor(
            // flatten set
            [&] (const set_type_impl& type) {
                auto v = pirow->get_view(cdef.name_as_text());
                auto f = cql_serialization_format::internal();
                auto n = read_collection_size(v, f);
                std::vector<managed_bytes> tmp;
                tmp.reserve(n);
                while (n--) {
                    tmp.emplace_back(read_collection_value(v, f)); // key
                    read_collection_value(v, f); // value. ignore.
                }
                return set_type_impl::serialize_partially_deserialized_form_fragmented({tmp.begin(), tmp.end()}, f);
            },
            [&] (const abstract_type& o) -> managed_bytes {
                return pirow->get_blob_fragmented(cdef.name_as_text());
            }
        ));
}

/* Given a timestamp, generates a timeuuid with the following properties:
 * 1. `t1` < `t2` implies timeuuid_type->less(timeuuid_type->decompose(generate_timeuuid(`t1`)),
 *                                            timeuuid_type->decompose(generate_timeuuid(`t2`))),
 * 2. utils::UUID_gen::micros_timestamp(generate_timeuuid(`t`)) == `t`.
 *
 * If `t1` == `t2`, then generate_timeuuid(`t1`) != generate_timeuuid(`t2`),
 * with unspecified nondeterministic ordering.
 */
utils::UUID generate_timeuuid(api::timestamp_type t) {
    return utils::UUID_gen::get_random_time_UUID_from_micros(std::chrono::microseconds{t});
}

class log_mutation_builder {
    const schema& _base_schema;
    const schema& _log_schema;
    const column_definition& _op_col;
    const column_definition& _ttl_col;

    // The base mutation's partition key
    std::vector<managed_bytes> _base_pk;

    // The cdc$time value of created rows
    const bytes _tuuid;
    // The timestamp of the created log mutation cells
    const api::timestamp_type _ts;
    // The ttl of the created log mutation cells
    const ttl_opt _ttl;

    // Keeps the next cdc$batch_seq_no value
    int _batch_no = 0;

    // The mutation under construction
    mutation& _log_mut;

public:
    log_mutation_builder(mutation& log_mut, api::timestamp_type ts,
                         const partition_key& base_pk, const schema& base_schema)
        : _base_schema(base_schema), _log_schema(*log_mut.schema()),
          _op_col(*_log_schema.get_column_definition(log_meta_column_name_bytes("operation"))),
          _ttl_col(*_log_schema.get_column_definition(log_meta_column_name_bytes("ttl"))),
          _base_pk(base_pk.explode_fragmented()),
          _tuuid(timeuuid_type->decompose(generate_timeuuid(ts))),
          _ts(ts),
          _ttl(_base_schema.cdc_options().ttl()
                  ? std::optional{std::chrono::seconds(_base_schema.cdc_options().ttl())} : std::nullopt),
          _log_mut(log_mut)
    {}

    const schema& base_schema() const {
        return _base_schema;
    }

    clustering_key create_ck(int batch) const {
        return clustering_key::from_exploded(_log_schema, { _tuuid, int32_type->decompose(batch) });
    }

    // Creates a new clustering row in the mutation, assigning it the next `cdc$batch_seq_no`.
    // The numbering of batch sequence numbers starts from 0.
    clustering_key allocate_new_log_row() {
        auto log_ck = create_ck(_batch_no++);
        set_key_columns(log_ck, _base_schema.partition_key_columns(), _base_pk);
        return log_ck;
    }

    bool has_rows() const {
        return _batch_no != 0;
    }

    clustering_key last_row_key() const {
        return create_ck(_batch_no - 1);
    }

    // A common pattern is to allocate a row and then immediately set its `cdc$operation` column.
    clustering_key allocate_new_log_row(operation op) {
        auto log_ck = allocate_new_log_row();
        set_operation(log_ck, op);
        return log_ck;
    }

    // Each clustering key column in the base schema has a corresponding column in the log schema with the same name.
    // This takes a base schema clustering key prefix and sets these columns
    // according to the prefix' values for the given log row.
    void set_clustering_columns(const clustering_key& log_ck, const clustering_key_prefix& base_ckey) {
        set_key_columns(log_ck, _base_schema.clustering_key_columns(), base_ckey.explode_fragmented());
    }

    // Sets the `cdc$operation` column for the given row.
    void set_operation(const clustering_key& log_ck, operation op) {
        _log_mut.set_cell(log_ck, _op_col, atomic_cell::make_live(
                    *_op_col.type, _ts, _op_col.type->decompose(operation_native_type(op)), _ttl));
    }

    // Sets the `cdc$ttl` column for the given row.
    // Warning: if the caller wants `cdc$ttl` to be null, they shouldn't call `set_ttl` with a non-null value.
    // Calling it with a non-null value and then with a null value will keep the non-null value.
    void set_ttl(const clustering_key& log_ck, ttl_opt ttl) {
        if (ttl) {
            _log_mut.set_cell(log_ck, _ttl_col, atomic_cell::make_live(
                        *_ttl_col.type, _ts, _ttl_col.type->decompose(ttl->count()), _ttl));
        }
    }

    // Each regular and static column in the base schema has a corresponding column in the log schema with the same name.
    // Given a reference to such a column from the base schema, this function sets the corresponding column
    // in the log to the given value for the given row.
    void set_value(const clustering_key& log_ck, const column_definition& base_cdef, const managed_bytes_view& value) {
        auto& log_cdef = *_log_schema.get_column_definition(log_data_column_name_bytes(base_cdef.name()));
        _log_mut.set_cell(log_ck, log_cdef, atomic_cell::make_live(*base_cdef.type, _ts, value, _ttl));
    }

    // Each regular and static column in the base schema has a corresponding column in the log schema
    // with boolean type and the name constructed by prefixing the original name with ``cdc$deleted_''
    // Given a reference to such a column from the base schema, this function sets the corresponding column
    // in the log to `true` for the given row. If not called, the column will be `null`.
    void set_deleted(const clustering_key& log_ck, const column_definition& base_cdef) {
        _log_mut.set_cell(log_ck, log_data_column_deleted_name_bytes(base_cdef.name()), data_value(true), _ts, _ttl);
    }

    // Each regular and static non-atomic column in the base schema has a corresponding column in the log schema
    // whose type is a frozen `set` of keys (the types of which depend on the base type) and whose name is constructed
    // by prefixing the original name with ``cdc$deleted_elements_''.
    // Given a reference to such a column from the base schema, this function sets the corresponding column
    // in the log to the given set of keys for the given row.
    void set_deleted_elements(const clustering_key& log_ck, const column_definition& base_cdef, const managed_bytes& deleted_elements) {
        auto& log_cdef = *_log_schema.get_column_definition(log_data_column_deleted_elements_name_bytes(base_cdef.name()));
        _log_mut.set_cell(log_ck, log_cdef, atomic_cell::make_live(*log_cdef.type, _ts, deleted_elements, _ttl));
    }

    void end_record() {
        if (has_rows()) {
            _log_mut.set_cell(last_row_key(), log_meta_column_name_bytes("end_of_batch"), data_value(true), _ts, _ttl);
        }
    }
private:
    void set_key_columns(const clustering_key& log_ck, schema::const_iterator_range_type columns, const std::vector<managed_bytes>& key) {
        size_t pos = 0;
        for (auto& column : columns) {
            if (pos >= key.size()) {
                break;
            }
            auto& cdef = *_log_schema.get_column_definition(log_data_column_name_bytes(column.name()));
            _log_mut.set_cell(log_ck, cdef, atomic_cell::make_live(*column.type, _ts, managed_bytes_view(key[pos]), _ttl));
            ++pos;
        }
    }
};

static managed_bytes get_managed_bytes(const atomic_cell_view& acv) {
    return managed_bytes(acv.value());
}

static ttl_opt get_ttl(const atomic_cell_view& acv) {
    return acv.is_live_and_has_ttl() ? std::optional{acv.ttl()} : std::nullopt;
}

static ttl_opt get_ttl(const row_marker& rm) {
    return rm.is_expiring() ? std::optional{rm.ttl()} : std::nullopt;
}

/**
 * Returns whether we should generate cdc delta values (beyond keys)
 */
static bool generate_delta_values(const schema& s) {
    return s.cdc_options().get_delta_mode() == cdc::delta_mode::full;
}

/* Visits the cells and tombstones of a single base mutation row and constructs corresponding delta-row cells
 * for the corresponding log mutation.
 *
 * Additionally updates state required to produce pre/post-image if configured to do so (`enable_updating_state`).
 */
struct process_row_visitor {
    const clustering_key& _log_ck;

    stats::part_type_set& _touched_parts;

    // The base row being visited gets a single corresponding log delta row.
    // This is the value of the "cdc$ttl" column for that delta row.
    ttl_opt _ttl_column = std::nullopt;

    // Used to create cells in the corresponding delta row in the log mutation.
    log_mutation_builder& _builder;

    /* Images-related state */
    const bool _enable_updating_state = false;

    // Null for the static row, non-null for clustered rows.
    const clustering_key* const _base_ck;

    // The state required to produce pre/post-image for the row being visited.
    // Might be null if the preimage query didn't return a result for this row.
    cell_map* _row_state;

    // The state required to produce pre/post-image for all rows.
    // We need to keep a reference to it since we might insert new row_states during the visitation.
    row_states_map& _clustering_row_states;

    const bool _generate_delta_values = true; 

    process_row_visitor(
            const clustering_key& log_ck, stats::part_type_set& touched_parts, log_mutation_builder& builder,
            bool enable_updating_state, const clustering_key* base_ck, cell_map* row_state,
            row_states_map& clustering_row_states, bool generate_delta_values)
        : _log_ck(log_ck), _touched_parts(touched_parts), _builder(builder),
          _enable_updating_state(enable_updating_state), _base_ck(base_ck), _row_state(row_state),
          _clustering_row_states(clustering_row_states),
          _generate_delta_values(generate_delta_values)
    {}

    void update_row_state(const column_definition& cdef, managed_bytes_opt value) {
        if (!_row_state) {
            // static row always has a valid state, so this must be a clustering row missing
            assert(_base_ck);
            auto [it, _] = _clustering_row_states.try_emplace(*_base_ck);
            _row_state = &it->second;
        }
        (*_row_state)[&cdef] = std::move(value);
    }

    void live_atomic_cell(const column_definition& cdef, const atomic_cell_view& cell) {
        _ttl_column = get_ttl(cell);

        if (cdef.type->get_kind() == abstract_type::kind::empty) {
            return;
        }

        managed_bytes value = get_managed_bytes(cell);

        // delta
        if (_generate_delta_values) {
            _builder.set_value(_log_ck, cdef, value);
        }

        // images
        if (_enable_updating_state) {
            update_row_state(cdef, std::move(value));
        }
    }

    void dead_atomic_cell(const column_definition& cdef, const atomic_cell_view&) {
        // delta
        if (_generate_delta_values) {
            _builder.set_deleted(_log_ck, cdef);
        }
        // images
        if (_enable_updating_state) {
            update_row_state(cdef, std::nullopt);
        }
    }

    void collection_column(const column_definition& cdef, auto&& visit_collection) {
        // The handling of dead cells and the tombstone is common for all collection types,
        // but we need separate visitors for different collection types to handle the live cells.
        // See `set_visitor`, `udt_visitior`, and `map_or_list_visitor` below.
        struct collection_visitor {
            bool _is_column_delete = false;
            std::vector<managed_bytes_view> _deleted_keys;

            ttl_opt& _ttl_column;

            collection_visitor(ttl_opt& ttl_column) : _ttl_column(ttl_column) {}

            void collection_tombstone(const tombstone&) {
                _is_column_delete = true;
            }

            void dead_collection_cell(bytes_view key, const atomic_cell_view&) {
                _deleted_keys.push_back(key);
            }

            constexpr bool finished() const { return false; }
        };


        // cdc$deleted_col, cdc$deleted_elements_col, col
        using result_t = std::tuple<bool, std::vector<managed_bytes_view>, managed_bytes_opt>;
        auto result = visit(*cdef.type, make_visitor(
            [&] (const set_type_impl&) -> result_t {
                _touched_parts.set<stats::part_type::SET>();

                struct set_visitor : public collection_visitor {
                    std::vector<managed_bytes_view> _added_keys;

                    set_visitor(ttl_opt& ttl_column) : collection_visitor(ttl_column) {}

                    void live_collection_cell(bytes_view key, const atomic_cell_view& cell) {
                        this->_ttl_column = get_ttl(cell);
                        _added_keys.emplace_back(key);
                    }
                } v(_ttl_column);

                visit_collection(v);

                managed_bytes_opt added_keys = v._added_keys.empty() ? std::nullopt :
                    std::optional{set_type_impl::serialize_partially_deserialized_form_fragmented(v._added_keys, cql_serialization_format::internal())};

                return {
                    v._is_column_delete,
                    std::move(v._deleted_keys),
                    std::move(added_keys)
                };
            },
            [&] (const user_type_impl& type) -> result_t  {
                _touched_parts.set<stats::part_type::UDT>();

                struct udt_visitor : public collection_visitor {
                    std::vector<managed_bytes_view_opt> _added_cells;

                    udt_visitor(ttl_opt& ttl_column, size_t num_keys)
                        : collection_visitor(ttl_column), _added_cells(num_keys) {}

                    void live_collection_cell(bytes_view key, const atomic_cell_view& cell) {
                        this->_ttl_column = get_ttl(cell);
                        _added_cells[deserialize_field_index(key)].emplace(cell.value());
                    }
                };

                udt_visitor v(_ttl_column, type.size());

                visit_collection(v);

                managed_bytes_opt added_cells = v._added_cells.empty() ? std::nullopt :
                    std::optional{type.build_value_fragmented(v._added_cells)};

                return {
                    v._is_column_delete,
                    std::move(v._deleted_keys),
                    std::move(added_cells)
                };
            },
            [&] (const collection_type_impl& type) -> result_t {
                _touched_parts.set(type.is_list() ? stats::part_type::LIST : stats::part_type::MAP);

                struct map_or_list_visitor : public collection_visitor {
                    std::vector<std::pair<managed_bytes_view, managed_bytes_view>> _added_cells;

                    map_or_list_visitor(ttl_opt& ttl_column)
                        : collection_visitor(ttl_column) {}

                    void live_collection_cell(bytes_view key, const atomic_cell_view& cell) {
                        this->_ttl_column = get_ttl(cell);
                        _added_cells.emplace_back(key, cell.value());
                    }
                };

                map_or_list_visitor v(_ttl_column);

                visit_collection(v);

                managed_bytes_opt added_cells = v._added_cells.empty() ? std::nullopt :
                    std::optional{map_type_impl::serialize_partially_deserialized_form_fragmented(v._added_cells, cql_serialization_format::internal())};

                return {
                    v._is_column_delete,
                    std::move(v._deleted_keys),
                    std::move(added_cells)
                };
            },
            [&] (const abstract_type& o) -> result_t {
                throw std::runtime_error(format("cdc process_change: unknown type {}", o.name()));
            }
        ));
        auto&& is_column_delete = std::get<0>(result);
        auto&& deleted_keys = std::get<1>(result);
        auto&& added_cells = std::get<2>(result);

        // FIXME: we're doing redundant work: first we serialize the set of deleted keys into a blob,
        // then we deserialize again when merging images below
        managed_bytes_opt deleted_elements = std::nullopt;
        if (!deleted_keys.empty()) {
            deleted_elements = set_type_impl::serialize_partially_deserialized_form_fragmented(deleted_keys, cql_serialization_format::internal());
        }

        // delta
        if (_generate_delta_values) {
            if (is_column_delete) {
                _builder.set_deleted(_log_ck, cdef);
            }

            if (deleted_elements) {
                _builder.set_deleted_elements(_log_ck, cdef, *deleted_elements);
            }

            if (added_cells) {
                _builder.set_value(_log_ck, cdef, *added_cells);
            }
        }

        // images
        if (_enable_updating_state) {
            // A column delete overwrites any data we gathered until now.
            managed_bytes_opt prev = is_column_delete ? std::nullopt : get_col_from_row_state(_row_state, cdef);

            managed_bytes_opt next;
            if (added_cells || (deleted_elements && prev)) {
                next = visit(*cdef.type, [&] (const auto& type) -> managed_bytes {
                    return merge(type, prev, added_cells, deleted_elements);
                });
            }

            update_row_state(cdef, std::move(next));
        }
    }

    constexpr bool finished() const { return false; }
};

struct process_change_visitor {
    stats::part_type_set& _touched_parts;

    log_mutation_builder& _builder;

    /* Images-related state */
    const bool _enable_updating_state = false;

    row_states_map& _clustering_row_states;
    cell_map& _static_row_state;

    const bool _generate_delta_values = true;

    void static_row_cells(auto&& visit_row_cells) {
        _touched_parts.set<stats::part_type::STATIC_ROW>();

        auto log_ck = _builder.allocate_new_log_row(operation::update);

        process_row_visitor v(
                log_ck, _touched_parts, _builder,
                _enable_updating_state, nullptr, &_static_row_state, _clustering_row_states, _generate_delta_values);
        visit_row_cells(v);

        _builder.set_ttl(log_ck, v._ttl_column);
    }

    void clustered_row_cells(const clustering_key& ckey, auto&& visit_row_cells) {
        _touched_parts.set<stats::part_type::CLUSTERING_ROW>();

        auto log_ck = _builder.allocate_new_log_row();
        _builder.set_clustering_columns(log_ck, ckey);

        struct clustering_row_cells_visitor : public process_row_visitor {
            operation _cdc_op = operation::update;

            using process_row_visitor::process_row_visitor;

            void marker(const row_marker& rm) {
                _ttl_column = get_ttl(rm);
                _cdc_op = operation::insert;
            }
        };

        clustering_row_cells_visitor v(
                log_ck, _touched_parts, _builder,
                _enable_updating_state, &ckey, get_row_state(_clustering_row_states, ckey),
                _clustering_row_states, _generate_delta_values);
        visit_row_cells(v);

        if (_enable_updating_state) {
            // #7716: if there are no regular columns, our visitor would not have visited any cells,
            // hence it would not have created a row_state for this row. In effect, postimage wouldn't be produced.
            // Ensure that the row state exists.
            _clustering_row_states.try_emplace(ckey);
        }

        _builder.set_operation(log_ck, v._cdc_op);
        _builder.set_ttl(log_ck, v._ttl_column);
    }

    void clustered_row_delete(const clustering_key& ckey, const tombstone&) {
        _touched_parts.set<stats::part_type::ROW_DELETE>();

        auto log_ck = _builder.allocate_new_log_row(operation::row_delete);
        _builder.set_clustering_columns(log_ck, ckey);

        if (_enable_updating_state && get_row_state(_clustering_row_states, ckey)) {
            _clustering_row_states.erase(ckey);
        }
    }

    void range_delete(const range_tombstone& rt) {
        _touched_parts.set<stats::part_type::RANGE_TOMBSTONE>();
        {
            const auto start_operation = rt.start_kind == bound_kind::incl_start
                    ? operation::range_delete_start_inclusive
                    : operation::range_delete_start_exclusive;
            auto log_ck = _builder.allocate_new_log_row(start_operation);
            _builder.set_clustering_columns(log_ck, rt.start);
        }
        {
            const auto end_operation = rt.end_kind == bound_kind::incl_end
                    ? operation::range_delete_end_inclusive
                    : operation::range_delete_end_exclusive;
            auto log_ck = _builder.allocate_new_log_row(end_operation);
            _builder.set_clustering_columns(log_ck, rt.end);
        }

        // #6900 - remove stored row data (for postimage)
        // if it falls inside the clustering range of the
        // tombstone.
        if (!_enable_updating_state) {
            return;
        }

        bound_view::compare cmp(_builder.base_schema());
        auto sb = rt.start_bound();
        auto eb = rt.end_bound();

        std::erase_if(_clustering_row_states, [&](auto& p) {
            auto& ck = p.first;
            return cmp(sb, ck) && !cmp(eb, ck);
        });
    }

    void partition_delete(const tombstone&) {
        _touched_parts.set<stats::part_type::PARTITION_DELETE>();
        auto log_ck = _builder.allocate_new_log_row(operation::partition_delete);
        if (_enable_updating_state) {
            _clustering_row_states.clear();
        }
    }

    constexpr bool finished() const { return false; }
};

class transformer final : public change_processor {
private:
    db_context _ctx;
    schema_ptr _schema;
    dht::decorated_key _dk;
    schema_ptr _log_schema;

    /**
     * #6070, #6084
     * Non-atomic column assignments which use a TTL are broken into two invocations
     * of `transform`, such as in the following example:
     * CREATE TABLE t (a int PRIMARY KEY, b map<int, int>) WITH cdc = {'enabled':true};
     * UPDATE t USING TTL 5 SET b = {0:0} WHERE a = 0;
     *
     * The above UPDATE creates a tombstone and a (0, 0) cell; because tombstones don't have the notion
     * of a TTL, we split the UPDATE into two separate changes (represented as two separate delta rows in the log,
     * resulting in two invocations of `transform`): one change for the deletion with no TTL,
     * and one change for adding cells with TTL = 5.
     *
     * In other words, we use the fact that
     * UPDATE t USING TTL 5 SET b = {0:0} WHERE a = 0;
     * is equivalent to
     * BEGIN UNLOGGED BATCH
     *  UPDATE t SET b = null WHERE a = 0;
     *  UPDATE t USING TTL 5 SET b = b + {0:0} WHERE a = 0;
     * APPLY BATCH;
     * (the mutations are the same in both cases),
     * and perform a separate `transform` call for each statement in the batch.
     *
     * An assignment also happens when an INSERT statement is used as follows:
     * INSERT INTO t (a, b) VALUES (0, {0:0}) USING TTL 5;
     * 
     * This will be split into three separate changes (three invocations of `transform`):
     * 1. One with TTL = 5 for the row marker (introduces by the INSERT), indicating that a row was inserted.
     * 2. One without a TTL for the tombstone, indicating that the collection was cleared.
     * 3. One with TTL = 5 for the addition of cell (0, 0), indicating that the collection
     *    was extended by a new key/value.
     *
     * Why do we need three changes and not two, like in the UPDATE case?
     * The tombstone needs to be a separate change because it doesn't have a TTL,
     * so only the row marker change could potentially be merged with the cell change (1 and 3 above).
     * However, we cannot do that: the row marker change is of INSERT type (cdc$operation == cdc::operation::insert),
     * but there is no way to create a statement that
     * - has a row marker,
     * - adds cells to a collection,
     * - but *doesn't* add a tombstone for this collection.
     * INSERT statements that modify collections *always* add tombstones.
     *
     * Merging the row marker with the cell addition would result in such an impossible statement.
     *
     * Instead, we observe that
     * INSERT INTO t (a, b) VALUES (0, {0:0}) USING TTL 5;
     * is equivalent to
     * BEGIN UNLOGGED BATCH
     *  INSERT INTO t (a) VALUES (0) USING TTL 5;
     *  UPDATE t SET b = null WHERE a = 0;
     *  UPDATE t USING TTL 5 SET b = b + {0:0} WHERE a = 0;
     * APPLY BATCH;
     * and perform a separate `transform` call for each statement in the batch.
     *
     * Unfortunately, due to splitting, the cell addition call (b + b {0:0}) does not know about the tombstone.
     * If it was performed independently from the tombstone call, it would create a wrong post-image:
     * the post-image would look as if the previous cells still existed.
     * For example, suppose that b was equal to {1:1} before the above statement was performed.
     * Then the final post-image for b for above statement/batch would be {0:0, 1:1}, when instead it should be {0:0}.
     *
     * To handle this we use the fact that
     * 1. changes without a TTL are treated as if TTL = 0,
     * 2. `transform` is invoked in order of increasing TTLs,
     * and we maintain state between `transform` invocations (`_clustering_row_states`, `_static_row_state`).
     *
     * Thus, the tombstone call will happen *before* the cell addition call,
     * so the cell addition call will know that there previously was a tombstone
     * and create a correct post-image.
     *
     * Furthermore, `transform` calls for INSERT changes (i.e. with a row marker)
     * happen before `transform` calls for UPDATE changes, so in the case of an INSERT
     * which modifies a collection column as above, the row marker call will happen first;
     * its post-image will still show {1:1} for the collection column. Good.
     */

    row_states_map _clustering_row_states;
    cell_map _static_row_state;

    std::vector<mutation> _result_mutations;
    std::optional<log_mutation_builder> _builder;

    // When enabled, process_change will update _clustering_row_states and _static_row_state
    bool _enable_updating_state = false;

    stats::part_type_set _touched_parts;

public:
    transformer(db_context ctx, schema_ptr s, dht::decorated_key dk)
        : _ctx(ctx)
        , _schema(std::move(s))
        , _dk(std::move(dk))
        , _log_schema(ctx._proxy.get_db().local().find_schema(_schema->ks_name(), log_name(_schema->cf_name())))
        , _clustering_row_states(0, clustering_key::hashing(*_schema), clustering_key::equality(*_schema))
    {
    }

    // DON'T move the transformer after this
    void begin_timestamp(api::timestamp_type ts, bool is_last) override {
        const auto stream_id = _ctx._cdc_metadata.get_stream(ts, _dk.token());
        _result_mutations.emplace_back(_log_schema, stream_id.to_partition_key(*_log_schema));
        _builder.emplace(_result_mutations.back(), ts, _dk.key(), *_schema);
        _enable_updating_state = _schema->cdc_options().postimage() || (!is_last && _schema->cdc_options().preimage());
    }

    void produce_preimage(const clustering_key* ck, const one_kind_column_set& columns_to_include) override {
        // iff we want full preimage, just ignore the affected columns and include everything. 
        generate_image(operation::pre_image, ck, _schema->cdc_options().full_preimage() ? nullptr : &columns_to_include);
    };

    void produce_postimage(const clustering_key* ck) override {
        generate_image(operation::post_image, ck, nullptr);
    }

    void generate_image(operation op, const clustering_key* ck, const one_kind_column_set* affected_columns) {
        assert(op == operation::pre_image || op == operation::post_image);

        // assert that post_image is always full
        assert(!(op == operation::post_image && affected_columns));

        assert(_builder);

        const auto kind = ck ? column_kind::regular_column : column_kind::static_column;

        cell_map* row_state;
        if (ck) {
            row_state = get_row_state(_clustering_row_states, *ck);
            if (!row_state) {
                // We have no data for this row, we can stop here
                // Empty row here means we had data, but column deletes
                // removed it. This we should report as pk/ck only
                return;
            }
        } else {
            row_state = &_static_row_state;
            // if the static row state is empty and we did not touch 
            // during processing, it either did not exist, or we did pk delete. 
            // In those cases, no postimage for you.
            // If we touched it, we should indicate it with an empty 
            // postimage. 
            if (row_state->empty() && !_touched_parts.contains(stats::part_type::STATIC_ROW)) {
                return;
            }
        }

        auto image_ck = _builder->allocate_new_log_row(op);
        if (ck) {
            _builder->set_clustering_columns(image_ck, *ck);
        }

        auto process_cell = [&, this] (const column_definition& cdef) {
            if (auto current = get_col_from_row_state(row_state, cdef)) {
                _builder->set_value(image_ck, cdef, *current);
            } else if (op == operation::pre_image) {
                // Cell is NULL. 
                // If we generate the preimage,
                // we should also fill in the deleted column.
                // Otherwise the user will not be able
                // to discern whether a value in the preimage
                // is NULL or it was not included in the
                // preimage ('full' preimage disabled).
                // If we generate the postimage,
                // we don't have to fill in the deleted column,
                // as all postimages are full.
                _builder->set_deleted(image_ck, cdef);
            }
        };

        if (affected_columns) {
            // Preimage case - include only data from requested columns
            for (auto it = affected_columns->find_first(); it != one_kind_column_set::npos; it = affected_columns->find_next(it)) {
                const auto id = column_id(it);
                const auto& cdef = _schema->column_at(kind, id);
                process_cell(cdef);
            }
        } else {
            // Postimage case - include data from all columns
            const auto column_range = _schema->columns(kind);
            std::for_each(column_range.begin(), column_range.end(), process_cell);
        }
    }

    // TODO: is pre-image data based on query enough. We only have actual column data. Do we need
    // more details like tombstones/ttl? Probably not but keep in mind.
    void process_change(const mutation& m) override {
        assert(_builder);
        process_change_visitor v {
            ._touched_parts = _touched_parts,
            ._builder = *_builder,
            ._enable_updating_state = _enable_updating_state,
            ._clustering_row_states = _clustering_row_states,
            ._static_row_state = _static_row_state,
            ._generate_delta_values = generate_delta_values(_builder->base_schema())
        };
        cdc::inspect_mutation(m, v);
    }

    void end_record() override {
        assert(_builder);
        _builder->end_record();
    }

    // Takes and returns generated cdc log mutations and associated statistics about parts touched during transformer's lifetime.
    // The `transformer` object on which this method was called on should not be used anymore.
    std::tuple<std::vector<mutation>, stats::part_type_set> finish() && {
        return std::make_pair<std::vector<mutation>, stats::part_type_set>(std::move(_result_mutations), std::move(_touched_parts));
    }

    static db::timeout_clock::time_point default_timeout() {
        return db::timeout_clock::now() + 10s;
    }

    future<lw_shared_ptr<cql3::untyped_result_set>> pre_image_select(
            service::client_state& client_state,
            db::consistency_level write_cl,
            const mutation& m)
    {
        auto& p = m.partition();
        if (p.clustered_rows().empty() && p.static_row().empty()) {
            return make_ready_future<lw_shared_ptr<cql3::untyped_result_set>>();
        }

        dht::partition_range_vector partition_ranges{dht::partition_range(m.decorated_key())};

        auto&& pc = _schema->partition_key_columns();
        auto&& cc = _schema->clustering_key_columns();

        std::vector<query::clustering_range> bounds;
        uint64_t row_limit = query::max_rows;

        const bool has_only_static_row = !p.static_row().empty() && p.clustered_rows().empty();
        if (cc.empty() || has_only_static_row) {
            bounds.push_back(query::clustering_range::make_open_ended_both_sides());
            if (has_only_static_row) {
                row_limit = 1;
            }
        } else {
            for (const rows_entry& r : p.clustered_rows()) {
                auto& ck = r.key();
                bounds.push_back(query::clustering_range::make_singular(ck));
            }
        }

        std::vector<const column_definition*> columns;
        columns.reserve(_schema->all_columns().size());

        std::transform(pc.begin(), pc.end(), std::back_inserter(columns), [](auto& c) { return &c; });
        std::transform(cc.begin(), cc.end(), std::back_inserter(columns), [](auto& c) { return &c; });

        query::column_id_vector static_columns, regular_columns;

        // TODO: this assumes all mutations touch the same set of columns. This might not be true, and we may need to do more horrible set operation here.
        if (!p.static_row().empty()) {
            // for postimage we need everything...
            if (_schema->cdc_options().postimage() || _schema->cdc_options().full_preimage()) {
                for (const column_definition& c: _schema->static_columns()) {
                    static_columns.emplace_back(c.id);
                    columns.emplace_back(&c);
                }
            } else {
                p.static_row().get().for_each_cell([&] (column_id id, const atomic_cell_or_collection&) {
                    auto& cdef =_schema->column_at(column_kind::static_column, id);
                    static_columns.emplace_back(id);
                    columns.emplace_back(&cdef);
                });
            }
        }
        if (!p.clustered_rows().empty()) {
            const bool has_row_delete = std::any_of(p.clustered_rows().begin(), p.clustered_rows().end(), [] (const rows_entry& re) {
                return re.row().deleted_at();
            });
            // for postimage we need everything...
            if (has_row_delete || _schema->cdc_options().postimage() || _schema->cdc_options().full_preimage()) {
                for (const column_definition& c: _schema->regular_columns()) {
                    regular_columns.emplace_back(c.id);
                    columns.emplace_back(&c);
                }
            } else {
                p.clustered_rows().begin()->row().cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection&) {
                    const auto& cdef =_schema->column_at(column_kind::regular_column, id);
                    regular_columns.emplace_back(id);
                    columns.emplace_back(&cdef);
                });
            }
        }
        
        auto selection = cql3::selection::selection::for_columns(_schema, std::move(columns));

        auto opts = selection->get_query_options();
        opts.set(query::partition_slice::option::collections_as_maps);
        opts.set_if<query::partition_slice::option::always_return_static_content>(!p.static_row().empty());

        auto partition_slice = query::partition_slice(std::move(bounds), std::move(static_columns), std::move(regular_columns), std::move(opts));
        const auto max_result_size = _ctx._proxy.get_max_result_size(partition_slice);
        auto command = ::make_lw_shared<query::read_command>(_schema->id(), _schema->version(), partition_slice, query::max_result_size(max_result_size), query::row_limit(row_limit));

        const auto select_cl = adjust_cl(write_cl);

      try {
        return _ctx._proxy.query(_schema, std::move(command), std::move(partition_ranges), select_cl, service::storage_proxy::coordinator_query_options(default_timeout(), empty_service_permit(), client_state)).then(
                [s = _schema, partition_slice = std::move(partition_slice), selection = std::move(selection)] (service::storage_proxy::coordinator_query_result qr) -> lw_shared_ptr<cql3::untyped_result_set> {
            return make_lw_shared<cql3::untyped_result_set>(*s, std::move(qr.query_result), *selection, partition_slice);
        });
      } catch (exceptions::unavailable_exception& e) {
        // `query` can throw `unavailable_exception`, which is seen by clients as ~ "NoHostAvailable". 
        // So, we'll translate it to a `read_failure_exception` with custom message.
        cdc_log.debug("Preimage: translating a (read) `unavailable_exception` to `request_execution_exception` - {}", e);
        throw exceptions::read_failure_exception("CDC preimage query could not achieve the CL.",
                e.consistency, e.alive, 0, e.required, false);
      }
    }

    // Note: this assumes that the results are from one partition only
    void load_preimage_results_into_state(lw_shared_ptr<cql3::untyped_result_set> preimage_set, bool static_only) {
        // static row
        if (!preimage_set->empty()) {
            // There may be some static row data
            const auto& row = preimage_set->front();
            for (auto& c : _schema->static_columns()) {
                if (auto maybe_cell_view = get_preimage_col_value(c, &row)) {
                    _static_row_state[&c] = std::move(*maybe_cell_view);
                }
            }
        }

        if (static_only) {
            return;
        }

        // clustering rows
        for (const auto& row : *preimage_set) {
            // Construct the clustering key for this row
            std::vector<managed_bytes> ck_parts;
            ck_parts.reserve(_schema->clustering_key_size());
            for (auto& c : _schema->clustering_key_columns()) {
                auto v = row.get_view_opt(c.name_as_text());
                if (!v) {
                    // We might get here if both of the following conditions are true:
                    // - In preimage query, we requested the static row and some clustering rows,
                    // - The partition had some static row data, but did not have any requested clustering rows.
                    // In such case, the result set will have an artificial row that only contains static columns,
                    // but no clustering columns. In such case, we can safely return from the function,
                    // as there will be no clustering row data to load into the state.
                    return;
                }
                ck_parts.emplace_back(managed_bytes(*v));
            }
            auto ck = clustering_key::from_exploded(std::move(ck_parts));

            // Collect regular rows
            cell_map cells;
            for (auto& c : _schema->regular_columns()) {
                if (auto maybe_cell_view = get_preimage_col_value(c, &row)) {
                    cells[&c] = std::move(*maybe_cell_view);
                }
            }

            _clustering_row_states.insert_or_assign(std::move(ck), std::move(cells));
        }
    }

    /** For preimage query use the same CL as for base write, except for CLs ANY and ALL. */
    static db::consistency_level adjust_cl(db::consistency_level write_cl) {
        if (write_cl == db::consistency_level::ANY) {
            return db::consistency_level::ONE;
        } else if (write_cl == db::consistency_level::ALL || write_cl == db::consistency_level::SERIAL) {
	        return db::consistency_level::QUORUM;
        } else if (write_cl == db::consistency_level::LOCAL_SERIAL) {
	        return db::consistency_level::LOCAL_QUORUM;
        }
        return write_cl;
    }
};

template <typename Func>
future<std::vector<mutation>>
transform_mutations(std::vector<mutation>& muts, decltype(muts.size()) batch_size, Func&& f) {
    return parallel_for_each(
            boost::irange(static_cast<decltype(muts.size())>(0), muts.size(), batch_size),
            std::forward<Func>(f))
        .then([&muts] () mutable { return std::move(muts); });
}

} // namespace cdc

future<std::tuple<std::vector<mutation>, lw_shared_ptr<cdc::operation_result_tracker>>>
cdc::cdc_service::impl::augment_mutation_call(lowres_clock::time_point timeout, std::vector<mutation>&& mutations, tracing::trace_state_ptr tr_state, db::consistency_level write_cl) {
    // we do all this because in the case of batches, we can have mixed schemas.
    auto e = mutations.end();
    auto i = std::find_if(mutations.begin(), e, [](const mutation& m) {
        return m.schema()->cdc_options().enabled();
    });

    if (i == e) {
        return make_ready_future<std::tuple<std::vector<mutation>, lw_shared_ptr<cdc::operation_result_tracker>>>(std::make_tuple(std::move(mutations), lw_shared_ptr<cdc::operation_result_tracker>()));
    }

    tracing::trace(tr_state, "CDC: Started generating mutations for log rows");
    mutations.reserve(2 * mutations.size());

    return do_with(std::move(mutations), service::query_state(service::client_state::for_internal_calls(), empty_service_permit()), operation_details{},
            [this, timeout, i, tr_state = std::move(tr_state), write_cl] (std::vector<mutation>& mutations, service::query_state& qs, operation_details& details) {
        return transform_mutations(mutations, 1, [this, &mutations, timeout, &qs, tr_state = tr_state, &details, write_cl] (int idx) mutable {
            auto& m = mutations[idx];
            auto s = m.schema();

            if (!s->cdc_options().enabled()) {
                return make_ready_future<>();
            }

            transformer trans(_ctxt, s, m.decorated_key());

            auto f = make_ready_future<lw_shared_ptr<cql3::untyped_result_set>>(nullptr);
            if (s->cdc_options().preimage() || s->cdc_options().postimage()) {
                // Note: further improvement here would be to coalesce the pre-image selects into one
                // iff a batch contains several modifications to the same table. Otoh, batch is rare(?)
                // so this is premature.
                tracing::trace(tr_state, "CDC: Selecting preimage for {}", m.decorated_key());
                f = trans.pre_image_select(qs.get_client_state(), write_cl, m).then_wrapped([this] (future<lw_shared_ptr<cql3::untyped_result_set>> f) {
                    auto& cdc_stats = _ctxt._proxy.get_cdc_stats();
                    cdc_stats.counters_total.preimage_selects++;
                    if (f.failed()) {
                        cdc_stats.counters_failed.preimage_selects++;
                    }
                    return f;
                });
            } else {
                tracing::trace(tr_state, "CDC: Preimage not enabled for the table, not querying current value of {}", m.decorated_key());
            }

            return f.then([trans = std::move(trans), &mutations, idx, tr_state, &details] (lw_shared_ptr<cql3::untyped_result_set> rs) mutable {
                auto& m = mutations[idx];
                auto& s = m.schema();

                if (rs) {
                    const auto& p = m.partition();
                    const bool static_only = !p.static_row().empty() && p.clustered_rows().empty();
                    trans.load_preimage_results_into_state(std::move(rs), static_only);
                }

                const bool preimage = s->cdc_options().preimage();
                const bool postimage = s->cdc_options().postimage();
                details.had_preimage |= preimage;
                details.had_postimage |= postimage;
                tracing::trace(tr_state, "CDC: Generating log mutations for {}", m.decorated_key());
                if (should_split(m)) {
                    tracing::trace(tr_state, "CDC: Splitting {}", m.decorated_key());
                    details.was_split = true;
                    process_changes_with_splitting(m, trans, preimage, postimage);
                } else {
                    tracing::trace(tr_state, "CDC: No need to split {}", m.decorated_key());
                    process_changes_without_splitting(m, trans, preimage, postimage);
                }
                auto [log_mut, touched_parts] = std::move(trans).finish();
                const int generated_count = log_mut.size();
                mutations.insert(mutations.end(), std::make_move_iterator(log_mut.begin()), std::make_move_iterator(log_mut.end()));

                // `m` might be invalidated at this point because of the push_back to the vector
                tracing::trace(tr_state, "CDC: Generated {} log mutations from {}", generated_count, mutations[idx].decorated_key());
                details.touched_parts.add(touched_parts);
            });
        }).then([this, tr_state, &details](std::vector<mutation> mutations) {
            tracing::trace(tr_state, "CDC: Finished generating all log mutations");
            auto tracker = make_lw_shared<cdc::operation_result_tracker>(_ctxt._proxy.get_cdc_stats(), details);
            return make_ready_future<std::tuple<std::vector<mutation>, lw_shared_ptr<cdc::operation_result_tracker>>>(std::make_tuple(std::move(mutations), std::move(tracker)));
        });
    });
}

bool cdc::cdc_service::needs_cdc_augmentation(const std::vector<mutation>& mutations) const {
    return std::any_of(mutations.begin(), mutations.end(), [](const mutation& m) {
        return m.schema()->cdc_options().enabled();
    });
}

future<std::tuple<std::vector<mutation>, lw_shared_ptr<cdc::operation_result_tracker>>>
cdc::cdc_service::augment_mutation_call(lowres_clock::time_point timeout, std::vector<mutation>&& mutations, tracing::trace_state_ptr tr_state, db::consistency_level write_cl) {
    return _impl->augment_mutation_call(timeout, std::move(mutations), std::move(tr_state), write_cl);
}
