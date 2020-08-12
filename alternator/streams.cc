/*
 * Copyright 2020 ScyllaDB
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
 * You should have received a copy of the GNU Affero General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <type_traits>
#include <boost/lexical_cast.hpp>
#include <boost/io/ios_state.hpp>

#include "base64.hh"
#include "log.hh"
#include "database.hh"
#include "db/config.hh"

#include "cdc/log.hh"
#include "cdc/generation.hh"
#include "db/system_distributed_keyspace.hh"
#include "utils/UUID_gen.hh"
#include "cql3/selection/selection.hh"
#include "cql3/result_set.hh"
#include "cql3/type_json.hh"
#include "schema_builder.hh"
#include "service/storage_service.hh"
#include "gms/feature.hh"
#include "gms/feature_service.hh"

#include "executor.hh"
#include "tags_extension.hh"
#include "rmw_operation.hh"

/**
 * Base template type to implement  rapidjson::internal::TypeHelper<...>:s
 * for types that are ostreamable/string constructible/castable.
 * Eventually maybe graduate to header, for other modules. 
 */
template<typename ValueType, typename T>
struct from_string_helper {
    static bool Is(const ValueType& v) {
        try {
            Get(v);
            return true;
        } catch (...) {
            return false;
        }
    }
    static T Get(const ValueType& v) {
        auto s = rjson::to_string_view(v);
        if constexpr (std::is_constructible_v<T, std::string_view>) {
            return T(s);
        } else if constexpr (std::is_constructible_v<T, sstring>) {
            return T(sstring(s));
        } else {
            return boost::lexical_cast<T>(s.data(), s.size());
        }
    }
    static ValueType& Set(ValueType&, T) = delete;
    static ValueType& Set(ValueType& v, T data, typename ValueType::AllocatorType& a) {
        std::ostringstream ss;
        ss << data;
#if __cplusplus >= 202002L
        auto s = ss.view();
#else
        auto s = ss.str();
#endif
        return v.SetString(s.data(), s.size(), a);
    }
};

template<typename ValueType>
struct rapidjson::internal::TypeHelper<ValueType, utils::UUID>
    : public from_string_helper<ValueType, utils::UUID>
{};

namespace alternator {

// stream arn _has_ to be 37 or more characters long. ugh...
// see https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_DescribeStream.html#API_streams_DescribeStream_RequestSyntax
// UUID is 36 bytes as string (including dashes). 
// Prepend a version/type marker -> 37
class stream_arn : public utils::UUID {
public:
    using UUID = utils::UUID;
    static constexpr char marker = 'S';

    stream_arn() = default;
    stream_arn(const UUID& uuid)
        : UUID(uuid)
    {}
    stream_arn(std::string_view v)
        : UUID(v.substr(1))
    {
        if (v[0] != marker) {
            throw std::invalid_argument(std::string(v));
        }
    }
    friend std::ostream& operator<<(std::ostream& os, const stream_arn& arn) {
        const UUID& uuid = arn;
        return os << marker << uuid;
    }
    friend std::istream& operator>>(std::istream& is, stream_arn& arn) {
        std::string s;
        is >> s;
        arn = stream_arn(s);
        return is;
    }
};

}

template<typename ValueType>
struct rapidjson::internal::TypeHelper<ValueType, alternator::stream_arn>
    : public from_string_helper<ValueType, alternator::stream_arn>
{};

namespace alternator {

future<alternator::executor::request_return_type> alternator::executor::list_streams(client_state& client_state, service_permit permit, rjson::value request) {
    _stats.api_operations.list_streams++;

    auto limit = rjson::get_opt<int>(request, "Limit").value_or(std::numeric_limits<int>::max());
    auto streams_start = rjson::get_opt<stream_arn>(request, "ExclusiveStartStreamArn");
    auto table = find_table(_proxy, request);
    auto& db = _proxy.get_db().local();
    auto& cfs = db.get_column_families();
    auto i = cfs.begin();
    auto e = cfs.end();

    if (limit < 1) {
        throw api_error::validation("Limit must be 1 or more");
    }

    // TODO: the unordered_map here is not really well suited for partial
    // querying - we're sorting on local hash order, and creating a table
    // between queries may or may not miss info. But that should be rare,
    // and we can probably expect this to be a single call.
    if (streams_start) {
        i = std::find_if(i, e, [&](const std::pair<utils::UUID, lw_shared_ptr<column_family>>& p) {
            return p.first == streams_start 
                && cdc::get_base_table(db, *p.second->schema())
                && is_alternator_keyspace(p.second->schema()->ks_name())
                ;
        });
        if (i != e) {
            ++i;
        }
    }

    auto ret = rjson::empty_object();
    auto streams = rjson::empty_array();

    std::optional<stream_arn> last;

    for (;limit > 0 && i != e; ++i) {
        auto s = i->second->schema();
        auto& ks_name = s->ks_name();
        auto& cf_name = s->cf_name();

        if (!is_alternator_keyspace(ks_name)) {
            continue;
        }
        if (table && ks_name != table->ks_name()) {
            continue;
        }
        if (cdc::is_log_for_some_table(ks_name, cf_name)) {
            if (table && table != cdc::get_base_table(db, *s)) {
                continue;
            }

            rjson::value new_entry = rjson::empty_object();

            last = i->first;
            rjson::set(new_entry, "StreamArn", *last);
            rjson::set(new_entry, "StreamLabel", rjson::from_string(ks_name + "." + cf_name));
            rjson::set(new_entry, "TableName", rjson::from_string(cdc::base_name(table_name(*s))));
            rjson::push_back(streams, std::move(new_entry));

            --limit;
        }
    }

    rjson::set(ret, "Streams", std::move(streams));

    if (last) {
        rjson::set(ret, "LastEvaluatedStreamArn", *last);
    }

    return make_ready_future<executor::request_return_type>(make_jsonable(std::move(ret)));
}

struct shard_id {
    static constexpr auto marker = 'H';

    utils::UUID table;
    db_clock::time_point time;
    cdc::stream_id id;

    shard_id(utils::UUID tab, db_clock::time_point t, cdc::stream_id i)
        : table(tab)
        , time(t)
        , id(i)
    {}
    shard_id(const sstring&);

    friend std::ostream& operator<<(std::ostream& os, const shard_id& id) {
        boost::io::ios_flags_saver fs(os);
        return os << marker << id.table << std::hex
            << ':' << id.time.time_since_epoch().count()
            << ':' << id.id.to_bytes()
            ;
    }
};

shard_id::shard_id(const sstring& s) {
    auto i = s.find(':');
    auto j = s.find(':', i + 1);

    if (s.at(0) != marker || i == sstring::npos || j == sstring::npos) {
        throw std::invalid_argument(s);
    }

    table = utils::UUID(s.substr(1, i));
    time = db_clock::time_point(db_clock::duration(std::stoull(s.substr(i + 1, j), nullptr, 16)));
    id = cdc::stream_id(from_hex(s.substr(j + 1)));
}

}

template<typename ValueType>
struct rapidjson::internal::TypeHelper<ValueType, alternator::shard_id>
    : public from_string_helper<ValueType, alternator::shard_id>
{};

namespace alternator {

enum class stream_view_type {
    KEYS_ONLY,
    NEW_IMAGE,
    OLD_IMAGE,
    NEW_AND_OLD_IMAGES,
};

std::ostream& operator<<(std::ostream& os, stream_view_type type) {
    switch (type) {
        case stream_view_type::KEYS_ONLY: os << "KEYS_ONLY"; break;
        case stream_view_type::NEW_IMAGE: os << "NEW_IMAGE"; break;
        case stream_view_type::OLD_IMAGE: os << "OLD_IMAGE"; break;
        case stream_view_type::NEW_AND_OLD_IMAGES: os << "NEW_AND_OLD_IMAGES"; break;
        default: throw std::logic_error(std::to_string(int(type)));
    }
    return os;
}
std::istream& operator>>(std::istream& is, stream_view_type& type) {
    sstring s;
    is >> s;
    if (s == "KEYS_ONLY") {
        type = stream_view_type::KEYS_ONLY;
    } else if (s == "NEW_IMAGE") {
        type = stream_view_type::NEW_IMAGE;
    } else if (s == "OLD_IMAGE") {
        type = stream_view_type::OLD_IMAGE;
    } else if (s == "NEW_AND_OLD_IMAGES") {
        type = stream_view_type::NEW_AND_OLD_IMAGES;
    } else {
        throw std::invalid_argument(s);
    }
    return is;
}

static stream_view_type cdc_options_to_steam_view_type(const cdc::options& opts) {
    stream_view_type type = stream_view_type::KEYS_ONLY;
    if (opts.preimage() && opts.postimage()) {
        type = stream_view_type::NEW_AND_OLD_IMAGES;
    } else if (opts.preimage()) {
        type = stream_view_type::OLD_IMAGE;
    } else if (opts.postimage()) {
        type = stream_view_type::NEW_IMAGE;
    }
    return type;
}

}

template<typename ValueType>
struct rapidjson::internal::TypeHelper<ValueType, alternator::stream_view_type>
    : public from_string_helper<ValueType, alternator::stream_view_type>
{};

namespace alternator {

using namespace std::string_literals;

/**
 * This implementation of CDC stream to dynamo shard mapping
 * is simply a 1:1 mapping, id=shard. Simple, and fairly easy
 * to do ranged queries later.
 * 
 * Downside is that we get a _lot_ of shards. And with actual
 * generations, we'll get hubba-hubba loads of them. 
 * 
 * Improvement ideas: 
 * 1.) Prune expired streams that cannot have records (check ttls)
 * 2.) Group cdc streams into N per shards (by token range)
 * 
 * The latter however makes it impossible to do a simple 
 * range query with limit (we need to do staggered querying, because
 * of how GetRecords work).
 * The results of a select from cdc log on more than one cdc stream,
 * where timestamp < T will typically look like: 
 * 
 * <shard 0> <ts0> ...
 * ....
 * <shard 0> <tsN> ...
 * <shard 1> <ts0p> ...
 * ...
 * <shard 1> <tsNp> ...
 *
 * Where tsN < timestamp and tsNp < timestamp, but range [ts0-tsN] and [ts0p-tsNp]
 * overlap. 
 * 
 * Now, if we have limit=X, we might end on a row inside shard 0.
 * For our next query, we would in the simple case just say "timestamp > ts3 and timestamp < T",
 * but of course this would then miss rows in shard 1 and above. 
 * 
 * However if we instead set the conditions to 
 * 
 *   "(token(stream_id) = token(shard 0) and timestamp > ts3 and timestamp < T)
 *      or (token(stream_id) > token(shard 0) and timestamp < T)"
 * 
 * we can get the correct data. Question is however how well this will perform, if at 
 * all.
 * 
 * For now, go simple, but maybe consider if this latter approach would work
 * and perform. 
 * 
 */

static std::chrono::seconds confidence_interval(const database& db) {
    return std::chrono::seconds(db.get_config().alternator_streams_time_window_s());
}

future<executor::request_return_type> executor::describe_stream(client_state& client_state, service_permit permit, rjson::value request) {
    _stats.api_operations.describe_stream++;

    auto limit = rjson::get_opt<int>(request, "Limit").value_or(100); // according to spec
    auto ret = rjson::empty_object();
    auto stream_desc = rjson::empty_object();
    // local java dynamodb server does _not_ validate the arn syntax. But "real" one does. 
    // I.e. unparsable arn -> error. 
    auto stream_arn = rjson::get<alternator::stream_arn>(request, "StreamArn");

    schema_ptr schema, bs;
    auto& db = _proxy.get_db().local();

    try {
        auto& cf = db.find_column_family(stream_arn);
        schema = cf.schema();
        bs = cdc::get_base_table(_proxy.get_db().local(), *schema);
    } catch (...) {        
    }
 
    if (!schema || !bs || !is_alternator_keyspace(schema->ks_name())) {
        throw api_error::resource_not_found("Invalid StreamArn");
    }

    if (limit < 1) {
        throw api_error::validation("Limit must be 1 or more");
    }

    std::optional<shard_id> shard_start;
    try {
        shard_start = rjson::get_opt<shard_id>(request, "ExclusiveStartShardId");
    } catch (...) {
        // If we cannot parse the start shard, it is treated as non-matching 
        // in dynamo. We can just shortcut this and say "no records", i.e limit=0
        limit = 0;
    }

    auto& opts = bs->cdc_options();

    auto status = "DISABLED";

    if (opts.enabled()) {
        auto& metadata = _ss.get_cdc_metadata();
        if (!metadata.streams_available()) {
            status = "ENABLING";
        } else {
            status = "ENABLED";
        }
    } 
    
    rjson::set(stream_desc, "StreamStatus", rjson::from_string(status));

    stream_view_type type = cdc_options_to_steam_view_type(opts);

    rjson::set(stream_desc, "StreamArn", alternator::stream_arn(schema->id()));
    rjson::set(stream_desc, "StreamViewType", type);
    rjson::set(stream_desc, "TableName", rjson::from_string(table_name(*bs)));

    describe_key_schema(stream_desc, *bs);

    if (!opts.enabled()) {
        rjson::set(ret, "StreamDescription", std::move(stream_desc));
        return make_ready_future<executor::request_return_type>(make_jsonable(std::move(ret)));
    }

    // TODO: label
    // TODO: creation time

    auto& tm = _proxy.get_token_metadata();
    // cannot really "resume" query, must iterate all data. because we cannot query neither "time" (pk) > something,
    // or on expired...
    // TODO: maybe add secondary index to topology table to enable this?
    return _sdks.cdc_get_versioned_streams({ tm.count_normal_token_owners() }).then([this, &db, schema, shard_start, limit, ret = std::move(ret), stream_desc = std::move(stream_desc)](std::map<db_clock::time_point, cdc::streams_version> topologies) mutable {
        auto i = topologies.begin();
        auto e = topologies.end();
        auto prev = e;
        auto threshold = db_clock::now() - confidence_interval(db);
        auto shards = rjson::empty_array();

        std::optional<shard_id> last;

        for (; limit > 0 && i != e; prev = i, ++i) {
            auto& [ts, sv] = *i;
            for (auto& id : sv.streams()) {
                if (shard_start && shard_start->id != id) {
                    continue;
                }
                if (shard_start && shard_start->id == id) {
                    shard_start = std::nullopt;
                    continue;
                }

                auto shard = rjson::empty_object();

                if (prev != e) {
                    auto token = id.token();
                    auto& pids = prev->second.streams();
                    auto pid = std::upper_bound(pids.begin(), pids.end(), token, [](const dht::token& t, const cdc::stream_id& id) {
                        return t < id.token();
                    });
                    if (pid != pids.end()) {
                        rjson::set(shard, "ParentShardId", shard_id(schema->id(), prev->first, *pid));
                    }
                }

                last = shard_id(schema->id(), ts, id);
                rjson::set(shard, "ShardId", *last);

                auto range = rjson::empty_object();
                rjson::set(range, "StartingSequenceNumber", utils::UUID_gen::min_time_UUID(ts.time_since_epoch().count()));
                if (sv.expired() && *sv.expired() < threshold) {
                    rjson::set(range, "EndingSequenceNumber", utils::UUID_gen::max_time_UUID((*sv.expired() + confidence_interval(db)).time_since_epoch().count()));
                }

                rjson::set(shard, "SequenceNumberRange", std::move(range));
                rjson::push_back(shards, std::move(shard));
                
                if (--limit == 0) {
                    break;
                }

            }
        }

        if (last) {
            rjson::set(stream_desc, "LastEvaluatedShardId", *last);
        }

        rjson::set(stream_desc, "Shards", std::move(shards));
        rjson::set(ret, "StreamDescription", std::move(stream_desc));
            
        return make_ready_future<executor::request_return_type>(make_jsonable(std::move(ret)));
    });
}

enum class shard_iterator_type {
    AT_SEQUENCE_NUMBER,
    AFTER_SEQUENCE_NUMBER,
    TRIM_HORIZON,
    LATEST,
};

std::ostream& operator<<(std::ostream& os, shard_iterator_type type) {
    switch (type) {
        case shard_iterator_type::AT_SEQUENCE_NUMBER: os << "AT_SEQUENCE_NUMBER"; break;
        case shard_iterator_type::AFTER_SEQUENCE_NUMBER: os << "AFTER_SEQUENCE_NUMBER"; break;
        case shard_iterator_type::TRIM_HORIZON: os << "TRIM_HORIZON"; break;
        case shard_iterator_type::LATEST: os << "LATEST"; break;
        default: throw std::logic_error(std::to_string(int(type)));
    }
    return os;
}
std::istream& operator>>(std::istream& is, shard_iterator_type& type) {
    sstring s;
    is >> s;
    if (s == "AT_SEQUENCE_NUMBER") {
        type = shard_iterator_type::AT_SEQUENCE_NUMBER;
    } else if (s == "AFTER_SEQUENCE_NUMBER") {
        type = shard_iterator_type::AFTER_SEQUENCE_NUMBER;
    } else if (s == "TRIM_HORIZON") {
        type = shard_iterator_type::TRIM_HORIZON;
    } else if (s == "LATEST") {
        type = shard_iterator_type::LATEST;
    } else {
        throw std::invalid_argument(s);
    }
    return is;
}

struct shard_iterator {    
    static auto constexpr marker = 'I';

    shard_id shard;
    utils::UUID threshold;

    shard_iterator(shard_id i, utils::UUID th)
        : shard(i)
        , threshold(th)
    {}
    shard_iterator(utils::UUID arn, db_clock::time_point ts, cdc::stream_id i, utils::UUID th)
        : shard(arn, ts, i)
        , threshold(th)
    {}
    shard_iterator(const sstring&);

    friend std::ostream& operator<<(std::ostream& os, const shard_iterator& id) {
        boost::io::ios_flags_saver fs(os);
        return os << marker << std::hex << id.threshold
            << ':' << id.shard
            ;
    }
private:
    shard_iterator(const sstring&, size_t);
};

shard_iterator::shard_iterator(const sstring& s) 
    : shard_iterator(s, s.find(':'))
{}

shard_iterator::shard_iterator(const sstring& s, size_t i)
    : shard(s.substr(i + 1))
    , threshold(s.substr(1, i))
{
    if (s.at(0) != marker || i == sstring::npos) {
        throw std::invalid_argument(s);
    }
}
}

/**
 * Increment a timeuuid. The lowest larger timeuud we 
 * can get (tm)
 */
static utils::UUID increment(const utils::UUID& uuid) {
    auto msb = uuid.get_most_significant_bits();
    auto lsb = uuid.get_least_significant_bits() + 1;
    if (lsb == 0) {
        ++msb;
    }
    return utils::UUID(msb, lsb);
}

template<typename ValueType>
struct rapidjson::internal::TypeHelper<ValueType, alternator::shard_iterator>
    : public from_string_helper<ValueType, alternator::shard_iterator>
{};

template<typename ValueType>
struct rapidjson::internal::TypeHelper<ValueType, alternator::shard_iterator_type>
    : public from_string_helper<ValueType, alternator::shard_iterator_type>
{};

namespace alternator {

future<executor::request_return_type> executor::get_shard_iterator(client_state& client_state, service_permit permit, rjson::value request) {
    _stats.api_operations.get_shard_iterator++;

    auto type = rjson::get<shard_iterator_type>(request, "ShardIteratorType");
    auto seq_num = rjson::get_opt<utils::UUID>(request, "SequenceNumber");
    
    if (type < shard_iterator_type::TRIM_HORIZON && !seq_num) {
        throw api_error::validation("Missing required parameter \"SequenceNumber\"");
    }
    if (type >= shard_iterator_type::TRIM_HORIZON && seq_num) {
        throw api_error::validation("Iterator of this type should not have sequence number");
    }

    auto stream_arn = rjson::get<alternator::stream_arn>(request, "StreamArn");
    auto& db = _proxy.get_db().local();
    
    schema_ptr schema = nullptr;
    std::optional<shard_id> sid;

    try {
        auto& cf = db.find_column_family(stream_arn);
        schema = cf.schema();
        sid = rjson::get<shard_id>(request, "ShardId");
    } catch (...) {
    }
    if (!schema || !cdc::get_base_table(db, *schema) || !is_alternator_keyspace(schema->ks_name()) || sid->table != schema->id()) {
        throw api_error::resource_not_found("Invalid StreamArn");
    }

    utils::UUID threshold;

    switch (type) {
        case shard_iterator_type::AT_SEQUENCE_NUMBER:
            threshold = *seq_num;
            break;
        case shard_iterator_type::AFTER_SEQUENCE_NUMBER:
            threshold = increment(*seq_num);
            break;
        case shard_iterator_type::TRIM_HORIZON:
            threshold = utils::UUID{};
            break;
        case shard_iterator_type::LATEST:
            threshold = utils::UUID_gen::min_time_UUID((db_clock::now() - confidence_interval(db)).time_since_epoch().count());
            break;
    }

    shard_iterator iter(*sid, threshold);

    auto ret = rjson::empty_object();
    rjson::set(ret, "ShardIterator", iter);

    return make_ready_future<executor::request_return_type>(make_jsonable(std::move(ret)));
}

struct event_id {
    cdc::stream_id stream;
    utils::UUID timestamp;

    static const auto marker = 'E';

    event_id(cdc::stream_id s, utils::UUID ts)
        : stream(s)
        , timestamp(ts)
    {}
    
    friend std::ostream& operator<<(std::ostream& os, const event_id& id) {
        boost::io::ios_flags_saver fs(os);
        return os << marker << std::hex << id.stream.to_bytes()
            << ':' << id.timestamp
            ;
    }
};
}

template<typename ValueType>
struct rapidjson::internal::TypeHelper<ValueType, alternator::event_id>
    : public from_string_helper<ValueType, alternator::event_id>
{};

namespace alternator {
    
future<executor::request_return_type> executor::get_records(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request) {
    _stats.api_operations.get_records++;
    auto start_time = std::chrono::steady_clock::now();

    auto iter = rjson::get<shard_iterator>(request, "ShardIterator");
    auto limit = rjson::get_opt<size_t>(request, "Limit").value_or(1000);

    if (limit < 1) {
        throw api_error::validation("Limit must be 1 or more");
    }

    auto& db = _proxy.get_db().local();
    schema_ptr schema, base;
    try {
        auto& log_table = db.find_column_family(iter.shard.table);
        schema = log_table.schema();
        base = cdc::get_base_table(db, *schema);
    } catch (...) {        
    }

    if (!schema || !base || !is_alternator_keyspace(schema->ks_name())) {
        throw api_error::resource_not_found(boost::lexical_cast<std::string>(iter.shard.table));
    }

    tracing::add_table_name(trace_state, schema->ks_name(), schema->cf_name());

    db::consistency_level cl = db::consistency_level::LOCAL_QUORUM;
    partition_key pk = iter.shard.id.to_partition_key(*schema);

    dht::partition_range_vector partition_ranges{ dht::partition_range::make_singular(dht::decorate_key(*schema, pk)) };

    auto high_ts = db_clock::now() - confidence_interval(db);
    auto high_uuid = utils::UUID_gen::min_time_UUID(high_ts.time_since_epoch().count());    
    auto lo = clustering_key_prefix::from_exploded(*schema, { iter.threshold.serialize() });
    auto hi = clustering_key_prefix::from_exploded(*schema, { high_uuid.serialize() });

    std::vector<query::clustering_range> bounds;
    using bound = typename query::clustering_range::bound;
    bounds.push_back(query::clustering_range::make(bound(lo), bound(hi, false)));

    static const bytes timestamp_column_name = cdc::log_meta_column_name_bytes("time");
    static const bytes op_column_name = cdc::log_meta_column_name_bytes("operation");

    auto key_names = boost::copy_range<std::unordered_set<std::string>>(
        boost::range::join(std::move(base->partition_key_columns()), std::move(base->clustering_key_columns()))
        | boost::adaptors::transformed([&] (const column_definition& cdef) { return cdef.name_as_text(); })
    );
    // Include all base table columns as values (in case pre or post is enabled).
    // This will include attributes not stored in the frozen map column
    auto attr_names = boost::copy_range<std::unordered_set<std::string>>(base->regular_columns()
        // this will include the :attrs column, which we will also force evaluating. 
        // But not having this set empty forces out any cdc columns from actual result 
        | boost::adaptors::transformed([] (const column_definition& cdef) { return cdef.name_as_text(); })
    );

    std::vector<const column_definition*> columns;
    columns.reserve(schema->all_columns().size());

    auto pks = schema->partition_key_columns();
    auto cks = schema->clustering_key_columns();

    std::transform(pks.begin(), pks.end(), std::back_inserter(columns), [](auto& c) { return &c; });
    std::transform(cks.begin(), cks.end(), std::back_inserter(columns), [](auto& c) { return &c; });

    auto regular_columns = boost::copy_range<query::column_id_vector>(schema->regular_columns() 
        | boost::adaptors::filtered([](const column_definition& cdef) { return cdef.name() == op_column_name || !cdc::is_cdc_metacolumn_name(cdef.name_as_text()); })
        | boost::adaptors::transformed([&] (const column_definition& cdef) { columns.emplace_back(&cdef); return cdef.id; })
    );

    stream_view_type type = cdc_options_to_steam_view_type(base->cdc_options());

    auto selection = cql3::selection::selection::for_columns(schema, std::move(columns));
    auto partition_slice = query::partition_slice(
        std::move(bounds)
        , {}, std::move(regular_columns), selection->get_query_options());
    auto command = ::make_lw_shared<query::read_command>(schema->id(), schema->version(), partition_slice, _proxy.get_max_result_size(partition_slice),
            query::row_limit(limit * 4));

    return _proxy.query(schema, std::move(command), std::move(partition_ranges), cl, service::storage_proxy::coordinator_query_options(default_timeout(), std::move(permit), client_state)).then(
            [this, schema, partition_slice = std::move(partition_slice), selection = std::move(selection), start_time = std::move(start_time), limit, key_names = std::move(key_names), attr_names = std::move(attr_names), type, iter, high_ts] (service::storage_proxy::coordinator_query_result qr) mutable {       
        cql3::selection::result_set_builder builder(*selection, gc_clock::now(), cql_serialization_format::latest());
        query::result_view::consume(*qr.query_result, partition_slice, cql3::selection::result_set_builder::visitor(builder, *schema, *selection));

        auto result_set = builder.build();
        auto records = rjson::empty_array();

        auto& metadata = result_set->get_metadata();

        auto op_index = std::distance(metadata.get_names().begin(), 
            std::find_if(metadata.get_names().begin(), metadata.get_names().end(), [](const lw_shared_ptr<cql3::column_specification>& cdef) {
                return cdef->name->name() == op_column_name;
            })
        );
        auto ts_index = std::distance(metadata.get_names().begin(), 
            std::find_if(metadata.get_names().begin(), metadata.get_names().end(), [](const lw_shared_ptr<cql3::column_specification>& cdef) {
                return cdef->name->name() == timestamp_column_name;
            })
        );

        std::optional<utils::UUID> timestamp;
        auto dynamodb = rjson::empty_object();
        auto record = rjson::empty_object();

        using op_utype = std::underlying_type_t<cdc::operation>;

        auto maybe_add_record = [&] {
            if (!dynamodb.ObjectEmpty()) {
                rjson::set(record, "dynamodb", std::move(dynamodb));
                dynamodb = rjson::empty_object();
            }
            if (!record.ObjectEmpty()) {
                // TODO: awsRegion?
                rjson::set(record, "eventID", event_id(iter.shard.id, *timestamp));
                rjson::set(record, "eventSource", "scylladb:alternator");
                rjson::push_back(records, std::move(record));
                record = rjson::empty_object();
                --limit;
            }
        };

        for (auto& row : result_set->rows()) {
            auto op = static_cast<cdc::operation>(value_cast<op_utype>(data_type_for<op_utype>()->deserialize(*row[op_index])));
            auto ts = value_cast<utils::UUID>(data_type_for<utils::UUID>()->deserialize(*row[ts_index]));

            if (timestamp && timestamp != ts) {
                maybe_add_record();
                if (limit == 0) {
                    break;
                }
            }

            timestamp = ts;

            if (!dynamodb.HasMember("Keys")) {
                auto keys = rjson::empty_object();
                describe_single_item(*selection, row, key_names, keys);
                rjson::set(dynamodb, "Keys", std::move(keys));
                rjson::set(dynamodb, "ApproximateCreationDateTime", utils::UUID_gen::unix_timestamp_in_sec(ts).count());
                rjson::set(dynamodb, "SequenceNumber", ts);
                rjson::set(dynamodb, "StreamViewType", type);
                //TODO: SizeInBytes
            }

            /**
             * We merge rows with same timestamp into a single event.
             * This is pretty much needed, because a CDC row typically
             * encodes ~half the info of an alternator write. 
             * 
             * A big, big downside to how alternator records are written
             * (i.e. CQL), is that the distinction between INSERT and UPDATE
             * is somewhat lost/unmappable to actual eventName. 
             * A write (currently) always looks like an insert+modify
             * regardless whether we wrote existing record or not. 
             * 
             * Maybe RMW ops could be done slightly differently so 
             * we can distinguish them here...
             * 
             * For now, all writes will become MODIFY.
             * 
             * Note: we do not check the current pre/post
             * flags on CDC log, instead we use data to 
             * drive what is returned. This is (afaict)
             * consistent with dynamo streams
             */
            switch (op) {
            case cdc::operation::pre_image:
            case cdc::operation::post_image:
            {
                auto item = rjson::empty_object();
                describe_single_item(*selection, row, attr_names, item, true);
                if (!item.ObjectEmpty()) {
                    rjson::set(dynamodb, op == cdc::operation::pre_image ? "OldImage" : "NewImage", std::move(item));
                }
                break;
            }
            case cdc::operation::update:
                rjson::set(record, "eventName", "MODIFY");
                break;
            case cdc::operation::insert:
                rjson::set(record, "eventName", "INSERT");
                break;
            default:
                rjson::set(record, "eventName", "REMOVE");
                break;
            }
        }
        if (limit > 0 && timestamp) {
            maybe_add_record();
        }

        auto ret = rjson::empty_object();
        auto nrecords = records.Size();
        rjson::set(ret, "Records", std::move(records));

        if (nrecords != 0) {
            // #9642. Set next iterators threshold to > last
            shard_iterator next_iter(iter.shard, increment(*timestamp));
            rjson::set(ret, "NextShardIterator", next_iter);
            _stats.api_operations.get_records_latency.add(std::chrono::steady_clock::now() - start_time);
             return make_ready_future<executor::request_return_type>(make_jsonable(std::move(ret)));
        }

        // ugh. figure out if we are and end-of-shard
        return cdc::get_local_streams_timestamp().then([this, iter, high_ts, start_time, ret = std::move(ret)](db_clock::time_point ts) mutable {
            auto& shard = iter.shard;            

            if (shard.time < ts && ts < high_ts) {
                rjson::set(ret, "NextShardIterator", "");
            } else {
                shard_iterator next_iter(iter.shard, utils::UUID_gen::min_time_UUID(high_ts.time_since_epoch().count()));
                rjson::set(ret, "NextShardIterator", iter);
            }
            _stats.api_operations.get_records_latency.add(std::chrono::steady_clock::now() - start_time);
            return make_ready_future<executor::request_return_type>(make_jsonable(std::move(ret)));
        });
    });
}

void executor::add_stream_options(const rjson::value& stream_specification, schema_builder& builder) {
    auto stream_enabled = rjson::find(stream_specification, "StreamEnabled");
    if (!stream_enabled || !stream_enabled->IsBool()) {
        throw api_error::validation("StreamSpecification needs boolean StreamEnabled");
    }

    if (stream_enabled->GetBool()) {
        auto& db = _proxy.get_db().local();

        if (!db.features().cluster_supports_cdc()) {
            throw api_error::validation("StreamSpecification: streams (CDC) feature not enabled in cluster.");
        }

        cdc::options opts;
        opts.enabled(true);
        auto type = rjson::get_opt<stream_view_type>(stream_specification, "StreamViewType").value_or(stream_view_type::KEYS_ONLY);
        switch (type) {
            default: 
                break;
            case stream_view_type::NEW_AND_OLD_IMAGES: 
                opts.postimage(true);
                opts.preimage(cdc::image_mode::full);
                break;
            case stream_view_type::OLD_IMAGE: 
                opts.preimage(cdc::image_mode::full);
                break;
            case stream_view_type::NEW_IMAGE: 
                opts.postimage(true);
                break;
        }
        builder.with_cdc_options(opts);
    } else {
        cdc::options opts;
        opts.enabled(false);
        builder.with_cdc_options(opts);
    }
}

}
