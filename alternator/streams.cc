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
#include <boost/multiprecision/cpp_int.hpp>

#include <seastar/json/formatter.hh>

#include "base64.hh"
#include "log.hh"
#include "database.hh"
#include "db/config.hh"

#include "cdc/log.hh"
#include "cdc/generation.hh"
#include "cdc/cdc_options.hh"
#include "cdc/metadata.hh"
#include "db/system_distributed_keyspace.hh"
#include "utils/UUID_gen.hh"
#include "cql3/selection/selection.hh"
#include "cql3/result_set.hh"
#include "cql3/type_json.hh"
#include "schema_builder.hh"
#include "service/storage_service.hh"
#include "service/storage_proxy.hh"
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
        auto s = ss.str();
        return v.SetString(s.data(), s.size(), a);
    }
};

template<typename ValueType>
struct rapidjson::internal::TypeHelper<ValueType, utils::UUID>
    : public from_string_helper<ValueType, utils::UUID>
{};

static db_clock::time_point as_timepoint(const utils::UUID& uuid) {
    return db_clock::time_point{utils::UUID_gen::unix_timestamp(uuid)};
}

/**
 * Note: scylla tables do not have a timestamp as such, 
 * but the UUID (for newly created tables at least) is 
 * a timeuuid, so we can use this to fake creation timestamp
 */
static sstring stream_label(const schema& log_schema) {
    auto ts = as_timepoint(log_schema.id());
    auto tt = db_clock::to_time_t(ts);
    struct tm tm;
    ::localtime_r(&tt, &tm);
    return seastar::json::formatter::to_json(tm);
}

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
            rjson::set(new_entry, "StreamLabel", rjson::from_string(stream_label(*s)));
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

    db_clock::time_point time;
    cdc::stream_id id;

    shard_id(db_clock::time_point t, cdc::stream_id i)
        : time(t)
        , id(i)
    {}
    shard_id(const sstring&);

    // dynamo specifies shardid as max 65 chars. 
    friend std::ostream& operator<<(std::ostream& os, const shard_id& id) {
        boost::io::ios_flags_saver fs(os);
        return os << marker << std::hex  
            << id.time.time_since_epoch().count()
            << ':' << id.id.to_bytes()
            ;
    }
};

shard_id::shard_id(const sstring& s) {
    auto i = s.find(':');

    if (s.at(0) != marker || i == sstring::npos) {
        throw std::invalid_argument(s);
    }

    time = db_clock::time_point(db_clock::duration(std::stoull(s.substr(1, i - 1), nullptr, 16)));
    id = cdc::stream_id(from_hex(s.substr(i + 1)));
}

struct sequence_number {
    utils::UUID uuid;

    sequence_number(utils::UUID uuid)
        : uuid(uuid)
    {}
    sequence_number(std::string_view);

    operator const utils::UUID&() const {
        return uuid;
    }

    friend std::ostream& operator<<(std::ostream& os, const sequence_number& num) {
        boost::io::ios_flags_saver fs(os);

        using namespace boost::multiprecision;

        /**
         * #7424 - aws sdk assumes sequence numbers are
         * monotonically growing bigints. 
         *
         * Timeuuids viewed as msb<<64|lsb are _not_,
         * but they are still sorted as
         *  timestamp() << 64|lsb
         * so we can simpy unpack the mangled msb
         * and use as hi 64 in our "bignum".
         */
        uint128_t hi = uint64_t(num.uuid.timestamp());
        uint128_t lo = uint64_t(num.uuid.get_least_significant_bits());

        return os << std::dec << ((hi << 64) | lo);
    }
};

sequence_number::sequence_number(std::string_view v) 
    : uuid([&] {
        using namespace boost::multiprecision;
        // workaround for weird clang 10 bug when calling constructor with
        // view directly.
        uint128_t tmp{std::string(v)};
        // see above
        return utils::UUID_gen::get_time_UUID_raw(utils::UUID_gen::decimicroseconds{uint64_t(tmp >> 64)},
            uint64_t(tmp & std::numeric_limits<uint64_t>::max()));
    }())
{}

}

template<typename ValueType>
struct rapidjson::internal::TypeHelper<ValueType, alternator::shard_id>
    : public from_string_helper<ValueType, alternator::shard_id>
{};

template<typename ValueType>
struct rapidjson::internal::TypeHelper<ValueType, alternator::sequence_number>
    : public from_string_helper<ValueType, alternator::sequence_number>
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
 * We would like to group cdc streams into N per shards 
 * (by token range/ ID set).
 * 
 * This however makes it impossible to do a simple 
 * range query with limit (we need to do staggered querying, because
 * of how GetRecords work).
 * 
 * We can do "paged" queries (i.e. with the get_records result limit and
 * continuing on "next" iterator) across cdc log PK:s (id:s), if we
 * somehow track paging state. 
 *
 * This is however problematic because while this can be encoded
 * in shard iterators (long), they _cannot_ be handled in sequence
 * numbers (short), and we need to be able to start a new iterator
 * using just a sequence number as watershed.
 * 
 * It also breaks per-shard sort order, where it is assumed that
 * records in a shard are presented "in order". 
 * 
 * To deal with both problems we would need to do merging of 
 * cdc streams. But this becomes difficult with actual cql paging etc,
 * we would potentially have way to many/way to few cql rows for 
 * whatever get_records query limit we have. And waste a lot of 
 * memory and cycles sorting "junk".
 * 
 * For now, go simple, but maybe consider if this latter approach would work
 * and perform. 
 * 
 * Parents:
 * DynamoDB has the concept of optional "parent shard", where parent
 * is the data set where data for some key range was recorded before
 * sharding was changed.
 *
 * While probably not critical, it is meant as an indicator as to
 * which shards should be read before other shards.
 *
 * In scylla, this is sort of akin to an ID having corresponding ID/ID:s
 * that cover the token range it represents. Because ID:s are per
 * vnode shard however, this relation can be somewhat ambigous.
 * We still provide some semblance of this by finding the ID in
 * older generation that has token start < current ID token start.
 * This will be a partial overlap, but it is the best we can do.
 */

static std::chrono::seconds confidence_interval(const database& db) {
    return std::chrono::seconds(db.get_config().alternator_streams_time_window_s());
}

// Dynamo docs says no data shall live longer than 24h.
static constexpr auto dynamodb_streams_max_window = 24h;

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
        if (!_cdc_metadata.streams_available()) {
            status = "ENABLING";
        } else {
            status = "ENABLED";
        }
    } 

    auto ttl = std::chrono::seconds(opts.ttl());
    
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

    auto normal_token_owners = _proxy.get_token_metadata_ptr()->count_normal_token_owners();

    // filter out cdc generations older than the table or now() - cdc::ttl (typically dynamodb_streams_max_window - 24h)
    auto low_ts = std::max(as_timepoint(schema->id()), db_clock::now() - ttl);

    return _sdks.cdc_get_versioned_streams(low_ts, { normal_token_owners }).then([this, &db, shard_start, limit, ret = std::move(ret), stream_desc = std::move(stream_desc)] (std::map<db_clock::time_point, cdc::streams_version> topologies) mutable {

        auto e = topologies.end();
        auto prev = e;
        auto shards = rjson::empty_array();

        std::optional<shard_id> last;

        auto i = topologies.begin();
        // if we're a paged query, skip to the generation where we left of.
        if (shard_start) {
            i = topologies.find(shard_start->time);
        }

        // for parent-child stuff we need id:s to be sorted by token
        // (see explanation above) since we want to find closest
        // token boundary when determining parent.
        // #7346 - we processed and searched children/parents in
        // stored order, which is not neccesarily token order,
        // so the finding of "closest" token boundary (using upper bound)
        // could give somewhat weird results.
        static auto token_cmp = [](const cdc::stream_id& id1, const cdc::stream_id& id2) {
            return id1.token() < id2.token();
        };

        // #7409 - shards must be returned in lexicographical order,
        // normal bytes compare is string_traits<int8_t>::compare.
        // thus bytes 0x8000 is less than 0x0000. By doing unsigned
        // compare instead we inadvertently will sort in string lexical.
        static auto id_cmp = [](const cdc::stream_id& id1, const cdc::stream_id& id2) {
            return compare_unsigned(id1.to_bytes(), id2.to_bytes()) < 0;
        };

        // need a prev even if we are skipping stuff
        if (i != topologies.begin()) {
            prev = std::prev(i);
        }

        for (; limit > 0 && i != e; prev = i, ++i) {
            auto& [ts, sv] = *i;

            last = std::nullopt;

            auto lo = sv.streams.begin();
            auto end = sv.streams.end();

            // #7409 - shards must be returned in lexicographical order,
            std::sort(lo, end, id_cmp);

            if (shard_start) {
                // find next shard position
                lo = std::upper_bound(lo, end, shard_start->id, id_cmp);
                shard_start = std::nullopt;
            }

            if (lo != end && prev != e) {
                // We want older stuff sorted in token order so we can find matching
                // token range when determining parent shard.
                std::stable_sort(prev->second.streams.begin(), prev->second.streams.end(), token_cmp);
            }

            auto expired = [&]() -> std::optional<db_clock::time_point> {
                auto j = std::next(i);
                if (j == e) {
                    return std::nullopt;
                }
                // add this so we sort of match potential 
                // sequence numbers in get_records result.
                return j->first + confidence_interval(db);
            }();

            while (lo != end) {
                auto& id = *lo++;

                auto shard = rjson::empty_object();

                if (prev != e) {
                    auto& pids = prev->second.streams;
                    auto pid = std::upper_bound(pids.begin(), pids.end(), id.token(), [](const dht::token& t, const cdc::stream_id& id) {
                        return t < id.token();
                    });
                    if (pid != pids.begin()) {
                        pid = std::prev(pid);
                    }
                    if (pid != pids.end()) {
                        rjson::set(shard, "ParentShardId", shard_id(prev->first, *pid));
                    }
                }

                last.emplace(ts, id);
                rjson::set(shard, "ShardId", *last);
                auto range = rjson::empty_object();
                rjson::set(range, "StartingSequenceNumber", sequence_number(utils::UUID_gen::min_time_UUID(ts.time_since_epoch())));
                if (expired) {
                    rjson::set(range, "EndingSequenceNumber", sequence_number(utils::UUID_gen::min_time_UUID(expired->time_since_epoch())));
                }

                rjson::set(shard, "SequenceNumberRange", std::move(range));
                rjson::push_back(shards, std::move(shard));
                
                if (--limit == 0) {
                    break;
                }

                last = std::nullopt;
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
    // A stream shard iterator can request either >=threshold
    // (inclusive of threshold) or >threshold (exclusive).
    static auto constexpr inclusive_marker = 'I';
    static auto constexpr exclusive_marker = 'i';

    static auto constexpr uuid_str_length = 36;

    utils::UUID table;
    utils::UUID threshold;
    shard_id shard;
    bool inclusive;

    shard_iterator(const sstring&);

    shard_iterator(utils::UUID t, shard_id i, utils::UUID th, bool inclusive)
        : table(t)
        , threshold(th)
        , shard(i)
        , inclusive(inclusive)
    {}
    friend std::ostream& operator<<(std::ostream& os, const shard_iterator& id) {
        return os << (id.inclusive ? inclusive_marker : exclusive_marker) << std::hex
            << id.table
            << ':' << id.threshold
            << ':' << id.shard
            ;
    }
};

shard_iterator::shard_iterator(const sstring& s) 
    : table(s.substr(1, uuid_str_length))
    , threshold(s.substr(2 + uuid_str_length, uuid_str_length))
    , shard(s.substr(3 + 2 * uuid_str_length))
    , inclusive(s.at(0) == inclusive_marker)
{
    if (s.at(0) != inclusive_marker && s.at(0) != exclusive_marker) {
        throw std::invalid_argument(s);
    }
}
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
    auto seq_num = rjson::get_opt<sequence_number>(request, "SequenceNumber");
    
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
    if (!schema || !cdc::get_base_table(db, *schema) || !is_alternator_keyspace(schema->ks_name())) {
        throw api_error::resource_not_found("Invalid StreamArn");
    }
    if (!sid) {
        throw api_error::resource_not_found("Invalid ShardId");
    }

    utils::UUID threshold;
    bool inclusive_of_threshold;

    switch (type) {
        case shard_iterator_type::AT_SEQUENCE_NUMBER:
            threshold = *seq_num;
            inclusive_of_threshold = true;
            break;
        case shard_iterator_type::AFTER_SEQUENCE_NUMBER:
            threshold = *seq_num;
            inclusive_of_threshold = false;
            break;
        case shard_iterator_type::TRIM_HORIZON:
            // zero UUID - lowest possible timestamp. Include all
            // data not ttl:ed away.
            threshold = utils::UUID{};
            inclusive_of_threshold = true;
            break;
        case shard_iterator_type::LATEST:
            threshold = utils::UUID_gen::min_time_UUID((db_clock::now() - confidence_interval(db)).time_since_epoch());
            inclusive_of_threshold = true;
            break;
    }

    shard_iterator iter(stream_arn, *sid, threshold, inclusive_of_threshold);

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
        auto& log_table = db.find_column_family(iter.table);
        schema = log_table.schema();
        base = cdc::get_base_table(db, *schema);
    } catch (...) {        
    }

    if (!schema || !base || !is_alternator_keyspace(schema->ks_name())) {
        throw api_error::resource_not_found(boost::lexical_cast<std::string>(iter.table));
    }

    tracing::add_table_name(trace_state, schema->ks_name(), schema->cf_name());

    db::consistency_level cl = db::consistency_level::LOCAL_QUORUM;
    partition_key pk = iter.shard.id.to_partition_key(*schema);

    dht::partition_range_vector partition_ranges{ dht::partition_range::make_singular(dht::decorate_key(*schema, pk)) };

    auto high_ts = db_clock::now() - confidence_interval(db);
    auto high_uuid = utils::UUID_gen::min_time_UUID(high_ts.time_since_epoch());
    auto lo = clustering_key_prefix::from_exploded(*schema, { iter.threshold.serialize() });
    auto hi = clustering_key_prefix::from_exploded(*schema, { high_uuid.serialize() });

    std::vector<query::clustering_range> bounds;
    using bound = typename query::clustering_range::bound;
    bounds.push_back(query::clustering_range::make(bound(lo, iter.inclusive), bound(hi, false)));

    static const bytes timestamp_column_name = cdc::log_meta_column_name_bytes("time");
    static const bytes op_column_name = cdc::log_meta_column_name_bytes("operation");
    static const bytes eor_column_name = cdc::log_meta_column_name_bytes("end_of_batch");

    auto key_names = boost::copy_range<attrs_to_get>(
        boost::range::join(std::move(base->partition_key_columns()), std::move(base->clustering_key_columns()))
        | boost::adaptors::transformed([&] (const column_definition& cdef) {
            return std::make_pair<std::string, attrs_to_get_node>(cdef.name_as_text(), {}); })
    );
    // Include all base table columns as values (in case pre or post is enabled).
    // This will include attributes not stored in the frozen map column
    auto attr_names = boost::copy_range<attrs_to_get>(base->regular_columns()
        // this will include the :attrs column, which we will also force evaluating. 
        // But not having this set empty forces out any cdc columns from actual result 
        | boost::adaptors::transformed([] (const column_definition& cdef) {
            return std::make_pair<std::string, attrs_to_get_node>(cdef.name_as_text(), {}); })
    );

    std::vector<const column_definition*> columns;
    columns.reserve(schema->all_columns().size());

    auto pks = schema->partition_key_columns();
    auto cks = schema->clustering_key_columns();

    std::transform(pks.begin(), pks.end(), std::back_inserter(columns), [](auto& c) { return &c; });
    std::transform(cks.begin(), cks.end(), std::back_inserter(columns), [](auto& c) { return &c; });

    auto regular_columns = boost::copy_range<query::column_id_vector>(schema->regular_columns() 
        | boost::adaptors::filtered([](const column_definition& cdef) { return cdef.name() == op_column_name || cdef.name() == eor_column_name || !cdc::is_cdc_metacolumn_name(cdef.name_as_text()); })
        | boost::adaptors::transformed([&] (const column_definition& cdef) { columns.emplace_back(&cdef); return cdef.id; })
    );

    stream_view_type type = cdc_options_to_steam_view_type(base->cdc_options());

    auto selection = cql3::selection::selection::for_columns(schema, std::move(columns));
    auto partition_slice = query::partition_slice(
        std::move(bounds)
        , {}, std::move(regular_columns), selection->get_query_options());

	auto& opts = base->cdc_options();
	auto mul = 2; // key-only, allow for delete + insert
    if (opts.preimage()) {
        ++mul;
    }
    if (opts.postimage()) {
        ++mul;
    }
    auto command = ::make_lw_shared<query::read_command>(schema->id(), schema->version(), partition_slice, _proxy.get_max_result_size(partition_slice),
            query::row_limit(limit * mul));

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
        auto eor_index = std::distance(metadata.get_names().begin(), 
            std::find_if(metadata.get_names().begin(), metadata.get_names().end(), [](const lw_shared_ptr<cql3::column_specification>& cdef) {
                return cdef->name->name() == eor_column_name;
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
            auto eor = row[eor_index].has_value() ? value_cast<bool>(boolean_type->deserialize(*row[eor_index])) : false;

            if (!dynamodb.HasMember("Keys")) {
                auto keys = rjson::empty_object();
                describe_single_item(*selection, row, key_names, keys);
                rjson::set(dynamodb, "Keys", std::move(keys));
                rjson::set(dynamodb, "ApproximateCreationDateTime", utils::UUID_gen::unix_timestamp_in_sec(ts).count());
                rjson::set(dynamodb, "SequenceNumber", sequence_number(ts));
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
                describe_single_item(*selection, row, key_names, item);
                rjson::set(dynamodb, op == cdc::operation::pre_image ? "OldImage" : "NewImage", std::move(item));
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
            if (eor) {
                maybe_add_record();
                timestamp = ts;
                if (limit == 0) {
                    break;
                }
            }
        }

        auto ret = rjson::empty_object();
        auto nrecords = records.Size();
        rjson::set(ret, "Records", std::move(records));

        if (nrecords != 0) {
            // #9642. Set next iterators threshold to > last
            shard_iterator next_iter(iter.table, iter.shard, *timestamp, false);
            // Note that here we unconditionally return NextShardIterator,
            // without checking if maybe we reached the end-of-shard. If the
            // shard did end, then the next read will have nrecords == 0 and
            // will notice end end of shard and not return NextShardIterator.
            rjson::set(ret, "NextShardIterator", next_iter);
            _stats.api_operations.get_records_latency.add(std::chrono::steady_clock::now() - start_time);
            return make_ready_future<executor::request_return_type>(make_jsonable(std::move(ret)));
        }

        // ugh. figure out if we are and end-of-shard
        auto normal_token_owners = _proxy.get_token_metadata_ptr()->count_normal_token_owners();
        
        return _sdks.cdc_current_generation_timestamp({ normal_token_owners }).then([this, iter, high_ts, start_time, ret = std::move(ret)](db_clock::time_point ts) mutable {
            auto& shard = iter.shard;            

            if (shard.time < ts && ts < high_ts) {
                // The DynamoDB documentation states that when a shard is
                // closed, reading it until the end has NextShardIterator
                // "set to null". Our test test_streams_closed_read
                // confirms that by "null" they meant not set at all.
            } else {
                // We could have return the same iterator again, but we did
                // a search from it until high_ts and found nothing, so we
                // can also start the next search from high_ts.
                // TODO: but why? It's simpler just to leave the iterator be.
                shard_iterator next_iter(iter.table, iter.shard, utils::UUID_gen::min_time_UUID(high_ts.time_since_epoch()), true);
                rjson::set(ret, "NextShardIterator", iter);
            }
            _stats.api_operations.get_records_latency.add(std::chrono::steady_clock::now() - start_time);
            return make_ready_future<executor::request_return_type>(make_jsonable(std::move(ret)));
        });
    });
}

void executor::add_stream_options(const rjson::value& stream_specification, schema_builder& builder) const {
    auto stream_enabled = rjson::find(stream_specification, "StreamEnabled");
    if (!stream_enabled || !stream_enabled->IsBool()) {
        throw api_error::validation("StreamSpecification needs boolean StreamEnabled");
    }

    if (stream_enabled->GetBool()) {
        auto& db = _proxy.get_db().local();

        if (!db.features().cluster_supports_cdc()) {
            throw api_error::validation("StreamSpecification: streams (CDC) feature not enabled in cluster.");
        }
        if (!db.features().cluster_supports_alternator_streams()) {
            throw api_error::validation("StreamSpecification: alternator streams feature not enabled in cluster.");
        }

        cdc::options opts;
        opts.enabled(true);
        opts.set_delta_mode(cdc::delta_mode::keys);
        opts.ttl(std::chrono::duration_cast<std::chrono::seconds>(dynamodb_streams_max_window).count());

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

void executor::supplement_table_stream_info(rjson::value& descr, const schema& schema) const {
    auto& opts = schema.cdc_options();
    if (opts.enabled()) {
        auto& db = _proxy.get_db().local();
        auto& cf = db.find_column_family(schema.ks_name(), cdc::log_name(schema.cf_name()));
        stream_arn arn(cf.schema()->id());
        rjson::set(descr, "LatestStreamArn", arn);
        rjson::set(descr, "LatestStreamLabel", rjson::from_string(stream_label(*cf.schema())));

        auto stream_desc = rjson::empty_object();
        rjson::set(stream_desc, "StreamEnabled", true);

        auto mode = stream_view_type::KEYS_ONLY;
        if (opts.preimage() && opts.postimage()) {
            mode = stream_view_type::NEW_AND_OLD_IMAGES;
        } else if (opts.preimage()) {
            mode = stream_view_type::OLD_IMAGE;
        } else if (opts.postimage()) {
            mode = stream_view_type::NEW_IMAGE;
        }
        rjson::set(stream_desc, "StreamViewType", mode);
        rjson::set(descr, "StreamSpecification", std::move(stream_desc));
    }
}

}
