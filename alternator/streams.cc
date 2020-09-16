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
        auto s = ss.str();
        return v.SetString(s.data(), s.size(), a);
    }
};

template<typename ValueType>
struct rapidjson::internal::TypeHelper<ValueType, utils::UUID>
    : public from_string_helper<ValueType, utils::UUID>
{};

static db_clock::time_point as_timepoint(const utils::UUID& uuid) {
    return db_clock::time_point{std::chrono::milliseconds(utils::UUID_gen::get_adjusted_timestamp(uuid))};
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

/**
 * Note: because "end_time" will be zero for non-closed generation
 * shards, but non-zero once said generation expires, we will in 
 * fact give "multiple versions" of the same shard, i.e. a shard
 * will change ID (as presented to the user).
 * 
 * The dynamo specs do not forbid this, but we are (probably)
 * expected to allow a shard that is still valid (not 24h+ old)
 * to still be queried, hence we "repair" info in iterator 
 * query later on.
 * 
 * The alternative would be to either be stateful, or query
 * end timestamp on get_shard_iterator. But we cannot currently 
 * do this in any really efficient way for a single shard, so
 * let's be mutable instead.
 * 
 */
struct shard_id {
    static constexpr auto marker = 'H';
    static constexpr auto hex_width = sizeof(uint64_t)*8/4;

    db_clock::time_point start_time;
    db_clock::time_point end_time; // zero if open
    dht::token lo_token; // range start, inclusive (first id of vnode)
    dht::token hi_token; // range end, inclusive (last id of vnode)

    shard_id(db_clock::time_point start,  db_clock::time_point end, dht::token lo, dht::token hi)
        : start_time(start)
        , end_time(end)
        , lo_token(lo)
        , hi_token(hi)
    {
        // cdc generations cannot really have start==end, but for our purposes
        // it is still passable/valid, so allow it.
        assert(end_time == db_clock::time_point{} || start_time < end_time);
    }
    shard_id(const sstring&);

    bool has_expired() const {
        return end_time != db_clock::time_point{};
    }
    bool is_wraparound() const {
        return lo_token > hi_token;
    }

    // dynamo specifies shardid as max 65 chars. Thus we cannot use separators, 
    // instead we have to rely on 16 width hex
    friend std::ostream& operator<<(std::ostream& os, const shard_id& id) {
        boost::io::ios_flags_saver fs(os);
        return os << marker << std::hex << std::setfill('0') 
            << std::setw(hex_width) << id.start_time.time_since_epoch().count()
            << std::setw(hex_width) << id.end_time.time_since_epoch().count()
            << std::setw(hex_width) << (id.lo_token.is_minimum() ? 0ull : dht::token::to_int64(id.lo_token))
            << std::setw(hex_width) << (id.hi_token.is_maximum() ? 0ull : dht::token::to_int64(id.hi_token))
            ;
    }
};

shard_id::shard_id(const sstring& s) {
    if (s.at(0) != marker || s.size() != (1 + hex_width * 4)) {
        throw std::invalid_argument(s);
    }

    start_time = db_clock::time_point(db_clock::duration(std::stoull(s.substr(1, hex_width), nullptr, 16)));
    end_time = db_clock::time_point(db_clock::duration(std::stoull(s.substr(1 + hex_width, hex_width), nullptr, 16)));
    auto lo = std::stoull(s.substr(1 + 2*hex_width, hex_width), nullptr, 16);
    auto hi = std::stoull(s.substr(1 + 3*hex_width), nullptr, 16);
    lo_token = lo == 0 ? dht::minimum_token() : dht::token::from_int64(lo);
    hi_token = hi == 0 ? dht::maximum_token() : dht::token::from_int64(hi);

    if (end_time != db_clock::time_point{} && end_time < start_time) {
        throw std::invalid_argument(format("Invalid ShardID {}. Start={}, End={}", s, start_time, end_time));
    }
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

        uint128_t hi = uint64_t(num.uuid.get_most_significant_bits());
        uint128_t lo = uint64_t(num.uuid.get_least_significant_bits());

        return os << std::dec << ((hi << 64) | lo);
    }
};

sequence_number::sequence_number(std::string_view v) 
    : uuid([&] {
        using namespace boost::multiprecision;
        uint128_t tmp{v};
        return utils::UUID(uint64_t(tmp >> 64), uint64_t(tmp & std::numeric_limits<uint64_t>::max()));
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
 * is making a shard equal to a vnode token range, by using the 
 * cdc stream id "index" grouping. 
 * 
 * We stil get a lot of shards, but fewer than with 1:1 mapping
 * cdc stream -> shard. This means our queries will cross actual
 * scylla shards, but that is not neccessarily a bad thing.
 * 
 * Note we could possibly reduce the number of shards by simply 
 * splitting the id/token range into N (chosen by some critera, 
 * probably ring size) and hope the range queries crossing vnode
 * bounds are not that bad. To do so, it is a fairly easy change
 * in the code below.
 */

static std::chrono::seconds confidence_interval(const database& db) {
    return std::chrono::seconds(db.get_config().alternator_streams_time_window_s());
}

// Dynamo docs says no data shall live longer than 24h.
static constexpr auto dynamo_streams_max_window = 24h;

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

    const auto& tm = _proxy.get_token_metadata();
    // cannot really "resume" query, must iterate all data. because we cannot query neither "time" (pk) > something,
    // or on expired...
    // TODO: maybe add secondary index to topology table to enable this?
    return _sdks.cdc_get_versioned_streams({ tm.count_normal_token_owners() }).then([this, &db, schema, shard_start, limit, ret = std::move(ret), stream_desc = std::move(stream_desc)](std::map<db_clock::time_point, cdc::streams_version> topologies) mutable {

        // filter out cdc generations older than the table or now() - dynamo_streams_max_window (24h)
        auto low_ts = std::max(as_timepoint(schema->id()), db_clock::now() - dynamo_streams_max_window);

        auto i = topologies.lower_bound(low_ts);
        // need first gen _intersecting_ the timestamp.
        if (i != topologies.begin()) {
            i = std::prev(i);
        }

        auto e = topologies.end();
        auto prev = e;
        auto shards = rjson::empty_array();

        std::optional<shard_id> last;

        // i is now at the youngest generation we include. make a mark of it.
        auto first = i;

        // if we're a paged query, skip to the generation where we left of.
        if (shard_start) {
            i = topologies.find(shard_start->start_time);
        }

        // need a prev even if we are skipping stuff
        if (i != first) {
            prev = std::prev(i);
            std::stable_sort(prev->second.streams.begin(), prev->second.streams.end(), [](auto& id1, auto& id2) { 
                return id1.token() < id2.token(); 
            });
        }

        for (; limit > 0 && i != e; prev = i, ++i) {
            auto& [ts, sv] = *i;

            last = std::nullopt;

            auto get_expiration = [&](auto iter) {
                if (++iter != e) {
                    return iter->first;
                }
                return db_clock::time_point{};
            };

            auto lo = sv.streams.begin();
            auto end = sv.streams.end();

            // first sort by index.
            std::stable_sort(lo, end, [](auto& id1, auto& id2) {
                return id1.index() < id2.index();
            });

            if (shard_start) {
                // find next shard position
                // first id set is the wraparound range. skip it here.
                lo = std::find_if(lo, end, [](const cdc::stream_id& id) { return id.index() != 0; });
                lo = std::upper_bound(lo, end, shard_start->hi_token, [](const dht::token& t, const cdc::stream_id& id) {
                    return t < id.token();
                });
                shard_start = std::nullopt;
            }

            for (auto hi = lo; lo != end; lo = hi) {
                auto& id = *lo++;

                hi = std::find_if(lo, end, [i = id.index()](const cdc::stream_id& id) {
                    return id.index() != i;
                });
                auto t = id.token();
                auto et = std::prev(hi)->token();

                last.emplace(ts, get_expiration(i), t, et);

                auto shard = rjson::empty_object();

                if (prev != e) {
                    auto& pids = prev->second.streams;
                    // maybe re-sort by token, so we can find the previous shard bounds
                    // if prev is sorted by index, we can just rotate it slightly to make it sorted by
                    // token.
                    if (pids.front().token() > pids.back().token()) {
                        auto nf = std::find_if(pids.begin(), pids.end(), [t = pids.back().token()](const cdc::stream_id& id) {
                            return id.token() < t;
                        });
                        std::rotate(pids.begin(), nf, pids.end());
                    }
                    auto pid = std::upper_bound(pids.begin(), pids.end(), t, [](const dht::token& t, const cdc::stream_id& id) {
                        return t < id.token();
                    });
                    if (pid != pids.begin()) {
                        pid = std::prev(pid);
                    }
                    if (pid != pids.end()) {
                        auto index = pid->index();
                        auto cmp = [index](const cdc::stream_id& id) {
                            return id.index() != index;
                        };
                        // reverse iteration: base will point to elem matching index
                        auto plo = index == 0
                            ? std::find_if(pids.rbegin(), pids.rend(), cmp).base()
                            : std::find_if(std::make_reverse_iterator(pid), pids.rend(), cmp).base()
                            ;
                        auto phi = index == 0
                            ? std::find_if(pids.begin(), pids.end(), cmp)
                            : std::find_if(pid, pids.end(), cmp)
                            ;
                        auto lt = plo->token();
                        auto et = std::prev(phi)->token();
                        rjson::set(shard, "ParentShardId", shard_id(prev->first, get_expiration(prev), lt, et));
                    }
                }

                auto range = rjson::empty_object();
                rjson::set(range, "StartingSequenceNumber", sequence_number(utils::UUID_gen::min_time_UUID(ts.time_since_epoch().count())));
                if (last->has_expired()) {
                    rjson::set(range, "EndingSequenceNumber", sequence_number(utils::UUID_gen::min_time_UUID(last->end_time.time_since_epoch().count())));
                }

                rjson::set(shard, "ShardId", *last);
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

    auto f = make_ready_future<shard_id>(*sid);

    if (!sid->has_expired()) {
        // see comment in get_records. Need to check if this (open)
        // shard is in fact still valid.
        auto& ep = utils::fb_utilities::get_broadcast_address();
        auto ts = cdc::get_streams_timestamp_for(ep, _ss.gossiper());
        if (ts != db_clock::time_point{} &&  ts != sid->start_time) {
            // The shard expired between user queried it and 
            // this call. Client probably does not expect a shard ID to 
            // "change"/disappear like this sort of does (replaced with
            // one with an end time in it), so we'll simply repair it
            // in place and give an iterator that knows its end time.
            // Hopefully the client will then read until end-of-shard
            // data using this iterator, and can move on to next
            // describe_stream result set next.
            const auto& tm = _proxy.get_token_metadata();
            f = _sdks.get_next_stream_timestamp({ tm.count_normal_token_owners() }, sid->start_time).then([sid = *sid](db_clock::time_point ts) mutable {
                sid.end_time = ts;
                return sid;
            });
        }
    }

    return f.then([&db, stream_arn, schema, type, seq_num = std::move(seq_num)](shard_id sid) {
        if (sid.has_expired() && (db_clock::now() - dynamo_streams_max_window) <= sid.end_time) {
            throw api_error::trimmed_data_access_exception("Expired shard");
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
                threshold = utils::UUID_gen::min_time_UUID(sid.start_time.time_since_epoch().count());
                inclusive_of_threshold = true;
                break;
            case shard_iterator_type::LATEST:
                threshold = utils::UUID_gen::min_time_UUID((db_clock::now() - confidence_interval(db)).time_since_epoch().count());
                inclusive_of_threshold = true;
                break;
        }

        shard_iterator iter(stream_arn, sid, threshold, inclusive_of_threshold);

        auto ret = rjson::empty_object();
        rjson::set(ret, "ShardIterator", iter);

        return make_ready_future<executor::request_return_type>(make_jsonable(std::move(ret)));
    });
}

struct event_id {
    utils::UUID stream;
    utils::UUID timestamp;

    static const auto marker = 'E';

    event_id(utils::UUID s, utils::UUID ts)
        : stream(s)
        , timestamp(ts)
    {}
    
    friend std::ostream& operator<<(std::ostream& os, const event_id& id) {
        boost::io::ios_flags_saver fs(os);
        return os << marker << id.stream << ':' << id.timestamp;
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

    auto f = make_ready_future<shard_iterator>(iter);

    if (!iter.shard.has_expired()) {
        // ugh. figure out if we have end-of-shard
        // We try to avoid the extra query by using the
        // gossip state as a cache check - i.e. know if
        // topology has changed and invalidated the current
        // iterator. This is far from exact, so we could still
        // generate duplicate records at generation changes
        // unless our confidence window is large enough.
        auto& ep = utils::fb_utilities::get_broadcast_address();
        auto ts = cdc::get_streams_timestamp_for(ep, _ss.gossiper());
        if (ts != db_clock::time_point{} &&  ts != iter.shard.start_time) {
            // Sigh. there seem to be at least one new generation after this.
            // Do a query for our actual end timestamp (i.e. start of generation
            // after use), and update the iterator.
            // We could just say "expired iterator". This would cause kinesis et all 
            // to get a new iterator with the same shard ID though, which is why
            // we replace end time in get_shard_iterator if needed. But we can just as
            // well save the round trip and update the iterator ourselves.
            const auto& tm = _proxy.get_token_metadata();
            f = _sdks.get_next_stream_timestamp({ tm.count_normal_token_owners() }, iter.shard.start_time).then([iter](db_clock::time_point ts) mutable {
                iter.shard.end_time = ts;
                return iter;
            });
        }
    }

    return f.then([this, schema, base, limit, &client_state, permit = std::move(permit)](shard_iterator iter) mutable {
        return do_get_records(client_state, std::move(permit), schema, base, limit, iter);
    }).finally([this, start_time] {
        _stats.api_operations.get_records_latency.add(std::chrono::steady_clock::now() - start_time);
    });
}

future<executor::request_return_type> executor::do_get_records(client_state& client_state, service_permit permit, schema_ptr schema, schema_ptr base, size_t limit, shard_iterator iter) {
    db::consistency_level cl = db::consistency_level::LOCAL_QUORUM;

    auto lo_pk_bound = dht::partition_range::bound(dht::ring_position::starting_at(iter.shard.lo_token));
    auto hi_pk_bound = dht::partition_range::bound(dht::ring_position::ending_at(iter.shard.hi_token));

    auto partition_ranges = iter.shard.is_wraparound()
        ? dht::partition_range_vector{ dht::partition_range::make_starting_with(std::move(lo_pk_bound)), dht::partition_range::make_ending_with(std::move(hi_pk_bound))}
        : dht::partition_range_vector{ dht::partition_range::make(std::move(lo_pk_bound), std::move(hi_pk_bound)) }
        ;

    auto& db = _proxy.get_db().local();
    auto high_ts = db_clock::now() - confidence_interval(db);
    if (iter.shard.has_expired() && high_ts > iter.shard.end_time) {
        high_ts = iter.shard.end_time;
    }

    auto high_uuid = utils::UUID_gen::min_time_UUID(high_ts.time_since_epoch().count());
    auto lo = clustering_key_prefix::from_exploded(*schema, { iter.threshold.serialize() });
    auto hi = clustering_key_prefix::from_exploded(*schema, { high_uuid.serialize() });

    std::vector<query::clustering_range> bounds;
    using bound = typename query::clustering_range::bound;
    bounds.push_back(query::clustering_range::make(bound(lo, iter.inclusive), bound(hi, false)));

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
            [this, schema, partition_slice = std::move(partition_slice), selection = std::move(selection), limit, key_names = std::move(key_names), attr_names = std::move(attr_names), type, iter, high_ts] (service::storage_proxy::coordinator_query_result qr) mutable {
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
                rjson::set(record, "eventID", event_id(iter.table, *timestamp));
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
        }
        if (limit > 0 && timestamp) {
            maybe_add_record();
        }

        auto ret = rjson::empty_object();
        rjson::set(ret, "Records", std::move(records));

        auto high = timestamp ? *timestamp : utils::UUID_gen::min_time_UUID(high_ts.time_since_epoch().count());
        if (!iter.shard.has_expired() || as_timepoint(high) < iter.shard.end_time) {
            rjson::set(ret, "NextShardIterator", shard_iterator(iter.table, iter.shard, high, false));
        }
        return make_ready_future<executor::request_return_type>(make_jsonable(std::move(ret)));
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

        cdc::options opts;
        opts.enabled(true);
        opts.set_delta_mode(cdc::delta_mode::keys);

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
