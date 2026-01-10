/*
 * Copyright 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <type_traits>
#include <ranges>
#include <generator>
#include <boost/lexical_cast.hpp>
#include <boost/regex.hpp>
#include <boost/io/ios_state.hpp>
#include <boost/multiprecision/cpp_int.hpp>

#include <seastar/json/formatter.hh>

#include "db/config.hh"

#include "cdc/log.hh"
#include "cdc/generation.hh"
#include "cdc/cdc_options.hh"
#include "cdc/metadata.hh"
#include "db/system_distributed_keyspace.hh"
#include "utils/UUID_gen.hh"
#include "cql3/selection/selection.hh"
#include "cql3/result_set.hh"
#include "cql3/column_identifier.hh"
#include "replica/database.hh"
#include "schema/schema_builder.hh"
#include "service/storage_proxy.hh"
#include "gms/feature.hh"
#include "gms/feature_service.hh"

#include "executor.hh"
#include "streams.hh"
#include "data_dictionary/data_dictionary.hh"
#include "utils/rjson.hh"

extern logging::logger elogger;

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

static db_clock::time_point as_timepoint(const table_id& tid) {
    return db_clock::time_point{utils::UUID_gen::unix_timestamp(tid.uuid())};
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
    stream_arn(const table_id& tid)
        : UUID(tid.uuid())
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

} // namespace alternator

template<typename ValueType>
struct rapidjson::internal::TypeHelper<ValueType, alternator::stream_arn>
    : public from_string_helper<ValueType, alternator::stream_arn>
{};

namespace alternator {

static sstring get_table_name_from_dynamodb_stream_arn(std::string_view text_stream_arn);
static table_id get_cdc_log_table_id_from_stream_arn(service::storage_proxy& proxy, std::string_view text_stream_arn);
static std::pair<schema_ptr, schema_ptr> get_stream_schema_and_base_schema_from_arn(service::storage_proxy& proxy, std::string_view arn);
static std::tuple<schema_ptr, schema_ptr, std::string> get_stream_schema_and_base_schema_from_request(service::storage_proxy& proxy, const rjson::value &request);

future<alternator::executor::request_return_type> alternator::executor::list_streams(client_state& client_state, service_permit permit, rjson::value request) {
    _stats.api_operations.list_streams++;

    auto limit = rjson::get_opt<int>(request, "Limit").value_or(100);
    auto streams_start = rjson::get_opt<stream_arn>(request, "ExclusiveStartStreamArn");
    auto table = find_table(_proxy, request);
    auto db = _proxy.data_dictionary();

    if (limit < 1) {
        throw api_error::validation("Limit must be 1 or more");
    }

    std::vector<data_dictionary::table> cfs;

    if (table) {
        auto log_name = cdc::log_name(table->cf_name());
        try {
            cfs.emplace_back(db.find_table(table->ks_name(), log_name));
        } catch (data_dictionary::no_such_column_family&) {
            cfs.clear();
        }
    } else {
        cfs = db.get_tables();
    }

    // # 12601 (maybe?) - sort the set of tables on ID. This should ensure we never
    // generate duplicates in a paged listing here. Can obviously miss things if they 
    // are added between paged calls and end up with a "smaller" UUID/ARN, but that 
    // is to be expected.
    if (std::cmp_less(limit, cfs.size()) || streams_start) {
        std::sort(cfs.begin(), cfs.end(), [](const data_dictionary::table& t1, const data_dictionary::table& t2) {
            return t1.schema()->id().uuid() < t2.schema()->id().uuid();
        });
    }

    auto i = cfs.begin();
    auto e = cfs.end();

    if (streams_start) {
        i = std::find_if(i, e, [&](const data_dictionary::table& t) {
            return t.schema()->id().uuid() == streams_start
                && cdc::get_base_table(db.real_database(), *t.schema())
                && is_alternator_keyspace(t.schema()->ks_name())
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
        auto s = i->schema();
        auto& ks_name = s->ks_name();
        auto& cf_name = s->cf_name();

        if (!is_alternator_keyspace(ks_name)) {
            continue;
        }
        if (cdc::is_log_for_some_table(db.real_database(), ks_name, cf_name)) {
            rjson::value new_entry = rjson::empty_object();

            last = i->schema()->id();
            rjson::add(new_entry, "StreamArn", *last);
            rjson::add(new_entry, "StreamLabel", rjson::from_string(stream_label(*s)));
            rjson::add(new_entry, "TableName", rjson::from_string(cdc::base_name(table_name(*s))));
            rjson::push_back(streams, std::move(new_entry));

            --limit;
        }
    }

    rjson::add(ret, "Streams", std::move(streams));

    if (last) {
        rjson::add(ret, "LastEvaluatedStreamArn", *last);
    }

    return make_ready_future<executor::request_return_type>(rjson::print(std::move(ret)));
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
        fmt::print(os, "{} {:x}:{}", marker, id.time.time_since_epoch().count(), id.id.to_bytes());
        return os;
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
         * so we can simply unpack the mangled msb
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

} // namespace alternator

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

} // namespace alternator

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
 * vnode shard however, this relation can be somewhat ambiguous.
 * We still provide some semblance of this by finding the ID in
 * older generation that has token start < current ID token start.
 * This will be a partial overlap, but it is the best we can do.
 */

static std::chrono::seconds confidence_interval(data_dictionary::database db) {
    return std::chrono::seconds(db.get_config().alternator_streams_time_window_s());
}

using namespace std::chrono_literals;

// Dynamo docs says no data shall live longer than 24h.
static constexpr auto dynamodb_streams_max_window = 24h;

// for parent-child stuff we need id:s to be sorted by token
// (see explanation above) since we want to find closest
// token boundary when determining parent.
// #7346 - we processed and searched children/parents in
// stored order, which is not necessarily token order,
// so the finding of "closest" token boundary (using upper bound)
// could give somewhat weird results.
static bool token_cmp(const cdc::stream_id& id1, const cdc::stream_id& id2) {
    return id1.token() < id2.token();
}

// #7409 - shards must be returned in lexicographical order,
// normal bytes compare is string_traits<int8_t>::compare.
// thus bytes 0x8000 is less than 0x0000. By doing unsigned
// compare instead we inadvertently will sort in string lexical.
static bool id_cmp(const cdc::stream_id& id1, const cdc::stream_id& id2) {
    return compare_unsigned(id1.to_bytes(), id2.to_bytes()) < 0;
}

stream_id_range::stream_id_range(
        utils::chunked_vector<cdc::stream_id> &items,
        utils::chunked_vector<cdc::stream_id>::iterator lo1,
        utils::chunked_vector<cdc::stream_id>::iterator end1)
    : items(&items)
    , lo1(lo1)
    , end1(end1)
    , lo2(items.end())
    , end2(items.end())
{
    assert(end1 <= lo2);
}

stream_id_range::stream_id_range(
        utils::chunked_vector<cdc::stream_id> &items,
        utils::chunked_vector<cdc::stream_id>::iterator lo1,
        utils::chunked_vector<cdc::stream_id>::iterator end1,
        utils::chunked_vector<cdc::stream_id>::iterator lo2,
        utils::chunked_vector<cdc::stream_id>::iterator end2)
    : items(&items)
    , lo1(lo1)
    , end1(end1)
    , lo2(lo2)
    , end2(end2)
{
    assert(end1 <= lo2);
}

bool stream_id_range::apply_starting_position_update(const cdc::stream_id &update_to) {
    skip_to = &update_to;
    return true;
}

std::generator<const cdc::stream_id&> stream_id_range::iterate() {
    assert(end1 <= lo2);
    // we need to manually swap both ranges into one, sort and yield over
    auto tgt = end1;
    auto src = lo2;
    for(; src != end2; ++src, ++tgt) {
        std::swap(*tgt, *src);
    }
    std::sort(lo1, tgt, id_cmp);
    if (skip_to) {
        auto it = std::lower_bound(lo1, tgt, *skip_to, id_cmp);
        if (it == items->end() || it->token() != skip_to->token()) {
            elogger.info("Could not find starting position update shard id {}", *skip_to);
        }
        else {
            elogger.info("QWERTY found starting position");
            lo1 = std::next(it);
        }
    }

    // auto it = std::lower_bound(lo1, end1, update_to.token(), [](const cdc::stream_id& id, const dht::token& t) {
    //     return id.token() < t;
    // });
    // bool found_in_first = true;
    // if (it == end1) {
    //     found_in_first = false;
    //     it = std::lower_bound(lo2, end2, update_to.token(), [](const cdc::stream_id& id, const dht::token& t) {
    //         return id.token() < t;
    //     });
    // }
    // if (it == items->end() || *it != update_to) {
    //     elogger.info("Could not find starting position update shard id {} in stream id ranges [{}] [{}]", 
    //         update_to, fmt::join(lo1, end1, "; "), fmt::join(lo2, end2, "; "));
    //     return false;
    // }
    // if (found_in_first) {
    //     lo1 = std::next(it);
    // } else {
    //     lo1 = end1;
    //     lo2 = std::next(it);
    // }
    // return true;

    for(auto it = lo1; it != tgt; ++it) {
        co_yield *it;
    }
}


// the function returns `stream_id_range` that will allow iteration over children shards for the shard `parent`
// a child shard is defined as a shard that touches token range that was previously covered by `parent` shard
// shard contains a token, that represents end of the token range for that shard (inclusive)
// begginning of the token range is defined by previous shard's token + 1
// NOTE: vnodes wrap around, tablets don't - tablet guarantees that last token in the token space is always present as one of the shards
// NOTE: both streams generation are guaranteed to cover whole range and be non-empty
//
// to find children we will first find parent shard in parent_streams by its token
// then we will find previous shard in parent stream - that will determine range
// then based on the range we will find children shards in current_streams
// NOTE: function sorts / reorders current_streams
// NOTE: function assumes parent_streams is sorted by token_cmp and it doesn't modify it

stream_id_range find_children_range_from_parent_token(
    const utils::chunked_vector<cdc::stream_id>& parent_streams,
    utils::chunked_vector<cdc::stream_id>& current_streams,
    cdc::stream_id parent,
    bool uses_tablets
) {
    // sanity checks for required preconditions
    if (parent_streams.empty()) {
        on_internal_error(elogger, fmt::format("parent_streams is empty") );
    }
    if (current_streams.empty()) {
        on_internal_error(elogger, fmt::format("current_streams is empty") );
    }

    // first let's cover obvious cases
    // if we have only one parent shard, then all children belong to it
    if (parent_streams.size() == 1) {
        return stream_id_range{ current_streams, current_streams.begin(), current_streams.end() };
    }
    // if we have only one current shard, then every parent maps to it
    if (current_streams.size() == 1) {
        return stream_id_range{ current_streams, current_streams.begin(), current_streams.end() };
    }

    // find parent shard in parent_streams, it must be present and have exact match
    auto parent_shard_end_it = std::lower_bound(parent_streams.begin(), parent_streams.end(), parent.token(), [](const cdc::stream_id& id, const dht::token& t) {
        return id.token() < t;
    });
    if (parent_shard_end_it == parent_streams.end() || parent_shard_end_it->token() != parent.token()) {
        throw api_error::validation(fmt::format("1 Invalid ShardFilter.ShardId value - shard {} not found", parent));
    }
    
    std::sort(current_streams.begin(), current_streams.end(), token_cmp);

    utils::chunked_vector<cdc::stream_id>::iterator lo;
    auto end = std::lower_bound(current_streams.begin(), current_streams.end(), parent_shard_end_it->token(), [](const cdc::stream_id& id, const dht::token& t) {
            return id.token() < t;
    });

    if (uses_tablets) {
        // tablets version - tablets don't wrap around and last token is always present
        // this means no lower_bound will return end() value - if it does, last token is not present
        if (end == current_streams.end()) {
            on_internal_error(elogger, fmt::format("last token from token space not present in tokens, for parent shard id {}, got parent shards [{}] and children shards [{}]", 
                parent, fmt::join(parent_streams, "; "), fmt::join(current_streams, "; ")));
        }

        // let's assume we've parent (first line) and child generation (second line):
        // NOTE: token space doesn't wrap around - instead we have a guarantee that last token
        // will be present as one of the shards
        // P=|    1    2    3    4|
        // C=| a  b    c       d e|
        // we want to find children for each token from parent:
        // 1 -> a,b
        // 2 -> c
        // 3 -> d
        // 4 -> d, e
        // first we find token in P that is end of range (parent_shard_end_it)
        // - if parent_shard_end_it - 1 exists
        //   - we take it as parent_shard_begin_it
        //   - find lower or equal token in C to parent_shard_begin_it, set it to lo
        //      - if lo->token() == parent_shard_begin_it->token() then increment lo
        //      lo now points at first child with token > parent_shard_begin_it->token()
        // - else previous one to parent_shard_end_it does not exist
        //   - set lo = C.begin()
        // - find lower or equal token in C to parent_shard_end_it, set it to end, increment once
        //      end now points at first child with token > parent_shard_end_it
        // - range [lo, end) represents children

        // end of parent token is also first token in parent streams - it means beginning of the parent's range
        // is the beginning of the token space - this means first child stream will be start of the children range
        if (parent_shard_end_it == parent_streams.begin()) {
            lo = current_streams.begin();
        }
        else {
            // normal case - we have previous parent shard that determines beginning of the range (exclusive)
            auto parent_shard_begin_it = std::prev(parent_shard_end_it);
            lo = std::lower_bound(current_streams.begin(), current_streams.end(), parent_shard_begin_it->token(), [](const cdc::stream_id& id, const dht::token& t) {
                return id.token() < t;
            });
            // sanity check - lo must not be end() - last token must be present
            if (lo == current_streams.end()) {
                on_internal_error(elogger, fmt::format("last token from token space not present in tokens, for shard id {}, parent shard id {}, got parent shards [{}] and children shards [{}]", 
                    parent, *parent_shard_begin_it, fmt::join(parent_streams, "; "), fmt::join(current_streams, "; ")));
            }
            // we have found beginning of the children stream, but since it's exclusive, we need to increment it if it's exact match
            if (lo->token() == parent_shard_begin_it->token()) {
                ++lo;
            }
        }
        // last match is inclusive - so we need to increment end
        ++end;

        // simple range
        return stream_id_range{ current_streams, lo, end };
    }
    else {
        // vnodes version - vnodes wrap around
        // wrapping around make whole algorithm extremely confusing, because we wrap around on two levels,
        // both parent shard might wrap around and children range might wrap around as well

        // helper function to find a range in current_streams based on range from parent_streams, but without wrap around
        // if lo is not set, it means beginning of the range
        // if end is not set, it means end of the range
        auto find_range_in_children = [&](std::optional<utils::chunked_vector<cdc::stream_id>::const_iterator> lo, std::optional<utils::chunked_vector<cdc::stream_id>::const_iterator> end) -> std::pair<utils::chunked_vector<cdc::stream_id>::iterator, utils::chunked_vector<cdc::stream_id>::iterator> {
            utils::chunked_vector<cdc::stream_id>::iterator res_lo, res_end;
            if (!lo) {
                // beginning of the range
                res_lo = current_streams.begin();
            }
            else {
                auto it = std::lower_bound(current_streams.begin(), current_streams.end(), (*lo)->token(), [](const cdc::stream_id& id, const dht::token& t) {
                    return id.token() < t;
                });
                // beginning of the range is exclusive, so if we have exact match, increment once
                if (it != current_streams.end() && it->token() == (*lo)->token()) {
                    ++it;
                }
                res_lo = it;
            }
            if (!end) {
                // end of the range
                res_end = current_streams.end();
            }
            else {
                auto it = std::lower_bound(current_streams.begin(), current_streams.end(), (*end)->token(), [](const cdc::stream_id& id, const dht::token& t) {
                    return id.token() < t;
                });
                // end of the range is inclusive, so increment once
                if (it != current_streams.end()) {
                    ++it;
                }
                res_end = it;
            }
            return { res_lo, res_end };
        };
        auto parent_shard_begin_it = parent_shard_end_it;
        if (parent_shard_begin_it == parent_streams.begin()) {
            // end of the parent shard is also first token in parent streams - it means wrap around case for parent
            // beginning of the parent's range is the last token in the parent streams
            // for example:
            // P=|         0 10    |
            // C=| -20 -10         |
            // searching for parent shard at 0 will get us here - end of the parent is the first parent shard
            // so beginning of the parent's range is the last parent shard (10)
            parent_shard_begin_it = std::prev(parent_streams.end());

            // we find two unwrapped ranges here - from beginning of current_streams to the end of the parent's range
            // (end is inclusive) - in our example it's (-inf, 0]
            auto [ lo1, end1 ] = find_range_in_children(std::nullopt, parent_shard_end_it);
            // and from the beginning of the parent's range (exclusive) to the end of current_streams
            // our example is (10, +inf)
            auto [ lo2, end2 ] = find_range_in_children(parent_shard_begin_it, std::nullopt);

            // in rare cases those two ranges might overlap - so we check and merge if needed
            // for example:
            // P=|     -30 -20      |
            // C=| -40          -10 |
            // searching for parent shard at -30 will get us here - end of the parent is -30, beginning is -20
            // first search will give us (-inf, -10] with end1 pointing to current_streams.end()
            // (because the range needs to include -10 position, so the iterator will point to the next one after - end of the current_streams)
            // second search will give us [-10, +inf) with lo2 pointing to current_streams[1]
            // which is less then end1 - so we need to merge those two ranges
            if (lo2 < end1) {
                assert(lo1 <= lo2);
                assert(end1 <= end2);
                end1 = end2;
                lo2 = end2 = current_streams.end();
            }
            return stream_id_range{ current_streams, lo1, end1, lo2, end2 };
        }
        else {
            // simpler case - parent doesn't wrap around and we have both begin and end in normal order
            // we search for single unwrapped range and adjust later if needed
            --parent_shard_begin_it;
            auto [ lo1, end1 ] = find_range_in_children(parent_shard_begin_it, parent_shard_end_it);
            auto lo2 = current_streams.end();
            auto end2 = current_streams.end();

            // it's possible for simple case to still wrap around, when parent range lies after all children shards
            // for example:
            // P=|         0 10    |
            // C=| -20 -10         |
            // when searching for parent shart at 0, we get parent range [0, 10)
            // unwrapped search will produce empty range and miss -20 child shard, which is actually
            // owner of [0, 10) range (and is also a first shard in current generation)
            // note, that searching for 0 parent will give correct result, but because algorithm in that case
            // detects wrap around case and chooses different if
            if (parent_shard_end_it->token() > current_streams.back().token() && lo1 != current_streams.begin()) {
                // wrap around case - we need to add first current element as well
                end2 = lo2 = current_streams.begin();
                ++end2;
                std::swap(lo1, lo2);
                std::swap(end1, end2);
            }
            return stream_id_range{ current_streams, lo1, end1, lo2, end2 };
        }
    }    
}

future<executor::request_return_type> executor::describe_stream(client_state& client_state, service_permit permit, rjson::value request) {
    _stats.api_operations.describe_stream++;

    auto limit = rjson::get_opt<int>(request, "Limit").value_or(100); // according to spec
    auto ret = rjson::empty_object();
    auto stream_desc = rjson::empty_object();

    auto [ schema, bs, stream_arn ] = get_stream_schema_and_base_schema_from_request(_proxy, request);

    auto db = _proxy.data_dictionary();

    if (limit < 1) {
        throw api_error::validation("Limit must be 1 or more");
    }

    std::optional<shard_id> shard_start;
    try {
        shard_start = rjson::get_opt<shard_id>(request, "ExclusiveStartShardId");
        elogger.info("QWERTY got start shard");
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

    rjson::add(stream_desc, "StreamStatus", rjson::from_string(status));

    stream_view_type type = cdc_options_to_steam_view_type(opts);

    rjson::add(stream_desc, "StreamArn", alternator::stream_arn(schema->id()));
    rjson::add(stream_desc, "StreamViewType", type);
    rjson::add(stream_desc, "TableName", rjson::from_string(table_name(*bs)));

    describe_key_schema(stream_desc, *bs);

    if (!opts.enabled()) {
        rjson::add(ret, "StreamDescription", std::move(stream_desc));
        co_return rjson::print(std::move(ret));
    }

    // TODO: label
    // TODO: creation time

    std::map<db_clock::time_point, cdc::streams_version> topologies;

    if (schema->table().uses_tablets()) {
        // filter out cdc generations older than the table or now() - cdc::ttl (typically dynamodb_streams_max_window - 24h)
        auto low_ts = db_clock::now() - ttl;
        topologies = co_await _system_keyspace.read_cdc_for_tablets_versioned_streams(bs->ks_name(), bs->cf_name(), low_ts);
    }
    else {
        auto normal_token_owners = _proxy.get_token_metadata_ptr()->count_normal_token_owners();
    // filter out cdc generations older than the table or now() - cdc::ttl (typically dynamodb_streams_max_window - 24h)
    auto low_ts = std::max(as_timepoint(schema->id()), db_clock::now() - ttl);
        topologies = co_await _sdks.cdc_get_versioned_streams(low_ts, { normal_token_owners });
    }

    std::optional<shard_id> shard_filter;

    if (const rjson::value *shard_filter_obj = rjson::find(request, "ShardFilter")) {
        if (!shard_filter_obj->IsObject()) {
            throw api_error::validation("Invalid ShardFilter value - must be object");
        }
        std::string type;
        try {
            type = rjson::get<std::string>(*shard_filter_obj, "Type");
        } catch (...) {
            throw api_error::validation("Invalid ShardFilter.Type value - must be string `CHILD_SHARDS`");
        }
        if (type != "CHILD_SHARDS") {
            throw api_error::validation("Invalid ShardFilter.Type value - must be string `CHILD_SHARDS`");
        }
        try {
            shard_filter = rjson::get<shard_id>(*shard_filter_obj, "ShardId");
        } catch (std::exception &e) {
            throw api_error::validation(std::format("1 Invalid ShardFilter.ShardId value - not a valid ShardId: {}", e.what()));
        }
    }

    const auto e = topologies.end();
    auto prev = e;
    auto shards = rjson::empty_array();

    std::optional<shard_id> last;

    auto i = topologies.begin();
    // if we're a paged query, skip to the generation where we left of.
    if (shard_start) {
        i = topologies.find(shard_start->time);
    }

    // need a prev even if we are skipping stuff
    if (i != topologies.begin()) {
        prev = std::prev(i);
    }

    for (; limit > 0 && i != e; prev = i, ++i) {
        auto& [ts, sv] = *i;

        if (shard_filter && (prev == e || prev->first != shard_filter->time)) {
            shard_start = std::nullopt;
            continue;
        }
        last = std::nullopt;

        // #7409 - shards must be returned in lexicographical order,
        std::sort(sv.streams.begin(), sv.streams.end(), id_cmp);
        if (prev != e) {
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

        std::optional<stream_id_range> shard_range;

        if (shard_filter) {
            // sanity check - we should never get here as there is if above (`shard_filter && prev == e` => `continue`)
            if (prev == e) {
                on_internal_error(elogger, fmt::format("Could not find parent generation for shard id {}, got generations [{}]", shard_filter->id, fmt::join(topologies | std::ranges::views::keys, "; ")));
            }
            
            const bool uses_tablets = schema->table().uses_tablets();
            shard_range = find_children_range_from_parent_token(
                prev->second.streams,
                i->second.streams,
                shard_filter->id,
                uses_tablets
            );
        }
        else {
            shard_range = stream_id_range{ i->second.streams, i->second.streams.begin(), i->second.streams.end() };
        }
        if (shard_start) {
            if (!shard_range->apply_starting_position_update(shard_start->id)) {
                limit = 0;
                break;
            }
            
        }
        for(const auto &id : shard_range->iterate()) {
            auto shard = rjson::empty_object();

            if (prev != e) {
                auto& pids = prev->second.streams;
                auto pid = std::lower_bound(pids.begin(), pids.end(), id.token(), [](const cdc::stream_id& id, const dht::token& t) {
                    return id.token() < t;
                });
                if (pid == pids.end()) {
                    if (!schema->table().uses_tablets()) {
                        // for vnodes token-ring wraps around, so if lower_bound returns end() it means we have to pick the first one
                        pid = pids.begin();
                    }
                    else {                    
                        on_internal_error(elogger, fmt::format("Could not find parent shard for shard id {}, got shards [{}]", id, fmt::join(pids, " ")));
                    }
                }
                rjson::add(shard, "ParentShardId", shard_id(prev->first, *pid));
            }

            last.emplace(ts, id);
            rjson::add(shard, "ShardId", *last);
            auto range = rjson::empty_object();
            rjson::add(range, "StartingSequenceNumber", sequence_number(utils::UUID_gen::min_time_UUID(ts.time_since_epoch())));
            if (expired) {
                rjson::add(range, "EndingSequenceNumber", sequence_number(utils::UUID_gen::min_time_UUID(expired->time_since_epoch())));
            }

            rjson::add(shard, "SequenceNumberRange", std::move(range));
            rjson::push_back(shards, std::move(shard));
            
            if (--limit == 0) {
                break;
            }

            last = std::nullopt;
        }
        shard_start = std::nullopt;
    }

    if (last) {
        rjson::add(stream_desc, "LastEvaluatedShardId", *last);
    }

    rjson::add(stream_desc, "Shards", std::move(shards));
    rjson::add(ret, "StreamDescription", std::move(stream_desc));
        
    co_return rjson::print(std::move(ret));
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

    auto db = _proxy.data_dictionary();

    auto [ schema, bs, stream_arn ] = get_stream_schema_and_base_schema_from_request(_proxy, request);
    std::optional<shard_id> sid;

    try {
        sid = rjson::get<shard_id>(request, "ShardId");
    } catch (...) {
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

    shard_iterator iter(alternator::stream_arn(schema->id()), *sid, threshold, inclusive_of_threshold);

    auto ret = rjson::empty_object();
    rjson::add(ret, "ShardIterator", iter);

    return make_ready_future<executor::request_return_type>(rjson::print(std::move(ret)));
}

struct event_id {
    cdc::stream_id stream;
    utils::UUID timestamp;

    static constexpr auto marker = 'E';

    event_id(cdc::stream_id s, utils::UUID ts)
        : stream(s)
        , timestamp(ts)
    {}
    
    friend std::ostream& operator<<(std::ostream& os, const event_id& id) {
        fmt::print(os, "{}{}:{}", marker, id.stream.to_bytes(), id.timestamp);
        return os;
    }
};
} // namespace alternator

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
    if (limit > 1000) {
        throw api_error::validation("Limit must be less than or equal to 1000");
    }

    auto db = _proxy.data_dictionary();
    schema_ptr schema, base;
    try {
        auto log_table = db.find_column_family(table_id(iter.table));
        schema = log_table.schema();
        base = cdc::get_base_table(db.real_database(), *schema);
    } catch (...) {        
    }

    if (!schema || !base || !is_alternator_keyspace(schema->ks_name())) {
        co_return api_error::resource_not_found(fmt::to_string(iter.table));
    }

    tracing::add_table_name(trace_state, schema->ks_name(), schema->cf_name());

    co_await verify_permission(_enforce_authorization, _warn_authorization, client_state, schema, auth::permission::SELECT, _stats);

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

    std::optional<attrs_to_get> key_names =
        base->primary_key_columns()
        | std::views::transform([&] (const column_definition& cdef) {
            return std::make_pair<std::string, attrs_to_get_node>(cdef.name_as_text(), {}); })
        | std::ranges::to<attrs_to_get>()
    ;
    // Include all base table columns as values (in case pre or post is enabled).
    // This will include attributes not stored in the frozen map column
    std::optional<attrs_to_get> attr_names = base->regular_columns()
        // this will include the :attrs column, which we will also force evaluating. 
        // But not having this set empty forces out any cdc columns from actual result 
        | std::views::transform([] (const column_definition& cdef) {
            return std::make_pair<std::string, attrs_to_get_node>(cdef.name_as_text(), {}); })
        | std::ranges::to<attrs_to_get>()
    ;

    std::vector<const column_definition*> columns;
    columns.reserve(schema->all_columns().size());

    auto pks = schema->partition_key_columns();
    auto cks = schema->clustering_key_columns();

    std::transform(pks.begin(), pks.end(), std::back_inserter(columns), [](auto& c) { return &c; });
    std::transform(cks.begin(), cks.end(), std::back_inserter(columns), [](auto& c) { return &c; });
    auto regular_column_start_idx = columns.size();
    auto regular_column_filter = std::views::filter([](const column_definition& cdef) { return cdef.name() == op_column_name || cdef.name() == eor_column_name || !cdc::is_cdc_metacolumn_name(cdef.name_as_text()); });
    std::ranges::transform(schema->regular_columns() | regular_column_filter, std::back_inserter(columns), [](auto& c) { return &c; });

    auto regular_columns = std::ranges::subrange(columns.begin() + regular_column_start_idx, columns.end())
        | std::views::transform(&column_definition::id)
        | std::ranges::to<query::column_id_vector>()
    ;

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
            query::tombstone_limit(_proxy.get_tombstone_limit()), query::row_limit(limit * mul));

    service::storage_proxy::coordinator_query_result qr = co_await _proxy.query(schema, std::move(command), std::move(partition_ranges), cl, service::storage_proxy::coordinator_query_options(default_timeout(), std::move(permit), client_state));
    cql3::selection::result_set_builder builder(*selection, gc_clock::now());
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
    const auto dc_name = _proxy.get_token_metadata_ptr()->get_topology().get_datacenter();

    using op_utype = std::underlying_type_t<cdc::operation>;

    auto maybe_add_record = [&] {
        if (!dynamodb.ObjectEmpty()) {
            rjson::add(record, "dynamodb", std::move(dynamodb));
            dynamodb = rjson::empty_object();
        }
        if (!record.ObjectEmpty()) {
            rjson::add(record, "awsRegion", rjson::from_string(dc_name));
            rjson::add(record, "eventID", event_id(iter.shard.id, *timestamp));
            rjson::add(record, "eventSource", "scylladb:alternator");
            rjson::add(record, "eventVersion", "1.1");
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
            rjson::add(dynamodb, "Keys", std::move(keys));
            rjson::add(dynamodb, "ApproximateCreationDateTime", utils::UUID_gen::unix_timestamp_in_sec(ts).count());
            rjson::add(dynamodb, "SequenceNumber", sequence_number(ts));
            rjson::add(dynamodb, "StreamViewType", type);
            // TODO: SizeBytes
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
            describe_single_item(*selection, row, attr_names, item, nullptr, true);
            describe_single_item(*selection, row, key_names, item);
            rjson::add(dynamodb, op == cdc::operation::pre_image ? "OldImage" : "NewImage", std::move(item));
            break;
        }
        case cdc::operation::update:
            rjson::add(record, "eventName", "MODIFY");
            break;
        case cdc::operation::insert:
            rjson::add(record, "eventName", "INSERT");
            break;
        case cdc::operation::service_row_delete:
        case cdc::operation::service_partition_delete:
        {
            auto user_identity = rjson::empty_object();
            rjson::add(user_identity, "Type", "Service");
            rjson::add(user_identity, "PrincipalId", "dynamodb.amazonaws.com");
            rjson::add(record, "userIdentity", std::move(user_identity));
            rjson::add(record, "eventName", "REMOVE");
            break;
        }
        default:
            rjson::add(record, "eventName", "REMOVE");
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
    rjson::add(ret, "Records", std::move(records));

    if (nrecords != 0) {
        // #9642. Set next iterators threshold to > last
        shard_iterator next_iter(iter.table, iter.shard, *timestamp, false);
        // Note that here we unconditionally return NextShardIterator,
        // without checking if maybe we reached the end-of-shard. If the
        // shard did end, then the next read will have nrecords == 0 and
        // will notice end end of shard and not return NextShardIterator.
        rjson::add(ret, "NextShardIterator", next_iter);
        _stats.api_operations.get_records_latency.mark(std::chrono::steady_clock::now() - start_time);
        co_return rjson::print(std::move(ret));
    }

    // ugh. figure out if we are and end-of-shard

    // read_cdc_for_tablets_current_generation_timestamp
    db_clock::time_point ts;
    if (schema->table().uses_tablets()) {
        ts = co_await _system_keyspace.read_cdc_for_tablets_current_generation_timestamp(base->ks_name(), base->cf_name());
    }
    else {
    auto normal_token_owners = _proxy.get_token_metadata_ptr()->count_normal_token_owners();
        ts = co_await _sdks.cdc_current_generation_timestamp({ normal_token_owners });
    }

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
        rjson::add(ret, "NextShardIterator", iter);
    }
    _stats.api_operations.get_records_latency.mark(std::chrono::steady_clock::now() - start_time);
    if (is_big(ret)) {
        co_return make_streamed(std::move(ret));
    }
    co_return rjson::print(std::move(ret));
}

bool executor::add_stream_options(const rjson::value& stream_specification, schema_builder& builder, service::storage_proxy& sp) {
    auto stream_enabled = rjson::find(stream_specification, "StreamEnabled");
    if (!stream_enabled || !stream_enabled->IsBool()) {
        throw api_error::validation("StreamSpecification needs boolean StreamEnabled");
    }

    if (stream_enabled->GetBool()) {
        if (!sp.features().alternator_streams) {
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
        return true;
    } else {
        cdc::options opts;
        opts.enabled(false);
        builder.with_cdc_options(opts);
        return false;
    }
}

void executor::supplement_table_stream_info(rjson::value& descr, const schema& schema, const service::storage_proxy& sp) {
    auto& opts = schema.cdc_options();
    if (opts.enabled()) {
        auto db = sp.data_dictionary();
        auto cf = db.find_table(schema.ks_name(), cdc::log_name(schema.cf_name()));
        stream_arn arn(cf.schema()->id());
        rjson::add(descr, "LatestStreamArn", arn);
        rjson::add(descr, "LatestStreamLabel", rjson::from_string(stream_label(*cf.schema())));

        auto stream_desc = rjson::empty_object();
        rjson::add(stream_desc, "StreamEnabled", true);

        auto mode = stream_view_type::KEYS_ONLY;
        if (opts.preimage() && opts.postimage()) {
            mode = stream_view_type::NEW_AND_OLD_IMAGES;
        } else if (opts.preimage()) {
            mode = stream_view_type::OLD_IMAGE;
        } else if (opts.postimage()) {
            mode = stream_view_type::NEW_IMAGE;
        }
        rjson::add(stream_desc, "StreamViewType", mode);
        rjson::add(descr, "StreamSpecification", std::move(stream_desc));
    }
}

// this will parse Amazon's ARN format for DynamoDB streams
// for example `arn:aws:dynamodb:us-east-1:797456418907:table/dynamodb_streams_verification_table_rc/stream/2025-12-18T17:38:48.952`
// we only care about table name - dynamodb_streams_verification_table_rc
static boost::regex arn_regex(R"(arn:aws:dynamodb:[^:]+:\d{12}:[^\/]+\/([^\/]+)\/[^\/]+\/.+)");

sstring get_table_name_from_dynamodb_stream_arn(std::string_view text_stream_arn)
{
    boost::cmatch re_match;
    if (!boost::regex_match(text_stream_arn.data(), text_stream_arn.data() + text_stream_arn.size(), re_match, arn_regex)) {
        throw api_error::validation(std::format("`{}` is not a valid StreamArn", text_stream_arn));
    }
    return sstring{ re_match[1].str() };
}

table_id get_cdc_log_table_id_from_stream_arn(service::storage_proxy& proxy, std::string_view text_stream_arn)
{
    // first try if it's our own format
    try {
        return table_id(stream_arn{ text_stream_arn });
    }
    catch(...) {
        // we except here either std::invalid_format or marshal_exception - both mean not our format
    }
    sstring table_name = get_table_name_from_dynamodb_stream_arn(text_stream_arn);
    auto ks_name = sstring{ executor::KEYSPACE_NAME_PREFIX } + table_name;
    // TODO: code later on expects XXX_cdc_log table (not the user's table,
    // but scylla's cdc log table for it), fix it - return user's table (less confusing),
    // and make cdc table where is needed.
    if (!cdc::is_log_name(table_name)) {
        table_name = cdc::log_name(table_name);
    }
    try {
        return proxy.data_dictionary().find_schema(ks_name, table_name)->id();
    } catch(data_dictionary::no_such_column_family&) {
        throw api_error::resource_not_found(std::format("Invalid StreamArn - table {} not found", table_name));
    }
}

std::pair<schema_ptr, schema_ptr> get_stream_schema_and_base_schema_from_arn(service::storage_proxy& proxy, std::string_view arn) {
    // local java dynamodb server does _not_ validate the arn syntax. But "real" one does. 
    // I.e. unparsable arn -> error. 
    auto stream_arn = get_cdc_log_table_id_from_stream_arn(proxy, arn);

    schema_ptr schema, bs;
    try {
        auto cf = proxy.data_dictionary().find_column_family(table_id(stream_arn));
        schema = cf.schema();
        bs = cdc::get_base_table(proxy.data_dictionary().real_database(), *schema);
    } catch (...) {
    }
 
    if (!schema || !bs || !is_alternator_keyspace(schema->ks_name())) {
        throw api_error::resource_not_found("Invalid StreamArn");
    }
    return { std::move(schema), std::move(bs) };
}

// parse the StreamArn from the request and return the stream schema, the base table schema and the StreamArn itself.
// If possible it's better to return to the user the same StreamArn they provided.
std::tuple<schema_ptr, schema_ptr, std::string> get_stream_schema_and_base_schema_from_request(service::storage_proxy& proxy, const rjson::value &request) {
    auto text_stream_arn = rjson::get<std::string>(request, "StreamArn");
    auto [ schema, bs ] = get_stream_schema_and_base_schema_from_arn(proxy, text_stream_arn);
    return { std::move(schema), std::move(bs), std::move(text_stream_arn) };
}

} // namespace alternator
