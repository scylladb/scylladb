/*
 * Copyright 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <type_traits>
#include <ranges>
#include <generator>
#include <boost/lexical_cast.hpp>
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

static logging::logger slogger("alternator-streams");

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

// Debug printer for cdc::stream_id - used only for logging/debugging, not for
// serialization or user-visible output. We print both signed and unsigned value
// as we use both.
template <>
struct fmt::formatter<cdc::stream_id> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const cdc::stream_id &id, FormatContext& ctx) const {
        fmt::format_to(ctx.out(), "{} ", id.token());

        for (auto b : id.to_bytes()) {
            fmt::format_to(ctx.out(), "{:02x}", (unsigned char)b);
        }
        return ctx.out();
    }
};

namespace alternator {
// stream arn has certain format (see https://docs.aws.amazon.com/IAM/latest/UserGuide/reference-arns.html)
// we need to follow it as Kinesis Client Library does check
// NOTE: we're holding inside a name of cdc log table, not a user table
class stream_arn {
    std::string _arn;
    std::string_view _table_name;
    std::string_view _keyspace_name;
public:
    // ARN to get table name from
    stream_arn(std::string arn) : _arn(std::move(arn)) {
        auto parts = parse_arn(_arn, "StreamArn", "stream", "/stream/");
        _table_name = parts.table_name;
        _keyspace_name = parts.keyspace_name;
    }
    // NOTE: it must be a schema of a CDC log table, not a base table, because that's what we are encoding in ARN and returning to users.
    // we need base schema for creation time
    stream_arn(schema_ptr s, schema_ptr base_schema) {
        auto creation_time = get_table_creation_time(*base_schema);
        auto now = std::chrono::system_clock::time_point{ std::chrono::duration_cast<std::chrono::system_clock::duration>(std::chrono::duration<double>(creation_time)) };

        // KCL checks for arn / aws / dynamodb and account-id being a number
        _arn = fmt::format("arn:aws:dynamodb:us-east-1:000000000000:table/{}@{}/stream/{:%FT%T}", s->ks_name(), s->cf_name(), now);
        auto table_index = std::string_view{"arn:aws:dynamodb:us-east-1:000000000000:table/"}.size();
        auto x1 = _arn.find("@", table_index);
        auto x2 = _arn.find("/", x1);
        _table_name = std::string_view{ _arn }.substr(x1 + 1, x2 - (x1 + 1));
        _keyspace_name = std::string_view{ _arn }.substr(table_index, x1 - table_index);
    }

    std::string_view unparsed() const { return _arn; }
    std::string_view table_name() const { return _table_name; }
    std::string_view keyspace_name() const { return _keyspace_name; }
    friend std::ostream& operator<<(std::ostream& os, const stream_arn& arn) {
        os << arn._arn;
        return os;
    }
};

// NOTE: this will return schema for cdc log table, not the base table.
static schema_ptr get_schema_from_arn(service::storage_proxy& proxy, const stream_arn& arn)
{
    if (!cdc::is_log_name(arn.table_name())) {
        throw api_error::resource_not_found(fmt::format("{} as found in ARN {} is not a valid name for a CDC table", arn.table_name(), arn.unparsed()));
    }
    try {
        return proxy.data_dictionary().find_schema(arn.keyspace_name(), arn.table_name());
    } catch(data_dictionary::no_such_column_family&) {
        throw api_error::resource_not_found(fmt::format("`{}` is not a valid StreamArn - table {} not found", arn.unparsed(), arn.table_name()));
    }
}

// ShardId. Must be between 28 and 65 characters inclusive.
// UUID is 36 bytes as string (including dashes). 
// Prepend a version/type marker (`S`) -> 37
class stream_shard_id : public utils::UUID {
public:
    using UUID = utils::UUID;
    static constexpr char marker = 'S';

    stream_shard_id() = default;
    stream_shard_id(const UUID& uuid)
        : UUID(uuid)
    {}
    stream_shard_id(const table_id& tid)
        : UUID(tid.uuid())
    {}
    stream_shard_id(std::string_view v)
        : UUID(v.substr(1))
    {
        if (v[0] != marker) {
            throw std::invalid_argument(std::string(v));
        }
    }
    friend std::ostream& operator<<(std::ostream& os, const stream_shard_id& arn) {
        const UUID& uuid = arn;
        return os << marker << uuid;
    }
    friend std::istream& operator>>(std::istream& is, stream_shard_id& arn) {
        std::string s;
        is >> s;
        arn = stream_shard_id(s);
        return is;
    }
};

} // namespace alternator

template<typename ValueType>
struct rapidjson::internal::TypeHelper<ValueType, alternator::stream_shard_id>
    : public from_string_helper<ValueType, alternator::stream_shard_id>
{};
template<typename ValueType>
struct rapidjson::internal::TypeHelper<ValueType, alternator::stream_arn>
    : public from_string_helper<ValueType, alternator::stream_arn>
{};

namespace alternator {

future<alternator::executor::request_return_type> alternator::executor::list_streams(client_state& client_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.list_streams++;

    auto limit = rjson::get_opt<int>(request, "Limit").value_or(100);
    auto streams_start = rjson::get_opt<stream_shard_id>(request, "ExclusiveStartStreamArn");
    auto table = find_table(_proxy, request);
    auto db = _proxy.data_dictionary();

    if (limit < 1) {
        throw api_error::validation("Limit must be 1 or more");
    }

    // Audit the input table name (if specified), not the output table names.
    maybe_audit(audit_info, audit::statement_category::QUERY,
                table ? table->ks_name() : "", table ? table->cf_name() : "",
                "ListStreams", request);

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
    std::optional<stream_shard_id> last;

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
            auto arn = stream_arn{ i->schema(), cdc::get_base_table(db.real_database(), *i->schema()) };
            rjson::add(new_entry, "StreamArn", arn);
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

// find the parent Streams shard in previous generation for the given child Streams shard
// takes care of wrap-around case in vnodes
// prev_streams must be sorted by token
const cdc::stream_id& find_parent_shard_in_previous_generation(db_clock::time_point prev_timestamp, const utils::chunked_vector<cdc::stream_id> &prev_streams, const cdc::stream_id &child) {
    if (prev_streams.empty()) {
        // something is really wrong - streams are empty
        // let's try internal_error in hope it will be notified and fixed
        on_internal_error(slogger, fmt::format("streams are empty for cdc generation at {} ({})", prev_timestamp, prev_timestamp.time_since_epoch().count()));
    }
    auto it = std::lower_bound(prev_streams.begin(), prev_streams.end(), child.token(), [](const cdc::stream_id& id, const dht::token& t) {
        return id.token() < t;
    });
    if (it == prev_streams.end()) {
        // wrap around case - take first
        it = prev_streams.begin();
    }
    return *it;
}

// The function compare_lexicographically() below sorts stream shard ids in the
// way we need to present them in our output. However, when processing lists of
// shards internally, especially for finding child shards, it's more convenient
// for us to sort the shard ids by the different function defined here -
// compare_by_token(). It sorts the ids by numeric token (the end token of the
// token range belonging to this shard), and makes algorithms like lower_bound()
// possible.
static bool compare_by_token(const cdc::stream_id& id1, const cdc::stream_id& id2) {
    return id1.token() < id2.token();
}

// #7409 - shards must be returned in lexicographical order.
// Normal bytes compare is string_traits<int8_t>::compare,
// thus bytes 0x8000 is less than 0x0000. Instead, we need to use unsigned compare.
// KCL depends on this ordering, so we need to adhere.
static bool compare_lexicographically(const cdc::stream_id& id1, const cdc::stream_id& id2) {
    return compare_unsigned(id1.to_bytes(), id2.to_bytes()) < 0;
}

stream_id_range::stream_id_range(
        utils::chunked_vector<cdc::stream_id> &items,
        utils::chunked_vector<cdc::stream_id>::iterator lo1,
        utils::chunked_vector<cdc::stream_id>::iterator end1) : stream_id_range(items, lo1, end1, items.end(), items.end()) {}
stream_id_range::stream_id_range(
        utils::chunked_vector<cdc::stream_id> &items,
        utils::chunked_vector<cdc::stream_id>::iterator lo1,
        utils::chunked_vector<cdc::stream_id>::iterator end1,
        utils::chunked_vector<cdc::stream_id>::iterator lo2,
        utils::chunked_vector<cdc::stream_id>::iterator end2)
    : _lo1(lo1)
    , _end1(end1)
    , _lo2(lo2)
    , _end2(end2)
{
    if (_lo2 != items.end()) {
        if (_lo1 != items.begin()) {
            on_internal_error(slogger, fmt::format("Invalid stream_id_range: _lo1 != items.begin()"));
        }
        if (_end2 != items.end()) {
            on_internal_error(slogger, fmt::format("Invalid stream_id_range: _end2 != items.end()"));
        }
    }
    if (_end1 > _lo2)
        on_internal_error(slogger, fmt::format("Invalid stream_id_range: _end1 > _lo2"));
}

void stream_id_range::set_starting_position(const cdc::stream_id &update_to) {
    _skip_to = &update_to;
}

void stream_id_range::prepare_for_iterating()
{
    if (_prepared) return;
    _prepared = true;
    // here we deal with unfortunate possibility of wrap around range - in which case we actually have
    // two ranges (lo1, end1) and (lo2, end2), where lo1 will be begin() and end2 will be end().
    // the whole range needs to be sorted by `compare_lexicographically`, so we have to manually merge two ranges together and then sort them.
    // We also need to apply starting position update, if it was set, after merging and sorting.
    if (_end1 > _lo2)
        on_internal_error(slogger, fmt::format("Invalid stream_id_range: _end1 > _lo2"));

    auto tgt = _end1;
    auto src = _lo2;
    // just try to move second range just after first one - if we have only one range,
    // second range will be empty and nothing will happen here
    for(; src != _end2; ++src, ++tgt) {
        std::swap(*tgt, *src);
    }
    // sort merged ranges by compare_lexicographically
    std::sort(_lo1, tgt, compare_lexicographically);

    // apply starting position update if it was set
    // as a sanity check we require to find EXACT token match
    if (_skip_to) {
        auto it = std::lower_bound(_lo1, tgt, *_skip_to, compare_lexicographically);
        if (it == tgt || it->token() != _skip_to->token()) {
            slogger.info("Could not find starting position update shard id {}", *_skip_to);
        } else {
            _lo1 = std::next(it);
        }
    }
    _end1 = tgt;
}

// the function returns `stream_id_range` that will allow iteration over children Streams shards for the Streams shard `parent`
// a child Streams shard is defined as a Streams shard that touches token range that was previously covered by `parent` Streams shard
// Streams shard contains a token, that represents end of the token range for that Streams shard (inclusive)
// begginning of the token range is defined by previous Streams shard's token + 1
// NOTE: With vnodes, ranges of Streams' shards wrap, while with tablets the biggest allowed token number is always a range end.
// NOTE: both streams generation are guaranteed to cover whole range and be non-empty
// NOTE: it's possible to get more than one stream shard with the same token value (thus some of those stream shards will be empty) -
// for simplicity we will emit empty stream shards as well.
//
// to find children we will first find parent Streams shard in parent_streams by its token
// then we will find previous Streams shard in parent stream - that will determine range
// then based on the range we will find children Streams shards in current_streams
// NOTE: function sorts / reorders current_streams
// NOTE: function assumes parent_streams is sorted by compare_by_token and it doesn't modify it
stream_id_range find_children_range_from_parent_token(
    const utils::chunked_vector<cdc::stream_id>& parent_streams,
    utils::chunked_vector<cdc::stream_id>& current_streams,
    cdc::stream_id parent,
    bool uses_tablets
) {
    // sanity checks for required preconditions
    if (parent_streams.empty()) {
        on_internal_error(slogger, fmt::format("parent_streams is empty") );
    }
    if (current_streams.empty()) {
        on_internal_error(slogger, fmt::format("current_streams is empty") );
    }

    // first let's cover obvious cases
    // if we have only one parent Streams shard, then all children belong to it
    if (parent_streams.size() == 1) {
        return stream_id_range{ current_streams, current_streams.begin(), current_streams.end() };
    }
    // if we have only one current Streams shard, then every parent maps to it
    if (current_streams.size() == 1) {
        return stream_id_range{ current_streams, current_streams.begin(), current_streams.end() };
    }

    // find parent Streams shard in parent_streams, it must be present and have exact match
    auto parent_shard_end_it = std::lower_bound(parent_streams.begin(), parent_streams.end(), parent.token(), [](const cdc::stream_id& id, const dht::token& t) {
        return id.token() < t;
    });
    if (parent_shard_end_it == parent_streams.end() || parent_shard_end_it->token() != parent.token()) {
        throw api_error::validation(fmt::format("Invalid ShardFilter.ShardId value - shard {} not found", parent));
    }

    std::sort(current_streams.begin(), current_streams.end(), compare_by_token);

    utils::chunked_vector<cdc::stream_id>::iterator child_shard_begin_it;
    // upper_bound gives us the first element with token strictly greater than
    // parent's end token - this is the correct one-past-end for an inclusive
    // boundary and handles duplicate tokens (multiple children sharing a token)
    auto child_shard_end_it = std::upper_bound(current_streams.begin(), current_streams.end(), parent_shard_end_it->token(), [](const dht::token& t, const cdc::stream_id& id) {
            return t < id.token();
    });

    if (uses_tablets) {
        // tablets version - tablets don't wrap around and last token is always present
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
        // first we find token in P that is end of range of parent - parent_shard_end_it
        // - if parent_shard_end_it - 1 exists
        //   - we take it as parent_shard_begin_it
        //   - find the first child with token > parent_shard_begin_it and set it to child_shard_begin_it
        // - else previous one to parent_shard_end_it does not exist
        //   - set child_shard_begin_it = C.begin()
        // - find the first child with token > parent_shard_end_it and set it to child_shard_end_it
        // - range [child_shard_begin_it, child_shard_end_it) represents children

        // When the parent's end token is not directly present in the children
        // (merge scenario: several parent shards merged into fewer children),
        // the child whose range absorbs the parent's end is the first child
        // with token > parent_end_token.  upper_bound already points there,
        // so we advance past it to include it in the [begin, end) range.
        if (child_shard_end_it == current_streams.begin() || std::prev(child_shard_end_it)->token() != parent_shard_end_it->token()) {
            if (child_shard_end_it == current_streams.end()) {
                on_internal_error(slogger, fmt::format("parent end token not present in children tokens and no child with greater token exists, for parent shard id {}, got parent shards [{}] and children shards [{}]",
                    parent, fmt::join(parent_streams, "; "), fmt::join(current_streams, "; ")));
            }
            ++child_shard_end_it;
        }

        // end of parent token is also first token in parent streams - it means beginning of the parent's range
        // is the beginning of the token space - this means first child stream will be start of the children range
        if (parent_shard_end_it == parent_streams.begin()) {
            child_shard_begin_it = current_streams.begin();
        } else {
            // normal case - we have previous parent Streams shard that determines beginning of the range (exclusive)
            // upper_bound skips past all children at the previous parent's token (including duplicates)
            auto parent_shard_begin_it = std::prev(parent_shard_end_it);
            child_shard_begin_it = std::upper_bound(current_streams.begin(), current_streams.end(), parent_shard_begin_it->token(), [](const dht::token& t, const cdc::stream_id& id) {
                return t < id.token();
            });
        }

        // simple range
        return stream_id_range{ current_streams, child_shard_begin_it, child_shard_end_it };
    } else {
        // vnodes version - vnodes wrap around
        // wrapping around make whole algorithm extremely confusing, because we wrap around on two levels,
        // both parent Streams shard might wrap around and children range might wrap around as well

        // helper function to find a range in current_streams based on range from parent_streams, but without wrap around
        // if lo is not set, it means start from beginning of current_streams
        // if end is not set, it means go until end of current_streams
        auto find_range_in_children = [&](std::optional<utils::chunked_vector<cdc::stream_id>::const_iterator> lo, std::optional<utils::chunked_vector<cdc::stream_id>::const_iterator> end) -> std::pair<utils::chunked_vector<cdc::stream_id>::iterator, utils::chunked_vector<cdc::stream_id>::iterator> {
            utils::chunked_vector<cdc::stream_id>::iterator res_lo, res_end;
            if (!lo) {
                // beginning of the range
                res_lo = current_streams.begin();
            } else {
                // we use upper_bound as beginning of the range is exclusive
                res_lo = std::upper_bound(current_streams.begin(), current_streams.end(), (*lo)->token(), [](const dht::token& t, const cdc::stream_id& id) {
                    return t < id.token();
                });
            }
            if (!end) {
                // end of the range
                res_end = current_streams.end();
            } else {
                // end of the range is inclusive, so we use upper_bound to find the first element
                // with token strictly greater than the end token - this correctly handles the case
                // where multiple children share the same token (e.g. small vnodes where several
                // shards fall back to the vnode-end token)
                res_end = std::upper_bound(current_streams.begin(), current_streams.end(), (*end)->token(), [](const dht::token& t, const cdc::stream_id& id) {
                    return t < id.token();
                });
                // When the parent's end token is not directly present in the
                // children (merge scenario), the child whose range absorbs the
                // parent's end is at res_end.  Advance past it so that the
                // half-open range [res_lo, res_end) includes it.
                if (res_end != current_streams.end() &&
                        (res_end == current_streams.begin() || std::prev(res_end)->token() != (*end)->token())) {
                    ++res_end;
                }
            }
            return { res_lo, res_end };
        };
        auto parent_shard_begin_it = parent_shard_end_it;
        if (parent_shard_begin_it == parent_streams.begin()) {
            // end of the parent Streams shard is also first token in parent streams - it means wrap around case for parent
            // beginning of the parent's range is the last token in the parent streams
            // for example:
            // P=|         0 10    |
            // C=| -20 -10         |
            // searching for parent Streams shard at 0 will get us here - end of the parent is the first parent Streams shard
            // so beginning of the parent's range is the last parent Streams shard (10)
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
            // searching for parent Streams shard at -30 will get us here - end of the parent is -30, beginning is -20
            // first search will give us (-inf, +inf) with end1 pointing to current_streams.end()
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
        } else {
            // simpler case - parent doesn't wrap around and we have both begin and end in normal order
            // we search for single unwrapped range and adjust later if needed
            --parent_shard_begin_it;
            auto [ lo1, end1 ] = find_range_in_children(parent_shard_begin_it, parent_shard_end_it);
            auto lo2 = current_streams.end();
            auto end2 = current_streams.end();

            // it's possible for simple case to still wrap around, when parent range lies after all children Streams shards
            // for example:
            // P=|         0 10    |
            // C=| -20 -10         |
            // when searching for parent shart at 0, we get parent range [0, 10)
            // unwrapped search will produce empty range and miss -20 child Streams shard, which is actually
            // owner of [0, 10) range (and is also a first Streams shard in current generation)
            // note, that searching for 0 parent will give correct result, but because algorithm in that case
            // detects wrap around case and chooses different if
            if (parent_shard_end_it->token() > current_streams.back().token() && lo1 != current_streams.begin()) {
                // wrap around case - children at the beginning of the sorted array
                // wrap around the ring and cover the parent's range.  Include all
                // children sharing the first token (duplicate tokens are possible
                // for small vnodes where multiple shards fall back to the same token)
                end2 = lo2 = current_streams.begin();
                while(end2 != current_streams.end() && end2->token() == current_streams.front().token()) {
                    ++end2;
                }
                std::swap(lo1, lo2);
                std::swap(end1, end2);
            }
            return stream_id_range{ current_streams, lo1, end1, lo2, end2 };
        }
    }
}

future<executor::request_return_type> executor::describe_stream(client_state& client_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.describe_stream++;

    auto limit = rjson::get_opt<int>(request, "Limit").value_or(100); // according to spec
    auto ret = rjson::empty_object();
    auto stream_desc = rjson::empty_object();
    // local java dynamodb server does _not_ validate the arn syntax. But "real" one does. 
    // I.e. unparsable arn -> error. 
    auto stream_arn = rjson::get<alternator::stream_arn>(request, "StreamArn");

    schema_ptr bs;
    auto db = _proxy.data_dictionary();
    auto schema = get_schema_from_arn(_proxy, stream_arn);

    try {
        bs = cdc::get_base_table(db.real_database(), *schema);
    } catch (...) {        
    }
 
    if (!schema || !bs || !is_alternator_keyspace(schema->ks_name())) {
        throw api_error::resource_not_found("Invalid StreamArn");
    }
    auto normal_token_owners = _proxy.get_token_metadata_ptr()->count_normal_token_owners();

    // _sdks.cdc_get_versioned_streams() uses quorum_if_many() underneath, which uses CL=QUORUM for many token owners and CL=ONE otherwise.
    auto describe_cl = (normal_token_owners > 1) ? db::consistency_level::QUORUM : db::consistency_level::ONE;
    maybe_audit(audit_info, audit::statement_category::QUERY, schema->ks_name(),
                bs->cf_name() + "|" + schema->cf_name(), "DescribeStream", request, describe_cl);

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

    rjson::add(stream_desc, "StreamStatus", rjson::from_string(status));

    stream_view_type type = cdc_options_to_steam_view_type(opts);

    rjson::add(stream_desc, "StreamArn", stream_arn);
    rjson::add(stream_desc, "StreamViewType", type);
    rjson::add(stream_desc, "TableName", rjson::from_string(table_name(*bs)));

    describe_key_schema(stream_desc, *bs);

    if (!opts.enabled()) {
        rjson::add(ret, "StreamDescription", std::move(stream_desc));
        co_return rjson::print(std::move(ret));
    }

    // TODO: label
    // TODO: creation time

    // filter out cdc generations older than the table or now() - cdc::ttl (typically dynamodb_streams_max_window - 24h)
    auto low_ts = std::max(as_timepoint(schema->id()), db_clock::now() - ttl);

    std::map<db_clock::time_point, cdc::streams_version> topologies = co_await _sdks.cdc_get_versioned_streams(low_ts, { normal_token_owners });
    const auto e = topologies.end();
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
        } catch (const std::exception &e) {
            throw api_error::validation(fmt::format("Invalid ShardFilter.ShardId value - not a valid ShardId: {}", e.what()));
        }
        if (topologies.find(shard_filter->time) == topologies.end()) {
            throw api_error::validation(fmt::format("Invalid ShardFilter.ShardId value - corresponding generation not found: {}", shard_filter->id));
        }
    }

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
        std::sort(sv.streams.begin(), sv.streams.end(), compare_lexicographically);
        if (prev != e) {
            // We want older stuff sorted in token order so we can find matching
            // token range when determining parent Streams shard.
            std::stable_sort(prev->second.streams.begin(), prev->second.streams.end(), compare_by_token);
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
                on_internal_error(slogger, fmt::format("Could not find parent generation for shard id {}, got generations [{}]", shard_filter->id, fmt::join(topologies | std::ranges::views::keys, "; ")));
            }

            const bool uses_tablets = schema->table().uses_tablets();
            shard_range = find_children_range_from_parent_token(
                prev->second.streams,
                i->second.streams,
                shard_filter->id,
                uses_tablets
            );
        } else {
            shard_range = stream_id_range{ i->second.streams, i->second.streams.begin(), i->second.streams.end() };
        }
        if (shard_start) {
            shard_range->set_starting_position(shard_start->id);
        }
        shard_range->prepare_for_iterating();
        for(const auto &id : *shard_range) {
            auto shard = rjson::empty_object();

            if (prev != e) {
                auto &pid = find_parent_shard_in_previous_generation(prev->first, prev->second.streams, id);
                rjson::add(shard, "ParentShardId", shard_id(prev->first, pid));
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

future<executor::request_return_type> executor::get_shard_iterator(client_state& client_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
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
    auto db = _proxy.data_dictionary();

    std::optional<shard_id> sid;
    auto schema = get_schema_from_arn(_proxy, stream_arn);
    schema_ptr base_schema = nullptr;
    try {
        base_schema = cdc::get_base_table(db.real_database(), *schema);
        sid = rjson::get<shard_id>(request, "ShardId");
    } catch (...) {
    }
    if (!schema || !base_schema || !is_alternator_keyspace(schema->ks_name())) {
        throw api_error::resource_not_found("Invalid StreamArn");
    }

    // Uses only node-local context (the metadata) to generate response
    maybe_audit(audit_info, audit::statement_category::QUERY, schema->ks_name(),
                base_schema->cf_name() + "|" + schema->cf_name(), "GetShardIterator", request);

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

    shard_iterator iter(schema->id().uuid(), *sid, threshold, inclusive_of_threshold);

    auto ret = rjson::empty_object();
    rjson::add(ret, "ShardIterator", iter);
    return make_ready_future<executor::request_return_type>(rjson::print(std::move(ret)));
}

struct event_id {
    cdc::stream_id stream;
    utils::UUID timestamp;
    size_t index = 0;

    static constexpr auto marker = 'E';

    event_id(cdc::stream_id s, utils::UUID ts, size_t index)
        : stream(s)
        , timestamp(ts)
        , index(index)
    {}
    
    friend std::ostream& operator<<(std::ostream& os, const event_id& id) {
        fmt::print(os, "{}{}:{}:{}", marker, id.stream.to_bytes(), id.timestamp, id.index);
        return os;
    }
};
} // namespace alternator

template<typename ValueType>
struct rapidjson::internal::TypeHelper<ValueType, alternator::event_id>
    : public from_string_helper<ValueType, alternator::event_id>
{};

namespace alternator {
    namespace {
        struct managed_bytes_ptr_hash {
            size_t operator()(const managed_bytes *k) const noexcept {
                return std::hash<managed_bytes>{}(*k);
            }
        };
        struct managed_bytes_ptr_equal {
            bool operator()(const managed_bytes *a, const managed_bytes *b) const noexcept {
                return *a == *b;
            }
        };
    }

future<executor::request_return_type> executor::get_records(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
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
    db::consistency_level cl = db::consistency_level::LOCAL_QUORUM;

    maybe_audit(audit_info, audit::statement_category::QUERY, schema->ks_name(),
                base->cf_name() + "|" + schema->cf_name(), "GetRecords", request, cl);

    tracing::add_table_name(trace_state, schema->ks_name(), schema->cf_name());

    co_await verify_permission(_enforce_authorization, _warn_authorization, client_state, schema, auth::permission::SELECT, _stats);

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
    
    auto base_cks = base->clustering_key_columns();
    if (base_cks.size() > 1) {
        throw api_error::internal(fmt::format("invalid alternator table, clustering key count ({}) is bigger than one", base_cks.size()));
    }
    const bytes *clustering_key_column_name = !base_cks.empty() ? &base_cks.front().name() : nullptr;

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
    auto clustering_key_index = clustering_key_column_name ? std::distance(metadata.get_names().begin(), 
        std::find_if(metadata.get_names().begin(), metadata.get_names().end(), [&](const lw_shared_ptr<cql3::column_specification>& cdef) {
            return cdef->name->name() == *clustering_key_column_name;
        })
    ) : 0;

    std::optional<utils::UUID> timestamp;
    struct Record {
        rjson::value record;
        rjson::value dynamodb;
    };
    const managed_bytes empty_managed_bytes;
    std::unordered_map<const managed_bytes*, Record, managed_bytes_ptr_hash, managed_bytes_ptr_equal> records_map;
    const auto dc_name = _proxy.get_token_metadata_ptr()->get_topology().get_datacenter();

    using op_utype = std::underlying_type_t<cdc::operation>;

    for (auto& row : result_set->rows()) {
        auto op = static_cast<cdc::operation>(value_cast<op_utype>(data_type_for<op_utype>()->deserialize(*row[op_index])));
        auto ts = value_cast<utils::UUID>(data_type_for<utils::UUID>()->deserialize(*row[ts_index]));
        auto eor = row[eor_index].has_value() ? value_cast<bool>(boolean_type->deserialize(*row[eor_index])) : false;
        const managed_bytes* cs_ptr = clustering_key_column_name ? &*row[clustering_key_index] : &empty_managed_bytes;
        auto records_it = records_map.emplace(cs_ptr, Record{});
        auto &record = records_it.first->second;

        if (records_it.second) {
            record.dynamodb = rjson::empty_object();
            record.record = rjson::empty_object();
            auto keys = rjson::empty_object();
            describe_single_item(*selection, row, key_names, keys);
            rjson::add(record.dynamodb, "Keys", std::move(keys));
            rjson::add(record.dynamodb, "ApproximateCreationDateTime", utils::UUID_gen::unix_timestamp_in_sec(ts).count());
            rjson::add(record.dynamodb, "SequenceNumber", sequence_number(ts));
            rjson::add(record.dynamodb, "StreamViewType", type);
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
         * 
         * Note: BatchWriteItem will generate multiple records with
         * the same timestamp, when write isolation is set to always
         * (which triggers lwt), so we need to unpack them based on clustering key.
         */
        switch (op) {
        case cdc::operation::pre_image:
        case cdc::operation::post_image:
        {
            auto item = rjson::empty_object();
            describe_single_item(*selection, row, attr_names, item, nullptr, true);
            describe_single_item(*selection, row, key_names, item);
            rjson::add(record.dynamodb, op == cdc::operation::pre_image ? "OldImage" : "NewImage", std::move(item));
            break;
        }
        case cdc::operation::update:
            rjson::add(record.record, "eventName", "MODIFY");
            break;
        case cdc::operation::insert:
            rjson::add(record.record, "eventName", "INSERT");
            break;
        case cdc::operation::service_row_delete:
        case cdc::operation::service_partition_delete:
        {
            auto user_identity = rjson::empty_object();
            rjson::add(user_identity, "Type", "Service");
            rjson::add(user_identity, "PrincipalId", "dynamodb.amazonaws.com");
            rjson::add(record.record, "userIdentity", std::move(user_identity));
            rjson::add(record.record, "eventName", "REMOVE");
            break;
        }
        default:
            rjson::add(record.record, "eventName", "REMOVE");
            break;
        }
        if (eor) {
            size_t index = 0;
            for (auto& [_, rec] : records_map) {
                rjson::add(rec.record, "awsRegion", rjson::from_string(dc_name));
                rjson::add(rec.record, "eventID", event_id(iter.shard.id, *timestamp, index++));
                rjson::add(rec.record, "eventSource", "scylladb:alternator");
                rjson::add(rec.record, "eventVersion", "1.1");

                rjson::add(rec.record, "dynamodb", std::move(rec.dynamodb));
                rjson::push_back(records, std::move(rec.record));
            }

            records_map.clear();
            timestamp = ts;
            if (records.Size() >= limit) {
                // Note: we might have more than limit rows here - BatchWriteItem will emit multiple items
                // with the same timestamp and we have no way of resume iteration midway through those,
                // so we return all of them here.
                break;
            }
        }
    }

    auto ret = rjson::empty_object();
    rjson::add(ret, "Records", std::move(records));

    if (timestamp) {
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
    auto normal_token_owners = _proxy.get_token_metadata_ptr()->count_normal_token_owners();

    db_clock::time_point ts = co_await _sdks.cdc_current_generation_timestamp({ normal_token_owners });
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
        // cdc::delta_mode is ignored by Alternator, so aim for the least overhead.
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
        stream_arn arn(cf.schema(), cdc::get_base_table(db.real_database(), *cf.schema()));
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

} // namespace alternator
