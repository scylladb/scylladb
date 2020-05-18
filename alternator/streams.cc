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

#include "cdc/log.hh"
#include "cdc/generation.hh"
#include "db/system_distributed_keyspace.hh"
#include "utils/UUID_gen.hh"

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
        throw validation_exception("Limit must be 1 or more");
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


}
