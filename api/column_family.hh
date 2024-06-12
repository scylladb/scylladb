/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "replica/database.hh"
#include <seastar/json/json_elements.hh>
#include <any>
#include "api/api_init.hh"

namespace db {
class system_keyspace;
}

namespace api {

void set_column_family(http_context& ctx, httpd::routes& r, sharded<db::system_keyspace>& sys_ks);
void unset_column_family(http_context& ctx, httpd::routes& r);

table_id get_uuid(const sstring& name, const replica::database& db);
future<> foreach_column_family(http_context& ctx, const sstring& name, std::function<void(replica::column_family&)> f);


template<class Mapper, class I, class Reducer>
future<I> map_reduce_cf_raw(http_context& ctx, const sstring& name, I init,
        Mapper mapper, Reducer reducer) {
    auto uuid = get_uuid(name, ctx.db.local());
    using mapper_type = std::function<std::unique_ptr<std::any>(replica::database&)>;
    using reducer_type = std::function<std::unique_ptr<std::any>(std::unique_ptr<std::any>, std::unique_ptr<std::any>)>;
    return ctx.db.map_reduce0(mapper_type([mapper, uuid](replica::database& db) {
        return std::make_unique<std::any>(I(mapper(db.find_column_family(uuid))));
    }), std::make_unique<std::any>(std::move(init)), reducer_type([reducer = std::move(reducer)] (std::unique_ptr<std::any> a, std::unique_ptr<std::any> b) mutable {
        return std::make_unique<std::any>(I(reducer(std::any_cast<I>(std::move(*a)), std::any_cast<I>(std::move(*b)))));
    })).then([] (std::unique_ptr<std::any> r) {
        return std::any_cast<I>(std::move(*r));
    });
}


template<class Mapper, class I, class Reducer>
future<json::json_return_type> map_reduce_cf(http_context& ctx, const sstring& name, I init,
        Mapper mapper, Reducer reducer) {
    return map_reduce_cf_raw(ctx, name, init, mapper, reducer).then([](const I& res) {
        return make_ready_future<json::json_return_type>(res);
    });
}

template<class Mapper, class I, class Reducer, class Result>
future<json::json_return_type> map_reduce_cf(http_context& ctx, const sstring& name, I init,
        Mapper mapper, Reducer reducer, Result result) {
    return map_reduce_cf_raw(ctx, name, init, mapper, reducer).then([result](const I& res) mutable {
        result = res;
        return make_ready_future<json::json_return_type>(result);
    });
}

future<json::json_return_type> map_reduce_cf_time_histogram(http_context& ctx, const sstring& name, std::function<utils::time_estimated_histogram(const replica::column_family&)> f);

struct map_reduce_column_families_locally {
    std::any init;
    std::function<std::unique_ptr<std::any>(replica::column_family&)> mapper;
    std::function<std::unique_ptr<std::any>(std::unique_ptr<std::any>, std::unique_ptr<std::any>)> reducer;
    future<std::unique_ptr<std::any>> operator()(replica::database& db) const {
        auto res = seastar::make_lw_shared<std::unique_ptr<std::any>>(std::make_unique<std::any>(init));
        return db.get_tables_metadata().for_each_table_gently([res, this] (table_id, seastar::lw_shared_ptr<replica::table> table) {
            *res = reducer(std::move(*res), mapper(*table.get()));
            return make_ready_future();
        }).then([res] () {
            return std::move(*res);
        });
    }
};

template<class Mapper, class I, class Reducer>
future<I> map_reduce_cf_raw(http_context& ctx, I init,
        Mapper mapper, Reducer reducer) {
    using mapper_type = std::function<std::unique_ptr<std::any>(replica::column_family&)>;
    using reducer_type = std::function<std::unique_ptr<std::any>(std::unique_ptr<std::any>, std::unique_ptr<std::any>)>;
    auto wrapped_mapper = mapper_type([mapper = std::move(mapper)] (replica::column_family& cf) mutable {
        return std::make_unique<std::any>(I(mapper(cf)));
    });
    auto wrapped_reducer = reducer_type([reducer = std::move(reducer)] (std::unique_ptr<std::any> a, std::unique_ptr<std::any> b) mutable {
        return std::make_unique<std::any>(I(reducer(std::any_cast<I>(std::move(*a)), std::any_cast<I>(std::move(*b)))));
    });
    return ctx.db.map_reduce0(map_reduce_column_families_locally{init,
            std::move(wrapped_mapper), wrapped_reducer}, std::make_unique<std::any>(init), wrapped_reducer).then([] (std::unique_ptr<std::any> res) {
        return std::any_cast<I>(std::move(*res));
    });
}


template<class Mapper, class I, class Reducer>
future<json::json_return_type> map_reduce_cf(http_context& ctx, I init,
        Mapper mapper, Reducer reducer) {
    return map_reduce_cf_raw(ctx, init, mapper, reducer).then([](const I& res) {
        return make_ready_future<json::json_return_type>(res);
    });
}

future<json::json_return_type>  get_cf_stats(http_context& ctx, const sstring& name,
        int64_t replica::column_family_stats::*f);

future<json::json_return_type>  get_cf_stats(http_context& ctx,
        int64_t replica::column_family_stats::*f);


std::tuple<sstring, sstring> parse_fully_qualified_cf_name(sstring name);

}
