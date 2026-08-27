/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/core/coroutine.hh>
#include <seastar/util/closeable.hh>
#include <fmt/ranges.h>

#include "storage_manager.hh"
#include "api/api.hh"
#include "api/api-doc/storage_service.json.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/object_storage_client.hh"
#include "sstables/storage.hh"
#include "utils/lister.hh"
#include "utils/UUID.hh"

namespace api {

namespace ss = httpd::storage_service_json;
using namespace json;
using namespace seastar::httpd;

static const sstring object_storage_prefix = "sstables";

static future<> collect_object_storage_entries(abstract_lister& lister, std::vector<sstring>& entries) {
    while (auto entry = co_await lister.get()) {
        entries.push_back(entry->name);
    }
}

static future<std::vector<sstring>> list_object_storage_entries(sstables::object_storage_client& client, sstring bucket, sstring prefix) {
    std::vector<sstring> entries;
    auto lister = client.make_object_lister(std::move(bucket), std::move(prefix), [] (const std::filesystem::path&, const directory_entry&) { return true; });
    co_await with_closeable(std::move(lister), [&entries] (abstract_lister& lister) {
        return collect_object_storage_entries(lister, entries);
    });
    co_return entries;
}

static std::optional<sstring> first_path_component(std::string_view path) {
    if (path.empty()) {
        return std::nullopt;
    }
    auto pos = path.find('/');
    return sstring(path.substr(0, pos));
}

static
future<json::json_return_type>
rest_object_storage_sstables(sharded<sstables::storage_manager>& sstm, std::unique_ptr<http::request> req) {
    auto bucket = req->get_query_param("bucket");
    auto endpoint = req->get_query_param("endpoint");
    auto& client = *sstm.local().get_endpoint_client(std::move(endpoint));
    auto table_entries = co_await list_object_storage_entries(client, bucket, fmt::format("{}/", object_storage_prefix));
    std::map<sstring, ss::object_storage_sstable> sstables;
    for (const auto& entry : table_entries) {
        auto sid = first_path_component(entry);
        if (!sid) {
            continue;
        }
        auto& sst = sstables[*sid];
        sst.sstable_id = *sid;
    }
    for (auto& [sid_string, sst] : sstables) {
        auto sid = sstables::sstable_id(utils::UUID(sid_string));
        auto refs = co_await sstables::list_object_storage_references(client, bucket, object_storage_prefix, sid);
        std::ranges::sort(refs);
        sst.num_references = refs.size();
        sst.references = std::move(refs);
    }
    std::vector<ss::object_storage_sstable> result;
    result.reserve(sstables.size());
    for (auto& sst : sstables | std::views::values) {
        result.emplace_back(std::move(sst));
    }
    co_return result;
}

void set_storage_manager(http_context& ctx, routes& r, sharded<sstables::storage_manager>& sstm) {
    ss::object_storage_sstables.set(r, [&sstm] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        co_return co_await rest_object_storage_sstables(sstm, std::move(req));
    });
}

void unset_storage_manager(http_context& ctx, routes& r) {
    ss::object_storage_sstables.unset(r);
}

}
