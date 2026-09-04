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
#include "sstables/sstable_version.hh"
#include "utils/lister.hh"
#include "utils/UUID.hh"

extern logging::logger apilog;

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

// Component object names in the live object-storage layout carry no version or
// format, so the sstable descriptor is kept in the TOC object's attributes.
// An sstable that is still being created, or already being removed, may have no
// readable TOC: report what we do know rather than failing the whole listing.
static future<> set_object_storage_sstable_descriptor(sstables::object_storage_client& client, const sstring& bucket, sstables::sstable_id sid, ss::object_storage_sstable& sst) {
    auto toc = sstables::object_name(bucket, object_storage_prefix, sid, sstables::sstable_version_constants::TOC_SUFFIX);
    sstables::object_storage_attributes attributes;
    try {
        attributes = co_await client.get_object_metadata(toc);
    } catch (...) {
        apilog.warn("object_storage_sstables: could not read the attributes of {}: {}", toc.str(), std::current_exception());
        co_return;
    }
    if (auto it = attributes.find(sstring(sstables::object_storage_sstable_version_attribute)); it != attributes.end()) {
        sst.version = it->second;
    }
    if (auto it = attributes.find(sstring(sstables::object_storage_sstable_format_attribute)); it != attributes.end()) {
        sst.format = it->second;
    }
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
    // FIXME: this walks the sstables one at a time, and each one costs two
    // remote round trips - listing its references and reading its TOC
    // attributes - so reporting a bucket that holds many sstables is slow.
    //
    // Making this loop concurrent on this shard is not the fix: the endpoint
    // client splits object_storage_connections_per_shard across the scheduling
    // groups that use it, in proportion to their shares, and this handler has
    // no scheduling group of its own, so it would compete for the default
    // group's connections with normal object-storage work.  The API should run
    // in the maintenance scheduling group instead, and the traversal should be
    // spread across shards rather than made concurrent within one.
    for (auto& [sid_string, sst] : sstables) {
        auto sid = sstables::sstable_id(utils::UUID(sid_string));
        auto refs = co_await sstables::list_object_storage_references(client, bucket, object_storage_prefix, sid);
        std::ranges::sort(refs);
        sst.num_references = refs.size();
        sst.references = std::move(refs);
        co_await set_object_storage_sstable_descriptor(client, bucket, sid, sst);
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
