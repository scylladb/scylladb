/*
 * Copyright (C) 2026-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/core/thread.hh>
#include <seastar/util/file.hh>

#include "manifest.hh"
#include "db/snapshot_types.hh"
#include "dht/token.hh"
#include "sstables/sstables_manager.hh"
#include "locator/tablets.hh"
#include "utils/log.hh"
#include "utils/rjson.hh"

// Defined in db/snapshot-ctl.cc
extern logging::logger snap_log;

namespace db::snapshot {

manifest_json::info::info() {
    register_params();
}

manifest_json::info::info(const info& e) {
    register_params();
    version = e.version;
    scope = e.scope;
}

manifest_json::info& manifest_json::info::operator=(const info& e) {
    if (this != &e) {
        version = e.version;
        scope = e.scope;
    }
    return *this;
}

void manifest_json::info::register_params() {
    add(&version, "version");
    add(&scope, "scope");
}

manifest_json::node_info::node_info() {
    register_params();
}

manifest_json::node_info::node_info(const node_info& e) {
    register_params();
    host_id = e.host_id;
    datacenter = e.datacenter;
    rack = e.rack;
}

manifest_json::node_info::node_info(const db::snapshot_node_entry& e) {
    register_params();
    host_id = e.node.to_sstring();
    datacenter = e.datacenter;
    rack = e.rack;
}

manifest_json::node_info& manifest_json::node_info::operator=(const node_info& e) {
    if (this != &e) {
        host_id = e.host_id;
        datacenter = e.datacenter;
        rack = e.rack;
    }
    return *this;
}

void manifest_json::node_info::register_params() {
    add(&host_id, "host_id");
    add(&datacenter, "datacenter");
    add(&rack, "rack");
}


manifest_json::snapshot_info::snapshot_info() {
    register_params();
}

manifest_json::snapshot_info::snapshot_info(const snapshot_info& e) {
    register_params();
    name = e.name;
    created_at = e.created_at;
    expires_at = e.expires_at;
}

manifest_json::snapshot_info& manifest_json::snapshot_info::operator=(const snapshot_info& e) {
    if (this != &e) {
        name = e.name;
        created_at = e.created_at;
        expires_at = e.expires_at;
    }
    return *this;
}

void manifest_json::snapshot_info::register_params() {
    add(&name, "name");
    add(&created_at, "created_at");
    add(&expires_at, "expires_at");
}

manifest_json::table_info::table_info() {
    register_params();
}

manifest_json::table_info::table_info(const table_info& e) {
    register_params();
    keyspace_name = e.keyspace_name;
    table_name = e.table_name;
    table_id = e.table_id;
    tablets_type = e.tablets_type;
    tablet_count = e.tablet_count;
}

manifest_json::table_info& manifest_json::table_info::operator=(const table_info& e) {
    if (this != &e) {
        keyspace_name = e.keyspace_name;
        table_name = e.table_name;
        table_id = e.table_id;
        tablets_type = e.tablets_type;
        tablet_count = e.tablet_count;
    }
    return *this;
}

void manifest_json::table_info::register_params() {
    add(&keyspace_name, "keyspace_name");
    add(&table_name, "table_name");
    add(&table_id, "table_id");
    add(&tablets_type, "tablets_type");
    add(&tablet_count, "tablet_count");
}

manifest_json::sstable_info::sstable_info() {
    register_params();
}

manifest_json::sstable_info::sstable_info(const sstables::sstable_snapshot_metadata& e) {
    register_params();
    id = fmt::to_string(e.id);
    toc_name = e.toc_name;
    data_size = e.data_size;
    index_size = e.index_size;
    first_token = e.first_token;
    last_token = e.last_token;
    repaired_at = e.repaired_at;
    if (e.tablet_id) {
        tablet_id = *e.tablet_id;
    }
}

manifest_json::sstable_info::sstable_info(const sstable_info& e) {
    register_params();
    id = e.id;
    toc_name = e.toc_name;
    data_size = e.data_size;
    index_size = e.index_size;
    first_token = e.first_token;
    last_token = e.last_token;
    tablet_id = e.tablet_id;
    repaired_at = e.repaired_at;
    node = e.node;
    additional_nodes = e.additional_nodes;
}

manifest_json::sstable_info::sstable_info(sstable_info&& e) {
    register_params();
    id = e.id;
    toc_name = std::move(e.toc_name);
    data_size = e.data_size;
    index_size = e.index_size;
    first_token = e.first_token;
    last_token = e.last_token;
    tablet_id = e.tablet_id;
    repaired_at = e.repaired_at;
    node = std::move(e.node);
    additional_nodes = std::move(e.additional_nodes);
}

manifest_json::sstable_info::sstable_info(const db::snapshot_sstable_entry& e) {
    register_params();
    id = e.sstable_id.to_sstring();
    toc_name = e.toc_name;
    data_size = e.data_size;
    index_size = e.index_size;
    first_token = dht::token::to_int64(e.first_token);
    last_token = dht::token::to_int64(e.last_token);
    tablet_id = e.tablet_id;
    repaired_at = e.repaired_at;
    node = e.node.to_sstring();
}

manifest_json::sstable_info& manifest_json::sstable_info::operator=(sstable_info&& e) {
    id = e.id;
    toc_name = std::move(e.toc_name);
    data_size = e.data_size;
    index_size = e.index_size;
    first_token = e.first_token;
    last_token = e.last_token;
    tablet_id = e.tablet_id;
    repaired_at = e.repaired_at;
    node = std::move(e.node);
    additional_nodes = std::move(e.additional_nodes);
    return *this;
}

void manifest_json::sstable_info::register_params() {
    add(&id, "id");
    add(&toc_name, "toc_name");
    add(&data_size, "data_size");
    add(&index_size, "index_size");
    add(&first_token, "first_token");
    add(&last_token, "last_token");
    add(&tablet_id, "tablet_id");
    add(&repaired_at, "repaired_at");
    add(&node, "node");
    add(&additional_nodes, "additional_nodes");
}

manifest_json::tablet_info::tablet_info() {
    register_params();
}

manifest_json::tablet_info::tablet_info(const db::snapshot_tablet_entry& e) {
    register_params();
    id = e.tablet_id;
    first_token = dht::token::to_int64(e.first_token);
    last_token = dht::token::to_int64(e.last_token);
    repair_time = db_clock::to_time_t(e.repair_time);
    repaired_at = e.repaired_at;
}

manifest_json::tablet_info::tablet_info(const tablet_info& e) {
    register_params();
    id = e.id;
    first_token = e.first_token;
    last_token = e.last_token;
    repair_time = e.repair_time;
    repaired_at = e.repaired_at;
}

manifest_json::tablet_info& manifest_json::tablet_info::operator=(tablet_info&& e) {
    id = e.id;
    first_token = e.first_token;
    last_token = e.last_token;
    repair_time = e.repair_time;
    repaired_at = e.repaired_at;
    return *this;
}

void manifest_json::tablet_info::register_params() {
    add(&id, "id");
    add(&first_token, "first_token");
    add(&last_token, "last_token");
    add(&repair_time, "repair_time");
    add(&repaired_at, "repaired_at");
}

manifest_json::manifest_json() {
    register_params();
}

manifest_json::manifest_json(manifest_json&& e) {
    register_params();
    manifest = std::move(e.manifest);
    node = std::move(e.node);
    snapshot = std::move(e.snapshot);
    table = std::move(e.table);
    sstables = std::move(e.sstables);
    tablets = std::move(e.tablets);
    nodes = std::move(e.nodes);
}

manifest_json& manifest_json::operator=(manifest_json&& e) {
    if (this != &e) {
        manifest = std::move(e.manifest);
        node = std::move(e.node);
        snapshot = std::move(e.snapshot);
        table = std::move(e.table);
        sstables = std::move(e.sstables);
        tablets = std::move(e.tablets);
        nodes = std::move(e.nodes);
    }
    return *this;
}

void manifest_json::register_params() {
    add(&manifest, "manifest");
    add(&node, "node");
    add(&snapshot, "snapshot");
    add(&table, "table");
    add(&sstables, "sstables");
    add(&tablets, "tablets");
    add(&nodes, "nodes");
}

static std::optional<gc_clock::time_point> get_time(const rjson::value& snapshot, const char* name) {
    const auto* v = rjson::find(snapshot, name);
    if (!v || !v->IsInt64()) {
        return std::nullopt;
    }
    return gc_clock::time_point(gc_clock::duration(v->GetInt64()));
}

seastar::future<std::optional<snapshot_times>> read_snapshot_times(std::filesystem::path snapshot_dir) {
    auto manifest_path = snapshot_dir / manifest_filename;
    rjson::chunked_content content;
    try {
        content = co_await seastar::util::read_entire_file(manifest_path);
    } catch (const std::system_error& e) {
        // The manifest is optional: snapshots taken by older versions,
        // or by an external tool, may not have one at all.
        if (e.code().value() != ENOENT) {
            snap_log.warn("Could not read {}: {}", manifest_path.native(), e);
        }
        co_return std::nullopt;
    } catch (...) {
        snap_log.warn("Could not read {}: {}", manifest_path.native(), std::current_exception());
        co_return std::nullopt;
    }

    // Parsing the manifest may take a while since it lists all the sstables
    // in the snapshot, so do it in a thread, where it can yield.
    co_return co_await seastar::async([manifest_path = std::move(manifest_path), content = std::move(content)] () mutable -> std::optional<snapshot_times> {
        rjson::value manifest;
        try {
            manifest = rjson::parse_yieldable(std::move(content));
        } catch (...) {
            snap_log.warn("Could not parse {}: {}", manifest_path.native(), std::current_exception());
            return std::nullopt;
        }
        if (!manifest.IsObject()) {
            snap_log.warn("Unexpected content in {}: not a JSON object", manifest_path.native());
            return std::nullopt;
        }
        const auto* snapshot = rjson::find(manifest, "snapshot");
        if (!snapshot || !snapshot->IsObject()) {
            // Manifests written before the snapshot section was introduced.
            return std::nullopt;
        }
        auto created_at = get_time(*snapshot, "created_at");
        if (!created_at) {
            return std::nullopt;
        }
        return snapshot_times{
            .created_at = *created_at,
            .expires_at = get_time(*snapshot, "expires_at"),
        };
    });
}

}
