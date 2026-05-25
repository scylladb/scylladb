/*
 * Copyright (C) 2026-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <string>
#include <seastar/json/json_elements.hh>

namespace sstables {
    struct sstable_snapshot_metadata;
}

namespace locator {
    enum class tablet_layout;
}

namespace db {
    struct snapshot_tablet_entry;
}

namespace db::snapshot {

namespace json = seastar::json;
using sstring = seastar::sstring;

struct manifest_json : public json::json_base {
    struct info : public json::json_base {
        json::json_element<sstring> version;
        json::json_element<sstring> scope;

        info();
        info(const info& e);
        info& operator=(const info& e);
    private:
        void register_params();
    };

    struct node_info : public json::json_base {
        json::json_element<sstring> host_id;
        json::json_element<sstring> datacenter;
        json::json_element<sstring> rack;

        node_info();
        node_info(const node_info& e);
        node_info& operator=(const node_info& e);
    private:
        void register_params();
    };

    struct snapshot_info : public json::json_base {
        json::json_element<sstring> name;
        json::json_element<time_t> created_at;
        json::json_element<time_t> expires_at;

        snapshot_info();
        snapshot_info(const snapshot_info& e);
        snapshot_info& operator=(const snapshot_info& e);
    private:
        void register_params();
    };

    struct table_info : public json::json_base {
        json::json_element<sstring> keyspace_name;
        json::json_element<sstring> table_name;
        json::json_element<sstring> table_id;
        json::json_element<sstring> tablets_type;
        json::json_element<size_t> tablet_count;

        table_info();
        table_info(const table_info& e);
        table_info& operator=(const table_info& e);
    private:
        void register_params();
    };

    struct sstable_info : public json::json_base {
        json::json_element<sstring> id;
        json::json_element<sstring> toc_name;
        json::json_element<uint64_t> data_size;
        json::json_element<uint64_t> index_size;
        json::json_element<int64_t> first_token;
        json::json_element<int64_t> last_token;
        json::json_element<uint64_t> tablet_id;
        json::json_element<int64_t> repaired_at;

        sstable_info();
        sstable_info(const sstables::sstable_snapshot_metadata& e);
        sstable_info(const sstable_info& e);
        sstable_info(sstable_info&& e);
        sstable_info& operator=(sstable_info&& e);
    private:
        void register_params();
    };

    struct tablet_info : public json::json_base {
        json::json_element<uint64_t> id;
        json::json_element<int64_t> first_token;
        json::json_element<int64_t> last_token;
        json::json_element<time_t> repair_time;
        json::json_element<int64_t> repaired_at;

        tablet_info();
        tablet_info(const db::snapshot_tablet_entry& e);
        tablet_info(const tablet_info& e);
        tablet_info& operator=(tablet_info&& e);
    private:
        void register_params();
    };

    json::json_element<info> manifest;
    json::json_element<node_info> node;
    json::json_element<snapshot_info> snapshot;
    json::json_element<table_info> table;
    json::json_chunked_list<sstable_info> sstables;
    json::json_chunked_list<tablet_info> tablets;
    json::json_chunked_list<node_info> nodes;

    manifest_json();
    manifest_json(manifest_json&& e);
    manifest_json& operator=(manifest_json&& e);
private:
    void register_params();
};

}
