/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "bytes.hh"
#include "keys/keys.hh"
#include "mutation/mutation.hh"
#include "mutation/timestamp.hh"
#include "schema/schema.hh"
#include "utils/hash.hh"
#include "utils/rjson.hh"

namespace service { class storage_proxy; }

// The item validation and mutation building of PutItem and DeleteItem, shared by the
// operations which write items: PutItem, DeleteItem, BatchWriteItem and ImportTable.
// The functions declared here are defined in executor.cc.

namespace alternator {

struct search_schema_attribute_info {
    std::string type;
    bool is_hash;
};

// Per-table information needed to validate the attribute values written by
// PutItem, UpdateItem or a BatchWriteItem PutRequest against GSI/LSI keys
// and vector-index constraints (see si_key_attributes(),
// vector_index_attributes() and search_schema_attributes() in executor.cc). It
// depends only on the table's schema, not on the item being written, so a
// caller writing multiple items to the same table (BatchWriteItem, ImportTable) should
// compute it once and reuse it, instead of recomputing it - and re-parsing
// the vector indexes' JSON-encoded options - for every item.
struct write_item_index_info {
    std::unordered_map<bytes, std::string> key_attributes;
    std::unordered_map<bytes, int> vec_attrs;
    std::unordered_map<bytes, search_schema_attribute_info> search_schema_attrs;

    // Defined in executor.cc, after si_key_attributes(), vector_index_attributes()
    // and search_schema_attributes().
    write_item_index_info(service::storage_proxy& proxy, const schema& s);
};

// The put_or_delete_item class builds the mutations needed by the PutItem and
// DeleteItem operations - either as stand-alone commands or part of a list
// of commands in BatchWriteItem or ImportTable.
// put_or_delete_item splits each operation into two stages: Constructing the
// object parses and validates the user input (throwing exceptions if there
// are input errors). Later, build() generates the actual mutation, with a
// specified timestamp. This split is needed because of the peculiar needs of
// BatchWriteItem and LWT. BatchWriteItem needs all parsing to happen before
// any writing happens (if one of the commands has an error, none of the
// writes should be done). LWT makes it impossible for the parse step to
// generate "mutation" objects, because the timestamp still isn't known.
class put_or_delete_item {
private:
    partition_key _pk;
    clustering_key _ck;
    struct cell {
        bytes column_name;
        bytes value;
    };
    // PutItem: engaged _cells, write these cells to item (_pk, _ck).
    // DeleteItem: disengaged _cells, delete the entire item (_pk, _ck).
    std::optional<std::vector<cell>> _cells;
    // WCU calculation takes into account some length in bytes,
    // that length can have different meaning depends on the operation but the
    // the calculation of length in bytes to WCU is the same.
    // ImportTable also bounds the size of its batches with it.
    uint64_t _length_in_bytes = 0;
public:
    struct delete_item {};
    struct put_item {};
    put_or_delete_item(const rjson::value& key, schema_ptr schema, delete_item);
    put_or_delete_item(const rjson::value& item, schema_ptr schema, put_item, const write_item_index_info& index_info);
    // put_or_delete_item doesn't keep a reference to schema (so it can be
    // moved between shards for LWT) so it needs to be given again to build():
    mutation build(schema_ptr schema, api::timestamp_type ts) const;
    const partition_key& pk() const { return _pk; }
    const clustering_key& ck() const { return _ck; }
    uint64_t length_in_bytes() const noexcept {
        return _length_in_bytes;
    }
    void set_length_in_bytes(uint64_t length) noexcept {
        _length_in_bytes = length;
    }
    bool is_put_item() noexcept {
        return _cells.has_value();
    }
};

// The primary key of an item, with the hash and equality of a set of keys of one table.
using primary_key = std::pair<partition_key, clustering_key>;
struct primary_key_hash {
    schema_ptr _s;
    size_t operator()(const primary_key& key) const {
        return utils::hash_combine(partition_key::hashing(*_s)(key.first), clustering_key::hashing(*_s)(key.second));
    }
};
struct primary_key_equal {
    schema_ptr _s;
    bool operator()(const primary_key& k1, const primary_key& k2) const {
        return partition_key::equality(*_s)(k1.first, k2.first) && clustering_key::equality(*_s)(k1.second, k2.second);
    }
};

} // namespace alternator
