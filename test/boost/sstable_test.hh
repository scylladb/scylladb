
/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/assert.hh"
#include "types/map.hh"
#include "sstables/sstables.hh"
#include "replica/database.hh"
#include "schema/schema.hh"
#include "schema/schema_builder.hh"
#include "types/set.hh"
#include "types/list.hh"
#include <seastar/core/thread.hh>
#include "test/lib/test_services.hh"
#include "test/lib/sstable_test_env.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/tmpdir.hh"
#include <boost/test/unit_test.hpp>
#include <array>

constexpr auto la = sstables::sstable::version_types::la;
constexpr auto big = sstables::sstable::format_types::big;

class column_family_test {
    lw_shared_ptr<replica::column_family> _cf;
public:
    column_family_test(lw_shared_ptr<replica::column_family> cf) : _cf(cf) {}

    future<> add_sstable(sstables::shared_sstable sstable, sstables::offstrategy offstrategy = sstables::offstrategy::no) {
        if (offstrategy) {
            // Otherwise, on_compaction_completion always adds the new_sstabes to the main set
            return _cf->add_sstable_and_update_cache(sstable, offstrategy);
        }
        auto new_sstables = { sstable };
        return _cf->try_get_table_state_with_static_sharding().on_compaction_completion(sstables::compaction_completion_desc{ .new_sstables = new_sstables }, sstables::offstrategy::no);
    }

    future<> rebuild_sstable_list(compaction::table_state& table_s, const std::vector<sstables::shared_sstable>& new_sstables,
            const std::vector<sstables::shared_sstable>& sstables_to_remove, sstables::offstrategy offstrategy = sstables::offstrategy::no) {
        return table_s.on_compaction_completion(sstables::compaction_completion_desc{ .old_sstables = sstables_to_remove, .new_sstables = new_sstables }, offstrategy);
    }

    static void update_sstables_known_generation(replica::column_family& cf, sstables::generation_type generation) {
        cf.update_sstables_known_generation(generation);
    }

    static sstables::generation_type calculate_generation_for_new_table(replica::column_family& cf) {
        return cf.calculate_generation_for_new_table();
    }

    static const std::unique_ptr<replica::storage_group_manager>& get_storage_group_manager(replica::column_family& cf) {
        return cf._sg_manager;
    }
};

namespace sstables {

inline sstring get_test_dir(const sstring& name, const sstring& ks, const sstring& cf)
{
    return seastar::format("test/resource/sstables/{}/{}/{}-1c6ace40fad111e7b9cf000000000002", name, ks, cf);
}

inline sstring get_test_dir(const sstring& name, const schema_ptr s)
{
    return seastar::format("test/resource/sstables/{}/{}/{}-1c6ace40fad111e7b9cf000000000002", name, s->ks_name(), s->cf_name());
}

inline schema_ptr composite_schema() {
    static thread_local auto s = [] {
        schema_builder builder(make_shared_schema({}, "tests", "composite",
        // partition key
        {{"name", bytes_type}, {"col1", bytes_type}},
        // clustering key
        {},
        // regular columns
        {},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "Table with a composite key as pkey"
       ));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return s;
}

inline schema_ptr set_schema() {
    static thread_local auto s = [] {
        auto my_set_type = set_type_impl::get_instance(bytes_type, false);
        schema_builder builder(make_shared_schema({}, "tests", "set_pk",
        // partition key
        {{"ss", my_set_type}},
        // clustering key
        {},
        // regular columns
        {
            {"ns", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "Table with a set as pkeys"
       ));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return s;
}

inline schema_ptr map_schema() {
    static thread_local auto s = [] {
        auto my_map_type = map_type_impl::get_instance(bytes_type, bytes_type, false);
        schema_builder builder(make_shared_schema({}, "tests", "map_pk",
        // partition key
        {{"ss", my_map_type}},
        // clustering key
        {},
        // regular columns
        {
            {"ns", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "Table with a map as pkeys"
       ));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return s;
}

inline schema_ptr list_schema() {
    static thread_local auto s = [] {
        auto my_list_type = list_type_impl::get_instance(bytes_type, false);
        schema_builder builder(make_shared_schema({}, "tests", "list_pk",
        // partition key
        {{"ss", my_list_type}},
        // clustering key
        {},
        // regular columns
        {
            {"ns", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "Table with a list as pkeys"
       ));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return s;
}

inline schema_ptr uncompressed_schema(int32_t min_index_interval = 0) {
    auto uncompressed = [=] {
        schema_builder builder(make_shared_schema(generate_legacy_id("ks", "uncompressed"), "ks", "uncompressed",
        // partition key
        {{"name", utf8_type}},
        // clustering key
        {},
        // regular columns
        {{"col1", utf8_type}, {"col2", int32_type}},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "Uncompressed data"
       ));
       builder.set_compressor_params(compression_parameters());
       if (min_index_interval) {
           builder.set_min_index_interval(min_index_interval);
       }
       return builder.build(schema_builder::compact_storage::no);
    }();
    return uncompressed;
}

inline sstring uncompressed_dir() {
    return get_test_dir("uncompressed", uncompressed_schema());
}

inline schema_ptr complex_schema() {
    static thread_local auto s = [] {
        auto my_list_type = list_type_impl::get_instance(bytes_type, true);
        auto my_map_type = map_type_impl::get_instance(bytes_type, bytes_type, true);
        auto my_set_type = set_type_impl::get_instance(bytes_type, true);
        auto my_fset_type = set_type_impl::get_instance(bytes_type, false);
        auto my_set_static_type = set_type_impl::get_instance(bytes_type, true);

        schema_builder builder(make_shared_schema({}, "tests", "complex_schema",
        // partition key
        {{"key", bytes_type}},
        // clustering key
        {{"clust1", bytes_type}, {"clust2", bytes_type}},
        // regular columns
        {
            {"reg_set", my_set_type},
            {"reg_list", my_list_type},
            {"reg_map", my_map_type},
            {"reg_fset", my_fset_type},
            {"reg", bytes_type},
        },
        // static columns
        {{"static_obj", bytes_type}, {"static_collection", my_set_static_type}},
        // regular column name type
        bytes_type,
        // comment
        "Table with a complex schema, including collections and static keys"
       ));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return s;
}

inline schema_ptr columns_schema() {
    static thread_local auto columns = [] {
        schema_builder builder(make_shared_schema(generate_legacy_id("name", "columns"), "name", "columns",
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {{"columnfamily_name", utf8_type}, {"column_name", utf8_type}},
        // regular columns
        {
            {"component_index", int32_type},
            {"index_name", utf8_type},
            {"index_options", utf8_type},
            {"index_type", utf8_type},
            {"type", utf8_type},
            {"validator", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "column definitions"
       ));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return columns;
}

inline schema_ptr compact_simple_dense_schema() {
    static thread_local auto s = [] {
        schema_builder builder(make_shared_schema({}, "tests", "compact_simple_dense",
        // partition key
        {{"ks", bytes_type}},
        // clustering key
        {{"cl1", bytes_type}},
        // regular columns
        {{"cl2", bytes_type}},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "Table with a compact storage, and a single clustering key"
       ));
       return builder.build(schema_builder::compact_storage::yes);
    }();
    return s;
}

inline schema_ptr compact_dense_schema() {
    static thread_local auto s = [] {
        schema_builder builder(make_shared_schema({}, "tests", "compact_simple_dense",
        // partition key
        {{"ks", bytes_type}},
        // clustering key
        {{"cl1", bytes_type}, {"cl2", bytes_type}},
        // regular columns
        {{"cl3", bytes_type}},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "Table with a compact storage, and a compound clustering key"
       ));
       return builder.build(schema_builder::compact_storage::yes);
    }();
    return s;
}

inline schema_ptr compact_sparse_schema() {
    static thread_local auto s = [] {
        schema_builder builder(make_shared_schema({}, "tests", "compact_sparse",
        // partition key
        {{"ks", bytes_type}},
        // clustering key
        {},
        // regular columns
        {
            {"cl1", bytes_type},
            {"cl2", bytes_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "Table with a compact storage, but no clustering keys"
       ));
       return builder.build(schema_builder::compact_storage::yes);
    }();
    return s;
}

// This is "imported" from system_keyspace.cc. But we will copy it for two reasons:
// 1) This is private there, and for good reason.
// 2) If the schema for the peers table ever change (it does from ka to la), we want to make
//    sure we are testing the exact some one we have in our test dir.
inline schema_ptr peers_schema() {
    static thread_local auto peers = [] {
        schema_builder builder(make_shared_schema(generate_legacy_id("system", "peers"), "system", "peers",
        // partition key
        {{"peer", inet_addr_type}},
        // clustering key
        {},
        // regular columns
        {
                {"data_center", utf8_type},
                {"host_id", uuid_type},
                {"preferred_ip", inet_addr_type},
                {"rack", utf8_type},
                {"release_version", utf8_type},
                {"rpc_address", inet_addr_type},
                {"schema_version", uuid_type},
                {"tokens", set_type_impl::get_instance(utf8_type, true)},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "information about known peers in the cluster"
       ));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return peers;
}

enum class status {
    dead,
    live,
    ttl,
};

inline bool check_status_and_done(atomic_cell_view c, status expected) {
    if (expected == status::dead) {
        BOOST_REQUIRE(c.is_live() == false);
        return true;
    }
    BOOST_REQUIRE(c.is_live() == true);
    BOOST_REQUIRE(c.is_live_and_has_ttl() == (expected == status::ttl));
    return false;
}

template <status Status>
inline void match(const row& row, const schema& s, bytes col, const data_value& value, int64_t timestamp = 0, int32_t expiration = 0) {
    auto cdef = s.get_column_definition(col);

    BOOST_CHECK_NO_THROW(row.cell_at(cdef->id));
    auto c = row.cell_at(cdef->id).as_atomic_cell(*cdef);
    if (check_status_and_done(c, Status)) {
        return;
    }

    auto expected = cdef->type->decompose(value);
    auto val = c.value().linearize();
    SCYLLA_ASSERT(val == expected);
    BOOST_REQUIRE(c.value().linearize() == expected);
    if (timestamp) {
        BOOST_REQUIRE(c.timestamp() == timestamp);
    }
    if (expiration) {
        BOOST_REQUIRE(c.expiry() == gc_clock::time_point(gc_clock::duration(expiration)));
    }
}

inline void match_live_cell(const row& row, const schema& s, bytes col, const data_value& value) {
    match<status::live>(row, s, col, value);
}

inline void match_expiring_cell(const row& row, const schema& s, bytes col, const data_value& value, int64_t timestamp, int32_t expiration) {
    match<status::ttl>(row, s, col, value);
}

inline void match_dead_cell(const row& row, const schema& s, bytes col) {
    match<status::dead>(row, s, col, 0); // value will be ignored
}

inline void match_absent(const row& row, const schema& s, bytes col) {
    auto cdef = s.get_column_definition(col);
    BOOST_REQUIRE(row.find_cell(cdef->id) == nullptr);
}

inline collection_mutation_description
match_collection(const row& row, const schema& s, bytes col, const tombstone& t) {
    auto cdef = s.get_column_definition(col);

    BOOST_CHECK_NO_THROW(row.cell_at(cdef->id));
    auto c = row.cell_at(cdef->id).as_collection_mutation();
    return c.with_deserialized(*cdef->type, [&] (collection_mutation_view_description mut) {
        BOOST_REQUIRE(mut.tomb == t);
        return mut.materialize(*cdef->type);
    });
}

template <status Status>
inline void match_collection_element(const std::pair<bytes, atomic_cell>& element, const bytes_opt& col, const bytes_opt& expected_serialized_value) {
    if (col) {
        BOOST_REQUIRE(element.first == *col);
    }

    if (check_status_and_done(element.second, Status)) {
        return;
    }

    // For simplicity, we will have all set elements in our schema presented as
    // bytes - which serializes to itself.  Then we don't need to meddle with
    // the schema for the set type, and is enough for the purposes of this
    // test.
    if (expected_serialized_value) {
        BOOST_REQUIRE(element.second.value().linearize() == *expected_serialized_value);
    }
}

}

inline dht::decorated_key make_dkey(schema_ptr s, bytes b)
{
    auto sst_key = sstables::key::from_bytes(b);
    return dht::decorate_key(*s, sst_key.to_partition_key(*s));
}

// Must be called from a seastar thread.
shared_sstable verify_mutation(test_env& env, shared_sstable sst, lw_shared_ptr<replica::memtable> mt, bytes key, std::function<void(mutation_opt&)> verify);
inline shared_sstable verify_mutation(test_env& env, std::function<shared_sstable()> sst_gen, lw_shared_ptr<replica::memtable> mt, bytes key, std::function<void(mutation_opt&)> verify) {
    return verify_mutation(env, sst_gen(), std::move(mt), std::move(key), std::move(verify));
}
shared_sstable verify_mutation(test_env& env, shared_sstable sstp, bytes key, std::function<void(mutation_opt&)> verify);

shared_sstable verify_mutation(test_env& env, shared_sstable sst, lw_shared_ptr<replica::memtable> mt, dht::partition_range pr, std::function<stop_iteration(mutation_opt&)> verify);
inline shared_sstable verify_mutation(test_env& env, std::function<shared_sstable()> sst_gen, lw_shared_ptr<replica::memtable> mt, dht::partition_range pr, std::function<stop_iteration(mutation_opt&)> verify) {
    return verify_mutation(env, sst_gen(), std::move(mt), std::move(pr), std::move(verify));
}
shared_sstable verify_mutation(test_env& env, shared_sstable sstp, dht::partition_range pr, std::function<stop_iteration(mutation_opt&)> verify);
