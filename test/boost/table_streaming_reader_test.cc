/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#undef SEASTAR_TESTING_MAIN
#include <seastar/util/closeable.hh>
#include <seastar/testing/test_case.hh>

#include <map>
#include <set>

#include "dht/i_partitioner.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "types/types.hh"
#include "schema/schema.hh"
#include "replica/database.hh"
#include "locator/tablets.hh"
#include "readers/mutation_reader.hh"
#include "mutation/mutation.hh"
#include "sstables/exceptions.hh"
#include "test/lib/sstable_utils.hh"
#include "db/commitlog/commitlog.hh"

BOOST_AUTO_TEST_SUITE(table_streaming_reader_test)

// Verifies that the streaming reader can read memtables from multiple tablets
// (storage groups) on the same shard. The test creates a streaming reader on
// two token ranges belonging to different tablets and verifies that the
// memtable data from both tablets is read, while data from other tablets are not.
SEASTAR_TEST_CASE(test_streaming_reader_reads_all_tablets_memtables) {
    if (this_smp_shard_count() > 1) {
        std::cerr << "Cannot run test " << get_name() << " with this_smp_shard_count() > 2" << std::endl;
        return make_ready_future<>();
    }
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto ks_name = sstring("test_ks");
        // The test runs on a single shard (see custom_args in test_config.yaml),
        // so this shard owns all tablets.
        e.execute_cql(format("CREATE KEYSPACE {} "
                "WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} "
                "AND tablets = {{'enabled': true, 'initial': 4}}", ks_name)).get();
        e.execute_cql(format("CREATE TABLE {}.t (pk int PRIMARY KEY, v int)", ks_name)).get();

        auto& db = e.local_db();
        auto s = db.find_schema(ks_name, "t");
        auto uuid = s->id();
        auto& table = db.find_column_family(uuid);

        auto& stm = db.get_shared_token_metadata();
        const auto& tmap = stm.get()->tablets().get_tablet_map(uuid);

        // Find one representative partition key per tablet.
        // Data is applied to the memtable (not flushed) so that the streaming
        // reader must go through add_memtables_to_reader_list(). The tablet map
        // is ordered by token, so iterating it in ascending tablet_id order also
        // yields keys in ascending token order (required for sorted ranges).
        std::map<locator::tablet_id, dht::decorated_key> keys_by_tablet;
        for (int32_t i = 0; i < 100000 && keys_by_tablet.size() < 3; ++i) {
            auto pkey = partition_key::from_single_value(*s, int32_type->decompose(i));
            auto dk = dht::decorate_key(*s, pkey);
            auto tid = tmap.get_tablet_id(dk.token());
            keys_by_tablet.try_emplace(tid, std::move(dk));
        }
        BOOST_REQUIRE_MESSAGE(keys_by_tablet.size() >= 3, "Could not find keys in three distinct tablets");

        // Pick three tablets in ascending token order. Insert one key to each
        // one of them and then stream the outer two (non-adjacent) tablets.
        auto it = keys_by_tablet.begin();
        const dht::decorated_key& low_key = (it++)->second;
        const dht::decorated_key& mid_key = (it++)->second;
        const dht::decorated_key& high_key = (it++)->second;

        auto apply = [&] (const dht::decorated_key& dk, int32_t v) {
            mutation m(s, dk);
            m.set_clustered_cell(clustering_key_prefix::make_empty(), "v", v, api::new_timestamp());
            db.apply(s, freeze(m), tracing::trace_state_ptr(), db::commitlog::force_sync::no, db::no_timeout).get();
        };
        apply(low_key, 1);
        apply(mid_key, 2);
        apply(high_key, 3);

        // Two sorted, disjoint singular ranges in non-adjacent tablets. The
        // middle tablet is not covered by any range.
        dht::partition_range_vector ranges;
        ranges.push_back(dht::partition_range::make_singular(low_key));
        ranges.push_back(dht::partition_range::make_singular(high_key));

        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto reader = table.make_streaming_reader(s, semaphore.make_permit(), ranges, gc_clock::now());
        auto close_reader = deferred_close(reader);

        std::set<dht::decorated_key, dht::decorated_key::less_comparator> read_keys{
                dht::decorated_key::less_comparator(s)};
        while (auto mutation = read_mutation_from_mutation_reader(reader).get()) {
            read_keys.insert(mutation->decorated_key());
        }

        BOOST_REQUIRE_MESSAGE(read_keys.contains(low_key),
                "Streaming reader missed the partition in the first range's tablet");
        BOOST_REQUIRE_MESSAGE(read_keys.contains(high_key),
                "Streaming reader missed the partition in the second range's tablet");
        BOOST_REQUIRE_MESSAGE(!read_keys.contains(mid_key),
                "Streaming reader returned a partition from an unrequested tablet");
    });
}

// Verifies that the streaming reader checks the integrity of the sstables
// it reads also when it reads a single partition of a table with tablets -
// which the table reads with the sstable set's single-partition reader.
// The test corrupts the Data component of an uncompressed sstable, whose
// checksums are verified only by a reader which checks integrity, and
// expects the read to fail the checksum check.
SEASTAR_TEST_CASE(test_streaming_reader_checks_integrity_of_single_partition) {
    if (this_smp_shard_count() > 1) {
        std::cerr << "Cannot run test " << get_name() << " with this_smp_shard_count() > 1" << std::endl;
        return make_ready_future<>();
    }
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE KEYSPACE test_ks "
                "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} "
                "AND tablets = {'enabled': true}").get();
        e.execute_cql("CREATE TABLE test_ks.t (pk int PRIMARY KEY, v int) "
                "WITH compression = {'sstable_compression': ''}").get();
        e.execute_cql("INSERT INTO test_ks.t (pk, v) VALUES (1, 1)").get();

        auto& db = e.local_db();
        auto s = db.find_schema("test_ks", "t");
        auto& table = db.find_column_family(s->id());
        table.flush().get();
        auto sstables = table.get_sstables();
        BOOST_REQUIRE_EQUAL(sstables->size(), 1);
        auto sst = *sstables->begin();

        // Overwrite the beginning of the Data component.
        auto f = open_file_dma(sstables::test(sst).filename(component_type::Data).native(), open_flags::wo).get();
        auto len = f.disk_write_dma_alignment();
        auto buf = temporary_buffer<char>::aligned(f.memory_dma_alignment(), len);
        std::fill(buf.get_write(), buf.get_write() + len, 0xba);
        f.dma_write(0, buf.get(), len).get();
        f.close().get();

        auto dk = dht::decorate_key(*s, partition_key::from_single_value(*s, int32_type->decompose(1)));
        auto range = dht::partition_range::make_singular(dk);
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto reader = table.make_streaming_reader(s, semaphore.make_permit(), range, gc_clock::now());
        auto close_reader = deferred_close(reader);
        BOOST_REQUIRE_EXCEPTION(read_mutation_from_mutation_reader(reader).get(), sstables::malformed_sstable_exception,
                [] (const sstables::malformed_sstable_exception& e) {
                    return sstring(e.what()).find("failed checksum") != sstring::npos;
                });
    });
}

BOOST_AUTO_TEST_SUITE_END()
