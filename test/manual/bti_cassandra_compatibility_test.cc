/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/testing/random.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/app-template.hh>
#include <boost/program_options.hpp>
#include <xxhash.h>
#include <fmt/std.h>
#include "readers/from_mutations.hh"
#include "schema/schema_builder.hh"
#include "sstables/sstable_writer.hh"
#include "sstables/trie/bti_index.hh"
#include "test/lib/random_schema.hh"
#include "test/lib/sstable_test_env.hh"
#include "test/lib/sstable_utils.hh"
#include "readers/compacting.hh"

namespace bpo = boost::program_options;

struct mutations_cfg {
    uint64_t partition_count = 100;
    uint64_t row_count = 100;
    uint64_t range_tombstone_count = 5;
};

struct test_config {
    std::string phase;
    std::string test;
    std::optional<std::string> scylla_directory;
    std::optional<std::string> cassandra_directory;
    unsigned random_seed;
    mutations_cfg mutations_cfg;
};

// Cassandra can't handle negative partition timestamps in BTI sstables.
static tests::timestamp_generator nonnegative_timestamp_generator(unsigned seed) {
    constexpr api::timestamp_type min_positive_timestamp = 1;
    return [gen = tests::uncompactible_timestamp_generator(seed, min_positive_timestamp)] (std::mt19937& engine, tests::timestamp_destination dest, api::timestamp_type min_timestamp) {
        return gen(engine, dest, min_positive_timestamp);
    };
}

static tests::expiry_generator future_expiry_expiry_generator() {
    return [] (std::mt19937& engine, tests::timestamp_destination dest) -> std::optional<tests::expiry_info> {
        using namespace std::chrono;
        // Expiry in the future. We don't want GC to happen during the test.
        auto min_expiry_point = gc_clock::from_time_t(system_clock::to_time_t(sys_days(year{2037}/January/1)));
        auto max_expiry_point = gc_clock::from_time_t(system_clock::to_time_t(sys_days(year{2038}/January/1)));
        std::uniform_int_distribution<int64_t> seconds_dist(0, (max_expiry_point - min_expiry_point).count() - 1);
        auto expiry_point = min_expiry_point + gc_clock::duration(seconds_dist(engine));

        return tests::expiry_info{
            .ttl = std::chrono::days(1),
            .expiry_point = expiry_point,
        };
    };
}

// Some types don't have BTI encoding implemented yet.
static bool supported_type(const abstract_type& at_raw) {
    if (at_raw.is_reversed()) {
        return false;
    }
    const auto& at = at_raw.without_reversed();
    if (at.is_multi_cell() || at.is_collection()
        || at.is_tuple() || at.is_vector() || at.is_user_type() || at == *timestamp_type || at == *date_type
    ) {
        return false;
    }
    return true;
}

tests::random_schema generate_random_schema(unsigned& seed) {
    auto ks_name = "ks";
    auto partition_key_count_dist = std::uniform_int_distribution<size_t>(1, 2);
    auto clustering_key_count_dist = std::uniform_int_distribution<size_t>(1, 2);
    auto regular_column_count_dist = std::uniform_int_distribution<size_t>(0, 1);
    auto static_column_count_dist = std::uniform_int_distribution<size_t>(0, 1);
    auto random_spec = tests::make_random_schema_specification(
        ks_name,
        partition_key_count_dist,
        clustering_key_count_dist,
        regular_column_count_dist,
        static_column_count_dist,
        tests::random_schema_specification::compress_sstable::no
    );

    struct spec_wrapper : tests::random_schema_specification {
        tests::random_schema_specification& _base;
        spec_wrapper(tests::random_schema_specification& base)
            : tests::random_schema_specification(base.keyspace_name())
            , _base(base)
        {}
        // Some types don't have BTI encoding implemented yet.
        std::vector<data_type> fix_key_columns(std::vector<data_type> types) {
            for (auto& t : types) {
                if (!supported_type(*t)) {
                    t = bytes_type;
                }
            }
            return types;
        }
        // Creating matching UDTs in Cassandra would be a needless pain.
        // We avoid the problem by not using UDTs in this test.
        std::vector<data_type> fix_value_columns(std::vector<data_type> types) {
            for (auto& t : types) {
                if (!supported_type(*t)) {
                    t = bytes_type;
                }
            }
            return types;
        }
        std::vector<data_type> partition_key_columns(std::mt19937 &engine) override {
            return fix_key_columns(_base.partition_key_columns(engine));
        }
        std::vector<data_type> clustering_key_columns(std::mt19937 &engine) override {
            return fix_key_columns(_base.clustering_key_columns(engine));
        }
        std::vector<data_type> static_columns(std::mt19937 &engine) override {
            return fix_value_columns(_base.static_columns(engine));
        }
        std::vector<data_type> regular_columns(std::mt19937 &engine) override {
            return fix_value_columns(_base.regular_columns(engine));
        }
        compress_sstable & compress() override {
            return _base.compress();
        }
        sstring table_name(std::mt19937 &engine) override {
            return _base.table_name(engine);
        }
        sstring udt_name(std::mt19937 &engine) override {
            return _base.udt_name(engine);
        }
    };

    auto wrapped_spec = spec_wrapper(*random_spec);
    return tests::random_schema{seed, wrapped_spec};
}

static future<utils::chunked_vector<mutation>> generate_mutations(tests::random_schema& rs, unsigned seed, const mutations_cfg& cfg) {
    auto schema = rs.schema();

    auto partition_count_dist = std::uniform_int_distribution<size_t>(1, cfg.partition_count);
    auto clustering_row_count_dist = std::uniform_int_distribution<size_t>(0, cfg.row_count);
    auto range_tombstone_count_dist = std::uniform_int_distribution<size_t>(0, cfg.range_tombstone_count);
retry:
    auto mutations = co_await tests::generate_random_mutations(
        seed,
        rs,
        nonnegative_timestamp_generator(seed),
        future_expiry_expiry_generator(),
        partition_count_dist,
        clustering_row_count_dist,
        range_tombstone_count_dist
    );
    // Empty partition keys are not allowed in Cassandra.
    for (auto it = mutations.begin(); it != mutations.end(); ++it) {
        bool empty = std::ranges::empty(it->key().legacy_form(*rs.schema()));
        if (empty) {
            if (mutations.size() > 1) {
                it = mutations.erase(it);
            } else {
                seed += 1;
                goto retry;
            }
        }
    }
    co_return mutations;
}

static sstables::deletion_time sstable_deletion_time_from_tombstone(tombstone t) {
    if (!t) {
        return sstables::deletion_time::make_live();
    }
    return sstables::deletion_time{
        .local_deletion_time = t.deletion_time.time_since_epoch().count(),
        .marked_for_delete_at = t.timestamp,
    };
}

static sstables::bound_kind_m pip_bound_weight_to_m_bound_weight(const bound_weight bw) {
    // The conversion from m_bound_weight to bound_weight is lossy.
    // Here, we are doing the inverse, so we have to make some choice for the resulting bound_kind_m.
    // It doesn't really matter, because the row writer is going to just convert it back to bound_weight anyway.
    switch (bw) {
    case bound_weight::before_all_prefixed:
        return sstables::bound_kind_m::excl_end_incl_start;
    case bound_weight::after_all_prefixed:
        return sstables::bound_kind_m::incl_end_excl_start;
    case bound_weight::equal:
        return sstables::bound_kind_m::clustering;
    }
    abort();
}

static sstables::clustering_info clustering_info_from_pip(position_in_partition_view pipv) {
    return sstables::clustering_info{
        .clustering = pipv.key(),
        .kind = pip_bound_weight_to_m_bound_weight(pipv.get_bound_weight()),
    };
}

struct position_and_fragment {
    uint64_t offset;
    std::optional<tombstone> preceding_range_tombstone;
    mutation_fragment_v2 fragment;
};

struct partitions_metadata {
    dht::decorated_key first_key;
    dht::decorated_key last_key;
    uint64_t partition_count;
};

// Checks that the index files (passed via filesystem paths)
// are consistent with the fragments they were (supposed to be) built on.
void test_index_files(
    sstables::test_env& env,
    schema_ptr s,
    std::span<const position_and_fragment> fragments,
    const partitions_metadata& pm,
    std::filesystem::path partitions_db_path,
    std::filesystem::path rows_db_path,
    uint64_t data_db_size,
    std::optional<uint64_t> expected_index_granularity
) {
    // Open the index files, wrap them into `cached_file`.
    file partitions_db = open_file_dma(partitions_db_path.c_str(), open_flags::create | open_flags::ro).get();
    file rows_db = open_file_dma(rows_db_path.c_str(), open_flags::create | open_flags::ro).get();
    auto close_partitions_db = deferred_close(partitions_db);
    auto close_rows_db = deferred_close(rows_db);

    // Wrap the index files them into `cached_file`.
    auto stats = cached_file_stats();
    auto cached_file_lru = lru();
    auto region = logalloc::region();
    auto partitions_db_size = partitions_db.size().get();
    auto rows_db_size = rows_db.size().get();
    auto partitions_db_cached = cached_file(partitions_db, stats, cached_file_lru, region, partitions_db_size, "Partitions.db");
    auto rows_db_cached = cached_file(rows_db, stats, cached_file_lru, region, rows_db_size, "Rows.db");

    // Read the Partitions.db footer, check the metadata contained there.
    auto footer = sstables::trie::read_bti_partitions_db_footer(*s, sstable::version_types::me, partitions_db_cached.get_file(), partitions_db_size).get();
    SCYLLA_ASSERT(footer.partition_count == pm.partition_count);
    SCYLLA_ASSERT(sstables::key_view(footer.first_key) == sstables::key::from_partition_key(*s, pm.first_key.key()));
    SCYLLA_ASSERT(sstables::key_view(footer.last_key) == sstables::key::from_partition_key(*s, pm.last_key.key()));

    auto ir = sstables::trie::make_bti_index_reader(
        partitions_db_cached,
        rows_db_cached,
        footer.trie_root_position,
        data_db_size,
        s,
        env.make_reader_permit());

    // We validate the index by iterating over fragments and checking that
    // the index gives the right results when it's forwarded to the position of each fragment. 

    uint64_t prev_frag_offset = 0;
    uint64_t curr_partition_start = 0;
    bool curr_partition_has_row_index = false;
    // Data.db range returned by the index when forwarded to the previous fragment.
    std::optional<std::pair<uint64_t, uint64_t>> prev_range = std::pair<uint64_t, uint64_t>{0, 0};
    for (const auto& [offset, rt, mf] : fragments) {
        // We check the granularity at the points where we cross into a new Data.db range.
        if (prev_range && offset >= prev_range.value().second) {
            if (expected_index_granularity) {
                testlog.debug("Previous pos: {}, previous range: {}, allowed max size: {}", prev_frag_offset, *prev_range, offset - prev_frag_offset + *expected_index_granularity);
                SCYLLA_ASSERT(prev_frag_offset - prev_range->first <= *expected_index_granularity);
            }
            prev_range.reset();
        }
        if (mf.is_partition_start()) {
            auto dk = mf.as_partition_start().key();
            auto potential_match = ir->advance_lower_and_check_if_present(dk).get();
            auto range = ir->data_file_positions();
            auto expected_partition_tombstone = mf.as_partition_start().partition_tombstone();
            auto actual_partition_tombstone = ir->partition_tombstone().transform([] (const sstables::deletion_time& dt) { return tombstone(dt); });
            testlog.debug("After advancing to partition {}, range start: {}, offset: {}, partition_tombstone: {} (expected: {})",
                dk, range.start, offset, actual_partition_tombstone, expected_partition_tombstone);
            SCYLLA_ASSERT(potential_match);
            SCYLLA_ASSERT(!ir->eof());
            SCYLLA_ASSERT(range.start == offset);
            SCYLLA_ASSERT(ir->element_kind() == sstables::indexable_element::partition);
            curr_partition_start = offset;
            curr_partition_has_row_index = ir->partition_tombstone().has_value();
            if (curr_partition_has_row_index) {
                SCYLLA_ASSERT(mf.as_partition_start().partition_tombstone() == tombstone(ir->partition_tombstone().value()));
                SCYLLA_ASSERT(dk.key() == ir->get_partition_key().value());
            }
        }
        if (mf.is_clustering_row() || mf.is_range_tombstone_change()) {
            position_in_partition_view pos = mf.is_clustering_row()
                ? mf.as_clustering_row().position()
                : mf.as_range_tombstone_change().position();
            ir->advance_upper_past(pos).get();
            ir->advance_to(pos).get();
            auto range = ir->data_file_positions();
            auto ir_end_open_tombstone = ir->end_open_marker().transform([] (const open_rt_marker& m) { return m.tomb; });
            testlog.debug("At clustering row/range tombstone change, range start: {}, offset: {}, ir_end_open_tombstone: {}",
                    range.start, offset, ir_end_open_tombstone);
            if (!curr_partition_has_row_index) {
                SCYLLA_ASSERT(ir->element_kind() == sstables::indexable_element::partition);
                SCYLLA_ASSERT(range.start == curr_partition_start);
            }
            auto pointed_entry = std::ranges::lower_bound(fragments, range.start, {}, &position_and_fragment::offset);
            SCYLLA_ASSERT(pointed_entry != fragments.end());
            SCYLLA_ASSERT(pointed_entry->offset == range.start);
            SCYLLA_ASSERT(pointed_entry->preceding_range_tombstone == ir_end_open_tombstone);
            auto pointed_element_kind = pointed_entry->fragment.is_partition_start()
                ? sstables::indexable_element::partition
                : sstables::indexable_element::cell;
            SCYLLA_ASSERT(ir->element_kind() == pointed_element_kind);
            SCYLLA_ASSERT(range.start <= offset);
            SCYLLA_ASSERT(range.end.has_value() && range.end.value() > offset);
            if (prev_range) {
                SCYLLA_ASSERT(range.start == prev_range->first);
                SCYLLA_ASSERT(range.end.value() == prev_range->second);
            } else {
                prev_range = std::make_pair(range.start, range.end.value());
            }
        }
        if (mf.is_end_of_partition()) {
            ir->advance_upper_past(position_in_partition::for_partition_end()).get();
            ir->advance_to(position_in_partition::for_partition_end()).get();
            auto range = ir->data_file_positions();
            testlog.debug("At partition end, range start: {}, offset: {}", range.start, offset);
            if (!curr_partition_has_row_index) {
                SCYLLA_ASSERT(ir->element_kind() == sstables::indexable_element::partition);
                SCYLLA_ASSERT(range.start == curr_partition_start);
            } else {
                SCYLLA_ASSERT(range.start == offset);
            }
            SCYLLA_ASSERT(range.end.has_value() && range.end.value() == offset + 1);
            prev_range.reset();
        }
        prev_frag_offset = offset;
    }
}

void do_test(const test_config& cfg) {
    if (cfg.phase == "prepare") {
    } else if (cfg.phase == "check") {
        if (!cfg.cassandra_directory) {
            throw std::runtime_error("In `check` phase, --cassandra-directory must be provided");
        }
    } else {
        throw std::runtime_error("Unknown phase: " + cfg.phase);
    }

    unsigned seed = cfg.random_seed;
    // rehash with xxhash
    seed = XXH64(&seed, sizeof(seed), seed);
    auto random_schema = generate_random_schema(seed);

    auto mutations = generate_mutations(random_schema, seed, cfg.mutations_cfg).get();
    SCYLLA_ASSERT(!mutations.empty());
    auto schema = mutations.front().schema();
    auto adjusted_schema = schema_builder(schema)
        .set_compaction_strategy(sstables::compaction_strategy_type::size_tiered)
        .build();
    auto schema_description = adjusted_schema->describe(
        schema_describe_helper{.type = schema_describe_helper::type::table},
        cql3::describe_option::STMTS
    );
    auto schema_text = schema_description.create_statement.value().linearize();
    testlog.info("Generated {} mutations with schema:\n{}", mutations.size(), schema_text);

    auto write_file = [] (const sstring& file_name, const sstring& text) {
        auto f = open_file_dma(file_name, open_flags::wo | open_flags::create | open_flags::truncate).get();
        auto os = make_file_output_stream(f, file_output_stream_options{}).get();
        os.write(text).get();
        os.flush().get();
        os.close().get();
    };

    if (cfg.scylla_directory) {
        auto schema_fname = std::string(std::filesystem::path(cfg.scylla_directory.value()) / "schema.cql");
        auto tablename_fname = std::string(std::filesystem::path(cfg.scylla_directory.value()) / "tablename.txt");
        auto schema_text = schema_description.create_statement.value().linearize();
        write_file(schema_fname, schema_text);
        write_file(tablename_fname, adjusted_schema->cf_name());
    }

    sstables::test_env::do_with_async([&] (sstables::test_env& env) {
        auto make_reader_from_mutations = [&] () {
            auto r = make_mutation_reader_from_mutations(adjusted_schema, env.make_reader_permit(), mutations);
            return make_compacting_reader(std::move(r),
                gc_clock::now(),
                can_never_purge,
                tombstone_gc_state(nullptr));
        };
        // We want to use Data.db size to validate that Cassandra imported
        // the Scylla-generated sstable without any unexpected compaction.

        // However, Data.db size depends on encoding stats.
        // So here we do a preliminary sstable write to get the encoding stats out of it.
        auto sst_1st_pass = make_sstable_easy(
            env,
            make_reader_from_mutations(),
            env.manager().configure_writer()
        );
        auto enc_stats = sst_1st_pass->get_encoding_stats_for_compaction();

        // This is the actual sstable write that we are going to use for the test.
        // 
        // During the "prepare" phase, we write the sstable to the specified directory,
        // so that it can be imported into Cassandra.
        //
        // During the "check" phase, we write exactly the same sstable to a temporary directory,
        // just so we can learn the Data.db positions of each fragment for the purposes
        // of the test logic.
        // (There's no easy way to learn the Data.db positions from *reading* sstables,
        // only from writing them. And adding one would be troublesome.)
        auto sst = cfg.phase == "prepare"
            ? env.make_sstable(
                adjusted_schema,
                cfg.scylla_directory.value(),
                sstables::sstable_version_types::me)
            : env.make_sstable(
                adjusted_schema,
                sstables::sstable_version_types::me);
        auto mr = make_reader_from_mutations();
        auto close_mr = deferred_close(mr);
        auto wr = sst->get_writer(*schema, mutations.size(), env.manager().configure_writer(), enc_stats);

        std::vector<position_and_fragment> fragments;
        auto permit = env.make_reader_permit();
        std::optional<tombstone> current_range_tombstone;
        uint64_t row_idx = 0;
        while (mutation_fragment_v2_opt mfopt = mr().get()) {
            testlog.debug("Consuming at pos {} (row {}): {}", wr.data_file_position_for_tests(), row_idx,
                    mutation_fragment_v2::printer(*adjusted_schema, *mfopt));
            row_idx += bool(mfopt->is_clustering_row());
            fragments.push_back(position_and_fragment{
                .offset = wr.data_file_position_for_tests(),
                .preceding_range_tombstone = current_range_tombstone,
                .fragment = mutation_fragment_v2(*adjusted_schema, permit, *mfopt)
            });
            struct consumer {
                decltype(wr)& _wr;
                decltype(current_range_tombstone)& _current_range_tombstone;
                void consume(static_row sr) { _wr.consume(std::move(sr)); }
                void consume(clustering_row cr) { _wr.consume(std::move(cr)); }
                void consume(range_tombstone_change rt) {
                    if (rt.tombstone()) {
                        _current_range_tombstone = rt.tombstone();
                    } else {
                        _current_range_tombstone.reset();
                    }
                    _wr.consume(std::move(rt));
                }
                void consume(partition_start ps) {
                    _wr.consume_new_partition(std::move(ps.key()));
                    _wr.consume(ps.partition_tombstone());
                }
                void consume (partition_end pe) {
                    _current_range_tombstone.reset();
                    _wr.consume_end_of_partition();
                }
            };
            consumer c(wr, current_range_tombstone);
            std::move(*mfopt).consume(c);
        }
        wr.consume_end_of_stream();
        sst->open_data().get();
        auto sst_size = sst->data_size();
        testlog.info("sst size: {}", sst->data_size());

        if (cfg.phase == "prepare") {
            auto sst_dir = cfg.scylla_directory.value();
            testlog.info("Writing Scylla-generated BTI index to {}", sst_dir);

            // Construct the file paths for the generated BTI index files.
            auto big_index_path = sst->index_filename().format();
            auto suffix = std::string_view("Index.db");
            SCYLLA_ASSERT(big_index_path.ends_with(suffix));
            auto path_without_suffix = big_index_path.substr(0, big_index_path.size() - suffix.size());
            auto bti_partitions_path = std::string(path_without_suffix) + "Partitions.db";
            auto bti_rows_path = std::string(path_without_suffix) + "Rows.db";

            // Open the BTI index files, wrap them into `sstables::file_writer`.
            auto partitions_db = open_file_dma(bti_partitions_path.c_str(), open_flags::create | open_flags::wo | open_flags::truncate).get();
            auto partitions_db_stream = make_file_output_stream(partitions_db, file_output_stream_options{}).get();
            auto partitions_db_writer = sstables::file_writer(std::move(partitions_db_stream));
            auto close_partitions_db = defer([&] () { partitions_db_writer.close(); });
            auto rows_db = open_file_dma(bti_rows_path.c_str(), open_flags::create | open_flags::wo | open_flags::truncate).get();
            auto rows_db_stream = make_file_output_stream(rows_db, file_output_stream_options{}).get();
            auto rows_db_writer = sstables::file_writer(std::move(rows_db_stream));
            auto close_rows_db = defer([&] () { rows_db_writer.close(); });

            // Construct BTI index writers on top of the `file_writer`s.
            auto bti_partition_index_writer = sstables::trie::bti_partition_index_writer(partitions_db_writer);
            auto bti_row_index_writer = sstables::trie::bti_row_index_writer(rows_db_writer);

            struct current_partition_data {
                uint64_t data_file_offset;
                tombstone partition_tombstone;
                dht::decorated_key dk;
                uint64_t row_count = 0;
            };
            struct current_clustering_data {
                position_in_partition first_ckp;
                position_in_partition last_ckp;
                uint64_t first_data_file_offset;
                uint64_t last_data_file_offset;
                tombstone preceding_range_tombstone;
            };
            const int rows_per_block = 3;
            std::optional<current_partition_data> current_partition;
            std::optional<current_clustering_data> current_clustering_block;

            auto push_clustering_block = [&] {
                SCYLLA_ASSERT(current_clustering_block);
                bti_row_index_writer.add(
                    *adjusted_schema,
                    clustering_info_from_pip(current_clustering_block->first_ckp),
                    clustering_info_from_pip(current_clustering_block->last_ckp),
                    current_clustering_block->first_data_file_offset - current_partition->data_file_offset,
                    sstable_deletion_time_from_tombstone(current_clustering_block->preceding_range_tombstone));
                current_clustering_block.reset();
            };
            // Iterate over fragments, feed them to the BTI index writers.
            for (const auto& frag : fragments) {
                if (frag.fragment.is_partition_start()) {
                    auto dk = frag.fragment.as_partition_start().key();
                    current_partition.emplace(current_partition_data{
                        .data_file_offset = frag.offset,
                        .partition_tombstone = frag.fragment.as_partition_start().partition_tombstone(),
                        .dk = dk,
                    });
                } else if (frag.fragment.is_clustering_row() || frag.fragment.is_range_tombstone_change()) {
                    auto key = frag.fragment.is_clustering_row()
                        ? position_in_partition(frag.fragment.as_clustering_row().key())
                        : position_in_partition(frag.fragment.as_range_tombstone_change().position());
                    if (!current_clustering_block) {
                        current_clustering_block.emplace(current_clustering_data{
                            .first_ckp = key,
                            .last_ckp = key,
                            .first_data_file_offset = frag.offset,
                            .preceding_range_tombstone = frag.preceding_range_tombstone.value_or(tombstone()),
                        });
                    }
                    current_clustering_block->last_ckp = key;
                    if (current_partition->row_count % rows_per_block == rows_per_block - 1) {
                        push_clustering_block();
                    }
                    ++current_partition->row_count;
                } else if (frag.fragment.is_end_of_partition()) {
                    if (current_clustering_block) {
                        push_clustering_block();
                    }
                    auto payload = bti_row_index_writer.finish(
                        sst->get_version(),
                        *adjusted_schema,
                        current_partition->data_file_offset,
                        frag.offset,
                        sstables::key::from_partition_key(*adjusted_schema, current_partition->dk.key()),
                        sstable_deletion_time_from_tombstone(current_partition->partition_tombstone)
                    );
                    bti_partition_index_writer.add(*adjusted_schema, current_partition->dk, payload);
                    current_partition.reset();
                }
            }
            std::move(bti_partition_index_writer).finish(
                sst->get_version(),
                disk_string_view<uint16_t>(sstables::key::from_partition_key(*adjusted_schema, mutations.front().key()).get_bytes()),
                disk_string_view<uint16_t>(sstables::key::from_partition_key(*adjusted_schema, mutations.back().key()).get_bytes())
            );
            testlog.info("Finished BTI index writing");
        }

        if (cfg.phase == "check") {
            // Find Cassandra's BTI index files in the specified directory.
            std::string cassandra_dir = cfg.cassandra_directory.value();
            std::optional<std::string> cassandra_partitions_db_path;
            std::optional<std::string> cassandra_rows_db_path;
            for (const auto& p : std::filesystem::directory_iterator(cassandra_dir)) {
                auto path = p.path().string();
                if (path.ends_with("Partitions.db")) {
                    cassandra_partitions_db_path = path;
                } else if (path.ends_with("Rows.db")) {
                    cassandra_rows_db_path = path;
                }
            }
            SCYLLA_ASSERT(cassandra_partitions_db_path);
            SCYLLA_ASSERT(cassandra_rows_db_path);

            // Find Scylla's BTI index files in the specified directory.
            std::string scylla_dir = cfg.scylla_directory.value();
            std::optional<std::string> scylla_partitions_db_path;
            std::optional<std::string> scylla_rows_db_path;
            for (const auto& p : std::filesystem::directory_iterator(scylla_dir)) {
                auto path = p.path().string();
                if (path.ends_with("Partitions.db")) {
                    scylla_partitions_db_path = path;
                } else if (path.ends_with("Rows.db")) {
                    scylla_rows_db_path = path;
                }
            }
            SCYLLA_ASSERT(scylla_partitions_db_path);
            SCYLLA_ASSERT(scylla_rows_db_path);

            auto partition_meta = partitions_metadata{
                .first_key = mutations.front().decorated_key(),
                .last_key = mutations.back().decorated_key(),
                .partition_count = mutations.size(),
            };

            testlog.info("Testing Scylla-generated BTI index files:");
            // We write an index block every 3 rows, so we can't expect a specific index granularity here
            auto expected_scylla_index_granularity = std::nullopt;
            test_index_files(
                env,
                adjusted_schema,
                fragments,
                partition_meta,
                scylla_partitions_db_path.value(),
                scylla_rows_db_path.value(),
                sst_size,
                expected_scylla_index_granularity
            );

            testlog.info("Testing Cassandra-generated BTI index files:");
            const uint64_t expected_cassandra_index_granularity = 4096;
            test_index_files(
                env,
                adjusted_schema,
                fragments,
                partition_meta,
                cassandra_partitions_db_path.value(),
                cassandra_rows_db_path.value(),
                sst_size,
                expected_cassandra_index_granularity
            );
        }
    }).get();
}

int main(int ac, char** av) {
    app_template::config cfg;
    cfg.description = R"---(
This is the C++ part of the test of BTI format compatibility between
Scylla and Cassandra.
It works in conjunction with bti_cassandra_compatibility_test.py,
and is designed to be launched by it.
With --phase=prepare, this executable generates a BIG sstable
with a random schema and data, and with corresponding BTI index files.
With --phase=check, this executable checks is Scylla can read
BTI index files generated by Cassandra (and files generated by Scylla)
for the same BIG sstable.
)---";
    app_template app(cfg);
    mutations_cfg default_mutations_cfg;
    app.add_options()
        ("phase", bpo::value<std::string>()->required(), "Test phase: `prepare` or `check`.")
        ("scylla-directory", bpo::value<std::string>(), "In `prepare` phase, output directory for the Scylla-generated BIG sstable. "
                                                 "In `check` phase, input directory with the same sstable.")
        ("cassandra-directory", bpo::value<std::string>(), "In `check` phase, output directory for a Cassandra-generated BTI sstable. ")
        ("random-seed", bpo::value<unsigned>()->required(), "Random seed for the test. Must be the same across both test phases.")
        ("partition-count", bpo::value<uint64_t>()->default_value(default_mutations_cfg.partition_count), "Max number of partitions in the test sstable.")
        ("row-count", bpo::value<uint64_t>()->default_value(default_mutations_cfg.row_count), "Max number of rows per partition in the test sstable.")
        ("range-tombstone-count", bpo::value<uint64_t>()->default_value(default_mutations_cfg.range_tombstone_count), "Max number of range tombstones in the test sstable.");

        return app.run(ac, av, [&app] {
        return seastar::async([&app] {
            auto options = app.configuration();
            auto convert_to_std_optional = [] (const bpo::variable_value& v) -> std::optional<std::string> {
                if (v.empty()) {
                    return std::nullopt;
                } else {
                    return v.as<std::string>();
                }
            };
            auto seed = options["random-seed"].as<unsigned>();
            seastar::testing::local_random_engine.seed(seed);
            auto cfg = test_config{
                .phase = options["phase"].as<std::string>(),
                .scylla_directory = convert_to_std_optional(options["scylla-directory"]),
                .cassandra_directory = convert_to_std_optional(options["cassandra-directory"]),
                .random_seed = seed,
                .mutations_cfg = mutations_cfg{
                    .partition_count = options["partition-count"].as<uint64_t>(),
                    .row_count = options["row-count"].as<uint64_t>(),
                    .range_tombstone_count = options["range-tombstone-count"].as<uint64_t>(),
                },
            };
            do_test(cfg);
        });
    });
}
