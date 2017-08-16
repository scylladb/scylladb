/*
 * Copyright (C) 2017 ScyllaDB
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
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm/copy.hpp>

#include "core/sstring.hh"
#include "core/thread.hh"

#include "database.hh"
#include "mutation_reader.hh"
#include "schema_builder.hh"
#include "partition_slice_builder.hh"
#include "tmpdir.hh"
#include "sstable_mutation_readers.hh"
#include "cell_locking.hh"

#include "tests/test-utils.hh"
#include "tests/mutation_assertions.hh"
#include "tests/mutation_reader_assertions.hh"
#include "tests/result_set_assertions.hh"
#include "tests/simple_schema.hh"
#include "tests/sstable_utils.hh"
#include "tests/sstable_test.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

struct sst_factory {
    schema_ptr s;
    sstring path;
    unsigned gen;
    int level;

    sst_factory(schema_ptr s, const sstring& path, unsigned gen, int level)
        : s(s)
        , path(path)
        , gen(gen)
        , level(level)
    {}

    sstables::shared_sstable operator()() {
        auto sst = make_lw_shared<sstables::sstable>(s, path, gen, sstables::sstable::version_types::la, sstables::sstable::format_types::big);
        sst->set_unshared();

        //TODO set sstable level, to make the test more interesting

        return sst;
    }
};

SEASTAR_TEST_CASE(combined_mutation_reader_test) {
    return seastar::async([] {
        //logging::logger_registry().set_logger_level("database", logging::log_level::trace);

        simple_schema s;

        const auto pkeys = s.make_pkeys(4);
        const auto ckeys = s.make_ckeys(4);

        std::vector<mutation> base_mutations = boost::copy_range<std::vector<mutation>>(
                pkeys | boost::adaptors::transformed([&s](const auto& k) { return mutation(k, s.schema()); }));

        // Data layout:
        //   d[xx]
        // b[xx][xx]c
        // a[x    x]

        int i{0};

        // sstable d
        std::vector<mutation> table_d_mutations;

        i = 1;
        table_d_mutations.emplace_back(base_mutations[i]);
        s.add_row(table_d_mutations.back(), ckeys[i], sprint("val_d_%i", i));

        i = 2;
        table_d_mutations.emplace_back(base_mutations[i]);
        s.add_row(table_d_mutations.back(), ckeys[i], sprint("val_d_%i", i));
        const auto t_static_row = s.add_static_row(table_d_mutations.back(), sprint("%i_static_val", i));

        // sstable b
        std::vector<mutation> table_b_mutations;

        i = 0;
        table_b_mutations.emplace_back(base_mutations[i]);
        s.add_row(table_b_mutations.back(), ckeys[i], sprint("val_b_%i", i));

        i = 1;
        table_b_mutations.emplace_back(base_mutations[i]);
        s.add_row(table_b_mutations.back(), ckeys[i], sprint("val_b_%i", i));

        // sstable c
        std::vector<mutation> table_c_mutations;

        i = 2;
        table_c_mutations.emplace_back(base_mutations[i]);
        const auto t_row = s.add_row(table_c_mutations.back(), ckeys[i], sprint("val_c_%i", i));

        i = 3;
        table_c_mutations.emplace_back(base_mutations[i]);
        s.add_row(table_c_mutations.back(), ckeys[i], sprint("val_c_%i", i));

        // sstable a
        std::vector<mutation> table_a_mutations;

        i = 0;
        table_a_mutations.emplace_back(base_mutations[i]);
        s.add_row(table_a_mutations.back(), ckeys[i], sprint("val_a_%i", i));

        i = 3;
        table_a_mutations.emplace_back(base_mutations[i]);
        s.add_row(table_a_mutations.back(), ckeys[i], sprint("val_a_%i", i));

        auto tmp = make_lw_shared<tmpdir>();

        std::cout << tmp->path << std::endl;

        unsigned gen{0};

        std::vector<sstables::shared_sstable> tables = {
                make_sstable_containing(sst_factory(s.schema(), tmp->path, gen++, 0), table_a_mutations),
                make_sstable_containing(sst_factory(s.schema(), tmp->path, gen++, 1), table_b_mutations),
                make_sstable_containing(sst_factory(s.schema(), tmp->path, gen++, 1), table_c_mutations),
                make_sstable_containing(sst_factory(s.schema(), tmp->path, gen++, 2), table_d_mutations)
        };

        auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::leveled, {});
        auto sstables = make_lw_shared<sstables::sstable_set>(cs.make_sstable_set(s.schema()));

        std::vector<mutation_reader> sstable_mutation_readers;

        for (auto table : tables) {
            sstables->insert(table);

            sstable_mutation_readers.emplace_back(make_mutation_reader<sstable_range_wrapping_reader>(
                    table,
                    s.schema(),
                    query::full_partition_range,
                    query::full_slice,
                    seastar::default_priority_class(),
                    streamed_mutation::forwarding::no,
                    ::mutation_reader::forwarding::yes));
        }

        auto list_reader = make_combined_reader(std::move(sstable_mutation_readers), ::mutation_reader::forwarding::yes);

        auto incremental_reader = make_range_sstable_reader(
                s.schema(),
                sstables,
                query::full_partition_range,
                query::full_slice,
                seastar::default_priority_class(),
                nullptr,
                streamed_mutation::forwarding::no,
                ::mutation_reader::forwarding::yes);

        // merge c[0] with d[1]
        i = 2;
        auto c_d_merged = mutation(pkeys[i], s.schema());
        s.add_row(c_d_merged, ckeys[i], sprint("val_c_%i", i), t_row);
        s.add_static_row(c_d_merged, sprint("%i_static_val", i), t_static_row);

        assert_that(std::move(list_reader))
            .produces(table_a_mutations.front())
            .produces(table_b_mutations[1])
            .produces(c_d_merged)
            .produces(table_a_mutations.back());

        assert_that(std::move(incremental_reader))
            .produces(table_a_mutations.front())
            .produces(table_b_mutations[1])
            .produces(c_d_merged)
            .produces(table_a_mutations.back());
    });
};
