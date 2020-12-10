/*
 * Copyright (C) 2020 ScyllaDB
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

#include "database.hh"
#include "test/lib/simple_schema.hh"
#include "test/perf/perf.hh"
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include "test/lib/sstable_test_env.hh"
#include <iostream>

static shared_sstable sstable_for_overlapping_test(test_env& env, const schema_ptr& schema, int64_t gen, sstring first_key, sstring last_key, uint32_t level = 0) {
    auto sst = env.make_sstable(schema, "", gen, la, big);
    sstables::test(sst).set_values_for_leveled_strategy(0, level, 0, std::move(first_key), std::move(last_key));
    return sst;
}

int main(int argc, char* argv[]) {
    app_template app;
    return app.run(argc, argv, [&app] {
      return test_env::do_with_async([] (test_env& env) {
        using namespace std::chrono;
        using namespace std::chrono_literals;
        auto start = high_resolution_clock::now();
        simple_schema ss;
        auto s = ss.schema();
        auto cm = make_lw_shared<compaction_manager>();
        column_family::config cfg;
        auto cl_stats = make_lw_shared<cell_locker_stats>();
        auto tracker = make_lw_shared<cache_tracker>();
        auto cf = make_lw_shared<column_family>(s, cfg, column_family::no_commitlog(), *cm, *cl_stats, *tracker);
        cf->set_compaction_strategy(sstables::compaction_strategy_type::leveled);

        constexpr int ssts_in_level_0 = 10;
        std::function<int(int)> idx_to_level = [&] (int i) {
            if (i < ssts_in_level_0) {
                return 0;
            }
            return 1 + idx_to_level((i - ssts_in_level_0) / 10 + ssts_in_level_0 - 1);
        };
        auto level_to_size = [] (int level) {
            if (level == 0) {
                return ssts_in_level_0;
            }
            return int(pow(10, level));
        };

        auto kt_pair = token_generation_for_current_shard(1120);
        auto min_max_keys = [&kt_pair, &level_to_size] (auto level, auto pos_in_level) -> std::pair<sstring, sstring> {
            auto last_key_idx = kt_pair.size() - 1;
            if (level == 0) {
                return { kt_pair[0].first, kt_pair[last_key_idx].first };
            }
            auto total_ranges = kt_pair.size();
            auto level_size_in_ssts = level_to_size(level);
            unsigned ranges_per_sst = std::max(1U, unsigned(floor(float(total_ranges) / level_size_in_ssts)));
            sstring min_key = kt_pair.at(pos_in_level).first;
            sstring max_key = kt_pair.at(std::min(pos_in_level + ranges_per_sst - 1, unsigned(last_key_idx))).first;
            return {min_key, max_key};
        };

        std::vector<shared_sstable> inputs[3], outputs[3];

        std::array<unsigned, 9> pos_in_levels{0};
        pos_in_levels.fill(0);
        auto start2 = high_resolution_clock::now();
        for (auto i = 0; i < 1120; i++) {
            auto level = idx_to_level(i);
            auto [min, max] = min_max_keys(level, pos_in_levels[level]++);
            auto sst = sstable_for_overlapping_test(env, s, i, min, max, uint32_t(level));
            column_family_test(cf).add_sstable(sst);
            if (level >= 1 && pos_in_levels[level] < 10) {
                inputs[level-1].push_back(sst);
            }
            seastar::thread::maybe_yield();
        }

        for (auto i = 0; i < 30; i++) {
            auto [min, max] = min_max_keys(1 + i / 10, i % 10);
            auto sst = sstable_for_overlapping_test(env, s, i, min, max, 1 + i / 10);
            outputs[i / 10].push_back(sst);
            seastar::thread::maybe_yield();
        }
        for (auto i = 0; i < 3; i++) {
            auto t1 = high_resolution_clock::now();
            column_family_test(cf).rebuild_sstable_list(outputs[i], inputs[i]);
            auto t2 = high_resolution_clock::now();
            std::cout << "Replacing 10 L" << i + 1 <<" sstables took " 
                      << std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count() << "ms to complete\n";
        }
      });
    });
}
