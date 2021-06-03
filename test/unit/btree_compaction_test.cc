/*
 * Copyright (C) 2021-present ScyllaDB
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

#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>
#include <map>
#include <iostream>
#include <fmt/core.h>
#include "utils/logalloc.hh"

constexpr int TEST_NODE_SIZE = 7;
constexpr int TEST_LINEAR_THRESHOLD = 19;

#include "tree_test_key.hh"
#include "utils/intrusive_btree.hh"
#include "btree_validation.hh"
#include "collection_stress.hh"

using namespace intrusive_b;
using namespace seastar;

class test_key : public tree_test_key_base {
public:
    member_hook _hook;
    test_key(int nr) noexcept : tree_test_key_base(nr) {}
    test_key(const test_key&) = delete;
    test_key(test_key&& o) noexcept : tree_test_key_base(std::move(o)), _hook(std::move(o._hook)) {}
};

using test_tree = tree<test_key, &test_key::_hook, test_key_tri_compare, TEST_NODE_SIZE, TEST_LINEAR_THRESHOLD, key_search::both, with_debug::yes>;
using test_validator = validator<test_key, &test_key::_hook, test_key_tri_compare, TEST_NODE_SIZE, TEST_LINEAR_THRESHOLD>;

int main(int argc, char **argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("count", bpo::value<int>()->default_value(10000), "number of keys to fill the tree with")
        ("iters", bpo::value<int>()->default_value(13), "number of iterations")
        ("verb",  bpo::value<bool>()->default_value(false), "be verbose");

    return app.run(argc, argv, [&app] {
        auto count = app.configuration()["count"].as<int>();
        auto rep = app.configuration()["iters"].as<int>();
        auto verb = app.configuration()["verb"].as<bool>();

        return seastar::async([count, rep, verb] {
            stress_config cfg;
            cfg.count = count;
            cfg.iters = rep;
            cfg.verb = verb;

            tree_pointer<test_tree> t;
            test_validator tv;

            stress_compact_collection(cfg,
                /* insert */ [&] (int key) {
                    test_key *k = current_allocator().construct<test_key>(key);
                    auto ti = t->insert(*k, test_key_tri_compare{});
                    assert(ti.second);
                },
                /* erase */ [&] (int key) {
                    auto deleter = current_deleter<test_key>();
                    t->erase_and_dispose(test_key(key), test_key_tri_compare{}, deleter);
                },
                /* validate */ [&] {
                    if (verb) {
                        fmt::print("Validating:\n");
                        tv.print_tree(*t, '|');
                    }
                    tv.validate(*t);
                },
                /* clear */ [&] {
                    t->clear_and_dispose(current_deleter<test_key>());
                }
            );
        });
    });
}
