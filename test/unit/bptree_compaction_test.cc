/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>
#include <map>
#include <string>
#include <iostream>
#include <fmt/core.h>

constexpr int TEST_NODE_SIZE = 7;

#include "tree_test_key.hh"
#include "utils/assert.hh"
#include "utils/bptree.hh"
#include "bptree_validation.hh"
#include "collection_stress.hh"

using namespace bplus;
using namespace seastar;

using test_key = tree_test_key_base;

class test_data {
    int _value;
public:
    test_data() : _value(0) {}
    test_data(test_key& k) : _value((int)k + 10) {}

    operator unsigned long() const { return _value; }
    bool match_key(const test_key& k) const { return _value == (int)k + 10; }
};
using test_tree = tree<test_key, test_data, test_key_compare, TEST_NODE_SIZE, key_search::both, with_debug::yes>;
using test_validator = validator<test_key, test_data, test_key_compare, TEST_NODE_SIZE>;

int main(int argc, char **argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("count", bpo::value<int>()->default_value(10000), "number of keys to fill the tree with")
        ("iters", bpo::value<int>()->default_value(13), "number of iterations")
        ("verb",  bpo::value<bool>()->default_value(false), "be verbose");

    return app.run(argc, argv, [&app] {
        auto count = app.configuration()["count"].as<int>();
        auto iter = app.configuration()["iters"].as<int>();
        auto verb = app.configuration()["verb"].as<bool>();

        return seastar::async([count, iter, verb] {
            stress_config cfg;
            cfg.count = count;
            cfg.iters = iter;
            cfg.verb = verb;

            tree_pointer<test_tree> t(test_key_compare{});
            test_validator tv;

            stress_compact_collection(cfg,
                /* insert */ [&] (int key) {
                    test_key k(key);
                    auto ti = t->emplace(copy_key(k), k);
                    SCYLLA_ASSERT(ti.second);
                },
                /* erase */ [&] (int key) {
                    t->erase(test_key(key));
                },
                /* validate */ [&] {
                    if (verb) {
                        fmt::print("Validating:\n");
                        tv.print_tree(*t, '|');
                    }
                    tv.validate(*t);
                },
                /* clear */ [&] {
                    t->clear();
                }
            );
        });
    });
}
