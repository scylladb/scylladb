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
#include <fmt/ostream.h>

constexpr int TEST_NODE_SIZE = 8;
constexpr int TEST_LINEAR_THRESH = 21;

#include "utils/intrusive_btree.hh"
#include "btree_validation.hh"
#include "test/unit/tree_test_key.hh"
#include "collection_stress.hh"

using namespace intrusive_b;
using namespace seastar;

class test_key : public tree_test_key_base {
public:
    member_hook _hook;
    test_key(int nr) noexcept : tree_test_key_base(nr) {}
    test_key(const test_key&) = delete;
    test_key(test_key&&) = delete;
};

using test_tree = tree<test_key, &test_key::_hook, test_key_tri_compare, TEST_NODE_SIZE, TEST_LINEAR_THRESH, key_search::both, with_debug::yes>;
using test_validator = validator<test_key, &test_key::_hook, test_key_tri_compare, TEST_NODE_SIZE, TEST_LINEAR_THRESH>;
using test_iterator_checker = iterator_checker<test_key, &test_key::_hook, test_key_tri_compare, TEST_NODE_SIZE, TEST_LINEAR_THRESH>;

int main(int argc, char **argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("count", bpo::value<int>()->default_value(4132), "number of keys to fill the tree with")
        ("iters", bpo::value<int>()->default_value(9), "number of iterations")
        ("keys",  bpo::value<std::string>()->default_value("rand"), "how to generate keys (rand, asc, desc)")
        ("verb",  bpo::value<bool>()->default_value(false), "be verbose");

    return app.run(argc, argv, [&app] {
        auto count = app.configuration()["count"].as<int>();
        auto iters = app.configuration()["iters"].as<int>();
        auto ks = app.configuration()["keys"].as<std::string>();
        auto verb = app.configuration()["verb"].as<bool>();

        return seastar::async([count, iters, ks, verb] {
            test_key_tri_compare cmp;
            auto t = std::make_unique<test_tree>();
            std::map<int, unsigned long> oracle;
            test_validator tv;
            auto* itc = new test_iterator_checker(tv, *t);

            stress_config cfg;
            cfg.count = count;
            cfg.iters = iters;
            cfg.keys = ks;
            cfg.verb = verb;
            auto itv = 0;

            stress_collection(cfg,
                /* insert */ [&] (int key) {
                    auto ir = t->insert(std::make_unique<test_key>(key), cmp);
                    assert(ir.second);
                    oracle[key] = key;

                    if (itv++ % 7 == 0) {
                        if (!itc->step()) {
                            delete itc;
                            itc = new test_iterator_checker(tv, *t);
                        }
                    }
                },
                /* erase */ [&] (int key) {
                    test_key k(key);
                    auto deleter = [] (test_key* k) noexcept { delete k; };

                    if (itc->here(k)) {
                        delete itc;
                        itc = nullptr;
                    }

                    t->erase_and_dispose(key, cmp, deleter);
                    oracle.erase(key);

                    if (itc == nullptr) {
                        itc = new test_iterator_checker(tv, *t);
                    }

                    if (itv++ % 5 == 0) {
                        if (!itc->step()) {
                            delete itc;
                            itc = new test_iterator_checker(tv, *t);
                        }
                    }
                },
                /* validate */ [&] {
                    if (verb) {
                        fmt::print("Validating\n");
                        tv.print_tree(*t, '|');
                    }
                    tv.validate(*t);
                },
                /* step */ [&] (stress_step step) {
                    if (step == stress_step::before_erase) {
                        auto sz = t->calculate_size();
                        if (sz != (size_t)count) {
                            fmt::print("Size {} != count {}\n", sz, count);
                            throw "size";
                        }

                        auto ti = t->begin();
                        for (auto oe : oracle) {
                            if ((unsigned long)*ti != oe.second) {
                                fmt::print("Data mismatch {} vs {}\n", oe.second, *ti);
                                throw "oracle";
                            }
                            ti++;
                        }
                    }
                }
            );

            delete itc;
        });
    });
}
