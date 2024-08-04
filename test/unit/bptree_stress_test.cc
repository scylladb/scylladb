/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>
#include <map>
#include <fmt/core.h>

constexpr int TEST_NODE_SIZE = 16;

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

template <> struct fmt::formatter<test_data> : fmt::formatter<string_view> {
    auto format(test_data d, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", static_cast<unsigned long>(d));
    }
};

using test_tree = tree<test_key, test_data, test_key_compare, TEST_NODE_SIZE, key_search::both, with_debug::yes>;
using test_node = typename test_tree::node;
using test_validator = validator<test_key, test_data, test_key_compare, TEST_NODE_SIZE>;
using test_iterator_checker = iterator_checker<test_key, test_data, test_key_compare, TEST_NODE_SIZE>;

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
            auto t = std::make_unique<test_tree>(test_key_compare{});
            std::map<int, unsigned long> oracle;
            test_validator tv;
            auto* itc = new test_iterator_checker(tv, *t);

            stress_config cfg;
            cfg.count = count;
            cfg.iters = iters;
            cfg.keys = ks;
            cfg.verb = verb;
            auto rep = 0, itv = 0;

            stress_collection(cfg,
                /* insert */ [&] (int key) {
                    test_key k(key);

                    if (rep % 2 != 1) {
                        auto ir = t->emplace(copy_key(k), k);
                        SCYLLA_ASSERT(ir.second);
                    } else {
                        auto ir = t->lower_bound(k);
                        ir.emplace_before(copy_key(k), test_key_compare{}, k);
                    }
                    oracle[key] = key + 10;

                    if (itv++ % 7 == 0) {
                        if (!itc->step()) {
                            delete itc;
                            itc = new test_iterator_checker(tv, *t);
                        }
                    }
                },
                /* erase */ [&] (int key) {
                    test_key k(key);

                    if (itc->here(k)) {
                        delete itc;
                        itc = nullptr;
                    }

                    if (rep % 3 != 2) {
                        t->erase(k);
                    } else {
                        auto ri = t->find(k);
                        auto ni = ri;
                        ni++;
                        auto eni = ri.erase(test_key_compare{});
                        SCYLLA_ASSERT(ni == eni);
                    }
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
                    if (step == stress_step::iteration_finished) {
                        rep++;
                    }

                    if (step == stress_step::before_erase) {
                        auto sz = t->size_slow();
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
