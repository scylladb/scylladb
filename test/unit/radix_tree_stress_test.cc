/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>
#include <map>
#include <string>
#include <fmt/core.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>

#include "utils/assert.hh"
#include "utils/compact-radix-tree.hh"
#include "radix_tree_printer.hh"
#include "collection_stress.hh"

using namespace compact_radix_tree;
using namespace seastar;

class test_data {
    unsigned long *_data;
    unsigned long _val;
public:
    test_data(unsigned long val) : _data(new unsigned long(val)), _val(val) {}
    test_data(const test_data&) = delete;
    test_data(test_data&& o) noexcept : _data(std::exchange(o._data, nullptr)), _val(o._val) {}
    ~test_data() {
        if (_data != nullptr) {
            delete _data;
        }
    }

    unsigned long value() const {
        return _data == nullptr ? _val + 0x80000000 : *_data;
    }
};

template <> struct fmt::formatter<test_data> : fmt::formatter<string_view> {
    auto format(const test_data& d, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", d.value());
    }
};

using test_tree = tree<test_data>;

int main(int argc, char **argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("count", bpo::value<int>()->default_value(35642), "number of indices to fill the tree with")
        ("iters", bpo::value<int>()->default_value(5), "number of iterations")
        ("keys",  bpo::value<std::string>()->default_value("rand"), "how to generate keys (rand, asc, desc)")
        ("verb",  bpo::value<bool>()->default_value(false), "be verbose");

    return app.run(argc, argv, [&app] {
        auto count = app.configuration()["count"].as<int>();
        auto iters = app.configuration()["iters"].as<int>();
        auto ks = app.configuration()["keys"].as<std::string>();
        auto verb = app.configuration()["verb"].as<bool>();

        return seastar::async([count, iters, ks, verb] {
            auto t = std::make_unique<test_tree>();
            std::map<unsigned, test_data> oracle;

            unsigned col_size = 0;
            enum class validate {
                oracle, iterator, walk, lower_bound,
            };
            validate vld = validate::oracle;

            stress_config cfg;
            cfg.count = count;
            cfg.iters = 1;
            cfg.keys = ks;
            cfg.verb = verb;

            for (int i = 0; i < iters; i++) {
                stress_collection(cfg,
                    /* insert */ [&] (int key) {
                        t->emplace(key, key);
                        oracle.emplace(std::make_pair(key, key));
                        col_size++;
                    },
                    /* erase */ [&] (int key) {
                        t->erase(key);
                        oracle.erase(key);
                        col_size--;
                    },
                    /* validate */ [&] {
                        if (verb) {
                            compact_radix_tree::printer<test_data, unsigned>::show(*t);
                        }
                        if (vld == validate::oracle) {
                            for (auto&& d : oracle) {
                                test_data* td = t->get(d.first);
                                SCYLLA_ASSERT(td != nullptr);
                                SCYLLA_ASSERT(td->value() == d.second.value());
                            }
                            vld = validate::iterator;
                        } else if (vld == validate::iterator) {
                            unsigned nr = 0;
                            auto ti = t->begin();
                            while (ti != t->end()) {
                                SCYLLA_ASSERT(ti->value() == ti.key());
                                nr++;
                                ti++;
                                SCYLLA_ASSERT(nr <= col_size);
                            }
                            SCYLLA_ASSERT(nr == col_size);
                            vld = validate::walk;
                        } else if (vld == validate::walk) {
                            unsigned nr = 0;
                            t->walk([&nr, col_size] (unsigned idx, test_data& td) {
                                SCYLLA_ASSERT(idx == td.value());
                                nr++;
                                SCYLLA_ASSERT(nr <= col_size);
                                return true;
                            });
                            SCYLLA_ASSERT(nr == col_size);
                            vld = validate::lower_bound;
                        } else if (vld == validate::lower_bound) {
                            unsigned nr = 0;
                            unsigned idx = 0;
                            while (true) {
                                test_data* td = t->lower_bound(idx);
                                if (td == nullptr) {
                                    break;
                                }
                                SCYLLA_ASSERT(td->value() >= idx);
                                nr++;
                                idx = td->value() + 1;
                                SCYLLA_ASSERT(nr <= col_size);
                            }
                            SCYLLA_ASSERT(nr == col_size);
                            vld = validate::oracle;
                        }
                    },
                    /* step */ [] (stress_step step) { }
                );

                if (cfg.count < 4) {
                    cfg.count = count / 2;
                } else {
                    cfg.count /= 3;
                }
            }

            t->clear();
            oracle.clear();
        });
    });
}
