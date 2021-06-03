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
#include <vector>
#include <random>
#include <string>
#include <iostream>
#include <fmt/core.h>
#include "utils/logalloc.hh"

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

std::ostream& operator<<(std::ostream& out, const test_data& d) {
    out << d.value();
    return out;
}

using test_tree = tree<test_data>;

int main(int argc, char **argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("count", bpo::value<int>()->default_value(132564), "number of indices to fill the tree with")
        ("iters", bpo::value<int>()->default_value(32), "number of iterations")
        ("verb",  bpo::value<bool>()->default_value(false), "be verbose");

    return app.run(argc, argv, [&app] {
        auto count = app.configuration()["count"].as<int>();
        auto iter = app.configuration()["iters"].as<int>();
        auto verb = app.configuration()["verb"].as<bool>();

        return seastar::async([count, iter, verb] {
            tree_pointer<test_tree> t;

            stress_config cfg;
            cfg.count = count;
            cfg.iters = 1;
            cfg.keys = "rand";
            cfg.verb = verb;

            unsigned col_size = 0;

            for (int i = 0; i < iter; i++) {
                stress_compact_collection(cfg,
                    /* insert */ [&] (int key) {
                        t->emplace(key, key);
                        col_size++;
                    },
                    /* erase */ [&] (int key) {
                        t->erase(key);
                        col_size--;
                    },
                    /* validate */ [&] {
                        if (verb) {
                            compact_radix_tree::printer<test_data, unsigned>::show(*t);
                        }

                        int nr = 0;
                        auto ti = t->begin();
                        while (ti != t->end()) {
                            assert(ti->value() == ti.key());
                            nr++;
                            ti++;
                        }
                        assert(nr == col_size);
                    },
                    /* clear */ [&] {
                        t->clear();
                        col_size = 0;
                    }
                );

                if (cfg.count < 4) {
                    cfg.count = count / 3;
                } else {
                    cfg.count /= 2;
                }
            }
        });
    });
}
