/*
 * Copyright (C) 2021 ScyllaDB
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

#pragma once

#include <vector>
#include <random>
#include <string>

struct stress_config {
    int count;
    int iters;
    std::string keys;
    bool verb;
};

enum class stress_step { before_insert, before_erase, iteration_finished };

template <typename Insert, typename Erase, typename Validate, typename Step>
void stress_collection(const stress_config& conf, Insert&& insert, Erase&& erase, Validate&& validate, Step&& step) {
    std::vector<int> keys;

    fmt::print("Inserting {:d} k:v pairs {:d} times\n", conf.count, conf.iters);

    for (int i = 0; i < conf.count; i++) {
        keys.push_back(i + 1);
    }

    std::random_device rd;
    std::mt19937 g(rd());

    if (conf.keys == "desc") {
        fmt::print("Reversing keys vector\n");
        std::reverse(keys.begin(), keys.end());
    }

    bool shuffle = conf.keys == "rand";
    if (shuffle) {
        fmt::print("Will shuffle keys each iteration\n");
    }


    for (auto rep = 0; rep < conf.iters; rep++) {
        if (conf.verb) {
            fmt::print("Iteration {:d}\n", rep);
        }

        if (shuffle) {
            std::shuffle(keys.begin(), keys.end(), g);
        }

        step(stress_step::before_insert);
        for (int i = 0; i < conf.count; i++) {
            if (conf.verb) {
                fmt::print("+++ {}\n", keys[i]);
            }

            insert(keys[i]);
            if (i % (i/1000 + 1) == 0) {
                validate();
            }

            seastar::thread::maybe_yield();
        }

        if (shuffle) {
            std::shuffle(keys.begin(), keys.end(), g);
        }

        step(stress_step::before_erase);
        for (int i = 0; i < conf.count; i++) {
            if (conf.verb) {
                fmt::print("--- {}\n", keys[i]);
            }

            erase(keys[i]);
            if ((conf.count-i) % ((conf.count-i)/1000 + 1) == 0) {
                validate();
            }

            seastar::thread::maybe_yield();
        }

        step(stress_step::iteration_finished);
    }
}
