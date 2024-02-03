/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#define BOOST_TEST_MODULE core

#include <memory>
#include <vector>
#include <boost/test/unit_test.hpp>

#include "utils/anchorless_list.hh"

struct object : anchorless_list_base_hook<object> {
    int value;
};

BOOST_AUTO_TEST_CASE(test_achorless_list) {
    std::vector<std::unique_ptr<object>> objects;
    for (unsigned i = 0; i < 100; i++) {
        objects.emplace_back(std::make_unique<object>());
        objects.back()->value = i;
    }

    for (unsigned i = 50; i < 99; i++) {
        objects[i + 1]->insert_after(*objects[i]);
    }

    for (unsigned i = 50; i > 0; i--) {
        objects[i - 1]->insert_before(*objects[i]);
    }

    BOOST_REQUIRE(objects.front()->is_front());
    BOOST_REQUIRE(objects.back()->is_back());

    unsigned current = 0;
    for (auto&& v : objects.front()->all_elements()) {
        BOOST_REQUIRE_EQUAL(v.value, current++);
    }
    BOOST_REQUIRE_EQUAL(current, objects.size());

    current = 50;
    for (auto&& v : objects[50]->elements_from_this()) {
        BOOST_REQUIRE_EQUAL(v.value, current++);
    }
    BOOST_REQUIRE_EQUAL(current, objects.size());

    for (auto it = objects.begin(); it != objects.end(); ++it) {
        it = objects.erase(it);
    }

    current = 1;
    for (auto&& v : objects.front()->all_elements()) {
        BOOST_REQUIRE_EQUAL(v.value, current);
        current += 2;
    }
    BOOST_REQUIRE_EQUAL(current, objects.size() * 2 + 1);

    std::vector<std::unique_ptr<object>> moved_objects;
    for (auto& obj : objects) {
        moved_objects.emplace_back(std::make_unique<object>(std::move(*obj)));
    }
    objects.clear();

    current = 1;
    for (auto&& v : moved_objects.front()->all_elements()) {
        BOOST_REQUIRE_EQUAL(v.value, current);
        current += 2;
    }
    BOOST_REQUIRE_EQUAL(current, moved_objects.size() * 2 + 1);
}
