/*
 * Copyright (C) 2015 ScyllaDB
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
#include <boost/test/unit_test.hpp>
#include <seastar/util/variant_utils.hh>

template<typename Comparator, typename... T>
class total_order_check {
    using element = boost::variant<T...>;
    std::vector<std::vector<element>> _data;
    Comparator _cmp;
private:
    static int sgn(int x) {
        return (x > 0) - (x < 0);
    }

    // All in left are expect to compare with all in right with expected_order result
    void check_order(const std::vector<element>& left, const std::vector<element>& right, int order) {
        for (const element& left_e : left) {
            for (const element& right_e : right) {
                seastar::visit(left_e, [&] (auto&& a) {
                    seastar::visit(right_e, [&] (auto&& b) {
                        BOOST_TEST_MESSAGE(sprint("cmp(%s, %s) == %d", a, b, order));
                        auto r = _cmp(a, b);
                        auto actual = this->sgn(r);
                        if (actual != order) {
                            BOOST_FAIL(sprint("Expected cmp(%s, %s) == %d, but got %d", a, b, order, actual));
                        }
                    });
                });
            }
        }
    }
public:
    total_order_check(Comparator c = Comparator()) : _cmp(std::move(c)) {}

    total_order_check& next(element e) {
        _data.push_back(std::vector<element>());
        return equal_to(e);
    }

    total_order_check& equal_to(element e) {
        _data.back().push_back(e);
        return *this;
    }

    void check() {
        for (unsigned i = 0; i < _data.size(); ++i) {
            for (unsigned j = 0; j < _data.size(); ++j) {
                check_order(_data[i], _data[j], sgn(int(i) - int(j)));
            }
        }
    }
};
