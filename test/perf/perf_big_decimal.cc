/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *
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

#include "seastar/include/seastar/testing/perf_tests.hh"
#include <seastar/testing/test_runner.hh>

#include <random>

#include "utils/big_decimal.hh"
#include "test/lib/make_random_string.hh"

struct big_decimal_test {
    const sstring data_int = make_random_numeric_string(37);
    const sstring neg_data_int = "-" + make_random_numeric_string(37);
    const sstring data_fraction = make_random_numeric_string(37) + "." + make_random_numeric_string(19);
    const sstring neg_data_fraction = "-" + make_random_numeric_string(37) + "." + make_random_numeric_string(19);
    const sstring data_neg_exponent = make_random_numeric_string(18) + "E-" + make_random_numeric_string(7);
    const sstring neg_data_neg_exponent = "-" + make_random_numeric_string(18) + "E-" + make_random_numeric_string(7);
    const sstring neg_data_fraction_exponent = "-" + make_random_numeric_string(14) + "E" + make_random_numeric_string(6);
    const sstring neg_data_fraction_neg_exponent = "-" + make_random_numeric_string(14) + "E-" + make_random_numeric_string(6);
};

PERF_TEST_F(big_decimal_test, from_string) {
    perf_tests::do_not_optimize(big_decimal{data_int});
    perf_tests::do_not_optimize(big_decimal{neg_data_int});
    perf_tests::do_not_optimize(big_decimal{data_fraction});
    perf_tests::do_not_optimize(big_decimal{neg_data_fraction});
    perf_tests::do_not_optimize(big_decimal{data_neg_exponent});
    perf_tests::do_not_optimize(big_decimal{neg_data_neg_exponent});
    perf_tests::do_not_optimize(big_decimal{neg_data_fraction_exponent});
    perf_tests::do_not_optimize(big_decimal{neg_data_fraction_neg_exponent});
}

