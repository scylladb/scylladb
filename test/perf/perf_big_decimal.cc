/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/testing/perf_tests.hh>
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

