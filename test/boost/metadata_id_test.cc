/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */


#include "concrete_types.hh"
#include "cql3/column_specification.hh"
#include "seastar/core/shared_ptr.hh"
#include "types/types.hh"
#include "utils/hashers.hh"
#include <boost/function/function_base.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <cstdint>
#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>

#include <vector>

#include <fmt/ranges.h>

#include "cql3/restrictions/statement_restrictions.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/util.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/test_utils.hh"

BOOST_AUTO_TEST_SUITE(metadata_id_test)

using namespace cql3;

namespace {

BOOST_AUTO_TEST_CASE(metadata_id_from_empty_metadata) {
    auto m = cql3::metadata{std::vector<lw_shared_ptr<column_specification>>{}};
    BOOST_REQUIRE_EQUAL(m.calculate_metadata_id().size(), 16);
}

bytes compute_metadata_id(std::vector<std::pair<sstring, shared_ptr<const abstract_type>>> columns, sstring ks = "ks", sstring cf = "cf") {
    std::vector<lw_shared_ptr<column_specification>> columns_specification;
    for (auto & column : columns) {
        columns_specification.push_back(make_lw_shared(column_specification(ks, cf, make_shared<column_identifier>(column.first, false), column.second)));
    }
    return cql3::metadata{columns_specification}.calculate_metadata_id();
}

BOOST_AUTO_TEST_CASE(metadata_id_with_different_keyspace_and_table) {
    auto h1 = compute_metadata_id({{"id", uuid_type}}, "ks1", "cf1");
    auto h2 = compute_metadata_id({{"id", uuid_type}}, "ks2", "cf2");

    BOOST_REQUIRE_EQUAL(h1, h2);
}

BOOST_AUTO_TEST_CASE(metadata_id_with_different_column_name) {
    auto h1 = compute_metadata_id({{"id", uuid_type}});
    auto h2 = compute_metadata_id({{"id2", uuid_type}});

    BOOST_REQUIRE_NE(h1, h2);
}

BOOST_AUTO_TEST_CASE(metadata_id_with_different_column_type) {
    auto h1 = compute_metadata_id({{"id", uuid_type}});
    auto h2 = compute_metadata_id({{"id", int32_type}});

    BOOST_REQUIRE_NE(h1, h2);
}

BOOST_AUTO_TEST_CASE(metadata_id_with_different_column_number) {
    auto h1 = compute_metadata_id({{"val1", int32_type}});
    auto h2 = compute_metadata_id({{"val1", int32_type}, {"val2", int32_type}});

    BOOST_REQUIRE_NE(h1, h2);
}

BOOST_AUTO_TEST_CASE(metadata_id_with_udt) {
    auto ks = "ks";
    auto cf = "cf";
    data_type udt1 = user_type_impl::get_instance(ks, cf, {"f1"}, {int32_type}, true);
    auto h1 = compute_metadata_id({{"val1", udt1}});

    data_type udt2 = user_type_impl::get_instance("ks", "cf", {"f1", "f2"}, {int32_type, int32_type}, true);
    auto h2 = compute_metadata_id({{"val1", udt2}});
    BOOST_REQUIRE_NE(h1, h2);

    data_type udt3 = user_type_impl::get_instance("ks", "cf", {"f2"}, {int32_type}, true);
    auto h3 = compute_metadata_id({{"val1", udt3}});
    BOOST_REQUIRE_NE(h1, h3);

    data_type udt4 = user_type_impl::get_instance("ks", "cf", {"f1"}, {float_type}, true);
    auto h4 = compute_metadata_id({{"val1", udt4}});
    BOOST_REQUIRE_NE(h1, h4);
}

BOOST_AUTO_TEST_SUITE_END()

}
