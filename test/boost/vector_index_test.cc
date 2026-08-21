/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>
#include "cql3/column_identifier.hh"
#include "cql3/statements/index_target.hh"
#include "exceptions/exceptions.hh"
#include "index/vector_index.hh"
#include "utils/rjson.hh"

using namespace secondary_index;
using namespace cql3;

using statements::index_target;

BOOST_AUTO_TEST_SUITE(vector_index_test)

namespace {

::shared_ptr<index_target> make_single(const sstring& name) {
    auto col = ::make_shared<cql3::column_identifier>(name, true);
    return ::make_shared<index_target>(col, index_target::target_type::regular_values);
}

::shared_ptr<index_target> make_multi(const std::vector<sstring>& names) {
    std::vector<::shared_ptr<column_identifier>> cols;
    cols.reserve(names.size());
    for (const auto& n : names) {
        cols.push_back(::make_shared<column_identifier>(n, true));
    }
    return ::make_shared<index_target>(std::move(cols), index_target::target_type::regular_values);
}

} // namespace

BOOST_AUTO_TEST_CASE(serialize_targets_empty_targets_throws) {
    std::vector<::shared_ptr<index_target>> targets;
    BOOST_CHECK_THROW(vector_index::serialize_targets(targets), exceptions::invalid_request_exception);
}

BOOST_AUTO_TEST_CASE(serialize_targets_single_pk_only_target_throws) {
    std::vector<::shared_ptr<index_target>> targets = {make_multi({"p1", "p2"})};
    BOOST_CHECK_THROW(vector_index::serialize_targets(targets), exceptions::invalid_request_exception);
}


BOOST_AUTO_TEST_CASE(serialize_targets_single_column) {
    std::vector<::shared_ptr<index_target>> targets = {
            make_single("v"),
    };
    BOOST_CHECK_EQUAL(vector_index::serialize_targets(targets), "v");
}

BOOST_AUTO_TEST_CASE(serialize_targets_with_filtering_columns) {
    std::vector<::shared_ptr<index_target>> targets = {
            make_single("v"),
            make_single("f1"),
            make_single("f2"),
    };

    const auto result = vector_index::serialize_targets(targets);
    const auto json = rjson::parse(result);

    BOOST_REQUIRE(json.IsObject());

    BOOST_REQUIRE(!rjson::find(json, "pk"));

    auto tc = rjson::find(json, "tc");
    BOOST_REQUIRE(tc);
    BOOST_CHECK_EQUAL(rjson::to_string_view(*tc), "v");

    const auto* fc = rjson::find(json, "fc");
    BOOST_REQUIRE(fc);
    BOOST_REQUIRE(fc->IsArray());
    BOOST_REQUIRE_EQUAL(fc->Size(), 2);
    BOOST_CHECK_EQUAL(sstring(rjson::to_string_view(fc->GetArray()[0])), "f1");
    BOOST_CHECK_EQUAL(sstring(rjson::to_string_view(fc->GetArray()[1])), "f2");
}

BOOST_AUTO_TEST_CASE(serialize_targets_local_index) {
    std::vector<::shared_ptr<index_target>> targets = {
            make_multi({"p1", "p2"}),
            make_single("v"),
    };

    const auto result = vector_index::serialize_targets(targets);
    const auto json = rjson::parse(result);

    BOOST_REQUIRE(json.IsObject());

    const auto* pk = rjson::find(json, "pk");
    BOOST_REQUIRE(pk);
    BOOST_REQUIRE(pk->IsArray());
    BOOST_REQUIRE_EQUAL(pk->Size(), 2);
    BOOST_CHECK_EQUAL(sstring(rjson::to_string_view(pk->GetArray()[0])), "p1");
    BOOST_CHECK_EQUAL(sstring(rjson::to_string_view(pk->GetArray()[1])), "p2");

    auto tc = rjson::find(json, "tc");
    BOOST_REQUIRE(tc);
    BOOST_CHECK_EQUAL(rjson::to_string_view(*tc), "v");

    BOOST_REQUIRE(!rjson::find(json, "fc"));
}

BOOST_AUTO_TEST_CASE(serialize_targets_local_index_with_filtering_columns) {
    std::vector<::shared_ptr<index_target>> targets = {
            make_multi({"p1", "p2"}),
            make_single("v"),
            make_single("f1"),
            make_single("f2"),
    };

    const auto result = vector_index::serialize_targets(targets);
    const auto json = rjson::parse(result);

    BOOST_REQUIRE(json.IsObject());

    const auto* pk = rjson::find(json, "pk");
    BOOST_REQUIRE(pk);
    BOOST_REQUIRE(pk->IsArray());
    BOOST_REQUIRE_EQUAL(pk->Size(), 2);
    BOOST_CHECK_EQUAL(sstring(rjson::to_string_view(pk->GetArray()[0])), "p1");
    BOOST_CHECK_EQUAL(sstring(rjson::to_string_view(pk->GetArray()[1])), "p2");

    auto tc = rjson::find(json, "tc");
    BOOST_REQUIRE(tc);
    BOOST_CHECK_EQUAL(rjson::to_string_view(*tc), "v");

    const auto* fc = rjson::find(json, "fc");
    BOOST_REQUIRE(fc);
    BOOST_REQUIRE(fc->IsArray());
    BOOST_REQUIRE_EQUAL(fc->Size(), 2);
    BOOST_CHECK_EQUAL(sstring(rjson::to_string_view(fc->GetArray()[0])), "f1");
    BOOST_CHECK_EQUAL(sstring(rjson::to_string_view(fc->GetArray()[1])), "f2");
}

BOOST_AUTO_TEST_CASE(serialize_targets_multi_column_target_throws) {
    std::vector<::shared_ptr<index_target>> targets = {make_multi({"p1"}), make_multi({"c1"})};
    BOOST_CHECK_THROW(vector_index::serialize_targets(targets), exceptions::invalid_request_exception);
}

BOOST_AUTO_TEST_CASE(serialize_targets_multi_column_filtering_throws) {
    std::vector<::shared_ptr<index_target>> targets = {make_single("v"), make_multi({"c1"})};
    BOOST_CHECK_THROW(vector_index::serialize_targets(targets), exceptions::invalid_request_exception);
}

BOOST_AUTO_TEST_CASE(get_target_column_returns_column_with_uppercase_letters_from_escaped_string) {
    std::vector<::shared_ptr<index_target>> targets = {make_single("MyCol")};
    const auto serialized = vector_index::serialize_targets(targets);

    BOOST_CHECK_EQUAL(serialized, "\"MyCol\"");
    BOOST_CHECK_EQUAL(vector_index::get_target_column(serialized), "MyCol");
}

BOOST_AUTO_TEST_CASE(get_target_column_returns_column_with_quotes_from_escaped_string) {
    std::vector<::shared_ptr<index_target>> targets = {make_single("a\"b")};
    const auto serialized = vector_index::serialize_targets(targets);

    BOOST_CHECK_EQUAL(serialized, "\"a\"\"b\"");
    BOOST_CHECK_EQUAL(vector_index::get_target_column(serialized), "a\"b");
}

BOOST_AUTO_TEST_CASE(get_target_column_returns_column_with_uppercase_letters_from_json) {
    std::vector<::shared_ptr<index_target>> targets = {
            make_single("MyCol"),
            make_single("f1"),
    };
    const auto serialized = vector_index::serialize_targets(targets);

    BOOST_CHECK_EQUAL(vector_index::get_target_column(serialized), "MyCol");
}

BOOST_AUTO_TEST_CASE(get_target_column_returns_column_with_quotes_from_json) {
    std::vector<::shared_ptr<index_target>> targets = {
            make_single("a\"b"),
            make_single("f1"),
    };
    const auto serialized = vector_index::serialize_targets(targets);

    BOOST_CHECK_EQUAL(vector_index::get_target_column(serialized), "a\"b");
}


BOOST_AUTO_TEST_SUITE_END()
