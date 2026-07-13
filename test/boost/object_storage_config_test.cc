/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <filesystem>

#include <boost/test/unit_test.hpp>
#include <fmt/format.h>
#include <yaml-cpp/yaml.h>

#include "test/lib/scylla_test_case.hh"
#include "test/lib/tmpdir.hh"
#include "db/object_storage_endpoint_param.hh"

using param = db::object_storage_endpoint_param;

static param decode_yaml(std::string_view yaml) {
    return param::decode(YAML::Load(std::string(yaml)));
}

// A locally-mounted POSIX path endpoint decodes into a posix param, and the
// stored path is lexically normalized (not canonicalized, so no filesystem
// access and no symlink resolution).
SEASTAR_THREAD_TEST_CASE(test_posix_endpoint_decode) {
    tmpdir dir;
    // The tmpdir path is already absolute and normal, so normalization leaves
    // it unchanged; decode() must not resolve symlinks or otherwise rewrite it.
    auto expected = dir.path();

    auto ep = decode_yaml(fmt::format(
        "name: {}\n"
        "type: posix\n",
        dir.path().native()));

    BOOST_REQUIRE(ep.is_posix_storage());
    BOOST_REQUIRE(!ep.is_s3_storage());
    BOOST_REQUIRE(!ep.is_gs_storage());

    BOOST_REQUIRE_EQUAL(ep.type(), param::posix_type);
    BOOST_REQUIRE(ep.is_storage_of_type(param::posix_type));
    BOOST_REQUIRE(!ep.is_storage_of_type(param::s3_type));
    BOOST_REQUIRE(!ep.is_storage_of_type(param::gs_type));

    BOOST_REQUIRE_EQUAL(ep.get_posix_storage().path, expected);
    BOOST_REQUIRE_EQUAL(ep.key(), expected.native());
    BOOST_REQUIRE_EQUAL(ep.to_json_string(),
        fmt::format(R"({{ "type": "posix", "path": "{}" }})", expected.native()));
}

// The path is lexically normalized, so different spellings of the same location
// map to the same endpoint: a trailing slash and a "." / ".." component must
// all resolve to one key so that "/mnt/backup" and "/mnt/backup/" are
// equivalent. Normalization is purely lexical, so the "sub" component of the
// ".." case need not exist on disk.
SEASTAR_THREAD_TEST_CASE(test_posix_endpoint_path_normalization) {
    tmpdir dir;

    auto plain = decode_yaml(fmt::format("name: {}\ntype: posix\n", dir.path().native()));
    auto trailing_slash = decode_yaml(fmt::format("name: {}/\ntype: posix\n", dir.path().native()));
    auto dot = decode_yaml(fmt::format("name: {}/.\ntype: posix\n", dir.path().native()));
    auto dotdot = decode_yaml(fmt::format("name: {}/sub/..\ntype: posix\n", dir.path().native()));

    BOOST_REQUIRE_EQUAL(plain.key(), dir.path().native());
    BOOST_REQUIRE_EQUAL(trailing_slash.key(), plain.key());
    BOOST_REQUIRE_EQUAL(dot.key(), plain.key());
    BOOST_REQUIRE_EQUAL(dotdot.key(), plain.key());

    // Equal keys imply equal params, so the endpoint map deduplicates them.
    BOOST_REQUIRE(plain == trailing_slash);
    BOOST_REQUIRE(plain == dot);
    BOOST_REQUIRE(plain == dotdot);
}

// A relative path is rejected: an object storage endpoint must be unambiguous
// and not depend on the process' working directory.
SEASTAR_THREAD_TEST_CASE(test_posix_endpoint_rejects_relative_path) {
    BOOST_REQUIRE_THROW(
        decode_yaml("name: relative/path\ntype: posix\n"),
        std::invalid_argument);
}

// An empty path is rejected (an empty path is not absolute).
SEASTAR_THREAD_TEST_CASE(test_posix_endpoint_rejects_empty_path) {
    BOOST_REQUIRE_THROW(
        decode_yaml("name: \"\"\ntype: posix\n"),
        std::invalid_argument);
}

// A path with characters that are valid in a POSIX path but must be escaped in
// JSON (a double quote and a backslash) is escaped in to_json_string(). The
// param is built directly rather than decoded from YAML so the characters are
// preserved verbatim.
SEASTAR_THREAD_TEST_CASE(test_posix_endpoint_json_escaping) {
    param ep{param::posix_storage{"/mnt/we\"ir\\d"}};

    BOOST_REQUIRE_EQUAL(ep.to_json_string(),
        R"json({ "type": "posix", "path": "/mnt/we\"ir\\d" })json");
}
