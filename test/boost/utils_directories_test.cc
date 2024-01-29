/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#define BOOST_TEST_MODULE utils_directories_test

#include <boost/test/unit_test.hpp>

#include "db/config.hh"
#include "utils/directories.hh"

BOOST_AUTO_TEST_CASE(construction_from_config_with_default_values)
{
    // Given default constructed configuration.
    db::config cfg{};

    // When constructing directories object.
    utils::directories dirs{cfg};

    // Then paths point to a default workspace.
    BOOST_CHECK_EQUAL("/var/lib/scylla", dirs.get_work_dir());
    BOOST_CHECK_EQUAL("/var/lib/scylla/commitlog", dirs.get_commitlog_dir());
    BOOST_CHECK_EQUAL("/var/lib/scylla/commitlog/schema", dirs.get_schema_commitlog_dir());
    BOOST_CHECK_EQUAL("/var/lib/scylla/hints", dirs.get_hints_dir());
    BOOST_CHECK_EQUAL("/var/lib/scylla/view_hints", dirs.get_view_hints_dir());
    BOOST_CHECK_EQUAL("/var/lib/scylla/saved_caches", dirs.get_saved_caches_dir());

    const auto data_file_dirs = dirs.get_data_file_dirs();
    BOOST_REQUIRE_EQUAL(1u, data_file_dirs.size());
    BOOST_CHECK_EQUAL("/var/lib/scylla/data", data_file_dirs.at(0));
}

BOOST_AUTO_TEST_CASE(construction_from_config_with_custom_workdir)
{
    // Given a configuration with custom workspace directory.
    db::config cfg{};
    cfg.work_directory("/var/special/my_custom_work_dir");

    // When constructing directories object.
    utils::directories dirs{cfg};

    // Then paths point to a custom workspace.
    BOOST_CHECK_EQUAL("/var/special/my_custom_work_dir", dirs.get_work_dir());
    BOOST_CHECK_EQUAL("/var/special/my_custom_work_dir/commitlog", dirs.get_commitlog_dir());
    BOOST_CHECK_EQUAL("/var/special/my_custom_work_dir/commitlog/schema", dirs.get_schema_commitlog_dir());
    BOOST_CHECK_EQUAL("/var/special/my_custom_work_dir/hints", dirs.get_hints_dir());
    BOOST_CHECK_EQUAL("/var/special/my_custom_work_dir/view_hints", dirs.get_view_hints_dir());
    BOOST_CHECK_EQUAL("/var/special/my_custom_work_dir/saved_caches", dirs.get_saved_caches_dir());

    const auto data_file_dirs = dirs.get_data_file_dirs();
    BOOST_REQUIRE_EQUAL(1u, data_file_dirs.size());
    BOOST_CHECK_EQUAL("/var/special/my_custom_work_dir/data", data_file_dirs.at(0));
}

BOOST_AUTO_TEST_CASE(construction_from_config_with_custom_commitlog_dir)
{
    // Given a configuration with custom commitlog_directory.
    db::config cfg{};
    cfg.commitlog_directory("/var/specific/my_commitlog_directory");

    // When constructing directories object.
    utils::directories dirs{cfg};

    // Then all paths except the ones related to commitlog point to a default workspace.
    BOOST_CHECK_EQUAL("/var/lib/scylla", dirs.get_work_dir());
    BOOST_CHECK_EQUAL("/var/lib/scylla/hints", dirs.get_hints_dir());
    BOOST_CHECK_EQUAL("/var/lib/scylla/view_hints", dirs.get_view_hints_dir());
    BOOST_CHECK_EQUAL("/var/lib/scylla/saved_caches", dirs.get_saved_caches_dir());

    const auto data_file_dirs = dirs.get_data_file_dirs();
    BOOST_REQUIRE_EQUAL(1u, data_file_dirs.size());
    BOOST_CHECK_EQUAL("/var/lib/scylla/data", data_file_dirs.at(0));

    // And then the paths related to commitlog point to its directory.
    BOOST_CHECK_EQUAL("/var/specific/my_commitlog_directory", dirs.get_commitlog_dir());
    BOOST_CHECK_EQUAL("/var/specific/my_commitlog_directory/schema", dirs.get_schema_commitlog_dir());
}

BOOST_AUTO_TEST_CASE(construction_from_config_with_custom_schema_commitlog_dir)
{
    // Given a configuration with custom schema_commitlog_directory.
    db::config cfg{};
    cfg.schema_commitlog_directory("/var/specific/my_schema_commitlog_directory");

    // When constructing directories object.
    utils::directories dirs{cfg};

    // Then all paths except schema_commitlog point to a default workspace.
    BOOST_CHECK_EQUAL("/var/lib/scylla", dirs.get_work_dir());
    BOOST_CHECK_EQUAL("/var/lib/scylla/commitlog", dirs.get_commitlog_dir());
    BOOST_CHECK_EQUAL("/var/lib/scylla/hints", dirs.get_hints_dir());
    BOOST_CHECK_EQUAL("/var/lib/scylla/view_hints", dirs.get_view_hints_dir());
    BOOST_CHECK_EQUAL("/var/lib/scylla/saved_caches", dirs.get_saved_caches_dir());

    const auto data_file_dirs = dirs.get_data_file_dirs();
    BOOST_REQUIRE_EQUAL(1u, data_file_dirs.size());
    BOOST_CHECK_EQUAL("/var/lib/scylla/data", data_file_dirs.at(0));

    // And then schema_commitlog points to the custom dir.
    BOOST_CHECK_EQUAL("/var/specific/my_schema_commitlog_directory", dirs.get_schema_commitlog_dir());
}

BOOST_AUTO_TEST_CASE(construction_from_config_with_custom_data_file_dirs)
{
    // Given a configuration with custom data_file_directories.
    db::config cfg{};
    cfg.data_file_directories({"/var/data_1", "/var/another_data", "/super_data/here"});

    // When constructing directories object.
    utils::directories dirs{cfg};

    // Then all paths except data_file_directories point to a default workspace.
    BOOST_CHECK_EQUAL("/var/lib/scylla", dirs.get_work_dir());
    BOOST_CHECK_EQUAL("/var/lib/scylla/commitlog", dirs.get_commitlog_dir());
    BOOST_CHECK_EQUAL("/var/lib/scylla/commitlog/schema", dirs.get_schema_commitlog_dir());
    BOOST_CHECK_EQUAL("/var/lib/scylla/hints", dirs.get_hints_dir());
    BOOST_CHECK_EQUAL("/var/lib/scylla/view_hints", dirs.get_view_hints_dir());
    BOOST_CHECK_EQUAL("/var/lib/scylla/saved_caches", dirs.get_saved_caches_dir());

    // And then data_file_directories point to the custom dirs.
    const auto data_file_dirs = dirs.get_data_file_dirs();
    BOOST_REQUIRE_EQUAL(3u, data_file_dirs.size());
    BOOST_CHECK_EQUAL("/var/data_1", data_file_dirs.at(0));
    BOOST_CHECK_EQUAL("/var/another_data", data_file_dirs.at(1));
    BOOST_CHECK_EQUAL("/super_data/here", data_file_dirs.at(2));
}

BOOST_AUTO_TEST_CASE(construction_from_config_with_custom_hints_dir)
{
    // Given a configuration with custom hints_dir.
    db::config cfg{};
    cfg.hints_directory("/var/my_custom_hints_dir");

    // When constructing directories object.
    utils::directories dirs{cfg};

    // Then all paths except hints_dir point to a default workspace.
    BOOST_CHECK_EQUAL("/var/lib/scylla", dirs.get_work_dir());
    BOOST_CHECK_EQUAL("/var/lib/scylla/commitlog", dirs.get_commitlog_dir());
    BOOST_CHECK_EQUAL("/var/lib/scylla/commitlog/schema", dirs.get_schema_commitlog_dir());
    BOOST_CHECK_EQUAL("/var/lib/scylla/view_hints", dirs.get_view_hints_dir());
    BOOST_CHECK_EQUAL("/var/lib/scylla/saved_caches", dirs.get_saved_caches_dir());

    const auto data_file_dirs = dirs.get_data_file_dirs();
    BOOST_REQUIRE_EQUAL(1u, data_file_dirs.size());
    BOOST_CHECK_EQUAL("/var/lib/scylla/data", data_file_dirs.at(0));

    // And then hints_dir point to the custom dir.
    BOOST_CHECK_EQUAL("/var/my_custom_hints_dir", dirs.get_hints_dir());
}

BOOST_AUTO_TEST_CASE(construction_from_config_with_custom_view_hints_dir)
{
    // Given a configuration with custom view_hints_dir.
    db::config cfg{};
    cfg.view_hints_directory("/var/my_custom_view_hints_dir");

    // When constructing directories object.
    utils::directories dirs{cfg};

    // Then all paths except view_hints_dir point to a default workspace.
    BOOST_CHECK_EQUAL("/var/lib/scylla", dirs.get_work_dir());
    BOOST_CHECK_EQUAL("/var/lib/scylla/commitlog", dirs.get_commitlog_dir());
    BOOST_CHECK_EQUAL("/var/lib/scylla/commitlog/schema", dirs.get_schema_commitlog_dir());
    BOOST_CHECK_EQUAL("/var/lib/scylla/hints", dirs.get_hints_dir());
    BOOST_CHECK_EQUAL("/var/lib/scylla/saved_caches", dirs.get_saved_caches_dir());

    const auto data_file_dirs = dirs.get_data_file_dirs();
    BOOST_REQUIRE_EQUAL(1u, data_file_dirs.size());
    BOOST_CHECK_EQUAL("/var/lib/scylla/data", data_file_dirs.at(0));

    // And then view_hints_dir point to the custom dir.
    BOOST_CHECK_EQUAL("/var/my_custom_view_hints_dir", dirs.get_view_hints_dir());
}

BOOST_AUTO_TEST_CASE(construction_from_config_with_custom_saved_caches_dir)
{
    // Given a configuration with custom saved_caches_dir.
    db::config cfg{};
    cfg.saved_caches_directory("/var/caches/my_custom_cache");

    // When constructing directories object.
    utils::directories dirs{cfg};

    // Then all paths except saved_caches_dir point to a default workspace.
    BOOST_CHECK_EQUAL("/var/lib/scylla", dirs.get_work_dir());
    BOOST_CHECK_EQUAL("/var/lib/scylla/commitlog", dirs.get_commitlog_dir());
    BOOST_CHECK_EQUAL("/var/lib/scylla/commitlog/schema", dirs.get_schema_commitlog_dir());
    BOOST_CHECK_EQUAL("/var/lib/scylla/hints", dirs.get_hints_dir());
    BOOST_CHECK_EQUAL("/var/lib/scylla/view_hints", dirs.get_view_hints_dir());

    const auto data_file_dirs = dirs.get_data_file_dirs();
    BOOST_REQUIRE_EQUAL(1u, data_file_dirs.size());
    BOOST_CHECK_EQUAL("/var/lib/scylla/data", data_file_dirs.at(0));

    // And then saved_caches_dir points to the custom path.
    BOOST_CHECK_EQUAL("/var/caches/my_custom_cache", dirs.get_saved_caches_dir());
}

BOOST_AUTO_TEST_CASE(construction_from_config_with_multiple_custom_dirs)
{
    // Given a configuration with custom work_dir, commitlog_dir and saved_caches_dir.
    db::config cfg{};
    cfg.work_directory("/var/some/custom/workdir");
    cfg.commitlog_directory("/var/custom/commitlog_dir");
    cfg.saved_caches_directory("/var/caches/my_custom_cache");

    // When constructing directories object.
    utils::directories dirs{cfg};

    // Then all paths except commitlog and saved_caches_dir point to a custom workspace.
    BOOST_CHECK_EQUAL("/var/some/custom/workdir", dirs.get_work_dir());
    BOOST_CHECK_EQUAL("/var/some/custom/workdir/hints", dirs.get_hints_dir());
    BOOST_CHECK_EQUAL("/var/some/custom/workdir/view_hints", dirs.get_view_hints_dir());

    const auto data_file_dirs = dirs.get_data_file_dirs();
    BOOST_REQUIRE_EQUAL(1u, data_file_dirs.size());
    BOOST_CHECK_EQUAL("/var/some/custom/workdir/data", data_file_dirs.at(0));

    // And then paths related to commitlog points to the custom dir.
    BOOST_CHECK_EQUAL("/var/custom/commitlog_dir", dirs.get_commitlog_dir());
    BOOST_CHECK_EQUAL("/var/custom/commitlog_dir/schema", dirs.get_schema_commitlog_dir());

    // And then saved_caches_dir points to the custom path.
    BOOST_CHECK_EQUAL("/var/caches/my_custom_cache", dirs.get_saved_caches_dir());
}
