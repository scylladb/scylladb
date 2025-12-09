/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "test/lib/boost_test_tree_lister.hh"

#include <boost/test/framework.hpp>
#include <boost/test/tree/traverse.hpp>
#include <boost/test/unit_test_suite.hpp>

#include <fmt/core.h>

namespace {

/// Traverse the test tree and collect information about
/// its structure and the tests.
///
/// The output is going to be in the JSON format.
/// For more details, see the implementation of
/// `boost_test_tree_lister`.
void print_boost_tests() {
    namespace but = boost::unit_test;

    but::framework::finalize_setup_phase();

    boost_test_tree_lister traverser;
    but::traverse_test_tree(but::framework::master_test_suite().p_id, traverser, true);

    fmt::print("{}", traverser.get_result());
}

/// --------
/// Examples
/// --------
///
/// # This will NOT list the tests because Boost.Test
/// # will interpret it as an argument to the framework.
/// $ ./path/to/my/test/exec --list_json_content
///
/// # This will NOT list the tests because Boost.Test requires
/// # that all non-Boost.Test arguments be provided AFTER
/// # a `--` sequence (cf. example below).
/// $ ./path/to/my/test/exec list_json_content
///
/// # This will NOT list the tests because Boost.Test because
/// # the option simply doesn't match the exepected one.
/// $ ./path/to/my/test/exec list_json_content
///
/// # This DOES work and DOES what we expect, i.e. it lists the tests.
/// $ ./path/to/my/test/exec -- --list_json_content
bool list_tests(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
        std::string_view option = argv[i];
        if (option == "--list_json_content") {
            return true;
        }
    }

    return false;
}

struct boost_tree_lister_injector {
    boost_tree_lister_injector() {
        const auto& master_suite = boost::unit_test::framework::master_test_suite();
        /// The arguments here don't include Boost.Test-specific arguments.
        /// Those present correspond to the path to the binary and options
        /// specified for the "end-code".
        ///
        /// --------
        /// Examples
        /// --------
        /// $ ./path/to/my/test/exec my_custom_arg
        /// Arguments: [<path>, "my_custom_arg"]
        ///
        /// $ ./path/to/my/test/exec -- my_custom_arg
        /// Arguments: [<path>, "my_custom_arg"]
        ///
        /// $ ./path/to/my/test/exec --auto_start_dbg=0 -- my_custom_arg
        /// Arguments: [<path>, "my_custom_arg"]
        ///
        /// $ ./path/to/my/test/exec --auto_start_dbg=0 my_custom_arg
        /// Arguments: [<path>, "my_custom_arg"]
        ///
        /// ------------------------------------------
        /// Interaction with some Boost.Test arguments
        /// ------------------------------------------
        ///
        /// Note, however, that some Boost.Test options may prevent us
        /// from accessing this code. For instance, if the user runs
        ///
        /// $ ./path/to/my/test/exec --list_content -- my_custom_arg
        ///
        /// then Boost.Test will immediately move to its own code and not
        /// execute this one (because it's only called by a global fixture).
        auto&& [argc, argv] = std::make_pair(master_suite.argc, master_suite.argv);

        if (list_tests(argc, argv)) {
            print_boost_tests();

            // At this point, it's impossible to prevent Boost.Test
            // from executing the tests it collected. This is all
            // we can do (at least without writing a lot more code.
            // I don't know if it would still be possible to avoid it).
            std::exit(0);
        }
    }
};

} // anonymous namespace

BOOST_GLOBAL_FIXTURE(boost_tree_lister_injector);
