/*
 * Copyright (C) 2025-present ScyllaDB
 */
/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <filesystem>
#include <vector>
#include <variant>
#include <iosfwd>

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/util/process.hh>

namespace tests::proc {
    using namespace seastar;

    std::filesystem::path find_file_in_path(std::string_view name, 
        const std::vector<std::filesystem::path>& path_preprend = {},
        const std::vector<std::filesystem::path>& path_append = {}
    );

    class process_fixture {
        class impl;
        std::unique_ptr<impl> _impl;

        process_fixture(std::unique_ptr<impl>);
    public:
        using buffer_type = temporary_buffer<char>;
        using handler_result = consumption_result<char>;
        using stream_handler = noncopyable_function<future<handler_result>(buffer_type)>;
        using line_handler = noncopyable_function<future<handler_result>(std::string_view)>;
        using handler_type = std::variant<std::monostate, stream_handler, line_handler>;

        process_fixture(process_fixture&&) noexcept;
        ~process_fixture();

        static future<process_fixture> create(const std::filesystem::path& exec
            , const std::vector<std::string>& args
            , const std::vector<std::string>& env = {}
            , handler_type stdout_handler = {}
            , handler_type stderr_handler = {}
        );

        static line_handler create_copy_handler(std::ostream&);

        using wait_status = seastar::experimental::process::wait_status;

        future<wait_status> wait();
        void terminate();
        void kill();

        input_stream<char> cout();
        input_stream<char> cerr();
        output_stream<char> cin();
    };
}
