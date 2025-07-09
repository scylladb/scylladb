/*
 * Copyright (C) 2025-present ScyllaDB
 */
/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "proc_utils.hh"

#include <iostream>
#include <ranges>

#include <seastar/core/seastar.hh>
#include <seastar/core/gate.hh>

#include "utils/overloaded_functor.hh"

using namespace seastar;

class tests::proc::process_fixture::impl {
public:
    experimental::process _process;
    gate _gate;
    impl(experimental::process process)
        : _process(std::move(process))
    {}
};

tests::proc::process_fixture::process_fixture(std::unique_ptr<impl> i) 
    : _impl(std::move(i))
{}
tests::proc::process_fixture::process_fixture(process_fixture&&) noexcept = default;
tests::proc::process_fixture::~process_fixture() = default;

future<tests::proc::process_fixture> tests::proc::process_fixture::create(const std::filesystem::path& exec
    , const std::vector<std::string>& args
    , const std::vector<std::string>& env
    , handler_type stdout_handler
    , handler_type stderr_handler
) {
    experimental::spawn_parameters params;

    std::copy(args.begin(), args.end(), std::back_inserter(params.argv));
    std::copy(env.begin(), env.end(), std::back_inserter(params.env));

    process_fixture res(std::make_unique<impl>(co_await experimental::spawn_process(exec, params)));

    auto& proc = res._impl->_process;
    auto& gate = res._impl->_gate;

    auto wrap_line_handler = [](line_handler handler) {
        return [handler = std::move(handler), str = std::string{}](buffer_type buf) mutable -> future<consumption_result<char>> {
            auto off = str.size();
            str.reserve(off + buf.size());
            str.append(buf.begin(), buf.end());

            auto i = std::find(str.begin() + off, str.end(), '\n');
            if (i != str.end() || buf.empty()) {
                std::string_view v(str.begin(), i);
                auto res = co_await handler(v);
                str.erase(str.begin(), i == str.end() ? i : i + 1);
                co_return res;
            }
            co_return continue_consuming{};
        };
    };

    auto wrap_handler = [&](handler_type handler, input_stream<char> (experimental::process::*func)()) {
        auto h = std::visit(overloaded_functor(
            [&](std::monostate) -> stream_handler { return {}; },
            [&](line_handler&& h) -> stream_handler { return wrap_line_handler(std::move(h)); },
            [&](stream_handler&& h) -> stream_handler { return std::move(h); }
        ), std::move(handler));

        if (h) {
            auto g = gate.hold();
            auto strm = std::make_unique<input_stream<char>>(std::invoke(func, proc));
            auto& sr = *strm;
            (void)sr.consume(std::move(h)).finally([g = std::move(g), strm = std::move(strm)] {});
        }
    };

    wrap_handler(std::move(stdout_handler), &experimental::process::cout);
    wrap_handler(std::move(stderr_handler), &experimental::process::cerr);

    co_return res;
}

tests::proc::process_fixture::line_handler tests::proc::process_fixture::create_copy_handler(std::ostream& os) {
    return [&os](std::string_view v) mutable -> future<consumption_result<char>> {
        os << v << std::endl;
        co_return continue_consuming{};
    };
}

future<experimental::process::wait_status> tests::proc::process_fixture::wait() {
    co_await _impl->_gate.close();
    co_return co_await _impl->_process.wait();
}

void tests::proc::process_fixture::terminate() {
    _impl->_process.terminate();
}
void tests::proc::process_fixture::kill() {
    _impl->_process.kill();
}

input_stream<char> tests::proc::process_fixture::cout() {
    return _impl->_process.cout();
}

input_stream<char> tests::proc::process_fixture::cerr() {
    return _impl->_process.cerr();
}

output_stream<char> tests::proc::process_fixture::cin() {
    return _impl->_process.cin();
}

namespace fs = std::filesystem;

fs::path tests::proc::find_file_in_path(std::string_view name, 
    const std::vector<fs::path>& path_preprend,
    const std::vector<fs::path>& path_append
) {
    static auto get_var = [](const char* name) {
        auto res = std::getenv(name);
        return res ? std::string(res) : std::string{};
    };
    static const std::vector<fs::path> system_paths = [] {
        std::vector<fs::path> res;
        // std::views::concat not yet in clang on my fedora.
        for (auto p : std::views::split(get_var("PATH"), ':')) {
            res.emplace_back(std::string_view(p));
        }
        res.emplace_back(get_var("PWD"));
        res.emplace_back("/usr/bin");
        res.emplace_back("/usr/local/bin");
        return res;
    }();

    for (auto& paths : { path_preprend, system_paths, path_append }) {
        for (auto& p : paths) {
            auto test = p / name;
            if (fs::exists(test) && !fs::is_directory(test)) {
                return test;
            }
        }
    }
    return fs::path();
}

