/*
 * Copyright (C) 2025-present ScyllaDB
 */
/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "proc_utils.hh"

#include <iostream>
#include <ranges>
#include <regex>

#include <seastar/core/seastar.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/sleep.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/api.hh>
#include <seastar/util/log.hh>

#include "utils/overloaded_functor.hh"
#include "test_utils.hh"

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

extern char **environ;

future<tests::proc::process_fixture> tests::proc::process_fixture::create(const std::filesystem::path& exec
    , const std::vector<std::string>& args
    , const std::vector<std::string>& env
    , handler_type stdout_handler
    , handler_type stderr_handler
    , bool inherit_env
) {
    experimental::spawn_parameters params;

    if (inherit_env) {
        // copy existing env
        for (auto** p = environ; *p != nullptr; ++p) {
            params.env.emplace_back(*p);
        }
    }

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

            auto i = buf.empty() ? str.size() : str.rfind('\n');
            if (i == std::string::npos) {
                co_return continue_consuming{};
            }

            auto range_view = std::string_view(str.data(), i);
            for (auto v : std::views::split(range_view, '\n')) {
                auto res = co_await handler(std::string_view(v));
                if (std::holds_alternative<stop_consuming<char>>(res.get())) {
                    co_return res;
                }
            }
            if (i < str.size() && str[i] == '\n') {
                ++i;
            }
            str.erase(str.begin(), str.begin() + i);
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

using namespace std::chrono_literals;
using namespace std::string_literals;

future<std::tuple<tests::proc::process_fixture, int>> tests::proc::start_docker_service(
    std::string_view name,
    std::string_view image,
    parse_service_callback stdout_parse,
    parse_service_callback stderr_parse,
    const std::vector<std::string>& docker_args,
    const std::vector<std::string>& image_args,
    int service_port)
{
    std::string container_name;

    auto exec = find_file_in_path("docker");
    if (exec.empty()) {
        exec = find_file_in_path("podman");
    }
    if (exec.empty()) {
        throw std::runtime_error("Could not find docker or podman.");
    }

    static int counter = 0;
    container_name = fmt::format("{}-{}-{}", name, ::getpid(), ++counter);

    // publish port ephemeral, allows parallel instances
    std::vector<std::string> params = {
        exec.string(),
        "run", "--rm", 
        "--name", container_name,
    };

    if (service_port == 0) {
        params.emplace_back("-P");
    } else {
        params.emplace_back("-p");
        params.emplace_back(std::to_string(service_port));
    }
    params.append_range(docker_args);
    params.emplace_back(image);
    params.append_range(image_args);

    struct in_use{};
    constexpr auto max_retries = 8;

    for (int retries = 0;; ++retries) {
        BOOST_TEST_MESSAGE(fmt::format("Will run {}", params));

        std::vector<std::string> env;
        int port = 0;

        promise<> ready_promise;
        auto ready_fut = ready_promise.get_future();

        auto create_handler = [](auto h_in, std::ostream& out) {
            auto h = process_fixture::create_copy_handler(out);
            promise<> ready_promise;
            future<> f = ready_promise.get_future();
            if (h_in) {
                h = [state = service_parse_state::cont, ready_promise = std::move(ready_promise), h = std::move(h), h_in = std::move(h_in)](std::string_view line) mutable -> future<consumption_result<char>> {
                    if (state == service_parse_state::cont) {
                        switch (state = h_in(line)) {
                            case service_parse_state::success: ready_promise.set_value(); break;
                            case service_parse_state::failed: ready_promise.set_exception(in_use{}); break;
                            default: 
                                // podman
                                if (line.find("Address already in use") != std::string::npos) {
                                    ready_promise.set_exception(in_use{});
                                    state = service_parse_state::failed;
                                }
                                // docker
                                if (line.find("port is already allocated") != std::string::npos) {
                                    ready_promise.set_exception(in_use{});
                                    state = service_parse_state::failed;
                                }
                                break;
                        }
                    }
                    return h(line);
                };
            } else {
                ready_promise.set_value();
            }
            return std::make_tuple(std::move(h), std::move(f));
        };

        auto [out_h, out_fut] = create_handler(std::move(stdout_parse), std::cout);
        auto [err_h, err_fut] = create_handler(std::move(stderr_parse), std::cerr);

        auto ps = co_await process_fixture::create(exec
            , params
            , env
            , std::move(out_h)
            , std::move(err_h)
        );

        std::exception_ptr p;
        bool retry = false;

        try {
            BOOST_TEST_MESSAGE("Waiting for process to laúnch...");
            // arbitrary timeout of 120s for the server to make some output. Very generous.
            // but since we (maybe) run docker, and might need to pull image, this can take
            // some time if we're unlucky.
            co_await with_timeout(std::chrono::steady_clock::now() + 120s, when_all(std::move(out_fut), std::move(err_fut)));
        } catch (in_use&) {
            retry = true;
            p = std::current_exception();
        } catch (...) {
            p = std::current_exception();
        }

        if (!p) {
            // query port
            promise<int> port_promise;
            auto port_future = port_promise.get_future();
            auto ps = co_await process_fixture::create(exec
                , { "container", "port", container_name }
                , {}
                , [done = false, port_promise = std::move(port_promise)](std::string_view line) mutable -> future<consumption_result<char>> {
                    if (done) {
                        co_return continue_consuming{};
                    }

                    static std::regex port_ex(R"foo(\d+\/\w+ -> [\w+\.\[\]\:]+:(\d+))foo");
                    std::cmatch m;
                    if (std::regex_match(line.begin(), line.end(), m, port_ex)) {
                        BOOST_TEST_MESSAGE(fmt::format("Got port line: {}", line));
                        done = true;
                        port_promise.set_value(std::stoi(m[1].str()));
                    }
                    co_return continue_consuming{};
                }
                , process_fixture::create_copy_handler(std::cerr)
            );

            try {
                BOOST_TEST_MESSAGE("Waiting for port query...");
                co_await ps.wait();
                port = co_await std::move(port_future);
            } catch (...) {
                p = std::current_exception();
            }
        }

        auto backoff = 0ms;
        auto con_retry = 0;
        while (!p) {
            if (backoff > 0ms) {
                co_await seastar::sleep(backoff);
            }
            try {
                BOOST_TEST_MESSAGE(fmt::format("Attempting to connect to {} at {}", name, port));
                // TODO: seastar does not have a connect with timeout. That would be helpful here. But alas...
                co_await with_timeout(std::chrono::steady_clock::now() + 20s, seastar::connect(socket_address(net::inet_address("127.0.0.1"), port)));
                BOOST_TEST_MESSAGE(fmt::format("{} up and available", name)); // debug print. Why not.
            } catch (std::system_error&) {
                retry = true;
                if (con_retry++ < max_retries) {
                    backoff = 100ms;
                    continue;
                }
                p = std::current_exception();
            } catch (...) {
                p = std::current_exception();
            }
            break;
        }

        if (p != nullptr) {
            BOOST_TEST_MESSAGE(fmt::format("Got exception starting {}: {}", name, p));
            ps.terminate();
            co_await ps.wait();
            if (!retry || retries >= max_retries) {
                std::rethrow_exception(p);
            }
            continue;
        }

        co_return std::make_tuple(std::move(ps), port);
    }
}
