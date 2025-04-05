/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/seastar.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/util/process.hh>
#include <seastar/util/short_streams.hh>

#include "utils/rjson.hh"
#include "utils/exceptions.hh"
#include "exceptions.hh"
#include "azure_cli_credentials.hh"

namespace azure {

azure_cli_credentials::azure_cli_credentials(const sstring& logctx)
    : credentials(logctx)
{}

access_token azure_cli_credentials::make_token(const rjson::value& json, const resource_type& resource_uri) {
    auto token = rjson::get<std::string>(json, "accessToken");
    auto expires_on = rjson::get<uint64_t>(json, "expires_on");
    return { token, std::chrono::system_clock::from_time_t(expires_on), resource_uri };
}

std::vector<sstring> azure_cli_credentials::make_env() {
    std::vector<sstring> vec;
    vec.reserve(3);

    auto path = std::getenv("PATH");
    auto home = std::getenv("HOME");
    auto azure_config_dir = std::getenv("AZURE_CONFIG_DIR");

    const auto DEFAULT_PATH = "/usr/bin:/usr/local/bin";
    if (path && *path) {
        vec.emplace_back(seastar::format("PATH={}:{}", path, DEFAULT_PATH));
    } else {
        vec.emplace_back(seastar::format("PATH={}", DEFAULT_PATH));
    }
    if (home) {
        vec.emplace_back(seastar::format("HOME={}", home));
    }
    if (azure_config_dir) {
        vec.emplace_back(seastar::format("AZURE_CONFIG_DIR={}", azure_config_dir));
    }
    return vec;
}

future<> azure_cli_credentials::refresh(const resource_type& resource_uri) {
    try {
        co_await do_refresh(resource_uri);
    } catch (auth_error&) {
        throw;
    } catch (...) {
        std::throw_with_nested(auth_error(fmt::format("{}", std::current_exception())));
    }
}

future<> azure_cli_credentials::do_refresh(const resource_type& resource_uri) {
    // This timeout is purely a safeguard for badly-behaved CLIs.
    // It is not expected to be reached under normal circumstances.
    const auto timeout = std::chrono::seconds(5);

    az_creds_logger.debug("[{}] Refreshing token", *this);
    using namespace seastar::experimental;
    constexpr char SHELL[] = "/bin/sh";
    const auto azcmd = seastar::format("az account get-access-token --resource {}", resource_uri);
    spawn_parameters params = {
        .argv = { SHELL, "-c", azcmd },
        .env = make_env(),
    };
    auto process = co_await spawn_process(SHELL, params);

    auto cout = process.cout();
    auto cerr = process.cerr();
    sstring output;
    sstring error;

    co_await with_timeout(timer<>::clock::now() + timeout, [&] -> future<> {
        auto read_cout = [&] -> future<> { output = co_await util::read_entire_stream_contiguous(cout); };
        auto read_cerr = [&] -> future<> { error = co_await util::read_entire_stream_contiguous(cerr); };
        co_await when_all_succeed(read_cout, read_cerr);
    }()).handle_exception([&] (std::exception_ptr ep) {
        if (try_catch<timed_out_error>(ep)) {
            az_creds_logger.debug("[{}] Azure CLI not responding. Killing it forcefully...", *this);
            process.kill();
        }
        std::rethrow_exception(ep);
    }).finally(seastar::coroutine::lambda([&] -> future<> {
        auto wstatus = co_await process.wait();
        auto* exited = std::get_if<process::wait_exited>(&wstatus);
        auto* signaled = std::get_if<experimental::process::wait_signaled>(&wstatus);
        if (exited && exited->exit_code != EXIT_SUCCESS) {
            az_creds_logger.debug("[{}] Azure CLI failed with exit status ({}): {}", *this, exited->exit_code, error);
            throw auth_error(seastar::format("Azure CLI failed with exit status ({})", exited->exit_code));
        }
        if (signaled) {
            az_creds_logger.debug("[{}] Azure CLI was terminated by signal: {} ({})", *this, signaled->terminating_signal, strsignal(signaled->terminating_signal));
            throw auth_error(seastar::format("Azure CLI was terminated by signal: {} ({})", signaled->terminating_signal, strsignal(signaled->terminating_signal)));
        }
    }));
    _token = make_token(rjson::parse(output), resource_uri);
}

}