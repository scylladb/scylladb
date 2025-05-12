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

    auto PATH = std::getenv("PATH");
    auto HOME = std::getenv("HOME");
    auto AZURE_CONFIG_DIR = std::getenv("AZURE_CONFIG_DIR");

    const auto DEFAULT_PATH = "/usr/bin:/usr/local/bin";
    if (PATH && *PATH) {
        vec.emplace_back(seastar::format("PATH={}{}{}", PATH, *PATH ? ":" : "", DEFAULT_PATH));
    } else {
        vec.emplace_back(seastar::format("PATH={}", DEFAULT_PATH));
    }
    if (HOME) {
        vec.emplace_back(seastar::format("HOME={}", HOME));
    }
    if (AZURE_CONFIG_DIR) {
        vec.emplace_back(seastar::format("AZURE_CONFIG_DIR={}", AZURE_CONFIG_DIR));
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

    log_debug("Refreshing token");
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
            log_debug("Azure CLI not responding. Killing it forcefully...");
            process.kill();
        }
        std::rethrow_exception(ep);
    }).finally([&] -> future<> {
        auto wstatus = co_await process.wait();
        auto* exited = std::get_if<process::wait_exited>(&wstatus);
        auto* signaled = std::get_if<experimental::process::wait_signaled>(&wstatus);
        if (exited && exited->exit_code != EXIT_SUCCESS) {
            log_debug("Azure CLI failed with exit status ({}): {}", exited->exit_code, error);
            throw auth_error(seastar::format("Azure CLI failed with exit status ({})", exited->exit_code));
        }
        if (signaled) {
            log_debug("Azure CLI was terminated by signal: {} ({})", signaled->terminating_signal, strsignal(signaled->terminating_signal));
            throw auth_error(seastar::format("Azure CLI was terminated by signal: {} ({})", signaled->terminating_signal, strsignal(signaled->terminating_signal)));
        }
    });
    token = make_token(rjson::parse(output), resource_uri);
}

}