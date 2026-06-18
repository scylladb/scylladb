/*
 * Copyright (C) 2026 ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "audit/audit_stdout_storage_helper.hh"

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <memory>

#include <sys/stat.h>
#include <unistd.h>

#include <seastar/core/coroutine.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/smp.hh>

#include <fmt/chrono.h>

#include "cql3/query_processor.hh"

namespace cql3 {

class query_processor;

}

namespace audit {

namespace {

future<seastar::output_stream<char>> make_stdout_output_stream() {
    // Classify the *inherited* stdout (STDOUT_FILENO) rather than a re-opened
    // handle, so the decision reflects what the process was actually launched
    // with.  fstat() here is only used to inspect the fd type; no I/O is done on
    // the raw descriptor.
    struct stat st;
    if (::fstat(STDOUT_FILENO, &st) != 0) {
        const int err = errno;
        throw audit_exception(seastar::format(
                "stdout audit backend: cannot stat stdout (fd {}): {}",
                STDOUT_FILENO, std::strerror(err)));
    }

    // Only stream-oriented descriptors are supported: pipes/FIFOs and character
    // devices (e.g. a terminal).  This is exactly what a container runtime
    // (Kubernetes/OpenShift/Docker) hands to the process as stdout, which is the
    // backend's target environment.
    //
    // We deliberately re-open /dev/stdout instead of writing STDOUT_FILENO
    // directly.  The pipe output stream requires the fd to be non-blocking, but
    // O_NONBLOCK lives on the open file description, which the inherited fd 1
    // shares with Scylla's own logger (Scylla logs to stdout by default).
    // Flipping O_NONBLOCK on fd 1 would make the logger's blocking writes fail
    // with EAGAIN, so we open a fresh description via /dev/stdout whose
    // O_NONBLOCK flag is private to the audit stream.  Re-opening /dev/stdout
    // yields a new fd to the same pipe (or re-opens the same character device).
    //
    // Caveat: re-opening can fail in a non-root container when the stdout pipe
    // was created by the container runtime under a different UID; the process
    // can write the already-open fd 1 but may not be permitted to reopen
    // /proc/self/fd/1.  Surface that as an actionable error instead of a cryptic
    // EACCES so operators know how to react.
    if (S_ISFIFO(st.st_mode) || S_ISCHR(st.st_mode)) {
        try {
            co_return co_await seastar::make_pipe_output_stream(std::filesystem::path("/dev/stdout"));
        } catch (const std::system_error& e) {
            throw audit_exception(seastar::format(
                    "stdout audit backend: cannot re-open /dev/stdout ({}). This can "
                    "happen in a non-root container when stdout is a pipe owned by "
                    "the container runtime under a different UID. Run Scylla as the "
                    "owner of the stdout pipe, or use the 'syslog'/'table' audit "
                    "backend instead.",
                    e.what()));
        }
    }

    // Regular files and sockets are rejected explicitly:
    //  - a regular file cannot be turned into a private non-blocking stream
    //    without truncating it: seastar's file output stream writes from offset
    //    zero, so it would overwrite existing stdout content; Scylla's logger
    //    would also race with audit on the same file;
    //  - a unix/inet socket cannot be re-opened through /proc/self/fd at all.
    // In both cases the correct setup is to let the container runtime own stdout
    // (a pipe), or to use the 'syslog'/'table' audit backends instead.
    throw audit_exception(seastar::format(
            "stdout audit backend: unsupported stdout type (mode {:#o}); stdout "
            "must be a pipe or character device (as provided by a container "
            "runtime). Redirecting Scylla's stdout to a regular file or socket "
            "is not supported for the 'stdout' audit backend; use the 'syslog' "
            "or 'table' backend instead.",
            static_cast<unsigned>(st.st_mode & S_IFMT)));
}

/// Writes audit messages to stdout, serialised on shard 0.
///
/// All audit writes are funnelled to shard 0 via smp::submit_to() and then
/// serialised through a semaphore so that concurrent events from different
/// shards don't interleave on the wire.
///
/// The backing stream is a seastar pipe output stream (pollable_fd-backed) over
/// a freshly opened /dev/stdout; see make_stdout_output_stream() for why only
/// pipe/character-device stdout is supported.
struct shard0_stdout_writer {
    seastar::semaphore semaphore{1};
    seastar::gate gate;
    seastar::output_stream<char> stream;

    shard0_stdout_writer(seastar::output_stream<char> os)
            : stream(std::move(os)) {
    }

    future<> do_write(sstring msg) {
        auto units = co_await get_units(semaphore, 1);
        co_await stream.write(msg);
        co_await stream.flush();
    }

    future<> write(std::string_view msg) {
        sstring line(msg);
        return seastar::with_gate(gate, [this, line = std::move(line)] () mutable {
            return do_write(std::move(line));
        });
    }

    future<> stop() {
        co_await gate.close();
        co_await stream.close();
    }
};

thread_local std::unique_ptr<shard0_stdout_writer> local_writer;

/// Collapse newlines so each audit record stays on a single log line.
sstring flatten_stdout_field(std::string_view value) {
    std::string result;
    result.reserve(value.size());
    for (char c : value) {
        result.push_back((c == '\n' || c == '\r') ? ' ' : c);
    }
    return sstring(result);
}

sstring make_stdout_audit_message(socket_address node_ip,
                                         std::string_view category,
                                         std::string_view cl,
                                         bool error,
                                         std::string_view keyspace,
                                         std::string_view query,
                                         socket_address client_ip,
                                         std::string_view table,
                                         std::string_view username) {
    const auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    tm time;
    localtime_r(&now, &time);

    return seastar::format(
            "{:%h %e %T} scylla-audit: "
            R"(node="{}", category="{}", cl="{}", error="{}", keyspace="{}", query="{}", client_ip="{}", table="{}", username="{}")"
            "\n",
            time,
            node_ip,
            flatten_stdout_field(category),
            flatten_stdout_field(cl),
            error ? "true" : "false",
            flatten_stdout_field(keyspace),
            flatten_stdout_field(query),
            client_ip,
            flatten_stdout_field(table),
            flatten_stdout_field(username));
}

} // anonymous namespace

future<> audit_stdout_storage_helper::stdout_send_helper(sstring msg) {
    try {
        auto foreign_msg = seastar::make_foreign(std::make_unique<sstring>(std::move(msg)));
        co_await seastar::smp::submit_to(0, [msg = std::move(foreign_msg)] () mutable {
            if (!local_writer) {
                throw std::logic_error("stdout audit backend is not started");
            }
            return local_writer->write(std::string_view(msg->data(), msg->size()));
        });
    } catch (const std::exception& e) {
        auto error_msg = seastar::format(
            "Stdout audit backend failed (writing a message to stdout resulted in {}).",
            e);
        logger.error("{}", error_msg);
        throw audit_exception(std::move(error_msg));
    }
}

audit_stdout_storage_helper::audit_stdout_storage_helper(cql3::query_processor& /*qp*/, service::migration_manager& /*mm*/) {
}

audit_stdout_storage_helper::~audit_stdout_storage_helper() = default;

future<> audit_stdout_storage_helper::start(const db::config& /*cfg*/) {
    if (this_shard_id() == 0) {
        auto os = co_await make_stdout_output_stream();
        local_writer = std::make_unique<shard0_stdout_writer>(std::move(os));
        logger.info("Initializing stdout audit backend.");
    }
    co_return;
}

future<> audit_stdout_storage_helper::stop() {
    if (this_shard_id() == 0 && local_writer) {
        co_await local_writer->stop();
        local_writer.reset();
    }
    co_return;
}

future<> audit_stdout_storage_helper::write(
        audit_sink_set sinks, const audit_info* ai, socket_address node_ip, socket_address client_ip, std::optional<db::consistency_level> cl, const sstring& username, bool error) {
    if (!sinks.contains(audit_sink::stdout)) {
        co_return;
    }
    auto cl_str = cl ? format("{}", *cl) : sstring("");
    co_return co_await stdout_send_helper(make_stdout_audit_message(
            node_ip,
            ai->category_string(),
            cl_str,
            error,
            ai->keyspace(),
            ai->query(),
            client_ip,
            ai->table(),
            username));
}

future<> audit_stdout_storage_helper::write_login(audit_sink_set sinks, const sstring& username, socket_address node_ip, socket_address client_ip, bool error) {
    if (!sinks.contains(audit_sink::stdout)) {
        co_return;
    }
    co_return co_await stdout_send_helper(make_stdout_audit_message(
            node_ip,
            "AUTH",
            "",
            error,
            "",
            "",
            client_ip,
            "",
            username));
}

} // namespace audit
