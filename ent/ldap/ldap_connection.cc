/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#define LDAP_DEPRECATED 1

#include "ldap_connection.hh"

#include <cerrno>
#include <cstring>
#include <fmt/format.h>
#include <stdexcept>
#include <string>

#include <seastar/core/seastar.hh>
#include <seastar/core/when_all.hh>
#include <seastar/util/later.hh>
#include <seastar/util/log.hh>

#include "seastarx.hh"

extern "C" {
// Declared in `ldap_pvt.h`, but this header is not usually installed by distributions even though
// it's considered public by upstream.
int ldap_init_fd(int, int, const char *, LDAP**);
}

namespace {

logger mylog{"ldap_connection"}; // `log` is taken by math.

constexpr int failure_code{-1}, success_code{0}; // LDAP return codes.

/// Helper function for Sockbuf_IO work.
ldap_connection* connection(Sockbuf_IO_Desc* sid) {
    return reinterpret_cast<ldap_connection*>(sid->sbiod_pvt);
}

/// Sockbuf_IO setup function for ldap_connection.
int ssbi_setup(Sockbuf_IO_Desc* sid, void* arg) {
    sid->sbiod_pvt = arg; // arg is ldap_connection, already set up.
    return success_code;
}

/// Sockbuf_IO remove function for ldap_connection.
int ssbi_remove(Sockbuf_IO_Desc* sid) {
    return success_code; // ldap_connection will be destructed by its owner.
}

void throw_if_failed(int status, const char* op, const ldap_connection& conn, int success = LDAP_SUCCESS) {
    if (status != success) {
        throw std::runtime_error(fmt::format("{} returned {}: {}", op, status, conn.get_error()));
    }
}

} // anonymous namespace

std::mutex ldap_connection::_global_init_mutex;

void ldap_connection::ldap_deleter::operator()(LDAP* ld) {
    mylog.trace("ldap_deleter: invoking unbind");
    int status = ldap_unbind(ld);
    if (status != LDAP_SUCCESS) {
        mylog.error("ldap_unbind failed with status {}", status);
    }
    mylog.trace("ldap_deleter done");
}

ldap_connection::ldap_connection(seastar::connected_socket&& socket) :
        _fd(file_desc::eventfd(0, EFD_NONBLOCK)) // Never ready for read, always ready for write.
    , _socket(std::move(socket))
    , _input_stream(_socket.input())
    , _output_stream(_socket.output())
    , _status(status::up)
    , _read_consumer(now())
    , _read_in_progress(false)
    , _outstanding_write(now())
    , _currently_polling(false) {
    // Proactively initiate Seastar read, before ldap_connection::read() is first called.
    read_ahead();

    // Libldap determines if we're ready for an sbi_write by polling _fd.  We're always ready to
    // accept an sbi_write because we chain writes as continuations.  Therefore, _fd always polls
    // ready to write.
    //
    // Libldap determines if we're ready for an sbi_read by first calling sbi_ctrl(DATA_READY); only
    // if that returns false does libldap then poll _fd.  We are ready to accept an sbi_read when a
    // prior Seastar read completed successfully and delivered some data into our _read_buffer.
    // Only sbi_ctrl() knows that, which is why _fd always polls NOT READY to read.
    //
    // NB: libldap never actually reads or writes directly to _fd.  It reads and writes through the
    // custom Sockbuf_IO we provide it.
    static constexpr int LDAP_PROTO_EXT = 4; // From ldap_pvt.h, which isn't always available.
    mylog.trace("constructor invoking ldap_init");
    LDAP* init_result;
    {
        std::lock_guard<std::mutex> global_init_lock{_global_init_mutex};
        throw_if_failed(ldap_init_fd(_fd.get(), LDAP_PROTO_EXT, nullptr, &init_result), "ldap_init_fd", *this);
    }
    _ldap.reset(init_result);
    static constexpr int opt_v3 = LDAP_VERSION3;
    throw_if_failed(
            ldap_set_option(_ldap.get(), LDAP_OPT_PROTOCOL_VERSION, &opt_v3), // Encouraged by ldap_set_option manpage.
            "ldap_set_option protocol version",
            *this,
            LDAP_OPT_SUCCESS);
    throw_if_failed(
            ldap_set_option(_ldap.get(), LDAP_OPT_RESTART, LDAP_OPT_ON), // Retry on EINTR, rather than return error.
            "ldap_set_option restart",
            *this,
            LDAP_OPT_SUCCESS);
    throw_if_failed(
            // Chasing referrals with this setup results in libldap crashing.
            ldap_set_option(_ldap.get(), LDAP_OPT_REFERRALS, LDAP_OPT_OFF),
            "ldap_set_option no referrals",
            *this,
            LDAP_OPT_SUCCESS);

    Sockbuf* sb;
    throw_if_failed(ldap_get_option(_ldap.get(), LDAP_OPT_SOCKBUF, &sb), "ldap_get_option", *this, LDAP_OPT_SUCCESS);
    mylog.trace("constructor adding Sockbuf_IO");
    throw_if_failed(
            ber_sockbuf_add_io(sb, const_cast<Sockbuf_IO*>(&seastar_sbio), LBER_SBIOD_LEVEL_PROVIDER, this),
            "ber_sockbuf_add_io",
            *this);
    mylog.trace("constructor done");
}

future<> ldap_connection::close() {
    if (_status == status::down) {
        mylog.error("close called while connection is down");
        return make_exception_future<>(std::runtime_error("double close() of ldap_connection"));
    }
    _ldap.reset(); // Sends one last message to the server before reclaiming memory.
    return when_all(
            _read_consumer.finally([this] { return _input_stream.close(); })
            .handle_exception([] (std::exception_ptr ep) {
                mylog.error("Seastar input stream closing failed: {}", ep);
            }),
            _outstanding_write.finally([this] { return _output_stream.close(); })
            .handle_exception([] (std::exception_ptr ep) {
                mylog.error("Seastar output stream closing failed: {}", ep);
            })
    ).discard_result().then([this] {
        shutdown();
        return make_ready_future<>();
    });
}

future<ldap_msg_ptr> ldap_connection::await_result(int msgid) {
    mylog.trace("await_result({})", msgid);

    if (_status != status::up) {
        mylog.error("await_result({}) error: connection is not up", msgid);
        ldap_abandon_ext(get_ldap(), msgid, /*sctrls=*/nullptr, /*cctrls=*/nullptr);
        return make_exception_future<ldap_msg_ptr>(std::runtime_error("ldap_connection status set to error"));
    }

    try {
        return _msgid_to_promise[msgid].get_future();
    } catch (...) {
        auto ex = std::current_exception();
        mylog.error("await_result({}) error: {}", msgid, ex);
        // Tell LDAP to abandon this msgid, since we failed to register it.
        ldap_abandon_ext(get_ldap(), msgid, /*sctrls=*/nullptr, /*cctrls=*/nullptr);
        return make_exception_future<ldap_msg_ptr>(ex);
    }
}

future<ldap_msg_ptr> ldap_connection::await_result(int status, int msgid) {
    mylog.trace("await_result({}, {})", status, msgid);
    if (status == LDAP_SUCCESS) {
        return await_result(msgid);
    } else {
        const char* err = ldap_err2string(status);
        mylog.trace("await_result({}, {}) reporting error {}", status, msgid, err);
        return make_exception_future<ldap_msg_ptr>(
                std::runtime_error(fmt::format("ldap operation error: {}", err)));
    }
}

future<ldap_msg_ptr> ldap_connection::simple_bind(const char *who, const char *passwd) {
    mylog.trace("simple_bind({})", who);
    if (_status != status::up) {
        mylog.error("simple_bind({}) punting, connection down", who);
        return make_exception_future<ldap_msg_ptr>(
                std::runtime_error("bind operation attempted on a closed ldap_connection"));
    }
    const int msgid = ldap_simple_bind(get_ldap(), who, passwd);
    if (msgid == -1) {
        const auto err = get_error();
        mylog.error("ldap simple bind error: {}", err);
        return make_exception_future<ldap_msg_ptr>(
                std::runtime_error(fmt::format("ldap simple bind error: {}", err)));
    } else {
        mylog.trace("simple_bind: msgid {}", msgid);
        return await_result(msgid);
    }
}

future<ldap_msg_ptr> ldap_connection::search(
        char *base,
        int scope,
        char *filter,
        char *attrs[],
        int attrsonly,
        LDAPControl **serverctrls,
        LDAPControl **clientctrls,
        struct timeval *timeout,
        int sizelimit) {
    mylog.trace("search");
    int msgid;
    if (_status != status::up) {
        mylog.error("search punting, connection down");
        return make_exception_future<ldap_msg_ptr>(
                std::runtime_error("search operation attempted on a closed ldap_connection"));
    }
    const int status = ldap_search_ext(
            get_ldap(), base, scope, filter, attrs, attrsonly, serverctrls, clientctrls, timeout, sizelimit, &msgid);
    return await_result(status, msgid);
}

sstring ldap_connection::get_error() const {
    int result_code;
    int status = ldap_get_option(get_ldap(), LDAP_OPT_RESULT_CODE, reinterpret_cast<void*>(&result_code));
    if (status != LDAP_OPT_SUCCESS) {
        mylog.error("ldap_get_option returned {}", status);
        return "error description unavailable";
    }
    return ldap_err2string(result_code);
}

int ldap_connection::sbi_ctrl(Sockbuf_IO_Desc* sid, int opt, void* arg) noexcept {
    mylog.debug("sbi_ctrl({}/{}, {}, {})", static_cast<void*>(sid), sid->sbiod_pvt, opt, arg);
    auto conn = connection(sid);
    switch (opt) {
    case LBER_SB_OPT_DATA_READY:
        return !conn->_read_buffer.empty()
               || conn->_status != status::up; // Let sbi_read proceed and report status; otherwise, LDAP loops forever.
    case LBER_SB_OPT_GET_FD:
        if (conn->_status == status::down) {
            errno = ENOTCONN;
            return -1;
        }
        *reinterpret_cast<ber_socket_t*>(arg) = conn->_fd.get();
        return 1;
    }
    return 0;
}

ber_slen_t ldap_connection::sbi_read(Sockbuf_IO_Desc* sid, void* buffer, ber_len_t size) noexcept {
    mylog.trace("sbi_read {}/{}", static_cast<const void*>(sid), static_cast<const void*>(sid->sbiod_pvt));
    try {
        return connection(sid)->read(reinterpret_cast<char*>(buffer), size);
    } catch (...) {
        mylog.error("Unexpected error while reading: {}", std::current_exception());
        return failure_code;
    }
}

ber_slen_t ldap_connection::sbi_write(Sockbuf_IO_Desc* sid, void* buffer, ber_len_t size) noexcept {
    mylog.trace("sbi_write {}/{}", static_cast<const void*>(sid), static_cast<const void*>(sid->sbiod_pvt));
    try {
        return connection(sid)->write(reinterpret_cast<const char*>(buffer), size);
    } catch (...) {
        mylog.error("Unexpected error while writing: {}", std::current_exception());
        return failure_code;
    }
}

int ldap_connection::sbi_close(Sockbuf_IO_Desc* sid) noexcept {
    mylog.debug("sbi_close {}/{}", static_cast<const void*>(sid), static_cast<const void*>(sid->sbiod_pvt));
    // Leave actual closing to the owner of *this.  Note sbi_close() will be invoked during
    // ldap_unbind(), which also calls sbi_write() to convey one last message to the server.  We
    // remain open here, to try to communicate that message.
    return success_code;
}

const Sockbuf_IO ldap_connection::seastar_sbio{
    // Strictly speaking, designated initializers like this are not in the standard, but they're
    // supported by both major compilers we use.
    .sbi_setup = &ssbi_setup,
    .sbi_remove = &ssbi_remove,
    .sbi_ctrl = &ldap_connection::sbi_ctrl,
    .sbi_read = &ldap_connection::sbi_read,
    .sbi_write = &ldap_connection::sbi_write,
    .sbi_close = &ldap_connection::sbi_close
};

void ldap_connection::read_ahead() {
    if (_read_in_progress) { // Differs from _read_consumer.available(), because handle_exception adds a continuation.
        mylog.warn("read_ahead called while a prior Seastar read is already in progress");
        return;
    }
    if (_input_stream.eof()) {
        mylog.error("read_ahead encountered EOF");
        set_status(status::eof);
        return;
    }
    mylog.trace("read_ahead");
    _read_in_progress = true;
    mylog.trace("read_ahead invoking socket read");
    _read_consumer = _input_stream.read().then([this] (temporary_buffer<char> b) {
        if (b.empty()) {
            mylog.debug("read_ahead received empty buffer; assuming EOF");
            set_status(status::eof);
            return;
        }
        mylog.trace("read_ahead received data of size {}", b.size());
        if (!_read_buffer.empty()) { // Shouldn't happen; read_ahead's purpose is to replenish empty _read_buffer.
            mylog.error("read_ahead dropping {} unconsumed bytes", _read_buffer.size());
        }
        _read_buffer = std::move(b);
        _read_in_progress = false;
        poll_results();
    }).handle_exception([this] (std::exception_ptr ep) {
        mylog.error("Seastar read failed: {}", ep);
        set_status(status::err);
    });
    mylog.trace("read_ahead done");
}

ber_slen_t ldap_connection::write(char const* b, ber_len_t size) {
    mylog.trace("write({})", size);
    switch (_status) {
    case status::err:
        mylog.trace("write({}) reporting error", size);
        errno = ECONNRESET;
        return -1;
    case status::down:
        mylog.trace("write({}) invoked after shutdown", size);
        errno = ENOTCONN;
        return -1;
    case status::up:
    case status::eof:
        ; // Proceed.
    }
    _outstanding_write = _outstanding_write.then([this, buf = temporary_buffer(b, size)] () mutable {
        if (_status != status::up) {
            return make_ready_future<>();
        }
        mylog.trace("write invoking socket write");
        return _output_stream.write(std::move(buf)).then([this] {
            // Sockbuf_IO doesn't seem to have the notion of flushing the stream, so we flush after
            // every write.
            mylog.trace("write invoking flush");
            return _output_stream.flush();
        }).handle_exception([this] (std::exception_ptr ep) {
            mylog.error("Seastar write failed: {}", ep);
            set_status(status::err);
        });
    });
    mylog.trace("write({}) done, status={}", size, _status);
    return _status == status::up ? size : -1; // _status can be err here if _outstanding_write threw.
}

ber_slen_t ldap_connection::read(char* b, ber_len_t size) {
    mylog.trace("read({})", size);
    switch (_status) {
    case status::eof:
        mylog.trace("read({}) reporting eof", size);
        return 0;
    case status::err:
        mylog.trace("read({}) reporting error", size);
        errno = ECONNRESET;
        return -1;
    case status::down:
        mylog.trace("read({}) invoked after shutdown", size);
        errno = ENOTCONN;
        return -1;
    case status::up:
        ; // Proceed.
    }
    if (_read_buffer.empty()) { // Can happen because libldap doesn't always wait for data to be ready.
        mylog.trace("read({}) found empty read buffer", size);
        // Don't invoke read_ahead() here; it was already invoked as soon as _read_buffer was
        // drained.  In fact, its Seastar read might have actually completed, and the buffer is
        // about to be filled by a waiting continuation.  We DON'T want another read_ahead() before
        // that data is consumed.
        errno = EWOULDBLOCK;
        return 0;
    }
    const auto byte_count = std::min(_read_buffer.size(), size);
    std::copy_n(_read_buffer.begin(), byte_count, b);
    _read_buffer.trim_front(byte_count);
    if (_read_buffer.empty()) {
        mylog.trace("read({}) replenishing buffer", size);
        read_ahead();
    }
    mylog.trace("read({}) returning {}", size, byte_count);
    return byte_count;
}

void ldap_connection::shutdown()  {
    mylog.trace("shutdown");
    set_status(status::down);
    mylog.trace("shutdown: shutdown input");
    _socket.shutdown_input();
    mylog.trace("shutdown: shutdown output");
    _socket.shutdown_output();
    mylog.trace("shutdown done");
}

void ldap_connection::poll_results() {
    mylog.trace("poll_results");
    if (!_ldap) { // Could happen during close(), which unbinds.
        mylog.debug("poll_results: _ldap is null, punting");
        return;
    }
    if (_currently_polling) {
        // This happens when ldap_result() calls read_ahead() and runs its inner continuation immediately.
        mylog.debug("poll_results: _currently_polling somewhere up the call-stack, punting");
        return;
    }

    // Ensure that _currently_polling is true until we return.
    class flag_guard {
        bool& _flag;
      public:
        flag_guard(bool& flag) : _flag(flag) { flag = true; }
        ~flag_guard() { _flag = false; }
    } guard(_currently_polling);

    LDAPMessage *result;
    while (!_read_buffer.empty() && _status == status::up) {
        static timeval zero_duration{};
        mylog.trace("poll_results: {} in buffer, invoking ldap_result", _read_buffer.size());
        const int status = ldap_result(get_ldap(), LDAP_RES_ANY, /*all=*/1, &zero_duration, &result);
        if (status > 0) {
            ldap_msg_ptr result_ptr(result);
            const int id = ldap_msgid(result);
            mylog.trace("poll_results: ldap_result returned status {}, id {}", status, id);
            const auto found = _msgid_to_promise.find(id);
            if (found == _msgid_to_promise.end()) {
                mylog.error("poll_results: got valid result for unregistered id {}, dropping it", id);
                ldap_msgfree(result);
            } else {
                found->second.set_value(std::move(result_ptr));
                _msgid_to_promise.erase(found);
            }
        } else if (status < 0) {
            mylog.error("poll_results: ldap_result returned status {}, error: {}", status, get_error());
            set_status(status::err);
        }
    }
    mylog.trace("poll_results done");
}

void ldap_connection::set_status(ldap_connection::status s) {
    _status = s;
    if (s != status::up) {
        mylog.trace("set_status: signal result-waiting futures");
        for (auto& e : _msgid_to_promise) {
            e.second.set_exception(std::runtime_error("ldap_connection status set to error"));
        }
        _msgid_to_promise.clear();
    }
}

ldap_reuser::ldap_reuser(sequential_producer<ldap_reuser::conn_ptr>::factory_t&& f)
    : _make_conn(std::move(f)), _reaper(now()) {
}

void ldap_reuser::reap(conn_ptr& conn) {
    if (!conn) {
        return;
    }
    if (auto p = conn.release()) { // Safe to close, other fibers are done with it.
        _reaper = _reaper.then([p = std::move(p)] () mutable {
            return p->close().then_wrapped([p = std::move(p)] (future<> fut) {
                if (fut.failed()) {
                    mylog.warn("failure closing dead ldap_connection: {}", fut.get_exception());
                }
                /* p disposed here */
            });
        });
    }
}

future<> ldap_reuser::stop() {
    reap(_conn);
    return std::move(_reaper);
}
