/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "bytes.hh"
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/print.hh>
#include <seastar/core/scattered_message.hh>
#include "redis/exceptions.hh"
#include "utils/fmt-compat.hh"

namespace redis {

class redis_message final {
    seastar::lw_shared_ptr<scattered_message<char>> _message;
public:
    redis_message() = delete;
    redis_message(const redis_message&) = delete;
    redis_message& operator=(const redis_message&) = delete;
    redis_message(redis_message&& o) noexcept : _message(std::move(o._message)) {}
    redis_message(lw_shared_ptr<scattered_message<char>> m) noexcept : _message(m) {}
    static seastar::future<redis_message> ok() {
        auto m = make_lw_shared<scattered_message<char>> ();
        m->append_static("+OK\r\n");
        return make_ready_future<redis_message>(m);
    }
    static seastar::future<redis_message> pong() {
        auto m = make_lw_shared<scattered_message<char>> ();
        m->append_static("+PONG\r\n");
        return make_ready_future<redis_message>(m);
    }
    static seastar::future<redis_message> zero() {
        auto m = make_lw_shared<scattered_message<char>> ();
        m->append_static(":0\r\n");
        return make_ready_future<redis_message>(m);
    }
    static seastar::future<redis_message> one() {
        auto m = make_lw_shared<scattered_message<char>> ();
        m->append_static(":1\r\n");
        return make_ready_future<redis_message>(m);
    }
    static seastar::future<redis_message> nil() {
        auto m = make_lw_shared<scattered_message<char>> ();
        m->append_static("$-1\r\n");
        return make_ready_future<redis_message>(m);
    }
    static seastar::future<redis_message> err() {
        return zero();
    }
    static seastar::future<redis_message> number(size_t n) {
        auto m = make_lw_shared<scattered_message<char>> ();
        m->append(fmt::format(":{}\r\n", n));
        return make_ready_future<redis_message>(m);
    }
    static seastar::future<redis_message> make_list_result(std::map<bytes, bytes>& list_result) {
        auto m = make_lw_shared<scattered_message<char>> ();
        m->append(fmt::format("*{}\r\n", list_result.size() * 2));
        for (auto r : list_result) {
            write_bytes(m, (bytes&)r.first);
            write_bytes(m, r.second);
        }
        return make_ready_future<redis_message>(m);
    }
    static seastar::future<redis_message> make_strings_result(bytes result) {
        auto m = make_lw_shared<scattered_message<char>> ();
        write_bytes(m, result);
        return make_ready_future<redis_message>(m);
    }
    static seastar::future<redis_message> unknown(const bytes& name) {
        return from_exception(make_message("-ERR unknown command '{}'\r\n", to_sstring(name)));
    }
    static seastar::future<redis_message> exception(const sstring& em) {
        auto m = make_lw_shared<scattered_message<char>> ();
        m->append(make_message("-ERR {}\r\n", em));
        return make_ready_future<redis_message>(m);
    }
    static seastar::future<redis_message> exception(const redis_exception& e) {
        return exception(e.what_message());
    }
    inline lw_shared_ptr<scattered_message<char>> message() { return _message; }
private:
    static seastar::future<redis_message> from_exception(sstring data) {
         auto m = make_lw_shared<scattered_message<char>> (); 
         m->append(data);
         return make_ready_future<redis_message>(m);
    }  
    template<typename... Args>
    static inline sstring make_message(const char* fmt, Args&&... args) noexcept {
        try {
            return fmt::format(fmt::runtime(fmt), std::forward<Args>(args)...);
        } catch (...) {
            return sstring();
        }   
    }
    static sstring to_sstring(const bytes& b) {
        return sstring(reinterpret_cast<const char*>(b.data()), b.size());
    }
    static void write_bytes(lw_shared_ptr<scattered_message<char>> m, bytes& b) {
        m->append(fmt::format("${}\r\n", b.size()));
        m->append(std::string_view(reinterpret_cast<const char*>(b.data()), b.size()));
        m->append_static("\r\n");
    }   
};

}
