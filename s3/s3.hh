/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/file.hh>
#include <seastar/net/api.hh>
#include <seastar/core/shared_ptr.hh>

namespace s3 {

using namespace seastar;

class connection {
    seastar::connected_socket _cs;
    seastar::socket _s;
    seastar::output_stream<char> _out;
    seastar::input_stream<char> _in;
    bool _closed = false; // close() was called
public:
    connection(seastar::connected_socket cs, seastar::socket s)
        : _cs(std::move(cs))
        , _s(std::move(s))
        , _out(_cs.output())
        , _in(_cs.input())
    { }
public:
    seastar::connected_socket& socket() { return _cs; }
    seastar::output_stream<char>& out() { return _out; }
    seastar::input_stream<char>& in() { return _in; }
    future<> close();
    bool closed() const { return _closed; }
};

// Has a scope independent of other objects.
// Can outlive connection_factory.
// Can be destroyed without prior call to ->close().
class connection_ptr {
    lw_shared_ptr<connection> _ptr;
public:
    using value_type = connection;
    connection_ptr() = default;
    connection_ptr(connection_ptr&&) = default;
    connection_ptr(const connection_ptr&) = default;
    connection_ptr(lw_shared_ptr<connection> ptr) : _ptr(std::move(ptr)) {}
    ~connection_ptr();
    connection_ptr& operator=(connection_ptr&& other) noexcept {
        if (this != &other) {
            this->~connection_ptr();
            new (this) connection_ptr(std::move(other));
        }
        return *this;
    }
    connection_ptr& operator=(const connection_ptr& other) noexcept {
        if (this != &other) {
            this->~connection_ptr();
            new (this) connection_ptr(other);
        }
        return *this;
    }
    connection& operator*() { return *_ptr; }
    connection* operator->() { return &*_ptr; }
    explicit operator bool() const { return bool(_ptr); }
};

class connection_factory {
public:
    // Returns a new connection which can be used exclusively by the caller.
    // It's not returned by later connect() calls until take_back().
    // Returned connection_ptr can outlive this instance.
    virtual future<connection_ptr> connect() = 0;

    // Signals that a given connection is no longer used
    // and can be returned by later connect() calls.
    virtual void take_back(connection_ptr) {};

    // Returns the name of the S3 host which connect() connects to.
    // The name should be globally-recognizable by the network stack
    // as it is put in the HTTP "Host" header.
    virtual sstring host_name() = 0;
};

using connection_factory_ptr = seastar::shared_ptr<connection_factory>;

// Connects over plain TCP/IPv4. No SSL.
connection_factory_ptr make_basic_connection_factory(seastar::sstring host, uint16_t port);

// S3 API client.
// Methods can be invoked concurrently.
class client {
public:
    // Removes the object.
    // The path does not have to outlive this call.
    virtual future<> remove_file(const seastar::sstring& path) = 0;

    // Initiates an upload of an object.
    // The returned data_sink is to be used to upload data.
    // The upload ends when the data_sink is closed.
    // FIXME: In-progress uploads may be still alive on the server side if this process crashes
    // We need a way to track them and abort stale uploads after a crash.
    // The path does not have to outlive this call.
    virtual future<data_sink> upload(const seastar::sstring& path) = 0;

    // Reads part of the object.
    // The path does not have to outlive this call.
    virtual future<temporary_buffer<char>> get_object(const seastar::sstring& path, uint64_t offset, size_t length) = 0;

    // Get the size of the object.
    // The path does not have to outlive this call.
    virtual future<uint64_t> get_size(const seastar::sstring& path) = 0;

    // Adapts an S3 object to a seastar::file
    // The returned file shares ownership of this client instance.
    // The path does not have to outlive this call.
    virtual future<file> open(const seastar::sstring& path) = 0;
};

using client_ptr = seastar::shared_ptr<client>;

client_ptr make_client(connection_factory_ptr);

}
