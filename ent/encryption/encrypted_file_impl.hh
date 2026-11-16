/*
 * Copyright (C) 2018-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/file.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/noncopyable_function.hh>

#include "symmetric_key.hh"

namespace encryption {

class symmetric_key;

shared_ptr<file_impl> make_encrypted_file(file, ::shared_ptr<symmetric_key>);

using get_key_func = std::function<future<::shared_ptr<symmetric_key>>()>;

shared_ptr<file_impl> make_delayed_encrypted_file(file, size_t, get_key_func);

std::unique_ptr<data_sink_impl> make_encrypted_sink(data_sink, ::shared_ptr<symmetric_key>);

std::unique_ptr<data_source_impl> make_encrypted_source(data_source source, shared_ptr<symmetric_key> k);

// Reads the byte range [offset, offset + length) of an encrypted file.
using encrypted_range_reader = noncopyable_function<future<data_source>(uint64_t offset, uint64_t length)>;

// Returns a source of the byte range [offset, offset + len) of the plaintext
// of an encrypted file whose plaintext has the given size. Unlike
// make_encrypted_source(), the range may start anywhere in the file: the
// source reads, with a single call to read_encrypted(), just the encrypted
// blocks which contain the range - and if the range includes the file's last
// block, also the bytes after it, from which the size is known.
future<data_source> make_encrypted_range_source(shared_ptr<symmetric_key> k, uint64_t offset, uint64_t len,
        uint64_t size, encrypted_range_reader read_encrypted);

}
