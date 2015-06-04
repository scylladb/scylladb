/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

/// \mainpage
///
/// Seastar is a high performance C++ application framework for high
/// concurrency server applications.
///
/// Please see:
///   - \ref future-module Documentation on futures and promises, which are
///          the seastar building blocks.
///   - \ref future-util Utililty functions for working with futures
///   - \ref memory-module Memory management
///   - \ref networking-module TCP/IP networking

#include "sstring.hh"
#include "future.hh"

// iostream.hh
template <class CharType> class input_stream;
template <class CharType> class output_stream;

// reactor.hh
class server_socket;
class connected_socket;
class socket_address;
class listen_options;

// file.hh
class file;
enum class open_flags;

// Networking API

/// \defgroup networking-module Networking
///
/// Seastar provides a simple networking API, backed by two
/// TCP/IP stacks: the POSIX stack, utilizing the kernel's
/// BSD socket APIs, and the native stack, implement fully
/// within seastar and able to drive network cards directly.
/// The native stack supports zero-copy on both transmit
/// and receive, and is implemented using seastar's high
/// performance, lockless sharded design.  The network stack
/// can be selected with the \c --network-stack command-line
/// parameter.

/// \addtogroup networking-module
/// @{

/// Listen for connections on a given port
///
/// Starts listening on a given address for incoming connections.
///
/// \param sa socket address to listen on
///
/// \return \ref server_socket object ready to accept connections.
///
/// \see listen(socket_address sa, listen_options opts)
server_socket listen(socket_address sa);

/// Listen for connections on a given port
///
/// Starts listening on a given address for incoming connections.
///
/// \param sa socket address to listen on
/// \param opts options controlling the listen operation
///
/// \return \ref server_socket object ready to accept connections.
///
/// \see listen(socket_address sa)
server_socket listen(socket_address sa, listen_options opts);

/// Establishes a connection to a given address
///
/// Attempts to connect to the given address.
///
/// \param sa socket address to connect to
///
/// \return a \ref connected_socket object, or an exception
future<connected_socket> connect(socket_address sa);

/// @}

// File API
future<file> open_file_dma(sstring name, open_flags flags);
future<file> open_directory(sstring name);
future<> make_directory(sstring name);
future<> remove_file(sstring pathname);

