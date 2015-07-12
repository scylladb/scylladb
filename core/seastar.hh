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
///   - \ref fileio-module File Input/Output
///   - \ref smp-module Multicore support
///   - \ref fiber-module Utilities for managing loosely coupled chains of
///          continuations, also known as fibers
///   - \ref thread-module Support for traditional threaded execution

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
/// can be selected with the \c \--network-stack command-line
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

/// \defgroup fileio-module File Input/Output
///
/// Seastar provides a file API to deal with persistent storage.
/// Unlike most file APIs, seastar offers unbuffered file I/O
/// (similar to, and based on, \c O_DIRECT).  Unbuffered I/O means
/// that the application is required to do its own caching, but
/// delivers better performance if this caching is done correctly.
///
/// For random I/O or sequential unbuffered I/O, the \ref file
/// class provides a set of methods for reading, writing, discarding,
/// or otherwise manipulating a file.  For buffered sequential I/O,
/// see \ref make_file_input_stream() and \ref make_file_output_stream().

/// \addtogroup fileio-module
/// @{

/// Opens or creates a file.  The "dma" in the name refers to the fact
/// that data transfers are unbuffered and uncached.
///
/// \param name  the name of the file to open or create
/// \param flags various flags controlling the open process
/// \return a \ref file object, as a future
///
/// \note
/// The file name is not guaranteed to be stable on disk, unless the
/// containing directory is sync'ed.
///
/// \relates file
future<file> open_file_dma(sstring name, open_flags flags);

/// Opens a directory.
///
/// \param name name of the directory to open
///
/// \return a \ref file object representing a directory.  The only
///    legal operations are \ref file::list_directory(),
///    \ref file::fsync(), and \ref file::close().
///
/// \relates file
future<file> open_directory(sstring name);

/// Creates a new directory.
///
/// \param name name of the directory to create
///
/// \note
/// The directory is not guaranteed to be stable on disk, unless the
/// containing directory is sync'ed.
future<> make_directory(sstring name);

/// Ensures a directory exists
///
/// Checks whether a directory exists, and if not, creates it.  Only
/// the last component of the directory name is created.
///
/// \param name name of the directory to potentially create
///
/// \note
/// The directory is not guaranteed to be stable on disk, unless the
/// containing directory is sync'ed.
future<> touch_directory(sstring name);

/// Recursively ensures a directory exists
///
/// Checks whether each component of a directory exists, and if not, creates it.
///
/// \param name name of the directory to potentially create
/// \param separator character used as directory separator
///
/// \note
/// This function fsyncs each component created, and is therefore guaranteed to be stable on disk.
future<> recursive_touch_directory(sstring name);

/// Removes (unlinks) a file.
///
/// \param name name of the file to remove
///
/// \note
/// The removal is not guaranteed to be stable on disk, unless the
/// containing directory is sync'ed.
future<> remove_file(sstring name);

/// Renames (moves) a file.
///
/// \param old_name existing file name
/// \param new_name new file name
///
/// \note
/// The rename is not guaranteed to be stable on disk, unless the
/// both containing directories are sync'ed.
future<> rename_file(sstring old_name, sstring new_name);

/// @}
