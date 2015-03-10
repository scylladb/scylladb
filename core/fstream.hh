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

// File <-> streams adapters
//
// Seastar files are block-based due to the reliance on DMA - you must read
// on sector boundaries.  The adapters in this file provide a byte stream
// interface to files, while retaining the zero-copy characteristics of
// seastar files.

#include "file.hh"
#include "iostream.hh"
#include "shared_ptr.hh"

// Create an input_stream for reading starting at a given position of the
// given file. Multiple fibers of execution (continuations) may safely open
// multiple input streams concurrently for the same file.
input_stream<char> make_file_input_stream(
        lw_shared_ptr<file> file, uint64_t offset = 0,
        uint64_t buffer_size = 8192);
