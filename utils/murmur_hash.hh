/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modified by Cloudius Systems.
 * Copyright 2015 Cloudius Systems.
 */

#pragma once

#include <cstdint>
#include <array>

#include "bytes.hh"

/**
 * This is a very fast, non-cryptographic hash suitable for general hash-based
 * lookup. See http://murmurhash.googlepages.com/ (Murmur Hash 2) and
 * https://code.google.com/p/smhasher/wiki/MurmurHash3.
 *
 * This code is not based on the original Murmur Hash C code, but rather
 * a translation of Cassandra's Java version back to C.
 **/

namespace utils {

namespace murmur_hash {

uint32_t hash32(bytes_view data, int32_t seed);
uint64_t hash2_64(bytes_view key, uint64_t seed);
void hash3_x64_128(bytes_view key, uint64_t seed, std::array<uint64_t, 2>& result);

} // namespace murmur_hash

} // namespace utils
