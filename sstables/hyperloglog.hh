/*
 * Copyright (c) 2013 Hideaki Ohno <hide.o.j55{at}gmail.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the 'Software'), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so.
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

/*
 * Copyright (C) 2011 Clearspring Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

#pragma once

/**
 * @file hyperloglog.hpp
 * @brief HyperLogLog cardinality estimator
 * @date Created 2013/3/20
 * @author Hideaki Ohno
 */

#include <vector>
#include <cmath>
#include <sstream>
#include <stdexcept>
#include <algorithm>
#include <seastar/core/byteorder.hh>
#include <seastar/core/temporary_buffer.hh>

#include "seastarx.hh"

#if 0
#include "murmur3.h"
#endif

#define HLL_HASH_SEED 313

namespace hll {

static constexpr double pow_2_32 = 4294967296.0; ///< 2^32
static constexpr double neg_pow_2_32 = -4294967296.0; ///< -(2^32)


static inline size_t size_unsigned_var_int(unsigned int value) {
    size_t size = 0;
    while ((value & 0xFFFFFF80) != 0L) {
        size++;
        value >>= 7;
    }
    size++;
    return size;
}

static inline size_t write_unsigned_var_int(unsigned int value, uint8_t* to) {
    size_t size = 0;
    while ((value & 0xFFFFFF80) != 0L) {
        *to = (value & 0x7F) | 0x80;
        value >>= 7;
        to++;
        size++;
    }
    *to = value & 0x7F;
    size++;
    return size;
}

/** @class HyperLogLog
 *  @brief Implement of 'HyperLogLog' estimate cardinality algorithm
 */
class HyperLogLog {
public:

    /**
     * Constructor
     *
     * @param[in] b bit width (register size will be 2 to the b power).
     *            This value must be in the range[4,16].Default value is 4.
     *
     * @exception std::invalid_argument the argument is out of range.
     */
    HyperLogLog(uint8_t b = 4) :
            b_(b), m_(1 << b), M_(m_, 0) {

        if (b < 4 || 16 < b) {
            throw std::invalid_argument("bit width must be in the range [4,16]");
        }

        double alpha;
        switch (m_) {
            case 16:
                alpha = 0.673;
                break;
            case 32:
                alpha = 0.697;
                break;
            case 64:
                alpha = 0.709;
                break;
            default:
                alpha = 0.7213 / (1.0 + 1.079 / m_);
                break;
        }
        alphaMM_ = alpha * m_ * m_;
    }

    static HyperLogLog from_bytes(temporary_buffer<uint8_t> bytes) {
        // FIXME: implement class that creates a HyperLogLog from an array of bytes.
        // This will useful if we need to work with the cardinality data from the
        // compaction metadata.
        abort();
    }

    /**
     * Adds element to the estimator
     *
     * @param[in] str string to add
     * @param[in] len length of string
     */
#if 0
    void add(const char* str, uint32_t len) {
        uint32_t hash;
        MurmurHash3_x86_32(str, len, HLL_HASH_SEED, (void*) &hash);
        uint32_t index = hash >> (32 - b_);
        uint8_t rank = rho((hash << b_), 32 - b_);
        if (rank > M_[index]) {
            M_[index] = rank;
        }
    }
#endif
    void offer_hashed(uint64_t hash) {
        uint32_t index = hash >> (64 - b_);
        uint8_t rank = rho((hash << b_), 64 - b_);

        if (rank > M_[index]) {
            M_[index] = rank;
        }
    }

    /*
     * Calculate the size of buffer returned by get_bytes().
     */
    size_t get_bytes_size() {
        size_t size = 0;
        size += sizeof(int); // version
        size += size_unsigned_var_int(b_); // p; register width = b_.
        size += size_unsigned_var_int(0); // sp; // sparse set = 0.
        size += size_unsigned_var_int(0); // type;
        size += size_unsigned_var_int(M_.size()); // register size;
        size += M_.size();
        return size;
    }

    temporary_buffer<uint8_t> get_bytes() {
        // FIXME: add support to SPARSE format.
        static constexpr int version = 2;

        size_t s = get_bytes_size();
        temporary_buffer<uint8_t> bytes(s);
        size_t offset = 0;
        // write version
        write_be<int32_t>(reinterpret_cast<char*>(bytes.get_write() + offset), -version);
        offset += sizeof(int);

        // write register width
        offset += write_unsigned_var_int(b_, bytes.get_write() + offset);
        // NOTE: write precision value for sparse set (not supported).
        offset += write_unsigned_var_int(0, bytes.get_write() + offset);
        // write type (NORMAL always!)
        offset += write_unsigned_var_int(0, bytes.get_write() + offset);
        // write register size
        offset += write_unsigned_var_int(M_.size(), bytes.get_write() + offset);
        // write register
        memcpy(bytes.get_write() + offset, M_.data(), M_.size());
        offset += M_.size();

        bytes.trim(offset);
        if (s != offset) {
            throw std::runtime_error("possible overflow while generating cardinality metadata");
        }
        return bytes;
    }

    /**
     * Estimates cardinality value.
     *
     * @return Estimated cardinality value.
     */
    double estimate() const {
        double estimate;
        double sum = 0.0;
        for (uint32_t i = 0; i < m_; i++) {
            sum += 1.0 / pow(2.0, M_[i]);
        }
        estimate = alphaMM_ / sum; // E in the original paper
        if (estimate <= 2.5 * m_) {
            uint32_t zeros = 0;
            for (uint32_t i = 0; i < m_; i++) {
                if (M_[i] == 0) {
                    zeros++;
                }
            }
            if (zeros != 0) {
                estimate = m_ * log(static_cast<double>(m_)/ zeros);
            }
        } else if (estimate > (1.0 / 30.0) * pow_2_32) {
            estimate = neg_pow_2_32 * log(1.0 - (estimate / pow_2_32));
        }
        return estimate;
    }

    /**
     * Merges the estimate from 'other' into this object, returning the estimate of their union.
     * The number of registers in each must be the same.
     *
     * @param[in] other HyperLogLog instance to be merged
     *
     * @exception std::invalid_argument number of registers doesn't match.
     */
    void merge(const HyperLogLog& other) {
        if (m_ != other.m_) {
            std::stringstream ss;
            ss << "number of registers doesn't match: " << m_ << " != " << other.m_;
            throw std::invalid_argument(ss.str().c_str());
        }
        for (uint32_t r = 0; r < m_; ++r) {
            if (M_[r] < other.M_[r]) {
                M_[r] = other.M_[r];
            }
        }
    }

    /**
     * Clears all internal registers.
     */
    void clear() {
        std::fill(M_.begin(), M_.end(), 0);
    }

    /**
     * Returns size of register.
     *
     * @return Register size
     */
    uint32_t registerSize() const {
        return m_;
    }

    /**
     * Exchanges the content of the instance
     *
     * @param[in,out] rhs Another HyperLogLog instance
     */
    void swap(HyperLogLog& rhs) {
        std::swap(b_, rhs.b_);
        std::swap(m_, rhs.m_);
        std::swap(alphaMM_, rhs.alphaMM_);
        M_.swap(rhs.M_);
    }

    /**
     * Dump the current status to a stream
     *
     * @param[out] os The output stream where the data is saved
     *
     * @exception std::runtime_error When failed to dump.
     */
    void dump(std::ostream& os) const {
        os.write((char*)&b_, sizeof(b_));
        os.write((char*)&M_[0], sizeof(M_[0]) * M_.size());
        if(os.fail()){
            throw std::runtime_error("Failed to dump");
        }
    }

    /**
     * Restore the status from a stream
     *
     * @param[in] is The input stream where the status is saved
     *
     * @exception std::runtime_error When failed to restore.
     */
    void restore(std::istream& is) {
        uint8_t b = 0;
        is.read((char*)&b, sizeof(b));
        HyperLogLog tempHLL(b);
        is.read((char*)&(tempHLL.M_[0]), sizeof(M_[0]) * tempHLL.m_);
        if(is.fail()){
           throw std::runtime_error("Failed to restore");
        }
        swap(tempHLL);
    }

private:
    uint8_t b_; ///< register bit width
    uint32_t m_; ///< register size
    double alphaMM_; ///< alpha * m^2
    std::vector<uint8_t> M_; ///< registers

    uint8_t rho(uint32_t x, uint8_t b) {
        uint8_t v = 1;
        while (v <= b && !(x & 0x80000000)) {
            v++;
            x <<= 1;
        }
        return v;
    }

};

} // namespace hll
