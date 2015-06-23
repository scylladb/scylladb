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

#if !defined(HYPERLOGLOG_HPP)
#define HYPERLOGLOG_HPP

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
#include "murmur3.h"

#define HLL_HASH_SEED 313

namespace hll {

static const double pow_2_32 = 4294967296.0; ///< 2^32
static const double neg_pow_2_32 = -4294967296.0; ///< -(2^32)

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
    HyperLogLog(uint8_t b = 4) throw (std::invalid_argument) :
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

    /**
     * Adds element to the estimator
     *
     * @param[in] str string to add
     * @param[in] len length of string
     */
    void add(const char* str, uint32_t len) {
        uint32_t hash;
        MurmurHash3_x86_32(str, len, HLL_HASH_SEED, (void*) &hash);
        uint32_t index = hash >> (32 - b_);
        uint8_t rank = rho((hash << b_), 32 - b_);
        if (rank > M_[index]) {
            M_[index] = rank;
        }
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
    void merge(const HyperLogLog& other) throw (std::invalid_argument) {
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
    void dump(std::ostream& os) const throw(std::runtime_error){
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
    void restore(std::istream& is) throw(std::runtime_error){
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

#endif // !defined(HYPERLOGLOG_HPP)
