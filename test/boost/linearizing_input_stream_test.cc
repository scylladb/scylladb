/*
 * Copyright (C) 2019-present ScyllaDB
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

#define BOOST_TEST_MODULE core

#include <random>

#include <boost/test/unit_test.hpp>

#include "bytes_ostream.hh"
#include "utils/linearizing_input_stream.hh"
#include "utils/serialization.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/log.hh"

namespace {

class fragment_vector {
public:
    using fragment_type = bytes_view;
    using iterator = std::vector<fragment_type>::const_iterator;
    using const_iterator = std::vector<fragment_type>::const_iterator;

private:
    std::vector<bytes> _fragments;
    std::vector<fragment_type> _fragment_views;
    size_t _size = 0;

public:
    explicit fragment_vector(std::vector<bytes> fragments)
        : _fragments(std::move(fragments)) {
        for (const auto& frag : _fragments) {
            _fragment_views.emplace_back(frag);
            _size += frag.size();
        }
    }
    const_iterator begin() const {
        return _fragment_views.begin();
    }
    const_iterator end() const {
        return _fragment_views.end();
    }
    size_t fragments() const {
        return _fragments.size();
    }
    size_t size_bytes() const {
        return _size;
    }
    bool empty() const {
        return _size == 0;
    }
};

struct value_description {
    virtual ~value_description() = default;
    virtual size_t size() const = 0;
    virtual void write(bytes_ostream::output_iterator&) const = 0;
    virtual void skip(utils::linearizing_input_stream<fragment_vector>&) const = 0;
    virtual void read_and_check_value(utils::linearizing_input_stream<fragment_vector>&) const = 0;
};

struct payload {
    bytes data;
    std::vector<std::unique_ptr<value_description>> value_descriptions;
};

template <typename T>
T get_int(std::mt19937& rnd_engine) {
    return tests::random::get_int(std::numeric_limits<T>::min(), std::numeric_limits<T>::max(), rnd_engine);
}

template <typename T>
class trivial_value_description : public value_description {
    T _value;

public:
    explicit trivial_value_description(T v) : _value(v) {
    }
    virtual size_t size() const override {
        return sizeof(T);
    }
    virtual void write(bytes_ostream::output_iterator& out) const override {
        ::write(out, _value);
    }
    virtual void skip(utils::linearizing_input_stream<fragment_vector>& in) const override {
        in.skip(sizeof(T));
    }
    virtual void read_and_check_value(utils::linearizing_input_stream<fragment_vector>& in) const override {
        const auto v = in.read_trivial<T>();
        BOOST_REQUIRE_EQUAL(_value, v);
    }
};

class string_value_description : public value_description {
    sstring _value;

public:
    explicit string_value_description(std::mt19937& rnd_engine, size_t size)
        // We know size is at least 3
        : _value(tests::random::get_sstring(size - serialize_int16_size, rnd_engine)) {
    }
    virtual size_t size() const override {
        return ::serialize_string_size(_value);
    }
    virtual void write(bytes_ostream::output_iterator& out) const override {
        serialize_string(out, _value);
    }
    virtual void skip(utils::linearizing_input_stream<fragment_vector>& in) const override {
        in.skip(size());
    }
    virtual void read_and_check_value(utils::linearizing_input_stream<fragment_vector>& in) const override {
        const auto size = in.read_trivial<uint16_t>();
        BOOST_REQUIRE_EQUAL(size, _value.size());
        const auto v = sstring(reinterpret_cast<const sstring::value_type*>(in.read(size).data()), size);
        BOOST_REQUIRE_EQUAL(_value, v);
    }

};

const payload generate_payload(std::mt19937& rnd_engine) {
    bytes_ostream ret;
    auto out = ret.write_begin();
    std::vector<std::unique_ptr<value_description>> value_descriptions;
    size_t total_size = 0;

    auto value_size_dist = tests::random::stepped_int_distribution<size_t>{{
        {50.0, {1,   8}},
        {50.0, {9, 100}}}};

    for (size_t i = 0; i < 100; ++i) {
        const auto size = value_size_dist(rnd_engine);
        std::unique_ptr<value_description> vd;
        switch (size) {
            case 1:
                vd = std::make_unique<trivial_value_description<uint8_t>>(get_int<int8_t>(rnd_engine));
                break;
            case 2:
                vd = std::make_unique<trivial_value_description<int16_t>>(get_int<int16_t>(rnd_engine));
                break;
            case 4:
                vd = std::make_unique<trivial_value_description<int32_t>>(get_int<int32_t>(rnd_engine));
                break;
            case 8:
                vd = std::make_unique<trivial_value_description<int64_t>>(get_int<int64_t>(rnd_engine));
                break;
            default:
                vd = std::make_unique<string_value_description>(rnd_engine, size);
                break;
        }
        BOOST_REQUIRE_EQUAL(vd->size(), size);
        total_size += size;

        vd->write(out);
        BOOST_REQUIRE_EQUAL(ret.size(), total_size);

        value_descriptions.emplace_back(std::move(vd));
    }

    return payload{bytes(ret.linearize()), std::move(value_descriptions)};
}

std::vector<bytes> no_fragmenting(std::mt19937&, bytes_view bv) {
    testlog.info("Fragmenting payload with {}()", __FUNCTION__);
    return {bytes{bv}};
}

template <size_t N>
std::vector<bytes> n_byte_fragments(std::mt19937&, bytes_view bv) {
    testlog.info("Fragmenting payload with {}<{}>()", __FUNCTION__, N);
    std::vector<bytes> ret;
    while (!bv.empty()) {
        const auto size = std::min(bv.size(), N);
        ret.emplace_back(bytes_view(bv.begin(), size));
        bv.remove_prefix(size);
    }
    return ret;
}

std::vector<bytes> random_fragments(std::mt19937& rnd_engine, bytes_view bv) {
    testlog.info("Fragmenting payload with {}()", __FUNCTION__);
    std::vector<bytes> ret;
    while (!bv.empty()) {
        std::uniform_int_distribution<size_t> size_dist{1, bv.size()};
        const auto size = size_dist(rnd_engine);
        ret.emplace_back(bytes_view(bv.begin(), size));
        bv.remove_prefix(size);
    }
    return ret;
}

}

BOOST_AUTO_TEST_CASE(test_linearizing_input_stream) {
    // REPLACE RANDOM SEED HERE.
    const auto seed = std::random_device{}();

    std::cout << "test seed: " << seed << std::endl;

    auto rnd_engine = std::mt19937(seed);
    auto payload = generate_payload(rnd_engine);
    std::uniform_int_distribution<uint8_t> skip_dist{0, 1};

    testlog.info("Read back data");

    for (auto&& fragment_fn : {no_fragmenting, n_byte_fragments<1>, n_byte_fragments<2>, n_byte_fragments<3>, random_fragments}) {
        size_t expected_size = payload.data.size();
        auto fragmented_payload = fragment_vector(fragment_fn(rnd_engine, payload.data));
        auto in = utils::linearizing_input_stream(fragmented_payload);

        testlog.info("Testing with payload: size={}, fragments={}", expected_size, fragmented_payload.fragments());
        BOOST_REQUIRE_EQUAL(fragmented_payload.size_bytes(), expected_size);
        BOOST_REQUIRE_EQUAL(in.size(), expected_size);

        for (auto&& value_description : payload.value_descriptions) {
            if (skip_dist(rnd_engine)) {
                value_description->skip(in);
            } else {
                value_description->read_and_check_value(in);
            }
            expected_size -= value_description->size();
            BOOST_REQUIRE_EQUAL(in.size(), expected_size);
        }

        BOOST_REQUIRE_EQUAL(in.size(), 0);
        BOOST_REQUIRE(in.empty());
    }
}
