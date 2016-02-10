/*
 * Copyright 2015 Cloudius Systems
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

#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>

#include "tests/test-utils.hh"

#include "key_reader.hh"
#include "schema_builder.hh"

static schema_ptr make_schema()
{
    return schema_builder("ks", "cf")
        .with_column("pk", int32_type, column_kind::partition_key)
        .with_column("v", int32_type, column_kind::regular_column)
        .build();
}

class keys_from_vector : public key_reader::impl {
    std::vector<dht::decorated_key> _keys;
    size_t _pos = 0;
public:
    keys_from_vector(schema_ptr s, std::vector<dht::decorated_key> keys) : _keys(std::move(keys)) {
        auto cmp = dht::decorated_key::less_comparator(std::move(s));
        std::sort(_keys.begin(), _keys.end(), cmp);
    }
    virtual future<dht::decorated_key_opt> operator()() override {
        if (_pos >= _keys.size()) {
            return make_ready_future<dht::decorated_key_opt>();
        }
        return make_ready_future<dht::decorated_key_opt>(_keys[_pos++]);
    }
};

SEASTAR_TEST_CASE(test_combined) {
    auto s = make_schema();
    auto make_key = [s] (int32_t value) {
        auto pk = partition_key::from_single_value(*s, int32_type->decompose(value));
        return dht::global_partitioner().decorate_key(*s, std::move(pk));
    };

    std::vector<dht::decorated_key> a = { make_key(0), make_key(1), make_key(2) };
    std::vector<dht::decorated_key> b = { make_key(3) };
    std::vector<dht::decorated_key> c = { make_key(4), make_key(5) };
    std::vector<key_reader> rds;
    rds.emplace_back(make_key_reader<keys_from_vector>(s, std::move(a)));
    rds.emplace_back(make_key_reader<keys_from_vector>(s, std::move(b)));
    rds.emplace_back(make_key_reader<keys_from_vector>(s, std::move(c)));
    auto reader = make_lw_shared(make_combined_reader(s, std::move(rds)));
    return (*reader)().then([reader, s, make_key] (dht::decorated_key_opt dk) {
        BOOST_REQUIRE(dk);
        BOOST_REQUIRE(dk->equal(*s, make_key(5)));
        return (*reader)();
    }).then([reader, s, make_key] (dht::decorated_key_opt dk) {
        BOOST_REQUIRE(dk);
        BOOST_REQUIRE(dk->equal(*s, make_key(1)));
        return (*reader)();
    }).then([reader, s, make_key] (dht::decorated_key_opt dk) {
        BOOST_REQUIRE(dk);
        BOOST_REQUIRE(dk->equal(*s, make_key(0)));
        return (*reader)();
    }).then([reader, s, make_key] (dht::decorated_key_opt dk) {
        BOOST_REQUIRE(dk);
        BOOST_REQUIRE(dk->equal(*s, make_key(2)));
        return (*reader)();
    }).then([reader, s, make_key] (dht::decorated_key_opt dk) {
        BOOST_REQUIRE(dk);
        BOOST_REQUIRE(dk->equal(*s, make_key(4)));
        return (*reader)();
    }).then([reader, s, make_key] (dht::decorated_key_opt dk) {
        BOOST_REQUIRE(dk);
        BOOST_REQUIRE(dk->equal(*s, make_key(3)));
        return (*reader)();
    }).then([reader] (dht::decorated_key_opt dk) {
        BOOST_REQUIRE(!dk);
    });
}

SEASTAR_TEST_CASE(test_filtering) {
    auto s = make_schema();
    auto make_key = [s] (int32_t value) {
        auto pk = partition_key::from_single_value(*s, int32_type->decompose(value));
        return dht::global_partitioner().decorate_key(*s, std::move(pk));
    };

    std::vector<dht::decorated_key> v = { make_key(0), make_key(1), make_key(2), make_key(3) };
    auto rd = make_key_reader<keys_from_vector>(s, std::move(v));
    auto reader = make_lw_shared(make_filtering_reader(std::move(rd), [s, make_key] (dht::decorated_key dk) {
        return !dk.equal(*s, make_key(2));
    }));
    return (*reader)().then([reader, make_key, s] (dht::decorated_key_opt dk) {
        BOOST_REQUIRE(dk);
        BOOST_REQUIRE(dk->equal(*s, make_key(1)));
        return (*reader)();
    }).then([reader, make_key, s] (dht::decorated_key_opt dk) {
        BOOST_REQUIRE(dk);
        BOOST_REQUIRE(dk->equal(*s, make_key(0)));
        return (*reader)();
    }).then([reader, make_key, s] (dht::decorated_key_opt dk) {
        BOOST_REQUIRE(dk);
        BOOST_REQUIRE(dk->equal(*s, make_key(3)));
        return (*reader)();
    }).then([reader] (dht::decorated_key_opt dk) {
        BOOST_REQUIRE(!dk);
    });
}
