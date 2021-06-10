/*
 * Copyright 2021-present ScyllaDB
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
 * You should have received a copy of the GNU Affero General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "idl/token.dist.hh"
#include "idl/ring_position.dist.hh"
#include "idl/range.dist.hh"
#include "idl/token.dist.impl.hh"
#include "idl/ring_position.dist.impl.hh"
#include "idl/range.dist.impl.hh"

namespace ser {

template<>
struct serializer<dht::ring_position> {
    template<typename Input>
    static dht::ring_position read(Input& in) {
        return seastar::with_serialized_stream(in, [] (auto& buf) {
            size_type size = deserialize(buf, boost::type<size_type>());
            auto in = buf.read_substream(size - sizeof(size_type));

            auto legacy_token = deserialize(in, boost::type<dht::legacy_token>());
            auto bound = deserialize(in, boost::type<dht::ring_position::token_bound>());
            auto key = deserialize(in, boost::type<std::optional<partition_key>>());

            switch (legacy_token._kind) {
            case dht::token_kind::after_all_keys:
                return dht::ring_position::max();
            case dht::token_kind::before_all_keys:
                return dht::ring_position::min();
            default:
                return dht::ring_position(dht::token(legacy_token), bound, std::move(key));
            }
        });
    }
    template<typename Output>
    static void write(Output& out, const dht::ring_position& obj) {
        set_size(out, obj);
        serialize(out, obj.token());
        serialize(out, obj.bound());
        serialize(out, obj.key());
    }
    template<typename Input>
    static void skip(Input& in) {
        seastar::with_serialized_stream(in, [] (auto& buf) {
            size_type size = deserialize(buf, boost::type<size_type>());
            buf.skip(size - sizeof(size_type));
        });
    }
};

template<>
struct serializer<dht::token> {
    template<typename Input>
    static dht::token read(Input& in) {
        return serializer<dht::legacy_token>::read(in);
    }
    template<typename Output>
    static void write(Output& out, const dht::token& obj) {
        return serializer<dht::legacy_token>::write(out, obj.legacy());
    }
    template<typename Input>
    static void skip(Input& in) {
        return serializer<dht::legacy_token>::skip(in);
    }
};

} // namespace ser
