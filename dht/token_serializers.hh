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

inline wrapping_interval<dht::token> convert_intervals(const wrapping_interval<dht::legacy_token>& legacy) {
    const auto& start = legacy.start();
    const auto& end = legacy.end();
    bool singular = legacy.is_singular();
    std::optional<interval_bound<dht::token>> left;
    if (start) {
        left = interval_bound<dht::token>(start->value(), start->is_inclusive());
    }
    std::optional<interval_bound<dht::token>> right;
    if (end) {
        right = interval_bound<dht::token>(end->value(), end->is_inclusive());
    }

    bool sane_start = !start || start->value()._kind == dht::token_kind::key;
    bool sane_end = !end || end->value()._kind == dht::token_kind::key;

    // Edge case hell.
    if (sane_start && sane_end) [[likely]] {
        // The sane case. Hopefully other cases never happen.
        return wrapping_interval<dht::token>(std::move(left), std::move(right), singular);
    } else if (sane_start) {
        if (end->value()._kind == dht::token_kind::before_all_keys) {
            return wrapping_interval<dht::token>(std::move(left), interval_bound<dht::token>(dht::minimum_token(), false), singular);
        } else {
            return wrapping_interval<dht::token>(std::move(left), {}, singular);
        }
    } else if (singular) {
        // What would a singular maximum_token() mean?
        return wrapping_interval<dht::token>({}, dht::minimum_token());
    } else if (sane_end) {
        if (start->value()._kind == dht::token_kind::after_all_keys) {
            return wrapping_interval<dht::token>(interval_bound<dht::token>(dht::greatest_token(), false), std::move(right));
        } else {
            return wrapping_interval<dht::token>({}, std::move(right));
        }
    } else {
        auto score = [](const std::optional<interval_bound<dht::legacy_token>>& b) -> int {
            if (!b) {
                return 0;
            }
            switch (b->value()._kind) {
                case dht::token_kind::after_all_keys:
                    return -1;
                case dht::token_kind::before_all_keys:
                    return 1;
                default:
                    return 0;
            }
        };
        int start_score = score(start);
        int end_score = score(end);
        bool is_wrapping = (end_score < start_score) || (end_score == start_score && (!start->is_inclusive() || !end->is_inclusive()));
        if (is_wrapping) {
            return wrapping_interval<dht::token>::make_open_ended_both_sides();
        } else {
            return wrapping_interval<dht::token>::make_starting_with(interval_bound<dht::token>(dht::greatest_token(), false));
        }
    }
}

template<>
template<typename Input>
range<dht::token> serializer<range<dht::token>>::read(Input& in) {
    auto legacy_range = deserialize(in, boost::type<range<dht::legacy_token>>());
    return convert_intervals(legacy_range);
}

template<>
template<typename Input>
nonwrapping_range<dht::token> serializer<nonwrapping_range<dht::token>>::read(Input& in) {
    auto legacy_range = deserialize(in, boost::type<nonwrapping_range<dht::legacy_token>>());
    return nonwrapping_range<dht::token>(convert_intervals(legacy_range));
}

} // namespace ser
