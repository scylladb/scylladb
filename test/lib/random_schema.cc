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

#include <boost/algorithm/string/join.hpp>
#include <boost/range/algorithm/sort.hpp>
#include <boost/range/algorithm/unique.hpp>

#include "cql3/cql3_type.hh"
#include "mutation.hh"
#include "mutation_fragment.hh"
#include "schema_builder.hh"
#include "test/lib/random_schema.hh"
#include "test/lib/random_utils.hh"
#include "types/list.hh"
#include "types/map.hh"
#include "types/set.hh"
#include "types/tuple.hh"
#include "types/user.hh"
#include "utils/big_decimal.hh"
#include "utils/UUID_gen.hh"

namespace tests {

type_generator::type_generator(random_schema_specification& spec) : _spec(spec) {
    struct simple_type_generator {
        data_type type;
        data_type operator()(std::mt19937&, is_multi_cell) { return type; }
    };
    _generators = {
        simple_type_generator{byte_type},
        simple_type_generator{short_type},
        simple_type_generator{int32_type},
        simple_type_generator{long_type},
        simple_type_generator{ascii_type},
        simple_type_generator{bytes_type},
        simple_type_generator{utf8_type},
        simple_type_generator{boolean_type},
        simple_type_generator{date_type},
        simple_type_generator{timeuuid_type},
        simple_type_generator{timestamp_type},
        simple_type_generator{simple_date_type},
        simple_type_generator{time_type},
        simple_type_generator{uuid_type},
        simple_type_generator{inet_addr_type},
        simple_type_generator{float_type},
        simple_type_generator{double_type},
        simple_type_generator{varint_type},
        simple_type_generator{decimal_type},
        simple_type_generator{duration_type}};

    // tuple
    _generators.emplace_back(
        [this] (std::mt19937& engine, is_multi_cell) {
            std::uniform_int_distribution<size_t> count_dist{2, 4};
            const auto count = count_dist(engine);
            std::vector<data_type> data_types;
            for (size_t i = 0; i < count; ++i) {
                data_types.emplace_back((*this)(engine, type_generator::is_multi_cell::no));
            }
            return tuple_type_impl::get_instance(std::move(data_types));
        });
    // user
    _generators.emplace_back(
        [this] (std::mt19937& engine, is_multi_cell multi_cell) mutable {
            std::uniform_int_distribution<size_t> count_dist{2, 4};
            const auto count = count_dist(engine);

            std::vector<bytes> field_names;
            std::vector<data_type> field_types;
            for (size_t i = 0; i < count; ++i) {
                field_names.emplace_back(to_bytes(format("f{}", i)));
                field_types.emplace_back((*this)(engine, type_generator::is_multi_cell::no));
            }

            return user_type_impl::get_instance(_spec.keyspace_name(), to_bytes(_spec.udt_name(engine)), std::move(field_names),
                    std::move(field_types), bool(multi_cell));
        });
    // list
    _generators.emplace_back(
        [this] (std::mt19937& engine, is_multi_cell multi_cell) {
            auto element_type = (*this)(engine, type_generator::is_multi_cell::no);
            return list_type_impl::get_instance(std::move(element_type), bool(multi_cell));
        });
    // set
    _generators.emplace_back(
        [this] (std::mt19937& engine, is_multi_cell multi_cell) {
            auto element_type = (*this)(engine, type_generator::is_multi_cell::no);
            return set_type_impl::get_instance(std::move(element_type), bool(multi_cell));
        });
    // map
    _generators.emplace_back(
        [this] (std::mt19937& engine, is_multi_cell multi_cell) {
            auto key_type = (*this)(engine, type_generator::is_multi_cell::no);
            auto value_type = (*this)(engine, type_generator::is_multi_cell::no);
            return map_type_impl::get_instance(std::move(key_type), std::move(value_type), bool(multi_cell));
        });
}

data_type type_generator::operator()(std::mt19937& engine, is_multi_cell multi_cell) {
    auto dist = std::uniform_int_distribution<size_t>(0, _generators.size() - 1);
    return _generators.at(dist(engine))(engine, multi_cell);
}

namespace {

class default_random_schema_specification : public random_schema_specification {
    std::unordered_set<unsigned> _used_table_ids;
    std::unordered_set<unsigned> _used_udt_ids;
    std::uniform_int_distribution<size_t> _partition_column_count_dist;
    std::uniform_int_distribution<size_t> _clustering_column_count_dist;
    std::uniform_int_distribution<size_t> _regular_column_count_dist;
    std::uniform_int_distribution<size_t> _static_column_count_dist;
    type_generator _type_generator;

private:
    static unsigned generate_unique_id(std::mt19937& engine, std::unordered_set<unsigned>& used_ids) {
        std::uniform_int_distribution<unsigned> id_dist(0, 1024);

        unsigned id;
        do {
            id = id_dist(engine);
        } while (used_ids.contains(id));

        used_ids.insert(id);
        return id;
    }

    std::vector<data_type> generate_types(std::mt19937& engine, std::uniform_int_distribution<size_t>& count_dist,
            type_generator::is_multi_cell multi_cell, bool allow_reversed = false) {
        std::uniform_int_distribution<uint8_t> reversed_dist{0, uint8_t(allow_reversed)};

        std::vector<data_type> types;

        const auto count = count_dist(engine);
        for (size_t c = 0; c < count; ++c) {
            auto type = _type_generator(engine, multi_cell);
            if (reversed_dist(engine)) {
                types.emplace_back(make_shared<reversed_type_impl>(std::move(type)));
            } else {
                types.emplace_back(std::move(type));
            }
        }

        return types;
    }

public:
    default_random_schema_specification(
            sstring keyspace_name,
            std::uniform_int_distribution<size_t> partition_column_count_dist,
            std::uniform_int_distribution<size_t> clustering_column_count_dist,
            std::uniform_int_distribution<size_t> regular_column_count_dist,
            std::uniform_int_distribution<size_t> static_column_count_dist)
        : random_schema_specification(std::move(keyspace_name))
        , _partition_column_count_dist(partition_column_count_dist)
        , _clustering_column_count_dist(clustering_column_count_dist)
        , _regular_column_count_dist(regular_column_count_dist)
        , _static_column_count_dist(static_column_count_dist)
        , _type_generator(*this) {
        assert(_partition_column_count_dist.a() > 0);
        assert(_regular_column_count_dist.a() > 0);
    }
    virtual sstring table_name(std::mt19937& engine) override {
        return format("table{}", generate_unique_id(engine, _used_table_ids));
    }
    virtual sstring udt_name(std::mt19937& engine) override {
        return format("UDT{}", generate_unique_id(engine, _used_udt_ids));
    }
    virtual std::vector<data_type> partition_key_columns(std::mt19937& engine) override {
        return generate_types(engine, _partition_column_count_dist, type_generator::is_multi_cell::no, false);
    }
    virtual std::vector<data_type> clustering_key_columns(std::mt19937& engine) override {
        return generate_types(engine, _clustering_column_count_dist, type_generator::is_multi_cell::no, true);
    }
    virtual std::vector<data_type> regular_columns(std::mt19937& engine) override {
        return generate_types(engine, _regular_column_count_dist, type_generator::is_multi_cell::yes, false);
    }
    virtual std::vector<data_type> static_columns(std::mt19937& engine) override {
        return generate_types(engine, _static_column_count_dist, type_generator::is_multi_cell::yes, false);
    }
};

} // anonymous namespace

std::unique_ptr<random_schema_specification> make_random_schema_specification(
        sstring keyspace_name,
        std::uniform_int_distribution<size_t> partition_column_count_dist,
        std::uniform_int_distribution<size_t> clustering_column_count_dist,
        std::uniform_int_distribution<size_t> regular_column_count_dist,
        std::uniform_int_distribution<size_t> static_column_count_dist) {
    return std::make_unique<default_random_schema_specification>(std::move(keyspace_name), partition_column_count_dist, clustering_column_count_dist,
            regular_column_count_dist, static_column_count_dist);
}

namespace {

utils::multiprecision_int generate_multiprecision_integer_value(std::mt19937& engine, size_t max_size_in_bytes) {
    using utils::multiprecision_int;

    const auto max_bytes = std::min(size_t(16), std::max(size_t(2), max_size_in_bytes) - 1);
    const auto generate_int = [] (std::mt19937& engine, size_t max_bytes) {
        if (max_bytes == 8) {
            return multiprecision_int(random::get_int<uint64_t>(engine));
        } else { // max_bytes < 8
            return multiprecision_int(random::get_int<uint64_t>(0, (uint64_t(1) << (max_bytes * 8)) - uint64_t(1), engine));
        }
    };

    if (max_bytes <= 8) {
        return generate_int(engine, max_bytes);
    } else { // max_bytes > 8
        auto ls = multiprecision_int(generate_int(engine, 8));
        auto ms = multiprecision_int(generate_int(engine, max_bytes - 8));
        return multiprecision_int(ls) + (multiprecision_int(ms) << 64);
    }
}

template <typename String>
String generate_string_value(std::mt19937& engine, typename String::value_type min, typename String::value_type max, size_t max_size_in_bytes) {
    auto size_dist = random::stepped_int_distribution<size_t>{{
        {95.0, {   0,   31}},
        { 4.5, {  32,   99}},
        { 0.4, { 100,  999}},
        { 0.1, {1000, 9999}}}};
    auto char_dist = std::uniform_int_distribution<typename String::value_type>(min, max);

    const auto size = std::min(size_dist(engine), max_size_in_bytes / sizeof(std::wstring::value_type));
    String str(size, '\0');

    for (size_t i = 0; i < size; ++i) {
        str[i] = char_dist(engine);
    }

    return str;
}

std::vector<data_value> generate_frozen_tuple_values(std::mt19937& engine, value_generator& val_gen, const std::vector<data_type>& member_types,
        size_t max_size_in_bytes) {
    std::vector<data_value> values;
    values.reserve(member_types.size());
    const auto member_max_size_in_bytes = max_size_in_bytes / member_types.size();

    for (auto member_type : member_types) {
        values.push_back(val_gen.generate_atomic_value(engine, *member_type, member_max_size_in_bytes));
    }

    return values;
}

data_model::mutation_description::collection generate_user_value(std::mt19937& engine, const user_type_impl& type,
        value_generator& val_gen) {
    using md = data_model::mutation_description;

    // Non-null fields.
    auto fields_num = std::uniform_int_distribution<size_t>(1, type.size())(engine);
    auto field_idxs = random::random_subset<unsigned>(type.size(), fields_num, engine);
    std::sort(field_idxs.begin(), field_idxs.end());

    md::collection collection;
    for (auto i: field_idxs) {
        collection.elements.push_back({serialize_field_index(i),
                val_gen.generate_atomic_value(engine, *type.type(i), value_generator::no_size_in_bytes_limit).serialize_nonnull()});
    }

    return collection;
}

data_model::mutation_description::collection generate_collection(std::mt19937& engine, const abstract_type& key_type,
        const abstract_type& value_type, value_generator& val_gen) {
    using md = data_model::mutation_description;
    auto key_generator = val_gen.get_atomic_value_generator(key_type);
    auto value_generator = val_gen.get_atomic_value_generator(value_type);

    auto size_dist = std::uniform_int_distribution<size_t>(0, 16);
    const auto size = size_dist(engine);

    std::map<bytes, md::atomic_value, serialized_compare> collection{key_type.as_less_comparator()};

    for (size_t i = 0; i < size; ++i) {
        collection.emplace(key_generator(engine, value_generator::no_size_in_bytes_limit).serialize_nonnull(),
                value_generator(engine, value_generator::no_size_in_bytes_limit).serialize().value_or(""));
    }

    md::collection flat_collection;
    flat_collection.elements.reserve(collection.size());
    for (auto&& [key, value] : collection) {
        flat_collection.elements.emplace_back(md::collection_element{key, value});
    }

    return flat_collection;
}

std::vector<data_value> generate_frozen_list(std::mt19937& engine, const abstract_type& value_type, value_generator& val_gen,
        size_t max_size_in_bytes) {
    auto value_generator = val_gen.get_atomic_value_generator(value_type);

    auto size_dist = std::uniform_int_distribution<size_t>(0, 4);
    const auto size = std::min(size_dist(engine), max_size_in_bytes / std::max(val_gen.min_size(value_type), size_t(1)));

    std::vector<data_value> collection;

    if (!size) {
        return collection;
    }

    const auto value_max_size_in_bytes = max_size_in_bytes / size;

    for (size_t i = 0; i < size; ++i) {
        collection.emplace_back(value_generator(engine, value_max_size_in_bytes));
    }

    return collection;
}

std::vector<data_value> generate_frozen_set(std::mt19937& engine, const abstract_type& key_type, value_generator& val_gen, size_t max_size_in_bytes) {
    auto key_generator = val_gen.get_atomic_value_generator(key_type);

    auto size_dist = std::uniform_int_distribution<size_t>(0, 4);
    const auto size = std::min(size_dist(engine), max_size_in_bytes / std::max(val_gen.min_size(key_type), size_t(1)));

    std::map<bytes, data_value, serialized_compare> collection{key_type.as_less_comparator()};
    std::vector<data_value> flat_collection;

    if (!size) {
        return flat_collection;
    }

    const auto value_max_size_in_bytes = max_size_in_bytes / size;

    for (size_t i = 0; i < size; ++i) {
        auto val = key_generator(engine, value_max_size_in_bytes);
        auto serialized_key = val.serialize_nonnull();
        collection.emplace(std::move(serialized_key), std::move(val));
    }

    flat_collection.reserve(collection.size());
    for (auto&& element : collection) {
        flat_collection.emplace_back(std::move(element.second));
    }
    return flat_collection;
}

std::vector<std::pair<data_value, data_value>> generate_frozen_map(std::mt19937& engine, const abstract_type& key_type,
        const abstract_type& value_type, value_generator& val_gen, size_t max_size_in_bytes) {
    auto key_generator = val_gen.get_atomic_value_generator(key_type);
    auto value_generator = val_gen.get_atomic_value_generator(value_type);

    auto size_dist = std::uniform_int_distribution<size_t>(0, 4);
    const auto min_item_size_in_bytes = val_gen.min_size(key_type) + val_gen.min_size(value_type);
    const auto size = std::min(size_dist(engine), max_size_in_bytes / std::max(min_item_size_in_bytes, size_t(1)));

    std::map<bytes, std::pair<data_value, data_value>, serialized_compare> collection(key_type.as_less_comparator());
    std::vector<std::pair<data_value, data_value>> flat_collection;

    if (!size) {
        return flat_collection;
    }

    const auto item_max_size_in_bytes = max_size_in_bytes / size;
    const auto key_max_size_in_bytes = item_max_size_in_bytes / 2;
    const auto value_max_size_in_bytes = item_max_size_in_bytes / 2;

    for (size_t i = 0; i < size; ++i) {
        auto key = key_generator(engine, key_max_size_in_bytes);
        auto serialized_key = key.serialize_nonnull();
        auto value = value_generator(engine, value_max_size_in_bytes);
        collection.emplace(std::move(serialized_key), std::pair(std::move(key), std::move(value)));
    }

    flat_collection.reserve(collection.size());
    for (auto&& element : collection) {
        flat_collection.emplace_back(std::move(element.second));
    }
    return flat_collection;
}

data_value generate_empty_value(std::mt19937&, size_t) {
    return data_value::make_null(empty_type);
}

data_value generate_byte_value(std::mt19937& engine, size_t) {
    return data_value(random::get_int<int8_t>(engine));
}

data_value generate_short_value(std::mt19937& engine, size_t) {
    return data_value(random::get_int<int16_t>(engine));
}

data_value generate_int32_value(std::mt19937& engine, size_t) {
    return data_value(random::get_int<int32_t>(engine));
}

data_value generate_long_value(std::mt19937& engine, size_t) {
    return data_value(random::get_int<int64_t>(engine));
}

data_value generate_ascii_value(std::mt19937& engine, size_t max_size_in_bytes) {
    return data_value(ascii_native_type{generate_string_value<sstring>(engine, 0, 127, max_size_in_bytes)});
}

data_value generate_bytes_value(std::mt19937& engine, size_t max_size_in_bytes) {
    return data_value(generate_string_value<bytes>(engine, std::numeric_limits<bytes::value_type>::min(),
            std::numeric_limits<bytes::value_type>::max(), max_size_in_bytes));
}

data_value generate_utf8_value(std::mt19937& engine, size_t max_size_in_bytes) {
    auto wstr = generate_string_value<std::wstring>(engine, 0, 0x0FFF, max_size_in_bytes);

    std::locale locale("en_US.utf8");
    using codec = std::codecvt<wchar_t, char, std::mbstate_t>;
    auto& f = std::use_facet<codec>(locale);

    sstring utf8_str(wstr.size() * f.max_length(), '\0');

    const wchar_t* from_next;
    char* to_next;
    std::mbstate_t mb{};
    auto res = f.out(mb, &wstr[0], &wstr[wstr.size()], from_next, &utf8_str[0], &utf8_str[utf8_str.size()], to_next);
    assert(res == codec::ok);
    utf8_str.resize(to_next - &utf8_str[0]);

    return data_value(std::move(utf8_str));
}

data_value generate_boolean_value(std::mt19937& engine, size_t) {
    auto dist = std::uniform_int_distribution<int8_t>(0, 1);
    return data_value(bool(dist(engine)));
}

data_value generate_date_value(std::mt19937& engine, size_t) {
    return data_value(date_type_native_type{db_clock::time_point(db_clock::duration(random::get_int<std::make_unsigned_t<db_clock::rep>>(engine)))});
}

data_value generate_timeuuid_value(std::mt19937&, size_t) {
    return data_value(timeuuid_native_type{utils::UUID_gen::get_time_UUID()});
}

data_value generate_timestamp_value(std::mt19937& engine, size_t) {
    using pt = db_clock::time_point;
    return data_value(pt(pt::duration(random::get_int<pt::rep>(engine))));
}

data_value generate_simple_date_value(std::mt19937& engine, size_t) {
    return data_value(simple_date_native_type{random::get_int<simple_date_native_type::primary_type>(engine)});
}

data_value generate_time_value(std::mt19937& engine, size_t) {
    return data_value(time_native_type{random::get_int<time_native_type::primary_type>(engine)});
}

data_value generate_uuid_value(std::mt19937& engine, size_t) {
    return data_value(utils::make_random_uuid());
}

data_value generate_inet_addr_value(std::mt19937& engine, size_t) {
    return data_value(net::ipv4_address(random::get_int<int32_t>(engine)));
}

data_value generate_float_value(std::mt19937& engine, size_t) {
    return data_value(random::get_real<float>(engine));
}

data_value generate_double_value(std::mt19937& engine, size_t) {
    return data_value(random::get_real<double>(engine));
}

data_value generate_varint_value(std::mt19937& engine, size_t max_size_in_bytes) {
    return data_value(generate_multiprecision_integer_value(engine, max_size_in_bytes));
}

data_value generate_decimal_value(std::mt19937& engine, size_t max_size_in_bytes) {
    auto scale_dist = std::uniform_int_distribution<int32_t>(-8, 8);
    return data_value(big_decimal(scale_dist(engine), generate_multiprecision_integer_value(engine, max_size_in_bytes - sizeof(int32_t))));
}

data_value generate_duration_value(std::mt19937& engine, size_t) {
    auto months = months_counter(random::get_int<months_counter::value_type>(engine));
    auto days = days_counter(random::get_int<days_counter::value_type>(0, 31, engine));
    auto nanoseconds = nanoseconds_counter(random::get_int<nanoseconds_counter::value_type>(86400000000000, engine));
    return data_value(cql_duration{months, days, nanoseconds});
}

data_value generate_frozen_tuple_value(std::mt19937& engine, const tuple_type_impl& type, value_generator& val_gen, size_t max_size_in_bytes) {
    assert(!type.is_multi_cell());
    return make_tuple_value(type.shared_from_this(), generate_frozen_tuple_values(engine, val_gen, type.all_types(), max_size_in_bytes));
}

data_value generate_frozen_user_value(std::mt19937& engine, const user_type_impl& type, value_generator& val_gen, size_t max_size_in_bytes) {
    assert(!type.is_multi_cell());
    return make_user_value(type.shared_from_this(), generate_frozen_tuple_values(engine, val_gen, type.all_types(), max_size_in_bytes));
}

data_model::mutation_description::collection generate_list_value(std::mt19937& engine, const list_type_impl& type, value_generator& val_gen) {
    assert(type.is_multi_cell());
    return generate_collection(engine, *type.name_comparator(), *type.value_comparator(), val_gen);
}

data_value generate_frozen_list_value(std::mt19937& engine, const list_type_impl& type, value_generator& val_gen, size_t max_size_in_bytes) {
    assert(!type.is_multi_cell());
    return make_list_value(type.shared_from_this(),
            generate_frozen_list(engine, *type.get_elements_type(), val_gen, max_size_in_bytes));
}

data_model::mutation_description::collection generate_set_value(std::mt19937& engine, const set_type_impl& type, value_generator& val_gen) {
    assert(type.is_multi_cell());
    return generate_collection(engine, *type.name_comparator(), *type.value_comparator(), val_gen);
}

data_value generate_frozen_set_value(std::mt19937& engine, const set_type_impl& type, value_generator& val_gen, size_t max_size_in_bytes) {
    assert(!type.is_multi_cell());
    return make_set_value(type.shared_from_this(),
            generate_frozen_set(engine, *type.get_elements_type(), val_gen, max_size_in_bytes));
}

data_model::mutation_description::collection generate_map_value(std::mt19937& engine, const map_type_impl& type, value_generator& val_gen) {
    assert(type.is_multi_cell());
    return generate_collection(engine, *type.name_comparator(), *type.value_comparator(), val_gen);
}

data_value generate_frozen_map_value(std::mt19937& engine, const map_type_impl& type, value_generator& val_gen, size_t max_size_in_bytes) {
    assert(!type.is_multi_cell());
    return make_map_value(type.shared_from_this(),
            generate_frozen_map(engine, *type.get_keys_type(), *type.get_values_type(), val_gen, max_size_in_bytes));
}

} // anonymous namespace

data_value value_generator::generate_atomic_value(std::mt19937& engine, const abstract_type& type, size_t max_size_in_bytes) {
    assert(!type.is_multi_cell());
    return get_atomic_value_generator(type)(engine, max_size_in_bytes);
}

value_generator::value_generator()
    : _regular_value_generators{
            {empty_type.get(), &generate_empty_value},
            {byte_type.get(), &generate_byte_value},
            {short_type.get(), &generate_short_value},
            {int32_type.get(), &generate_int32_value},
            {long_type.get(), &generate_long_value},
            {ascii_type.get(), &generate_ascii_value},
            {bytes_type.get(), &generate_bytes_value},
            {utf8_type.get(), &generate_utf8_value},
            {boolean_type.get(), &generate_boolean_value},
            {date_type.get(), &generate_date_value},
            {timeuuid_type.get(), &generate_timeuuid_value},
            {timestamp_type.get(), &generate_timestamp_value},
            {simple_date_type.get(), &generate_simple_date_value},
            {time_type.get(), &generate_time_value},
            {uuid_type.get(), &generate_uuid_value},
            {inet_addr_type.get(), &generate_inet_addr_value},
            {float_type.get(), &generate_float_value},
            {double_type.get(), &generate_double_value},
            {varint_type.get(), &generate_varint_value},
            {decimal_type.get(), &generate_decimal_value},
            {duration_type.get(), &generate_duration_value}} {
    std::mt19937 engine;
    for (const auto& [regular_type, regular_value_gen] : _regular_value_generators) {
        _regular_value_min_sizes.emplace(regular_type, regular_value_gen(engine, size_t{}).serialized_size());
    }
}

size_t value_generator::min_size(const abstract_type& type) {
    assert(!type.is_multi_cell());

    auto it = _regular_value_min_sizes.find(&type);
    if (it != _regular_value_min_sizes.end()) {
        return it->second;
    }

    std::mt19937 engine;

    if (auto maybe_user_type = dynamic_cast<const user_type_impl*>(&type)) {
        return generate_frozen_user_value(engine, *maybe_user_type, *this, size_t{}).serialized_size();
    }

    if (auto maybe_tuple_type = dynamic_cast<const tuple_type_impl*>(&type)) {
        return generate_frozen_tuple_value(engine, *maybe_tuple_type, *this, size_t{}).serialized_size();
    }

    if (auto maybe_list_type = dynamic_cast<const list_type_impl*>(&type)) {
        return generate_frozen_list_value(engine, *maybe_list_type, *this, size_t{}).serialized_size();
    }

    if (auto maybe_set_type = dynamic_cast<const set_type_impl*>(&type)) {
        return generate_frozen_set_value(engine, *maybe_set_type, *this, size_t{}).serialized_size();
    }

    if (auto maybe_map_type = dynamic_cast<const map_type_impl*>(&type)) {
        return generate_frozen_map_value(engine, *maybe_map_type, *this, size_t{}).serialized_size();
    }

    if (auto maybe_reversed_type = dynamic_cast<const reversed_type_impl*>(&type)) {
        return min_size(*maybe_reversed_type->underlying_type());
    }

    throw std::runtime_error(fmt::format("Don't know how to calculate min size for unknown type {}", type.name()));
}

value_generator::atomic_value_generator value_generator::get_atomic_value_generator(const abstract_type& type) {
    assert(!type.is_multi_cell());

    auto it = _regular_value_generators.find(&type);
    if (it != _regular_value_generators.end()) {
        return it->second;
    }

    if (auto maybe_user_type = dynamic_cast<const user_type_impl*>(&type)) {
        return [this, maybe_user_type] (std::mt19937& engine, size_t max_size_in_bytes) {
            return generate_frozen_user_value(engine, *maybe_user_type, *this, max_size_in_bytes);
        };
    }

    if (auto maybe_tuple_type = dynamic_cast<const tuple_type_impl*>(&type)) {
        return [this, maybe_tuple_type] (std::mt19937& engine, size_t max_size_in_bytes) {
            return generate_frozen_tuple_value(engine, *maybe_tuple_type, *this, max_size_in_bytes);
        };
    }

    if (auto maybe_list_type = dynamic_cast<const list_type_impl*>(&type)) {
        return [this, maybe_list_type] (std::mt19937& engine, size_t max_size_in_bytes) {
            return generate_frozen_list_value(engine, *maybe_list_type, *this, max_size_in_bytes);
        };
    }

    if (auto maybe_set_type = dynamic_cast<const set_type_impl*>(&type)) {
        return [this, maybe_set_type] (std::mt19937& engine, size_t max_size_in_bytes) {
            return generate_frozen_set_value(engine, *maybe_set_type, *this, max_size_in_bytes);
        };
    }

    if (auto maybe_map_type = dynamic_cast<const map_type_impl*>(&type)) {
        return [this, maybe_map_type] (std::mt19937& engine, size_t max_size_in_bytes) {
            return generate_frozen_map_value(engine, *maybe_map_type, *this, max_size_in_bytes);
        };
    }

    if (auto maybe_reversed_type = dynamic_cast<const reversed_type_impl*>(&type)) {
        return get_atomic_value_generator(*maybe_reversed_type->underlying_type());
    }

    throw std::runtime_error(fmt::format("Don't know how to generate value for unknown type {}", type.name()));
}

value_generator::generator value_generator::get_generator(const abstract_type& type) {
    auto it = _regular_value_generators.find(&type);
    if (it != _regular_value_generators.end()) {
        return [this, gen = it->second] (std::mt19937& engine) -> data_model::mutation_description::value {
            return gen(engine, no_size_in_bytes_limit).serialize_nonnull();
        };
    }

    if (auto maybe_user_type = dynamic_cast<const user_type_impl*>(&type)) {
        if (maybe_user_type->is_multi_cell()) {
            return [this, maybe_user_type] (std::mt19937& engine) -> data_model::mutation_description::value {
                return generate_user_value(engine, *maybe_user_type, *this);
            };
        } else {
            return [this, maybe_user_type] (std::mt19937& engine) -> data_model::mutation_description::value {
                return generate_frozen_user_value(engine, *maybe_user_type, *this, no_size_in_bytes_limit).serialize_nonnull();
            };
        }
    }

    if (auto maybe_tuple_type = dynamic_cast<const tuple_type_impl*>(&type)) {
        return [this, maybe_tuple_type] (std::mt19937& engine) -> data_model::mutation_description::value {
            return generate_frozen_tuple_value(engine, *maybe_tuple_type, *this, no_size_in_bytes_limit).serialize_nonnull();
        };
    }

    if (auto maybe_list_type = dynamic_cast<const list_type_impl*>(&type)) {
        if (maybe_list_type->is_multi_cell()) {
            return [this, maybe_list_type] (std::mt19937& engine) -> data_model::mutation_description::value {
                return generate_list_value(engine, *maybe_list_type, *this);
            };
        } else {
            return [this, maybe_list_type] (std::mt19937& engine) -> data_model::mutation_description::value {
                return generate_frozen_list_value(engine, *maybe_list_type, *this, no_size_in_bytes_limit).serialize_nonnull();
            };
        }
    }

    if (auto maybe_set_type = dynamic_cast<const set_type_impl*>(&type)) {
        if (maybe_set_type->is_multi_cell()) {
            return [this, maybe_set_type] (std::mt19937& engine) -> data_model::mutation_description::value {
                return generate_set_value(engine, *maybe_set_type, *this);
            };
        } else {
            return [this, maybe_set_type] (std::mt19937& engine) -> data_model::mutation_description::value {
                return generate_frozen_set_value(engine, *maybe_set_type, *this, no_size_in_bytes_limit).serialize_nonnull();
            };
        }
    }

    if (auto maybe_map_type = dynamic_cast<const map_type_impl*>(&type)) {
        if (maybe_map_type->is_multi_cell()) {
            return [this, maybe_map_type] (std::mt19937& engine) -> data_model::mutation_description::value {
                return generate_map_value(engine, *maybe_map_type, *this);
            };
        } else {
            return [this, maybe_map_type] (std::mt19937& engine) -> data_model::mutation_description::value {
                return generate_frozen_map_value(engine, *maybe_map_type, *this, no_size_in_bytes_limit).serialize_nonnull();
            };
        }
    }

    if (auto maybe_reversed_type = dynamic_cast<const reversed_type_impl*>(&type)) {
        return get_generator(*maybe_reversed_type->underlying_type());
    }

    throw std::runtime_error(fmt::format("Don't know how to generate value for unknown type {}", type.name()));
}

data_model::mutation_description::value value_generator::generate_value(std::mt19937& engine, const abstract_type& type) {
    return get_generator(type)(engine);
}

timestamp_generator default_timestamp_generator() {
    return [] (std::mt19937& engine, timestamp_destination, api::timestamp_type min_timestamp) {
        auto ts_dist = std::uniform_int_distribution<api::timestamp_type>(min_timestamp, api::max_timestamp);
        return ts_dist(engine);
    };
}

expiry_generator no_expiry_expiry_generator() {
    return [] (std::mt19937& engine, timestamp_destination destination) -> std::optional<expiry_info> {
        return std::nullopt;
    };
}

namespace {

schema_ptr build_random_schema(schema_registry& registry, uint32_t seed, random_schema_specification& spec) {
    auto engine = std::mt19937{seed};
    auto builder = schema_builder(registry, spec.keyspace_name(), spec.table_name(engine));

    auto pk_columns = spec.partition_key_columns(engine);
    assert(!pk_columns.empty()); // Let's not pull in boost::test here
    for (size_t pk = 0; pk < pk_columns.size(); ++pk) {
        builder.with_column(to_bytes(format("pk{}", pk)), std::move(pk_columns[pk]), column_kind::partition_key);
    }

    if (const auto ck_columns = spec.clustering_key_columns(engine); !ck_columns.empty()) {
        for (size_t ck = 0; ck < ck_columns.size(); ++ck) {
            builder.with_column(to_bytes(format("ck{}", ck)), std::move(ck_columns[ck]), column_kind::clustering_key);
        }
    }

    if (const auto static_columns = spec.static_columns(engine); !static_columns.empty()) {
        for (size_t s = 0; s < static_columns.size(); ++s) {
            builder.with_column(to_bytes(format("s{}", s)), std::move(static_columns[s]), column_kind::static_column);
        }
    }

    const auto regular_columns = spec.regular_columns(engine);
    assert(!regular_columns.empty()); // Let's not pull in boost::test here
    for (size_t r = 0; r < regular_columns.size(); ++r) {
        builder.with_column(to_bytes(format("v{}", r)), std::move(regular_columns[r]), column_kind::regular_column);
    }

    return builder.build();
}

sstring udt_to_str(const user_type_impl& udt) {
    std::vector<sstring> fields;
    for (size_t i = 0; i < udt.field_types().size(); ++i) {
        fields.emplace_back(format("{} {}", udt.field_name_as_string(i), udt.field_type(i)->as_cql3_type().to_string()));
    }
    return format("CREATE TYPE {} (\n\t{})",
            udt.get_name_as_string(),
            boost::algorithm::join(fields, ",\n\t"));
}

// Single element overload, for convenience.
std::unordered_set<const user_type_impl*> dump_udts(data_type type) {
    if (auto maybe_user_type = dynamic_cast<const user_type_impl*>(type.get())) {
        return {maybe_user_type};
    }
    return {};
}

std::unordered_set<const user_type_impl*> dump_udts(const std::vector<data_type>& types) {
    std::unordered_set<const user_type_impl*> udts;
    for (const auto& dt : types) {
        const auto* const type = dt.get();
        if (auto maybe_user_type = dynamic_cast<const user_type_impl*>(type)) {
            udts.insert(maybe_user_type);
            udts.merge(dump_udts(maybe_user_type->field_types()));
        } else if (auto maybe_tuple_type = dynamic_cast<const tuple_type_impl*>(type)) {
            udts.merge(dump_udts(maybe_tuple_type->all_types()));
        } else if (auto maybe_list_type = dynamic_cast<const list_type_impl*>(type)) {
            udts.merge(dump_udts(maybe_list_type->get_elements_type()));
        } else if (auto maybe_set_type = dynamic_cast<const set_type_impl*>(type)) {
            udts.merge(dump_udts(maybe_set_type->get_elements_type()));
        } else if (auto maybe_map_type = dynamic_cast<const map_type_impl*>(type)) {
            udts.merge(dump_udts(maybe_map_type->get_keys_type()));
            udts.merge(dump_udts(maybe_map_type->get_values_type()));
        } else if (auto maybe_reversed_type = dynamic_cast<const reversed_type_impl*>(type)) {
            udts.merge(dump_udts(maybe_reversed_type->underlying_type()));
        }
    }
    return udts;
}

std::vector<sstring> dump_udts(const schema& schema) {
    std::unordered_set<const user_type_impl*> udts;

    const auto cdefs_to_types = [] (const schema::const_iterator_range_type& cdefs) -> std::vector<data_type> {
        return boost::copy_range<std::vector<data_type>>(cdefs |
                boost::adaptors::transformed([] (const column_definition& cdef) { return cdef.type; }));
    };

    udts.merge(dump_udts(cdefs_to_types(schema.partition_key_columns())));
    udts.merge(dump_udts(cdefs_to_types(schema.clustering_key_columns())));
    udts.merge(dump_udts(cdefs_to_types(schema.regular_columns())));
    udts.merge(dump_udts(cdefs_to_types(schema.static_columns())));

    return boost::copy_range<std::vector<sstring>>(udts |
            boost::adaptors::transformed([] (const user_type_impl* const udt) { return udt_to_str(*udt); }));
}

std::vector<sstring> columns_specs(schema_ptr schema, column_kind kind) {
    const auto count = schema->columns_count(kind);
    if (!count) {
        return {};
    }

    std::vector<sstring> col_specs;
    for (column_count_type c = 0; c < count; ++c) {
        const auto& cdef = schema->column_at(kind, c);
        col_specs.emplace_back(format("{} {}{}", cdef.name_as_cql_string(), cdef.type->as_cql3_type().to_string(),
                kind == column_kind::static_column ? " static" : ""));
    }
    return col_specs;
}

std::vector<sstring> column_names(schema_ptr schema, column_kind kind) {
    const auto count = schema->columns_count(kind);
    if (!count) {
        return {};
    }

    std::vector<sstring> col_names;
    for (column_count_type c = 0; c < count; ++c) {
        const auto& cdef = schema->column_at(kind, c);
        col_names.emplace_back(cdef.name_as_cql_string());
    }
    return col_names;
}

void decorate_with_timestamps(const schema& schema, std::mt19937& engine, timestamp_generator& ts_gen, expiry_generator exp_gen,
        data_model::mutation_description::value& value) {
    std::visit(
            make_visitor(
                    [&] (data_model::mutation_description::atomic_value& v) {
                        v.timestamp = ts_gen(engine, timestamp_destination::cell_timestamp, api::min_timestamp);
                        if (auto expiry_opt = exp_gen(engine, timestamp_destination::cell_timestamp)) {
                            v.expiring = data_model::mutation_description::expiry_info{expiry_opt->ttl, expiry_opt->expiry_point};
                        }
                    },
                    [&] (data_model::mutation_description::collection& c) {
                        if (auto ts = ts_gen(engine, timestamp_destination::collection_tombstone, api::min_timestamp);
                                ts != api::missing_timestamp) {
                            if (ts == api::max_timestamp) {
                                // Caveat: leave some headroom for the cells
                                // having a timestamp larger than the
                                // tombstone's.
                                ts--;
                            }
                            auto expiry_opt = exp_gen(engine, timestamp_destination::collection_tombstone);
                            const auto deletion_time = expiry_opt ? expiry_opt->expiry_point : gc_clock::now();
                            c.tomb = tombstone(ts, deletion_time);
                        }
                        for (auto& [ key, value ] : c.elements) {
                            value.timestamp = ts_gen(engine, timestamp_destination::collection_cell_timestamp, c.tomb.timestamp);
                            assert(!c.tomb || value.timestamp > c.tomb.timestamp);
                            if (auto expiry_opt = exp_gen(engine, timestamp_destination::collection_cell_timestamp)) {
                                value.expiring = data_model::mutation_description::expiry_info{expiry_opt->ttl, expiry_opt->expiry_point};
                            }
                        }
                    }),
            value);
}

} // anonymous namespace

data_model::mutation_description::key random_schema::make_key(uint32_t n, value_generator& gen, schema::const_iterator_range_type columns,
        size_t max_size_in_bytes) {
    std::mt19937 engine(n);

    const size_t max_component_size = max_size_in_bytes / std::distance(columns.begin(), columns.end());

    std::vector<bytes> key;
    for (const auto& cdef : columns) {
        key.emplace_back(gen.generate_atomic_value(engine, *cdef.type, max_component_size).serialize_nonnull());
    }

    return key;
}

data_model::mutation_description::key random_schema::make_partition_key(uint32_t n, value_generator& gen) const {
    return make_key(n, gen, _schema->partition_key_columns(), std::numeric_limits<partition_key::compound::element_type::size_type>::max());
}

data_model::mutation_description::key random_schema::make_clustering_key(uint32_t n, value_generator& gen) const {
    assert(_schema->clustering_key_size() > 0);
    return make_key(n, gen, _schema->clustering_key_columns(), std::numeric_limits<clustering_key::compound::element_type::size_type>::max());
}

random_schema::random_schema(uint32_t seed, random_schema_specification& spec)
    : _schema(build_random_schema(_registry, seed, spec)) {
}

sstring random_schema::cql() const {
    auto udts = dump_udts(*_schema);

    sstring udts_str;
    if (!udts.empty()) {
        udts_str = boost::algorithm::join(udts, "\n");
    }

    std::vector<sstring> col_specs;
    for (auto kind : {column_kind::partition_key, column_kind::clustering_key, column_kind::regular_column, column_kind::static_column}) {
        auto cols = columns_specs(_schema, kind);
        std::move(cols.begin(), cols.end(), std::back_inserter(col_specs));
    }

    sstring primary_key;
    auto partition_column_names = column_names(_schema, column_kind::partition_key);
    auto clustering_key_names = column_names(_schema, column_kind::clustering_key);
    if (clustering_key_names.empty()) {
        primary_key = format("{} ({})", boost::algorithm::join(partition_column_names, ", "), boost::algorithm::join(clustering_key_names, ", "));
    } else {
        primary_key = format("{}", boost::algorithm::join(partition_column_names, ", "));
    }

    return format(
            "{}\nCREATE TABLE {}.{} (\n\t{}\n\tPRIMARY KEY ({}))",
            udts_str,
            _schema->ks_name(),
            _schema->cf_name(),
            boost::algorithm::join(col_specs, ",\n\t"),
            primary_key);
}

data_model::mutation_description::key random_schema::make_pkey(uint32_t n) {
    value_generator g;
    return make_partition_key(n, g);
}

std::vector<data_model::mutation_description::key> random_schema::make_pkeys(size_t n) {
    std::set<dht::decorated_key, dht::ring_position_less_comparator> keys{dht::ring_position_less_comparator{*_schema}};
    value_generator val_gen;

    uint32_t i{0};
    while (keys.size() < n) {
        keys.emplace(dht::decorate_key(*_schema, partition_key::from_exploded(make_partition_key(i, val_gen))));
        ++i;
    }

    return boost::copy_range<std::vector<data_model::mutation_description::key>>(keys |
            boost::adaptors::transformed([] (const dht::decorated_key& dkey) { return dkey.key().explode(); }));
}

data_model::mutation_description::key random_schema::make_ckey(uint32_t n) {
    value_generator g;
    return make_clustering_key(n, g);
}

std::vector<data_model::mutation_description::key> random_schema::make_ckeys(size_t n) {
    std::set<clustering_key, clustering_key::less_compare> keys{clustering_key::less_compare{*_schema}};
    value_generator val_gen;

    for (uint32_t i = 0; i < n; i++) {
        keys.emplace(clustering_key::from_exploded(make_clustering_key(i, val_gen)));
    }

    return boost::copy_range<std::vector<data_model::mutation_description::key>>(keys |
            boost::adaptors::transformed([] (const clustering_key& ckey) { return ckey.explode(); }));
}

data_model::mutation_description random_schema::new_mutation(data_model::mutation_description::key pkey) {
    return data_model::mutation_description(std::move(pkey));
}

data_model::mutation_description random_schema::new_mutation(uint32_t n) {
    return new_mutation(make_pkey(n));
}

void random_schema::set_partition_tombstone(std::mt19937& engine, data_model::mutation_description& md, timestamp_generator ts_gen,
        expiry_generator exp_gen) {
    if (const auto ts = ts_gen(engine, timestamp_destination::partition_tombstone, api::min_timestamp); ts != api::missing_timestamp) {
        auto expiry_opt = exp_gen(engine, timestamp_destination::partition_tombstone);
        const auto deletion_time = expiry_opt ? expiry_opt->expiry_point : gc_clock::now();
        md.set_partition_tombstone(tombstone(ts, deletion_time));
    }
}

void random_schema::add_row(std::mt19937& engine, data_model::mutation_description& md, data_model::mutation_description::key ckey,
        timestamp_generator ts_gen, expiry_generator exp_gen) {
    value_generator gen;
    for (const auto& cdef : _schema->regular_columns()) {
        auto value = gen.generate_value(engine, *cdef.type);
        decorate_with_timestamps(*_schema, engine, ts_gen, exp_gen, value);
        md.add_clustered_cell(ckey, cdef.name_as_text(), std::move(value));
    }
    if (auto ts = ts_gen(engine, timestamp_destination::row_marker, api::min_timestamp); ts != api::missing_timestamp) {
        if (auto expiry_opt = exp_gen(engine, timestamp_destination::row_marker)) {
            md.add_clustered_row_marker(ckey, tests::data_model::mutation_description::row_marker(ts, expiry_opt->ttl, expiry_opt->expiry_point));
        } else {
            md.add_clustered_row_marker(ckey, ts);
        }
    }
    if (auto ts = ts_gen(engine, timestamp_destination::row_tombstone, api::min_timestamp); ts != api::missing_timestamp) {
        auto expiry_opt = exp_gen(engine, timestamp_destination::row_tombstone);
        const auto deletion_time = expiry_opt ? expiry_opt->expiry_point : gc_clock::now();
        md.add_clustered_row_tombstone(ckey, row_tombstone{tombstone{ts, deletion_time}});
    }
}

void random_schema::add_row(std::mt19937& engine, data_model::mutation_description& md, uint32_t n, timestamp_generator ts_gen,
        expiry_generator exp_gen) {
    add_row(engine, md, make_ckey(n), std::move(ts_gen), std::move(exp_gen));
}

void random_schema::add_static_row(std::mt19937& engine, data_model::mutation_description& md, timestamp_generator ts_gen, expiry_generator exp_gen) {
    value_generator gen;
    for (const auto& cdef : _schema->static_columns()) {
        auto value = gen.generate_value(engine, *cdef.type);
        decorate_with_timestamps(*_schema, engine, ts_gen, exp_gen, value);
        md.add_static_cell(cdef.name_as_text(), std::move(value));
    }
}

void random_schema::delete_range(
        std::mt19937& engine,
        data_model::mutation_description& md,
        nonwrapping_range<data_model::mutation_description::key> range,
        timestamp_generator ts_gen,
        expiry_generator exp_gen) {
    auto expiry_opt = exp_gen(engine, timestamp_destination::range_tombstone);
    const auto deletion_time = expiry_opt ? expiry_opt->expiry_point : gc_clock::now();
    md.add_range_tombstone(std::move(range), tombstone{ts_gen(engine, timestamp_destination::range_tombstone, api::min_timestamp), deletion_time});
}

future<std::vector<mutation>> generate_random_mutations(
        tests::random_schema& random_schema,
        timestamp_generator ts_gen,
        expiry_generator exp_gen,
        std::uniform_int_distribution<size_t> partition_count_dist,
        std::uniform_int_distribution<size_t> clustering_row_count_dist,
        std::uniform_int_distribution<size_t> range_tombstone_count_dist) {
    auto engine = std::mt19937(tests::random::get_int<uint32_t>());
    const auto schema_has_clustering_columns = random_schema.schema()->clustering_key_size() > 0;
    const auto partition_count = partition_count_dist(engine);
    std::vector<mutation> muts;
    muts.reserve(partition_count);
    return do_with(std::move(engine), std::move(muts), [=, &random_schema] (std::mt19937& engine,
            std::vector<mutation>& muts) mutable {
        auto r = boost::irange(size_t{0}, partition_count);
        return do_for_each(r.begin(), r.end(), [=, &random_schema, &engine, &muts] (size_t pk) mutable {
            auto mut = random_schema.new_mutation(pk);
            random_schema.set_partition_tombstone(engine, mut, ts_gen, exp_gen);
            random_schema.add_static_row(engine, mut, ts_gen, exp_gen);

            if (!schema_has_clustering_columns) {
                muts.emplace_back(mut.build(random_schema.schema()));
                return;
            }

            auto ckeys = random_schema.make_ckeys(clustering_row_count_dist(engine));
            const auto clustering_row_count = ckeys.size();
            for (uint32_t ck = 0; ck < clustering_row_count; ++ck) {
                random_schema.add_row(engine, mut, ckeys[ck], ts_gen, exp_gen);
            }

            for (size_t i = 0; i < 4; ++i) {
                const auto a = tests::random::get_int<size_t>(0, ckeys.size() - 1, engine);
                const auto b = tests::random::get_int<size_t>(0, ckeys.size() - 1, engine);
                random_schema.delete_range(
                        engine,
                        mut,
                        nonwrapping_range<tests::data_model::mutation_description::key>::make(ckeys.at(std::min(a, b)), ckeys.at(std::max(a, b))),
                        ts_gen,
                        exp_gen);
            }
            muts.emplace_back(mut.build(random_schema.schema()));
        }).then([&random_schema, &muts] () mutable {
            boost::sort(muts, [s = random_schema.schema()] (const mutation& a, const mutation& b) {
                return a.decorated_key().less_compare(*s, b.decorated_key());
            });
            auto range = boost::unique(muts, [s = random_schema.schema()] (const mutation& a, const mutation& b) {
                return a.decorated_key().equal(*s, b.decorated_key());
            });
            muts.erase(range.end(), muts.end());
            return std::move(muts);
        });
    });
}

} // namespace tests
