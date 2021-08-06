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
 */

/*
 * Copyright (C) 2021-present ScyllaDB
 *
 * Modified by ScyllaDB
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

#pragma once

#include "cql3/values.hh"
#include "types/list.hh"
#include "types/set.hh"
#include "types/map.hh"
#include "types/tuple.hh"
#include "types/user.hh"

namespace cql3 {
    // A value represented by empty bytes()
    // Created for example by using blobasint(0x) in cqlsh
    struct empty_value {
    };

    struct bool_value {
        bool value;

        template<FragmentedView View>
        static bool_value from_serialized(View serialized_bytes);
    };

    struct int8_value {
        int8_t value;

        template<FragmentedView View>
        static int8_value from_serialized(View serialized_bytes);
    };

    struct int16_value {
        int16_t value;

        template<FragmentedView View>
        static int16_value from_serialized(View serialized_bytes);
    };

    struct int32_value {
        int32_t value;

        template<FragmentedView View>
        static int32_value from_serialized(View serialized_bytes);
    };

    struct int64_value {
        int64_t value;

        template<FragmentedView View>
        static int64_value from_serialized(View serialized_bytes);
    };

    struct counter_value {
        int64_t value;

        template<FragmentedView View>
        static counter_value from_serialized(View serialized_bytes);
    };

    struct varint_value {
        managed_bytes value;

        template<FragmentedView View>
        static varint_value from_serialized(View serialized_bytes);
    };

    struct float_value {
        float value;

        template<FragmentedView View>
        static float_value from_serialized(View serialized_bytes);
    };

    struct double_value {
        double value;

        template<FragmentedView View>
        static double_value from_serialized(View serialized_bytes);
    };

    struct decimal_value {
        managed_bytes value;

        template<FragmentedView View>
        static decimal_value from_serialized(View serialized_bytes);
    };

    struct ascii_value {
        managed_bytes value;

        template<FragmentedView View>
        static ascii_value from_serialized(View serialized_bytes);
    };

    struct utf8_value {
        managed_bytes value;

        template<FragmentedView View>
        static utf8_value from_serialized(View serialized_bytes);
    };

    struct date_value {
        managed_bytes value;

        template<FragmentedView View>
        static date_value from_serialized(View serialized_bytes);
    };

    struct simple_date_value {
        managed_bytes value;

        template<FragmentedView View>
        static simple_date_value from_serialized(View serialized_bytes);
    };

    struct duration_value {
        managed_bytes value;

        template<FragmentedView View>
        static duration_value from_serialized(View serialized_bytes);
    };

    struct time_value {
        managed_bytes value;

        template<FragmentedView View>
        static time_value from_serialized(View serialized_bytes);
    };

    struct timestamp_value {
        managed_bytes value;

        template<FragmentedView View>
        static timestamp_value from_serialized(View serialized_bytes);
    };

    struct timeuuid_value {
        managed_bytes value;

        template<FragmentedView View>
        static timeuuid_value from_serialized(View serialized_bytes);
    };

    struct blob_value {
        managed_bytes value;

        template<FragmentedView View>
        static blob_value from_serialized(View serialized_bytes);
    };

    struct inet_value {
        managed_bytes value;

        template<FragmentedView View>
        static inet_value from_serialized(View serialized_bytes);
    };

    struct uuid_value {
        managed_bytes value;

        template<FragmentedView View>
        static uuid_value from_serialized(View serialized_bytes);
    };

    struct tuple_value {
        std::vector<std::variant<managed_bytes, null_value>> elements;
        // Not every element in a tuple has a type, because in some cases the tuple_type_impl is empty.
        // TODO: Find the types in these cases.
        std::vector<std::optional<data_type>> elements_types;

        template<FragmentedView View>
        static tuple_value from_serialized(View serialized_bytes, cql_serialization_format, const tuple_type_impl&);
    };

    struct list_value {
        utils::chunked_vector<std::variant<managed_bytes, null_value>> elements;
        data_type elements_type;

        template<FragmentedView View>
        static list_value from_serialized(View serialized_bytes, cql_serialization_format, const list_type_impl&);
    };

    struct set_value {
        std::set<managed_bytes, serialized_compare> elements;
        data_type elements_type;

        template<FragmentedView View>
        static set_value from_serialized(View serialized_bytes, cql_serialization_format, const set_type_impl&);
    };

    struct map_value {
        std::map<managed_bytes, managed_bytes, serialized_compare> elements;
        data_type keys_type;
        data_type values_type;

        template<FragmentedView View>
        static map_value from_serialized(View serialized_bytes, cql_serialization_format, const map_type_impl&);
    };

    struct user_type_value {
        std::vector<std::variant<managed_bytes, null_value>> field_values;
        std::vector<data_type> field_values_types;

        template<FragmentedView View>
        static user_type_value from_serialized(View serialized_bytes, cql_serialization_format, const user_type_impl&);
    };

    using cql_value = std::variant<
        empty_value,
        unset_value,
        null_value,
        bool_value,
        int8_value,
        int16_value,
        int32_value,
        int64_value,
        counter_value,
        varint_value,
        float_value,
        double_value,
        decimal_value,
        ascii_value,
        utf8_value,
        date_value,
        simple_date_value,
        duration_value,
        time_value,
        timestamp_value,
        timeuuid_value,
        blob_value,
        inet_value,
        uuid_value,
        tuple_value,
        list_value,
        set_value,
        map_value,
        user_type_value>;

    template<FragmentedView View>
    cql_value cql_value_from_serialized(View serialized_bytes, cql_serialization_format, const abstract_type&);

    cql_value cql_value_from_raw_value(const cql3::raw_value&, cql_serialization_format, const abstract_type&);


    // A cql_value that is ordered in reverse order
    struct reversed_cql_value {
        cql_value value;
    };

    using ordered_cql_value = std::variant<cql_value, reversed_cql_value>;

    // If should_reverse is true returns reversed_cql_value, otherwise just the cql_value
    ordered_cql_value reverse_if_needed(cql_value&& value, bool should_reverse);

    // Takes the cql_value out of ordered_cql_value
    cql_value into_cql_value(ordered_cql_value&&);


    // Converts a cql_value to its serialized representation
    raw_value to_raw_value(const cql_value&, cql_serialization_format);
    
    raw_value to_raw_value(const empty_value&);
    raw_value to_raw_value(const unset_value&);
    raw_value to_raw_value(const null_value&);
    raw_value to_raw_value(const bool_value&);
    raw_value to_raw_value(const int8_value&);
    raw_value to_raw_value(const int16_value&);
    raw_value to_raw_value(const int32_value&);
    raw_value to_raw_value(const int64_value&);
    raw_value to_raw_value(const counter_value&);
    raw_value to_raw_value(const varint_value&);
    raw_value to_raw_value(const float_value&);
    raw_value to_raw_value(const double_value&);
    raw_value to_raw_value(const decimal_value&);
    raw_value to_raw_value(const ascii_value&);
    raw_value to_raw_value(const utf8_value&);
    raw_value to_raw_value(const date_value&);
    raw_value to_raw_value(const simple_date_value&);
    raw_value to_raw_value(const duration_value&);
    raw_value to_raw_value(const time_value&);
    raw_value to_raw_value(const timestamp_value&);
    raw_value to_raw_value(const timeuuid_value&);
    raw_value to_raw_value(const blob_value&);
    raw_value to_raw_value(const inet_value&);
    raw_value to_raw_value(const uuid_value&);
    raw_value to_raw_value(const tuple_value&);
    raw_value to_raw_value(const list_value&, cql_serialization_format);
    raw_value to_raw_value(const set_value&, cql_serialization_format);
    raw_value to_raw_value(const map_value&, cql_serialization_format);
    raw_value to_raw_value(const user_type_value&);


    template<FragmentedView View>
    bool_value bool_value::from_serialized(View serialized_bytes) {
        return bool_value {
            .value = read_simple_exactly<int8_t>(serialized_bytes) != 0
        };
    }

    template<FragmentedView View>
    int8_value int8_value::from_serialized(View serialized_bytes) {
        return int8_value {
            .value = read_simple_exactly<int8_t>(serialized_bytes)
        };
    }

    template<FragmentedView View>
    int16_value int16_value::from_serialized(View serialized_bytes) {
        return int16_value {
            .value = read_simple_exactly<int16_t>(serialized_bytes)
        };
    }

    template<FragmentedView View>
    int32_value int32_value::from_serialized(View serialized_bytes) {
        return int32_value {
            .value = read_simple_exactly<int32_t>(serialized_bytes)
        };
    }

    template<FragmentedView View>
    int64_value int64_value::from_serialized(View serialized_bytes) {
        return int64_value {
            .value = read_simple_exactly<int64_t>(serialized_bytes)
        };
    }

    template<FragmentedView View>
    counter_value counter_value::from_serialized(View serialized_bytes) {
        return counter_value {
            .value = read_simple_exactly<int64_t>(serialized_bytes)
        };
    }

    template<FragmentedView View>
    varint_value varint_value::from_serialized(View serialized_bytes) {
        return varint_value {
            .value = managed_bytes(serialized_bytes)
        };
    }

    template<FragmentedView View>
    float_value float_value::from_serialized(View serialized_bytes) {
        static_assert(sizeof(float) == sizeof(int32_t));

        int32_t int_val = read_simple_exactly<int32_t>(serialized_bytes);
        float value;
        memcpy(&value, &int_val, sizeof(value));
        return float_value {
            .value = value
        };
    }

    template<FragmentedView View>
    double_value double_value::from_serialized(View serialized_bytes) {
        static_assert(sizeof(double) == sizeof(int64_t));

        int64_t int_val = read_simple_exactly<int64_t>(serialized_bytes);
        double value;
        memcpy(&value, &int_val, sizeof(value));
        return double_value {
            .value = value
        };
    }

    template<FragmentedView View>
    decimal_value decimal_value::from_serialized(View serialized_bytes) {
        return decimal_value {
            .value = managed_bytes(serialized_bytes)
        };
    }

    template<FragmentedView View>
    ascii_value ascii_value::from_serialized(View serialized_bytes) {
        return ascii_value {
            .value = managed_bytes(serialized_bytes)
        };
    }

    template<FragmentedView View>
    utf8_value utf8_value::from_serialized(View serialized_bytes) {
        return utf8_value {
            .value = managed_bytes(serialized_bytes)
        };
    }

    template<FragmentedView View>
    date_value date_value::from_serialized(View serialized_bytes) {
        return date_value {
            .value = managed_bytes(serialized_bytes)
        };
    }

    template<FragmentedView View>
    simple_date_value simple_date_value::from_serialized(View serialized_bytes) {
        return simple_date_value {
            .value = managed_bytes(serialized_bytes)
        };
    }

    template<FragmentedView View>
    duration_value duration_value::from_serialized(View serialized_bytes) {
        return duration_value {
            .value = managed_bytes(serialized_bytes)
        };
    }

    template<FragmentedView View>
    time_value time_value::from_serialized(View serialized_bytes) {
        return time_value {
            .value = managed_bytes(serialized_bytes)
        };
    }

    template<FragmentedView View>
    timestamp_value timestamp_value::from_serialized(View serialized_bytes) {
        return timestamp_value {
            .value = managed_bytes(serialized_bytes)
        };
    }

    template<FragmentedView View>
    timeuuid_value timeuuid_value::from_serialized(View serialized_bytes) {
        return timeuuid_value {
            .value = managed_bytes(serialized_bytes)
        };
    }

    template<FragmentedView View>
    blob_value blob_value::from_serialized(View serialized_bytes) {
        return blob_value {
            .value = managed_bytes(serialized_bytes)
        };
    }

    template<FragmentedView View>
    inet_value inet_value::from_serialized(View serialized_bytes) {
        return inet_value {
            .value = managed_bytes(serialized_bytes)
        };
    }

    template<FragmentedView View>
    uuid_value uuid_value::from_serialized(View serialized_bytes) {
        return uuid_value {
            .value = managed_bytes(serialized_bytes)
        };
    }

    template <FragmentedView View>
    tuple_value tuple_value::from_serialized(View serialized_bytes,
                                             cql_serialization_format sf,
                                             const tuple_type_impl& ttype) {

        std::vector<managed_bytes_opt> elements = ttype.split_fragmented(serialized_bytes);

        std::vector<std::variant<managed_bytes, null_value>> new_elements;
        elements.reserve(elements.size());
        for (managed_bytes_opt& element : elements) {
            if (element.has_value()) {
                new_elements.emplace_back(std::move(*element));
            } else {
                new_elements.emplace_back(null_value{});
            }
        }

        std::vector<std::optional<data_type>> elements_types;
        elements_types.reserve(elements.size());

        for (size_t i = 0; i < elements_types.size(); i++) {
            if (i < ttype.all_types().size()) {
                elements_types.emplace_back(ttype.all_types()[i]);
            } else {
                elements_types.emplace_back(std::nullopt);
            }
        }

        return tuple_value {
            .elements = std::move(new_elements),
            .elements_types = std::move(elements_types)
        };
    }

    template <FragmentedView View>
    list_value list_value::from_serialized(View serialized_bytes,
                                           cql_serialization_format sf,
                                           const list_type_impl& ltype) {
        utils::chunked_vector<std::variant<managed_bytes, null_value>> elements;
        if (sf.collection_format_unchanged()) {
            utils::chunked_vector<managed_bytes> tmp = partially_deserialize_listlike(serialized_bytes, sf);
            elements.reserve(tmp.size());
            for (auto&& element : tmp) {
                elements.emplace_back(std::move(element));
            }
        } else [[unlikely]] {
            auto l = value_cast<list_type_impl::native_type>(ltype.deserialize(serialized_bytes, sf));
            elements.reserve(l.size());
            for (auto&& element : l) {
                // elements can be null in lists that represent a set of IN values
                if (element.is_null()) {
                    elements.emplace_back(null_value{});
                } else {
                    elements.emplace_back(managed_bytes(ltype.get_elements_type()->decompose(element)));
                }
            }
        }

        return list_value {
            .elements = std::move(elements),
            .elements_type = ltype.get_elements_type()
        };
    }

    template <FragmentedView View>
    set_value set_value::from_serialized(View serialized_bytes,
                                         cql_serialization_format sf,
                                         const set_type_impl& stype) {
        std::set<managed_bytes, serialized_compare> elements(stype.get_elements_type()->as_less_comparator());
        if (sf.collection_format_unchanged()) {
            utils::chunked_vector<managed_bytes> tmp = partially_deserialize_listlike(serialized_bytes, sf);
            for (auto&& element : tmp) {
                elements.insert(std::move(element));
            }
        } else [[unlikely]] {
            auto s = value_cast<set_type_impl::native_type>(stype.deserialize(serialized_bytes, sf));
            for (auto&& element : s) {
                elements.insert(elements.end(), managed_bytes(stype.get_elements_type()->decompose(element)));
            }
        }

        return set_value {
            .elements = std::move(elements),
            .elements_type = stype.get_elements_type()
        };
    }

    template <FragmentedView View>
    map_value map_value::from_serialized(View serialized_bytes,
                                         cql_serialization_format sf,
                                         const map_type_impl& mtype) {
        std::map<managed_bytes, managed_bytes, serialized_compare> elems(mtype.get_keys_type()->as_less_comparator());
        if (sf.collection_format_unchanged()) {
            std::vector<std::pair<managed_bytes, managed_bytes>> tmp = partially_deserialize_map(serialized_bytes, sf);
            for (auto&& key_value : tmp) {
                elems.insert(std::move(key_value));
            }
        } else [[unlikely]] {
            auto m = value_cast<map_type_impl::native_type>(mtype.deserialize(serialized_bytes, sf));
            for (auto&& e : m) {
                elems.emplace(mtype.get_keys_type()->decompose(e.first),
                              mtype.get_values_type()->decompose(e.second));
            }
        }

        return map_value {
            .elements = elems,
            .keys_type = mtype.get_keys_type(),
            .values_type = mtype.get_values_type()
        };
    }

    template <FragmentedView View>
    user_type_value user_type_value::from_serialized(View serialized_bytes,
                                                     cql_serialization_format sf,
                                                     const user_type_impl& utype) {
        std::vector<managed_bytes_opt> elements = utype.split_fragmented(serialized_bytes);
        if (elements.size() > utype.size()) {
            throw std::runtime_error("User Defined Type value contained too many fields");
        }

        std::vector<std::variant<managed_bytes, null_value>> field_values;
        field_values.resize(elements.size());

        for (managed_bytes_opt& element : elements) {
            if (element.has_value()) {
                field_values.emplace_back(std::move(*element));
            } else {
                field_values.emplace_back(null_value{});
            }
        }

        if (utype.all_types().size() < field_values.size()) {
            throw std::runtime_error("user_type_value::from_serialized the type doesn't have enough types");
        }

        std::vector<data_type> field_values_types(utype.all_types());
        field_values_types.resize(field_values.size());

        return user_type_value {
            .field_values = std::move(field_values),
            .field_values_types = std::move(field_values_types)
        };
    }

    // Decides whether serialized bytes of size 0 mean empty_value or not
    bool is_0_bytes_value_empty_value(abstract_type::kind type_kind);

    template<FragmentedView View>
    cql_value cql_value_from_serialized(View serialized_bytes,
                                        cql_serialization_format sf,
                                        const abstract_type& val_type) {
        
        if (serialized_bytes.empty() && is_0_bytes_value_empty_value(val_type.get_kind())) {
            return cql_value(empty_value{});
        }

        switch (val_type.get_kind()) {
            case abstract_type::kind::ascii:
                return cql_value(ascii_value::from_serialized(serialized_bytes));

            case abstract_type::kind::boolean:
                return cql_value(bool_value::from_serialized(serialized_bytes));

            case abstract_type::kind::byte:
                return cql_value(int8_value::from_serialized(serialized_bytes));

            case abstract_type::kind::bytes:
                return cql_value(blob_value::from_serialized(serialized_bytes));

            case abstract_type::kind::counter:
                return cql_value(counter_value::from_serialized(serialized_bytes));

            case abstract_type::kind::date:
                return cql_value(date_value::from_serialized(serialized_bytes));

            case abstract_type::kind::decimal:
                return cql_value(decimal_value::from_serialized(serialized_bytes));

            case abstract_type::kind::double_kind:
                return cql_value(double_value::from_serialized(serialized_bytes));

            case abstract_type::kind::duration:
                return cql_value(duration_value::from_serialized(serialized_bytes));

            case abstract_type::kind::float_kind:
                return cql_value(float_value::from_serialized(serialized_bytes));

            case abstract_type::kind::inet:
                return cql_value(inet_value::from_serialized(serialized_bytes));

            case abstract_type::kind::int32:
                return cql_value(int32_value::from_serialized(serialized_bytes));

            case abstract_type::kind::long_kind:
                return cql_value(int64_value::from_serialized(serialized_bytes));

            case abstract_type::kind::short_kind:
                return cql_value(int16_value::from_serialized(serialized_bytes));

            case abstract_type::kind::simple_date:
                return cql_value(simple_date_value::from_serialized(serialized_bytes));

            case abstract_type::kind::time:
                return cql_value(time_value::from_serialized(serialized_bytes));

            case abstract_type::kind::timestamp:
                return cql_value(timestamp_value::from_serialized(serialized_bytes));

            case abstract_type::kind::timeuuid:
                return cql_value(timeuuid_value::from_serialized(serialized_bytes));

            case abstract_type::kind::utf8:
                return cql_value(utf8_value::from_serialized(serialized_bytes));

            case abstract_type::kind::uuid:
                return cql_value(uuid_value::from_serialized(serialized_bytes));

            case abstract_type::kind::varint:
                return cql_value(varint_value::from_serialized(serialized_bytes));

            case abstract_type::kind::list: {
                const list_type_impl* list_val_type = dynamic_cast<const list_type_impl*>(&val_type);
                if (list_val_type == nullptr) {
                    throw std::runtime_error("cql_value_from_serialized - val_type is not list_type_impl");
                }
                return cql_value(list_value::from_serialized(serialized_bytes, sf, *list_val_type));
            }

            case abstract_type::kind::set: {
                const set_type_impl* set_val_type = dynamic_cast<const set_type_impl*>(&val_type);
                if (set_val_type == nullptr) {
                    throw std::runtime_error("cql_value_from_serialized - val_type is not set_type_impl");
                }
                return cql_value(set_value::from_serialized(serialized_bytes, sf, *set_val_type));
            }

            case abstract_type::kind::map: {
                const map_type_impl* map_val_type = dynamic_cast<const map_type_impl*>(&val_type);
                if (map_val_type == nullptr) {
                    throw std::runtime_error("cql_value_from_serialized - val_type is not map_type_impl");
                }
                return cql_value(map_value::from_serialized(serialized_bytes, sf, *map_val_type));
            }

            case abstract_type::kind::tuple: {
                const tuple_type_impl* tuple_val_type = dynamic_cast<const tuple_type_impl*>(&val_type);
                if (tuple_val_type == nullptr) {
                    throw std::runtime_error("cql_value_from_serialized - val_type is not tuple_type_impl");
                }
                return cql_value(tuple_value::from_serialized(serialized_bytes, sf, *tuple_val_type));
            }

            case abstract_type::kind::user: {
                const user_type_impl* user_val_type = dynamic_cast<const user_type_impl*>(&val_type);
                if (user_val_type == nullptr) {
                    throw std::runtime_error("cql_value_from_serialized - val_type is not user_type_impl");
                }
                return cql_value(user_type_value::from_serialized(serialized_bytes, sf, *user_val_type));
            }

            case abstract_type::kind::empty:
                return cql_value(empty_value{});

            case abstract_type::kind::reversed:
                throw std::runtime_error("cql_value_from_serialized reversed_type is not allowed");

            default:
                throw std::runtime_error(
                    fmt::format("constants::value::to_cql_value - unhandled type: {}", val_type.name()));
        }
    }
}
