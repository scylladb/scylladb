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

#include "cql_value.hh"
#include "utils/overloaded_functor.hh"

namespace cql3 {
ordered_cql_value reverse_if_needed(cql_value&& value, bool should_reverse) {
    if (should_reverse) {
        return ordered_cql_value(reversed_cql_value{std::move(value)});
    } else {
        return ordered_cql_value(std::move(value));
    }
}

// Takes the cql_value out of ordered_cql_value
cql_value into_cql_value(ordered_cql_value&& ordered_cql_val) {
    return std::visit(overloaded_functor{
        [](cql_value&& val) { return std::move(val); },
        [](reversed_cql_value&& val) { return std::move(val.value); }
    }, std::move(ordered_cql_val));
}

cql_value cql_value_from_raw_value(const cql3::raw_value& raw_val,
                                   cql_serialization_format sf,
                                   const abstract_type& val_type) {
    if (raw_val.is_null()) {
        return cql_value(null_value{});
    }

    if (raw_val.is_unset_value()) {
        return cql_value(unset_value{});
    }
    // Now we know that raw_val.is_value()

    return raw_val.to_view().with_value([&](const FragmentedView auto& view) {
        return cql_value_from_serialized(view, sf, val_type);
    });
}

// Decides whether serialized bytes of size 0 mean empty_value or not
bool is_0_bytes_value_empty_value(abstract_type::kind type_kind) {
    switch (type_kind) {
        case abstract_type::kind::utf8:
        case abstract_type::kind::bytes:
        case abstract_type::kind::ascii:
        case abstract_type::kind::tuple:
            return false;

        case abstract_type::kind::boolean:
        case abstract_type::kind::byte:
        case abstract_type::kind::short_kind:
        case abstract_type::kind::int32:
        case abstract_type::kind::long_kind:
        case abstract_type::kind::float_kind:
        case abstract_type::kind::double_kind:
        case abstract_type::kind::counter:
        case abstract_type::kind::inet:
        case abstract_type::kind::uuid:
        case abstract_type::kind::date:
        case abstract_type::kind::simple_date:
        case abstract_type::kind::duration:
        case abstract_type::kind::time:
        case abstract_type::kind::timestamp:
        case abstract_type::kind::timeuuid:
        case abstract_type::kind::empty:
        case abstract_type::kind::list:
        case abstract_type::kind::set:
        case abstract_type::kind::map:
        case abstract_type::kind::user:
        case abstract_type::kind::decimal:
        case abstract_type::kind::varint:
            return true;

        case abstract_type::kind::reversed:
            throw std::runtime_error("is_0_bytes_value_empty_value reversed_type is not allowed");

        default:
            throw std::runtime_error(
                fmt::format("is_0_bytes_value_empty_value - unhandled type kind: {}", type_kind));
    }
}

raw_value to_raw_value(const cql_value& value, cql_serialization_format serialization_format) {
    return std::visit(overloaded_functor{
        [](const empty_value& val) {return to_raw_value(val);},
        [](const unset_value& val) {return to_raw_value(val);},
        [](const null_value& val) {return to_raw_value(val);},
        [](const bool_value& val) {return to_raw_value(val);},
        [](const int8_value& val) {return to_raw_value(val);},
        [](const int16_value& val) {return to_raw_value(val);},
        [](const int32_value& val) {return to_raw_value(val);},
        [](const int64_value& val) {return to_raw_value(val);},
        [](const counter_value& val) {return to_raw_value(val);},
        [](const varint_value& val) {return to_raw_value(val);},
        [](const float_value& val) {return to_raw_value(val);},
        [](const double_value& val) {return to_raw_value(val);},
        [](const decimal_value& val) {return to_raw_value(val);},
        [](const ascii_value& val) {return to_raw_value(val);},
        [](const utf8_value& val) {return to_raw_value(val);},
        [](const date_value& val) {return to_raw_value(val);},
        [](const simple_date_value& val) {return to_raw_value(val);},
        [](const duration_value& val) {return to_raw_value(val);},
        [](const time_value& val) {return to_raw_value(val);},
        [](const timestamp_value& val) {return to_raw_value(val);},
        [](const timeuuid_value& val) {return to_raw_value(val);},
        [](const blob_value& val) {return to_raw_value(val);},
        [](const inet_value& val) {return to_raw_value(val);},
        [](const uuid_value& val) {return to_raw_value(val);},
        [&](const tuple_value& val) {return to_raw_value(val);},
        [&](const list_value& val) {return to_raw_value(val, serialization_format);},
        [&](const set_value& val) {return to_raw_value(val, serialization_format);},
        [&](const map_value& val) {return to_raw_value(val, serialization_format);},
        [](const user_type_value& val) {return to_raw_value(val);}
        }, value);
}

raw_value to_raw_value(const empty_value&) {
    return raw_value::make_value(bytes());
}

raw_value to_raw_value(const unset_value&) {
    return raw_value::make_unset_value();
}

raw_value to_raw_value(const null_value&) {
    return raw_value::make_null();
}

raw_value to_raw_value(const bool_value& val) {
    managed_bytes result(managed_bytes::initialized_later{}, 1);
    result[0] = val.value ? 1 : 0;
    return raw_value::make_value(std::move(result));
}

raw_value to_raw_value(const int8_value& val) {
    return raw_value::make_value(managed_bytes(&val.value, 1));
}

raw_value to_raw_value(const int16_value& val) {
    int16_t be_value = cpu_to_be(val.value);
    managed_bytes result((int8_t*)&be_value, sizeof(be_value));
    return raw_value::make_value(std::move(result));
}

raw_value to_raw_value(const int32_value& val) {
    int32_t be_value = cpu_to_be(val.value);
    managed_bytes result((int8_t*)&be_value, sizeof(be_value));
    return raw_value::make_value(std::move(result));
}

raw_value to_raw_value(const int64_value& val) {
    int64_t be_value = cpu_to_be(val.value);
    managed_bytes result((int8_t*)&be_value, sizeof(be_value));
    return raw_value::make_value(std::move(result));
}

raw_value to_raw_value(const counter_value& val) {
    int64_t be_value = cpu_to_be(val.value);
    managed_bytes result((int8_t*)&be_value, sizeof(be_value));
    return raw_value::make_value(std::move(result));
}

raw_value to_raw_value(const varint_value& val) {
    return raw_value::make_value(val.value);
}

raw_value to_raw_value(const float_value& val) {
    int32_t cpu_value;
    memcpy(&cpu_value, &val.value, sizeof(cpu_value));

    int32_t be_value = cpu_to_be(cpu_value);
    managed_bytes result((int8_t*)&be_value, sizeof(be_value));
    return raw_value::make_value(std::move(result));
}

raw_value to_raw_value(const double_value& val) {
    int64_t cpu_value;
    memcpy(&cpu_value, &val.value, sizeof(cpu_value));

    int64_t be_value = cpu_to_be(cpu_value);
    managed_bytes result((int8_t*)&be_value, sizeof(be_value));
    return raw_value::make_value(std::move(result));
}

raw_value to_raw_value(const decimal_value& val) {
    return raw_value::make_value(val.value);
}

raw_value to_raw_value(const ascii_value& val) {
    return raw_value::make_value(val.value);
}

raw_value to_raw_value(const utf8_value& val) {
    return raw_value::make_value(val.value);
}

raw_value to_raw_value(const date_value& val) {
    return raw_value::make_value(val.value);
}

raw_value to_raw_value(const simple_date_value& val) {
    return raw_value::make_value(val.value);
}

raw_value to_raw_value(const duration_value& val) {
    return raw_value::make_value(val.value);
}

raw_value to_raw_value(const time_value& val) {
    return raw_value::make_value(val.value);
}

raw_value to_raw_value(const timestamp_value& val) {
    return raw_value::make_value(val.value);
}

raw_value to_raw_value(const timeuuid_value& val) {
    return raw_value::make_value(val.value);
}

raw_value to_raw_value(const blob_value& val) {
    return raw_value::make_value(val.value);
}

raw_value to_raw_value(const inet_value& val) {
    return raw_value::make_value(val.value);
}

raw_value to_raw_value(const uuid_value& val) {
    return raw_value::make_value(val.value);
}

raw_value to_raw_value(const tuple_value& val) {
    size_t serialized_size = 0;
    for (const std::variant<managed_bytes, null_value>& element : val.elements) {
        // Addition overflow shouldn't happen unless size_t is 32bit or serialized_size reaches 10^6 TB
        serialized_size += 4;

        if (auto elem = std::get_if<managed_bytes>(&element)) {
            if (elem->size() > std::numeric_limits<int32_t>::max()) {
                throw std::runtime_error(fmt::format("tuple_value element size is too big to be serialized ({} > {})",
                                                     elem->size(), std::numeric_limits<int32_t>::max()));
            }

            serialized_size += elem->size();
        }
    }

    managed_bytes result(managed_bytes::initialized_later{}, serialized_size);
    managed_bytes_mutable_view result_view(result);

    for (const std::variant<managed_bytes, null_value>& element : val.elements) {
        std::visit(overloaded_functor{
            [&](const managed_bytes& elem) {
                write<int32_t>(result_view, elem.size());
                write_fragmented(result_view, managed_bytes_view(elem));
            },
            [&](const null_value&) {
                write<int32_t>(result_view, -1);
            }
        }, element);
    }

    return raw_value::make_value(std::move(result));
}

raw_value to_raw_value(const list_value& val, cql_serialization_format sf) {
    size_t max_list_size = max_collection_size(sf);
    size_t max_element_size = max_collection_value_size(sf);

    if (val.elements.size() > max_list_size) {
        throw std::runtime_error(fmt::format("list_value length is too big to be serialized ({} > {})",
                                              val.elements.size(), max_list_size));
    }

    size_t serialized_size = collection_size_len(sf);
    for (const std::variant<managed_bytes, null_value>& element : val.elements) {
        // Addition overflow shouldn't happen unless size_t is 32bit or serialized_size reaches 10^6 TB
        if (auto element_bytes = std::get_if<managed_bytes>(&element)) {
            if (element_bytes->size() > max_element_size) {
                throw std::runtime_error(
                    fmt::format("list_value element length is too big to be serialized ({} > {})",
                    element_bytes->size(), max_element_size));
            }

            serialized_size += collection_value_len(sf) + element_bytes->size();
        } else {
            serialized_size += collection_value_len(sf);
        }
    }

    managed_bytes result(managed_bytes::initialized_later{}, serialized_size);
    managed_bytes_mutable_view result_view(result);

    write_collection_size(result_view, val.elements.size(), sf);

    for (const std::variant<managed_bytes, null_value>& element : val.elements) {
        if (auto element_bytes = std::get_if<managed_bytes>(&element)) {
            write_collection_value(result_view, sf, managed_bytes_view(*element_bytes));
        } else {
            if (sf.using_32_bits_for_collections()) {
                write<int32_t>(result_view, -1);
            } else {
                // NULL is represented by negative length.
                // In old format value length is represented as uint16_t.
                throw std::runtime_error("list_value unable to encode null in old serialization format");
            }
        }
    }

    return raw_value::make_value(std::move(result));
}

raw_value to_raw_value(const set_value& val, cql_serialization_format sf) {
    size_t max_set_size = max_collection_size(sf);
    size_t max_element_size = max_collection_value_size(sf);

    if (val.elements.size() > max_set_size) {
        throw std::runtime_error(fmt::format("set_value length is too big to be serialized ({} > {})",
                                              val.elements.size(), max_set_size));
    }

    size_t serialized_size = collection_size_len(sf);
    for (const managed_bytes& element : val.elements) {
        if (element.size() > max_element_size) {
            throw std::runtime_error(
                fmt::format("set_value element length is too big to be serialized ({} > {})",
                element.size(), max_element_size));
        }

        // Addition overflow shouldn't happen unless size_t is 32bit or serialized_size reaches 10^6 TB
        serialized_size += collection_value_len(sf) + element.size();
    }

    managed_bytes result(managed_bytes::initialized_later{}, serialized_size);
    managed_bytes_mutable_view result_view(result);

    write_collection_size(result_view, val.elements.size(), sf);

    for (const managed_bytes& element : val.elements) {
        write_collection_value(result_view, sf, managed_bytes_view(element));
    }

    return raw_value::make_value(std::move(result));
}

raw_value to_raw_value(const map_value& val, cql_serialization_format sf) {
    size_t max_map_size = max_collection_size(sf);
    size_t max_element_size = max_collection_value_size(sf);

    if (val.elements.size() > max_map_size) {
        throw std::runtime_error(fmt::format("map_value length is too big to be serialized ({} > {})",
                                              val.elements.size(), max_map_size));
    }

    size_t serialized_size = collection_size_len(sf);
    for (const auto& [key, value] : val.elements) {
        if (key.size() > max_element_size || value.size() > max_element_size) {
            throw std::runtime_error(
                fmt::format("map_value element length is too big to be serialized ({} > {})",
                std::max(key.size(), value.size()), max_element_size));
        }

        // Addition overflow shouldn't happen unless size_t is 32bit or serialized_size reaches 10^6 TB
        serialized_size += collection_value_len(sf) + key.size();
        serialized_size += collection_value_len(sf) + value.size();
    }

    managed_bytes result(managed_bytes::initialized_later{}, serialized_size);
    managed_bytes_mutable_view result_view(result);

    write_collection_size(result_view, val.elements.size(), sf);

    for (const auto& [key, value] : val.elements) {
        write_collection_value(result_view, sf, managed_bytes_view(key));
        write_collection_value(result_view, sf, managed_bytes_view(value));
    }

    return raw_value::make_value(std::move(result));
}

raw_value to_raw_value(const user_type_value& val) {
    size_t serialized_size = 0;
    for (const std::variant<managed_bytes, null_value>& field_value : val.field_values) {
        // Addition overflow shouldn't happen unless size_t is 32bit or serialized_size reaches 10^6 TB
        serialized_size += 4;

        if (auto elem = std::get_if<managed_bytes>(&field_value)) {
            if (elem->size() > std::numeric_limits<int32_t>::max()) {
                throw std::runtime_error(fmt::format("user_type_value element size is too big to be serialized ({} > {})",
                                                     elem->size(), std::numeric_limits<int32_t>::max()));
            }

            serialized_size += elem->size(); 
        }
    }

    managed_bytes result(managed_bytes::initialized_later{}, serialized_size);
    managed_bytes_mutable_view result_view(result);

    for (const std::variant<managed_bytes, null_value>& field_value : val.field_values) {
        std::visit(overloaded_functor{
            [&](const managed_bytes& elem) {
                write<int32_t>(result_view, elem.size());
                write_fragmented(result_view, managed_bytes_view(elem));
            },
            [&](const null_value&) {
                write<int32_t>(result_view, -1);
            }
        }, field_value);
    }

    return raw_value::make_value(std::move(result));
}
}
