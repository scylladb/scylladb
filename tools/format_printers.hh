/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>
#include <variant>
#include <vector>

#include <yaml-cpp/yaml.h>

#include "utils/rjson.hh"

// inspired by http://wg21.link/p2098
template<typename T, template<typename...> class C>
struct is_specialization_of : std::false_type {};
template<template<typename...> class C, typename... Args>
struct is_specialization_of<C<Args...>, C> : std::true_type {};
template<typename T, template<typename...> class C>
inline constexpr bool is_specialization_of_v = is_specialization_of<T, C>::value;
template<typename T>
concept is_vector = is_specialization_of_v<T, std::vector>;

using metrics_value = std::variant<bool,
                                   double,
                                   int64_t,
                                   uint64_t,
                                   std::string,
                                   std::vector<std::string>>;

class json_writer {
    rjson::streaming_writer _writer;
public:
    json_writer(std::ostream& os)
        : _writer{os} {}

    class map_writer;
    class seq_writer {
        rjson::streaming_writer& _writer;
    public:
        seq_writer(rjson::streaming_writer& writer)
            : _writer{writer} {
            _writer.StartArray();
        }
        seq_writer(const seq_writer&) = delete;
        ~seq_writer() {
            _writer.EndArray();
        }
        template<typename T>
        void put(const std::vector<T>& seq) {
            for (auto& element : seq) {
                _writer.Write(element);
            }
        }
        inline auto add_map() -> std::unique_ptr<map_writer>;
    };
    class map_writer {
        rjson::streaming_writer& _writer;
        map_writer(const map_writer&) = delete;
        void put_value(const auto& value) {
            _writer.Write(value);
        }
        void put_value(const metrics_value& value) {
            std::visit([&] (auto&& v) {
                using T = std::decay_t<decltype(v)>;
                if constexpr (is_vector<T>) {
                    seq_writer(_writer).put(v);
                } else {
                    _writer.Write(v);
                }
            }, value);
        }
    public:
        map_writer(rjson::streaming_writer& writer)
            : _writer{writer} {
            _writer.StartObject();
        }
        ~map_writer() {
            _writer.EndObject();
        }
        void add_item(std::string_view key, const auto& value) {
            _writer.Key(key);
            put_value(value);
        }
        auto add_seq(std::string_view name) -> std::unique_ptr<seq_writer> {
            _writer.Key(name);
            return std::make_unique<seq_writer>(_writer);
        }
        auto add_map(std::string_view name) -> std::unique_ptr<map_writer> {
            _writer.Key(name);
            return std::make_unique<map_writer>(_writer);
        }
    };
    map_writer map() {
        return map_writer{_writer};
    }
};

auto json_writer::seq_writer::add_map() -> std::unique_ptr<map_writer> {
    return std::make_unique<map_writer>(_writer);
}

class yaml_writer {
    YAML::Emitter _emitter;
public:
    yaml_writer(std::ostream& os)
        : _emitter{os} {}

    class map_writer;
    class seq_writer {
        YAML::Emitter& _emitter;
    public:
        seq_writer(YAML::Emitter& emitter) : _emitter{emitter} {
            _emitter << YAML::BeginSeq;
        }
        ~seq_writer() {
            _emitter << YAML::EndSeq;
        }
        template<typename T>
        void put(const std::vector<T>& seq) {
            for (auto& element : seq) {
                _emitter << element;
            }
        }
        inline auto add_map() -> std::unique_ptr<map_writer>;
    };
    class map_writer {
        YAML::Emitter& _emitter;
        template<typename T>
        void put_value(const T& value) {
            if constexpr (std::convertible_to<T, std::string>) {
                _emitter << YAML::SingleQuoted << std::string(value);
            } else if constexpr (is_vector<T>) {
                seq_writer(_emitter).put(value);
            } else {
                _emitter << value;
            }
        }
        void put_value(const metrics_value& value) {
            std::visit([&] (auto&& v) {
                put_value(v);
            }, value);
        }
    public:
        map_writer(YAML::Emitter& emitter)
            : _emitter{emitter} {
            _emitter << YAML::BeginMap;
        }
        ~map_writer() {
            _emitter << YAML::EndMap;
        }
        void add_item(std::string_view key, const auto& value) {
             _emitter << YAML::Key << std::string(key);
             _emitter << YAML::Value;
             put_value(value);
        }
        auto add_seq(std::string_view name) -> std::unique_ptr<seq_writer> {
            _emitter << YAML::Key << std::string(name);
            return std::make_unique<seq_writer>(_emitter);
        }
        auto add_map(std::string_view name) -> std::unique_ptr<map_writer> {
            _emitter << YAML::Key << std::string(name);
            return std::make_unique<map_writer>(_emitter);
        }
    };
    map_writer map() {
        return map_writer{_emitter};
    }
};

auto yaml_writer::seq_writer::add_map() -> std::unique_ptr<map_writer> {
    return std::make_unique<map_writer>(_emitter);
}
