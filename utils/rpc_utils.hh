/*
 * Copyright (C) 2023-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "utils/tuple_utils.hh"
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sharded.hh>

namespace utils {
    namespace internal {
        class rpc_default_value {
            template <typename T>
            struct marker {};
            template <typename T>
            static auto create(marker<std::unique_ptr<T>>) {
                return std::make_unique<T>();
            }
            template <typename T>
            static auto create(marker<std::shared_ptr<T>>) {
                return std::make_shared<T>();
            }
            template <typename T>
            static auto create(marker<seastar::lw_shared_ptr<T>>) {
                return make_lw_shared<T>();
            }
            template <typename T>
            static auto create(marker<seastar::shared_ptr<T>>) {
                return make_shared<T>();
            }
            template <typename T>
            static auto create(marker<seastar::foreign_ptr<T>>) {
                return make_foreign(create<T>());
            }
            template <typename T>
            static auto create(marker<T>) {
                return T{};
            }
        public:
            template <typename T>
            static T create() {
                return create(marker<T>{});
            }
        };
    }

    // The Seastar RPC infrastructure does not support null values in tuples in handler responses.
    // This function generates a tuple by default constructing all of its elements, except for
    // smart pointer types. For those, it returns a pointer to the default constructed value.
    template <Tuple T>
    T make_default_rpc_tuple() {
        return std::invoke([&]<std::size_t... Is>(std::index_sequence<Is...>) {
            return T { internal::rpc_default_value::create<std::tuple_element_t<Is, T>>()... };
        }, std::make_index_sequence<tuple_ex_size_v<T>>());
    }
}
