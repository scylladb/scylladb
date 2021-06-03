/*
 * Copyright (C) 2021-present ScyllaDB
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

#include <fmt/core.h>
#include "utils/compact-radix-tree.hh"

namespace compact_radix_tree {

template <typename T, typename Idx>
class printer {
    using tree_t = tree<T, Idx>;
    using node_head_t = typename tree_t::node_head;
    using leaf_node_t = typename tree_t::leaf_node;
    using inner_node_t = typename tree_t::inner_node;
    using layout = typename tree_t::layout;

    static std::string node_id(const node_head_t& n) {
        return fmt::format("{:03x}", (reinterpret_cast<uintptr_t>(&n)>>3) & 0xfff);
    }

    static std::string format(const T& val) noexcept { return fmt::format("{}", val); }
    static std::string format(const typename tree_t::node_head_ptr& p) noexcept { return node_id(*(p.raw())); }

    template <typename Tbl>
    static void print_indirect(const node_head_t& head, const Tbl& table, unsigned depth, std::string id) {
        fmt::print("{:<{}}{}.ind{} nr={} depth={} prefix={}.{}:", " ", int(depth * 2), id, Tbl::size, head._size, depth,
                head._prefix & tree_t::prefix_mask, head.prefix_len());
        for (unsigned i = 0; i < Tbl::size; i++) {
            if (table.has(i)) {
                fmt::print(" [{}] {}:{}", i, table._idx[i], format(table._slots[i]));
            }
        }
        fmt::print("\n");
    }

    template <typename Arr>
    static void print_direct(const node_head_t& head, const Arr& array, unsigned depth, std::string id) {
        unsigned cap = head._base_layout == layout::direct_static ? tree_t::node_index_limit : head._capacity;
        fmt::print("{:<{}}{}.dir{} nr={} depth={} prefix={}.{}:", " ", int(depth * 2), id, cap, array._data.count(head), depth,
                head._prefix & tree_t::prefix_mask, head.prefix_len());

        for (unsigned i = 0; i < cap; i++) {
            if (array._data.has(i)) {
                fmt::print(" [{}] {}", i, format(array._data._slots[i]));
            }
        }
        fmt::print("\n");
    }

    template <typename NT>
    static void print(const NT& n, unsigned depth) {
        switch (n._base._head._base_layout) {
        case layout::indirect_tiny: return print_indirect(n._base._head, n._base._layouts._this, depth, node_id(n._base._head));
        case layout::indirect_small: return print_indirect(n._base._head, n._base._layouts._other._this, depth, node_id(n._base._head));
        case layout::indirect_medium: return print_indirect(n._base._head, n._base._layouts._other._other._this, depth, node_id(n._base._head));
        case layout::indirect_large: return print_indirect(n._base._head, n._base._layouts._other._other._other._this, depth, node_id(n._base._head));
        case layout::direct_static: return print_direct(n._base._head, n._base._layouts._other._other._other._other._this, depth, node_id(n._base._head));
        default: break;
        }
        __builtin_unreachable();
    }

    template <>
    static void print<inner_node_t>(const inner_node_t& n, unsigned depth) {
        switch (n._base._head._base_layout) {
        case layout::direct_dynamic: return print_direct(n._base._head, n._base._layouts._this, depth, node_id(n._base._head));
        default: break;
        }
        __builtin_unreachable();
    }

    static void print(const node_head_t& n, unsigned depth) {
        if (depth == tree_t::leaf_depth) {
            print(n.template as_node<leaf_node_t>(), depth);
        } else {
            print(n.template as_node<inner_node_t>(), depth);
        }
    }

    public:
    static void show(const tree_t& t) {
        struct printing_visitor {
            bool sorted = false;

            bool operator()(Idx idx, const T& val) {
                std::abort();
            }
            bool operator()(const node_head_t& n, unsigned depth, bool enter) {
                if (enter) {
                    print(n, depth);
                }
                return depth != tree_t::leaf_depth;
            }
        };

        fmt::print("tree:\n");
        t.visit(printing_visitor{});
        fmt::print("---\n");
    }
};

} // namespace
