// Copyright (C) 2024-present ScyllaDB
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#include "tombstone_gc.hh"
#include <boost/icl/interval_map.hpp>

using repair_history_map = boost::icl::interval_map<dht::token, gc_clock::time_point, boost::icl::partial_absorber, std::less, boost::icl::inplace_max>;

class repair_history_map_ptr {
    lw_shared_ptr<repair_history_map> _ptr;
public:
    repair_history_map_ptr() = default;
    repair_history_map_ptr(lw_shared_ptr<repair_history_map> ptr) : _ptr(std::move(ptr)) {}
    repair_history_map& operator*() const { return _ptr.operator*(); }
    repair_history_map* operator->() const { return _ptr.operator->(); }
    explicit operator bool() const { return _ptr.operator bool(); }
};


