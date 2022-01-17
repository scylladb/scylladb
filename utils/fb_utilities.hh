/*
 */

/*
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <cstdint>
#include <optional>
#include "gms/inet_address.hh"

namespace utils {

using inet_address = gms::inet_address;

class fb_utilities {
private:
    static std::optional<inet_address>& broadcast_address() noexcept {
        static std::optional<inet_address> _broadcast_address;

        return _broadcast_address;
    }
    static std::optional<inet_address>& broadcast_rpc_address() noexcept {
        static std::optional<inet_address> _broadcast_rpc_address;

        return _broadcast_rpc_address;
    }
public:
   static constexpr int32_t MAX_UNSIGNED_SHORT = 0xFFFF;

   static void set_broadcast_address(inet_address addr) noexcept {
       broadcast_address() = addr;
   }

   static void set_broadcast_rpc_address(inet_address addr) noexcept {
       broadcast_rpc_address() = addr;
   }


   static const inet_address get_broadcast_address() noexcept {
       assert(broadcast_address());
       return *broadcast_address();
   }

   static const inet_address get_broadcast_rpc_address() noexcept {
       assert(broadcast_rpc_address());
       return *broadcast_rpc_address();
   }

    static bool is_me(gms::inet_address addr) noexcept {
        return addr == get_broadcast_address();
    }
};
}
