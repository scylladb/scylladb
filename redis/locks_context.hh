/*
 * Copyright (C) 2020 pengjian.uestc @ gmail.com
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

#include "db/config.hh"
#include "db/timeout_clock.hh"
#include "bytes.hh"
#include "seastar/core/future.hh"
#include "seastar/core/sstring.hh"
#include <unordered_map>
#include <vector>

using namespace seastar;

namespace redis {

using clock_type = db::timeout_clock;
using key_semaphore = basic_semaphore<semaphore_default_exception_factory, clock_type>;
using locks_map = std::unordered_map<bytes, key_semaphore>;

struct locks_context {
    locks_map _strings_locks;
    locks_map _lists_locks;
    locks_map _sets_locks;
    locks_map _hashes_locks;
    locks_map _zsets_locks;
};

class key_locks_manager {
    std::vector<locks_context> _locks_context;
public:
    key_locks_manager() = default;
    ~key_locks_manager() = default;
    inline locks_context& get_locks_context(unsigned db_index) {
        if (db_index >= _locks_context.size()) {
            _locks_context.resize(db_index + 1);
        }
        return _locks_context[db_index];
    }
};

namespace internal {

static key_semaphore& get_semaphore_for_key(locks_map& locks, const bytes& key) {
    return locks.try_emplace(key, 1).first->second;
}

static void release_semaphore_for_key(locks_map& locks, const bytes& key) {
    auto it = locks.find(key);
    if (it != locks.end() && (*it).second.current() == 1) {
        locks.erase(it);
    }   
}

template<typename Func>
static
futurize_t<std::result_of_t<Func()>> with_locked_key(locks_map& locks, const bytes& key, clock_type::time_point timeout, Func func) {
    return with_semaphore(get_semaphore_for_key(locks, key), 1, timeout - clock_type::now(), std::move(func)).finally([key, &locks] {
        release_semaphore_for_key(locks, key);
    });
}

}

static thread_local key_locks_manager _key_locks_manager;

static inline locks_context& get_locks_context(unsigned db_index) {
    return _key_locks_manager.get_locks_context(db_index);
}

template<typename Func>
static
futurize_t<std::result_of_t<Func()>> with_locked_key(locks_map& locks, const bytes& key, clock_type::time_point timeout, Func func) {
    return internal::with_locked_key(locks, key, timeout, std::forward<Func>(func));
}

template<typename Func>
static
futurize_t<std::result_of_t<Func()>> with_strings_locked_key(locks_context& context, const bytes& key, clock_type::time_point timeout, Func func) {
    return internal::with_locked_key(context._strings_locks, key, timeout, std::forward<Func>(func));
}

template<typename Func>
static
futurize_t<std::result_of_t<Func()>> with_lists_locked_key(locks_context& context, const bytes& key, clock_type::time_point timeout, Func func) {
    return internal::with_locked_key(context._lists_locks, key, timeout, std::forward<Func>(func));
}

template<typename Func>
static
futurize_t<std::result_of_t<Func()>> with_hashes_locked_key(locks_context& context, const bytes& key, clock_type::time_point timeout, Func func) {
    return internal::with_locked_key(context._hashes_locks, key, timeout, std::forward<Func>(func));
}

template<typename Func>
static
futurize_t<std::result_of_t<Func()>> with_sets_locked_key(locks_context& context, const bytes& key, clock_type::time_point timeout, Func func) {
    return internal::with_locked_key(context._sets_locks, key, timeout, std::forward<Func>(func));
}

template<typename Func>
static
futurize_t<std::result_of_t<Func()>> with_zsets_locked_key(locks_context& context, const bytes& key, clock_type::time_point timeout, Func func) {
    return internal::with_locked_key(context._zsets_locks, key, timeout, std::forward<Func>(func));
}

}
