/*
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#pragma once

#include <stdint.h>
#include "schema/schema_fwd.hh"
#include "utils/hash.hh"
#include "sstables/version.hh"
#include "seastarx.hh"

namespace db {

using segment_id_type = uint64_t;
using position_type = uint32_t;

struct replay_position {
    static constexpr size_t max_cpu_bits = 10; // 1024 cpus. should be enough for anyone
    static constexpr size_t max_ts_bits = 8 * sizeof(segment_id_type) - max_cpu_bits;
    static constexpr segment_id_type ts_mask = (segment_id_type(1) << max_ts_bits) - 1;
    static constexpr segment_id_type cpu_mask = ~ts_mask;

    segment_id_type id;
    position_type pos;

    replay_position(segment_id_type i = 0, position_type p = 0)
        : id(i), pos(p)
    {}

    replay_position(unsigned shard, segment_id_type i, position_type p = 0)
            : id((segment_id_type(shard) << max_ts_bits) | i), pos(p)
    {
        if (i & cpu_mask) {
            throw std::invalid_argument("base id overflow: " + std::to_string(i));
        }
    }

    auto operator<=>(const replay_position&) const noexcept = default;

    unsigned shard_id() const {
        return unsigned(id >> max_ts_bits);
    }
    segment_id_type base_id() const {
        return id & ts_mask;
    }
    replay_position base() const {
        return replay_position(base_id(), pos);
    }

    template <typename Describer>
    auto describe_type(sstables::sstable_version_types v, Describer f) { return f(id, pos); }

    bool valid() const {
        return id != 0 && pos != 0;
    }

    static const replay_position max;
};

class commitlog;
class cf_holder;

using cf_id_type = table_id;

class rp_handle {
public:
    // What ~rp_handle() does with the segment reference: mark_clean decrements the segment's
    // dirty count so the segment can be recycled, retain_segment keeps it, which leaves the
    // segment on disk to be replayed on the next startup. Survives moves.
    enum class destruction_policy : uint8_t {
        mark_clean,
        retain_segment,
    };

    rp_handle() noexcept;
    rp_handle(rp_handle&&) noexcept;
    rp_handle& operator=(rp_handle&&) noexcept;
    ~rp_handle();

    replay_position release();

    void set_destruction_policy(destruction_policy p) noexcept {
        _on_destruction = p;
    }

    operator bool() const {
        return _h && _rp != replay_position();
    }
    operator const replay_position&() const {
        return _rp;
    }
    const replay_position& rp() const {
        return _rp;
    }
private:
    friend class commitlog;

    rp_handle(shared_ptr<cf_holder>, cf_id_type, replay_position) noexcept;

    ::shared_ptr<cf_holder> _h;
    cf_id_type _cf;
    replay_position _rp;
    destruction_policy _on_destruction = destruction_policy::mark_clean;
};

}

namespace std {
template <>
struct hash<db::replay_position> {
    size_t operator()(const db::replay_position& v) const {
        return utils::tuple_hash()(v.id, v.pos);
    }
};
}

template <> struct fmt::formatter<db::replay_position> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const db::replay_position&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
