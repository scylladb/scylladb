#pragma once
#include <absl/container/btree_set.h>
#include <cstdint>
#include <ostream>
#include <fmt/core.h>
#include "schema/schema.hh"

class decorated_key_with_hash;
class mutation_fragment;

// Hash of a repair row
class repair_hash {
public:
    uint64_t hash = 0;
    repair_hash() = default;
    explicit repair_hash(uint64_t h) : hash(h) {
    }
    void clear() {
        hash = 0;
    }
    void add(const repair_hash& other) {
        hash ^= other.hash;
    }
    std::strong_ordering operator<=>(const repair_hash&) const = default;
    friend std::ostream& operator<<(std::ostream& os, const repair_hash& x) {
        return os << x.hash;
    }
};

using repair_hash_set = absl::btree_set<repair_hash>;

class repair_hasher {
    uint64_t _seed;
    schema_ptr _schema;
public:
    repair_hasher(uint64_t seed, schema_ptr s)
        : _seed(seed)
        , _schema(std::move(s))
    {}

    repair_hash do_hash_for_mf(const decorated_key_with_hash& dk_with_hash, const mutation_fragment& mf);
};

template <> struct fmt::formatter<repair_hash>  : fmt::formatter<string_view> {
    auto format(const repair_hash& x, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", x.hash);
    }
};
