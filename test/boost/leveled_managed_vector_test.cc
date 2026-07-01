/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>
#include <fmt/core.h>

#include "test/lib/scylla_test_case.hh"
#include "test/lib/nondeterministic_choice_stack.hh"

#include "utils/lsa/leveled_managed_vector.hh"
#include "utils/logalloc.hh"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <memory>
#include <new>
#include <random>
#include <utility>
#include <vector>

#include <time.h>

using namespace lsa::leveled_managed_vector_internal;

namespace {

// Element type with a configurable size, used to exercise sizeof(T) being
// smaller than, equal to, and greater than sizeof(node_ptr).
//
// It is trivially copyable so that it can be relocated by the LSA
// standard_migrator (which move-constructs and destroys). The first 4 bytes
// hold an identifier that we verify against a reference model; the rest is
// padding chosen so that sizeof(test_value<Size>) == Size exactly (we can't use
// std::array<char, N> for the padding because std::array<char, 0> still has
// sizeof 1).
template <size_t Size>
struct test_value {
    static_assert(Size >= sizeof(uint32_t));
    char bytes[Size];
    test_value() noexcept {
        std::memset(bytes, 0, Size);
    }
    explicit test_value(uint32_t id) noexcept {
        std::memset(bytes, 0, Size);
        std::memcpy(bytes, &id, sizeof(id));
    }
    uint32_t id() const noexcept {
        uint32_t v;
        std::memcpy(&v, bytes, sizeof(v));
        return v;
    }
};
static_assert(sizeof(test_value<4>) == 4);
static_assert(sizeof(test_value<8>) == 8);
static_assert(sizeof(test_value<16>) == 16);
static_assert(std::is_trivially_copyable_v<test_value<8>>);
static_assert(std::is_nothrow_move_constructible_v<test_value<8>>);
static_assert(std::is_nothrow_destructible_v<test_value<8>>);

// Facade over a source of nondeterministic choices that drives the scenario
// exploration below. The test asks for choices via choose_*(), runs one
// scenario, then calls rewind() to advance to the next one; the do/while loop
// ends when rewind() returns false. Putting this behind an interface lets us
// swap exploration strategies without touching the test body.
//
// Two implementations are provided:
//  - exhaustive_generator wraps a nondeterministic_choice_stack and visits every
//    reachable combination of choices exactly once (rewind() walks the choice
//    tree);
//  - random_generator makes uniformly random choices and remembers nothing
//    between scenarios; its rewind() merely limits how many scenarios run.
class choice_generator {
public:
    virtual ~choice_generator() = default;
    // Nondeterministically choose an integer in [0, n].
    virtual int choose_up_to(int n) = 0;
    // Nondeterministically choose a boolean.
    bool choose_bool() { return choose_up_to(1); }
    // Advance to the next scenario; returns false once exploration is complete.
    virtual bool rewind() = 0;
};

// Exhaustive exploration: every reachable combination of choices is visited.
class exhaustive_generator final : public choice_generator {
    nondeterministic_choice_stack _ncs;
public:
    int choose_up_to(int n) override { return _ncs.choose_up_to(n); }
    bool rewind() override { return _ncs.rewind(); }
};

// Random exploration: a fixed number of scenarios of uniformly random choices.
// Nothing is remembered between scenarios, so rewind() just counts them down.
class random_generator final : public choice_generator {
    std::mt19937 _rng;
    uint64_t _remaining_scenarios;
public:
    explicit random_generator(uint64_t scenarios, uint32_t seed = 0)
            : _rng(seed), _remaining_scenarios(scenarios) {
        SCYLLA_ASSERT(scenarios > 0);
    }
    int choose_up_to(int n) override {
        return std::uniform_int_distribution<int>(0, n)(_rng);
    }
    bool rewind() override { return --_remaining_scenarios > 0; }
};

// Constructs a fresh choice_generator. A fresh one is used for each (element
// size, max_alloc_size) configuration so they explore independently.
using generator_factory = std::function<std::unique_ptr<choice_generator>()>;

// An allocation_strategy that wraps another one and, when failure injection is
// enabled, nondeterministically throws std::bad_alloc before each allocation
// (driven by the choice stack, so every alloc/no-alloc combination is
// explored). It also counts live allocations and live bytes so the test can
// assert that an operation aborted by bad_alloc didn't leak any nodes, and that
// external_memory_usage() agrees with what the allocator actually has outstanding.
class failing_allocator final : public allocation_strategy {
    allocation_strategy& _underlying;
    choice_generator& _choices;
    bool _inject;
    size_t _max_alloc_size;
    int64_t _live = 0;
    int64_t _live_bytes = 0;
public:
    failing_allocator(allocation_strategy& underlying, choice_generator& choices, bool inject, size_t max_alloc_size)
        : _underlying(underlying), _choices(choices), _inject(inject), _max_alloc_size(max_alloc_size) {
        _preferred_max_contiguous_allocation = underlying.preferred_max_contiguous_allocation();
    }
    void* alloc(migrate_fn mf, size_t size, size_t alignment) override {
        // The whole point of max_alloc_size is to cap the container's individual
        // allocations; every node it requests must honour that bound.
        BOOST_REQUIRE_LE(size, _max_alloc_size);
        if (_inject && _choices.choose_bool()) {
            throw std::bad_alloc();
        }
        void* p = _underlying.alloc(mf, size, alignment);
        ++_live;
        _live_bytes += int64_t(size);
        return p;
    }
    void free(void* obj, size_t size) override {
        if (obj) {
            --_live;
            _live_bytes -= int64_t(size);
        }
        _underlying.free(obj, size);
    }
    void free(void* obj) override {
        // leveled_managed_vector frees exclusively through destroy(), which uses
        // the sized free() overload above, so this size-less overload is never
        // taken here and can't (nor needs to) update _live_bytes.
        if (obj) {
            --_live;
        }
        _underlying.free(obj);
    }
    size_t object_memory_size_in_allocator(const void* obj) const noexcept override {
        return _underlying.object_memory_size_in_allocator(obj);
    }
    uintptr_t reserve(size_t memory) override { return _underlying.reserve(memory); }
    void unreserve(uintptr_t opaque) noexcept override { _underlying.unreserve(opaque); }

    int64_t live_allocations() const noexcept { return _live; }
    // Total bytes requested from the underlying allocator and not yet freed
    // (summing the `size` argument of alloc()/free()). The leveled_managed_vector
    // requests exactly storage_size() per node, the same quantity
    // external_memory_usage() sums, so the two must match.
    int64_t live_bytes() const noexcept { return _live_bytes; }
};

// The node capacities for a given element size and max allocation size,
// computed the same way leveled_managed_vector does internally (those members
// are private, but node<>::max_capacity is public).
constexpr size_t inner_cap_for(size_t max_alloc) {
    return node<node_ptr>::max_capacity(max_alloc);
}
template <size_t Size>
constexpr size_t leaf_cap_for(size_t max_alloc) {
    return node<test_value<Size>>::max_capacity(max_alloc);
}

// Picks a sequence of "interesting" max_alloc_size values for a given element
// size:
//  - the smallest value for which leveled_managed_vector's static_asserts hold
//    (inner_node_capacity >= 2 and leaf_node_capacity >= 1);
//  - then the next smallest values that each raise one of the two capacities
//    above every value chosen so far.
// This yields a spread of small capacities (down to leaf_cap == 1, which makes
// the deepest trees per element), so even a handful of elements exercises
// multi-level trees, prepending of new roots, and spine resizing.
template <size_t Size, size_t Count>
consteval std::array<size_t, Count> interesting_alloc_sizes() {
    std::array<size_t, Count> result{};
    size_t found = 0;
    size_t best_inner = 0;
    size_t best_leaf = 0;
    for (size_t m = 1; m <= (size_t(1) << 16) && found < Count; ++m) {
        size_t ic = inner_cap_for(m);
        size_t lc = leaf_cap_for<Size>(m);
        if (ic < 2 || lc < 1) {
            continue;
        }
        if (found == 0 || ic > best_inner || lc > best_leaf) {
            result[found++] = m;
            best_inner = std::max(best_inner, ic);
            best_leaf = std::max(best_leaf, lc);
        }
    }
    if (found != Count) {
        throw "interesting_alloc_sizes: not enough distinct configurations found";
    }
    return result;
}

struct test_params {
    int steps; // total operations per scenario; runtime is exponential in this
    int emplace_back_count;
    int reserve_count;
    int grow_to_count;
    int set_count;
    int compact_count;
    int move_roundtrip_count;
    int max_reserve_arg;  // upper bound on the argument to reserve()
    int max_grow_to_arg;  // upper bound on the argument to grow_to()
    bool inject_failures = false; // make every allocation nondeterministically fail
};

// Exhaustively explores every sequence of up to `params.steps` operations, with
// per-operation count limits and every legal argument value within the
// configured bounds, on a freshly constructed
// leveled_managed_vector<test_value<Size>, MaxAlloc>, checking integrity against
// a std::vector reference model before and after every operation.
//
// When params.inject_failures is set, every node allocation may nondeterministi-
// cally throw bad_alloc. An operation that throws is abandoned (not retried);
// the structure must remain valid and leak-free, which exercises the
// "confirmed capacity" exception-safety / re-runnability guarantee.
template <size_t Size, size_t MaxAlloc>
void run_scenarios(const test_params& params, const generator_factory& make_generator) {
    using value_type = test_value<Size>;
    using vector_type = leveled_managed_vector<value_type, MaxAlloc>;

    BOOST_TEST_MESSAGE(fmt::format("sizeof(T)={} max_alloc_size={} inner_cap={} leaf_cap={} inject_failures={}",
            Size, MaxAlloc, inner_cap_for(MaxAlloc), leaf_cap_for<Size>(MaxAlloc), params.inject_failures));

    logalloc::region region;
    auto generator = make_generator();
    choice_generator& choices = *generator;
    failing_allocator alloc(region.allocator(), choices, params.inject_failures, MaxAlloc);

    // Verifies every const method against the model, plus a few structural
    // invariants of external_memory_usage(). external_memory_usage() never
    // decreases over the life of a vector (no operation here frees a node), so
    // we thread the previous value through to assert monotonicity.
    auto check_integrity = [&] (const vector_type& v, const std::vector<uint32_t>& model, size_t& prev_mem) {
        BOOST_REQUIRE_EQUAL(v.size(), model.size());
        BOOST_REQUIRE_EQUAL(v.empty(), model.empty());
        for (size_t i = 0; i < model.size(); ++i) {
            BOOST_REQUIRE_EQUAL(v[i].id(), model[i]);
        }
        size_t mem = v.external_memory_usage();
        // Pure function of the structure.
        BOOST_REQUIRE_EQUAL(v.external_memory_usage(), mem);
        // Must agree exactly with what the allocator has outstanding: v is the
        // only thing allocating through `alloc`, and it requests exactly the
        // per-node storage_size() that external_memory_usage() sums up. This
        // holds even after an operation aborted by an injected bad_alloc, since
        // partially-built (but still reachable) nodes are kept, not rolled back.
        BOOST_REQUIRE_EQUAL(mem, size_t(alloc.live_bytes()));
        // Accounts for at least the live elements.
        BOOST_REQUIRE_GE(mem, model.size() * sizeof(value_type));
        // Non-zero once anything is stored.
        if (!model.empty()) {
            BOOST_REQUIRE_GT(mem, 0u);
        }
        // Monotonic non-decreasing.
        BOOST_REQUIRE_GE(mem, prev_mem);
        prev_mem = mem;
    };

    uint64_t scenarios = 0;
    do {
        ++scenarios;
        // Everything that touches the vector (including its destruction at the
        // end of this block) must run with the (wrapped region) allocator as the
        // current allocator.
        with_allocator(alloc, [&] {
            vector_type v;
            std::vector<uint32_t> model;
            uint32_t next_id = 1; // 0 is reserved for grow_to()'s default-constructed elements
            size_t prev_mem = 0;

            // Runs an allocating mutation under a reclaim_lock (so the raw node
            // pointers the implementation holds across allocations can't be
            // invalidated by compaction mid-operation). Returns false if the
            // mutation was aborted by an injected bad_alloc.
            auto mutate = [&] (auto&& f) -> bool {
                try {
                    logalloc::reclaim_lock rl(region);
                    f();
                    return true;
                } catch (const std::bad_alloc&) {
                    return false;
                }
            };

            auto remaining = params;
            auto has_remaining_operations = [&] {
                return remaining.emplace_back_count > 0 || remaining.reserve_count > 0
                        || remaining.grow_to_count > 0 || remaining.set_count > 0
                        || remaining.compact_count > 0 || remaining.move_roundtrip_count > 0;
            };
            for (int step = 0; step < params.steps && has_remaining_operations(); ++step) {
                check_integrity(v, model, prev_mem);

                if (remaining.emplace_back_count > 0 && choices.choose_bool()) {
                    --remaining.emplace_back_count;
                    uint32_t id = next_id;
                    if (mutate([&] { v.emplace_back(id); })) {
                        model.push_back(id);
                        ++next_id;
                    }
                    continue;
                }

                if (remaining.reserve_count > 0 && choices.choose_bool()) {
                    --remaining.reserve_count;
                    size_t n = choices.choose_up_to(params.max_reserve_arg);
                    mutate([&] { v.reserve(n); }); // no observable change either way
                    continue;
                }

                if (remaining.grow_to_count > 0 && choices.choose_bool()) {
                    --remaining.grow_to_count;
                    size_t n = choices.choose_up_to(params.max_grow_to_arg);
                    size_t orig = model.size();
                    bool ok = mutate([&] { v.grow_to(n); });
                    if (!params.inject_failures) {
                        BOOST_REQUIRE(ok);
                        BOOST_REQUIRE_EQUAL(v.size(), std::max(orig, n));
                    }
                    // grow_to() is not atomic: when it throws it may have grown
                    // partially, appending default-constructed (id 0) elements.
                    while (model.size() < v.size()) {
                        model.push_back(0);
                    }
                    continue;
                }

                if (remaining.set_count > 0 && !model.empty() && choices.choose_bool()) {
                    --remaining.set_count;
                    // Overwrite every element with a fresh, never-before-used id
                    // (next_id only ever increases, so the new contents can't
                    // coincide with any old contents). This catches stale data
                    // anywhere in the structure, not just at a single index.
                    for (size_t i = 0; i < model.size(); ++i) {
                        uint32_t id = next_id++;
                        v[i] = value_type(id); // doesn't allocate, can't fail
                        model[i] = id;
                    }
                    continue;
                }

                if (remaining.compact_count > 0 && choices.choose_bool()) {
                    --remaining.compact_count;
                    size_t before = v.external_memory_usage();
                    region.full_compaction();
                    // Relocation must preserve the logical structure, hence the
                    // same external footprint.
                    BOOST_REQUIRE_EQUAL(v.external_memory_usage(), before);
                    continue;
                }

                if (remaining.move_roundtrip_count > 0 && choices.choose_bool()) {
                    --remaining.move_roundtrip_count;
                    vector_type moved = std::move(v);
                    BOOST_REQUIRE_EQUAL(v.size(), 0u);
                    BOOST_REQUIRE(v.empty());
                    v = std::move(moved);
                    continue;
                }
            }
            check_integrity(v, model, prev_mem);
        });
        // The vector is gone; every node it ever allocated must have been freed,
        // even along paths where operations were aborted by bad_alloc.
        BOOST_REQUIRE_EQUAL(alloc.live_allocations(), int64_t(0));
    } while (choices.rewind());

    BOOST_TEST_MESSAGE(fmt::format("  explored {} scenarios", scenarios));
}

template <size_t Size, size_t... Is>
void run_for_size(const test_params& params, const generator_factory& make_generator, std::index_sequence<Is...>) {
    constexpr auto sizes = interesting_alloc_sizes<Size, sizeof...(Is)>();
    (run_scenarios<Size, sizes[Is]>(params, make_generator), ...);
}

template <size_t Size, size_t AllocSizeCount>
void run_for_size(const test_params& params, const generator_factory& make_generator) {
    run_for_size<Size>(params, make_generator, std::make_index_sequence<AllocSizeCount>{});
}

// sizeof(node_ptr) is 8, so these sizes are smaller / equal / larger than it.
void run_all_sizes(const test_params& params, const generator_factory& make_generator) {
    constexpr size_t alloc_size_count = 4; // max_alloc_size values per element size
    run_for_size<4, alloc_size_count>(params, make_generator);
    run_for_size<8, alloc_size_count>(params, make_generator);
    run_for_size<16, alloc_size_count>(params, make_generator);
}

} // anonymous namespace

// A fresh exhaustive_generator per configuration, so each visits every reachable
// combination of choices exactly once.
generator_factory exhaustive() {
    return [] { return std::make_unique<exhaustive_generator>(); };
}

// A fresh random_generator per configuration, each seeded differently so the
// configurations don't all explore the same random path.
generator_factory randomized(uint64_t scenarios) {
    return [scenarios, seed = 0u]() mutable {
        return std::make_unique<random_generator>(scenarios, seed++);
    };
}

// Exhaustive exploration of operation sequences with infallible allocations,
// including LSA compaction (relocation) between operations.
SEASTAR_THREAD_TEST_CASE(test_random_operations_exhaustive) {
    run_all_sizes(test_params {
        .steps = 3,
        .emplace_back_count = 3,
        .reserve_count = 3,
        .grow_to_count = 3,
        .set_count = 3,
        .compact_count = 1,
        .move_roundtrip_count = 1,
        .max_reserve_arg = 7,
        .max_grow_to_arg = 7,
        .inject_failures = false,
    }, exhaustive());
}

// Exhaustive exploration of operation sequences with infallible allocations,
// including LSA compaction (relocation) between operations.
SEASTAR_THREAD_TEST_CASE(test_random_operations_exhaustive_with_alloc_failures) {
    run_all_sizes(test_params {
        .steps = 3,
        .emplace_back_count = 2,
        .reserve_count = 2,
        .grow_to_count = 2,
        .set_count = 2,
        .compact_count = 1,
        .move_roundtrip_count = 0,
        .max_reserve_arg = 4,
        .max_grow_to_arg = 4,
        .inject_failures = true,
    }, exhaustive());
}

// Random exploration, plugged into the same machinery via the choice_generator
// facade. The exhaustive cases above can only afford a handful of steps because
// the search space is exponential; random sampling reaches much longer operation
// sequences and larger structures (at the cost of not being exhaustive).
SEASTAR_THREAD_TEST_CASE(test_random_operations_randomized) {
    run_all_sizes(test_params {
        .steps = 30,
        .emplace_back_count = 30,
        .reserve_count = 30,
        .grow_to_count = 30,
        .set_count = 30,
        .compact_count = 30,
        .move_roundtrip_count = 30,
        .max_reserve_arg = 32,
        .max_grow_to_arg = 32,
        .inject_failures = true,
    }, randomized(1000));
}

// Test an allocation that a chunked_managed_vector would be unable to handle.
SEASTAR_THREAD_TEST_CASE(test_big_alloc) {
    logalloc::region region;
    logalloc::allocating_section as;
    with_allocator(region.allocator(), [&] {
        as(region, [&] {
            auto vec = lsa::leveled_managed_vector<test_value<20>, logalloc::max_managed_object_size>();
            vec.reserve(1000000);
        });
    });
}