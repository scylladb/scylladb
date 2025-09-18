/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "test/lib/log.hh"
#include "test/lib/key_utils.hh"
#include <fmt/ranges.h>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/testing/test_case.hh>
#include "sstables/trie/trie_traversal.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include <generator>
#include <numeric>

namespace trie = sstables::trie;

using trie::const_bytes;

// generate_all_subsets(4, 2) = {{0, 1}, {0, 2}, {0, 3}, {1, 2}, {1, 3}, {2, 3}}
std::vector<std::vector<size_t>> generate_all_subsets(size_t n, size_t k) {
    if (k == 0) {
        return {std::vector<size_t>()};
    }
    using sample = std::vector<size_t>;
    std::vector<size_t> wksp(k);
    auto first = wksp.begin();
    auto last = wksp.end();
    // Fill wksp with first possible sample.
    std::ranges::iota(wksp, 0);
    std::vector<sample> samples;
    while (true) {
        samples.push_back(wksp);
        // Advance wksp to next possible sample.
        auto mt = last;
        --mt;
        while (mt > first && *mt == n - (last - mt)) {
            --mt;
        }
        if (mt == first && *mt == n - (last - mt)) {
            break;
        }
        ++(*mt);
        while (++mt != last) {
            *mt = *(mt - 1) + 1;
        }
    }
    return samples;
}

inline const_bytes string_as_bytes(std::string_view sv) {
    return std::as_bytes(std::span(sv.data(), sv.size()));
}

inline std::string_view bytes_as_string(const_bytes sv) {
    return {reinterpret_cast<const char*>(sv.data()), sv.size()};
}

template <>
struct fmt::formatter<trie::trail_entry> : fmt::formatter<string_view> {
    auto format(const trie::trail_entry& r, fmt::format_context& ctx) const
            -> decltype(ctx.out()) {
        return fmt::format_to(ctx.out(), "trail_entry(id={} child_idx={} payload_bits={})", r.pos, r.child_idx, r.payload_bits);
    }
};

// Builds a trie from the given set of strings (in the most obviously correct way it can),
// and computes the full set of legal cursor states for such a trie and their corresponding semantics.
// (Each state corresponds to some place in the order established by the strings).
struct reference_trie {
    struct node {
        std::vector<std::pair<std::byte, uint64_t>> children;
        uint8_t payload_bits = 0;
        int lookup_child(std::byte x) {
            return std::ranges::lower_bound(children, x, {}, [] (const auto& c) { return c.first; }) - children.begin();
        }
    };

    std::vector<node> _nodes;

    uint64_t add_node() {
        _nodes.emplace_back();
        return _nodes.size() - 1;
    }

    node* at(uint64_t pos) {
        return &_nodes.at(pos);
    }

    reference_trie(std::span<const_bytes> strings) {
        add_node();
        for (const auto& s : strings) {
            uint64_t it = 0;
            for (size_t i = 0; i < s.size(); ++i) {
                auto b = std::byte(s[i]);
                auto child_idx = at(it)->lookup_child(b);
                if (child_idx < int(at(it)->children.size())) {
                    it = it + at(it)->children.at(child_idx).second;
                } else {
                    auto new_node = add_node(); 
                    at(it)->children.emplace_back(b, new_node - it);
                    it = new_node;
                }
            }
            at(it)->payload_bits = 1;
        }
        std::ranges::reverse(_nodes);
    }

    struct cursor_state {
        decltype(trie::traversal_state::trail) trail;
        std::strong_ordering operator<=>(const cursor_state&) const = default;
    };

    // The "semantics" of the cursor state.
    // The output semantics of a given cursor operation should be the right function
    // of the input semantics, and nothing else.
    struct meaning {
        // The cursor corresponds to some position in the key universe.
        // Either to one of the present keys, or to some position "in between".
        // (For a given pair of neighboring keys, all possible positions in between
        // those keys are semantically equal and have the same rank).
        //
        // That is, rank 0 means "before all present keys",
        // rank 1 means "at key idx 0",
        // rank 2 means "between key idx 0 and key idx 1",
        // rank 3 means "at key idx 1",
        // etc.
        int rank;
        // The depth doesn't matter in an input,
        // but we track it to check that the `edges_traversed` result of `traverse`
        // is consistent with it.
        int depth;
    };

    std::map<cursor_state, meaning> enumerate_all_legal_states() {
        struct recursion_state {
            int rank = 0;
            int depth = 0;
            cursor_state cursor;
            std::map<cursor_state, meaning> result;
        };

        recursion_state state;

        // Fun fact: at first I tried to capture `state` by reference, but that crashes the compiler (clang 18.1.8)
        // with `error: cannot compile this l-value expression yet`.
        // Passing it via a parameter works. It seems that recursive closures have some rough edges.
        auto visit = [&] (this auto& visit, recursion_state& state, uint64_t pos) -> void {
            node* x = at(pos);
            if (x->payload_bits) {
                state.rank += 1;
            }
            state.cursor.trail.push_back(trie::trail_entry{
                .pos = pos, 
                .n_children = x->children.size(),
                .child_idx = -1,
                .payload_bits = x->payload_bits,
            });
            state.result.emplace(state.cursor, meaning{.rank = state.rank, .depth = state.depth});
            testlog.trace("enumerate_all_legal_states: {}: {}, {}", fmt::join(state.cursor.trail, ", "), state.rank, state.depth);
            state.cursor.trail.pop_back();
            if (x->payload_bits) {
                state.rank += 1;
            }
            bool relevant = x->children.size() > 1 || x->payload_bits;
            for (int child_idx = 0; child_idx < int(x->children.size()); ++child_idx) {
                state.cursor.trail.push_back(trie::trail_entry{
                    .pos = pos, 
                    .n_children = x->children.size(),
                    .child_idx = child_idx,
                    .payload_bits = x->payload_bits,
                });
                state.result.emplace(state.cursor, meaning{.rank = state.rank, .depth = state.depth});
                testlog.trace("enumerate_all_legal_states: {}: {}, {}", fmt::join(state.cursor.trail, ", "), state.rank, state.depth);
                if (!relevant) {
                    state.cursor.trail.pop_back();
                }
                state.depth += 1;
                visit(state, pos - x->children[child_idx].second);
                state.depth -= 1;
                if (relevant) {
                    state.cursor.trail.pop_back();
                }
            }
            state.cursor.trail.push_back(trie::trail_entry{
                .pos = pos,
                .n_children = x->children.size(),
                .child_idx = x->children.size(),
                .payload_bits = x->payload_bits,
            });
            state.result.emplace(state.cursor, meaning{.rank = state.rank, .depth = state.depth});
            testlog.trace("enumerate_all_legal_states: {}: {}, {}", fmt::join(state.cursor.trail, ", "), state.rank, state.depth);
            state.cursor.trail.pop_back();
        };

        visit(state, _nodes.size() - 1);
        return state.result;
    }
};

struct custom_node_reader {
    std::optional<uint64_t> cached_pos;
    reference_trie& rt;
    // A node_reader is allowed to skip an arbitrary number
    // of "unimportant" nodes.
    bool multibyte_traverse = true;

    void allow_multibyte_traverse(bool allow) {
        multibyte_traverse = allow;
    }

    bool cached(int64_t pos) {
        return cached_pos && cached_pos.value() == uint64_t(pos);
    }

    const_bytes get_payload(int64_t pos) {
        return const_bytes();
    }

    seastar::future<> load(int64_t pos, const reader_permit&, const tracing::trace_state_ptr&) {
        cached_pos = pos;
        return seastar::make_ready_future<>();
    }

    trie::load_final_node_result read_node(int64_t pos) {
        SCYLLA_ASSERT(pos == cached_pos);
        auto* x = rt.at(pos);
        return trie::load_final_node_result{
            .n_children = x->children.size(),
            .payload_bits = x->payload_bits,
        };
    }
    trie::node_traverse_result walk_down_along_key(int64_t pos, const_bytes key) {
        SCYLLA_ASSERT(pos == cached_pos);
        int i;
        for (i = 0; i + 1 < int(key.size()); ++i) {
            if (multibyte_traverse
                && rt.at(pos)->payload_bits == 0
                && rt.at(pos)->children.size() == 1
                && rt.at(pos)->children.begin()->first == key[i]
            ) {
                pos = pos - rt.at(pos)->children.front().second;
            } else {
                break;
            }
        }
        auto child = rt.at(pos)->lookup_child(key[i]);
        auto found = child != int(rt.at(pos)->children.size());
        return trie::node_traverse_result{
            .payload_bits = rt.at(pos)->payload_bits,
            .n_children = rt.at(pos)->children.size(),
            .found_idx = found ? child : rt.at(pos)->children.size(),
            .found_byte = found ? int(rt.at(pos)->children.at(child).first) : -1,
            .traversed_key_bytes = i,
            .body_pos = pos,
            .child_offset = found ? rt.at(pos)->children.at(child).second : -1,
        };
    }
    trie::node_traverse_sidemost_result walk_down_leftmost_path(int64_t pos) {
        SCYLLA_ASSERT(pos == cached_pos);
        auto* x = rt.at(pos);
        auto has_children = x->children.size() > 0;
        return trie::node_traverse_sidemost_result{
            .payload_bits = x->payload_bits,
            .n_children = x->children.size(),
            .body_pos = pos,
            .child_offset = has_children ? x->children.begin()->second : -1,
        };
    }
    trie::node_traverse_sidemost_result walk_down_rightmost_path(int64_t pos) {
        SCYLLA_ASSERT(pos == cached_pos);
        auto* x = rt.at(pos);
        auto has_children = x->children.size() > 0;
        return trie::node_traverse_sidemost_result{
            .payload_bits = x->payload_bits,
            .n_children = x->children.size(),
            .body_pos = pos,
            .child_offset = has_children ? x->children.rbegin()->second : -1,
        };
    }
    trie::get_child_result get_child(int64_t pos, int child_idx, bool forward) {
        SCYLLA_ASSERT(pos == cached_pos);
        auto* x = rt.at(pos);
        SCYLLA_ASSERT(size_t(child_idx) < x->children.size());
        return trie::get_child_result{
            .idx = child_idx,
            .offset = x->children[child_idx].second,
        };
    }
};
static_assert(trie::node_reader<custom_node_reader>);

static auto single_fragment_generator(std::span<const std::byte> only_frag) -> std::generator<std::span<const std::byte>> {
    co_yield only_frag;
}
static auto onebyte_fragment_generator(std::span<const std::byte> only_frag) -> std::generator<std::span<const std::byte>> {
    for (size_t i = 0; i < only_frag.size(); ++i) {
        co_yield only_frag.subspan(i, 1);
    }
}
static auto onebyte_or_empty_fragment_generator(std::span<const std::byte> only_frag) -> std::generator<std::span<const std::byte>> {
    for (size_t i = 0; i < only_frag.size(); ++i) {
        co_yield const_bytes();
        co_yield only_frag.subspan(i, 1);
    }
}
using fragment_generator_fn = decltype(&single_fragment_generator);
const static fragment_generator_fn fragment_generators[] = {
    single_fragment_generator,
    onebyte_fragment_generator,
    onebyte_or_empty_fragment_generator,
};

// Tests the traverse() function.
// 
// Testing strategy:
// build a trie containing the given set of keys,
// then test for all possible queries that the cursor created by `traverse`
// is in a legal state, and that the state corresponds to the right position in the set of keys.
// (And also test that "edges_traversed" is set correctly).
void test_traverse(std::span<const_bytes> key_domain, std::span<const_bytes> keys) {
    SCYLLA_ASSERT(!keys.empty());
    testlog.debug("test_traverse: testing key set: {}", keys);

    auto semaphore = tests::reader_concurrency_semaphore_wrapper();
    auto permit = semaphore.make_permit();
    auto trace_state = tracing::trace_state_ptr();

    auto ref_trie = reference_trie(keys);
    auto legal_states = ref_trie.enumerate_all_legal_states();
    custom_node_reader input{.rt = ref_trie};

    auto traverse_from_root = [&] (const_bytes key, fragment_generator_fn frag_gen) -> trie::traversal_state {
        return trie::traverse(input, frag_gen(key).begin(), ref_trie._nodes.size() - 1, permit, trace_state).get();
    };

    for (bool multibyte_traverse : {true, false})
    for (const auto& frag_gen : fragment_generators)
    for (const auto& x : key_domain) {
        input.allow_multibyte_traverse(multibyte_traverse);
        testlog.trace("Traversing \"{}\"", bytes_as_string(x));
        auto traversal_state = traverse_from_root(x, frag_gen);
        auto actual_cursor = reference_trie::cursor_state{
            .trail = traversal_state.trail
        };
        testlog.trace("Got: {}", fmt::join(actual_cursor.trail, ", "));
        auto lower_bound = std::ranges::lower_bound(keys, x, std::ranges::lexicographical_compare) - keys.begin();
        auto expected_rank = 2 * lower_bound + 1 - (lower_bound >= int(keys.size()) || !std::ranges::equal(x, keys[lower_bound]));
        auto actual_meaning = legal_states.at(actual_cursor);
        BOOST_REQUIRE_EQUAL(actual_meaning.rank, expected_rank);
        BOOST_REQUIRE_EQUAL(actual_meaning.depth, traversal_state.edges_traversed);
    }
}

// Tests the step() and step_back() functions.
// 
// Testing strategy:
// build a trie containing the given set of keys,
// then for every possible legal state of a cursor on that trie,
// check that the results of step()/step_back() are also legal states,
// which correspond to a rank incremented/decremented w.r.t. the starting state.
void test_step(std::span<const_bytes> keys) {
    testlog.debug("test_step: testing key set: {}", keys);

    auto semaphore = tests::reader_concurrency_semaphore_wrapper();
    auto permit = semaphore.make_permit();
    auto trace_state = tracing::trace_state_ptr();

    auto ref_trie = reference_trie(keys);
    auto legal_states = ref_trie.enumerate_all_legal_states();
    custom_node_reader input{.rt = ref_trie};

    for (const auto& x : legal_states) {
        testlog.trace("Testing state {}", fmt::join(x.first.trail, ", "));
        {
            auto trail = x.first.trail;
            trie::step(input, trail, permit, trace_state).get();
            // step() moves to the next key.
            // So if the starting state corresponds to a key (rank % 2 == 1), 
            // rank should grow by 2,
            // and if the starting state corresponds to an intermediate position (rank % 2 == 0),
            // rank should grow by 1.
            // (Unless the next key doesn't exist. Then the end state should correspond to the max rank).
            auto expected_rank = std::min<int>(x.second.rank + 1 + x.second.rank % 2, keys.size() * 2);
            testlog.trace("Got after step: {}", fmt::join(trail, ", "));
            auto actual_meaning = legal_states.at(reference_trie::cursor_state{.trail = trail});
            BOOST_REQUIRE_EQUAL(actual_meaning.rank, expected_rank);
        }
        {
            auto trail = x.first.trail;
            trie::step_back(input, trail, permit, trace_state).get();
            // step_back() moves to the previous key.
            // So if the starting state corresponds to a key (rank % 2 == 1), 
            // rank should shrink by 2,
            // and if the starting state corresponds to an intermediate position (rank % 2 == 0),
            // rank should shrink by 1.
            // (Unless the previous key doesn't exist. Then the end state should correspond to the first key.
            auto min_rank_for_step_back = keys.size() ? 1 : 0;
            auto expected_rank = std::max<int>(x.second.rank - 1 - x.second.rank % 2, min_rank_for_step_back);
            auto actual_meaning = legal_states.at(reference_trie::cursor_state{.trail = trail});
            BOOST_REQUIRE_EQUAL(actual_meaning.rank, expected_rank);
        }
    }
}

SEASTAR_THREAD_TEST_CASE(test_exhaustive) {
    // The length of the test grows at least exponentially with those parameters.
    // Anything bigger than these is overly long for CI.
    // But it might be a good idea to make them slightly bigger to check
    // changes in trie code.
    size_t max_input_length = 3;
    size_t max_set_size = 3;
    const char chars[] = "bdf";
    auto all_strings = tests::generate_all_strings(chars, max_input_length);
    auto all_strings_views = std::vector<const_bytes>();
    for (const auto& x : all_strings) {
        all_strings_views.push_back(string_as_bytes(x));
    }
    size_t case_counter = 0;
    testlog.info("test_exhaustive: start");
    for (size_t set_size = 0; set_size <= max_set_size; ++set_size) {
        auto subsets = generate_all_subsets(all_strings.size(), set_size);
        testlog.info("{} subsets to test", subsets.size());
        std::vector<const_bytes> test_set;
        for (const auto& x : subsets) {
            test_set.clear();
            for (const auto& i : x) {
                test_set.push_back(all_strings_views[i]);
            }
            if (case_counter % 1000 == 0) {
                testlog.info("test_exhaustive: in progress: cases={}", case_counter);
            }
            if (test_set.empty()) {
                continue; // Empty tries are illegal.
            }
            test_traverse(all_strings_views, test_set);
            test_step(test_set);
            case_counter += 1;
        }
    }
    testlog.info("test_exhaustive: cases={}", case_counter);
}
