/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#define BOOST_TEST_MODULE core
#include <boost/test/unit_test.hpp>
#include <xxhash.h>
#include <fmt/ranges.h>
#include <numeric>
#include "test/lib/log.hh"
#include "test/lib/key_utils.hh"
#include "utils/bit_cast.hh"
#include "sstables/trie/trie_writer.hh"

using namespace sstables::trie;

inline const_bytes string_as_bytes(std::string_view sv) {
    return std::as_bytes(std::span(sv.data(), sv.size()));
}

inline std::string_view bytes_as_string(const_bytes sv) {
    return {reinterpret_cast<const char*>(sv.data()), sv.size()};
}

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

// Checks that the stream of nodes produced by a trie_writer
// with given parameters is consistent with the inputs.
void test_one_set_of_strings(
    const std::vector<std::string_view>& inputs,
    size_t branching_factor,
    size_t page_size,
    size_t max_chain_len,
    size_t start_pos,
    uint64_t seed
) {
    testlog.debug("test_one_set_of_strings: inputs={}, page_size={}, max_chain_len={}", inputs, page_size, max_chain_len);
    assert(std::ranges::is_sorted(inputs));
    struct trie_output_stream {
        sink_pos _pos{0};
        size_t _page_size = 0;
        uint64_t _seed = 0;
        uint64_t _padding = 0;
        struct serialized {
            trie_payload payload;
            std::vector<std::byte> transition;
            std::vector<sink_pos> children;
        };
        std::map<sink_pos, serialized> _output;

        trie_output_stream(size_t page_size, uint64_t seed) : _page_size(page_size), _seed(seed) {
        }
        node_size serialized_size(const writer_node& x, sink_pos start_pos) const {
            // Returns a "random" size in range [1, page_size].
            //
            // With enough test cases, this should be enough to test the full variety
            // of interesting situations in the writer.
            //
            // The writer assumes that the size depends only on the array of offsets of the
            // node to its children. Therefore we take the hash of the offsets as the "random" number.
            std::vector<uint64_t> relevant_data;
            size_t total_offset = 0;
            relevant_data.reserve(x.get_children().size() * 2);
            std::span children = x.get_children();
            for (auto it = children.rbegin(); it != children.rend(); ++it) {
                const auto& c = *it;
                testlog.trace("serialized_size: child={}, startpos={} ns={}, bs={}, pos={}", fmt::ptr(&c), start_pos.value, c->_node_size.value, c->_branch_size.value, c->_pos.value);

                if (c->_pos.valid()) {
                    relevant_data.push_back((start_pos - c->_pos).value);
                } else {
                    total_offset += c->_node_size.value;
                    relevant_data.push_back(total_offset - 1);
                    total_offset += c->_branch_size.value;
                }
            }
            auto hash = XXH64(relevant_data.data(), std::span(relevant_data).size_bytes(), _seed);
            auto result = 1 + hash % _page_size;
            testlog.trace("serialized_size: x={}, result={}, offsets={}", fmt::ptr(&x), result, relevant_data);
            return node_size(result);
        }
        sink_pos write(const writer_node& x, sink_pos start_pos) {
            serialized s;
            for (const auto& c : x.get_children()) {
                BOOST_REQUIRE(c->_pos.valid());
                s.children.push_back(c->_pos);
            }
            const_bytes transition_view{x._transition.get(), x._transition_length};
            s.transition = std::vector<std::byte>(transition_view.begin(), transition_view.end());
            s.payload = x._payload;
            sink_pos result = _pos + sink_offset(1);
            auto sz = serialized_size(x, _pos);
            _pos = _pos + sz;
            _output.insert({result, std::move(s)});
            testlog.trace("write: {}", sz.value);
            return result;
        }
        size_t page_size() const {
            return _page_size;
        };
        void pad_to_page_boundary() {
            size_t pad = bytes_left_in_page();
            _padding += pad;
            testlog.trace("pad: {}", pad);
            _pos = _pos + sink_offset(pad);
        };
        size_t bytes_left_in_page() const {
            return round_up(_pos.value + 1, page_size()) - _pos.value;
        };
        sink_pos pos() const {
            return _pos;
        }
        void skip(size_t sz) {
            _pos = _pos + sink_offset(sz);
        }
    };
    static_assert(trie_writer_sink<trie_output_stream>);

    trie_output_stream out(page_size, seed);

    out.skip(start_pos);

    const auto max_expected_alloc = branching_factor * sizeof(writer_node) + sizeof(std::max_align_t);
    auto wr = trie_writer(out, max_chain_len, std::bit_ceil(max_expected_alloc));

    std::vector<trie_payload> payloads;

    std::string_view prev = {};
    for (size_t i = 0; i < inputs.size(); ++i) {
        size_t depth = std::ranges::mismatch(inputs[i], prev).in1 - inputs[i].begin();
        payloads.push_back(trie_payload(1 + i%15, object_representation(42 + i)));
        wr.add(depth, string_as_bytes(inputs[i]).subspan(depth), payloads.back());
        prev = inputs[i];
    }

    auto root_pos = wr.finish();

    if (inputs.size() == 0) {
        BOOST_REQUIRE(!root_pos.valid());
        BOOST_REQUIRE_EQUAL(out.pos().value, start_pos);
        return;
    }

    assert(root_pos.valid());

    std::vector<std::string> outputs;
    std::vector<trie_payload> output_payloads;
    struct local_state {
        sink_pos _idx;
        int _stage;
    };
    std::vector<size_t> transition_lengths;
    std::vector<std::byte> transition_stack;
    std::vector<local_state> stack;
    stack.push_back({root_pos, -1});
    while (stack.size()) {
        auto& [idx, stage] = stack.back();
        const auto& node = out._output.at(idx);
        if (stage < 0) {
            transition_stack.insert(transition_stack.end(), node.transition.begin(), node.transition.end());
            transition_lengths.push_back(node.transition.size());
            if (node.payload._payload_bits) {
                outputs.push_back(std::string(bytes_as_string(transition_stack).substr(1)));
                output_payloads.push_back(node.payload);
            }
            stage += 1;
        }
        if (stage < int(node.children.size())) {
            stage += 1;
            stack.push_back({node.children[stage - 1], -1});
            continue;
        }
        transition_stack.resize(transition_stack.size() - transition_lengths.back());
        transition_lengths.pop_back();
        stack.pop_back();
    }
    testlog.debug("Outputs: {}", outputs);
    BOOST_REQUIRE(std::ranges::equal(inputs, outputs));
    BOOST_REQUIRE(std::ranges::equal(payloads, output_payloads));
}

// Checks that the writer produces the right results with all possible datasets,
// smaller (w.r.t. various parameters) than some arbitrary choice.
//
// The choice should be small enough to finish in reasonable time,
// and large enough to provide coverage for all logic.
// (If you modify the limits, you should probably at least check
// that you didn't make code coverage lower).
BOOST_AUTO_TEST_CASE(test_exhaustive) {
    // testlog.set_level(seastar::log_level::trace);
    // trie_logger.set_level(seastar::log_level::trace);
    size_t max_input_length = 3;
    size_t max_set_size = 3;
    size_t max_page_size = 3;
    size_t max_start_pos = 1;
    const char chars[] = "abc";
    auto all_strings = tests::generate_all_strings(chars, max_input_length);
    size_t case_counter = 0;
    testlog.info("test_exhaustive: start");
    for (size_t set_size = 0; set_size <= max_set_size; ++set_size) {
        auto subsets = generate_all_subsets(all_strings.size(), set_size);
        std::vector<std::string_view> test_set;
        for (const auto& x : subsets) {
            test_set.clear();
            for (const auto& i : x) {
                test_set.push_back(all_strings[i]);
            }
            for (size_t page_size = 1; page_size <= max_page_size; ++page_size)
            for (size_t max_chain_len = 1; max_chain_len <= max_input_length; ++max_chain_len)
            for (size_t start_pos = 0; start_pos <= max_start_pos; ++start_pos) {
                if (case_counter % 1000 == 0) {
                    testlog.debug("test_exhaustive: in progress: cases={}", case_counter);
                }
                test_one_set_of_strings(test_set, std::size(chars), page_size, max_chain_len, start_pos, case_counter);
                case_counter += 1;
            }
        }
    }
    testlog.info("test_exhaustive: cases={}", case_counter);
}
