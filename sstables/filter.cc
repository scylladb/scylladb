/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 *
 */
#include "core/future.hh"
#include "core/future-util.hh"
#include "core/shared_ptr.hh"
#include "core/do_with.hh"
#include <seastar/core/align.hh>

#include "types.hh"
#include "sstables.hh"
#include "utils/bloom_filter.hh"

namespace sstables {

future<> sstable::read_filter() {
    if (!has_component(sstable::component_type::Filter)) {
        _filter = std::make_unique<utils::filter::always_present_filter>();
        return make_ready_future<>();
    }

    return do_with(sstables::filter(), [this] (auto& filter) {
        return this->read_simple<sstable::component_type::Filter>(filter).then([this, &filter] {
            large_bitset bs(filter.buckets.elements.size() * 64);
            for (size_t i = 0; i != filter.buckets.elements.size(); ++i) {
                auto w = filter.buckets.elements[i];
                for (size_t j = 0; j < 64; ++j) {
                    if (w & (uint64_t(1) << j)) {
                        bs.set(i * 64 + j);
                    }
                }
            }
            _filter = utils::filter::create_filter(filter.hashes, std::move(bs));
        });
    });
}

void sstable::write_filter() {
    if (!has_component(sstable::component_type::Filter)) {
        return;
    }

    auto f = static_cast<utils::filter::murmur3_bloom_filter *>(_filter.get());

    auto&& bs = f->bits();
    std::deque<uint64_t> v(align_up(bs.size(), size_t(64)) / 64);
    for (size_t i = 0; i != bs.size(); ++i) {
        if (bs.test(i)) {
            v[i / 64] |= uint64_t(1) << (i % 64);
        }
    }

    auto filter = sstables::filter(f->num_hashes(), std::move(v));
    write_simple<sstable::component_type::Filter>(filter);
}

}
