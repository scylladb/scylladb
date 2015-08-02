/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 *
 */
#include "core/future.hh"
#include "core/future-util.hh"
#include "core/shared_ptr.hh"
#include "core/do_with.hh"

#include "types.hh"
#include "sstables.hh"
#include "utils/bloom_filter.hh"

namespace sstables {

future<> sstable::read_filter() {
    auto ft = _filter_tracker;
    return _filter_tracker->start(std::move(ft)).then([this] {
        // FIXME: should stop this service. This one is definitely wrong to stop at_exit.
        // We should use a Deleter class in lw_shared_ptr
        if (!has_component(sstable::component_type::Filter)) {
            _filter = std::make_unique<utils::filter::always_present_filter>();
            return make_ready_future<>();
        }

        return do_with(sstables::filter(), [this] (auto& filter) {
            return this->read_simple<sstable::component_type::Filter>(filter).then([this, &filter] {
                utils::filter::bloom_filter::bitmap bs;
                bs.append(filter.buckets.elements.begin(), filter.buckets.elements.end());
                _filter = utils::filter::create_filter(filter.hashes, std::move(bs));
            });
        });
    });
}

void sstable::write_filter() {
    if (!has_component(sstable::component_type::Filter)) {
        return;
    }

    auto f = static_cast<utils::filter::murmur3_bloom_filter *>(_filter.get());

    std::vector<utils::filter::bloom_filter::bitmap_block> v;
    boost::to_block_range(f->bits(), std::back_inserter(v));

    auto filter = sstables::filter(f->num_hashes(), std::move(v));
    write_simple<sstable::component_type::Filter>(filter);
}

}
