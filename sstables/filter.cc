/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 *
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
#include "core/future.hh"
#include "core/future-util.hh"
#include "core/shared_ptr.hh"
#include "core/do_with.hh"
#include <seastar/core/align.hh>

#include "types.hh"
#include "sstables.hh"
#include "utils/bloom_filter.hh"

namespace sstables {

future<> sstable::read_filter(const io_priority_class& pc) {
    if (!has_component(sstable::component_type::Filter)) {
        _filter = std::make_unique<utils::filter::always_present_filter>();
        return make_ready_future<>();
    }

    return do_with(sstables::filter(), [this, &pc] (auto& filter) {
        return this->read_simple<sstable::component_type::Filter>(filter, pc).then([this, &filter] {
            large_bitset bs(filter.buckets.elements.size() * 64);
            bs.load(filter.buckets.elements.begin(), filter.buckets.elements.end());
            _filter = utils::filter::create_filter(filter.hashes, std::move(bs));
        }).then([this] {
            return engine().file_size(this->filename(sstable::component_type::Filter));
        });
    }).then([this] (auto size) {
        _filter_file_size = size;
    });
}

void sstable::write_filter(const io_priority_class& pc) {
    if (!has_component(sstable::component_type::Filter)) {
        return;
    }

    auto f = static_cast<utils::filter::murmur3_bloom_filter *>(_filter.get());

    auto&& bs = f->bits();
    std::deque<uint64_t> v(align_up(bs.size(), size_t(64)) / 64);
    bs.save(v.begin());
    auto filter = sstables::filter(f->num_hashes(), std::move(v));
    write_simple<sstable::component_type::Filter>(filter, pc);
}

}
