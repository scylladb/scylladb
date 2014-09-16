/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "packet.hh"

namespace net {

constexpr size_t packet::internal_data_size;

void packet::linearize(size_t at_frag, size_t desired_size) {
    size_t nr_frags = 0;
    size_t accum_size = 0;
    while (accum_size < desired_size) {
        accum_size += _fragments[at_frag + nr_frags].size;
        ++nr_frags;
    }
    std::unique_ptr<char[]> new_frag{new char[accum_size]};
    auto p = new_frag.get();
    for (size_t i = 0; i < nr_frags; ++i) {
        auto& f = _fragments[at_frag + i];
        p = std::copy(f.base, f.base + f.size, p);
    }
    _fragments.erase(_fragments.begin() + at_frag + 1, _fragments.begin() + at_frag + nr_frags);
    _fragments[at_frag] = fragment{new_frag.get(), accum_size};
    _deleter = make_deleter(std::move(_deleter), [buf = std::move(new_frag)] {});
}

}
