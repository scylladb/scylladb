/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "sstables/sstables.hh"
#include "query-request.hh"
#include "mutation_reader.hh"

class sstable_range_wrapping_reader final : public mutation_reader::impl {
    lw_shared_ptr<sstables::sstable> _sst;
    sstables::mutation_reader _smr;
public:
    sstable_range_wrapping_reader(lw_shared_ptr<sstables::sstable> sst,
        schema_ptr s, const query::partition_range& pr)
        : _sst(sst)
        , _smr(sst->read_range_rows(std::move(s), pr)) {
    }
    virtual future<mutation_opt> operator()() override {
        return _smr.read();
    }
};
