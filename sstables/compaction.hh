/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 *
 */

#pragma once

#include "sstables.hh"

namespace sstables {
    class sstable_creator {
    public:
        // new_tmp() creates a new sstable, which is marked temporary
        // until all temporary sstables are finalized with commit().
        virtual shared_sstable new_tmp() = 0;
        virtual void commit() = 0;
        virtual ~sstable_creator() { };
    };


    future<> compact_sstables(std::vector<shared_sstable> sstables,
            schema_ptr schema, sstable_creator& creator);
}
