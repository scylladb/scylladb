/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */
#pragma once

namespace sstables {
class sstable;
}

class filter_tracker {
    uint64_t false_positive = 0;
    uint64_t true_positive = 0;

    uint64_t last_false_positive = 0;
    uint64_t last_true_positive = 0;
public:
    void add_false_positive() {
        false_positive++;
    }

    void add_true_positive() {
        true_positive++;
    }

    friend class sstables::sstable;
};
