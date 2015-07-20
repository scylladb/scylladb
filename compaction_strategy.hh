/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

class column_family;

namespace sstables {

enum class compaction_strategy_type {
    null,
    major,
    size_tiered,
    // FIXME: Add support to LevelTiered, and DateTiered.
};

class compaction_strategy_impl;

class compaction_strategy {
    ::shared_ptr<compaction_strategy_impl> _compaction_strategy_impl;
public:
    compaction_strategy(::shared_ptr<compaction_strategy_impl> impl);

    compaction_strategy();
    ~compaction_strategy();
    compaction_strategy(const compaction_strategy&);
    compaction_strategy(compaction_strategy&&);
    compaction_strategy& operator=(compaction_strategy&&);

    future<> compact(column_family& cfs);
};

// Creates a compaction_strategy object from one of the strategies available.
compaction_strategy make_compaction_strategy(compaction_strategy_type strategy);

}
