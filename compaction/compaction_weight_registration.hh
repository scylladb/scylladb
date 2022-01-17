/*
 * Copyright (C) 2017-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

class compaction_manager;

class compaction_weight_registration {
    compaction_manager* _cm;
    int _weight;
public:
    compaction_weight_registration(compaction_manager* cm, int weight);

    compaction_weight_registration& operator=(const compaction_weight_registration&) = delete;
    compaction_weight_registration(const compaction_weight_registration&) = delete;

    compaction_weight_registration& operator=(compaction_weight_registration&& other) noexcept;

    compaction_weight_registration(compaction_weight_registration&& other) noexcept;

    ~compaction_weight_registration();

    // Release immediately the weight hold by this object
    void deregister();

    int weight() const;
};
