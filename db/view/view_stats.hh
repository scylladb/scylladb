/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "service/storage_proxy_stats.hh"

namespace db {

namespace view {

struct stats : public service::storage_proxy_stats::write_stats {
    int64_t view_updates_pushed_local = 0;
    int64_t view_updates_pushed_remote = 0;
    int64_t view_updates_failed_local = 0;
    int64_t view_updates_failed_remote = 0;
    using label_instance = seastar::metrics::label_instance;
    stats(const sstring& category, label_instance ks_label, label_instance cf_label);
    void register_stats();
private:
    label_instance _ks_label;
    label_instance _cf_label;

};

} // namespace view

} // namespace db
