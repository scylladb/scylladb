/*
 * Copyright (C) 2020-present ScyllaDB
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
