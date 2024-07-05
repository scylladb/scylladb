/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "mutation/json.hh"
#include "sstables/sstables.hh"

namespace tools {

class mutation_fragment_stream_json_writer {
    mutation_json::mutation_partition_json_writer _writer;
    bool _clustering_array_created;
private:
    void write(const clustering_row& cr);
    void write(const range_tombstone_change& rtc);
public:
    explicit mutation_fragment_stream_json_writer(const schema& s, std::ostream& os = std::cout)
        : _writer(s, os) {}
    mutation_json::json_writer& writer() { return _writer.writer(); }
    void start_stream();
    void start_sstable(const sstables::sstable* const sst);
    void start_partition(const partition_start& ps);
    void partition_element(const static_row& sr);
    void partition_element(const clustering_row& cr);
    void partition_element(const range_tombstone_change& rtc);
    void end_partition();
    void end_sstable();
    void end_stream();
};

} // namespace tools
