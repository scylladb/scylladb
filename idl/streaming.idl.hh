/*
 * Copyright 2016-present ScyllaDB
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

namespace streaming {

class stream_request {
    sstring keyspace;
    // For compatibility with <= 1.5, we use wrapping ranges
    // (though we never send wraparounds; only allow receiving them)
    std::vector<range<dht::token>> ranges_compat();
    std::vector<sstring> column_families;
};

class stream_summary {
    utils::UUID cf_id;
    int files;
    long total_size;
};


class prepare_message {
    std::vector<streaming::stream_request> requests;
    std::vector<streaming::stream_summary> summaries;
    uint32_t dst_cpu_id;
};

enum class stream_reason : uint8_t {
    unspecified,
    bootstrap,
    decommission,
    removenode,
    rebuild,
    repair,
    replace,
};

enum class stream_mutation_fragments_cmd : uint8_t {
    error,
    mutation_fragment_data,
    end_of_stream,
};

}
