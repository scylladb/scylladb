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

#include <cinttypes>

namespace ser {

template <typename T>
class serializer;

};


namespace query {

/*
 * This struct is used in two incompatible ways.
 *
 * SEPARATE_PAGE_SIZE_AND_SAFETY_LIMIT cluster feature determines which way is
 * used.
 *
 * 1. If SEPARATE_PAGE_SIZE_AND_SAFETY_LIMIT is not enabled on the cluster then
 *    `page_size` field is ignored. Depending on the query type the meaning of
 *    the remaining two fields is:
 *
 *    a. For unpaged queries or for reverse queries:
 *
 *          * `soft_limit` is used to warn about queries that result exceeds
 *            this limit. If the limit is exceeded, a warning will be written to
 *            the log.
 *
 *          * `hard_limit` is used to terminate a query which result exceeds
 *            this limit. If the limit is exceeded, the operation will end with
 *            an exception.
 *
 *    b. For all other queries, `soft_limit` == `hard_limit` and their value is
 *       really a page_size in bytes. If the page is not previously cut by the
 *       page row limit then reaching the size of `soft_limit`/`hard_limit`
 *       bytes will cause a page to be finished.
 *
 * 2. If SEPARATE_PAGE_SIZE_AND_SAFETY_LIMIT is enabled on the cluster then all
 *    three fields are always set. They are used in different places:
 *
 *    a. `soft_limit` and `hard_limit` are used for unpaged queries and in a
 *       reversing reader used for reading KA/LA sstables. Their meaning is the
 *       same as in (1.a) above.
 *
 *    b. all other queries use `page_size` field only and the meaning of the
 *       field is the same ase in (1.b) above.
 *
 * Two interpretations of the `max_result_size` struct are not compatible so we
 * need to take care of handling a mixed clusters.
 *
 * As long as SEPARATE_PAGE_SIZE_AND_SAFETY_LIMIT cluster feature is not
 * supported by all nodes in the clustser, new nodes will always use the
 * interpretation described in the point (1). `soft_limit` and `hard_limit`
 * fields will be set appropriately to the query type and `page_size` field
 * will be set to 0. Old nodes will ignare `page_size` anyways and new nodes
 * will know to ignore it as well when it's set to 0. Old nodes will never set
 * `page_size` and that means new nodes will give it a default value of 0 and
 * ignore it for messages that miss this field.
 *
 * Once SEPARATE_PAGE_SIZE_AND_SAFETY_LIMIT cluster feature becomes supported by
 * the whole cluster, new nodes will start to set `page_size` to the right value
 * according to the interpretation described in the point (2).
 *
 * For each request, only the coordinator looks at
 * SEPARATE_PAGE_SIZE_AND_SAFETY_LIMIT and based on it decides for this request
 * whether it will be handled with interpretation (1) or (2). Then all the
 * replicas can check the decision based only on the message they receive.
 * If page_size is set to 0 or not set at all then the request will be handled
 * using the interpretation (1). Otherwise, interpretation (2) will be used.
 */
struct max_result_size {
    uint64_t soft_limit;
    uint64_t hard_limit;
private:
    uint64_t page_size = 0;
public:

    max_result_size() = delete;
    explicit max_result_size(uint64_t max_size) : soft_limit(max_size), hard_limit(max_size) { }
    explicit max_result_size(uint64_t soft_limit, uint64_t hard_limit) : soft_limit(soft_limit), hard_limit(hard_limit) { }
    max_result_size(uint64_t soft_limit, uint64_t hard_limit, uint64_t page_size)
            : soft_limit(soft_limit)
            , hard_limit(hard_limit)
            , page_size(page_size)
    { }
    uint64_t get_page_size() const {
        return page_size == 0 ? hard_limit : page_size;
    }
    friend bool operator==(const max_result_size&, const max_result_size&);
    friend class ser::serializer<query::max_result_size>;
};

inline bool operator==(const max_result_size& a, const max_result_size& b) {
    return a.soft_limit == b.soft_limit && a.hard_limit == b.hard_limit && a.page_size == b.page_size;
}

}
