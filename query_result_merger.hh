#include "core/distributed.hh"
#include "query.hh"

namespace query {

// Merges non-overlapping results into one
// Implements @Reducer concept from distributed.hh
class result_merger {
    std::vector<foreign_ptr<lw_shared_ptr<query::result>>> _partial;
public:
    void reserve(size_t size) {
        _partial.reserve(size);
    }

    void operator()(foreign_ptr<lw_shared_ptr<query::result>> r) {
        _partial.emplace_back(std::move(r));
    }

    foreign_ptr<lw_shared_ptr<query::result>> get() && {
        auto merged = make_lw_shared<query::result>();

        size_t partition_count = 0;
        for (auto&& r : _partial) {
            partition_count += r->partitions.size();
        }

        merged->partitions.reserve(partition_count);

        for (auto&& r : _partial) {
            std::copy(r->partitions.begin(), r->partitions.end(), std::back_inserter(merged->partitions));
        }

        return make_foreign(std::move(merged));
    }
};

}
