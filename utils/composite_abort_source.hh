#include "seastar/core/abort_source.hh"
#include "utils/small_vector.hh"

namespace utils {
// A facility to combine several abort_source-s and expose them as a single abort_source.
// The combined abort_source is aborted whenever any of the added abort_sources is aborted.
// Typical use case: there are several sources of abort signal (e.g. timeout and node shutdown)
// and we what some routine to be aborted on any of them.
class composite_abort_source {
    abort_source _as;
    utils::small_vector<abort_source::subscription, 2> _subscriptions;
public:
    void add(abort_source& as) {
        as.check();
        auto sub = as.subscribe([this]() noexcept { _as.request_abort(); });
        assert(sub);
        _subscriptions.push_back(std::move(*sub));
    }
    abort_source& abort_source() noexcept {
        return _as;
    }
};
}