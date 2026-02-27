#pragma once

#include "utils/assert.hh"
#include <seastar/core/future.hh>
#include <optional>

namespace utils {

using namespace seastar;

/// Synchronizes two processes (primary and secondary) in a way such that the primary process can know
/// that between block().get() and unblock() the secondary process is at a particular execution point, blocked in enter().
///
/// The primary calls block() to arm the throttle. The returned future resolves when the secondary calls enter().
/// enter() will return a future to the secondary which will resolve when the primary calls unblock().
class throttle {
    unsigned _block_counter = 0;
    promise<> _p; // valid when _block_counter != 0, resolves when goes down to 0
    std::optional<promise<>> _entered;
    bool _one_shot;
public:
    // one_shot means whether only the first enter() after block() will block.
    throttle(bool one_shot = false) : _one_shot(one_shot) {}
    future<> enter() {
        if (_block_counter && (!_one_shot || _entered)) {
            promise<> p1;
            promise<> p2;

            auto f1 = p1.get_future();

            // Intentional, the future is waited on indirectly.
            (void)p2.get_future().then([p1 = std::move(p1), p3 = std::move(_p)] () mutable {
                p1.set_value();
                p3.set_value();
            });
            _p = std::move(p2);
            if (_entered) {
                _entered->set_value();
                _entered.reset();
            }
            return f1;
        } else {
            return make_ready_future<>();
        }
    }

    future<> block() {
        ++_block_counter;
        _p = promise<>();
        _entered = promise<>();
        return _entered->get_future();
    }

    void unblock() {
        SCYLLA_ASSERT(_block_counter);
        if (--_block_counter == 0) {
            _p.set_value();
        }
    }
};

} // namespace utils
