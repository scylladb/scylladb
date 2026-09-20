/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <memory>

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>
#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/util/defer.hh>

#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/sstables_manager_subscription.hh"

#include "test/lib/simple_schema.hh"
#include "test/lib/sstable_test_env.hh"
#include "test/lib/sstable_utils.hh"

namespace {

// State shared between the test and the notification callback.
//
// The callback records its progress here rather than in the handler itself,
// so that the test can observe a notification that outlives the handler
// without dereferencing the destroyed handler.
struct notification_probe {
    // Resolved by the test to resume the suspended notification.
    promise<> resume;
    // Cleared by ~test_event_handler.
    bool handler_alive = true;
    bool notified = false;
    bool completed = false;
    // Set if the notification callback ran after the handler was destroyed.
    // This is the use-after-free.
    bool resumed_after_destruction = false;
};

// Mimics db::snapshot::backup_task_impl::worker: the deleted_sstable callback
// suspends and then uses the handler.
class test_event_handler final : public sstables::sstables_manager_event_handler {
    notification_probe& _probe;

public:
    explicit test_event_handler(notification_probe& probe) noexcept
        : _probe(probe)
    { }

    ~test_event_handler() {
        _probe.handler_alive = false;
    }

    virtual future<> deleted_sstable(sstables::generation_type gen) const override {
        // Read the handler's state before suspending, the way
        // backup_task_impl::worker::deleted_sstable captures `this`
        // in the lambda it passes to smp::submit_to.
        auto& probe = _probe;
        probe.notified = true;

        // backup_task_impl::worker::deleted_sstable suspends here, on
        // smp::submit_to(_task._backup_shard, ...). Suspend on a promise the
        // test controls, so that the handler can be destroyed while the
        // notification is in flight, deterministically.
        co_await probe.resume.get_future();

        // Round-trip through another shard, like the real callback does.
        co_await smp::submit_to((this_shard_id() + 1) % this_smp_shard_count(), [] { });

        // The real callback dereferences the handler here (it calls
        // _task.on_sstable_deletion(gen)). Check that the handler is still
        // alive rather than reproducing the undefined behaviour.
        if (!probe.handler_alive) {
            probe.resumed_after_destruction = true;
        }
        probe.completed = true;
    }
};

} // anonymous namespace

BOOST_AUTO_TEST_SUITE(sstables_manager_subscription_test)

// Reproducer for the use-after-free where an sstables_manager notification
// outlives the handler it was delivered to.
//
// An sstable deletion notification may suspend, and the future it returns is
// discarded by the boost::signals2 signal that invokes it. Disconnecting the
// subscription does not wait for a notification that is already in flight, so
// the handler must be stopped, which does wait for it, before it is destroyed.
SEASTAR_TEST_CASE(test_deleted_sstable_notification_does_not_outlive_the_handler) {
    return sstables::test_env::do_with_async([] (sstables::test_env& env) {
        simple_schema ss;
        auto s = ss.schema();
        auto pks = ss.make_pkeys(1);

        auto mut = mutation(s, pks[0]);
        mut.partition().apply_insert(*s, ss.make_ckey(0), ss.new_timestamp());
        auto sst = make_sstable_containing(env.make_sstable(s), {std::move(mut)}).get();

        notification_probe probe;
        auto handler = std::make_unique<test_event_handler>(probe);
        env.manager().subscribe(*handler);

        // Deleting the sstable notifies the handler synchronously, from
        // sstables_manager::on_unlink. The callback then suspends.
        sst->unlink().get();
        BOOST_REQUIRE(probe.notified);
        BOOST_REQUIRE(!probe.completed);

        // Stop the handler with the notification still in flight, the way
        // sharded<worker>::stop() does at the end of
        // db::snapshot::backup_task_impl::do_backup().
        auto stopped = handler->stop();
        {
            // Resume the notification and wait for stop() when leaving this
            // scope, even if the check fails, so that the handler is not
            // destroyed with the notification still in flight.
            auto resume = defer([&] () noexcept {
                probe.resume.set_value();
                stopped.get();
            });
            // stop() must not complete while the notification is in flight.
            BOOST_REQUIRE(!stopped.available());
        }
        BOOST_REQUIRE(probe.completed);

        handler.reset();
        BOOST_REQUIRE(!probe.resumed_after_destruction);
    });
}

BOOST_AUTO_TEST_SUITE_END()
