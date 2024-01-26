#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include "schema/schema_fwd.hh"
#include "reader_permit.hh"
#include "streaming/stream_reason.hh"
#include "repair/decorated_key_with_hash.hh"
#include "readers/upgrading_consumer.hh"
#include <seastar/core/coroutine.hh>

using namespace seastar;

namespace db {
    class system_distributed_keyspace;
    namespace view {
        class view_update_generator;
    }
}

class mutation_fragment_queue {
public:
    class impl {
        std::vector<mutation_fragment_v2> _pending;
    public:
        virtual future<> push(mutation_fragment_v2 mf) = 0;
        virtual void abort(std::exception_ptr ep) = 0;
        virtual void push_end_of_stream() = 0;
        virtual ~impl() {}
        future<> flush() {
            for (auto&& mf : _pending) {
                co_await push(std::move(mf));
            }
            _pending.clear();
        }

        std::vector<mutation_fragment_v2>& pending() {
            return _pending;
        }
    };

private:

    class consumer {
        std::vector<mutation_fragment_v2>& _fragments;
    public:
        explicit consumer(std::vector<mutation_fragment_v2>& fragments)
            : _fragments(fragments)
        {}

        void operator()(mutation_fragment_v2 mf) {
            _fragments.push_back(std::move(mf));
        }
    };
    seastar::shared_ptr<impl> _impl;
    upgrading_consumer<consumer> _consumer;

public:
    mutation_fragment_queue(schema_ptr s, reader_permit permit, seastar::shared_ptr<impl> impl)
        : _impl(std::move(impl))
        , _consumer(*s, std::move(permit), consumer(_impl->pending()))
    {}

    future<> push(mutation_fragment mf) {
        _consumer.consume(std::move(mf));
        return _impl->flush();
    }

    void abort(std::exception_ptr ep) {
        _impl->abort(ep);
    }

    void push_end_of_stream() {
        _impl->push_end_of_stream();
    }
};

class repair_writer : public enable_lw_shared_from_this<repair_writer> {
    schema_ptr _schema;
    reader_permit _permit;
    // Current partition written to disk
    lw_shared_ptr<const decorated_key_with_hash> _current_dk_written_to_sstable;
    // Is current partition still open. A partition is opened when a
    // partition_start is written and is closed when a partition_end is
    // written.
    bool _partition_opened;
    named_semaphore _sem{1, named_semaphore_exception_factory{"repair_writer"}};
    bool _created_writer = false;
    uint64_t _estimated_partitions = 0;
public:
    class impl {
    public:
        virtual mutation_fragment_queue& queue() = 0;
        virtual future<> wait_for_writer_done() = 0;
        virtual void create_writer(lw_shared_ptr<repair_writer> writer) = 0;
        virtual ~impl() = default;
    };
private:
    std::unique_ptr<impl> _impl;
    mutation_fragment_queue* _mq;
public:
    repair_writer(
            schema_ptr schema,
            reader_permit permit,
            std::unique_ptr<impl> impl)
            : _schema(std::move(schema))
            , _permit(std::move(permit))
            , _impl(std::move(impl))
            , _mq(&_impl->queue())
    {}


    void set_estimated_partitions(uint64_t estimated_partitions) {
        _estimated_partitions = estimated_partitions;
    }

    uint64_t get_estimated_partitions() {
        return _estimated_partitions;
    }

    void create_writer() {
        _impl->create_writer(shared_from_this());
        _created_writer = true;
    }

    future<> do_write(lw_shared_ptr<const decorated_key_with_hash> dk, mutation_fragment mf);

    future<> wait_for_writer_done();

    named_semaphore& sem() {
        return _sem;
    }

    schema_ptr schema() const noexcept {
        return _schema;
    }

    mutation_fragment_queue& queue() {
        return _impl->queue();
    }

private:
    future<> write_start_and_mf(lw_shared_ptr<const decorated_key_with_hash> dk, mutation_fragment mf);
    future<> write_partition_end();
    future<> write_end_of_stream();
};

lw_shared_ptr<repair_writer> make_repair_writer(
            schema_ptr schema,
            reader_permit permit,
            streaming::stream_reason reason,
            sharded<replica::database>& db,
            sharded<db::system_distributed_keyspace>& sys_dist_ks,
            sharded<db::view::view_update_generator>& view_update_generator);

