#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include "schema_fwd.hh"
#include <optional>
#include "reader_permit.hh"
#include "streaming/stream_reason.hh"
#include "repair/decorated_key_with_hash.hh"
#include "readers/queue.hh"
#include "sstables/sstable_set.hh"

using namespace seastar;

namespace db {
    class system_distributed_keyspace;
    namespace view {
        class view_update_generator;
    }
}

class repair_writer : public enable_lw_shared_from_this<repair_writer> {
    schema_ptr _schema;
    reader_permit _permit;
    uint64_t _estimated_partitions;
    std::optional<future<>> _writer_done;
    std::optional<queue_reader_handle> _mq;
    // Current partition written to disk
    lw_shared_ptr<const decorated_key_with_hash> _current_dk_written_to_sstable;
    // Is current partition still open. A partition is opened when a
    // partition_start is written and is closed when a partition_end is
    // written.
    bool _partition_opened;
    streaming::stream_reason _reason;
    named_semaphore _sem{1, named_semaphore_exception_factory{"repair_writer"}};
public:
    repair_writer(
            schema_ptr schema,
            reader_permit permit,
            uint64_t estimated_partitions,
            streaming::stream_reason reason)
            : _schema(std::move(schema))
            , _permit(std::move(permit))
            , _estimated_partitions(estimated_partitions)
            , _reason(reason) {
    }

    void create_writer(sharded<replica::database>& db, sharded<db::system_distributed_keyspace>& sys_dist_ks, sharded<db::view::view_update_generator>& view_update_gen);

    future<> do_write(lw_shared_ptr<const decorated_key_with_hash> dk, mutation_fragment mf);

    future<> wait_for_writer_done();

    named_semaphore& sem() {
        return _sem;
    }

private:
    future<> write_start_and_mf(lw_shared_ptr<const decorated_key_with_hash> dk, mutation_fragment mf);

    static sstables::offstrategy is_offstrategy_supported(streaming::stream_reason reason) {
        static const std::unordered_set<streaming::stream_reason> operations_supported = {
            streaming::stream_reason::bootstrap,
            streaming::stream_reason::replace,
            streaming::stream_reason::removenode,
            streaming::stream_reason::decommission,
            streaming::stream_reason::repair,
            streaming::stream_reason::rebuild,
        };
        return sstables::offstrategy(operations_supported.contains(reason));
    }

    future<> write_partition_end();
    future<> write_end_of_stream();
    future<> do_wait_for_writer_done();
};

