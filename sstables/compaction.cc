/*
 * Copyright 2015 Cloudius Systems
 */

#include <vector>
#include <functional>

#include "core/future-util.hh"
#include "core/pipe.hh"

#include "sstables.hh"
#include "compaction.hh"
#include "mutation_reader.hh"

namespace sstables {

// compact_sstables compacts the given list of sstables creating one
// (currently) or more (in the future) new sstables. The new sstables
// are created using the "sstable_creator" object passed by the caller.
future<> compact_sstables(std::vector<shared_sstable> sstables,
        schema_ptr schema, std::function<shared_sstable()> creator) {
    std::vector<::mutation_reader> readers;
    uint64_t estimated_partitions = 0;
    auto newtab = creator();

    for (auto sst : sstables) {
        // We also capture the sstable, so we keep it alive while the read isn't done
        readers.emplace_back([sst, r = make_lw_shared(sst->read_rows(schema))] () mutable { return r->read(); });
        // FIXME: If the sstables have cardinality estimation bitmaps, use that
        // for a better estimate for the number of partitions in the merged
        // sstable than just adding up the lengths of individual sstables.
        estimated_partitions += sst->get_estimated_key_count();
        // Compacted sstable keeps track of its ancestors.
        newtab->add_ancestor(sst->generation());
    }
    auto combined_reader = make_combined_reader(std::move(readers));

    // We use a fixed-sized pipe between the producer fiber (which reads the
    // individual sstables and merges them) and the consumer fiber (which
    // only writes to the sstable). Things would have worked without this
    // pipe (the writing fiber would have also performed the reads), but we
    // prefer to do less work in the writer (which is a seastar::thread),
    // and also want the extra buffer to ensure we do fewer context switches
    // to that seastar::thread.
    // TODO: better tuning for the size of the pipe. Perhaps should take into
    // account the size of the individual mutations?
    seastar::pipe<mutation> output{16};
    auto output_reader = make_lw_shared<seastar::pipe_reader<mutation>>(std::move(output.reader));
    auto output_writer = make_lw_shared<seastar::pipe_writer<mutation>>(std::move(output.writer));

    auto done = make_lw_shared<bool>(false);
    future<> read_done = do_until([done] { return *done; }, [done, output_writer, combined_reader = std::move(combined_reader)] {
        return combined_reader().then([done = std::move(done), output_writer = std::move(output_writer)] (auto mopt) {
            if (mopt) {
                return output_writer->write(std::move(*mopt));
            } else {
                *done = true;
                return make_ready_future<>();
            }
        });
    });

    ::mutation_reader mutation_queue_reader = [output_reader] () {
        return output_reader->read();
    };

    future<> write_done = newtab->write_components(
            std::move(mutation_queue_reader), estimated_partitions, schema).then([newtab] {
        return newtab->load().then([newtab] {});
    });

    // Wait for both read_done and write_done fibers to finish.
    // FIXME: if write_done throws an exception, we get a broken pipe
    // exception on read_done, and then we don't handle write_done's
    // exception, causing a warning message of "ignored exceptional future".
    return read_done.then([write_done = std::move(write_done)] () mutable { return std::move(write_done); });
}

}
