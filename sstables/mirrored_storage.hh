// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
/*
 * Copyright (C) 2015-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/file.hh>
#include "sstables/storage.hh"

namespace s3 {
  class client;
}

namespace sstables {

/// mirrored_storage provides the persistent storage for SSTable.
///
/// each mirrored_storage instance is associated with a sstable. in addition to
/// persisting the sstable to the configured S3 bucket S3 bucket, it caches the
/// sstable with the local filesystem for low-latency access.
class mirrored_storage final : public storage {

    shared_ptr<s3::client> _s3_client;
    const sstring _bucket;
    const sstring _location;

    const std::filesystem::path _dir;
    std::filesystem::path _temp_dir;

public:
    mirrored_storage(shared_ptr<s3::client> s3_client,
                   sstring bucket,
                   sstring dir);
    ~mirrored_storage() override;
    future<> seal(const sstable& sst) override;
    future<> snapshot(const sstable& sst, sstring dir, absolute_path abs) const override;
    future<> change_state(const sstable& sst,
                          sstable_state to,
                          generation_type generation,
                          delayed_commit_changes* delay) override;
    // runs in async context

    // before starting writing the sstable components on the storage, mark the
    // beginning of creation. this step marks the beginning of the transaction
    // of creation the components of an sstable.
    void open(sstable& sst) override;
    future<> wipe(const sstable& sst, sync_dir) noexcept override;
    future<file> open_component(const sstable& sst,
                                component_type type,
                                open_flags flags,
                                file_open_options options,
                                bool check_integrity) override;
    future<data_sink> make_data_or_index_sink(sstable& sst,
                                                      component_type type) override;
    future<data_sink> make_component_sink(sstable& sst,
                                                  component_type type,
                                                  open_flags oflags,
                                                  file_output_stream_options options) override;
    future<> destroy(const sstable& sst) override;
    noncopyable_function<future<>(std::vector<shared_sstable>)> atomic_deleter() const override;
    future<> remove_by_registry_entry(entry_descriptor desc) override;

    sstring prefix() const  override;

private:
    void _open_local(sstable& sst);
    void _open_remote(sstable& sst);

    future<> _wipe_local(const sstable& sst, sync_dir sync) noexcept;
    future<> _wipe_remote(const sstable& sst) noexcept;

    void _open_local(const sstable& sst,
                     component_type type,
                     open_flags flags,
                     file_open_options options,
                     bool check_integrity);
    void _open_remote(const sstable& sst,
                      component_type type,
                      open_flags flags,
                      file_open_options options,
                      bool check_integrity);

    future<file> _open_component_local(const sstable& sst,
                                       component_type type,
                                       open_flags flags,
                                       file_open_options options,
                                       bool check_integrity);
    future<file> _open_component_remote(const sstable& sst,
                                        component_type type,
                                        open_flags flags,
                                        file_open_options options,
                                        bool check_integrity);

    future<data_sink> _make_data_sink_local(sstable& sst, component_type type);
    sstring _make_s3_object_name(const sstable& sst, component_type type) const;

    future<> _seal_local(const sstable& sst);
    future<> _seal_remote(const sstable& sst);
};

} // namespace sstables
