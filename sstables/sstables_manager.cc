/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "log.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/partition_index_cache.hh"
#include "sstables/sstables.hh"
#include "db/config.hh"
#include "gms/feature.hh"
#include "gms/feature_service.hh"
#include "db/system_keyspace.hh"
#include "utils/s3/client.hh"

namespace sstables {

logging::logger smlogger("sstables_manager");

sstables_manager::sstables_manager(
    db::large_data_handler& large_data_handler, const db::config& dbcfg, gms::feature_service& feat, cache_tracker& ct, size_t available_memory, directory_semaphore& dir_sem, noncopyable_function<locator::host_id()>&& resolve_host_id, storage_manager* shared)
    : _storage(shared)
    , _large_data_handler(large_data_handler), _db_config(dbcfg), _features(feat), _cache_tracker(ct)
    , _sstable_metadata_concurrency_sem(
        max_count_sstable_metadata_concurrent_reads,
        max_memory_sstable_metadata_concurrent_reads(available_memory),
        "sstable_metadata_concurrency_sem",
        std::numeric_limits<size_t>::max(),
        utils::updateable_value(std::numeric_limits<uint32_t>::max()),
        utils::updateable_value(std::numeric_limits<uint32_t>::max()))
    , _dir_semaphore(dir_sem)
    , _resolve_host_id(std::move(resolve_host_id))
{
}

sstables_manager::~sstables_manager() {
    assert(_closing);
    assert(_active.empty());
    assert(_undergoing_close.empty());
}

storage_manager::storage_manager(const db::config& cfg, config stm_cfg)
    : _s3_clients_memory(0)
    , _config_updater(this_shard_id() == 0 ? std::make_unique<config_updater>(cfg, *this) : nullptr)
{
    for (auto [ep, ecfg] : cfg.object_storage_config()) {
        _s3_endpoints.emplace(std::make_pair(std::move(ep), make_lw_shared<s3::endpoint_config>(std::move(ecfg))));
    }
}

future<> storage_manager::stop() {
    if (_config_updater) {
        co_await _config_updater->action.join();
    }

    for (auto ep : _s3_endpoints) {
        if (ep.second.client != nullptr) {
            co_await ep.second.client->close();
        }
    }
}

void storage_manager::update_config(const db::config& cfg) {
    for (auto [ep, ecfg] : cfg.object_storage_config()) {
        auto s3_cfg = make_lw_shared<s3::endpoint_config>(std::move(ecfg));
        auto [it, added] = _s3_endpoints.try_emplace(ep, std::move(s3_cfg));
        if (!added) {
            if (it->second.client != nullptr) {
                it->second.client->update_config(s3_cfg);
            }
            it->second.cfg = std::move(s3_cfg);
        }
    }
}

shared_ptr<s3::client> storage_manager::get_endpoint_client(sstring endpoint) {
    auto found = _s3_endpoints.find(endpoint);
    if (found == _s3_endpoints.end()) {
        smlogger.error("unable to find {} in configured object-storage endpoints", endpoint);
        throw std::invalid_argument(format("endpoint {} not found", endpoint));
    }
    auto& ep = found->second;
    if (ep.client == nullptr) {
        ep.client = s3::client::make(endpoint, ep.cfg, _s3_clients_memory, [ &ct = container() ] (std::string ep) {
            return ct.local().get_endpoint_client(ep);
        });
    }
    return ep.client;
}

storage_manager::config_updater::config_updater(const db::config& cfg, storage_manager& sstm)
    : action([&sstm, &cfg] () mutable {
        return sstm.container().invoke_on_all([&cfg] (auto& sstm) {
            sstm.update_config(cfg);
        });
    })
    , observer(cfg.object_storage_config.observe(action.make_observer()))
{}

locator::host_id sstables_manager::get_local_host_id() const {
    return _resolve_host_id();
}

bool sstables_manager::uuid_sstable_identifiers() const {
    return _features.uuid_sstable_identifiers;
}

shared_sstable sstables_manager::make_sstable(schema_ptr schema,
        sstring table_dir,
        const data_dictionary::storage_options& storage,
        generation_type generation,
        sstable_state state,
        sstable_version_types v,
        sstable_format_types f,
        gc_clock::time_point now,
        io_error_handler_gen error_handler_gen,
        size_t buffer_size) {
    return make_lw_shared<sstable>(std::move(schema), std::move(table_dir), storage, generation, state, v, f, get_large_data_handler(), *this, now, std::move(error_handler_gen), buffer_size);
}

sstable_writer_config sstables_manager::configure_writer(sstring origin) const {
    sstable_writer_config cfg;

    cfg.promoted_index_block_size = _db_config.column_index_size_in_kb() * 1024;
    cfg.promoted_index_auto_scale_threshold = (size_t)_db_config.column_index_auto_scale_threshold_in_kb() * 1024;
    if (!cfg.promoted_index_auto_scale_threshold) {
        cfg.promoted_index_auto_scale_threshold = std::numeric_limits<size_t>::max();
    }
    cfg.validation_level = _db_config.enable_sstable_key_validation()
            ? mutation_fragment_stream_validation_level::clustering_key
            : mutation_fragment_stream_validation_level::token;
    cfg.summary_byte_cost = summary_byte_cost(_db_config.sstable_summary_ratio());

    cfg.origin = std::move(origin);

    return cfg;
}

void sstables_manager::add(sstable* sst) {
    _active.push_back(*sst);
}

void sstables_manager::deactivate(sstable* sst) {
    // At this point, sst has a reference count of zero, since we got here from
    // lw_shared_ptr_deleter<sstables::sstable>::dispose().
    _active.erase(_active.iterator_to(*sst));
    _undergoing_close.push_back(*sst);
    // guard against sstable::close_files() calling shared_from_this() and immediately destroying
    // the result, which will dispose of the sstable recursively
    auto ptr = sst->shared_from_this();
    (void)sst->destroy().finally([ptr] {
        // destruction of ptr will call maybe_done() and release close()
    });
}

void sstables_manager::remove(sstable* sst) {
    _undergoing_close.erase(_undergoing_close.iterator_to(*sst));
    delete sst;
    maybe_done();
}

void sstables_manager::maybe_done() {
    if (_closing && _active.empty() && _undergoing_close.empty()) {
        _done.set_value();
    }
}

future<> sstables_manager::delete_atomically(std::vector<shared_sstable> ssts) {
    if (ssts.empty()) {
        co_return;
    }

    // All sstables here belong to the same table, thus they do live
    // in the same storage so it's OK to get the deleter from the
    // front element. The deleter implementation is welcome to check
    // that sstables from the vector really live in it.
    auto deleter = ssts.front()->get_storage().atomic_deleter();
    co_await deleter(std::move(ssts));
}

future<> sstables_manager::close() {
    _closing = true;
    maybe_done();
    co_await _done.get_future();
    co_await _sstable_metadata_concurrency_sem.stop();
}

void sstables_manager::plug_system_keyspace(db::system_keyspace& sys_ks) noexcept {
    _sys_ks = sys_ks.shared_from_this();
}

void sstables_manager::unplug_system_keyspace() noexcept {
    _sys_ks = nullptr;
}

}   // namespace sstables
