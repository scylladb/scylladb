/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/coroutine/switch_to.hh>
#include <unordered_map>
#include "utils/log.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/sstable_directory.hh"
#include "sstables/sstables_registry.hh"
#include "sstables/partition_index_cache.hh"
#include "sstables/sstables.hh"
#include "object_storage_client.hh"
#include "db/config.hh"
#include "db/object_storage_endpoint_param.hh"
#include "gms/feature.hh"
#include "gms/feature_service.hh"
#include "utils/assert.hh"
#include "utils/http.hh"
#include "exceptions/exceptions.hh"

namespace sstables {

logging::logger smlogger("sstables_manager");

sstables_manager::sstables_manager(
    sstring name, db::large_data_handler& large_data_handler, db::corrupt_data_handler& corrupt_data_handler, struct config cfg, gms::feature_service& feat, cache_tracker& ct, directory_semaphore& dir_sem,
    noncopyable_function<locator::host_id()>&& resolve_host_id, sstable_compressor_factory& compressor_factory, const abort_source& abort, std::vector<file_io_extension*> file_io_extensions, scheduling_group maintenance_sg, storage_manager* shared)
    : _storage(shared)
    , _large_data_handler(large_data_handler)
    , _corrupt_data_handler(corrupt_data_handler)
    , _config(std::move(cfg))
    , _file_io_extensions(std::move(file_io_extensions))
    , _features(feat)
    , _cache_tracker(ct)
    , _sstable_metadata_concurrency_sem(
        max_count_sstable_metadata_concurrent_reads,
        max_memory_sstable_metadata_concurrent_reads(_config.available_memory),
        fmt::format("sstables_manager_{}", name),
        std::numeric_limits<size_t>::max(),
        utils::updateable_value(std::numeric_limits<uint32_t>::max()),
        utils::updateable_value(std::numeric_limits<uint32_t>::max()),
        reader_concurrency_semaphore::register_metrics::no)
    , _dir_semaphore(dir_sem)
    , _resolve_host_id(std::move(resolve_host_id))
    , _maintenance_sg(std::move(maintenance_sg))
    , _compressor_factory(compressor_factory)
    , _abort(abort)
    , _signal_gate("sstables_manager::signal")
{
    _components_reloader_status = components_reclaim_reload_fiber();
}

sstables_manager::~sstables_manager() {
    SCYLLA_ASSERT(_closing);
    SCYLLA_ASSERT(_active.empty());
    SCYLLA_ASSERT(_undergoing_close.empty());
}

void sstables_manager::subscribe(sstables_manager_event_handler& handler) {
    handler.subscribe(_signal_source.connect([this, &handler] (sstables::generation_type gen, notification_event_type event) mutable -> future<> {
        if (auto gh = _signal_gate.try_hold()) {
            switch (event) {
            case notification_event_type::deleted:
                co_await handler.deleted_sstable(gen);
            }
        }
    }));
}

using osp = db::object_storage_endpoint_param;

storage_manager::object_storage_endpoint::object_storage_endpoint(db::object_storage_endpoint_param ep)
    : cfg(ep)
{}

storage_manager::storage_manager(const db::config& cfg, config stm_cfg)
    : _object_storage_clients_memory(stm_cfg.object_storage_clients_memory)
    , _config_updater(std::make_unique<config_updater>(cfg, *this))
{
    for (auto& e : cfg.object_storage_endpoints()) {
        _object_storage_endpoints.emplace(std::make_pair(e.key(), e));
    }

    if (!stm_cfg.skip_metrics_registration) {
        namespace sm = seastar::metrics;
        metrics.add_group("object_storage", {
            sm::make_gauge("memory_usage", [this, limit = stm_cfg.object_storage_clients_memory] { return limit - _object_storage_clients_memory.available_units(); },
                    sm::description("Total number of bytes consumed by object storage client"), {}),
        });
    }
}

future<> storage_manager::stop() {
    if (_config_updater) {
        co_await _config_updater->action.join();
    }

    for (auto ep : _object_storage_endpoints) {
        if (ep.second.client != nullptr) {
            co_await ep.second.client->close();
        }
    }
}

future<> storage_manager::update_config(const db::config& cfg) {
    // Updates S3 client configurations if the endpoint is already known and
    // removes the entries that are not present in the new configuration.
    // Even though we remove obsolete S3 clients from this map, each IO
    // holds a shared_ptr to the client, so the clients will be kept alive for
    // as long as needed. 
    // This was split in two loops to guarantee the code is exception safe with
    // regards to _s3_endpoints content.
    std::unordered_set<sstring> updates;
    for (auto& e : cfg.object_storage_endpoints()) {
        auto endpoint = e.key();
        updates.insert(endpoint);

        auto [it, added] = _object_storage_endpoints.try_emplace(endpoint, e);
        if (!added) {
            if (it->second.client != nullptr) {
                co_await it->second.client->update_config(e);
            }
            it->second.cfg = e;
        }
    }

    std::erase_if(_object_storage_endpoints, [&updates](const auto& e) {
        return !updates.contains(e.first);
    });

    co_return;
}

auto storage_manager::get_endpoint(const sstring& endpoint) -> object_storage_endpoint& {
    auto found = _object_storage_endpoints.find(endpoint);
    if (found == _object_storage_endpoints.end()) {
        smlogger.error("unable to find {} in configured object-storage endpoints", endpoint);
        throw std::invalid_argument(format("endpoint {} not found", endpoint));
    }
    return found->second;
}

shared_ptr<sstables::object_storage_client> storage_manager::get_endpoint_client(sstring endpoint) {
    auto& ep = get_endpoint(endpoint);
    if (ep.client == nullptr) {
        ep.client = make_object_storage_client(ep.cfg, _object_storage_clients_memory, [&ct = container()] (std::string ep) {
            return ct.local().get_endpoint_client(ep);
        });
    }
    return ep.client;
}

sstring storage_manager::get_endpoint_type(sstring endpoint) {
    return get_endpoint(endpoint).cfg.type();
}

bool storage_manager::is_known_endpoint(sstring endpoint) const {
    return _object_storage_endpoints.contains(endpoint);
}

std::vector<sstring> storage_manager::endpoints(sstring type) const noexcept {
    return _object_storage_endpoints | std::views::filter([&type] (auto& ep) {
        return type == "" || ep.second.cfg.is_storage_of_type(type);
    }) | std::views::keys | std::ranges::to<std::vector>();
}

storage_manager::config_updater::config_updater(const db::config& cfg, storage_manager& sstm)
    : action([&sstm, &cfg] () mutable {
        return sstm.update_config(cfg);
    })
    , observer(cfg.object_storage_endpoints.observe(action.make_observer()))
{}

sstables::sstable::version_types sstables_manager::get_highest_supported_format() const noexcept {
    // FIXME: start announcing `ms` here after it becomes the default.
    // (There are several tests which expect that new sstables are written with
    // the format reported by this API).
    //
    // After `ms` becomes the default, this function look like this:
    //
    // if (_features.ms_sstable) {
    //     return sstable_version_types::ms;
    // } else {
    //     return sstable_version_types::me;
    // }
    return sstable_version_types::me;
}

sstables::sstable::version_types sstables_manager::get_preferred_sstable_version() const {
    auto preferred_format = sstables::version_from_string(_config.format());
    auto ms_supported = bool(_features.ms_sstable);
    if (ms_supported && preferred_format == sstable_version_types::ms) {
        return sstable_version_types::ms;
    } else {
        return sstable_version_types::me;
    }
}

sstables::sstable::version_types sstables_manager::get_safe_sstable_version_for_rewrites(sstable_version_types existing_version) const {
    auto preferred_format = sstables::version_from_string(_config.format());
    auto ms_supported = bool(_features.ms_sstable) || existing_version >= sstable_version_types::ms;
    if (ms_supported && preferred_format == sstable_version_types::ms) {
        return sstable_version_types::ms;
    } else {
        return sstable_version_types::me;
    }
}

locator::host_id sstables_manager::get_local_host_id() const {
    return _resolve_host_id();
}

shared_sstable sstables_manager::make_sstable(schema_ptr schema,
        const data_dictionary::storage_options& storage,
        generation_type generation,
        sstable_state state,
        sstable_version_types v,
        sstable_format_types f,
        db_clock::time_point now,
        io_error_handler_gen error_handler_gen,
        size_t buffer_size) {
    return make_lw_shared<sstable>(std::move(schema), storage, generation, state, v, f, get_large_data_handler(), get_corrupt_data_handler(), *this, now, std::move(error_handler_gen), buffer_size);
}

sstable_writer_config sstables_manager::configure_writer(sstring origin) const {
    sstable_writer_config cfg;

    cfg.promoted_index_block_size = _config.column_index_size;
    cfg.promoted_index_auto_scale_threshold = (size_t)_config.column_index_auto_scale_threshold_in_kb() * 1024;
    if (!cfg.promoted_index_auto_scale_threshold) {
        cfg.promoted_index_auto_scale_threshold = std::numeric_limits<size_t>::max();
    }
    cfg.validation_level = _config.enable_sstable_key_validation
            ? mutation_fragment_stream_validation_level::clustering_key
            : mutation_fragment_stream_validation_level::token;
    cfg.summary_byte_cost = summary_byte_cost(_config.sstable_summary_ratio);

    cfg.origin = std::move(origin);

    return cfg;
}

void sstables_manager::increment_total_reclaimable_memory(sstable* sst) {
    _total_reclaimable_memory += sst->total_reclaimable_memory_size();
    _components_memory_change_event.signal();
}

future<> sstables_manager::maybe_reclaim_components() {
    while(_total_reclaimable_memory > get_components_memory_reclaim_threshold()) {
        // Memory consumption is above threshold. Reclaim from the SSTable that
        // has the most reclaimable memory to get the total consumption under limit.
        // FIXME: Take SSTable usage into account during reclaim - see https://github.com/scylladb/scylladb/issues/21897
        auto sst_with_max_memory = std::max_element(_active.begin(), _active.end(), [](const sstable& sst1, const sstable& sst2) {
            return sst1.total_reclaimable_memory_size() < sst2.total_reclaimable_memory_size();
        });

        auto memory_reclaimed = sst_with_max_memory->reclaim_memory_from_components();
        _total_memory_reclaimed += memory_reclaimed;
        _total_reclaimable_memory -= memory_reclaimed;
        _reclaimed.insert(*sst_with_max_memory);
        // TODO: As of now only bloom filter is reclaimed. Print actual component names when adding support for more components.
        smlogger.info("Reclaimed {} bytes of memory from components of {}. Total memory reclaimed so far is {} bytes",
                memory_reclaimed, sst_with_max_memory->get_filename(), _total_memory_reclaimed);
        }
        co_await coroutine::maybe_yield();
}

size_t sstables_manager::get_components_memory_reclaim_threshold() const {
    return _config.available_memory * _config.memory_reclaim_threshold();
}

size_t sstables_manager::get_memory_available_for_reclaimable_components() const {
    return get_components_memory_reclaim_threshold() - _total_reclaimable_memory;
}

future<> sstables_manager::components_reclaim_reload_fiber() {
    auto components_memory_reclaim_threshold_observer = _config.memory_reclaim_threshold.observe([&] (double) {
        // any change to the components_memory_reclaim_threshold config should trigger reload/reclaim
        _components_memory_change_event.signal();
    });

    co_await coroutine::switch_to(_maintenance_sg);

    sstlog.trace("components_reloader_fiber start");

    while (true) {
        co_await _components_memory_change_event.when();

        if (_closing) {
            co_return;
        }

        if (_total_reclaimable_memory > get_components_memory_reclaim_threshold()) {
            // reclaim memory to bring total memory usage under threshold
            co_await maybe_reclaim_components();
        } else {
            // memory available for reloading components of previously reclaimed SSTables
            co_await maybe_reload_components();
        }
    }
}

future<> sstables_manager::maybe_reload_components() {
    // Reload bloom filters from the smallest to largest so as to maximize
    // the number of bloom filters being reloaded.
    auto memory_available = get_memory_available_for_reclaimable_components();
    while (!_reclaimed.empty() && memory_available > 0) {
        auto sstable_to_reload = _reclaimed.begin();
        const size_t reclaimed_memory = sstable_to_reload->total_memory_reclaimed();
        if (reclaimed_memory > memory_available) {
            // cannot reload anymore sstables
            break;
        }

        // Increment the total memory before reloading to prevent any parallel
        // fibers from loading new bloom filters into memory.
        _total_reclaimable_memory += reclaimed_memory;
        _reclaimed.erase(sstable_to_reload);
        // Use a lw_shared_ptr to prevent the sstable from getting deleted when
        // the components are being reloaded.
        auto sstable_ptr = sstable_to_reload->shared_from_this();
        try {
            co_await sstable_ptr->reload_reclaimed_components();
        } catch (...) {
            // reload failed due to some reason
            sstlog.warn("Failed to reload reclaimed SSTable components : {}", std::current_exception());
            // revert back changes made before the reload
            _total_reclaimable_memory -= reclaimed_memory;
            _reclaimed.insert(*sstable_to_reload);
            break;
        }

        _total_memory_reclaimed -= reclaimed_memory;
        memory_available = get_memory_available_for_reclaimable_components();
    }
}

void sstables_manager::reclaim_memory_and_stop_tracking_sstable(sstable* sst) {
    // remove the sstable from the memory tracking metrics
    _total_reclaimable_memory -= sst->total_reclaimable_memory_size();
    _total_memory_reclaimed -= sst->total_memory_reclaimed();
    // reclaim any remaining memory from the sstable
    sst->reclaim_memory_from_components();
    // disable further reload of components
    _reclaimed.erase(*sst);
    sst->disable_component_memory_reload();
}

void sstables_manager::add(sstable* sst) {
    _active.push_back(*sst);
}

void sstables_manager::deactivate(sstable* sst) {
    // Drop reclaimable components if they are still in memory
    // and remove SSTable from the reclaimable memory tracking
    reclaim_memory_and_stop_tracking_sstable(sst);

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
    _components_memory_change_event.signal();
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
    // in the same storage so it's OK to get the deleter from _signal_mana
    // front element. The deleter implementation is welcome to check
    // that sstables from the vector really live in it.
    auto& storage = ssts.front()->get_storage();
    auto ctx = co_await storage.atomic_delete_prepare(ssts);

    co_await coroutine::parallel_for_each(ssts, [] (shared_sstable sst) {
        return sst->unlink(sstables::storage::sync_dir::no);
    });

    co_await storage.atomic_delete_complete(std::move(ctx));
}

future<std::unordered_set<sstring>> sstables_manager::take_snapshot(std::vector<shared_sstable> ssts, sstring name) {
    std::unordered_set<sstring> table_names;

    co_await _dir_semaphore.parallel_for_each(ssts, [&name, &table_names] (sstables::shared_sstable sstable) {
        table_names.insert(sstable->component_basename(sstables::component_type::Data));
        return io_check([sstable, &name] {
            return sstable->snapshot(name);
        });
    });

    co_return table_names;
}

future<> sstables_manager::close() {
    _closing = true;
    maybe_done();
    co_await _done.get_future();
    co_await _sstable_metadata_concurrency_sem.stop();
    // stop the components reload fiber
    _components_memory_change_event.signal();
    co_await std::move(_components_reloader_status);
    co_await _signal_gate.close();
}

void sstables_manager::plug_sstables_registry(std::unique_ptr<sstables::sstables_registry> sr) noexcept {
    _sstables_registry = std::move(sr);
}

void sstables_manager::unplug_sstables_registry() noexcept {
    _sstables_registry.reset();
}

future<lw_shared_ptr<const data_dictionary::storage_options>> sstables_manager::init_table_storage(const schema& s, const data_dictionary::storage_options& so) {
    return sstables::init_table_storage(*this, s, so);
}

future<> sstables_manager::init_keyspace_storage(const data_dictionary::storage_options& so, sstring dir) {
    return sstables::init_keyspace_storage(*this, so, dir);
}

future<> sstables_manager::destroy_table_storage(const data_dictionary::storage_options& so) {
    return sstables::destroy_table_storage(so);
}

void sstables_manager::validate_new_keyspace_storage_options(const data_dictionary::storage_options& so) {
    std::visit(overloaded_functor {
        [] (const data_dictionary::storage_options::local&) {
        },
        [this] (const data_dictionary::storage_options::object_storage& so) {
            if (!_features.keyspace_storage_options) {
                throw exceptions::invalid_request_exception("Keyspace storage options not supported in the cluster");
            }
            // It's non-system keyspace
            if (!is_known_endpoint(so.endpoint)) {
                throw exceptions::configuration_exception(format("Endpoint {} not configured", so.endpoint));
            }
        }
    }, so.value);
}

std::vector<std::filesystem::path> sstables_manager::get_local_directories(const data_dictionary::storage_options::local& so) const {
    return sstables::get_local_directories(_config.data_file_directories, so);
}

void sstables_manager::on_unlink(sstable* sst) {
    reclaim_memory_and_stop_tracking_sstable(sst);
    _signal_source(sst->generation(), notification_event_type::deleted);
}

sstables_registry::~sstables_registry() = default;

}   // namespace sstables
