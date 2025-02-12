/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <type_traits>
#include <fmt/ranges.h>
#include <fmt/std.h>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/util/file.hh>
#include <boost/algorithm/string.hpp>
#include "sstables/sstable_directory.hh"
#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"
#include "compaction/compaction_manager.hh"
#include "utils/log.hh"
#include "sstable_directory.hh"
#include "utils/assert.hh"
#include "utils/lister.hh"
#include "utils/overloaded_functor.hh"
#include "utils/directories.hh"
#include "utils/s3/client.hh"
#include "replica/database.hh"
#include "dht/auto_refreshing_sharder.hh"

static logging::logger dirlog("sstable_directory");

namespace sstables {

bool manifest_json_filter(const fs::path&, const directory_entry& entry) {
    // Filter out directories. If type of the entry is unknown - check its name.
    if (entry.type.value_or(directory_entry_type::regular) != directory_entry_type::directory && (entry.name == "manifest.json" || entry.name == "schema.cql")) {
        return false;
    }

    return true;
}

sstable_directory::filesystem_components_lister::filesystem_components_lister(std::filesystem::path dir)
        : _directory(dir)
        , _state(std::make_unique<scan_state>())
{
}

sstable_directory::filesystem_components_lister::filesystem_components_lister(std::filesystem::path dir, sstables_manager& mgr, const data_dictionary::storage_options::s3& os)
        : _directory(dir)
        , _state(std::make_unique<scan_state>())
        , _client(mgr.get_endpoint_client(os.endpoint))
        , _bucket(os.bucket)
{
}

sstable_directory::sstables_registry_components_lister::sstables_registry_components_lister(sstables::sstables_registry& sstables_registry, table_id owner)
        : _sstables_registry(sstables_registry)
        , _owner(std::move(owner))
{
}

sstable_directory::restore_components_lister::restore_components_lister(const data_dictionary::storage_options::value_type& options,
                                                                        std::vector<sstring> toc_filenames)
        : _toc_filenames(std::move(toc_filenames))
{
}

std::unique_ptr<sstable_directory::components_lister>
sstable_directory::make_components_lister() {
    return std::visit(overloaded_functor {
        [this] (const data_dictionary::storage_options::local& loc) mutable -> std::unique_ptr<sstable_directory::components_lister> {
            if (loc.dir.empty()) {
                on_internal_error(dirlog, "Local storage options is missing 'dir'");
            }
            return std::make_unique<sstable_directory::filesystem_components_lister>(make_path(loc.dir.native(), _state));
        },
        [this] (const data_dictionary::storage_options::s3& os) mutable -> std::unique_ptr<sstable_directory::components_lister> {
            return std::visit(overloaded_functor {
                [this, &os] (const sstring& prefix) -> std::unique_ptr<sstable_directory::components_lister> {
                    if (prefix.empty()) {
                        on_internal_error(sstlog, "S3 storage options is missing 'prefix'");
                    }
                    return std::make_unique<sstable_directory::filesystem_components_lister>(fs::path(prefix), _manager, os);
                },
                [this] (const table_id& owner) -> std::unique_ptr<sstable_directory::components_lister> {
                    if (owner.id.is_null()) {
                        on_internal_error(sstlog, "S3 storage options is missing 'owner'");
                    }
                    return std::make_unique<sstable_directory::sstables_registry_components_lister>(_manager.sstables_registry(), owner);
                }
            }, os.location);
        }
    }, _storage_opts->value);
}

sstable_directory::sstable_directory(replica::table& table,
        sstable_state state,
        io_error_handler_gen error_handler_gen)
    : sstable_directory(
        table.get_sstables_manager(),
        table.schema(),
        std::make_unique<dht::auto_refreshing_sharder>(table.shared_from_this()),
        table.get_storage_options_ptr(),
        std::move(state),
        std::move(error_handler_gen)
    )
{}

sstable_directory::sstable_directory(replica::table& table,
        sstable_state state,
        lw_shared_ptr<const data_dictionary::storage_options> storage_opts,
        io_error_handler_gen error_handler_gen)
    : sstable_directory(
        table.get_sstables_manager(),
        table.schema(),
        std::make_unique<dht::auto_refreshing_sharder>(table.shared_from_this()),
        std::move(storage_opts),
        state,
        std::move(error_handler_gen)
    )
{}

sstable_directory::sstable_directory(replica::table& table,
        lw_shared_ptr<const data_dictionary::storage_options> storage_opts,
        std::vector<sstring> sstables,
        io_error_handler_gen error_handler_gen)
    : _manager(table.get_sstables_manager())
    , _schema(table.schema())
    , _storage_opts(std::move(storage_opts))
    , _state(sstable_state::upload)
    , _error_handler_gen(error_handler_gen)
    , _storage(make_storage(_manager, *_storage_opts, _state))
    , _lister(std::make_unique<sstable_directory::restore_components_lister>(_storage_opts->value,
                                                                             std::move(sstables)))
    , _sharder_ptr(std::make_unique<dht::auto_refreshing_sharder>(table.shared_from_this()))
    , _sharder(*_sharder_ptr)
    , _unshared_remote_sstables(smp::count)
{}

sstable_directory::sstable_directory(sstables_manager& manager,
        schema_ptr schema,
        const dht::sharder& sharder,
        sstring table_dir,
        sstable_state state,
        io_error_handler_gen error_handler_gen)
    : sstable_directory(
        manager,
        std::move(schema),
        &sharder,
        make_lw_shared<const data_dictionary::storage_options>(data_dictionary::make_local_options(fs::path(table_dir))),
        state,
        std::move(error_handler_gen)
    )
{}

using unique_sharder_ptr = std::unique_ptr<dht::sharder>;

sstable_directory::sstable_directory(sstables_manager& manager,
        schema_ptr schema,
        std::variant<unique_sharder_ptr, const dht::sharder*> sharder,
        lw_shared_ptr<const data_dictionary::storage_options> storage_opts,
        sstable_state state,
        io_error_handler_gen error_handler_gen)
    : _manager(manager)
    , _schema(std::move(schema))
    , _storage_opts(std::move(storage_opts))
    , _state(state)
    , _error_handler_gen(error_handler_gen)
    , _storage(make_storage(_manager, *_storage_opts, _state))
    , _lister(make_components_lister())
    , _sharder_ptr(std::holds_alternative<unique_sharder_ptr>(sharder) ? std::move(std::get<unique_sharder_ptr>(sharder)) : nullptr)
    , _sharder(_sharder_ptr ? *_sharder_ptr : *std::get<const dht::sharder*>(sharder))
    , _unshared_remote_sstables(smp::count)
{}

void sstable_directory::filesystem_components_lister::handle(sstables::entry_descriptor desc, fs::path filename) {
    // TODO: decorate sstable_directory with some noncopyable_function<shard_id (generation_type)>
    //       to communicate how different tables place sstables into shards.
    if (!sstables::sstable_generation_generator::maybe_owned_by_this_shard(desc.generation)) {
        return;
    }

    dirlog.trace("for SSTable directory, scanning {}", filename);
    _state->generations_found.emplace(desc.generation, filename);

    switch (desc.component) {
    case component_type::TemporaryStatistics:
        // We generate TemporaryStatistics when we rewrite the Statistics file,
        // for instance on mutate_level. We should delete it - so we mark it for deletion
        // here, but just the component. The old statistics file should still be there
        // and we'll go with it.
        _state->files_for_removal.insert(filename.native());
        break;
    case component_type::TOC:
        _state->descriptors.emplace(desc.generation, std::move(desc));
        break;
    case component_type::TemporaryTOC:
        _state->temp_toc_found.push_back(std::move(desc));
        break;
    default:
        // Do nothing, and will validate when trying to load the file.
        break;
    }
}

void sstable_directory::validate(sstables::shared_sstable sst, process_flags flags) const {
    schema_ptr s = sst->get_schema();
    if (s->is_counter() && !sst->has_scylla_component()) {
        sstring error = "Direct loading non-Scylla SSTables containing counters is not supported.";
        if (flags.enable_dangerous_direct_import_of_cassandra_counters) {
            dirlog.info("{} But trying to continue on user's request.", error);
        } else {
            dirlog.error("{} Use sstableloader instead.", error);
            throw std::runtime_error(fmt::format("{} Use sstableloader instead.", error));
        }
    }
    if (s->is_view() && !flags.allow_loading_materialized_view) {
        throw std::runtime_error("Loading Materialized View SSTables is not supported. Re-create the view instead.");
    }
    if (!sst->is_uploaded()) {
        sst->validate_originating_host_id();
    }
}

future<sstables::shared_sstable> sstable_directory::load_sstable(sstables::entry_descriptor desc,
        const data_dictionary::storage_options& storage_opts, sstables::sstable_open_config cfg) const {
    shared_sstable sst = _manager.make_sstable(_schema, storage_opts, desc.generation, _state, desc.version, desc.format, gc_clock::now(), _error_handler_gen);
    co_await sst->load(_sharder, cfg);
    co_return sst;
}

future<>
sstable_directory::process_descriptor(sstables::entry_descriptor desc,
        process_flags flags,
        noncopyable_function<data_dictionary::storage_options()>&& get_storage_options) {
    if (desc.version > _max_version_seen) {
        _max_version_seen = desc.version;
    }

    auto storage_opts = get_storage_options();
    auto shards = co_await get_shards_for_this_sstable(desc, storage_opts, flags);
    if (flags.sort_sstables_according_to_owner && shards.size() == 1 && shards[0] != this_shard_id()) {
        // identified a remote unshared sstable
        _unshared_remote_sstables[shards[0]].push_back(std::move(desc));
        co_return;
    }

    auto sst = co_await load_sstable(desc, storage_opts, flags.sstable_open_config);
    validate(sst, flags);

    if (flags.need_mutate_level) {
        dirlog.trace("Mutating {} to level 0\n", sst->get_filename());
        co_await sst->mutate_sstable_level(0);
    }

    if (flags.sort_sstables_according_to_owner) {
        if (shards.size() == 1) {
            dirlog.trace("{} identified as a local unshared SSTable", sst->get_filename());
            _unshared_local_sstables.push_back(std::move(sst));
        } else {
            dirlog.trace("{} identified as a shared SSTable, shards={}", sst->get_filename(), shards);
            _shared_sstable_info.push_back(co_await sst->get_open_info());
        }
    } else {
        dirlog.debug("Added {} to unsorted sstables list", sst->get_filename());
        _unsorted_sstables.push_back(std::move(sst));
    }
}

generation_type
sstable_directory::highest_generation_seen() const {
    return _max_generation_seen;
}

sstables::sstable_version_types
sstable_directory::highest_version_seen() const {
    return _max_version_seen;
}

future<> sstable_directory::prepare(process_flags flags) {
    return _lister->prepare(*this, flags, *_storage);
}

future<> sstable_directory::filesystem_components_lister::prepare(sstable_directory& dir, process_flags flags, storage& st) {
    if (_client) {
        co_return;
    }

    if (dir._state == sstable_state::quarantine) {
        if (!co_await file_exists(_directory.native())) {
            co_return;
        }
    }

    // verify owner and mode on the sstables directory
    // and all its subdirectories, except for "snapshots"
    // as there could be a race with scylla-manager that might
    // delete snapshots concurrently
    co_await utils::directories::verify_owner_and_mode(_directory, utils::directories::recursive::no);

    directory_lister lister(_directory, lister::dir_entry_types::of<directory_entry_type::directory>());
    co_await with_closeable(std::move(lister), [this] (directory_lister& lister) -> future<> {
        while (auto de = co_await lister.get()) {
            if (de->name != sstables::snapshots_dir) {
                co_await utils::directories::verify_owner_and_mode(_directory / de->name, utils::directories::recursive::yes);
            }
        }
    });

    if (flags.garbage_collect) {
        co_await garbage_collect(st);
    }
}

future<> sstable_directory::sstables_registry_components_lister::prepare(sstable_directory& dir, process_flags flags, storage& st) {
    if (flags.garbage_collect) {
        co_await garbage_collect(st);
    }
}

future<> sstable_directory::restore_components_lister::prepare(sstable_directory& dir, process_flags flags, storage& st) {
    return make_ready_future();
}

future<> sstable_directory::process_sstable_dir(process_flags flags) {
    return _lister->process(*this, flags);
}

future<> sstable_directory::filesystem_components_lister::process(sstable_directory& directory, process_flags flags) {
    if (directory._state == sstable_state::quarantine) {
        if (!co_await file_exists(_directory.native())) {
            co_return;
        }
    }

    dirlog.debug("Start processing directory {} for SSTables", _directory);
    // It seems wasteful that each shard is repeating this scan, and to some extent it is.
    // However, we still want to open the files and especially call process_dir() in a distributed
    // fashion not to overload any shard. Also in the common case the SSTables will all be
    // unshared and be on the right shard based on their generation number. In light of that there are
    // two advantages of having each shard repeat the directory listing:
    //
    // - The directory listing part already interacts with data_structures inside scan_state. We
    //   would have to either transfer a lot of file information among shards or complicate the code
    //   to make sure they all update their own version of scan_state and then merge it.
    // - If all shards scan in parallel, they can start loading sooner. That is faster than having
    //   a separate step to fetch all files, followed by another step to distribute and process.
    //
    // For bucket lister, slashes are not considered as path components separator, so in order for
    // the lister to list only basename components, append trailing / to prefix name

    auto lister = !_client ?
            abstract_lister::make<directory_lister>(_directory, lister::dir_entry_types::of<directory_entry_type::regular>(), &manifest_json_filter) :
            abstract_lister::make<s3::client::bucket_lister>(_client, _bucket, _directory.native() + "/", &manifest_json_filter);

    co_await with_closeable(std::move(lister), coroutine::lambda([this, &directory] (abstract_lister& lister) -> future<> {
        while (auto de = co_await lister.get()) {
            auto component_path = _directory / de->name;
            auto comps = sstables::parse_path(component_path, directory._schema->ks_name(), directory._schema->cf_name());
            handle(std::move(comps), component_path);
        }
    }));

    // Always okay to delete files with a temporary TOC. We want to do it before we process
    // the generations seen: it's okay to reuse those generations since the files will have
    // been deleted anyway.
    for (auto& desc: _state->temp_toc_found) {
        auto range = _state->generations_found.equal_range(desc.generation);
        for (auto it = range.first; it != range.second; ++it) {
            auto& path = it->second;
            dirlog.trace("Scheduling to remove file {}, from an SSTable with a Temporary TOC", path.native());
            _state->files_for_removal.insert(path.native());
        }
        _state->generations_found.erase(range.first, range.second);
        _state->descriptors.erase(desc.generation);
    }

    auto msg = format("After {} scanned, {} descriptors found, {} different files found",
            _directory, _state->descriptors.size(), _state->generations_found.size());

    if (!_state->generations_found.empty()) {
        directory._max_generation_seen = std::ranges::fold_left(_state->generations_found | std::views::keys, sstables::generation_type{}, [] (generation_type a, generation_type b) {
            return std::max<generation_type>(a, b);
        });

        msg = format("{}, highest generation seen: {}", msg, directory._max_generation_seen);
    } else {
        msg = format("{}, no numeric generation was seen", msg);
    }

    dirlog.debug("{}", msg);

    // _descriptors is everything with a TOC. So after we remove this, what's left is
    // SSTables for which a TOC was not found.
    auto descriptors = std::move(_state->descriptors);
    co_await directory._manager.dir_semaphore().parallel_for_each(descriptors, [this, flags, &directory] (std::pair<const generation_type, sstables::entry_descriptor>& t) {
        auto& desc = std::get<1>(t);
        _state->generations_found.erase(desc.generation);
        // This will try to pre-load this file and throw an exception if it is invalid
        return directory.process_descriptor(std::move(desc), flags,
                                            [&directory] { return *directory._storage_opts; });
    });

    // For files missing TOC, it depends on where this is coming from.
    // If scylla was supposed to have generated this SSTable, this is not okay and
    // we refuse to proceed. If this coming from, say, an import, then we just delete,
    // log and proceed.
    for (auto& path : _state->generations_found | std::views::values) {
        if (flags.throw_on_missing_toc) {
            throw sstables::malformed_sstable_exception(seastar::format("At directory: {}: no TOC found for SSTable {}!. Refusing to boot", _directory.native(), path.native()));
        } else {
            dirlog.info("Found incomplete SSTable {} at directory {}. Removing", path.native(), _directory.native());
            _state->files_for_removal.insert(path.native());
        }
    }
}

future<> sstable_directory::sstables_registry_components_lister::process(sstable_directory& directory, process_flags flags) {
    dirlog.debug("Start processing registry entry {} (state {})", _owner, directory._state);
    return _sstables_registry.sstables_registry_list(_owner, [this, flags, &directory] (sstring status, sstable_state state, entry_descriptor desc) {
        if (state != directory._state) {
            return make_ready_future<>();
        }
        if (status != "sealed") {
            dirlog.warn("Skip processing {} {} entry from {} (must have been picked up by garbage collector)", status, desc.generation, _owner);
            return make_ready_future<>();
        }
        if (!sstable_generation_generator::maybe_owned_by_this_shard(desc.generation)) {
            return make_ready_future<>();
        }

        dirlog.debug("Processing {} entry from {}", desc.generation, _owner);
        return directory.process_descriptor(std::move(desc), flags,
                                            [&directory] { return *directory._storage_opts; });
    });
}

future<> sstable_directory::restore_components_lister::process(sstable_directory& directory, process_flags flags) {
    co_await coroutine::parallel_for_each(_toc_filenames, [flags, &directory] (sstring toc_filename) -> future<> {
        std::filesystem::path sst_path{toc_filename};
        entry_descriptor desc = sstables::parse_path(sst_path, "", "");
        if (!sstable_generation_generator::maybe_owned_by_this_shard(desc.generation)) {
            co_return;
        }
        dirlog.debug("Processing {} entry from {}", desc.generation, toc_filename);
        co_await directory.process_descriptor(
            std::move(desc), flags,
            [&directory, prefix=sst_path.parent_path().native()] {
                return directory._storage_opts->append_to_s3_prefix(prefix);
            });
    });
}

future<> sstable_directory::commit_directory_changes() {
    return _lister->commit().finally([x = std::move(_lister)] {});
}

future<> sstable_directory::filesystem_components_lister::commit() {
    // Remove all files scheduled for removal
    return parallel_for_each(std::exchange(_state->files_for_removal, {}), [] (sstring path) {
        dirlog.info("Removing file {}", path);
        return remove_file(std::move(path));
    });
}

future<> sstable_directory::sstables_registry_components_lister::commit() {
    return make_ready_future<>();
}

future<> sstable_directory::restore_components_lister::commit() {
    return make_ready_future<>();
}

future<> sstable_directory::sstables_registry_components_lister::garbage_collect(storage& st) {
    std::set<generation_type> gens_to_remove;
    co_await _sstables_registry.sstables_registry_list(_owner, coroutine::lambda([&st, &gens_to_remove] (sstring status, sstable_state state, entry_descriptor desc) -> future<> {
        if (status == "sealed") {
            co_return;
        }

        dirlog.info("Removing dangling {} {} entry", desc.generation, status);
        gens_to_remove.insert(desc.generation);
        co_await st.remove_by_registry_entry(std::move(desc));
    }));
    co_await coroutine::parallel_for_each(gens_to_remove, [this] (auto gen) -> future<> {
        co_await _sstables_registry.delete_entry(_owner, gen);
    });
}

future<>
sstable_directory::move_foreign_sstables(sharded<sstable_directory>& source_directory) {
    return parallel_for_each(std::views::iota(0u, smp::count), [this, &source_directory] (unsigned shard_id) mutable {
        auto info_vec = std::exchange(_unshared_remote_sstables[shard_id], {});
        if (info_vec.empty()) {
            return make_ready_future<>();
        }
        // Should be empty, since an SSTable that belongs to this shard is not remote.
        SCYLLA_ASSERT(shard_id != this_shard_id());
        dirlog.debug("Moving {} unshared SSTables to shard {} ", info_vec.size(), shard_id);
        return source_directory.invoke_on(shard_id, &sstables::sstable_directory::load_foreign_sstables, std::move(info_vec));
    });
}

future<shared_sstable> sstable_directory::load_foreign_sstable(foreign_sstable_open_info& info) {
    auto sst = _manager.make_sstable(_schema, *_storage_opts, info.generation, _state, info.version, info.format, gc_clock::now(), _error_handler_gen);
    co_await sst->load(std::move(info));
    co_return sst;
}

future<>
sstable_directory::load_foreign_sstables(sstable_entry_descriptor_vector info_vec) {
    co_await _manager.dir_semaphore().parallel_for_each(info_vec, [this] (const sstables::entry_descriptor& info) {
        return load_sstable(info, *_storage_opts).then([this] (auto sst) {
            _unshared_local_sstables.push_back(sst);
            return make_ready_future<>();
        });
    });
}

future<std::vector<shard_id>> sstable_directory::get_shards_for_this_sstable(
        const sstables::entry_descriptor& desc, const data_dictionary::storage_options& storage_opts, process_flags flags) const {
    auto sst = _manager.make_sstable(_schema, storage_opts, desc.generation, _state, desc.version, desc.format, gc_clock::now(), _error_handler_gen);
    co_await sst->load_owner_shards(_sharder);
    validate(sst, flags);
    co_return sst->get_shards_for_this_sstable();
}

future<>
sstable_directory::remove_sstables(std::vector<sstables::shared_sstable> sstlist) {
    dirlog.debug("Removing {} SSTables", sstlist.size());
    return parallel_for_each(std::move(sstlist), [] (const sstables::shared_sstable& sst) {
        dirlog.trace("Removing SSTable {}", sst->get_filename());
        return sst->unlink().then([sst] {});
    });
}

future<>
sstable_directory::collect_output_unshared_sstables(std::vector<sstables::shared_sstable> resharded_sstables, can_be_remote remote_ok) {
    dirlog.debug("Collecting {} output SSTables (remote={})", resharded_sstables.size(), remote_ok);
    return parallel_for_each(std::move(resharded_sstables), [this, remote_ok] (sstables::shared_sstable sst) {
        auto shards = sst->get_shards_for_this_sstable();
        SCYLLA_ASSERT(shards.size() == 1);
        auto shard = shards[0];

        if (shard == this_shard_id()) {
            dirlog.trace("Collected output SSTable {} already local", sst->get_filename());
            _unshared_local_sstables.push_back(std::move(sst));
            return make_ready_future<>();
        }

        if (!remote_ok) {
            return make_exception_future<>(std::runtime_error("Unexpected remote sstable"));
        }

        dirlog.trace("Collected output SSTable {} is remote. Storing it", sst->get_filename());
        _unshared_remote_sstables[shard].push_back(sst->get_descriptor(component_type::Data));
        return make_ready_future<>();
    });
}

future<>
sstable_directory::remove_unshared_sstables(std::vector<sstables::shared_sstable> sstlist) {
    // When removing input sstables from reshaping: Those SSTables used to be in the unshared local
    // list. So not only do we have to remove them, we also have to update the list. Because we're
    // dealing with a vector it's easier to just reconstruct the list.
    dirlog.debug("Removing {} unshared SSTables", sstlist.size());
    std::unordered_set<sstables::shared_sstable> exclude;

    for (auto& sst : sstlist) {
        exclude.insert(sst);
    }

    auto old = std::exchange(_unshared_local_sstables, {});

    for (auto& sst : old) {
        if (!exclude.contains(sst)) {
            _unshared_local_sstables.push_back(sst);
        }
    }

    // Do this last for exception safety. If there is an exception on unlink we
    // want to at least leave the SSTable unshared list in a sane state.
    co_await remove_sstables(std::move(sstlist));
    dirlog.debug("Finished removing all SSTables");
}


future<>
sstable_directory::do_for_each_sstable(std::function<future<>(sstables::shared_sstable)> func) {
    auto sstables = std::move(_unshared_local_sstables);
    co_await _manager.dir_semaphore().parallel_for_each(sstables, std::move(func));
}

future<>
sstable_directory::filter_sstables(std::function<future<bool>(sstables::shared_sstable)> func) {
    std::vector<sstables::shared_sstable> filtered;
    co_await _manager.dir_semaphore().parallel_for_each(_unshared_local_sstables, [func = std::move(func), &filtered] (sstables::shared_sstable sst) -> future<> {
        auto keep = co_await func(sst);
        if (keep) {
            filtered.emplace_back(sst);
        }
    });
    _unshared_local_sstables = std::move(filtered);
}

void
sstable_directory::store_phaser(utils::phased_barrier::operation op) {
    _operation_barrier.emplace(std::move(op));
}

sstable_directory::sstable_open_info_vector
sstable_directory::retrieve_shared_sstables() {
    return std::exchange(_shared_sstable_info, {});
}

bool sstable_directory::compare_sstable_storage_prefix(const sstring& prefix_a, const sstring& prefix_b) noexcept {
    size_t size_a = prefix_a.size();
    if (prefix_a.back() == '/') {
        size_a--;
    }
    size_t size_b = prefix_b.size();
    if (prefix_b.back() == '/') {
        size_b--;
    }
    return size_a == size_b && sstring::traits_type::compare(prefix_a.begin(), prefix_b.begin(), size_a) == 0;
}

future<sstring> sstable_directory::create_pending_deletion_log(opened_directory& base_dir, const std::vector<shared_sstable>& ssts) {
    return seastar::async([&] {
        min_max_tracker<generation_type> gen_tracker;
        sstring pending_delete_log;

        for (const auto& sst : ssts) {
            gen_tracker.update(sst->generation());
        }

        sstring pending_delete_dir = (base_dir.path() / sstables::pending_delete_dir).native();
        pending_delete_log = format("{}/sstables-{}-{}.log", pending_delete_dir, gen_tracker.min(), gen_tracker.max());
        sstring tmp_pending_delete_log = pending_delete_log + ".tmp";
        dirlog.trace("Writing {}", tmp_pending_delete_log);

            touch_directory(pending_delete_dir).get();
            auto oflags = open_flags::wo | open_flags::create | open_flags::exclusive;
            // Create temporary pending_delete log file.
            auto f = open_file_dma(tmp_pending_delete_log, oflags).get();
            // Write all toc names into the log file.
            auto out = make_file_output_stream(std::move(f), 4096).get();
            auto close_out = deferred_close(out);

        try {
            auto trim_size = base_dir.native().size() + 1; // Account for the '/' delimiter
            for (const auto& sst : ssts) {
                auto toc = sst->toc_filename();
                if (toc.size() <= trim_size) {
                    on_internal_error(dirlog, fmt::format("Sstable {} outside of basedir {} is scheduled for deletion", toc, base_dir.native()));
                }
                out.write(toc.begin() + trim_size, toc.size() - trim_size).get();
                out.write("\n").get();
                dirlog.trace("Wrote '{}' to {}", sstring(toc.begin() + trim_size, toc.size() - trim_size), tmp_pending_delete_log);
            }

            out.flush().get();
            close_out.close_now();
        } catch (...) {
            dirlog.warn("Error while writing {}: {}. Ignoring.", tmp_pending_delete_log, std::current_exception());
        }

            // Once flushed and closed, the temporary log file can be renamed.
            io_check(rename_file, tmp_pending_delete_log, pending_delete_log).get();

            // Guarantee that the changes above reached the disk.
            base_dir.sync(general_disk_error_handler).get();
            dirlog.debug("{} written successfully.", pending_delete_log);

        return pending_delete_log;
    });
}

// FIXME: Go through maybe_delete_large_partitions_entry on recovery since
// this is an indication we crashed in the middle of atomic deletion
future<> sstable_directory::filesystem_components_lister::replay_pending_delete_log(fs::path pending_delete_log) {
    dirlog.debug("Reading pending_deletes log file {}", pending_delete_log);
    fs::path pending_delete_dir = pending_delete_log.parent_path();
    try {
        sstring sstdir = pending_delete_dir.parent_path().native();
        auto text = co_await seastar::util::read_entire_file_contiguous(pending_delete_log);

        sstring all(text.begin(), text.end());
        std::vector<sstring> basenames;
        boost::split(basenames, all, boost::is_any_of("\n"), boost::token_compress_on);
        auto tocs = std::ranges::to<std::vector<sstring>>(basenames | std::views::filter([] (auto&& basename) { return !basename.empty(); }));
        dirlog.debug("TOCs to remove: {}", tocs);
        co_await parallel_for_each(tocs, [&sstdir] (const sstring& name) {
            // Only move TOC to TOC.tmp, the rest will be finished by regular process
            return make_toc_temporary(sstdir + "/" + name).discard_result();
        });
        dirlog.debug("Replayed {}, removing", pending_delete_log);
        co_await remove_file(pending_delete_log.native());
    } catch (...) {
        dirlog.warn("Error replaying {}: {}. Ignoring.", pending_delete_log, std::current_exception());
    }
}

future<> sstable_directory::filesystem_components_lister::garbage_collect(storage& st) {
    // First pass, cleanup temporary sstable directories and sstables pending delete.
    co_await cleanup_column_family_temp_sst_dirs();
    co_await handle_sstables_pending_delete();
}

future<> sstable_directory::filesystem_components_lister::cleanup_column_family_temp_sst_dirs() {
    directory_lister lister(_directory, lister::dir_entry_types::of<directory_entry_type::directory>());
    auto futures = co_await with_closeable(std::move(lister), [this] (directory_lister& lister) -> future<std::vector<future<>>> {
        std::vector<future<>> futures;
        while (auto de = co_await lister.get()) {
            // push futures that remove files/directories into an array of futures,
            // so that the supplied callback will not block lister from
            // reading the next entry in the directory.
            fs::path dirpath = _directory / de->name;
            if (dirpath.extension().string() == tempdir_extension) {
                dirlog.info("Found temporary sstable directory: {}, removing", dirpath);
                futures.push_back(io_check([dirpath = std::move(dirpath)] () { return lister::rmdir(dirpath); }));
            }
        }
        co_return futures;
    });

    co_await when_all_succeed(futures.begin(), futures.end()).discard_result();
}

future<> sstable_directory::filesystem_components_lister::handle_sstables_pending_delete() {
    auto pending_delete_dir = _directory / sstables::pending_delete_dir;
    auto exists = co_await file_exists(pending_delete_dir.native());
    if (!exists) {
        co_return;
    }

    directory_lister lister(pending_delete_dir, lister::dir_entry_types::of<directory_entry_type::regular>());
    auto futures = co_await with_closeable(std::move(lister), coroutine::lambda([this, &pending_delete_dir] (directory_lister& lister) -> future<std::vector<future<>>> {
        std::vector<future<>> futures;
        while (auto de = co_await lister.get()) {
            // push nested futures that remove files/directories into an array of futures,
            // so that the supplied callback will not block lister from
            // reading the next entry in the directory.
            fs::path file_path = pending_delete_dir / de->name;
            if (file_path.extension() == ".tmp") {
                dirlog.info("Found temporary pending_delete log file: {}, deleting", file_path);
                futures.push_back(remove_file(file_path.string()));
            } else if (file_path.extension() == ".log") {
                dirlog.info("Found pending_delete log file: {}, replaying", file_path);
                auto f = replay_pending_delete_log(std::move(file_path));
                futures.push_back(std::move(f));
            } else {
                dirlog.debug("Found unknown file in pending_delete directory: {}, ignoring", file_path);
            }
        }
        co_return futures;
    }));

    co_await when_all_succeed(futures.begin(), futures.end()).discard_result();
}

future<sstables::generation_type>
highest_generation_seen(sharded<sstables::sstable_directory>& directory) {
    co_return co_await directory.map_reduce0(
        std::mem_fn(&sstables::sstable_directory::highest_generation_seen),
        sstables::generation_type{},
        [] (sstables::generation_type a, sstables::generation_type b) {
            return std::max(a, b);
        });
}

}
