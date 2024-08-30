#include "dictionary_service.hh"
#include "service/raft/raft_group0.hh"
#include "gms/feature_service.hh"
#include "service/raft/raft_group0_client.hh"
#include <seastar/core/coroutine.hh>
#include "db/system_keyspace.hh"

dictionary_service::dictionary_service(
    utils::dict_sampler& ds,
    db::system_keyspace& sys_ks,
    utils::alien_worker& alien_worker,
    service::raft_group0_client& raft_group0_client,
    service::raft_group0& raft_group0,
    abort_source& as,
    gms::feature_service& fs,
    config<> cfg
)
    : _sys_ks(sys_ks)
    , _our_host_id(cfg.our_host_id)
    , _rpc_dict_training_when(std::move(cfg.rpc_dict_training_when))
    , _raft_group0_client(raft_group0_client)
    , _as(as)
    , _training_fiber_future(make_ready_future<>())
    , _leadership_observer(raft_group0.observe_leadership([this] (bool leader) {
        utils::dict_trainer_logger.debug("dictionary_service: _leadership_observer triggered");
        _is_leader = leader;
        maybe_toggle_dict_training();
    }))
    , _when_observer(_rpc_dict_training_when.observe([this] (const auto&) {
        utils::dict_trainer_logger.debug("dictionary_service: _when_observer triggered");
        maybe_toggle_dict_training();
    }))
    , _feature_observer(fs.compression_dicts.when_enabled([
        rpc_dict_training_min_time_seconds = std::move(cfg.rpc_dict_training_min_time_seconds),
        rpc_dict_training_min_bytes = std::move(cfg.rpc_dict_training_min_bytes),
        &alien_worker,
        &ds,
        this
    ] {
        utils::dict_trainer_logger.debug("dictionary_service: _feature_observer triggered");
        _training_fiber_future = _training_fiber.start(
            ds,
            [this] (utils::dict_sampler::dict_type d) { return publish_dict(std::move(d)); },
            std::move(rpc_dict_training_min_time_seconds),
            std::move(rpc_dict_training_min_bytes),
            alien_worker
        );
    }))
{
    maybe_toggle_dict_training();
}


void dictionary_service::maybe_toggle_dict_training() {
    auto when = _rpc_dict_training_when();
    utils::dict_trainer_logger.debug("dictionary_service::maybe_toggle_dict_training(), called, _is_leader={}, when={}", _is_leader, when);
    if (when == utils::dict_training_loop::when::type::NEVER) {
        _training_fiber.pause();
    } else if (when == utils::dict_training_loop::when::type::ALWAYS) {
        _training_fiber.unpause();
    } else if (when == utils::dict_training_loop::when::type::WHEN_LEADER) {
        _is_leader ? _training_fiber.unpause() : _training_fiber.pause();
    }
};

future<> dictionary_service::stop() {
    utils::dict_trainer_logger.debug("dictionary_service::stop(), called");
    // Don't let the feature observer start the training fiber after it's cancelled.
    _feature_observer.reset();
    _training_fiber.cancel();
    co_await std::move(_training_fiber_future);
}

future<> dictionary_service::publish_dict(utils::dict_sampler::dict_type d) {
    while (true) {
        utils::dict_trainer_logger.debug("dictionary_service::publish_dict(), called");
        try {
            utils::dict_trainer_logger.debug("dictionary_service::publish_dict(), trying");
            auto batch = service::group0_batch(co_await _raft_group0_client.start_operation(_as));
            auto write_ts = batch.write_timestamp();
            auto new_dict_ts = db_clock::now();
            auto data = bytes(reinterpret_cast<const bytes::value_type*>(d.data()), d.size());
            mutation publish_new_dict = co_await _sys_ks.get_insert_dict_mutation(std::move(data), _our_host_id, new_dict_ts, write_ts);
            batch.add_mutation(std::move(publish_new_dict), "publish new compression dictionary");
            utils::dict_trainer_logger.debug("dictionary_service::publish_dict(), committing");
            co_await std::move(batch).commit(_raft_group0_client, _as, {});
            utils::dict_trainer_logger.debug("dictionary_service::publish_dict(), finished");
            break;
        } catch (const service::group0_concurrent_modification&) {
            utils::dict_trainer_logger.debug("group0_concurrent_modification in dictionary_service::publish_dict(), retrying");
        }
    }
}
