#include "locator/host_id.hh"
#include "utils/enum_option.hh"
#include "utils/updateable_value.hh"
#include "utils/dict_trainer.hh"
#include <any>

namespace db {
    class system_keyspace;
} // namespace db

namespace utils {
    class alien_worker;
} // namespace utils

namespace service {
    class raft_group0_client;
    class raft_group0;
} // namespace service

namespace gms {
    class feature_service;
} // namespace gms

// A bag of code responsible for starting, stopping, pausing and unpausing RPC compression
// dictionary training, and for publishing its results to system.dicts (via Raft group 0).
// 
// It starts the training when the relevant cluster feature is enabled,
// pauses and unpauses the training appropriately whenever relevant config or leadership status are updated,
// and publishes new dictionaries whenever the training fiber produces them.
class dictionary_service {
    db::system_keyspace& _sys_ks;
    locator::host_id _our_host_id;
    utils::updateable_value<enum_option<utils::dict_training_loop::when>> _rpc_dict_training_when;
    service::raft_group0_client& _raft_group0_client;
    abort_source& _as;
    utils::dict_training_loop _training_fiber;
    future<> _training_fiber_future;

    bool _is_leader = false;
    utils::observer<bool> _leadership_observer;
    utils::observer<enum_option<utils::dict_training_loop::when>> _when_observer;
    std::optional<std::any> _feature_observer;

    void maybe_toggle_dict_training();
    future<> publish_dict(utils::dict_sampler::dict_type);
public:
    // This template trick forces the user of `config` to initialize all fields explicitly.
    template <typename Uninitialized = void>
    struct config {
        locator::host_id our_host_id = Uninitialized();
        utils::updateable_value<uint32_t> rpc_dict_training_min_time_seconds = Uninitialized();
        utils::updateable_value<uint64_t> rpc_dict_training_min_bytes = Uninitialized();
        utils::updateable_value<enum_option<utils::dict_training_loop::when>> rpc_dict_training_when = Uninitialized();
    };
    // Note: the training fiber will start as soon as the relevant cluster feature is enabled. 
    dictionary_service(
        utils::dict_sampler&,
        db::system_keyspace&,
        utils::alien_worker&,
        service::raft_group0_client&,
        service::raft_group0&,
        abort_source& stop_signal,
        gms::feature_service&,
        config<>
    );
    // For clean shutdown, this must be called and awaited before destruction.
    future<> stop();
};
