#pragma once

#include "seastar/core/sharded.hh"
#include "dht/i_partitioner_fwd.hh"

namespace replica {
class database;
}

namespace netw {
class messaging_service;
}

using namespace seastar;

namespace db::view {

class view_building_worker : public seastar::peering_sharded_service<view_building_worker> {
    replica::database& _db;
    sharded<netw::messaging_service>& _messaging;

public:
    view_building_worker(replica::database& db, sharded<netw::messaging_service>& messaging);

    future<> stop();

    class consumer;
private:
    future<dht::token_range> build_view_range(sstring ks_name, sstring view_name, dht::token_range range);

    void init_messaging_service();
    future<> uninit_messaging_service();
};

}
