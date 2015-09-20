
/*
 * Copyright 2015 Cloudius Systems
 */

#ifndef SERVICE_QUERY_STATE_HH
#define SERVICE_QUERY_STATE_HH

#include "service/client_state.hh"

namespace service {

class query_state final {
private:
    client_state& _client_state;
public:
    query_state(client_state& client_state_) : _client_state(client_state_) {}
    client_state& get_client_state() {
        return _client_state;
    }
    api::timestamp_type get_timestamp() {
        return _client_state.get_timestamp();
    }
};

}

#endif
