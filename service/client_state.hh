#ifndef SERVICE_CLIENT_STATE_HH
#define SERVICE_CLIENT_STATE_HH

namespace service {

class client_state {
private:
    sstring _keyspace;

public:
    // FIXME: stub

    void validate_login() const {
    }

    void set_keyspace(sstring keyspace) {
        _keyspace = keyspace;
    }

    sstring get_keyspace() const {
        return _keyspace;
    }    
};

}

#endif
