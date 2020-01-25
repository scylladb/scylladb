namespace dht {
class token {
    enum class kind : int {
        before_all_keys,
        key,
        after_all_keys,
    };
    dht::token::kind _kind;
    bytes data();
};

class decorated_key {
    dht::token token();
    partition_key key();
};

}
