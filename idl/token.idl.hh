namespace dht {

enum class token_kind : int {
    before_all_keys,
    key,
    after_all_keys,
};

class legacy_token {
    dht::token_kind _kind;
    bytes data();
};

class decorated_key {
    dht::token token();
    partition_key key();
};

}
