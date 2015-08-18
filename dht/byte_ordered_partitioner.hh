/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "i_partitioner.hh"
#include "bytes.hh"

#include "sstables/key.hh"

namespace dht {

class byte_ordered_partitioner final : public i_partitioner {
public:
    virtual const sstring name() { return "org.apache.cassandra.dht.ByteOrderedPartitioner"; }
    virtual token get_token(const schema& s, partition_key_view key) override {
        auto&& legacy = key.legacy_form(s);
        return token(token::kind::key, bytes(legacy.begin(), legacy.end()));
    }
    virtual token get_token(const sstables::key_view& key) override {
        auto v = bytes_view(key);
        if (v.empty()) {
            return minimum_token();
        }
        return token(token::kind::key, bytes(v.begin(), v.end()));
    }
    virtual token get_random_token() override;
    virtual bool preserves_order() override { return true; }
    virtual std::map<token, float> describe_ownership(const std::vector<token>& sorted_tokens) override;
    virtual data_type get_token_validator() override { return bytes_type; }
    virtual int tri_compare(const token& t1, const token& t2) override {
        return compare_unsigned(t1._data, t2._data);
    }
    virtual token midpoint(const token& t1, const token& t2) const;
    virtual sstring to_sstring(const dht::token& t) const override {
        if (t._kind == dht::token::kind::before_all_keys) {
            return sstring();
        } else {
            return to_hex(t._data);
        }
    }
    virtual dht::token from_sstring(const sstring& t) const override {
        if (t.empty()) {
            return minimum_token();
        } else {
            auto data = from_hex(t);
            return token(token::kind::key, bytes(data.begin(), data.end()));
        }
    }
    virtual unsigned shard_of(const token& t) const override;
};

}
