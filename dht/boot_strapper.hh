/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */
#pragma once
#include "gms/inet_address.hh"
#include "locator/token_metadata.hh"
#include "dht/token.hh"
#include <unordered_set>
#include "replica/database_fwd.hh"
#include "streaming/stream_reason.hh"
#include "service/topology_guard.hh"
#include <seastar/core/distributed.hh>
#include <seastar/core/abort_source.hh>

namespace streaming { class stream_manager; }
namespace gms { class gossiper; }
namespace db { class config; }

namespace dht {

using check_token_endpoint = bool_class<struct check_token_endpoint_tag>;

class boot_strapper {
    using inet_address = gms::inet_address;
    using token_metadata = locator::token_metadata;
    using token_metadata_ptr = locator::token_metadata_ptr;
    using token = dht::token;
    distributed<replica::database>& _db;
    sharded<streaming::stream_manager>& _stream_manager;
    abort_source& _abort_source;
    /* endpoint that needs to be bootstrapped */
    locator::host_id _address;
    /* its DC/RACK info */
    locator::endpoint_dc_rack _dr;
    /* token of the node being bootstrapped. */
    std::unordered_set<token> _tokens;
    const locator::token_metadata_ptr _token_metadata_ptr;
public:
    boot_strapper(distributed<replica::database>& db, sharded<streaming::stream_manager>& sm, abort_source& abort_source,
            locator::host_id addr, locator::endpoint_dc_rack dr, std::unordered_set<token> tokens, const token_metadata_ptr tmptr)
        : _db(db)
        , _stream_manager(sm)
        , _abort_source(abort_source)
        , _address(addr)
        , _dr(std::move(dr))
        , _tokens(tokens)
        , _token_metadata_ptr(std::move(tmptr)) {
    }

    future<> bootstrap(streaming::stream_reason reason, gms::gossiper& gossiper, service::frozen_topology_guard, inet_address replace_address = {});

    /**
     * if initialtoken was specified, use that (split on comma).
     * otherwise, if num_tokens == 1, pick a token to assume half the load of the most-loaded node.
     * else choose num_tokens tokens at random
     */
    static std::unordered_set<token> get_bootstrap_tokens(const token_metadata_ptr tmptr, sstring initialtoken, uint32_t num_tokens, check_token_endpoint check);
    static std::unordered_set<token> get_bootstrap_tokens(token_metadata_ptr tmptr, const db::config& cfg, check_token_endpoint check);


    /**
     * Same as above but does not consult initialtoken config
     */
    static std::unordered_set<token> get_random_bootstrap_tokens(const token_metadata_ptr tmptr, size_t num_tokens);

    static std::unordered_set<token> get_random_tokens(const token_metadata_ptr tmptr, size_t num_tokens);
#if 0
    public static class StringSerializer implements IVersionedSerializer<String>
    {
        public static final StringSerializer instance = new StringSerializer();

        public void serialize(String s, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(s);
        }

        public String deserialize(DataInput in, int version) throws IOException
        {
            return in.readUTF();
        }

        public long serializedSize(String s, int version)
        {
            return TypeSizes.NATIVE.sizeof(s);
        }
    }
#endif

private:
    const token_metadata& get_token_metadata() {
        return *_token_metadata_ptr;
    }
};

} // namespace dht
