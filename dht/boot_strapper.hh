/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modified by Cloudius Systems
 * Copyright 2015 Cloudius Systems
 */
#pragma once
#include "gms/inet_address.hh"
#include "locator/token_metadata.hh"
#include "dht/i_partitioner.hh"
#include <unordered_set>
#include "database.hh"

namespace dht {

class boot_strapper {
    using inet_address = gms::inet_address;
    using token_metadata = locator::token_metadata;
    using token = dht::token;
    /* endpoint that needs to be bootstrapped */
    inet_address _address;
    /* token of the node being bootstrapped. */
    std::vector<token> _tokens;
    token_metadata _token_metadata;
public:
    boot_strapper(inet_address addr, std::vector<token> tokens, token_metadata tmd)
        : _address(addr)
        , _tokens(tokens)
        , _token_metadata(tmd) {
    }

    void bootstrap() {
#if 0
        if (logger.isDebugEnabled())
            logger.debug("Beginning bootstrap process");

        RangeStreamer streamer = new RangeStreamer(tokenMetadata, tokens, address, "Bootstrap");
        streamer.addSourceFilter(new RangeStreamer.FailureDetectorSourceFilter(FailureDetector.instance));

        for (String keyspaceName : Schema.instance.getNonSystemKeyspaces())
        {
            AbstractReplicationStrategy strategy = Keyspace.open(keyspaceName).getReplicationStrategy();
            streamer.addRanges(keyspaceName, strategy.getPendingAddressRanges(tokenMetadata, tokens, address));
        }

        try
        {
            streamer.fetchAsync().get();
            StorageService.instance.finishBootstrapping();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException("Interrupted while waiting on boostrap to complete. Bootstrap will have to be restarted.");
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException("Error during boostrap: " + e.getCause().getMessage(), e.getCause());
        }
#endif
    }

    /**
     * if initialtoken was specified, use that (split on comma).
     * otherwise, if num_tokens == 1, pick a token to assume half the load of the most-loaded node.
     * else choose num_tokens tokens at random
     */
    static std::unordered_set<token> get_bootstrap_tokens(token_metadata metadata, database& db) {
#if 0
        Collection<String> initialTokens = DatabaseDescriptor.getInitialTokens();
        // if user specified tokens, use those
        if (initialTokens.size() > 0)
        {
            logger.debug("tokens manually specified as {}",  initialTokens);
            List<Token> tokens = new ArrayList<Token>(initialTokens.size());
            for (String tokenString : initialTokens)
            {
                Token token = StorageService.getPartitioner().getTokenFactory().fromString(tokenString);
                if (metadata.getEndpoint(token) != null)
                    throw new ConfigurationException("Bootstrapping to existing token " + tokenString + " is not allowed (decommission/removenode the old node first).");
                tokens.add(token);
            }
            return tokens;
        }
#endif
        // FIXME: DatabaseDescriptor.getNumTokens();
        size_t num_tokens = 3;
        if (num_tokens < 1) {
            throw std::runtime_error("num_tokens must be >= 1");
        }

        // if (numTokens == 1)
        //     logger.warn("Picking random token for a single vnode.  You should probably add more vnodes; failing that, you should probably specify the token manually");

        return get_random_tokens(metadata, num_tokens);
    }

    static std::unordered_set<token> get_random_tokens(token_metadata metadata, size_t num_tokens) {
        std::unordered_set<token> tokens;
        while (tokens.size() < num_tokens) {
            auto token = global_partitioner().get_random_token();
            auto ep = metadata.get_endpoint(token);
            if (!ep) {
                tokens.emplace(token);
            }
        }
        return tokens;
    }

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
};

} // namespace dht
