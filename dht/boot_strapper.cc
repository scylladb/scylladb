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

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "dht/boot_strapper.hh"
#include "service/storage_service.hh"

namespace dht {

future<> boot_strapper::bootstrap() {
    // FIXME: Stream data from other nodes
    service::get_local_storage_service().finish_bootstrapping();
    return make_ready_future<>();
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

std::unordered_set<token> boot_strapper::get_bootstrap_tokens(token_metadata metadata, database& db) {
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
    size_t num_tokens = db.get_config().num_tokens();
    if (num_tokens < 1) {
        throw std::runtime_error("num_tokens must be >= 1");
    }

    // if (numTokens == 1)
    //     logger.warn("Picking random token for a single vnode.  You should probably add more vnodes; failing that, you should probably specify the token manually");

    return get_random_tokens(metadata, num_tokens);
}

std::unordered_set<token> boot_strapper::get_random_tokens(token_metadata metadata, size_t num_tokens) {
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


} // namespace dht
