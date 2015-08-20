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

} // namespace dht
