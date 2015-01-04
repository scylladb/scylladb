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
 * Modified by Cloudius Systems.
 * Copyright 2015 Cloudius Systems.
 */

#include "UUID_gen.hh"

#include <stdlib.h>

namespace utils {


#if 0
private static byte[] hash(Collection<InetAddress> data)
{
    try
    {
        MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        for(InetAddress addr : data)
            messageDigest.update(addr.getAddress());

        return messageDigest.digest();
    }
    catch (NoSuchAlgorithmException nsae)
    {
        throw new RuntimeException("MD5 digest algorithm is not available", nsae);
    }
}


static int64_t make_node()
{
   /*
    * We don't have access to the MAC address but need to generate a node part
    * that identify this host as uniquely as possible.
    * The spec says that one option is to take as many source that identify
    * this node as possible and hash them together. That's what we do here by
    * gathering all the ip of this host.
    * Note that FBUtilities.getBroadcastAddress() should be enough to uniquely
    * identify the node *in the cluster* but it triggers DatabaseDescriptor
    * instanciation and the UUID generator is used in Stress for instance,
    * where we don't want to require the yaml.
    */
    Collection<InetAddress> localAddresses = FBUtilities.getAllLocalAddresses();
    if (localAddresses.isEmpty())
        throw new RuntimeException("Cannot generate the node component of the UUID because cannot retrieve any IP addresses.");

    // ideally, we'd use the MAC address, but java doesn't expose that.
    byte[] hash = hash(localAddresses);
    long node = 0;
    for (int i = 0; i < Math.min(6, hash.length); i++)
        node |= (0x00000000000000ff & (long)hash[i]) << (5-i)*8;
    assert (0xff00000000000000L & node) == 0;

    // Since we don't use the mac address, the spec says that multicast
    // bit (least significant bit of the first octet of the node ID) must be 1.
    return node | 0x0000010000000000L;
}
#endif

static int64_t make_node()
{
    // FIXME: don't leave this constant zero! See the above commented-out code
    // which is what Cassandra's UUIDGen.java did. We can also get the MAC address.
    return 0;
}


static int64_t make_clock_seq_and_node()
{
    // The original Java code did this, shuffling the number of millis
    // since the epoch, and taking 14 bits of it. We don't do exactly
    // the same, but the idea is the same.
    //long clock = new Random(System.currentTimeMillis()).nextLong();
    unsigned int seed = std::chrono::system_clock::now().time_since_epoch().count();
    int clock = rand_r(&seed);

    long lsb = 0;
    lsb |= 0x8000000000000000L;                 // variant (2 bits)
    lsb |= (clock & 0x0000000000003FFFL) << 48; // clock sequence (14 bits)
    lsb |= make_node();                          // 6 bytes
    return lsb;
}

const int64_t UUID_gen::clock_seq_and_node = make_clock_seq_and_node();
const std::unique_ptr<UUID_gen> UUID_gen::instance (new UUID_gen());


} // namespace utils
