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

#pragma once

#include <vector>
#include "types.hh"
#include "core/sstring.hh"
#include "util/serialization.hh"
#include "utils/UUID.hh"
#include "version_generator.hh"

namespace gms {

/**
 * This abstraction represents the state associated with a particular node which an
 * application wants to make available to the rest of the nodes in the cluster.
 * Whenever a piece of state needs to be disseminated to the rest of cluster wrap
 * the state in an instance of <i>ApplicationState</i> and add it to the Gossiper.
 * <p></p>
 * e.g. if we want to disseminate load information for node A do the following:
 * <p></p>
 * ApplicationState loadState = new ApplicationState(<string representation of load>);
 * Gossiper.instance.addApplicationState("LOAD STATE", loadState);
 */

class versioned_value //implements Comparable<VersionedValue>
{

public:
    // this must be a char that cannot be present in any token
    static constexpr char DELIMITER = ',';
    static constexpr const char DELIMITER_STR[] = { DELIMITER, 0 };

    // values for ApplicationState.STATUS
    static constexpr const char *STATUS_BOOTSTRAPPING = "BOOT";
    static constexpr const char *STATUS_NORMAL = "NORMAL";
    static constexpr const char *STATUS_LEAVING = "LEAVING";
    static constexpr const char *STATUS_LEFT = "LEFT";
    static constexpr const char *STATUS_MOVING = "MOVING";

    static constexpr const char *REMOVING_TOKEN = "removing";
    static constexpr const char *REMOVED_TOKEN = "removed";

    static constexpr const char *HIBERNATE = "hibernate";

    // values for ApplicationState.REMOVAL_COORDINATOR
    static constexpr const char *REMOVAL_COORDINATOR = "REMOVER";

    const int version;
    const sstring value;

private:
    versioned_value(const sstring& value, int version = version_generator::get_next_version())
        : version(version), value(value)
    {
#if 0
        // blindly interning everything is somewhat suboptimal -- lots of VersionedValues are unique --
        // but harmless, and interning the non-unique ones saves significant memory.  (Unfortunately,
        // we don't really have enough information here in VersionedValue to tell the probably-unique
        // values apart.)  See CASSANDRA-6410.
        this.value = value.intern();
#endif
    }

    versioned_value(sstring&& value, int version = version_generator::get_next_version())
        : version(version), value(std::move(value)) {
    }


public:
    int compareTo(const versioned_value &value)
    {
        return version - value.version;
    }

    friend inline std::ostream& operator<<(std::ostream& os, const versioned_value& x) {
        return os << "Value(" << x.value << "," << x.version <<  ")";
    }

#if 0
    private static String versionString(String... args)
    {
        return StringUtils.join(args, VersionedValue.DELIMITER);
    }
#endif

    class versioned_value_factory
    {
    public:
#if 0
        final IPartitioner partitioner;

        public VersionedValueFactory(IPartitioner partitioner)
        {
            this.partitioner = partitioner;
        }
#endif

        versioned_value clone_with_higher_version(const versioned_value& value)
        {
            return versioned_value(value.value);
        }

#if 0
        public versioned_value bootstrapping(const std::vector<token> tokens)
        {
            return new versioned_value(versionString(VersionedValue.STATUS_BOOTSTRAPPING,
                                                    makeTokenString(tokens)));
        }

        public VersionedValue normal(Collection<Token> tokens)
        {
            return new VersionedValue(versionString(VersionedValue.STATUS_NORMAL,
                                                    makeTokenString(tokens)));
        }

        private String makeTokenString(Collection<Token> tokens)
        {
            return partitioner.getTokenFactory().toString(Iterables.get(tokens, 0));
        }
#endif

        static inline versioned_value load(double load)
        {
            return versioned_value(to_sstring_sprintf(load, "%g"));
        }

        versioned_value schema(const utils::UUID &new_version)
        {
            return versioned_value(new_version.to_sstring());
        }

#if 0
        versioned_value leaving(Collection<Token> tokens)
        {
            return new VersionedValue(versionString(VersionedValue.STATUS_LEAVING,
                                                    makeTokenString(tokens)));
        }

        public VersionedValue left(Collection<Token> tokens, long expireTime)
        {
            return new VersionedValue(versionString(VersionedValue.STATUS_LEFT,
                                                    makeTokenString(tokens),
                                                    Long.toString(expireTime)));
        }

        public VersionedValue moving(Token token)
        {
            return new VersionedValue(VersionedValue.STATUS_MOVING + VersionedValue.DELIMITER + partitioner.getTokenFactory().toString(token));
        }
#endif

        versioned_value host_id(const utils::UUID& hostId)
        {
            return versioned_value(hostId.to_sstring());
        }

#if 0
        public VersionedValue tokens(Collection<Token> tokens)
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bos);
            try
            {
                TokenSerializer.serialize(partitioner, tokens, out);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            return new VersionedValue(new String(bos.toByteArray(), ISO_8859_1));
        }
#endif

        versioned_value removing_nonlocal(const utils::UUID& hostId)
        {
            return versioned_value(sstring(REMOVING_TOKEN) + sstring(DELIMITER_STR) + hostId.to_sstring());
        }

        versioned_value removed_nonlocal(const utils::UUID& hostId, int64_t expireTime)
        {
            return versioned_value(sstring(REMOVED_TOKEN) + sstring(DELIMITER_STR) + hostId.to_sstring() + sstring(DELIMITER_STR) + to_sstring(expireTime));
        }

        versioned_value removal_coordinator(const utils::UUID& hostId)
        {
            return versioned_value(sstring(REMOVAL_COORDINATOR) + sstring(DELIMITER_STR) + hostId.to_sstring());
        }

        versioned_value hibernate(bool value)
        {
            return versioned_value(sstring(HIBERNATE) + sstring(DELIMITER_STR) + (value ? "true" : "false"));
        }

        versioned_value datacenter(const sstring& dcId)
        {
            return versioned_value(dcId);
        }

        versioned_value rack(const sstring &rackId)
        {
            return versioned_value(rackId);
        }

#if 0
        public VersionedValue rpcaddress(InetAddress endpoint)
        {
            return new VersionedValue(endpoint.getHostAddress());
        }

        public VersionedValue releaseVersion()
        {
            return new VersionedValue(FBUtilities.getReleaseVersionString());
        }

        public VersionedValue networkVersion()
        {
            return new VersionedValue(String.valueOf(MessagingService.current_version));
        }
#endif

        versioned_value internalIP(const sstring &private_ip)
        {
            return versioned_value(private_ip);
        }

        versioned_value severity(double value)
        {
            return versioned_value(to_sstring_sprintf(value, "%g"));
        }
    };

    // The following replaces VersionedValueSerializer from the Java code
public:
    void serialize(bytes::iterator& out) const {
        serialize_string(out, value);
        serialize_int32(out, version);
    }

    static versioned_value deserialize(bytes_view& v) {
        auto value = read_simple_short_string(v);
        auto version = read_simple<int32_t>(v);
        return versioned_value(std::move(value), version);
    }

    size_t serialized_size() const {
        return serialize_string_size(value) + serialize_int32_size;
    }
}; // class versioned_value

} // namespace gms
