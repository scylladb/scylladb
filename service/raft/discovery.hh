/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once
#include "service/raft/group0_fwd.hh"

namespace service {

// Raft leader discovery FSM
// https://github.com/kbr-/scylla-raft-boot/blob/master/boot.tla
//
// Contact all known peers, extending the transitive closure of
// the known peers, sharing this server's Raft Id and the list of
// its peers. Once the transitive closure of peers has been built,
// select the peer with the smallest Raft Id to be the leader. To
// be used during initial setup of Raft Group 0.
//
// This class is public for testing.
// In production code, `persistent_discovery` should be used.
class discovery {
public:
    // During discovery, peers are identified based on their Internet
    // address, not Raft server id.
    struct discovery_peer_hash {
        size_t operator()(const discovery_peer& address) const {
            return std::hash<gms::inet_address>{}(address.ip_addr);
        }
    };
    struct discovery_peer_equal {
        bool operator()(const discovery_peer& lhs, const discovery_peer& rhs) const {
            return lhs.ip_addr == rhs.ip_addr;
        }
    };
    // When a fresh cluster is bootstrapping, peer list is
    // used to build a transitive closure of all cluster members
    // and select an initial Raft configuration of the cluster.
    using peer_list = std::vector<discovery_peer>;
    using peer_set = std::unordered_set<discovery_peer, discovery_peer_hash, discovery_peer_equal>;
    struct i_am_leader {};
    struct pause {};
    using request_list = std::vector<std::pair<discovery_peer, peer_list>>;

    // @sa discovery::tick()
    using tick_output = std::variant<i_am_leader, pause, request_list>;
private:
    discovery_peer _self;
    // Assigned if this server elects itself a leader.
    bool _is_leader = false;
    // _seeds + all peers we've discovered, excludes _self.
    // The user of `discovery` must ensure that all _peers are persisted
    // before externalizing any output (returned from `tick` or `request`).
    peer_set _peers;
    // A subset of _peers which have responded to our requests, excludes _self.
    peer_set _responded;
    // _peers + self - the peer list we're sharing; if this node
    // is a leader, empty list to save bandwidth
    peer_list _peer_list;
    // outstanding messages
    request_list _requests;
private:
    // Update this state machine with new peer data and
    // create outbound messages if necessary.
    void step(const peer_list& peers);
    // Check if we can run election and then elect itself
    // a leader.
    void maybe_become_leader();
public:
    // For construction, pass this server's Internet address and
    // Raft id - and a set of seed Internet addresses. It's OK to
    // leave Raft ids of seed peers unset, they will be updated as
    // these peers respond.
    //
    // For discovery to work correctly the following must hold:
    //
    // - this server's Raft id must survive restarts.
    // The opposite would be a Byzantine failure: imagine
    // we generate and share a big id first, so another node
    // elects itself a leader. Then this node restarts, generates
    // the smallest known id and elects itself a leader too.
    //
    // - the seed graph must contain a vertex which is reachable from
    // every other vertex, for example it can be be fully
    // connected, with either each server having at least one
    // common seed or seed connections forming a loop. A rule of
    // thumb is to use the same seed list everywhere.
    //
    // - the set of discovered peers must survive restarts.
    // When `discovery` is constructed again after a restart,
    // `seeds` must contain at least all peers discovered until now,
    // and `self` must be the same as before.
    // Each time a new peer is discovered or its Raft ID is updated,
    // it will become present in the set returned by `peers()`.
    // The caller of `tick`/`request` must ensure that all peers
    // discovered before this call are persisted before the output
    // of this call is externalized (e.g. before a response to `request`
    // is sent back to the requester, or before a new request is sent
    // based on the output of `tick`).
    //
    // `seeds` may contain `self` but doesn't have to.
    discovery(discovery_peer self, const peer_list& seeds);

    // To be used on the receiving peer to generate a reply
    // while the discovery protocol is in progress.
    // Until the node becomes a leader, returns the list of known peers.
    // When (if) the node becomes a leader, returns std::nullopt.
    std::optional<peer_list> request(const peer_list& peers);

    // Submit a reply from one of the peers to this discovery
    // state machine. If this node is a leader, response is
    // ignored.
    void response(discovery_peer from, const peer_list& peers);

    // Until all peers answer, returns a list of messages for the
    // peers which haven't replied yet. As soon as all peers have
    // replied, returns a pause{}, to allow some node to become
    // a leader, and then a list of messages for all peers which
    // can be used to find the leader. If this node is a leader,
    // returns leader{}.
    tick_output tick();

    // The list of peers discovered until now (including the seeds and self).
    // Must be persisted before externalizing output (from `tick` or `request`).
    const peer_list& get_peer_list() const { return _peer_list; }

    // A helper for testing.
    bool is_leader() { return _is_leader; }

    // A helper used for testing
    raft::server_id id() const { return _self.id; }
};

} // namespace raft

