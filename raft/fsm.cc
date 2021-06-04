/*
 * Copyright (C) 2020-present ScyllaDB
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
#include "fsm.hh"
#include <random>
#include <seastar/core/coroutine.hh>

namespace raft {

leader::~leader() {
    log_limiter_semaphore.broken(not_a_leader(fsm.current_leader()));
}

fsm::fsm(server_id id, term_t current_term, server_id voted_for, log log,
        failure_detector& failure_detector, fsm_config config) :
        _my_id(id), _current_term(current_term), _voted_for(voted_for),
        _log(std::move(log)), _failure_detector(failure_detector), _config(config) {
    if (id == raft::server_id{}) {
        throw std::invalid_argument("raft::fsm: raft instance cannot have id zero");
    }
    // The snapshot can not contain uncommitted entries
    _commit_idx = _log.get_snapshot().idx;
    _observed.advance(*this);
    logger.trace("{}: starting, current term {}, log length {}", _my_id, _current_term, _log.last_idx());
    reset_election_timeout();
}

future<> fsm::wait_max_log_size() {
    check_is_leader();

   return leader_state().log_limiter_semaphore.wait();
}

const configuration& fsm::get_configuration() const {
    check_is_leader();
    return _log.get_configuration();
}

template<typename T>
const log_entry& fsm::add_entry(T command) {
    // It's only possible to add entries on a leader.
    check_is_leader();
    if(leader_state().stepdown) {
        // A leader that is stepping down should not add new entries
        // to its log (see 3.10), but it still does not know who the new
        // leader will be.
        throw not_a_leader({});
    }

    if constexpr (std::is_same_v<T, configuration>) {
        if (_log.last_conf_idx() > _commit_idx ||
            _log.get_configuration().is_joint()) {
            // 4.1. Cluster membership changes/Safety.
            //
            // Leaders avoid overlapping configuration changes by
            // not beginning a new change until the previous
            // change’s entry has committed. It is only safe to
            // start another membership change once a majority of
            // the old cluster has moved to operating under the
            // rules of C_new.
            logger.trace("A{}configuration change at index {} is not yet committed (commit_idx: {})",
                _log.get_configuration().is_joint() ? " joint " : " ",
                _log.last_conf_idx(), _commit_idx);
            throw conf_change_in_progress();
        }
        // 4.3. Arbitrary configuration changes using joint consensus
        //
        // When the leader receives a request to change the
        // configuration from C_old to C_new , it stores the
        // configuration for joint consensus (C_old,new) as a log
        // entry and replicates that entry using the normal Raft
        // mechanism.
        configuration tmp(_log.get_configuration());
        tmp.enter_joint(command.current);
        command = std::move(tmp);
    }

    _log.emplace_back(seastar::make_lw_shared<log_entry>({_current_term, _log.next_idx(), std::move(command)}));
    _sm_events.signal();

    if constexpr (std::is_same_v<T, configuration>) {
        // 4.1. Cluster membership changes/Safety.
        //
        // The new configuration takes effect on each server as
        // soon as it is added to that server’s log: the C_new
        // entry is replicated to the C_new servers, and
        // a majority of the new configuration is used to
        // determine the C_new entry’s commitment.
        leader_state().tracker.set_configuration(_log.get_configuration(), _log.last_idx());
    }

    return *_log[_log.last_idx()];
}

template const log_entry& fsm::add_entry(command command);
template const log_entry& fsm::add_entry(configuration command);
template const log_entry& fsm::add_entry(log_entry::dummy dummy);

void fsm::advance_commit_idx(index_t leader_commit_idx) {

    auto new_commit_idx = std::min(leader_commit_idx, _log.stable_idx());

    logger.trace("advance_commit_idx[{}]: leader_commit_idx={}, new_commit_idx={}",
        _my_id, leader_commit_idx, new_commit_idx);

    if (new_commit_idx > _commit_idx) {
        _commit_idx = new_commit_idx;
        _sm_events.signal();
        logger.trace("advance_commit_idx[{}]: signal apply_entries: committed: {}",
            _my_id, _commit_idx);
    }
}


void fsm::update_current_term(term_t current_term)
{
    assert(_current_term < current_term);
    _current_term = current_term;
    _voted_for = server_id{};
}

void fsm::reset_election_timeout() {
    static thread_local std::default_random_engine re{std::random_device{}()};
    static thread_local std::uniform_int_distribution<> dist(1, ELECTION_TIMEOUT.count());
    _randomized_election_timeout = ELECTION_TIMEOUT + logical_clock::duration{dist(re)};
}

void fsm::become_leader() {
    assert(!std::holds_alternative<leader>(_state));
    _state.emplace<leader>(_config.max_log_size, *this);
    leader_state().log_limiter_semaphore.consume(_log.in_memory_size());
    _last_election_time = _clock.now();
    // a new leader needs to commit at lease one entry to make sure that
    // all existing entries in its log are committed as well. Also it should
    // send append entries RPC as soon as possible to establish its leadership
    // (3.4). Do both of those by committing a dummy entry.
    add_entry(log_entry::dummy());
    // set_configuration() begins replicating from the last entry
    // in the log.
    leader_state().tracker.set_configuration(_log.get_configuration(), _log.last_idx());
    logger.trace("fsm::become_leader() {} stable index: {} last index: {}",
        _my_id, _log.stable_idx(), _log.last_idx());
    replicate();
}

void fsm::become_follower(server_id leader) {
    if (leader == _my_id) {
        on_internal_error(logger, "fsm cannot become a follower of itself");
    }
    _state = follower{.current_leader = leader};
    if (leader != server_id{}) {
        _last_election_time = _clock.now();
    }
}

void fsm::become_candidate(bool is_prevote, bool is_leadership_transfer) {
    // When starting a campain we need to reset current leader otherwise
    // disruptive server prevention will stall an election if quorum of nodes
    // start election together since each one will ignore vote requests from others
    _state = candidate(_log.get_configuration(), is_prevote);

    reset_election_timeout();

    // 3.4 Leader election
    //
    // A possible outcome is that a candidate neither wins nor
    // loses the election: if many followers become candidates at
    // the same time, votes could be split so that no candidate
    // obtains a majority. When this happens, each candidate will
    // time out and start a new election by incrementing its term
    // and initiating another round of RequestVote RPCs.
    _last_election_time = _clock.now();

    auto& votes = candidate_state().votes;

    const auto& voters = votes.voters();
    if (!voters.contains(server_address{_my_id})) {
        // If the server is not part of the current configuration,
        // revert to the follower state without increasing
        // the current term.
        become_follower(server_id{});
        return;
    }

    term_t term{_current_term + 1};
    if (!is_prevote) {
        update_current_term(term);
    }
    // Replicate RequestVote
    for (const auto& server : voters) {
        if (server.id == _my_id) {
            // Vote for self.
            votes.register_vote(server.id, true);
            if (!is_prevote) {
                // Only record real votes
                _voted_for = _my_id;
            }
            // Already signaled _sm_events in update_current_term()
            continue;
        }
        logger.trace("{} [term: {}, index: {}, last log term: {}{}{}] sent vote request to {}",
            _my_id, term, _log.last_idx(), _log.last_term(), is_prevote ? ", prevote" : "",
            is_leadership_transfer ? ", force" : "", server.id);

        send_to(server.id, vote_request{term, _log.last_idx(), _log.last_term(), is_prevote, is_leadership_transfer});
    }
    if (votes.tally_votes() == vote_result::WON) {
        // A single node cluster.
        if (is_prevote) {
            become_candidate(false);
        } else {
            become_leader();
        }
    }
}

future<fsm_output> fsm::poll_output() {
    logger.trace("fsm::poll_output() {} stable index: {} last index: {}",
        _my_id, _log.stable_idx(), _log.last_idx());

    while (true) {
        auto diff = _log.last_idx() - _log.stable_idx();

        if (diff > 0 || !_messages.empty() || !_observed.is_equal(*this)) {
            break;
        }
        co_await _sm_events.wait();
    }
    co_return get_output();
}

fsm_output fsm::get_output() {
    fsm_output output;

    auto diff = _log.last_idx() - _log.stable_idx();

    if (diff > 0) {
        output.log_entries.reserve(diff);

        for (auto i = _log.stable_idx() + 1; i <= _log.last_idx(); i++) {
            // Copy before saving to storage to prevent races with log updates,
            // e.g. truncation of the log.
            // TODO: avoid copies by making sure log truncate is
            // copy-on-write.
            output.log_entries.emplace_back(_log[i]);
        }
    }

    if (_observed._current_term != _current_term || _observed._voted_for != _voted_for) {
        output.term_and_vote = {_current_term, _voted_for};
    }

    // see if there was a new snapshot that has to be handled
    if (_observed._snapshot.id != _log.get_snapshot().id) {
        output.snp = _log.get_snapshot();
    }

    // Return committed entries.
    // Observer commit index may be smaller than snapshot index
    // in which case we should not attempt committing entries belonging
    // to a snapshot.
    auto observed_ci =  std::max(_observed._commit_idx, _log.get_snapshot().idx);
    if (observed_ci < _commit_idx) {
        output.committed.reserve(_commit_idx - observed_ci);

        for (auto idx = observed_ci + 1; idx <= _commit_idx; ++idx) {
            const auto& entry = _log[idx];
            output.committed.push_back(entry);
        }
    }

    // Get a snapshot of all unsent messages.
    // Do it after populating log_entries and committed arrays
    // to not lose messages in case arrays population throws
    std::swap(output.messages, _messages);

    // Fill server_address_set corresponding to the configuration from
    // the rpc point of view.
    //
    // Effective rpc configuration changes when one of the following applies:
    // * `last_conf_idx()` could have changed or
    // * A new configuration entry may be overwritten by application of two
    //   snapshots with different configurations following each other.
    // * Leader overwrites a follower's log.
    if (_observed._last_conf_idx != _log.last_conf_idx() ||
            (_observed._current_term != _log.last_term() &&
             _observed._last_term != _log.last_term())) {
        configuration last_log_conf = _log.get_configuration();
        last_log_conf.current.merge(last_log_conf.previous);
        output.rpc_configuration = last_log_conf.current;
    }

    // Advance the observed state.
    _observed.advance(*this);

    // Be careful to do that only after any use of stable_idx() in this
    // function and after any code that may throw
    if (output.log_entries.size()) {
        // We advance stable index before the entries are
        // actually persisted, because if writing to stable storage
        // will fail the FSM will be stopped and get_output() will
        // never be called again, so any new sate that assumes that
        // the entries are stable will not be observed.
        advance_stable_idx(output.log_entries.back()->idx);
    }

    return output;
}

void fsm::advance_stable_idx(index_t idx) {
    index_t prev_stable_idx = _log.stable_idx();
    _log.stable_to(idx);
    logger.trace("advance_stable_idx[{}]: prev_stable_idx={}, idx={}", _my_id, prev_stable_idx, idx);
    if (is_leader()) {
        replicate();
        auto leader_progress = leader_state().tracker.find(_my_id);
        if (leader_progress) {
            // If this server is leader and is part of the current
            // configuration, update it's progress and optionally
            // commit new entries.
            leader_progress->accepted(idx);
            maybe_commit();
        }
    }
}

void fsm::maybe_commit() {

    index_t new_commit_idx = leader_state().tracker.committed(_commit_idx);

    if (new_commit_idx <= _commit_idx) {
        return;
    }
    bool committed_conf_change = _commit_idx < _log.last_conf_idx() &&
        new_commit_idx >= _log.last_conf_idx();

    if (_log[new_commit_idx]->term != _current_term) {

        // 3.6.2 Committing entries from previous terms
        // Raft never commits log entries from previous terms by
        // counting replicas. Only log entries from the leader’s
        // current term are committed by counting replicas; once
        // an entry from the current term has been committed in
        // this way, then all prior entries are committed
        // indirectly because of the Log Matching Property.
        logger.trace("maybe_commit[{}]: cannot commit because of term {} != {}",
            _my_id, _log[new_commit_idx]->term, _current_term);
        return;
    }
    logger.trace("maybe_commit[{}]: commit {}", _my_id, new_commit_idx);

    _commit_idx = new_commit_idx;
    // We have a quorum of servers with match_idx greater than the
    // current commit index. Commit && apply more entries.
    _sm_events.signal();

    if (committed_conf_change) {
        logger.trace("maybe_commit[{}]: committed conf change at idx {}", _my_id, _log.last_conf_idx());
        if (_log.get_configuration().is_joint()) {
            // 4.3. Arbitrary configuration changes using joint consensus
            //
            // Once the joint consensus has been committed, the
            // system then transitions to the new configuration.
            configuration cfg(_log.get_configuration());
            cfg.leave_joint();
            _log.emplace_back(seastar::make_lw_shared<log_entry>({_current_term, _log.next_idx(), std::move(cfg)}));
            leader_state().tracker.set_configuration(_log.get_configuration(), _log.last_idx());
            // Leaving joint configuration may commit more entries
            // even if we had no new acks. Imagine the cluster is
            // in joint configuration {{A, B}, {A, B, C, D, E}}.
            // The leader's view of stable indexes is:
            //
            // Server  Match Index
            // A       5
            // B       5
            // C       6
            // D       7
            // E       8
            //
            // The commit index would be 5 if we use joint
            // configuration, and 6 if we assume we left it. Let
            // it happen without an extra FSM step.
            maybe_commit();
        } else if (leader_state().tracker.find(_my_id) == nullptr) {
            logger.trace("maybe_commit[{}]: stepping down as leader", _my_id);
            // 4.2.2 Removing the current leader
            //
            // The leader temporarily manages a configuration
            // in which it is not a member.
            //
            // A leader that is removed from the configuration
            // steps down once the C_new entry is committed.
            //
            // If the leader stepped down before this point,
            // it might still time out and become leader
            // again, delaying progress.
            transfer_leadership();
        }
    }
}

void fsm::tick_leader() {
    if (election_elapsed() >= ELECTION_TIMEOUT) {
        // 6.2 Routing requests to the leader
        // A leader in Raft steps down if an election timeout
        // elapses without a successful round of heartbeats to a majority
        // of its cluster; this allows clients to retry their requests
        // with another server.
        return become_follower(server_id{});
    }

    auto active =  leader_state().tracker.get_activity_tracker();
    active(_my_id); // +1 for self
    for (auto& [id, progress] : leader_state().tracker) {
        if (progress.id != _my_id) {
            if (_failure_detector.is_alive(progress.id)) {
                active(progress.id);
            }
            switch(progress.state) {
            case follower_progress::state::PROBE:
                // allow one probe to be resent per follower per time tick
                progress.probe_sent = false;
                break;
            case follower_progress::state::PIPELINE:
                if (progress.in_flight == follower_progress::max_in_flight) {
                    progress.in_flight--; // allow one more packet to be sent
                }
                break;
            case follower_progress::state::SNAPSHOT:
                continue;
            }
            if (progress.match_idx < _log.stable_idx() || progress.commit_idx < _commit_idx) {
                logger.trace("tick[{}]: replicate to {} because match={} < stable={} || "
                    "follower commit_idx={} < commit_idx={}",
                    _my_id, progress.id, progress.match_idx, _log.stable_idx(),
                    progress.commit_idx, _commit_idx);

                replicate_to(progress, true);
            }
        }
    }
    if (active) {
        // Advance last election time if we heard from
        // the quorum during this tick.
        _last_election_time = _clock.now();
    }
}

void fsm::tick() {
    _clock.advance();

    auto has_stable_leader = [this]() {
        // We may have received a C_new which does not contain the
        // current leader.  If the configuration is joint, the
        // leader will drive down the transition to C_new even if
        // it is not part of it. But if it is C_new already, it
        // has likely have stepped down.  Since the failure
        // detector may still report the leader node as alive and
        // healthy, we must not apply the stable leader rule
        // in this case.
        const configuration& conf = _log.get_configuration();
        return current_leader() && (conf.is_joint() || conf.current.contains(server_address{current_leader()})) &&
            _failure_detector.is_alive(current_leader());
    };

    if (is_leader()) {
        tick_leader();
    } else if (has_stable_leader()) {
        // Ensure the follower doesn't disrupt a valid leader
        // simply because there were no AppendEntries RPCs recently.
        _last_election_time = _clock.now();
    } else if (is_past_election_timeout()) {
        logger.trace("tick[{}]: becoming a candidate at term {}, last election: {}, now: {}", _my_id,
            _current_term, _last_election_time, _clock.now());
        become_candidate(_config.enable_prevoting);
    }
}

void fsm::append_entries(server_id from, append_request&& request) {
    logger.trace("append_entries[{}] received ct={}, prev idx={} prev term={} commit idx={}, idx={} num entries={}",
            _my_id, request.current_term, request.prev_log_idx, request.prev_log_term,
            request.leader_commit_idx, request.entries.size() ? request.entries[0]->idx : index_t(0), request.entries.size());

    assert(is_follower());
    // 3.4. Leader election
    // A server remains in follower state as long as it receives
    // valid RPCs from a leader.
    _last_election_time = _clock.now();

    // Ensure log matching property, even if we append no entries.
    // 3.5
    // Until the leader has discovered where it and the
    // follower’s logs match, the leader can send
    // AppendEntries with no entries (like heartbeats) to save
    // bandwidth.
    auto [match, term] = _log.match_term(request.prev_log_idx, request.prev_log_term);
    if (!match) {
        logger.trace("append_entries[{}]: no matching term at position {}: expected {}, found {}",
                _my_id, request.prev_log_idx, request.prev_log_term, term);
        // Reply false if log doesn't contain an entry at
        // prevLogIndex whose term matches prevLogTerm (§5.3).
        send_to(from, append_reply{_current_term, _commit_idx, append_reply::rejected{request.prev_log_idx, _log.last_idx()}});
        return;
    }

    // If there are no entries it means that the leader wants
    // to ensure forward progress. Reply with the last index
    // that matches.
    index_t last_new_idx = request.prev_log_idx;

    if (!request.entries.empty()) {
        last_new_idx = _log.maybe_append(std::move(request.entries));
    }

    advance_commit_idx(request.leader_commit_idx);

    send_to(from, append_reply{_current_term, _commit_idx, append_reply::accepted{last_new_idx}});
}

void fsm::append_entries_reply(server_id from, append_reply&& reply) {
    assert(is_leader());

    follower_progress* opt_progress = leader_state().tracker.find(from);
    if (opt_progress == nullptr) {
        // A message from a follower removed from the
        // configuration.
        return;
    }
    follower_progress& progress = *opt_progress;

    if (progress.state == follower_progress::state::PIPELINE) {
        if (progress.in_flight) {
            // in_flight is not precise, so do not let it underflow
            progress.in_flight--;
        }
    }

    if (progress.state == follower_progress::state::SNAPSHOT) {
        logger.trace("append_entries_reply[{}->{}]: ignored in snapshot state", _my_id, from);
        return;
    }

    progress.commit_idx = reply.commit_idx;

    if (std::holds_alternative<append_reply::accepted>(reply.result)) {
        // accepted
        index_t last_idx = std::get<append_reply::accepted>(reply.result).last_new_idx;

        logger.trace("append_entries_reply[{}->{}]: accepted match={} last index={}",
            _my_id, from, progress.match_idx, last_idx);

        progress.accepted(last_idx);

        progress.become_pipeline();

        // If a leader is stepping down, transfer the leadership
        // to a first voting node that has fully replicated log.
        if (leader_state().stepdown && !leader_state().timeout_now_sent &&
                         progress.can_vote && progress.match_idx == _log.last_idx()) {
            send_timeout_now(progress.id);
            // We may have resigned leadership if a stepdown process completed
            // while the leader is no longer part of the configuration.
            if (!is_leader()) {
                return;
            }
        }

        // check if any new entry can be committed
        maybe_commit();

        // The call to maybe_commit() may initiate and immediately complete stepdown process
        // so the comment above the provios is_leader() check applies here too. 
        if (!is_leader()) {
            return;
        }
    } else {
        // rejected
        append_reply::rejected rejected = std::get<append_reply::rejected>(reply.result);

        logger.trace("append_entries_reply[{}->{}]: rejected match={} index={}",
            _my_id, from, progress.match_idx, rejected.non_matching_idx);

        // check reply validity
        if (progress.is_stray_reject(rejected)) {
            logger.trace("append_entries_reply[{}->{}]: drop stray append reject", _my_id, from);
            return;
        }

        // is_stray_reject may return a false negative so even if the check above passes,
        // we may still be dealing with a stray reject. That's fine though; it is always safe
        // to rollback next_idx on a reject and in fact that's what the Raft spec (TLA+) does.
        // Detecting stray rejects is an optimization that should rarely even be needed.

        // Start re-sending from the non matching index, or from
        // the last index in the follower's log.
        // FIXME: make it more efficient
        progress.next_idx = std::min(rejected.non_matching_idx, index_t(rejected.last_idx + 1));

        progress.become_probe();

        // By `is_stray_reject(rejected) == false` we know that `rejected.non_matching_idx > progress.match_idx`
        // and `rejected.last_idx + 1 > progress.match_idx`. By the assignment to `progress.next_idx` above, we get:
        assert(progress.next_idx > progress.match_idx);
    }

    // We may have just applied a configuration that removes this
    // follower, so re-track it.
    opt_progress = leader_state().tracker.find(from);
    if (opt_progress != nullptr) {
        logger.trace("append_entries_reply[{}->{}]: next_idx={}, match_idx={}",
            _my_id, from, opt_progress->next_idx, opt_progress->match_idx);

        replicate_to(*opt_progress, false);
    }
}

void fsm::request_vote(server_id from, vote_request&& request) {

    // We can cast a vote in any state. If the candidate's term is
    // lower than ours, we ignore the request. Otherwise we first
    // update our current term and convert to a follower.
    assert(request.is_prevote || _current_term == request.current_term);

    bool can_vote =
        // We can vote if this is a repeat of a vote we've already cast...
        _voted_for == from ||
        // ...we haven't voted and we don't think there's a leader yet in this term...
        (_voted_for == server_id{} && current_leader() == server_id{}) ||
        // ...this is prevote for a future term...
        // (we will get here if the node does not know any leader yet and already
        //  voted for some other node, but now it get even newer prevote request)
        (request.is_prevote && request.current_term > _current_term);

    // ...and we believe the candidate is up to date.
    if (can_vote && _log.is_up_to_date(request.last_log_idx, request.last_log_term)) {

        logger.trace("{} [term: {}, index: {}, last log term: {}, voted_for: {}] "
            "voted for {} [log_term: {}, log_index: {}]",
            _my_id, _current_term, _log.last_idx(), _log.last_term(), _voted_for,
            from, request.last_log_term, request.last_log_idx);
        if (!request.is_prevote) { // Only record real votes
            // If a server grants a vote, it must reset its election
            // timer. See Raft Summary.
            _last_election_time = _clock.now();
            _voted_for = from;
        }
        // The term in the original message and current local term are the
        // same in the case of regular votes, but different for pre-votes.
        //
        // When responding to {Pre,}Vote messages we include the term
        // from the message, not the local term. To see why, consider the
        // case where a single node was previously partitioned away and
        // its local term is now out of date. If we include the local term
        // (recall that for pre-votes we don't update the local term), the
        // (pre-)campaigning node on the other end will proceed to ignore
        // the message (it ignores all out of date messages).
        send_to(from, vote_reply{request.current_term, true, request.is_prevote});
    } else {
        // If a vote is not granted, this server is a potential
        // viable candidate, so it should not reset its election
        // timer, to avoid election disruption by non-viable
        // candidates.
        logger.trace("{} [term: {}, index: {}, log_term: {}, voted_for: {}] "
            "rejected vote for {} [current_term: {}, log_term: {}, log_index: {}, is_prevote: {}]",
            _my_id, _current_term, _log.last_idx(), _log.last_term(), _voted_for,
            from, request.current_term, request.last_log_term, request.last_log_idx, request.is_prevote);

        send_to(from, vote_reply{_current_term, false, request.is_prevote});
    }
}

void fsm::request_vote_reply(server_id from, vote_reply&& reply) {
    assert(is_candidate());

    logger.trace("{} received a {} vote from {}", _my_id, reply.vote_granted ? "yes" : "no", from);

    auto& state = std::get<candidate>(_state);
    // Should not register a reply to prevote as a real vote
    if (state.is_prevote != reply.is_prevote) {
        return;
    }
    state.votes.register_vote(from, reply.vote_granted);

    switch (state.votes.tally_votes()) {
    case vote_result::UNKNOWN:
        break;
    case vote_result::WON:
        if (state.is_prevote) {
            become_candidate(false);
        } else {
            become_leader();
        }
        break;
    case vote_result::LOST:
        become_follower(server_id{});
        break;
    }
}

static size_t entry_size(const log_entry& e) {
    struct overloaded {
        size_t operator()(const command& c) {
            return c.size();
        }
        size_t operator()(const configuration& c) {
            size_t size = 0;
            for (auto& s : c.current) {
                size += sizeof(s.id);
                size += s.info.size();
            }
            return size;
        }
        size_t operator()(const log_entry::dummy& d) {
            return 0;
        }
    };
    return std::visit(overloaded{}, e.data) + sizeof(e);
}

void fsm::replicate_to(follower_progress& progress, bool allow_empty) {

    logger.trace("replicate_to[{}->{}]: called next={} match={}",
        _my_id, progress.id, progress.next_idx, progress.match_idx);

    while (progress.can_send_to()) {
        index_t next_idx = progress.next_idx;
        if (progress.next_idx > _log.stable_idx()) {
            next_idx = index_t(0);
            logger.trace("replicate_to[{}->{}]: next past stable next={} stable={}, empty={}",
                    _my_id, progress.id, progress.next_idx, _log.stable_idx(), allow_empty);
            if (!allow_empty) {
                // Send out only persisted entries.
                return;
            }
        }

        allow_empty = false; // allow only one empty message

        // A log containing a snapshot, a few trailing entries and
        // a few new entries may look like this:
        // E - log entry
        // S_idx - snapshot index
        // E_i1 E_i2 E_i3 Ei_4 E_i5 E_i6
        //      ^
        //      S_idx = i2
        // If the follower's next_idx is i1 we need to
        // enter snapshot transfer mode even when we have
        // i1 in the log, since it is not possible to get the term of
        // the entry previous to i1 and verify that the follower's tail
        // contains no uncommitted entries.
        index_t prev_idx = progress.next_idx - index_t{1};
        std::optional<term_t> prev_term = _log.term_for(prev_idx);
        if (!prev_term) {
            const snapshot& snapshot = _log.get_snapshot();
            // We need to transfer the snapshot before we can
            // continue syncing the log.
            progress.become_snapshot(snapshot.idx);
            send_to(progress.id, install_snapshot{_current_term, snapshot});
            logger.trace("replicate_to[{}->{}]: send snapshot next={} snapshot={}",
                    _my_id, progress.id, progress.next_idx,  snapshot.idx);
            return;
        }

        append_request req = {
            .current_term = _current_term,
            .leader_id = _my_id,
            .prev_log_idx = prev_idx,
            .prev_log_term = prev_term.value(),
            .leader_commit_idx = _commit_idx,
            .entries = std::vector<log_entry_ptr>()
        };

        if (next_idx) {
            size_t size = 0;
            while (next_idx <= _log.stable_idx() && size < _config.append_request_threshold) {
                const auto& entry = _log[next_idx];
                req.entries.push_back(entry);
                logger.trace("replicate_to[{}->{}]: send entry idx={}, term={}",
                             _my_id, progress.id, entry->idx, entry->term);
                size += entry_size(*entry);
                next_idx++;
                if (progress.state == follower_progress::state::PROBE) {
                    break; // in PROBE mode send only one entry
                }
            }

            if (progress.state == follower_progress::state::PIPELINE) {
                progress.in_flight++;
                // Optimistically update next send index. In case
                // a message is lost there will be negative reply that
                // will re-send idx.
                progress.next_idx = next_idx;
            }
        } else {
            logger.trace("replicate_to[{}->{}]: send empty", _my_id, progress.id);
        }

        send_to(progress.id, std::move(req));

        if (progress.state == follower_progress::state::PROBE) {
             progress.probe_sent = true;
        }
    }
}

void fsm::replicate() {
    assert(is_leader());
    for (auto& [id, progress] : leader_state().tracker) {
        if (progress.id != _my_id) {
            replicate_to(progress, false);
        }
    }
}

bool fsm::can_read() {
    check_is_leader();

    if (_log[_log.last_idx()]->term != _current_term) {
        return false;
    }

    // TODO: for now always return false to let the caller know that
    // applying dummy entry is needed before reading (to confirm the leadership),
    // but in the future we may return true here if we can guaranty leadership
    // by means of a "stable leader" optimization. "Stable leader" ensures that
    // a follower does not vote for other leader if it recently (during a couple
    // of last ticks) heard from existing one, so if the leader is already committed
    // entries during this tick it guaranties that it communicated with
    // majority of nodes and no other leader could have been elected.

    return false;
}

void fsm::install_snapshot_reply(server_id from, snapshot_reply&& reply) {
    follower_progress* opt_progress= leader_state().tracker.find(from);
    // The follower is removed from the configuration.
    if (opt_progress == nullptr) {
        return;
    }
    follower_progress& progress = *opt_progress;

    if (progress.state != follower_progress::state::SNAPSHOT) {
        logger.trace("install_snapshot_reply[{}]: called not in snapshot state", _my_id);
        return;
    }

    // No matter if snapshot transfer failed or not move back to probe state
    progress.become_probe();

    if (reply.success) {
        // If snapshot was successfully transferred start replication immediately
        replicate_to(progress, false);
    }
    // Otherwise wait for a heartbeat. Next attempt will move us to SNAPSHOT state
    // again and snapshot transfer will be attempted one more time.
}

bool fsm::apply_snapshot(snapshot snp, size_t trailing) {
    logger.trace("apply_snapshot[{}]: term: {}, idx: {}", _my_id, _current_term, snp.idx);
    const auto& current_snp = _log.get_snapshot();
    // Uncommitted entries can not appear in the snapshot
    assert(snp.idx <= _commit_idx || is_follower());
    if (snp.idx <= current_snp.idx) {
        logger.error("apply_snapshot[{}]: ignore outdated snapshot {}/{} current one is {}/{}",
                        _my_id, snp.id, snp.idx, current_snp.id, current_snp.idx);
        return false;
    }
    size_t units = _log.apply_snapshot(std::move(snp), trailing);
    if (is_leader()) {
        logger.trace("apply_snapshot[{}]: signal {} available units", _my_id, units);
        leader_state().log_limiter_semaphore.signal(units);
    }
    return true;
}

void fsm::transfer_leadership() {
    check_is_leader();
    leader_state().stepdown = true;
    // Stop new requests from commig in
    leader_state().log_limiter_semaphore.consume(_config.max_log_size);
    // If there is a fully up-to-date voting replica make it start an election
    for (auto&& [_, p] : leader_state().tracker) {
        if (p.id != _my_id && p.can_vote && p.match_idx == _log.last_idx()) {
            send_timeout_now(p.id);
            break;
        }
    }
}

void fsm::send_timeout_now(server_id id) {
    logger.trace("send_timeout_now[{}] send timeout_now to {}", _my_id, id);
    send_to(id, timeout_now{_current_term});
    leader_state().timeout_now_sent = true;
    if (leader_state().tracker.find(_my_id) == nullptr) {
        logger.trace("send_timeout_now[{}] become follower", _my_id);
        become_follower({});
    }
}

void fsm::stop() {
    _sm_events.broken();
}

std::ostream& operator<<(std::ostream& os, const fsm& f) {
    os << "current term: " << f._current_term << ", ";
    os << "current leader: " << f.current_leader() << ", ";
    os << "len messages: " << f._messages.size() << ", ";
    os << "voted for: " << f._voted_for << ", ";
    os << "commit idx:" << f._commit_idx << ", ";
    // os << "last applied: " << f._last_applied << ", ";
    os << "log (" << f._log << "), ";
    os << "observed (current term: " << f._observed._current_term << ", ";
    os << "voted for: " << f._observed._voted_for << ", ";
    os << "commit index: " << f._observed._commit_idx << "), ";
    os << "current time: " << f._clock.now() << ", ";
    os << "last election time: " << f._last_election_time << ", ";
    if (f.is_candidate()) {
        os << "votes (" << f.candidate_state().votes << "), ";
    }
    os << "messages: " << f._messages.size() << ", ";

    if (std::holds_alternative<leader>(f._state)) {
        os << "leader, ";
    } else if (std::holds_alternative<candidate>(f._state)) {
        os << "candidate";
    } else if (std::holds_alternative<follower>(f._state)) {
        os << "follower";
    }
    if (f.is_leader()) {
        os << "followers (";
        for (const auto& [server_id, follower_progress]: f.leader_state().tracker) {
            os << server_id << ", ";
            os << follower_progress.next_idx << ", ";
            os << follower_progress.match_idx << ", ";
            if (follower_progress.state == follower_progress::state::PROBE) {
                os << "PROBE, ";
            } else if (follower_progress.state == follower_progress::state::PIPELINE) {
                os << "PIPELINE, ";
            }
            os << follower_progress.in_flight;
            os << "; ";
        }
        os << ")";

    }
    return os;
}

} // end of namespace raft
