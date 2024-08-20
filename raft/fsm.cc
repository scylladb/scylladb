/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include "fsm.hh"
#include <random>
#include <seastar/core/coroutine.hh>
#include "utils/assert.hh"
#include "utils/error_injection.hh"

namespace raft {

leader::~leader() {
    if (log_limiter_semaphore) {
        log_limiter_semaphore->broken(not_a_leader(fsm.current_leader()));
    }
}

fsm::fsm(server_id id, term_t current_term, server_id voted_for, log log,
        index_t commit_idx, failure_detector& failure_detector, fsm_config config,
        seastar::condition_variable& sm_events) :
        _my_id(id), _current_term(current_term), _voted_for(voted_for),
        _log(std::move(log)), _failure_detector(failure_detector), _config(config), _sm_events(sm_events) {
    if (id == raft::server_id{}) {
        throw std::invalid_argument("raft::fsm: raft instance cannot have id zero");
    }
    // The snapshot can not contain uncommitted entries
    _commit_idx = _log.get_snapshot().idx;
    _observed.advance(*this);
    // After we observed the state advance commit_idx to persisted one (if provided)
    // so that the log can be replayed
    _commit_idx = std::max(_commit_idx, commit_idx);
    logger.trace("fsm[{}]: starting, current term {}, log length {}, commit index {}", _my_id, _current_term, _log.last_idx(), _commit_idx);

    // Init timeout settings
    if (_log.get_configuration().current.size() == 1 && _log.get_configuration().can_vote(_my_id)) {
        become_candidate(_config.enable_prevoting);
    } else {
        reset_election_timeout();
    }
}

future<semaphore_units<>> fsm::wait_for_memory_permit(seastar::abort_source* as, size_t size) {
    check_is_leader();

    auto& sm = *leader_state().log_limiter_semaphore;
    return as ? get_units(sm, size, *as) : get_units(sm, size);
}

const configuration& fsm::get_configuration() const {
    return _log.get_configuration();
}

template<typename T>
const log_entry& fsm::add_entry(T command) {
    // It's only possible to add entries on a leader.
    check_is_leader();
    if (leader_state().stepdown) {
        // A leader that is stepping down should not add new entries
        // to its log (see 3.10), but it still does not know who the new
        // leader will be.
        throw not_a_leader({});
    }

    if constexpr (std::is_same_v<T, configuration>) {
        // Do not permit changes which would render the cluster
        // unusable, such as transitioning to an empty configuration or
        // one with no voters.
        raft::configuration::check(command.current);
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
            logger.trace("[{}] A{}configuration change at index {} is not yet committed (config {}) (commit_idx: {})",
                _my_id, _log.get_configuration().is_joint() ? " joint " : " ",
                _log.last_conf_idx(), _log.get_configuration(), _commit_idx);
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

        logger.trace("[{}] appending joint config entry at {}: {}", _my_id, _log.next_idx(), command);
    }

    utils::get_local_injector().inject("fsm::add_entry/test-failure",
                                       [] { throw std::runtime_error("fsm::add_entry/test-failure"); });

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

    return *_log[_log.last_idx().value()];
}

template const log_entry& fsm::add_entry(command command);
template const log_entry& fsm::add_entry(configuration command);
template const log_entry& fsm::add_entry(log_entry::dummy dummy);

void fsm::advance_commit_idx(index_t leader_commit_idx) {

    auto new_commit_idx = std::min(leader_commit_idx, _log.last_idx());

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
    SCYLLA_ASSERT(_current_term < current_term);
    _current_term = current_term;
    _voted_for = server_id{};
}

void fsm::reset_election_timeout() {
    static thread_local std::default_random_engine re{std::random_device{}()};
    static thread_local std::uniform_int_distribution<> dist;
    // Timeout within range of [1, conf size]
    _randomized_election_timeout = ELECTION_TIMEOUT + logical_clock::duration{dist(re,
            std::uniform_int_distribution<int>::param_type{1,
                    std::max((size_t) ELECTION_TIMEOUT.count(),
                            _log.get_configuration().current.size())})};
}

void fsm::become_leader() {
    SCYLLA_ASSERT(!std::holds_alternative<leader>(_state));
    _output.state_changed = true;
    _state.emplace<leader>(_config.max_log_size, *this);

    // The semaphore is not used on the follower, so the limit could
    // be temporarily exceeded here, and the value of
    // the counter in the semaphore could become negative.
    // This is not a problem though as applier_fiber triggers a snapshot
    // if memory usage approaches the limit.
    // As _applied_idx moves forward, snapshots will eventually release
    // sufficient memory for at least one waiter (add_entry) to proceed.
    // The amount of memory used by log::apply_snapshot for trailing items
    // is limited by the condition
    // _config.snapshot_trailing_size <= _config.max_log_size - max_command_size,
    // which means that at least one command will eventually return from semaphore::wait.
    leader_state().log_limiter_semaphore->consume(_log.memory_usage());

    _last_election_time = _clock.now();
    _ping_leader = false;
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
}

void fsm::become_follower(server_id leader) {
    if (leader == _my_id) {
        on_internal_error(logger, "fsm cannot become a follower of itself");
    }
    if (!std::holds_alternative<follower>(_state)) {
        _output.state_changed = true;
    }
    // Note that current state should be destroyed only after the new one is
    // assigned. The exchange here guarantis that.
    std::exchange(_state, follower{.current_leader = leader});
    if (leader != server_id{}) {
        _ping_leader = false;
        _last_election_time = _clock.now();
    }
}

void fsm::become_candidate(bool is_prevote, bool is_leadership_transfer) {
    if (!std::holds_alternative<candidate>(_state)) {
        _output.state_changed = true;
    }
    // When starting a campaign we need to reset current leader otherwise
    // disruptive server prevention will stall an election if quorum of nodes
    // start election together since each one will ignore vote requests from others

    // Note that current state should be destroyed only after the new one is
    // assigned. The exchange here guarantis that.
    std::exchange(_state, candidate(_log.get_configuration(), is_prevote));

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
    if (!voters.contains(_my_id)) {
        // We're not a voter in the current configuration (perhaps we completely left it).
        //
        // But sometimes, if we're transitioning between configurations
        // such that we were a voter in the previous configuration, we may still need
        // to become a candidate: the new configuration may be unable to proceed without us.
        //
        // For example, if Cold = {A, B}, Cnew = {B}, A is a leader, switching from Cold to Cnew,
        // and Cnew wasn't yet received by B, then B won't be able to win an election:
        // B will ask A for a vote because it is still in the joint configuration
        // and A won't grant it because B has a shorter log. A is the only node
        // that can become a leader at this point.
        //
        // However we can easily determine when we don't need to become a candidate:
        // If Cnew is already committed, that means that a quorum in Cnew had to accept
        // the Cnew entry, so there is a quorum in Cnew that can proceed on their own.
        //
        // Ref. Raft PhD 4.2.2.
        if (_log.last_conf_idx() <= _commit_idx) {
            // Cnew already committed, no need to become a candidate.
            become_follower(server_id{});
            return;
        }

        // The last configuration is not committed yet.
        // This means we must still have access to the previous configuration.
        // Become a candidate only if we were previously a voter.
        auto prev_cfg = _log.get_prev_configuration();
        SCYLLA_ASSERT(prev_cfg);
        if (!prev_cfg->can_vote(_my_id)) {
            // We weren't a voter before.
            become_follower(server_id{});
            return;
        }
    }

    term_t term{_current_term + term_t{1}};
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
            logger.trace("become_candidate[{}] won prevote", _my_id);
            become_candidate(false);
        } else {
            logger.trace("become_candidate[{}] won vote", _my_id);
            become_leader();
        }
    }
}

bool fsm::has_output() const {
    logger.trace("fsm::has_output() {} stable index: {} last index: {}",
        _my_id, _log.stable_idx(), _log.last_idx());

    auto diff = _log.last_idx() - _log.stable_idx();

    return diff > index_t{0} || !_messages.empty() || !_observed.is_equal(*this) || _output.max_read_id_with_quorum
        || (is_leader() && leader_state().last_read_id_changed) || _output.snp || !_output.snps_to_drop.empty()
        || _output.state_changed;
}

fsm_output fsm::get_output() {
    auto diff = _log.last_idx() - _log.stable_idx();

    if (is_leader()) {
        // send delayed read quorum requests if any
        if (leader_state().last_read_id_changed) {
            broadcast_read_quorum(leader_state().last_read_id);
            leader_state().last_read_id_changed = false;
        }
        // replicate accumulated entries
        if (diff) {
            replicate();
        }
    }

    fsm_output output = std::exchange(_output, fsm_output{});

    if (diff.value() > 0) {
        output.log_entries.reserve(diff.value());

        for (auto i = _log.stable_idx() + index_t{1}; i <= _log.last_idx(); i++) {
            // Copy before saving to storage to prevent races with log updates,
            // e.g. truncation of the log.
            // TODO: avoid copies by making sure log truncate is
            // copy-on-write.
            output.log_entries.emplace_back(_log[i.value()]);
        }
    }

    if (_observed._current_term != _current_term || _observed._voted_for != _voted_for) {
        output.term_and_vote = {_current_term, _voted_for};
    }

    // Return committed entries.
    // Observer commit index may be smaller than snapshot index
    // in which case we should not attempt committing entries belonging
    // to a snapshot.
    auto observed_ci =  std::max(_observed._commit_idx, _log.get_snapshot().idx);
    if (observed_ci < _commit_idx) {
        output.committed.reserve((_commit_idx - observed_ci).value());

        for (auto idx = observed_ci + index_t{1}; idx <= _commit_idx; ++idx) {
            const auto& entry = _log[idx.value()];
            output.committed.push_back(entry);
        }
    }

    // Get a snapshot of all unsent messages.
    // Do it after populating log_entries and committed arrays
    // to not lose messages in case arrays population throws
    std::swap(output.messages, _messages);

    // Get status of leadership transfer (if any)
    output.abort_leadership_transfer = std::exchange(_abort_leadership_transfer, false);

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
        output.configuration = last_log_conf.current;
    }

    // Advance the observed state.
    _observed.advance(*this);

    // Be careful to do that only after any use of stable_idx() in this
    // function and after any code that may throw
    if (output.log_entries.size()) {
        // We advance stable index before the entries are
        // actually persisted, because if writing to stable storage
        // will fail the FSM will be stopped and get_output() will
        // never be called again, so any new state that assumes that
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

    if (_log[new_commit_idx.value()]->term != _current_term) {

        // 3.6.2 Committing entries from previous terms
        // Raft never commits log entries from previous terms by
        // counting replicas. Only log entries from the leader’s
        // current term are committed by counting replicas; once
        // an entry from the current term has been committed in
        // this way, then all prior entries are committed
        // indirectly because of the Log Matching Property.
        logger.trace("maybe_commit[{}]: cannot commit because of term {} != {}",
            _my_id, _log[new_commit_idx.value()]->term, _current_term);
        return;
    }
    logger.trace("maybe_commit[{}]: commit {}", _my_id, new_commit_idx);

    _commit_idx = new_commit_idx;
    // We have a quorum of servers with match_idx greater than the
    // current commit index. Commit && apply more entries.
    _sm_events.signal();

    if (committed_conf_change) {
        logger.trace("maybe_commit[{}]: committed conf change at idx {} (config: {})", _my_id, _log.last_conf_idx(), _log.get_configuration());
        if (_log.get_configuration().is_joint()) {
            // 4.3. Arbitrary configuration changes using joint consensus
            //
            // Once the joint consensus has been committed, the
            // system then transitions to the new configuration.
            configuration cfg(_log.get_configuration());
            cfg.leave_joint();
            logger.trace("[{}] appending non-joint config entry at {}: {}", _my_id, _log.next_idx(), cfg);
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
        } else {
            auto lp = leader_state().tracker.find(_my_id);
            if (lp == nullptr || !lp->can_vote) {
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
        if (is_leader() && leader_state().last_read_id != leader_state().max_read_id_with_quorum) {
            // Since after reconfiguration the quorum will be calculated based on a new config
            // old reads may never get the quorum. Think about reconfiguration from {A, B, C} to
            // {A, D, E}. Since D, E never got read_quorum request they will never reply, so the
            // read will be stuck at least till leader tick. Re-broadcast last request here to expedite
            // its completion
            broadcast_read_quorum(leader_state().last_read_id);
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

    auto& state = leader_state();
    auto active = state.tracker.get_activity_tracker();
    active(_my_id); // +1 for self
    for (auto& [id, progress] : state.tracker) {
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
            if (progress.match_idx < _log.last_idx() || progress.commit_idx < _commit_idx) {
                logger.trace("tick[{}]: replicate to {} because match={} < last_idx={} || "
                    "follower commit_idx={} < commit_idx={}",
                    _my_id, progress.id, progress.match_idx, _log.last_idx(),
                    progress.commit_idx, _commit_idx);

                replicate_to(progress, true);
            }
        }
    }
    if (state.last_read_id != state.max_read_id_with_quorum) {
        // Re-send last read barrier to ensure forward progress in the face of packet loss
        broadcast_read_quorum(state.last_read_id);
    }
    if (active) {
        // Advance last election time if we heard from
        // the quorum during this tick.
        _last_election_time = _clock.now();
    }

    if (state.stepdown) {
        logger.trace("tick[{}]: stepdown is active", _my_id);
        auto me = leader_state().tracker.find(_my_id);
        if (me == nullptr || !me->can_vote) {
            logger.trace("tick[{}]: not aborting stepdown because we have been removed from the configuration", _my_id);
            // Do not abort stepdown if not part of the current
            // config or non voting member since the node cannot
            // be a leader any longer
        } else if (*state.stepdown <= _clock.now()) {
            logger.trace("tick[{}]: cancel stepdown", _my_id);
            // Cancel stepdown (only if the leader is part of the cluster)
            leader_state().log_limiter_semaphore->signal(_config.max_log_size);
            state.stepdown.reset();
            state.timeout_now_sent.reset();
            _abort_leadership_transfer = true;
            _sm_events.signal(); // signal to handle aborting of leadership transfer
        } else if (state.timeout_now_sent) {
            logger.trace("tick[{}]: resend timeout_now", _my_id);
            // resend timeout now in case it was lost
            send_to(*state.timeout_now_sent, timeout_now{_current_term});
        }
    }
}

void fsm::tick() {
    _clock.advance();

    auto has_stable_leader = [this]() {
        // A leader that is not voting member of a current configuration
        // has likely have stepped down.  Since the failure
        // detector may still report the leader node as alive and
        // healthy, we must not apply the stable leader rule
        // in this case.
        const configuration& conf = _log.get_configuration();
        return current_leader() && conf.can_vote(current_leader()) &&
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

    if (is_follower() && !current_leader() && _ping_leader) {
        // We are a follower but a leader is not known. It will not be known
        // until a communication from a leader which (for an idle leader) may
        // not happen any time soon since we use external failure detector and
        // our leader does not send periodic empty append messages. By sending
        // a special append message reject we solicit a reply from a leader.
        // Non leaders will ignore the append reply.
        auto& cfg = get_configuration();
        // If conf is joint it means a leader will send us a non joint one eventually
        if (!cfg.is_joint() && cfg.current.contains(_my_id)) {
            for (auto s : cfg.current) {
                if (s.can_vote && s.addr.id != _my_id && _failure_detector.is_alive(s.addr.id)) {
                    logger.trace("tick[{}]: searching for a leader. Pinging {}", _my_id, s.addr.id);
                    send_to(s.addr.id, append_reply{_current_term, _commit_idx, append_reply::rejected{index_t{0}, index_t{0}}});
                }
            }
        }
    }
}

void fsm::append_entries(server_id from, append_request&& request) {
    logger.trace("append_entries[{}] received ct={}, prev idx={} prev term={} commit idx={}, idx={} num entries={}",
            _my_id, request.current_term, request.prev_log_idx, request.prev_log_term,
            request.leader_commit_idx, request.entries.size() ? request.entries[0]->idx : index_t(0), request.entries.size());

    SCYLLA_ASSERT(is_follower());

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

    // Do not advance commit index further than last_new_idx, or we could incorrectly
    // mark outdated entries as committed (see #9965).
    advance_commit_idx(std::min(request.leader_commit_idx, last_new_idx));

    send_to(from, append_reply{_current_term, _commit_idx, append_reply::accepted{last_new_idx}});
}

void fsm::append_entries_reply(server_id from, append_reply&& reply) {
    SCYLLA_ASSERT(is_leader());

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

    progress.commit_idx = std::max(progress.commit_idx, reply.commit_idx);

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

        // If non_matching_idx and last_idx are zero it means that a follower is looking for a leader
        // as such message cannot be a result of real mismatch.
        // Send an empty append message to notify it that we are the leader
        if (rejected.non_matching_idx == index_t{0} && rejected.last_idx == index_t{0}) {
            logger.trace("append_entries_reply[{}->{}]: send empty append message", _my_id, from);
            replicate_to(progress, true);
            return;
        }

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
        progress.next_idx = std::min(rejected.non_matching_idx, rejected.last_idx + index_t{1});

        progress.become_probe();

        // By `is_stray_reject(rejected) == false` we know that `rejected.non_matching_idx > progress.match_idx`
        // and `rejected.last_idx + 1 > progress.match_idx`. By the assignment to `progress.next_idx` above, we get:
        SCYLLA_ASSERT(progress.next_idx > progress.match_idx);
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
    SCYLLA_ASSERT(request.is_prevote || _current_term == request.current_term);

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
    SCYLLA_ASSERT(is_candidate());

    logger.trace("request_vote_reply[{}] received a {} vote from {}", _my_id, reply.vote_granted ? "yes" : "no", from);

    auto& state = std::get<candidate>(_state);
    // Should not register a reply to prevote as a real vote
    if (state.is_prevote != reply.is_prevote) {
        logger.trace("request_vote_reply[{}] ignoring prevote from {} as state is vote", _my_id, from);
        return;
    }
    state.votes.register_vote(from, reply.vote_granted);

    switch (state.votes.tally_votes()) {
    case vote_result::UNKNOWN:
        break;
    case vote_result::WON:
        if (state.is_prevote) {
            logger.trace("request_vote_reply[{}] won prevote", _my_id);
            become_candidate(false);
        } else {
            logger.trace("request_vote_reply[{}] won vote", _my_id);
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
                size += sizeof(s.addr.id);
                size += s.addr.info.size();
                size += sizeof(s.can_vote);
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
        if (progress.next_idx > _log.last_idx()) {
            next_idx = index_t(0);
            logger.trace("replicate_to[{}->{}]: next past last next={} stable={}, empty={}",
                    _my_id, progress.id, progress.next_idx, _log.last_idx(), allow_empty);
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
            const snapshot_descriptor& snapshot = _log.get_snapshot();
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
            .prev_log_idx = prev_idx,
            .prev_log_term = prev_term.value(),
            .leader_commit_idx = _commit_idx,
            .entries = std::vector<log_entry_ptr>()
        };

        if (next_idx) {
            size_t size = 0;
            while (next_idx <= _log.last_idx() && size < _config.append_request_threshold) {
                const auto& entry = _log[next_idx.value()];
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
    SCYLLA_ASSERT(is_leader());
    for (auto& [id, progress] : leader_state().tracker) {
        if (progress.id != _my_id) {
            replicate_to(progress, false);
        }
    }
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

bool fsm::apply_snapshot(snapshot_descriptor snp, size_t max_trailing_entries, size_t max_trailing_bytes, bool local) {
    logger.trace("apply_snapshot[{}]: current term: {}, term: {}, idx: {}, id: {}, local: {}",
            _my_id, _current_term, snp.term, snp.idx, snp.id, local);
    // If the snapshot is locally generated, all entries up to its index must have been locally applied,
    // so in particular they must have been observed as committed.
    // Remote snapshots are only applied if we're a follower.
    SCYLLA_ASSERT((local && snp.idx <= _observed._commit_idx) || (!local && is_follower()));

    // We don't apply snapshots older than the last applied one.
    // Furthermore, for remote snapshots, we can *only* apply them if they are fresher than our commit index.
    // Applying older snapshots could result in out-of-order command application to the replicated state machine,
    // leading to serializability violations.
    const auto& current_snp = _log.get_snapshot();
    if (snp.idx <= current_snp.idx || (!local && snp.idx <= _commit_idx)) {
        logger.error("apply_snapshot[{}]: ignore outdated snapshot {}/{} current one is {}/{}, commit_idx={}",
                        _my_id, snp.id, snp.idx, current_snp.id, current_snp.idx, _commit_idx);
        _output.snps_to_drop.push_back(snp.id);
        return false;
    }

    _output.snps_to_drop.push_back(current_snp.id);

    // If the snapshot is local, _commit_idx is larger than snp.idx.
    // Otherwise snp.idx becomes the new commit index.
    _commit_idx = std::max(_commit_idx, snp.idx);
    const auto [units, new_first_index] = _log.apply_snapshot(std::move(snp), max_trailing_entries, max_trailing_bytes);
    _output.snp.emplace(fsm_output::applied_snapshot{
        .snp = _log.get_snapshot(),
        .is_local = local,
        .preserved_log_entries = _log.get_snapshot().idx.value() + 1 - new_first_index.value()});
    if (is_leader()) {
        logger.trace("apply_snapshot[{}]: signal {} available units", _my_id, units);
        leader_state().log_limiter_semaphore->signal(units);
    }
    _sm_events.signal();
    return true;
}

void fsm::transfer_leadership(logical_clock::duration timeout) {
    check_is_leader();
    auto leader = leader_state().tracker.find(_my_id);
    if (configuration::voter_count(get_configuration().current) == 1 && leader && leader->can_vote) {
        // If there is only one voter and it is this node we cannot have another node
        // to transfer leadership to
        throw raft::no_other_voting_member();
    }

    leader_state().stepdown = _clock.now() + timeout;
    // Stop new requests from coming in
    leader_state().log_limiter_semaphore->consume(_config.max_log_size);
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
    leader_state().timeout_now_sent = id;
    auto me = leader_state().tracker.find(_my_id);
    if (me == nullptr || !me->can_vote) {
        logger.trace("send_timeout_now[{}] become follower", _my_id);
        become_follower({});
    }
}

void fsm::broadcast_read_quorum(read_id id) {
    logger.trace("broadcast_read_quorum[{}] send read id {}", _my_id, id);
    for (auto&& [_, p] : leader_state().tracker) {
        if (p.can_vote) {
            if (p.id == _my_id) {
                handle_read_quorum_reply(_my_id, read_quorum_reply{_current_term, _commit_idx, id});
            } else {
                send_to(p.id, read_quorum{_current_term, std::min(p.match_idx, _commit_idx), id});
            }
        }
    }
}

void fsm::handle_read_quorum_reply(server_id from, const read_quorum_reply& reply) {
    SCYLLA_ASSERT(is_leader());
    logger.trace("handle_read_quorum_reply[{}] got reply from {} for id {}", _my_id, from, reply.id);
    auto& state = leader_state();
    follower_progress* progress = state.tracker.find(from);
    if (progress == nullptr) {
        // A message from a follower removed from the
        // configuration.
        return;
    }
    progress->commit_idx = std::max(progress->commit_idx, reply.commit_idx);
    progress->max_acked_read = std::max(progress->max_acked_read, reply.id);

    if (reply.id <= state.max_read_id_with_quorum) {
        // We already have a quorum for a more resent id, so no need to recalculate
        return;
    }

    read_id new_committed_read = leader_state().tracker.committed(state.max_read_id_with_quorum);

    if (new_committed_read <= state.max_read_id_with_quorum) {
        return; // nothing new is committed
    }

    _output.max_read_id_with_quorum = state.max_read_id_with_quorum = new_committed_read;

    logger.trace("handle_read_quorum_reply[{}] new commit read {}", _my_id, new_committed_read);

    _sm_events.signal();
}

std::optional<std::pair<read_id, index_t>> fsm::start_read_barrier(server_id requester) {
    check_is_leader();

    // Make sure that only a leader or a not that is part of the config can request read barrier
    // Nodes outside of the config may never get the data, so they will not be able to read it.
    if (requester != _my_id && leader_state().tracker.find(requester) == nullptr) {
        throw std::runtime_error("Read barrier requested by a node outside of the configuration");
    }

    auto term_for_commit_idx = _log.term_for(_commit_idx);
    SCYLLA_ASSERT(term_for_commit_idx);

    if (*term_for_commit_idx != _current_term) {
        return {};
    }

    read_id id = next_read_id();
    logger.trace("start_read_barrier[{}] starting read barrier with id {}", _my_id, id);
    return std::make_pair(id, _commit_idx);
}

void fsm::stop() {
    if (is_leader()) {
        // Become follower to stop accepting requests
        // (in particular, abort waits on log_limiter_semaphore and prevent new ones).
        become_follower({});
    }
}

} // end of namespace raft

auto fmt::formatter<raft::fsm>::format(const raft::fsm& f, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    auto out = ctx.out();
    out = fmt::format_to(out, "current term: {}, current leader: {}, len messages: {}, voted_for: {}, commit idx: {}, log ({}), ",
               f._current_term, f.current_leader(), f._messages.size(), f._voted_for, f._commit_idx, f._log);
    out = fmt::format_to(out, "observed (current term: {}, voted for {}, commit index: {}), ",
               f._observed._current_term, f._observed._voted_for, f._observed._commit_idx);
    out = fmt::format_to(out, "current time: {}, last election time: {}, ",
               f._clock.now(), f._last_election_time);
    if (f.is_candidate()) {
        out = fmt::format_to(out, "votes ({}), ", f.candidate_state().votes);
    }
    out = fmt::format_to(out, "messages: {}, ", f._messages.size());
    if (std::holds_alternative<raft::leader>(f._state)) {
        out = fmt::format_to(out, "leader, ");
    } else if (std::holds_alternative<raft::candidate>(f._state)) {
        out = fmt::format_to(out, "candidate");
    } else if (std::holds_alternative<raft::follower>(f._state)) {
        out = fmt::format_to(out, "follower");
    }
    if (f.is_leader()) {
        out = fmt::format_to(out, "followers (");
        for (const auto& [server_id, follower_progress]: f.leader_state().tracker) {
            out = fmt::format_to(out, "{}, {}, {}, ", server_id, follower_progress.next_idx, follower_progress.match_idx);
            if (follower_progress.state == raft::follower_progress::state::PROBE) {
                out = fmt::format_to(out, "PROBE, ");
            } else if (follower_progress.state == raft::follower_progress::state::PIPELINE) {
                out = fmt::format_to(out, "PIPELINE, ");
            }
            out = fmt::format_to(out, "{}; ", follower_progress.in_flight);
        }
        out = fmt::format_to(out, ")");
    }
    return out;
}
