/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include "utils/assert.hh"
#include "log.hh"

namespace raft {

log_entry_ptr& log::get_entry(index_t i) {
    return _log[(i - _first_idx).value()];
}

const log_entry_ptr& log::get_entry(index_t i) const {
    return _log[(i - _first_idx).value()];
}

size_t log::range_memory_usage(log_entries::iterator first, log_entries::iterator last) const {
    size_t result = 0;
    for (auto it = first; it != last; ++it) {
        result += memory_usage_of(**it, _max_command_size);
    }
    return result;
}

log_entry_ptr& log::operator[](size_t i) {
    SCYLLA_ASSERT(!_log.empty() && index_t(i) >= _first_idx);
    return get_entry(index_t(i));
}

void log::emplace_back(log_entry_ptr&& e) {
    _log.emplace_back(std::move(e));
    _memory_usage += memory_usage_of(*_log.back(), _max_command_size);
    if (std::holds_alternative<configuration>(_log.back()->data)) {
        _prev_conf_idx = _last_conf_idx;
        _last_conf_idx = last_idx();
    }
}

bool log::empty() const {
    return _log.empty();
}

bool log::is_up_to_date(index_t idx, term_t term) const {
    // 3.6.1 Election restriction
    // Raft determines which of two logs is more up-to-date by comparing the
    // index and term of the last entries in the logs. If the logs have last
    // entries with different terms, then the log with the later term is more
    // up-to-date. If the logs end with the same term, then whichever log is
    // longer is more up-to-date.
    return term > last_term() || (term == last_term() && idx >= last_idx());
}

index_t log::last_idx() const {
    return index_t(_log.size()) + _first_idx - index_t(1);
}

index_t log::next_idx() const {
    return last_idx() + index_t(1);
}

void log::truncate_uncommitted(index_t idx) {
    SCYLLA_ASSERT(idx >= _first_idx);
    auto it = _log.begin() + (idx - _first_idx).value();
    const auto released_memory = range_memory_usage(it, _log.end());
    _log.erase(it, _log.end());
    _log.shrink_to_fit();
    _memory_usage -= released_memory;
    stable_to(std::min(_stable_idx, last_idx()));
    if (_last_conf_idx > last_idx()) {
        // If _prev_conf_idx is 0, this log does not contain any
        // other configuration changes, since no two uncommitted
        // configuration changes can be in progress.
        SCYLLA_ASSERT(_prev_conf_idx < _last_conf_idx);
        _last_conf_idx = _prev_conf_idx;
        _prev_conf_idx = index_t{0};
    }
}

void log::init_last_conf_idx() {
    for (auto it = _log.rbegin(); it != _log.rend() && (**it).idx != _snapshot.idx; ++it) {
        if (std::holds_alternative<configuration>((**it).data)) {
            if (_last_conf_idx == index_t{0}) {
                _last_conf_idx = (**it).idx;
            } else {
                _prev_conf_idx = (**it).idx;
                break;
            }
        }
   }
}

term_t log::last_term() const {
    if (_log.empty()) {
        return _snapshot.term;
    }
    return _log.back()->term;
}

void log::stable_to(index_t idx) {
    SCYLLA_ASSERT(idx <= last_idx());
    _stable_idx = idx;
}

std::pair<bool, term_t> log::match_term(index_t idx, term_t term) const {
    if (idx == index_t{0}) {
        // Special case of empty log on leader,
        // TLA+ line 324.
        return std::make_pair(true, term_t(0));
    }

    // We got an AppendEntries inside out snapshot, it has to much by
    // log matching property
    if (idx < _snapshot.idx) {
        return std::make_pair(true, last_term());
    }

    term_t my_term;

    if (idx == _snapshot.idx) {
        my_term = _snapshot.term;
    } else {
        auto i = idx - _first_idx;

        if (i.value() >= _log.size()) {
            // We have a gap between the follower and the leader.
            return std::make_pair(false, term_t(0));
        }

        my_term =  _log[i.value()]->term;
    }

    return my_term == term ? std::make_pair(true, term_t(0)) : std::make_pair(false, my_term);
}

std::optional<term_t> log::term_for(index_t idx) const {
    if (!_log.empty() && idx >= _first_idx) {
        return _log[(idx - _first_idx).value()]->term;
    }
    if (idx == _snapshot.idx) {
        return _snapshot.term;
    }
    return {};
}

const configuration& log::get_configuration() const {
    return _last_conf_idx ? std::get<configuration>(_log[(_last_conf_idx - _first_idx).value()]->data) : _snapshot.config;
}

const configuration& log::last_conf_for(index_t idx) const {
    SCYLLA_ASSERT(last_idx() >= idx);
    SCYLLA_ASSERT(idx >= _snapshot.idx);

    if (!_last_conf_idx) {
        SCYLLA_ASSERT(!_prev_conf_idx);
        return _snapshot.config;
    }

    if (idx >= _last_conf_idx) {
        return std::get<configuration>(get_entry(_last_conf_idx)->data);
    }

    if (!_prev_conf_idx) {
        // There are no config entries between _snapshot and _last_conf_idx.
        return _snapshot.config;
    }

    if (idx >= _prev_conf_idx) {
        return std::get<configuration>(get_entry(_prev_conf_idx)->data);
    }

    for (; idx > _snapshot.idx; --idx) {
        if (auto cfg = std::get_if<configuration>(&get_entry(idx)->data)) {
            return *cfg;
        }
    }

    return _snapshot.config;
}

index_t log::maybe_append(std::vector<log_entry_ptr>&& entries) {
    SCYLLA_ASSERT(!entries.empty());

    index_t last_new_idx = entries.back()->idx;

    // We must scan through all entries if the log already
    // contains them to ensure the terms match.
    for (auto& e : entries) {
        if (e->idx <= last_idx()) {
            if (e->idx < _first_idx) {
                logger.trace("append_entries: skipping entry with idx {} less than log start {}",
                    e->idx, _first_idx);
                continue;
            }
            if (e->term == get_entry(e->idx)->term) {
                logger.trace("append_entries: entries with index {} has matching terms {}", e->idx, e->term);
                continue;
            }
            logger.trace("append_entries: entries with index {} has non matching terms e.term={}, _log[i].term = {}",
                e->idx, e->term, get_entry(e->idx)->term);
            // If an existing entry conflicts with a new one (same
            // index but different terms), delete the existing
            // entry and all that follow it (§5.3).
            SCYLLA_ASSERT(e->idx > _snapshot.idx);
            truncate_uncommitted(e->idx);
        }
        // Assert log monotonicity
        SCYLLA_ASSERT(e->idx == next_idx());
        emplace_back(std::move(e));
    }

    return last_new_idx;
}

const configuration* log::get_prev_configuration() const {
    if (_prev_conf_idx) {
        return &std::get<configuration>(get_entry(_prev_conf_idx)->data);
    }

    if (_last_conf_idx > _snapshot.idx) {
        return &_snapshot.config;
    }

    // _last_conf_idx <= _snapshot.idx means we only have the last configuration (from the snapshot).
    return nullptr;
}

std::tuple<size_t, index_t> log::apply_snapshot(snapshot_descriptor&& snp, size_t max_trailing_entries,
                                                size_t max_trailing_bytes) {
    SCYLLA_ASSERT (snp.idx > _snapshot.idx);

    size_t released_memory;
    auto idx = snp.idx;

    if (idx > last_idx()) {
        // Remove all entries ignoring 'trailing' arguments,
        // since otherwise there would be a gap between old
        // entries and the next entry index.
        released_memory = std::exchange(_memory_usage, 0);
        _log.clear();
        _log.shrink_to_fit();
        _first_idx = idx + index_t{1};
    } else {
        auto entries_to_remove = _log.size() - (last_idx() - idx).value();
        size_t trailing_bytes = 0;
        for (size_t i = 0; i < max_trailing_entries && entries_to_remove > 0; ++i) {
            trailing_bytes += memory_usage_of(*_log[entries_to_remove - 1], _max_command_size);
            if (trailing_bytes > max_trailing_bytes) {
                break;
            }
            --entries_to_remove;
        }
        released_memory = range_memory_usage(_log.begin(), _log.begin() + entries_to_remove);
        _log.erase(_log.begin(), _log.begin() + entries_to_remove);
        _log.shrink_to_fit();
        _memory_usage -= released_memory;
        _first_idx = _first_idx + index_t{entries_to_remove};
    }

    _stable_idx = std::max(idx, _stable_idx);

    if (idx >= _prev_conf_idx) {
        // The log cannot be truncated beyond snapshot index, so
        // if previous config index is smaller we can forget it.
        _prev_conf_idx = index_t{0};
        if (idx >= _last_conf_idx) {
            // If last config index is included in the snapshot
            // use the config from the snapshot as last one
            _last_conf_idx = index_t{0};
        }
    }

    _snapshot = std::move(snp);

    return {released_memory, _first_idx};
}

} // end of namespace raft

auto fmt::formatter<raft::log>::format(const raft::log& log, fmt::format_context& ctx) const
    -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "first idx: {}, last idx: {}, next idx: {}, stable idx: {}, last term: {}",
                          log._first_idx, log.last_idx(), log.next_idx(), log.stable_idx(), log.last_term());
}
