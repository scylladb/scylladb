/*
 * Copyright (C) 2017-present ScyllaDB
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

#pragma once

#include <boost/intrusive/unordered_set.hpp>

#include "utils/small_vector.hh"
#include "mutation_fragment.hh"
#include "mutation_partition.hh"
#include "xx_hasher.hh"

#include "db/timeout_clock.hh"

class cells_range {
    using ids_vector_type = utils::small_vector<column_id, 5>;

    position_in_partition_view _position;
    ids_vector_type _ids;
public:
    using iterator = ids_vector_type::iterator;
    using const_iterator = ids_vector_type::const_iterator;

    cells_range()
        : _position(position_in_partition_view(position_in_partition_view::static_row_tag_t())) { }

    explicit cells_range(position_in_partition_view pos, const row& cells)
        : _position(pos)
    {
        _ids.reserve(cells.size());
        cells.for_each_cell([this] (auto id, auto&&) {
            _ids.emplace_back(id);
        });
    }

    position_in_partition_view position() const { return _position; }
    bool empty() const { return _ids.empty(); }

    auto begin() const { return _ids.begin(); }
    auto end() const { return _ids.end(); }
};

class partition_cells_range {
    const mutation_partition& _mp;
public:
    class iterator {
        const mutation_partition& _mp;
        std::optional<mutation_partition::rows_type::const_iterator> _position;
        cells_range _current;
    public:
        explicit iterator(const mutation_partition& mp)
            : _mp(mp)
            , _current(position_in_partition_view(position_in_partition_view::static_row_tag_t()), mp.static_row().get())
        { }

        iterator(const mutation_partition& mp, mutation_partition::rows_type::const_iterator it)
            : _mp(mp)
            , _position(it)
        { }

        iterator& operator++() {
            if (!_position) {
                _position = _mp.clustered_rows().begin();
            } else {
                ++(*_position);
            }
            if (_position != _mp.clustered_rows().end()) {
                auto it = *_position;
                _current = cells_range(position_in_partition_view(position_in_partition_view::clustering_row_tag_t(), it->key()),
                        it->row().cells());
            }
            return *this;
        }

        iterator operator++(int) {
            iterator it(*this);
            operator++();
            return it;
        }

        cells_range& operator*() {
            return _current;
        }

        cells_range* operator->() {
            return &_current;
        }

        bool operator==(const iterator& other) const {
            return _position == other._position;
        }
        bool operator!=(const iterator& other) const {
            return !(*this == other);
        }
    };
public:
    explicit partition_cells_range(const mutation_partition& mp) : _mp(mp) { }

    iterator begin() const {
        return iterator(_mp);
    }
    iterator end() const {
        return iterator(_mp, _mp.clustered_rows().end());
    }
};

class locked_cell;

struct cell_locker_stats {
    uint64_t lock_acquisitions = 0;
    uint64_t operations_waiting_for_lock = 0;
};

class cell_locker {
private:
    class partition_entry;

    struct cell_address {
        position_in_partition position;
        column_id id;
    };

    class cell_entry : public bi::unordered_set_base_hook<bi::link_mode<bi::auto_unlink>>,
                       public enable_lw_shared_from_this<cell_entry> {
        partition_entry& _parent;
        cell_address _address;
        db::timeout_semaphore _semaphore { 0 };

        friend class cell_locker;
    public:
        cell_entry(partition_entry& parent, position_in_partition position, column_id id)
            : _parent(parent)
            , _address { std::move(position), id }
        { }

        // Upgrades cell_entry to another schema.
        // Changes the value of cell_address, so cell_entry has to be
        // temporarily removed from its parent partition_entry.
        // Returns true if the cell_entry still exist in the new schema and
        // should be reinserted.
        bool upgrade(const schema& from, const schema& to, column_kind kind) noexcept {
            auto& old_column_mapping = from.get_column_mapping();
            auto& column = old_column_mapping.column_at(kind, _address.id);
            auto cdef = to.get_column_definition(column.name());
            if (!cdef) {
                return false;
            }
            _address.id = cdef->id;
            return true;
        }

        const position_in_partition& position() const {
            return _address.position;
        }

        future<> lock(db::timeout_clock::time_point _timeout) {
            return _semaphore.wait(_timeout);
        }
        void unlock() {
            _semaphore.signal();
        }

        ~cell_entry() {
            if (!is_linked()) {
                return;
            }
            unlink();
            if (!--_parent._cell_count) {
                delete &_parent;
            }
        }

        class hasher {
            const schema* _schema; // pointer instead of reference for default assignment
        public:
            explicit hasher(const schema& s) : _schema(&s) { }

            size_t operator()(const cell_address& ca) const {
                xx_hasher hasher;
                ca.position.feed_hash(hasher, *_schema);
                ::feed_hash(hasher, ca.id);
                return static_cast<size_t>(hasher.finalize_uint64());
            }
            size_t operator()(const cell_entry& ce) const {
                return operator()(ce._address);
            }
        };

        class equal_compare {
            position_in_partition::equal_compare _cmp;
        private:
            bool do_compare(const cell_address& a, const cell_address& b) const {
                return a.id == b.id && _cmp(a.position, b.position);
            }
        public:
            explicit equal_compare(const schema& s) : _cmp(s) { }
            bool operator()(const cell_address& ca, const cell_entry& ce) const {
                return do_compare(ca, ce._address);
            }
            bool operator()(const cell_entry& ce, const cell_address& ca) const {
                return do_compare(ca, ce._address);
            }
            bool operator()(const cell_entry& a, const cell_entry& b) const {
                return do_compare(a._address, b._address);
            }
        };
    };

    class partition_entry : public bi::unordered_set_base_hook<bi::link_mode<bi::auto_unlink>> {
        using cells_type = bi::unordered_set<cell_entry,
                                             bi::equal<cell_entry::equal_compare>,
                                             bi::hash<cell_entry::hasher>,
                                             bi::constant_time_size<false>>;

        static constexpr size_t initial_bucket_count = 16;
        using max_load_factor = std::ratio<3, 4>;
        dht::decorated_key _key;
        cell_locker& _parent;
        size_t _rehash_at_size = compute_rehash_at_size(initial_bucket_count);
        std::unique_ptr<cells_type::bucket_type[]> _buckets; // TODO: start with internal storage?
        size_t _cell_count = 0; // cells_type::empty() is not O(1) if the hook is auto-unlink
        cells_type::bucket_type _internal_buckets[initial_bucket_count];
        cells_type _cells;
        schema_ptr _schema;

        friend class cell_entry;
    private:
        static constexpr size_t compute_rehash_at_size(size_t bucket_count) {
            return bucket_count * max_load_factor::num / max_load_factor::den;
        }
        void maybe_rehash() {
            if (_cell_count >= _rehash_at_size) {
                auto new_bucket_count = std::min(_cells.bucket_count() * 2, _cells.bucket_count() + 1024);
                auto buckets = std::make_unique<cells_type::bucket_type[]>(new_bucket_count);

                _cells.rehash(cells_type::bucket_traits(buckets.get(), new_bucket_count));
                _buckets = std::move(buckets);

                _rehash_at_size = compute_rehash_at_size(new_bucket_count);
            }
        }
    public:
        partition_entry(schema_ptr s, cell_locker& parent, const dht::decorated_key& dk)
            : _key(dk)
            , _parent(parent)
            , _cells(cells_type::bucket_traits(_internal_buckets, initial_bucket_count),
                     cell_entry::hasher(*s), cell_entry::equal_compare(*s))
            , _schema(s)
        { }

        ~partition_entry() {
            if (is_linked()) {
                _parent._partition_count--;
            }
        }

        // Upgrades partition entry to new schema. Returns false if all
        // cell_entries has been removed during the upgrade.
        bool upgrade(schema_ptr new_schema);

        void insert(lw_shared_ptr<cell_entry> cell) {
            _cells.insert(*cell);
            _cell_count++;
            maybe_rehash();
        }

        cells_type& cells() {
            return _cells;
        }

        struct hasher {
            size_t operator()(const dht::decorated_key& dk) const {
                return std::hash<dht::decorated_key>()(dk);
            }
            size_t operator()(const partition_entry& pe) const {
                return operator()(pe._key);
            }
        };

        class equal_compare {
            dht::decorated_key_equals_comparator _cmp;
        public:
            explicit equal_compare(const schema& s) : _cmp(s) { }
            bool operator()(const dht::decorated_key& dk, const partition_entry& pe) {
                return _cmp(dk, pe._key);
            }
            bool operator()(const partition_entry& pe, const dht::decorated_key& dk) {
                return _cmp(dk, pe._key);
            }
            bool operator()(const partition_entry& a, const partition_entry& b) {
                return _cmp(a._key, b._key);
            }
        };
    };

    using partitions_type = bi::unordered_set<partition_entry,
                                              bi::equal<partition_entry::equal_compare>,
                                              bi::hash<partition_entry::hasher>,
                                              bi::constant_time_size<false>>;

    static constexpr size_t initial_bucket_count = 4 * 1024;
    using max_load_factor = std::ratio<3, 4>;

    std::unique_ptr<partitions_type::bucket_type[]> _buckets;
    partitions_type _partitions;
    size_t _partition_count = 0;
    size_t _rehash_at_size = compute_rehash_at_size(initial_bucket_count);
    schema_ptr _schema;

    // partitions_type uses equality comparator which keeps a reference to the
    // original schema, we must ensure that it doesn't die.
    schema_ptr _original_schema;
    cell_locker_stats& _stats;

    friend class locked_cell;
private:
    struct locker;

    static constexpr size_t compute_rehash_at_size(size_t bucket_count) {
        return bucket_count * max_load_factor::num / max_load_factor::den;
    }
    void maybe_rehash() {
        if (_partition_count >= _rehash_at_size) {
            auto new_bucket_count = std::min(_partitions.bucket_count() * 2, _partitions.bucket_count() + 64 * 1024);
            auto buckets = std::make_unique<partitions_type::bucket_type[]>(new_bucket_count);

            _partitions.rehash(partitions_type::bucket_traits(buckets.get(), new_bucket_count));
            _buckets = std::move(buckets);

            _rehash_at_size = compute_rehash_at_size(new_bucket_count);
        }
    }
public:
    explicit cell_locker(schema_ptr s, cell_locker_stats& stats)
        : _buckets(std::make_unique<partitions_type::bucket_type[]>(initial_bucket_count))
        , _partitions(partitions_type::bucket_traits(_buckets.get(), initial_bucket_count),
                      partition_entry::hasher(), partition_entry::equal_compare(*s))
        , _schema(s)
        , _original_schema(std::move(s))
        , _stats(stats)
    { }

    ~cell_locker() {
        assert(_partitions.empty());
    }

    void set_schema(schema_ptr s) {
        _schema = s;
    }
    schema_ptr schema() const {
        return _schema;
    }

    // partition_cells_range is required to be in cell_locker::schema()
    future<std::vector<locked_cell>> lock_cells(const dht::decorated_key& dk, partition_cells_range&& range,
                                                db::timeout_clock::time_point timeout);
};


class locked_cell {
    lw_shared_ptr<cell_locker::cell_entry> _entry;
public:
    explicit locked_cell(lw_shared_ptr<cell_locker::cell_entry> entry)
        : _entry(std::move(entry)) { }

    locked_cell(const locked_cell&) = delete;
    locked_cell(locked_cell&&) = default;

    ~locked_cell() {
        if (_entry) {
            _entry->unlock();
        }
    }
};

struct cell_locker::locker {
    cell_entry::hasher _hasher;
    cell_entry::equal_compare _eq_cmp;
    partition_entry& _partition_entry;

    partition_cells_range _range;
    partition_cells_range::iterator _current_ck;
    cells_range::const_iterator _current_cell;

    db::timeout_clock::time_point _timeout;
    std::vector<locked_cell> _locks;
    cell_locker_stats& _stats;
private:
    void update_ck() {
        if (!is_done()) {
            _current_cell = _current_ck->begin();
        }
    }

    future<> lock_next();

    bool is_done() const { return _current_ck == _range.end(); }
public:
    explicit locker(const ::schema& s, cell_locker_stats& st, partition_entry& pe, partition_cells_range&& range, db::timeout_clock::time_point timeout)
        : _hasher(s)
        , _eq_cmp(s)
        , _partition_entry(pe)
        , _range(std::move(range))
        , _current_ck(_range.begin())
        , _timeout(timeout)
        , _stats(st)
    {
        update_ck();
    }

    locker(const locker&) = delete;
    locker(locker&&) = delete;

    future<> lock_all() {
        // Cannot defer before first call to lock_next().
        return lock_next().then([this] {
            return do_until([this] { return is_done(); }, [this] {
                return lock_next();
            });
        });
    }

    std::vector<locked_cell> get() && { return std::move(_locks); }
};

inline
future<std::vector<locked_cell>> cell_locker::lock_cells(const dht::decorated_key& dk, partition_cells_range&& range, db::timeout_clock::time_point timeout) {
    partition_entry::hasher pe_hash;
    partition_entry::equal_compare pe_eq(*_schema);

    auto it = _partitions.find(dk, pe_hash, pe_eq);
    std::unique_ptr<partition_entry> partition;
    if (it == _partitions.end()) {
        partition = std::make_unique<partition_entry>(_schema, *this, dk);
    } else if (!it->upgrade(_schema)) {
        partition = std::unique_ptr<partition_entry>(&*it);
        _partition_count--;
        _partitions.erase(it);
    }

    if (partition) {
        std::vector<locked_cell> locks;
        for (auto&& r : range) {
            if (r.empty()) {
                continue;
            }
            for (auto&& c : r) {
                auto cell = make_lw_shared<cell_entry>(*partition, position_in_partition(r.position()), c);
                _stats.lock_acquisitions++;
                partition->insert(cell);
                locks.emplace_back(std::move(cell));
            }
        }

        if (!locks.empty()) {
            _partitions.insert(*partition.release());
            _partition_count++;
            maybe_rehash();
        }
        return make_ready_future<std::vector<locked_cell>>(std::move(locks));
    }

    auto l = std::make_unique<locker>(*_schema, _stats, *it, std::move(range), timeout);
    auto f = l->lock_all();
    return f.then([l = std::move(l)] {
        return std::move(*l).get();
    });
}

inline
future<> cell_locker::locker::lock_next() {
    while (!is_done()) {
        if (_current_cell == _current_ck->end()) {
            ++_current_ck;
            update_ck();
            continue;
        }

        auto cid = *_current_cell++;

        cell_address ca { position_in_partition(_current_ck->position()), cid };
        auto it = _partition_entry.cells().find(ca, _hasher, _eq_cmp);
        if (it != _partition_entry.cells().end()) {
            _stats.operations_waiting_for_lock++;
            return it->lock(_timeout).then([this, ce = it->shared_from_this()] () mutable {
                _stats.operations_waiting_for_lock--;
                _stats.lock_acquisitions++;
                _locks.emplace_back(std::move(ce));
            });
        }

        auto cell = make_lw_shared<cell_entry>(_partition_entry, position_in_partition(_current_ck->position()), cid);
        _stats.lock_acquisitions++;
        _partition_entry.insert(cell);
        _locks.emplace_back(std::move(cell));
    }
    return make_ready_future<>();
}

inline
bool cell_locker::partition_entry::upgrade(schema_ptr new_schema) {
    if (_schema == new_schema) {
        return true;
    }

    auto buckets = std::make_unique<cells_type::bucket_type[]>(_cells.bucket_count());
    auto cells = cells_type(cells_type::bucket_traits(buckets.get(), _cells.bucket_count()),
                            cell_entry::hasher(*new_schema), cell_entry::equal_compare(*new_schema));

    _cells.clear_and_dispose([&] (cell_entry* cell_ptr) noexcept {
        auto& cell = *cell_ptr;
        auto kind = cell.position().is_static_row() ? column_kind::static_column
                                                    : column_kind::regular_column;
        auto reinsert = cell.upgrade(*_schema, *new_schema, kind);
        if (reinsert) {
            cells.insert(cell);
        } else {
            _cell_count--;
        }
    });

    // bi::unordered_set move assignment is actually a swap.
    // Original _buckets cannot be destroyed before the container using them is
    // so we need to explicitly make sure that the original _cells is no more.
    _cells = std::move(cells);
    auto destroy = [] (auto) { };
    destroy(std::move(cells));

    _buckets = std::move(buckets);
    _schema = new_schema;
    return _cell_count;
}
