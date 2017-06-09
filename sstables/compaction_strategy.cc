/*
 * Copyright (C) 2016 ScyllaDB
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
 */

#include <vector>
#include <chrono>

#include "sstables.hh"
#include "compaction.hh"
#include "database.hh"
#include "compaction_strategy.hh"
#include "schema.hh"
#include "cql3/statements/property_definitions.hh"
#include "leveled_manifest.hh"
#include "sstable_set.hh"
#include "compatible_ring_position.hh"
#include <boost/range/algorithm/find.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/icl/interval_map.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>
#include "date_tiered_compaction_strategy.hh"

logging::logger date_tiered_manifest::logger = logging::logger("DateTieredCompactionStrategy");
logging::logger leveled_manifest::logger("LeveledManifest");

namespace sstables {

extern logging::logger clogger;

class incremental_selector_impl {
public:
    virtual ~incremental_selector_impl() {}
    virtual std::pair<dht::token_range, std::vector<shared_sstable>> select(const dht::token& token) = 0;
};

class sstable_set_impl {
public:
    virtual ~sstable_set_impl() {}
    virtual std::unique_ptr<sstable_set_impl> clone() const = 0;
    virtual std::vector<shared_sstable> select(const dht::partition_range& range) const = 0;
    virtual void insert(shared_sstable sst) = 0;
    virtual void erase(shared_sstable sst) = 0;
    virtual std::unique_ptr<incremental_selector_impl> make_incremental_selector() const = 0;
};

sstable_set::sstable_set(std::unique_ptr<sstable_set_impl> impl, lw_shared_ptr<sstable_list> all)
        : _impl(std::move(impl))
        , _all(std::move(all)) {
}

sstable_set::sstable_set(const sstable_set& x)
        : _impl(x._impl->clone())
        , _all(make_lw_shared(sstable_list(*x._all))) {
}

sstable_set::sstable_set(sstable_set&&) noexcept = default;

sstable_set&
sstable_set::operator=(const sstable_set& x) {
    if (this != &x) {
        auto tmp = sstable_set(x);
        *this = std::move(tmp);
    }
    return *this;
}

sstable_set&
sstable_set::operator=(sstable_set&&) noexcept = default;

std::vector<shared_sstable>
sstable_set::select(const dht::partition_range& range) const {
    return _impl->select(range);
}

void
sstable_set::insert(shared_sstable sst) {
    _impl->insert(sst);
    try {
        _all->insert(sst);
    } catch (...) {
        _impl->erase(sst);
        throw;
    }
}

void
sstable_set::erase(shared_sstable sst) {
    _impl->erase(sst);
    _all->erase(sst);
}

sstable_set::~sstable_set() = default;

sstable_set::incremental_selector::incremental_selector(std::unique_ptr<incremental_selector_impl> impl)
    : _impl(std::move(impl)) {
}

sstable_set::incremental_selector::~incremental_selector() = default;

sstable_set::incremental_selector::incremental_selector(sstable_set::incremental_selector&&) noexcept = default;

const std::vector<shared_sstable>&
sstable_set::incremental_selector::select(const dht::token& t) const {
    if (!_current_token_range || !_current_token_range->contains(t, dht::token_comparator())) {
        auto&& x = _impl->select(t);
        _current_token_range = std::move(std::get<0>(x));
        _current_sstables = std::move(std::get<1>(x));
    }
    return _current_sstables;
}

sstable_set::incremental_selector
sstable_set::make_incremental_selector() const {
    return incremental_selector(_impl->make_incremental_selector());
}

// default sstable_set, not specialized for anything
class bag_sstable_set : public sstable_set_impl {
    // erasing is slow, but select() is fast
    std::vector<shared_sstable> _sstables;
public:
    virtual std::unique_ptr<sstable_set_impl> clone() const override {
        return std::make_unique<bag_sstable_set>(*this);
    }
    virtual std::vector<shared_sstable> select(const dht::partition_range& range = query::full_partition_range) const override {
        return _sstables;
    }
    virtual void insert(shared_sstable sst) override {
        _sstables.push_back(std::move(sst));
    }
    virtual void erase(shared_sstable sst) override {
        _sstables.erase(boost::find(_sstables, sst));
    }
    virtual std::unique_ptr<incremental_selector_impl> make_incremental_selector() const override;
    class incremental_selector;
};

class bag_sstable_set::incremental_selector : public incremental_selector_impl {
    const std::vector<shared_sstable>& _sstables;
public:
    incremental_selector(const std::vector<shared_sstable>& sstables)
        : _sstables(sstables) {
    }
    virtual std::pair<dht::token_range, std::vector<shared_sstable>> select(const dht::token& token) override {
        return std::make_pair(dht::token_range::make_open_ended_both_sides(), _sstables);
    }
};

std::unique_ptr<incremental_selector_impl> bag_sstable_set::make_incremental_selector() const {
    return std::make_unique<incremental_selector>(_sstables);
}

// specialized when sstables are partitioned in the token range space
// e.g. leveled compaction strategy
class partitioned_sstable_set : public sstable_set_impl {
    using value_set = std::unordered_set<shared_sstable>;
    using interval_map_type = boost::icl::interval_map<compatible_ring_position, value_set>;
    using interval_type = interval_map_type::interval_type;
    using map_iterator = interval_map_type::const_iterator;
private:
    schema_ptr _schema;
    std::vector<shared_sstable> _unleveled_sstables;
    interval_map_type _leveled_sstables;
private:
    static interval_type make_interval(const schema& s, const dht::partition_range& range) {
        return interval_type::closed(
                compatible_ring_position(s, range.start()->value()),
                compatible_ring_position(s, range.end()->value()));
    }
    interval_type make_interval(const dht::partition_range& range) const {
        return make_interval(*_schema, range);
    }
    interval_type singular(const dht::ring_position& rp) const {
        auto crp = compatible_ring_position(*_schema, rp);
        return interval_type::closed(crp, crp);
    }
    std::pair<map_iterator, map_iterator> query(const dht::partition_range& range) const {
        if (range.start() && range.end()) {
            return _leveled_sstables.equal_range(make_interval(range));
        }
        else if (range.start() && !range.end()) {
            auto start = singular(range.start()->value());
            return { _leveled_sstables.lower_bound(start), _leveled_sstables.end() };
        } else if (!range.start() && range.end()) {
            auto end = singular(range.end()->value());
            return { _leveled_sstables.begin(), _leveled_sstables.upper_bound(end) };
        } else {
            return { _leveled_sstables.begin(), _leveled_sstables.end() };
        }
    }
public:
    explicit partitioned_sstable_set(schema_ptr schema)
            : _schema(std::move(schema)) {
    }
    virtual std::unique_ptr<sstable_set_impl> clone() const override {
        return std::make_unique<partitioned_sstable_set>(*this);
    }
    virtual std::vector<shared_sstable> select(const dht::partition_range& range) const override {
        auto ipair = query(range);
        auto b = std::move(ipair.first);
        auto e = std::move(ipair.second);
        value_set result;
        while (b != e) {
            boost::copy(b++->second, std::inserter(result, result.end()));
        }
        auto r = _unleveled_sstables;
        r.insert(r.end(), result.begin(), result.end());
        return r;
    }
    virtual void insert(shared_sstable sst) override {
        if (sst->get_sstable_level() == 0) {
            _unleveled_sstables.push_back(std::move(sst));
        } else {
            auto first = sst->get_first_decorated_key().token();
            auto last = sst->get_last_decorated_key().token();
            using bound = dht::partition_range::bound;
            _leveled_sstables.add({
                    make_interval(
                            dht::partition_range(
                                    bound(dht::ring_position::starting_at(first)),
                                    bound(dht::ring_position::ending_at(last)))),
                    value_set({sst})});
        }
    }
    virtual void erase(shared_sstable sst) override {
        if (sst->get_sstable_level() == 0) {
            _unleveled_sstables.erase(std::remove(_unleveled_sstables.begin(), _unleveled_sstables.end(), sst), _unleveled_sstables.end());
        } else {
            auto first = sst->get_first_decorated_key().token();
            auto last = sst->get_last_decorated_key().token();
            using bound = dht::partition_range::bound;
            _leveled_sstables.subtract({
                    make_interval(
                            dht::partition_range(
                                    bound(dht::ring_position::starting_at(first)),
                                    bound(dht::ring_position::ending_at(last)))),
                    value_set({sst})});
        }
    }
    virtual std::unique_ptr<incremental_selector_impl> make_incremental_selector() const override;
    class incremental_selector;
};

class partitioned_sstable_set::incremental_selector : public incremental_selector_impl {
    schema_ptr _schema;
    const std::vector<shared_sstable>& _unleveled_sstables;
    map_iterator _it;
    const map_iterator _end;
private:
    static dht::token_range to_token_range(const interval_type& i) {
        return dht::token_range::make({i.lower().token(), boost::icl::is_left_closed(i.bounds())},
            {i.upper().token(), boost::icl::is_right_closed(i.bounds())});
    }
public:
    incremental_selector(schema_ptr schema, const std::vector<shared_sstable>& unleveled_sstables, const interval_map_type& leveled_sstables)
        : _schema(std::move(schema))
        , _unleveled_sstables(unleveled_sstables)
        , _it(leveled_sstables.begin())
        , _end(leveled_sstables.end()) {
    }
    virtual std::pair<dht::token_range, std::vector<shared_sstable>> select(const dht::token& token) override {
        auto pr = dht::partition_range::make(dht::ring_position::starting_at(token), dht::ring_position::ending_at(token));
        auto interval = make_interval(*_schema, std::move(pr));
        auto ssts = _unleveled_sstables;

        while (_it != _end) {
            if (boost::icl::contains(_it->first, interval)) {
                ssts.insert(ssts.end(), _it->second.begin(), _it->second.end());
                return std::make_pair(to_token_range(_it->first), std::move(ssts));
            }
            // we don't want to skip current interval if token lies before it.
            if (boost::icl::lower_less(interval, _it->first)) {
                return std::make_pair(dht::token_range::make({token, true}, {_it->first.lower().token(), false}),
                    std::move(ssts));
            }
            _it++;
        }
        return std::make_pair(dht::token_range::make_open_ended_both_sides(), std::move(ssts));
    }
};

std::unique_ptr<incremental_selector_impl> partitioned_sstable_set::make_incremental_selector() const {
    return std::make_unique<incremental_selector>(_schema, _unleveled_sstables, _leveled_sstables);
}

class compaction_strategy_impl {
    static constexpr float DEFAULT_TOMBSTONE_THRESHOLD = 0.2f;
    // minimum interval needed to perform tombstone removal compaction in seconds, default 86400 or 1 day.
    static constexpr long DEFAULT_TOMBSTONE_COMPACTION_INTERVAL = 86400;

    const sstring TOMBSTONE_THRESHOLD_OPTION = "tombstone_threshold";
    const sstring TOMBSTONE_COMPACTION_INTERVAL_OPTION = "tombstone_compaction_interval";
protected:
    bool _use_clustering_key_filter = false;
    float _tombstone_threshold = DEFAULT_TOMBSTONE_THRESHOLD;
    db_clock::duration _tombstone_compaction_interval = std::chrono::seconds(DEFAULT_TOMBSTONE_COMPACTION_INTERVAL);
public:
    static stdx::optional<sstring> get_value(const std::map<sstring, sstring>& options, const sstring& name) {
        auto it = options.find(name);
        if (it == options.end()) {
            return stdx::nullopt;
        }
        return it->second;
    }
protected:
    compaction_strategy_impl() = default;
    explicit compaction_strategy_impl(const std::map<sstring, sstring>& options) {
        using namespace cql3::statements;

        auto tmp_value = get_value(options, TOMBSTONE_THRESHOLD_OPTION);
        _tombstone_threshold = property_definitions::to_double(TOMBSTONE_THRESHOLD_OPTION, tmp_value, DEFAULT_TOMBSTONE_THRESHOLD);

        tmp_value = get_value(options, TOMBSTONE_COMPACTION_INTERVAL_OPTION);
        auto interval = property_definitions::to_long(TOMBSTONE_COMPACTION_INTERVAL_OPTION, tmp_value, DEFAULT_TOMBSTONE_COMPACTION_INTERVAL);
        _tombstone_compaction_interval = db_clock::duration(std::chrono::seconds(interval));

        // FIXME: validate options.
    }
public:
    virtual ~compaction_strategy_impl() {}
    virtual compaction_descriptor get_sstables_for_compaction(column_family& cfs, std::vector<sstables::shared_sstable> candidates) = 0;
    virtual std::vector<resharding_descriptor> get_resharding_jobs(column_family& cf, std::vector<sstables::shared_sstable> candidates);
    virtual void notify_completion(const std::vector<lw_shared_ptr<sstable>>& removed, const std::vector<lw_shared_ptr<sstable>>& added) { }
    virtual compaction_strategy_type type() const = 0;
    virtual bool parallel_compaction() const {
        return true;
    }
    virtual int64_t estimated_pending_compactions(column_family& cf) const = 0;
    virtual std::unique_ptr<sstable_set_impl> make_sstable_set(schema_ptr schema) const {
        return std::make_unique<bag_sstable_set>();
    }
    bool use_clustering_key_filter() const {
        return _use_clustering_key_filter;
    }

    // Check if a given sstable is entitled for tombstone compaction based on its
    // droppable tombstone histogram and gc_before.
    bool worth_dropping_tombstones(const shared_sstable& sst, gc_clock::time_point gc_before) {
        // ignore sstables that were created just recently because there's a chance
        // that expired tombstones still cover old data and thus cannot be removed.
        // We want to avoid a compaction loop here on the same data by considering
        // only old enough sstables.
        if (db_clock::now()-_tombstone_compaction_interval < sst->data_file_write_time()) {
            return false;
        }
        return sst->estimate_droppable_tombstone_ratio(gc_before) >= _tombstone_threshold;
    }
};

std::vector<resharding_descriptor>
compaction_strategy_impl::get_resharding_jobs(column_family& cf, std::vector<sstables::shared_sstable> candidates) {
    std::vector<resharding_descriptor> jobs;
    shard_id reshard_at_current = 0;

    clogger.debug("Trying to get resharding jobs for {}.{}...", cf.schema()->ks_name(), cf.schema()->cf_name());
    for (auto& candidate : candidates) {
        auto level = candidate->get_sstable_level();
        jobs.push_back(resharding_descriptor{{std::move(candidate)}, std::numeric_limits<uint64_t>::max(), reshard_at_current++ % smp::count, level});
    }
    return jobs;
}

//
// Null compaction strategy is the default compaction strategy.
// As the name implies, it does nothing.
//
class null_compaction_strategy : public compaction_strategy_impl {
public:
    virtual compaction_descriptor get_sstables_for_compaction(column_family& cfs, std::vector<sstables::shared_sstable> candidates) override {
        return sstables::compaction_descriptor();
    }

    virtual int64_t estimated_pending_compactions(column_family& cf) const override {
        return 0;
    }

    virtual compaction_strategy_type type() const {
        return compaction_strategy_type::null;
    }
};

//
// Major compaction strategy is about compacting all available sstables into one.
//
class major_compaction_strategy : public compaction_strategy_impl {
    static constexpr size_t min_compact_threshold = 2;
public:
    virtual compaction_descriptor get_sstables_for_compaction(column_family& cfs, std::vector<sstables::shared_sstable> candidates) override {
        // At least, two sstables must be available for compaction to take place.
        if (cfs.sstables_count() < min_compact_threshold) {
            return sstables::compaction_descriptor();
        }
        return sstables::compaction_descriptor(std::move(candidates));
    }

    virtual int64_t estimated_pending_compactions(column_family& cf) const override {
        return (cf.sstables_count() < min_compact_threshold) ? 0 : 1;
    }

    virtual compaction_strategy_type type() const {
        return compaction_strategy_type::major;
    }
};

class size_tiered_compaction_strategy_options {
    static constexpr uint64_t DEFAULT_MIN_SSTABLE_SIZE = 50L * 1024L * 1024L;
    static constexpr double DEFAULT_BUCKET_LOW = 0.5;
    static constexpr double DEFAULT_BUCKET_HIGH = 1.5;
    static constexpr double DEFAULT_COLD_READS_TO_OMIT = 0.05;
    const sstring MIN_SSTABLE_SIZE_KEY = "min_sstable_size";
    const sstring BUCKET_LOW_KEY = "bucket_low";
    const sstring BUCKET_HIGH_KEY = "bucket_high";
    const sstring COLD_READS_TO_OMIT_KEY = "cold_reads_to_omit";

    uint64_t min_sstable_size = DEFAULT_MIN_SSTABLE_SIZE;
    double bucket_low = DEFAULT_BUCKET_LOW;
    double bucket_high = DEFAULT_BUCKET_HIGH;
    double cold_reads_to_omit =  DEFAULT_COLD_READS_TO_OMIT;
public:
    size_tiered_compaction_strategy_options(const std::map<sstring, sstring>& options) {
        using namespace cql3::statements;

        auto tmp_value = compaction_strategy_impl::get_value(options, MIN_SSTABLE_SIZE_KEY);
        min_sstable_size = property_definitions::to_long(MIN_SSTABLE_SIZE_KEY, tmp_value, DEFAULT_MIN_SSTABLE_SIZE);

        tmp_value = compaction_strategy_impl::get_value(options, BUCKET_LOW_KEY);
        bucket_low = property_definitions::to_double(BUCKET_LOW_KEY, tmp_value, DEFAULT_BUCKET_LOW);

        tmp_value = compaction_strategy_impl::get_value(options, BUCKET_HIGH_KEY);
        bucket_high = property_definitions::to_double(BUCKET_HIGH_KEY, tmp_value, DEFAULT_BUCKET_HIGH);

        tmp_value = compaction_strategy_impl::get_value(options, COLD_READS_TO_OMIT_KEY);
        cold_reads_to_omit = property_definitions::to_double(COLD_READS_TO_OMIT_KEY, tmp_value, DEFAULT_COLD_READS_TO_OMIT);
    }

    size_tiered_compaction_strategy_options() {
        min_sstable_size = DEFAULT_MIN_SSTABLE_SIZE;
        bucket_low = DEFAULT_BUCKET_LOW;
        bucket_high = DEFAULT_BUCKET_HIGH;
        cold_reads_to_omit = DEFAULT_COLD_READS_TO_OMIT;
    }

    // FIXME: convert java code below.
#if 0
    public static Map<String, String> validateOptions(Map<String, String> options, Map<String, String> uncheckedOptions) throws ConfigurationException
    {
        String optionValue = options.get(MIN_SSTABLE_SIZE_KEY);
        try
        {
            long minSSTableSize = optionValue == null ? DEFAULT_MIN_SSTABLE_SIZE : Long.parseLong(optionValue);
            if (minSSTableSize < 0)
            {
                throw new ConfigurationException(String.format("%s must be non negative: %d", MIN_SSTABLE_SIZE_KEY, minSSTableSize));
            }
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", optionValue, MIN_SSTABLE_SIZE_KEY), e);
        }

        double bucketLow = parseDouble(options, BUCKET_LOW_KEY, DEFAULT_BUCKET_LOW);
        double bucketHigh = parseDouble(options, BUCKET_HIGH_KEY, DEFAULT_BUCKET_HIGH);
        if (bucketHigh <= bucketLow)
        {
            throw new ConfigurationException(String.format("%s value (%s) is less than or equal to the %s value (%s)",
                                                           BUCKET_HIGH_KEY, bucketHigh, BUCKET_LOW_KEY, bucketLow));
        }

        double maxColdReadsRatio = parseDouble(options, COLD_READS_TO_OMIT_KEY, DEFAULT_COLD_READS_TO_OMIT);
        if (maxColdReadsRatio < 0.0 || maxColdReadsRatio > 1.0)
        {
            throw new ConfigurationException(String.format("%s value (%s) should be between between 0.0 and 1.0",
                                                           COLD_READS_TO_OMIT_KEY, optionValue));
        }

        uncheckedOptions.remove(MIN_SSTABLE_SIZE_KEY);
        uncheckedOptions.remove(BUCKET_LOW_KEY);
        uncheckedOptions.remove(BUCKET_HIGH_KEY);
        uncheckedOptions.remove(COLD_READS_TO_OMIT_KEY);

        return uncheckedOptions;
    }
#endif
    friend class size_tiered_compaction_strategy;
};

class size_tiered_compaction_strategy : public compaction_strategy_impl {
    size_tiered_compaction_strategy_options _options;

    // Return a list of pair of shared_sstable and its respective size.
    std::vector<std::pair<sstables::shared_sstable, uint64_t>> create_sstable_and_length_pairs(const std::vector<sstables::shared_sstable>& sstables) const;

    // Group files of similar size into buckets.
    std::vector<std::vector<sstables::shared_sstable>> get_buckets(const std::vector<sstables::shared_sstable>& sstables) const;

    // Maybe return a bucket of sstables to compact
    std::vector<sstables::shared_sstable>
    most_interesting_bucket(std::vector<std::vector<sstables::shared_sstable>> buckets, unsigned min_threshold, unsigned max_threshold);

    // Return the average size of a given list of sstables.
    uint64_t avg_size(std::vector<sstables::shared_sstable>& sstables) {
        assert(sstables.size() > 0); // this should never fail
        uint64_t n = 0;

        for (auto& sstable : sstables) {
            // FIXME: Switch to sstable->bytes_on_disk() afterwards. That's what C* uses.
            n += sstable->data_size();
        }

        return n / sstables.size();
    }

    bool is_bucket_interesting(const std::vector<sstables::shared_sstable>& bucket, int min_threshold) const {
        return bucket.size() >= size_t(min_threshold);
    }

    bool is_any_bucket_interesting(const std::vector<std::vector<sstables::shared_sstable>>& buckets, int min_threshold) const {
        return boost::algorithm::any_of(buckets, [&] (const auto& bucket) {
            return this->is_bucket_interesting(bucket, min_threshold);
        });
    }
public:
    size_tiered_compaction_strategy() = default;
    size_tiered_compaction_strategy(const std::map<sstring, sstring>& options) :
        compaction_strategy_impl(options), _options(options) {}

    virtual compaction_descriptor get_sstables_for_compaction(column_family& cfs, std::vector<sstables::shared_sstable> candidates) override;

    virtual int64_t estimated_pending_compactions(column_family& cf) const override;

    friend std::vector<sstables::shared_sstable> size_tiered_most_interesting_bucket(lw_shared_ptr<sstable_list>);
    friend std::vector<sstables::shared_sstable> size_tiered_most_interesting_bucket(const std::list<sstables::shared_sstable>&);

    virtual compaction_strategy_type type() const {
        return compaction_strategy_type::size_tiered;
    }
};

std::vector<std::pair<sstables::shared_sstable, uint64_t>>
size_tiered_compaction_strategy::create_sstable_and_length_pairs(const std::vector<sstables::shared_sstable>& sstables) const {

    std::vector<std::pair<sstables::shared_sstable, uint64_t>> sstable_length_pairs;
    sstable_length_pairs.reserve(sstables.size());

    for(auto& sstable : sstables) {
        auto sstable_size = sstable->data_size();
        assert(sstable_size != 0);

        sstable_length_pairs.emplace_back(sstable, sstable_size);
    }

    return sstable_length_pairs;
}

std::vector<std::vector<sstables::shared_sstable>>
size_tiered_compaction_strategy::get_buckets(const std::vector<sstables::shared_sstable>& sstables) const {
    // sstables sorted by size of its data file.
    auto sorted_sstables = create_sstable_and_length_pairs(sstables);

    std::sort(sorted_sstables.begin(), sorted_sstables.end(), [] (auto& i, auto& j) {
        return i.second < j.second;
    });

    std::map<size_t, std::vector<sstables::shared_sstable>> buckets;

    bool found;
    for (auto& pair : sorted_sstables) {
        found = false;
        size_t size = pair.second;

        // look for a bucket containing similar-sized files:
        // group in the same bucket if it's w/in 50% of the average for this bucket,
        // or this file and the bucket are all considered "small" (less than `minSSTableSize`)
        for (auto& entry : buckets) {
            std::vector<sstables::shared_sstable> bucket = entry.second;
            size_t old_average_size = entry.first;

            if ((size > (old_average_size * _options.bucket_low) && size < (old_average_size * _options.bucket_high)) ||
                    (size < _options.min_sstable_size && old_average_size < _options.min_sstable_size)) {
                size_t total_size = bucket.size() * old_average_size;
                size_t new_average_size = (total_size + size) / (bucket.size() + 1);

                bucket.push_back(pair.first);
                buckets.erase(old_average_size);
                buckets.insert({ new_average_size, std::move(bucket) });

                found = true;
                break;
            }
        }

        // no similar bucket found; put it in a new one
        if (!found) {
            std::vector<sstables::shared_sstable> new_bucket;
            new_bucket.push_back(pair.first);
            buckets.insert({ size, std::move(new_bucket) });
        }
    }

    std::vector<std::vector<sstables::shared_sstable>> bucket_list;
    bucket_list.reserve(buckets.size());

    for (auto& entry : buckets) {
        bucket_list.push_back(std::move(entry.second));
    }

    return bucket_list;
}

std::vector<sstables::shared_sstable>
size_tiered_compaction_strategy::most_interesting_bucket(std::vector<std::vector<sstables::shared_sstable>> buckets,
        unsigned min_threshold, unsigned max_threshold)
{
    std::vector<std::pair<std::vector<sstables::shared_sstable>, uint64_t>> pruned_buckets_and_hotness;
    pruned_buckets_and_hotness.reserve(buckets.size());

    // FIXME: add support to get hotness for each bucket.

    for (auto& bucket : buckets) {
        // FIXME: the coldest sstables will be trimmed to meet the threshold, so we must add support to this feature
        // by converting SizeTieredCompactionStrategy::trimToThresholdWithHotness.
        // By the time being, we will only compact buckets that meet the threshold.
        bucket.resize(std::min(bucket.size(), size_t(max_threshold)));
        if (is_bucket_interesting(bucket, min_threshold)) {
            auto avg = avg_size(bucket);
            pruned_buckets_and_hotness.push_back({ std::move(bucket), avg });
        }
    }

    if (pruned_buckets_and_hotness.empty()) {
        return std::vector<sstables::shared_sstable>();
    }

    // NOTE: Compacting smallest sstables first, located at the beginning of the sorted vector.
    auto& min = *std::min_element(pruned_buckets_and_hotness.begin(), pruned_buckets_and_hotness.end(), [] (auto& i, auto& j) {
        // FIXME: ignoring hotness by the time being.

        return i.second < j.second;
    });
    auto hottest = std::move(min.first);

    return hottest;
}

compaction_descriptor size_tiered_compaction_strategy::get_sstables_for_compaction(column_family& cfs, std::vector<sstables::shared_sstable> candidates) {
    // make local copies so they can't be changed out from under us mid-method
    int min_threshold = cfs.schema()->min_compaction_threshold();
    int max_threshold = cfs.schema()->max_compaction_threshold();
    auto gc_before = gc_clock::now() - cfs.schema()->gc_grace_seconds();

    // TODO: Add support to filter cold sstables (for reference: SizeTieredCompactionStrategy::filterColdSSTables).

    auto buckets = get_buckets(candidates);

    if (is_any_bucket_interesting(buckets, min_threshold)) {
        std::vector<sstables::shared_sstable> most_interesting = most_interesting_bucket(std::move(buckets), min_threshold, max_threshold);
        return sstables::compaction_descriptor(std::move(most_interesting));
    }

    // if there is no sstable to compact in standard way, try compacting single sstable whose droppable tombstone
    // ratio is greater than threshold.
    // prefer oldest sstables from biggest size tiers because they will be easier to satisfy conditions for
    // tombstone purge, i.e. less likely to shadow even older data.
    for (auto&& sstables : buckets | boost::adaptors::reversed) {
        // filter out sstables which droppable tombstone ratio isn't greater than the defined threshold.
        auto e = boost::range::remove_if(sstables, [this, &gc_before] (const sstables::shared_sstable& sst) -> bool {
            return !worth_dropping_tombstones(sst, gc_before);
        });
        sstables.erase(e, sstables.end());
        if (sstables.empty()) {
            continue;
        }
        // find oldest sstable from current tier
        auto it = std::min_element(sstables.begin(), sstables.end(), [] (auto& i, auto& j) {
            return i->get_stats_metadata().min_timestamp < j->get_stats_metadata().min_timestamp;
        });
        return sstables::compaction_descriptor({ *it });
    }
    return sstables::compaction_descriptor();
}

int64_t size_tiered_compaction_strategy::estimated_pending_compactions(column_family& cf) const {
    int min_threshold = cf.schema()->min_compaction_threshold();
    int max_threshold = cf.schema()->max_compaction_threshold();
    std::vector<sstables::shared_sstable> sstables;
    int64_t n = 0;

    sstables.reserve(cf.sstables_count());
    for (auto& entry : *cf.get_sstables()) {
        sstables.push_back(entry);
    }

    for (auto& bucket : get_buckets(sstables)) {
        if (bucket.size() >= size_t(min_threshold)) {
            n += std::ceil(double(bucket.size()) / max_threshold);
        }
    }
    return n;
}

std::vector<sstables::shared_sstable> size_tiered_most_interesting_bucket(lw_shared_ptr<sstable_list> candidates) {
    size_tiered_compaction_strategy cs;

    std::vector<sstables::shared_sstable> sstables;
    sstables.reserve(candidates->size());
    for (auto& entry : *candidates) {
        sstables.push_back(entry);
    }

    auto buckets = cs.get_buckets(sstables);

    std::vector<sstables::shared_sstable> most_interesting = cs.most_interesting_bucket(std::move(buckets),
        DEFAULT_MIN_COMPACTION_THRESHOLD, DEFAULT_MAX_COMPACTION_THRESHOLD);

    return most_interesting;
}

std::vector<sstables::shared_sstable>
size_tiered_most_interesting_bucket(const std::list<sstables::shared_sstable>& candidates) {
    size_tiered_compaction_strategy cs;

    std::vector<sstables::shared_sstable> sstables(candidates.begin(), candidates.end());

    auto buckets = cs.get_buckets(sstables);

    std::vector<sstables::shared_sstable> most_interesting = cs.most_interesting_bucket(std::move(buckets),
        DEFAULT_MIN_COMPACTION_THRESHOLD, DEFAULT_MAX_COMPACTION_THRESHOLD);

    return most_interesting;
}

class leveled_compaction_strategy : public compaction_strategy_impl {
    static constexpr int32_t DEFAULT_MAX_SSTABLE_SIZE_IN_MB = 160;
    const sstring SSTABLE_SIZE_OPTION = "sstable_size_in_mb";

    int32_t _max_sstable_size_in_mb = DEFAULT_MAX_SSTABLE_SIZE_IN_MB;
    stdx::optional<std::vector<stdx::optional<dht::decorated_key>>> _last_compacted_keys;
    std::vector<int> _compaction_counter;
public:
    leveled_compaction_strategy(const std::map<sstring, sstring>& options) {
        using namespace cql3::statements;

        auto tmp_value = compaction_strategy_impl::get_value(options, SSTABLE_SIZE_OPTION);
        _max_sstable_size_in_mb = property_definitions::to_int(SSTABLE_SIZE_OPTION, tmp_value, DEFAULT_MAX_SSTABLE_SIZE_IN_MB);
        if (_max_sstable_size_in_mb >= 1000) {
            clogger.warn("Max sstable size of {}MB is configured; having a unit of compaction this large is probably a bad idea",
                _max_sstable_size_in_mb);
        } else if (_max_sstable_size_in_mb < 50) {
            clogger.warn("Max sstable size of {}MB is configured. Testing done for CASSANDRA-5727 indicates that performance improves up to 160MB",
                _max_sstable_size_in_mb);
        }
        _compaction_counter.resize(leveled_manifest::MAX_LEVELS);
    }

    virtual compaction_descriptor get_sstables_for_compaction(column_family& cfs, std::vector<sstables::shared_sstable> candidates) override;

    virtual std::vector<resharding_descriptor> get_resharding_jobs(column_family& cf, std::vector<shared_sstable> candidates) override;

    virtual void notify_completion(const std::vector<lw_shared_ptr<sstable>>& removed, const std::vector<lw_shared_ptr<sstable>>& added) override;

    // for each level > 0, get newest sstable and use its last key as last
    // compacted key for the previous level.
    void generate_last_compacted_keys(leveled_manifest& manifest);

    virtual int64_t estimated_pending_compactions(column_family& cf) const override;

    virtual bool parallel_compaction() const override {
        return false;
    }

    virtual compaction_strategy_type type() const {
        return compaction_strategy_type::leveled;
    }
    virtual std::unique_ptr<sstable_set_impl> make_sstable_set(schema_ptr schema) const override {
        return std::make_unique<partitioned_sstable_set>(std::move(schema));
    }
};

compaction_descriptor leveled_compaction_strategy::get_sstables_for_compaction(column_family& cfs, std::vector<sstables::shared_sstable> candidates) {
    // NOTE: leveled_manifest creation may be slightly expensive, so later on,
    // we may want to store it in the strategy itself. However, the sstable
    // lists managed by the manifest may become outdated. For example, one
    // sstable in it may be marked for deletion after compacted.
    // Currently, we create a new manifest whenever it's time for compaction.
    leveled_manifest manifest = leveled_manifest::create(cfs, candidates, _max_sstable_size_in_mb);
    if (!_last_compacted_keys) {
        generate_last_compacted_keys(manifest);
    }
    auto candidate = manifest.get_compaction_candidates(*_last_compacted_keys, _compaction_counter);

    if (candidate.sstables.empty()) {
        return sstables::compaction_descriptor();
    }

    clogger.debug("leveled: Compacting {} out of {} sstables", candidate.sstables.size(), cfs.get_sstables()->size());

    return std::move(candidate);
}

std::vector<resharding_descriptor> leveled_compaction_strategy::get_resharding_jobs(column_family& cf, std::vector<shared_sstable> candidates) {
    leveled_manifest manifest = leveled_manifest::create(cf, candidates, _max_sstable_size_in_mb);

    std::vector<resharding_descriptor> descriptors;
    shard_id target_shard = 0;
    auto get_shard = [&target_shard] { return target_shard++ % smp::count; };

    // Basically, we'll iterate through all levels, and for each, we'll sort the
    // sstables by first key because there's a need to reshard together adjacent
    // sstables.
    // The shard at which the job will run is chosen in a round-robin fashion.
    for (auto level = 0U; level <= manifest.get_level_count(); level++) {
        uint64_t max_sstable_size = !level ? std::numeric_limits<uint64_t>::max() : (_max_sstable_size_in_mb*1024*1024);
        auto& sstables = manifest.get_level(level);
        sstables.sort([] (auto& i, auto& j) {
            return i->compare_by_first_key(*j) < 0;
        });

        resharding_descriptor current_descriptor = resharding_descriptor{{}, max_sstable_size, get_shard(), level};

        for (auto it = sstables.begin(); it != sstables.end(); it++) {
            current_descriptor.sstables.push_back(*it);

            auto next = std::next(it);
            if (current_descriptor.sstables.size() == smp::count || next == sstables.end()) {
                descriptors.push_back(std::move(current_descriptor));
                current_descriptor = resharding_descriptor{{}, max_sstable_size, get_shard(), level};
            }
        }
    }
    return descriptors;
}

void leveled_compaction_strategy::notify_completion(const std::vector<lw_shared_ptr<sstable>>& removed, const std::vector<lw_shared_ptr<sstable>>& added) {
    if (removed.empty() || added.empty()) {
        return;
    }
    auto min_level = std::numeric_limits<uint32_t>::max();
    for (auto& sstable : removed) {
        min_level = std::min(min_level, sstable->get_sstable_level());
    }

    const sstables::sstable *last = nullptr;
    for (auto& candidate : added) {
        if (!last || last->compare_by_first_key(*candidate) < 0) {
            last = &*candidate;
        }
    }
    _last_compacted_keys.value().at(min_level) = last->get_last_decorated_key();
}

void leveled_compaction_strategy::generate_last_compacted_keys(leveled_manifest& manifest) {
    std::vector<stdx::optional<dht::decorated_key>> last_compacted_keys(leveled_manifest::MAX_LEVELS);
    for (auto i = 0; i < leveled_manifest::MAX_LEVELS - 1; i++) {
        if (manifest.get_level(i + 1).empty()) {
            continue;
        }

        const sstables::sstable* sstable_with_last_compacted_key = nullptr;
        stdx::optional<db_clock::time_point> max_creation_time;
        for (auto& sst : manifest.get_level(i + 1)) {
            auto wtime = sst->data_file_write_time();
            if (!max_creation_time || wtime >= *max_creation_time) {
                sstable_with_last_compacted_key = &*sst;
                max_creation_time = wtime;
            }
        }
        last_compacted_keys[i] = sstable_with_last_compacted_key->get_last_decorated_key();
    }
    _last_compacted_keys = std::move(last_compacted_keys);
}

int64_t leveled_compaction_strategy::estimated_pending_compactions(column_family& cf) const {
    std::vector<sstables::shared_sstable> sstables;
    sstables.reserve(cf.sstables_count());
    for (auto& entry : *cf.get_sstables()) {
        sstables.push_back(entry);
    }
    leveled_manifest manifest = leveled_manifest::create(cf, sstables, _max_sstable_size_in_mb);
    return manifest.get_estimated_tasks();
}

class date_tiered_compaction_strategy : public compaction_strategy_impl {
    date_tiered_manifest _manifest;
public:
    date_tiered_compaction_strategy(const std::map<sstring, sstring>& options)
        : _manifest(options)
    {
        _use_clustering_key_filter = true;
    }

    virtual compaction_descriptor get_sstables_for_compaction(column_family& cfs, std::vector<sstables::shared_sstable> candidates) override {
        auto gc_before = gc_clock::now() - cfs.schema()->gc_grace_seconds();
        auto sstables = _manifest.get_next_sstables(cfs, candidates, gc_before);
        clogger.debug("datetiered: Compacting {} out of {} sstables", sstables.size(), candidates.size());
        if (sstables.empty()) {
            return sstables::compaction_descriptor();
        }
        return sstables::compaction_descriptor(std::move(sstables));
    }

    virtual int64_t estimated_pending_compactions(column_family& cf) const override {
        return _manifest.get_estimated_tasks(cf);
    }

    virtual compaction_strategy_type type() const {
        return compaction_strategy_type::date_tiered;
    }
};

compaction_strategy::compaction_strategy(::shared_ptr<compaction_strategy_impl> impl)
    : _compaction_strategy_impl(std::move(impl)) {}
compaction_strategy::compaction_strategy() = default;
compaction_strategy::~compaction_strategy() = default;
compaction_strategy::compaction_strategy(const compaction_strategy&) = default;
compaction_strategy::compaction_strategy(compaction_strategy&&) = default;
compaction_strategy& compaction_strategy::operator=(compaction_strategy&&) = default;

compaction_strategy_type compaction_strategy::type() const {
    return _compaction_strategy_impl->type();
}

compaction_descriptor compaction_strategy::get_sstables_for_compaction(column_family& cfs, std::vector<sstables::shared_sstable> candidates) {
    return _compaction_strategy_impl->get_sstables_for_compaction(cfs, std::move(candidates));
}

std::vector<resharding_descriptor> compaction_strategy::get_resharding_jobs(column_family& cf, std::vector<sstables::shared_sstable> candidates) {
    return _compaction_strategy_impl->get_resharding_jobs(cf, std::move(candidates));
}

void compaction_strategy::notify_completion(const std::vector<lw_shared_ptr<sstable>>& removed, const std::vector<lw_shared_ptr<sstable>>& added) {
    _compaction_strategy_impl->notify_completion(removed, added);
}

bool compaction_strategy::parallel_compaction() const {
    return _compaction_strategy_impl->parallel_compaction();
}

int64_t compaction_strategy::estimated_pending_compactions(column_family& cf) const {
    return _compaction_strategy_impl->estimated_pending_compactions(cf);
}

bool compaction_strategy::use_clustering_key_filter() const {
    return _compaction_strategy_impl->use_clustering_key_filter();
}

sstable_set
compaction_strategy::make_sstable_set(schema_ptr schema) const {
    return sstable_set(
            _compaction_strategy_impl->make_sstable_set(std::move(schema)),
            make_lw_shared<sstable_list>());
}

compaction_strategy make_compaction_strategy(compaction_strategy_type strategy, const std::map<sstring, sstring>& options) {
    ::shared_ptr<compaction_strategy_impl> impl;

    switch(strategy) {
    case compaction_strategy_type::null:
        impl = make_shared<null_compaction_strategy>(null_compaction_strategy());
        break;
    case compaction_strategy_type::major:
        impl = make_shared<major_compaction_strategy>(major_compaction_strategy());
        break;
    case compaction_strategy_type::size_tiered:
        impl = make_shared<size_tiered_compaction_strategy>(size_tiered_compaction_strategy(options));
        break;
    case compaction_strategy_type::leveled:
        impl = make_shared<leveled_compaction_strategy>(leveled_compaction_strategy(options));
        break;
    case compaction_strategy_type::date_tiered:
        impl = make_shared<date_tiered_compaction_strategy>(date_tiered_compaction_strategy(options));
        break;
    default:
        throw std::runtime_error("strategy not supported");
    }

    return compaction_strategy(std::move(impl));
}

}
