/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "dht/range_streamer.hh"
#include "replica/database.hh"
#include "gms/gossiper.hh"
#include "log.hh"
#include "streaming/stream_plan.hh"
#include "db/config.hh"
#include <boost/range/adaptors.hpp>
#include <fmt/ranges.h>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sleep.hh>
#include "utils/assert.hh"
#include "utils/stall_free.hh"

namespace dht {

static logging::logger logger("range_streamer");

using inet_address = gms::inet_address;

std::unordered_map<inet_address, dht::token_range_vector>
range_streamer::get_range_fetch_map(const std::unordered_map<dht::token_range, std::vector<inet_address>>& ranges_with_sources,
                                    const std::unordered_set<std::unique_ptr<i_source_filter>>& source_filters,
                                    const sstring& keyspace) {
    std::unordered_map<inet_address, dht::token_range_vector> range_fetch_map_map;
    const auto& topo = _token_metadata_ptr->get_topology();
    for (const auto& x : ranges_with_sources) {
        const dht::token_range& range_ = x.first;
        const std::vector<inet_address>& addresses = x.second;
        bool found_source = false;
        for (const auto& address : addresses) {
            if (topo.is_me(address)) {
                // If localhost is a source, we have found one, but we don't add it to the map to avoid streaming locally
                found_source = true;
                continue;
            }

            auto filtered = false;
            for (const auto& filter : source_filters) {
                if (!filter->should_include(get_token_metadata().get_topology(), address)) {
                    filtered = true;
                    break;
                }
            }

            if (filtered) {
                logger.debug("In get_range_fetch_map, keyspace = {}, endpoint= {} is filtered", keyspace, address);
                continue;
            }

            range_fetch_map_map[address].push_back(range_);
            found_source = true;
            break; // ensure we only stream from one other node for each range
        }

        if (!found_source) {
            auto& ks = _db.local().find_keyspace(keyspace);
            auto rf = ks.get_vnode_effective_replication_map()->get_replication_factor();
            // When a replacing node replaces a dead node with keyspace of RF
            // 1, it is expected that replacing node could not find a peer node
            // that contains data to stream from.
            if (_reason == streaming::stream_reason::replace && rf == 1) {
                logger.warn("Unable to find sufficient sources to stream range {} for keyspace {} with RF = 1 for replace operation", range_, keyspace);
            } else {
                throw std::runtime_error(format("unable to find sufficient sources for streaming range {} in keyspace {}", range_, keyspace));
            }
        }
    }

    return range_fetch_map_map;
}

// Must be called from a seastar thread
std::unordered_map<dht::token_range, std::vector<inet_address>>
range_streamer::get_all_ranges_with_sources_for(const sstring& keyspace_name, locator::vnode_effective_replication_map_ptr erm, dht::token_range_vector desired_ranges) {
    logger.debug("{} ks={}", __func__, keyspace_name);

    auto range_addresses = erm->get_range_addresses().get();

    logger.debug("keyspace={}, desired_ranges.size={}, range_addresses.size={}", keyspace_name, desired_ranges.size(), range_addresses.size());

    std::unordered_map<dht::token_range, std::vector<inet_address>> range_sources;
    const auto address_ep = get_token_metadata().get_endpoint_for_host_id(_address);
    for (auto& desired_range : desired_ranges) {
        auto found = false;
        for (auto& x : range_addresses) {
            if (need_preempt()) {
                seastar::thread::yield();
            }
            const wrapping_interval<token>& src_range = x.first;
            if (src_range.contains(desired_range, dht::token_comparator{})) {
                inet_address_vector_replica_set preferred(x.second.begin(), x.second.end());
                get_token_metadata().get_topology().sort_by_proximity(address_ep, preferred);
                for (inet_address& p : preferred) {
                    range_sources[desired_range].push_back(p);
                }
                found = true;
            }
        }

        if (!found) {
            throw std::runtime_error(format("No sources found for {}", desired_range));
        }
    }

    return range_sources;
}

// Must be called from a seastar thread
std::unordered_map<dht::token_range, std::vector<inet_address>>
range_streamer::get_all_ranges_with_strict_sources_for(const sstring& keyspace_name, locator::vnode_effective_replication_map_ptr erm, dht::token_range_vector desired_ranges, gms::gossiper& gossiper) {
    logger.debug("{} ks={}", __func__, keyspace_name);
    SCYLLA_ASSERT (_tokens.empty() == false);

    auto& strat = erm->get_replication_strategy();

    //Active ranges
    auto metadata_clone = get_token_metadata().clone_only_token_map().get();
    auto range_addresses = strat.get_range_addresses(metadata_clone).get();

    //Pending ranges
    metadata_clone.update_topology(_address, _dr);
    metadata_clone.update_normal_tokens(_tokens, _address).get();
    auto pending_range_addresses  = strat.get_range_addresses(metadata_clone).get();
    metadata_clone.clear_gently().get();

    //Collects the source that will have its range moved to the new node
    std::unordered_map<dht::token_range, std::vector<inet_address>> range_sources;

    logger.debug("keyspace={}, desired_ranges.size={}, range_addresses.size={}", keyspace_name, desired_ranges.size(), range_addresses.size());

    for (auto& desired_range : desired_ranges) {
        for (auto& x : range_addresses) {
            const wrapping_interval<token>& src_range = x.first;
            if (need_preempt()) {
                seastar::thread::yield();
            }
            if (src_range.contains(desired_range, dht::token_comparator{})) {
                std::vector<inet_address> old_endpoints(x.second.begin(), x.second.end());
                auto it = pending_range_addresses.find(desired_range);
                if (it == pending_range_addresses.end()) {
                    throw std::runtime_error(format("Can not find desired_range = {} in pending_range_addresses", desired_range));
                }

                std::unordered_set<inet_address> new_endpoints(it->second.begin(), it->second.end());
                //Due to CASSANDRA-5953 we can have a higher RF then we have endpoints.
                //So we need to be careful to only be strict when endpoints == RF
                if (old_endpoints.size() == erm->get_replication_factor()) {
                    std::erase_if(old_endpoints,
                        [&new_endpoints] (inet_address ep) { return new_endpoints.contains(ep); });
                    if (old_endpoints.size() != 1) {
                        throw std::runtime_error(format("Expected 1 endpoint but found {:d}", old_endpoints.size()));
                    }
                }
                range_sources[desired_range].push_back(old_endpoints.front());
            }
        }

        //Validate
        auto it = range_sources.find(desired_range);
        if (it == range_sources.end()) {
            throw std::runtime_error(format("No sources found for {}", desired_range));
        }

        if (it->second.size() != 1) {
            throw std::runtime_error(format("Multiple endpoints found for {}", desired_range));
        }

        inet_address source_ip = it->second.front();

        if (gossiper.is_enabled() && !gossiper.is_alive(source_ip)) {
            throw std::runtime_error(format("A node required to move the data consistently is down ({}).  If you wish to move the data from a potentially inconsistent replica, restart the node with consistent_rangemovement=false", source_ip));
        }
    }

    return range_sources;
}

bool range_streamer::use_strict_sources_for_ranges(const sstring& keyspace_name, const locator::vnode_effective_replication_map_ptr& erm) {
    auto rf = erm->get_replication_factor();
    auto nr_nodes_in_ring = get_token_metadata().get_normal_token_owners().size();
    bool everywhere_topology = erm->get_replication_strategy().get_type() == locator::replication_strategy_type::everywhere_topology;
    // Use strict when number of nodes in the ring is equal or more than RF
    auto strict = _db.local().get_config().consistent_rangemovement()
           && !_tokens.empty()
           && !everywhere_topology
           && nr_nodes_in_ring >= rf;
    logger.debug("use_strict_sources_for_ranges: ks={}, nr_nodes_in_ring={}, rf={}, strict={}",
            keyspace_name, nr_nodes_in_ring, rf, strict);
    return strict;
}

void range_streamer::add_tx_ranges(const sstring& keyspace_name, std::unordered_map<inet_address, dht::token_range_vector> ranges_per_endpoint) {
    if (_nr_rx_added) {
        throw std::runtime_error("Mixed sending and receiving is not supported");
    }
    _nr_tx_added++;
    _to_stream.emplace(keyspace_name, std::move(ranges_per_endpoint));
}

void range_streamer::add_rx_ranges(const sstring& keyspace_name, std::unordered_map<inet_address, dht::token_range_vector> ranges_per_endpoint) {
    if (_nr_tx_added) {
        throw std::runtime_error("Mixed sending and receiving is not supported");
    }
    _nr_rx_added++;
    _to_stream.emplace(keyspace_name, std::move(ranges_per_endpoint));
}

// TODO: This is the legacy range_streamer interface, it is add_rx_ranges which adds rx ranges.
future<> range_streamer::add_ranges(const sstring& keyspace_name, locator::vnode_effective_replication_map_ptr erm, dht::token_range_vector ranges, gms::gossiper& gossiper, bool is_replacing) {
  return seastar::async([this, keyspace_name, erm = std::move(erm), ranges= std::move(ranges), &gossiper, is_replacing] () mutable {
    if (_nr_tx_added) {
        throw std::runtime_error("Mixed sending and receiving is not supported");
    }
    _nr_rx_added++;
    auto ranges_for_keyspace = !is_replacing && use_strict_sources_for_ranges(keyspace_name, erm)
        ? get_all_ranges_with_strict_sources_for(keyspace_name, erm, std::move(ranges), gossiper)
        : get_all_ranges_with_sources_for(keyspace_name, erm, std::move(ranges));

    if (logger.is_enabled(logging::log_level::debug)) {
        for (auto& x : ranges_for_keyspace) {
            logger.debug("{} : keyspace {} range {} exists on {}", _description, keyspace_name, x.first, x.second);
        }
    }

    std::unordered_map<inet_address, dht::token_range_vector> range_fetch_map = get_range_fetch_map(ranges_for_keyspace, _source_filters, keyspace_name);
    utils::clear_gently(ranges_for_keyspace).get();

    if (logger.is_enabled(logging::log_level::debug)) {
        for (auto& x : range_fetch_map) {
            logger.debug("{} : keyspace={}, ranges={} from source={}, range_size={}", _description, keyspace_name, x.second, x.first, x.second.size());
        }
    }
    _to_stream.emplace(keyspace_name, std::move(range_fetch_map));
  });
}

future<> range_streamer::stream_async() {
    _nr_ranges_remaining = nr_ranges_to_stream();
    _nr_total_ranges = _nr_ranges_remaining;
    _token_metadata_ptr = nullptr;
    logger.info("{} starts, nr_ranges_remaining={}", _description, _nr_ranges_remaining);
    auto start = lowres_clock::now();
    return do_for_each(_to_stream, [this, description = _description] (auto& stream) {
        const auto& keyspace = stream.first;
        auto& ip_range_vec = stream.second;
        auto ips = boost::copy_range<std::list<inet_address>>(ip_range_vec | boost::adaptors::map_keys);
        // Fetch from or send to peer node in parallel
        logger.info("{} with {} for keyspace={} started, nodes_to_stream={}", description, ips, keyspace, ip_range_vec.size());
        return parallel_for_each(ip_range_vec, [this, description, keyspace] (auto& ip_range) {
          auto& source = ip_range.first;
          auto& range_vec = ip_range.second;
          return seastar::with_semaphore(_limiter, 1, [this, description, keyspace, source, &range_vec] () mutable {
            return seastar::async([this, description, keyspace, source, &range_vec] () mutable {
                // TODO: It is better to use fiber instead of thread here because
                // creating a thread per peer can be some memory in a large cluster.
                auto start_time = lowres_clock::now();
                unsigned sp_index = 0;
                unsigned nr_ranges_streamed = 0;
                size_t nr_ranges_total = range_vec.size();
                auto do_streaming = [&] (dht::token_range_vector&& ranges_to_stream) {
                    auto sp = stream_plan(_stream_manager.local(), format("{}-{}-index-{:d}", description, keyspace, sp_index++),
                                          _reason, _topo_guard);
                    auto abort_listener = _abort_source.subscribe([&] () noexcept { sp.abort(); });
                    _abort_source.check();
                    logger.info("{} with {} for keyspace={}, streaming [{}, {}) out of {} ranges",
                            description, source, keyspace,
                            nr_ranges_streamed, nr_ranges_streamed + ranges_to_stream.size(), nr_ranges_total);
                    auto ranges_streamed = ranges_to_stream.size();
                    if (_nr_rx_added) {
                        sp.request_ranges(source, keyspace, std::move(ranges_to_stream), _tables);
                    } else if (_nr_tx_added) {
                        sp.transfer_ranges(source, keyspace, std::move(ranges_to_stream), _tables);
                    }
                    sp.execute().discard_result().get();
                    // Update finished percentage
                    nr_ranges_streamed += ranges_streamed;
                    _nr_ranges_remaining -= ranges_streamed;
                    float percentage = _nr_total_ranges == 0 ? 1 : (_nr_total_ranges - _nr_ranges_remaining) / (float)_nr_total_ranges;
                    _stream_manager.local().update_finished_percentage(_reason, percentage);
                    logger.info("Finished {} out of {} ranges for {}, finished percentage={}",
                            _nr_total_ranges - _nr_ranges_remaining, _nr_total_ranges, _reason, percentage);
                };
                dht::token_range_vector ranges_to_stream;
                try {
                    for (auto it = range_vec.begin(); it < range_vec.end();) {
                        ranges_to_stream.push_back(*it);
                        ++it;
                        auto fraction = _db.local().get_config().stream_plan_ranges_fraction();
                        size_t nr_ranges_per_stream_plan = nr_ranges_total * fraction;
                        if (ranges_to_stream.size() < nr_ranges_per_stream_plan) {
                            continue;
                        } else {
                            do_streaming(std::exchange(ranges_to_stream, {}));
                            it = range_vec.erase(range_vec.begin(), it);
                        }
                    }
                    if (ranges_to_stream.size() > 0) {
                        do_streaming(std::exchange(ranges_to_stream, {}));
                        range_vec.clear();
                    }
                } catch (...) {
                    auto t = std::chrono::duration_cast<std::chrono::duration<float>>(lowres_clock::now() - start_time).count();
                    logger.warn("{} with {} for keyspace={} failed, took {} seconds: {}", description, source, keyspace, t, std::current_exception());
                    throw;
                }
                auto t = std::chrono::duration_cast<std::chrono::duration<float>>(lowres_clock::now() - start_time).count();
                logger.info("{} with {} for keyspace={} succeeded, took {} seconds", description, source, keyspace, t);
              });
          });
        });
    }).finally([this, start] {
        auto t = std::chrono::duration_cast<std::chrono::seconds>(lowres_clock::now() - start).count();
        auto nr_ranges_remaining = nr_ranges_to_stream();
        if (nr_ranges_remaining) {
            logger.warn("{} failed, took {} seconds, nr_ranges_remaining={}", _description, t, nr_ranges_remaining);
        } else {
            logger.info("{} succeeded, took {} seconds, nr_ranges_remaining={}", _description, t, nr_ranges_remaining);
        }
    });
}

size_t range_streamer::nr_ranges_to_stream() {
    size_t nr_ranges_remaining = 0;
    for (auto& fetch : _to_stream) {
        const auto& keyspace = fetch.first;
        auto& ip_range_vec = fetch.second;
        for (auto& ip_range : ip_range_vec) {
            auto& source = ip_range.first;
            auto& range_vec = ip_range.second;
            nr_ranges_remaining += range_vec.size();
            logger.debug("Remaining: keyspace={}, source={}, ranges={}", keyspace, source, range_vec);
        }
    }
    return nr_ranges_remaining;
}

} // dht
