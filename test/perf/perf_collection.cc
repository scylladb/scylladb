/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>
#include <algorithm>
#include <vector>
#include <random>
#include <fmt/core.h>
#include "perf.hh"

using per_key_t = int64_t;

struct perf_key_compare {
    bool operator()(const per_key_t& a, const per_key_t& b) const noexcept { return a < b; }
    int64_t simplify_key(per_key_t k) noexcept {
        return k;
    }
};

struct perf_key_tri_compare {
    std::strong_ordering operator()(const per_key_t& a, const per_key_t& b) const noexcept {
        return a <=> b;
    }
};

#include "utils/assert.hh"
#include "utils/bptree.hh"

using namespace seastar;

/* On node size 32 and less linear search works better */
using test_bplus_tree = bplus::tree<per_key_t, unsigned long, perf_key_compare, 4, bplus::key_search::linear>;

static_assert(bplus::SimpleLessCompare<int64_t, perf_key_compare>);

#include "utils/intrusive_btree.hh"

class perf_intrusive_key {
    per_key_t _k;
public:
    intrusive_b::member_hook hook;

    explicit perf_intrusive_key(per_key_t v) noexcept : _k(v) {}
    perf_intrusive_key(perf_intrusive_key&&) noexcept = default;
    perf_intrusive_key(const perf_intrusive_key&) = delete;

    struct tri_compare {
        perf_key_tri_compare _cmp;
        std::strong_ordering operator()(const per_key_t a, const per_key_t b) const noexcept { return _cmp(a, b); }
        std::strong_ordering operator()(const perf_intrusive_key& a, const perf_intrusive_key& b) const noexcept { return _cmp(a._k, b._k); }
        std::strong_ordering operator()(const per_key_t a, const perf_intrusive_key& b) const noexcept { return _cmp(a, b._k); }
        std::strong_ordering operator()(const perf_intrusive_key& a, const per_key_t b) const noexcept { return _cmp(a._k, b); }
    };
};

using test_b_tree = intrusive_b::tree<perf_intrusive_key, &perf_intrusive_key::hook, perf_intrusive_key::tri_compare, 12, 18, intrusive_b::key_search::linear>;

class collection_tester {
public:
    virtual void insert(per_key_t k) = 0;
    virtual void lower_bound(per_key_t k) = 0;
    virtual void scan(int batch) = 0;
    virtual void erase(per_key_t k) = 0;
    virtual void drain(int batch) = 0;
    virtual void clear() = 0;
    virtual void clone() = 0;
    virtual void show_stats() = 0;
    virtual void insert_and_erase(per_key_t k) = 0;
    virtual ~collection_tester() {};
};

template <typename Col>
void scan_collection(Col& c, int batch) {
    int x = 0;
    auto i = c.begin();
    while (i != c.end()) {
        i++;
        if (++x % batch == 0) {
            seastar::thread::yield();
        }
    }
}

#include "utils/bptree.hh"

class bptree_tester : public collection_tester {
    /* On node size 32 (this test) linear search works better */
    using test_tree = bplus::tree<per_key_t, unsigned long, perf_key_compare, 4, bplus::key_search::linear>;

    test_tree _t;
public:
    bptree_tester() : _t(perf_key_compare{}) {}
    virtual void insert(per_key_t k) override { _t.emplace(k, 0); }
    virtual void lower_bound(per_key_t k) override {
        auto i = _t.lower_bound(k);
        SCYLLA_ASSERT(i != _t.end());
    }
    virtual void scan(int batch) override {
        scan_collection(_t, batch);
    }
    virtual void erase(per_key_t k) override { _t.erase(k); }
    virtual void drain(int batch) override {
        int x = 0;
        auto i = _t.begin();
        while (i != _t.end()) {
            i = i.erase(perf_key_compare{});
            if (++x % batch == 0) {
                seastar::thread::yield();
            }
        }
    }
    virtual void clear() override { _t.clear(); }
    virtual void clone() override { }
    virtual void insert_and_erase(per_key_t k) override {
        auto i = _t.emplace(k, 0);
        SCYLLA_ASSERT(i.second);
        i.first.erase(perf_key_compare{});
    }
    virtual void show_stats() override {
        struct bplus::stats st = _t.get_stats();
        fmt::print("nodes:     {}\n", st.nodes);
        for (int i = 0; i < (int)st.nodes_filled.size(); i++) {
            fmt::print("   {}: {} ({}%)\n", i, st.nodes_filled[i], st.nodes_filled[i] * 100 / st.nodes);
        }
        fmt::print("leaves:    {}\n", st.leaves);
        for (int i = 0; i < (int)st.leaves_filled.size(); i++) {
            fmt::print("   {}: {} ({}%)\n", i, st.leaves_filled[i], st.leaves_filled[i] * 100 / st.leaves);
        }
        fmt::print("datas:     {}\n", st.datas);
    }
    virtual ~bptree_tester() { clear(); }
};

#include "utils/compact-radix-tree.hh"

class radix_tester : public collection_tester {
public:
    using test_tree = compact_radix_tree::tree<unsigned long>;

private:
    test_tree _t;
public:
    radix_tester() : _t() {}
    virtual void insert(per_key_t k) override { _t.emplace(k, 0); }
    virtual void lower_bound(per_key_t k) override {
        auto i = _t.get(k);
        SCYLLA_ASSERT(i != nullptr);
    }
    virtual void scan(int batch) override {
        scan_collection(_t, batch);
    }
    virtual void erase(per_key_t k) override { _t.erase(k); }
    virtual void drain(int batch) override {
        int x = 0;
        while (!_t.empty()) {
            _t.erase(_t.begin().key());
            if (++x % batch == 0) {
                seastar::thread::yield();
            }
        }
    }
    virtual void clear() override { _t.clear(); }
    virtual void clone() override {
        test_tree ct;
        ct.clone_from(_t, [] (unsigned, const unsigned long& data) { return data; });
    }
    virtual void insert_and_erase(per_key_t k) override {
        _t.emplace(k, 0);
        _t.erase(k);
    }
    void show_node_stats(std::string typ, typename test_tree::stats::node_stats& st) {
        fmt::print("{}: indirect: {}/{}/{}/{}  direct: static {} dynamic {}\n", typ,
                st.indirect_tiny, st.indirect_small, st.indirect_medium, st.indirect_large,
                st.direct_static, st.direct_dynamic);
    }
    virtual void show_stats() override {
        test_tree::stats st = _t.get_stats();
        show_node_stats("inner", st.inners);
        show_node_stats(" leaf", st.leaves);
    }
    virtual ~radix_tester() override { clear(); }
};

class btree_tester : public collection_tester {
    test_b_tree _t;
    perf_intrusive_key::tri_compare _cmp;
public:
    btree_tester() : _t() {}
    virtual void insert(per_key_t k) override { _t.insert(std::make_unique<perf_intrusive_key>(k), _cmp); }
    virtual void lower_bound(per_key_t k) override {
        auto i = _t.lower_bound(k, _cmp);
        SCYLLA_ASSERT(i != _t.end());
    }
    virtual void erase(per_key_t k) override { _t.erase_and_dispose(k, _cmp, [] (perf_intrusive_key* k) noexcept { delete k; }); }
    virtual void drain(int batch) override {
        int x = 0;
        perf_intrusive_key* k;
        while ((k = _t.unlink_leftmost_without_rebalance()) != nullptr) {
            delete k;
            if (++x % batch == 0) {
                seastar::thread::yield();
            }
        }
    }
    virtual void scan(int batch) override {
        scan_collection(_t, batch);
    }
    virtual void clear() override {
        _t.clear_and_dispose([] (perf_intrusive_key* k) noexcept { delete k; });
    }
    virtual void clone() override { }
    virtual void insert_and_erase(per_key_t k) override {
        auto i = _t.insert_before(_t.end(), std::make_unique<perf_intrusive_key>(k));
        _t.erase_and_dispose(i, [] (perf_intrusive_key* k) noexcept { delete k; });
    }
    virtual void show_stats() override {
        struct intrusive_b::stats st = _t.get_stats();
        fmt::print("nodes:     {}\n", st.nodes);
        for (int i = 0; i < (int)st.nodes_filled.size(); i++) {
            fmt::print("   {}: {} ({}%)\n", i, st.nodes_filled[i], st.nodes_filled[i] * 100 / st.nodes);
        }
        fmt::print("leaves:    {}\n", st.leaves);
        for (int i = 0; i < (int)st.leaves_filled.size(); i++) {
            fmt::print("   {}: {} ({}%)\n", i, st.leaves_filled[i], st.leaves_filled[i] * 100 / st.leaves);
        }
    }
    virtual ~btree_tester() {
        _t.clear();
    }
};

class set_tester : public collection_tester {
    std::set<per_key_t> _s;
public:
    virtual void insert(per_key_t k) override { _s.insert(k); }
    virtual void lower_bound(per_key_t k) override {
        auto i = _s.lower_bound(k);
        SCYLLA_ASSERT(i != _s.end());
    }
    virtual void scan(int batch) override {
        scan_collection(_s, batch);
    }
    virtual void erase(per_key_t k) override { _s.erase(k); }
    virtual void drain(int batch) override {
        int x = 0;
        auto i = _s.begin();
        while (i != _s.end()) {
            i = _s.erase(i);
            if (++x % batch == 0) {
                seastar::thread::yield();
            }
        }
    }
    virtual void clear() override { _s.clear(); }
    virtual void clone() override { }
    virtual void insert_and_erase(per_key_t k) override {
        auto i = _s.insert(k);
        SCYLLA_ASSERT(i.second);
        _s.erase(i.first);
    }
    virtual void show_stats() override { }
    virtual ~set_tester() = default;
};

class map_tester : public collection_tester {
    std::map<per_key_t, unsigned long> _m;
public:
    virtual void insert(per_key_t k) override { _m[k] = 0; }
    virtual void lower_bound(per_key_t k) override {
        auto i = _m.lower_bound(k);
        SCYLLA_ASSERT(i != _m.end());
    }
    virtual void scan(int batch) override {
        scan_collection(_m, batch);
    }
    virtual void erase(per_key_t k) override { _m.erase(k); }
    virtual void drain(int batch) override {
        int x = 0;
        auto i = _m.begin();
        while (i != _m.end()) {
            i = _m.erase(i);
            if (++x % batch == 0) {
                seastar::thread::yield();
            }
        }
    }
    virtual void clear() override { _m.clear(); }
    virtual void clone() override { }
    virtual void insert_and_erase(per_key_t k) override {
        auto i = _m.insert({k, 0});
        SCYLLA_ASSERT(i.second);
        _m.erase(i.first);
    }
    virtual void show_stats() override { }
    virtual ~map_tester() = default;
};

int main(int argc, char **argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("count", bpo::value<int>()->default_value(5000000), "number of keys to fill the tree with")
        ("batch", bpo::value<int>()->default_value(50), "number of operations between deferring points")
        ("iters", bpo::value<int>()->default_value(1), "number of iterations")
        ("col", bpo::value<std::string>()->default_value("bptree"), "collection to test")
        ("test", bpo::value<std::string>()->default_value("erase"), "what to test (erase, drain, clear, find, oneshot)")
        ("stats", bpo::value<bool>()->default_value(false), "show stats");

    return app.run(argc, argv, [&app] {
        auto count = app.configuration()["count"].as<int>();
        auto iters = app.configuration()["iters"].as<int>();
        auto batch = app.configuration()["batch"].as<int>();
        auto col = app.configuration()["col"].as<std::string>();
        auto tst = app.configuration()["test"].as<std::string>();
        auto stats = app.configuration()["stats"].as<bool>();

        return seastar::async([count, iters, batch, col, tst, stats] {
            std::unique_ptr<collection_tester> c;

            if (col == "bptree") {
                c = std::make_unique<bptree_tester>();
            } else if (col == "btree") {
                c = std::make_unique<btree_tester>();
            } else if (col == "set") {
                c = std::make_unique<set_tester>();
            } else if (col == "map") {
                c = std::make_unique<map_tester>();
            } else if (col == "radix") {
                c = std::make_unique<radix_tester>();
            } else {
                fmt::print("Unknown collection\n");
                return;
            }

            std::vector<per_key_t> keys;

            for (per_key_t i = 0; i < count; i++) {
                keys.push_back(i + 1);
            }

            std::random_device rd;
            std::mt19937 g(rd());

            fmt::print("Inserting {:d} k:v pairs into {} {:d} times\n", count, col, iters);

            for (auto rep = 0; rep < iters; rep++) {
                std::shuffle(keys.begin(), keys.end(), g);
                seastar::thread::yield();

                if (tst == "oneshot") {
                    auto d = duration_in_seconds([&] {
                        for (int i = 0; i < count; i++) {
                            c->insert_and_erase(keys[i]);
                            if ((i + 1) % batch == 0) {
                                seastar::thread::yield();
                            }
                        }
                    });

                    fmt::print("one-shot: {:.6f} ms\n", d.count() * 1000);
                    continue;
                }

                auto d = duration_in_seconds([&] {
                    for (int i = 0; i < count; i++) {
                        c->insert(keys[i]);
                        if ((i + 1) % batch == 0) {
                            seastar::thread::yield();
                        }
                    }
                });

                fmt::print("fill: {:.6f} ms\n", d.count() * 1000);

                if (stats) {
                    c->show_stats();
                }

                if (tst == "erase") {
                    std::shuffle(keys.begin(), keys.end(), g);
                    seastar::thread::yield();

                    d = duration_in_seconds([&] {
                        for (int i = 0; i < count; i++) {
                            c->erase(keys[i]);
                            if ((i + 1) % batch == 0) {
                                seastar::thread::yield();
                            }
                        }
                    });

                    fmt::print("erase: {:.6f} ms\n", d.count() * 1000);
                } else if (tst == "drain") {
                    d = duration_in_seconds([&] {
                        c->drain(batch);
                    });

                    fmt::print("drain: {:.6f} ms\n", d.count() * 1000);
                } else if (tst == "clear") {
                    d = duration_in_seconds([&] {
                        c->clear();
                    });

                    fmt::print("clear: {:.6f} ms\n", d.count() * 1000);
                } else if (tst == "find") {
                    std::shuffle(keys.begin(), keys.end(), g);
                    seastar::thread::yield();

                    d = duration_in_seconds([&] {
                        for (int i = 0; i < count; i++) {
                            c->lower_bound(keys[i]);
                            if ((i + 1) % batch == 0) {
                                seastar::thread::yield();
                            }
                        }
                    });

                    fmt::print("find: {:.6f} ms\n", d.count() * 1000);
                } else if (tst == "scan") {
                    d = duration_in_seconds([&] {
                        c->scan(batch);
                    });

                    fmt::print("scan: {:.6f} ms\n", d.count() * 1000);
                } else if (tst == "clone") {
                    d = duration_in_seconds([&] {
                        c->clone();
                    });
                    fmt::print("clone: {:.6f} ms\n", d.count() * 1000);
                }

                c->clear();
            }
        });
    });
}
