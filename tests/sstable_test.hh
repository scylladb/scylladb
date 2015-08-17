#pragma once

#include "sstables/sstables.hh"
#include "schema.hh"
#include "schema_builder.hh"
#include "core/thread.hh"

static auto la = sstables::sstable::version_types::la;
static auto big = sstables::sstable::format_types::big;

class column_family_test {
    lw_shared_ptr<column_family> _cf;
public:
    column_family_test(lw_shared_ptr<column_family> cf) : _cf(cf) {}

    void add_sstable(sstables::sstable&& sstable) {
        auto generation = sstable.generation();
        _cf->_sstables->emplace(generation, make_lw_shared(std::move(sstable)));
    }
};

namespace sstables {

using sstable_ptr = lw_shared_ptr<sstable>;

class test {
    sstable_ptr _sst;
public:

    test(sstable_ptr s) : _sst(s) {}

    summary& _summary() {
        return _sst->_summary;
    }

    future<temporary_buffer<char>> data_read(uint64_t pos, size_t len) {
        return _sst->data_read(pos, len);
    }
    future<index_list> read_indexes(uint64_t position, uint64_t quantity) {
        return _sst->read_indexes(position, quantity);
    }

    future<> read_statistics() {
        return _sst->read_statistics();
    }

    statistics& get_statistics() {
        return _sst->_statistics;
    }

    future<> read_summary() {
        return _sst->read_summary();
    }

    future<summary_entry&> read_summary_entry(size_t i) {
        return _sst->read_summary_entry(i);
    }

    summary& get_summary() {
        return _sst->_summary;
    }

    future<> read_toc() {
        return _sst->read_toc();
    }

    auto& get_components() {
        return _sst->_components;
    }

    template <typename T>
    int binary_search(const T& entries, const key& sk) {
        return _sst->binary_search(entries, sk);
    }

    future<> store() {
        _sst->_components.erase(sstable::component_type::Index);
        _sst->_components.erase(sstable::component_type::Data);
        return seastar::async([sst = _sst] {
            sst->write_statistics();
            sst->write_compression();
            sst->write_filter();
            sst->write_summary();
            sst->write_toc();
        });
    }
};

inline future<sstable_ptr> reusable_sst(sstring dir, unsigned long generation) {
    auto sst = make_lw_shared<sstable>("ks", "cf", dir, generation, la, big);
    auto fut = sst->load();
    return std::move(fut).then([sst = std::move(sst)] {
        return make_ready_future<sstable_ptr>(std::move(sst));
    });
}

inline future<> working_sst(sstring dir, unsigned long generation) {
    return reusable_sst(dir, generation).then([] (auto ptr) { return make_ready_future<>(); });
}

inline schema_ptr composite_schema() {
    static thread_local auto s = [] {
        schema_builder builder(make_lw_shared(schema({}, "tests", "composite",
        // partition key
        {{"name", bytes_type}, {"col1", bytes_type}},
        // clustering key
        {},
        // regular columns
        {},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "Table with a composite key as pkey"
       )));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return s;
}

inline schema_ptr set_schema() {
    static thread_local auto s = [] {
        auto my_set_type = set_type_impl::get_instance(bytes_type, false);
        schema_builder builder(make_lw_shared(schema({}, "tests", "set_pk",
        // partition key
        {{"ss", my_set_type}},
        // clustering key
        {},
        // regular columns
        {
            {"ns", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "Table with a set as pkeys"
       )));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return s;
}

inline schema_ptr map_schema() {
    static thread_local auto s = [] {
        auto my_map_type = map_type_impl::get_instance(bytes_type, bytes_type, false);
        schema_builder builder(make_lw_shared(schema({}, "tests", "map_pk",
        // partition key
        {{"ss", my_map_type}},
        // clustering key
        {},
        // regular columns
        {
            {"ns", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "Table with a map as pkeys"
       )));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return s;
}

inline schema_ptr list_schema() {
    static thread_local auto s = [] {
        auto my_list_type = list_type_impl::get_instance(bytes_type, false);
        schema_builder builder(make_lw_shared(schema({}, "tests", "list_pk",
        // partition key
        {{"ss", my_list_type}},
        // clustering key
        {},
        // regular columns
        {
            {"ns", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "Table with a list as pkeys"
       )));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return s;
}

inline schema_ptr uncompressed_schema() {
    static thread_local auto uncompressed = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id("ks", "uncompressed"), "ks", "uncompressed",
        // partition key
        {{"name", utf8_type}},
        // clustering key
        {},
        // regular columns
        {{"col1", utf8_type}, {"col2", int32_type}},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "Uncompressed data"
       )));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return uncompressed;
}

inline schema_ptr complex_schema() {
    static thread_local auto s = [] {
        auto my_list_type = list_type_impl::get_instance(bytes_type, true);
        auto my_map_type = map_type_impl::get_instance(bytes_type, bytes_type, true);
        auto my_set_type = set_type_impl::get_instance(bytes_type, true);
        auto my_fset_type = set_type_impl::get_instance(bytes_type, false);
        auto my_set_static_type = set_type_impl::get_instance(bytes_type, true);

        schema_builder builder(make_lw_shared(schema({}, "tests", "complex_schema",
        // partition key
        {{"key", bytes_type}},
        // clustering key
        {{"clust1", bytes_type}, {"clust2", bytes_type}},
        // regular columns
        {
            {"reg_set", my_set_type},
            {"reg_list", my_list_type},
            {"reg_map", my_map_type},
            {"reg_fset", my_fset_type},
            {"reg", bytes_type},
        },
        // static columns
        {{"static_obj", bytes_type}, {"static_collection", my_set_static_type}},
        // regular column name type
        bytes_type,
        // comment
        "Table with a complex schema, including collections and static keys"
       )));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return s;
}

inline schema_ptr columns_schema() {
    static thread_local auto columns = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id("name", "columns"), "name", "columns",
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {{"columnfamily_name", utf8_type}, {"column_name", utf8_type}},
        // regular columns
        {
            {"component_index", int32_type},
            {"index_name", utf8_type},
            {"index_options", utf8_type},
            {"index_type", utf8_type},
            {"type", utf8_type},
            {"validator", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "column definitions"
       )));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return columns;
}

inline schema_ptr compact_simple_dense_schema() {
    static thread_local auto s = [] {
        schema_builder builder(make_lw_shared(schema({}, "tests", "compact_simple_dense",
        // partition key
        {{"ks", bytes_type}},
        // clustering key
        {{"cl1", bytes_type}},
        // regular columns
        {{"cl2", bytes_type}},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "Table with a compact storage, and a single clustering key"
       )));
       return builder.build(schema_builder::compact_storage::yes);
    }();
    return s;
}

inline schema_ptr compact_dense_schema() {
    static thread_local auto s = [] {
        schema_builder builder(make_lw_shared(schema({}, "tests", "compact_simple_dense",
        // partition key
        {{"ks", bytes_type}},
        // clustering key
        {{"cl1", bytes_type}, {"cl2", bytes_type}},
        // regular columns
        {{"cl3", bytes_type}},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "Table with a compact storage, and a compound clustering key"
       )));
       return builder.build(schema_builder::compact_storage::yes);
    }();
    return s;
}

inline schema_ptr compact_sparse_schema() {
    static thread_local auto s = [] {
        schema_builder builder(make_lw_shared(schema({}, "tests", "compact_sparse",
        // partition key
        {{"ks", bytes_type}},
        // clustering key
        {},
        // regular columns
        {
            {"cl1", bytes_type},
            {"cl2", bytes_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "Table with a compact storage, but no clustering keys"
       )));
       return builder.build(schema_builder::compact_storage::yes);
    }();
    return s;
}

enum class status {
    dead,
    live,
    ttl,
};

inline bool check_status_and_done(const atomic_cell &c, status expected) {
    if (expected == status::dead) {
        BOOST_REQUIRE(c.is_live() == false);
        return true;
    }
    BOOST_REQUIRE(c.is_live() == true);
    BOOST_REQUIRE(c.is_live_and_has_ttl() == (expected == status::ttl));
    return false;
}

template <status Status>
inline void match(const row& row, const schema& s, bytes col, const boost::any& value, int64_t timestamp = 0, int32_t expiration = 0) {
    auto cdef = s.get_column_definition(col);

    BOOST_CHECK_NO_THROW(row.cell_at(cdef->id));
    auto c = row.cell_at(cdef->id).as_atomic_cell();
    if (check_status_and_done(c, Status)) {
        return;
    }

    auto expected = cdef->type->decompose(value);
    BOOST_REQUIRE(c.value() == expected);
    if (timestamp) {
        BOOST_REQUIRE(c.timestamp() == timestamp);
    }
    if (expiration) {
        BOOST_REQUIRE(c.expiry() == gc_clock::time_point(gc_clock::duration(expiration)));
    }
}

inline void match_live_cell(const row& row, const schema& s, bytes col, const boost::any& value) {
    match<status::live>(row, s, col, value);
}

inline void match_expiring_cell(const row& row, const schema& s, bytes col, const boost::any& value, int64_t timestamp, int32_t expiration) {
    match<status::ttl>(row, s, col, value);
}

inline void match_dead_cell(const row& row, const schema& s, bytes col) {
    match<status::dead>(row, s, col, boost::any({}));
}

inline void match_absent(const row& row, const schema& s, bytes col) {
    auto cdef = s.get_column_definition(col);
    BOOST_REQUIRE_THROW(row.cell_at(cdef->id), std::out_of_range);
}

inline collection_type_impl::mutation
match_collection(const row& row, const schema& s, bytes col, const tombstone& t) {
    auto cdef = s.get_column_definition(col);

    BOOST_CHECK_NO_THROW(row.cell_at(cdef->id));
    auto c = row.cell_at(cdef->id).as_collection_mutation();
    auto ctype = static_pointer_cast<const collection_type_impl>(cdef->type);
    auto&& mut = ctype->deserialize_mutation_form(c);
    BOOST_REQUIRE(mut.tomb == t);
    return mut.materialize();
}

template <status Status>
inline void match_collection_element(const std::pair<bytes, atomic_cell>& element, const bytes_opt& col, const bytes_opt& expected_serialized_value) {
    if (col) {
        BOOST_REQUIRE(element.first == *col);
    }

    if (check_status_and_done(element.second, Status)) {
        return;
    }

    // For simplicity, we will have all set elements in our schema presented as
    // bytes - which serializes to itself.  Then we don't need to meddle with
    // the schema for the set type, and is enough for the purposes of this
    // test.
    if (expected_serialized_value) {
        BOOST_REQUIRE(element.second.value() == *expected_serialized_value);
    }
}

class test_setup {
    file _f;
    std::function<future<> (directory_entry de)> _walker;
    sstring _path;
    subscription<directory_entry> _listing;

    static sstring& path() {
        static sstring _p = "tests/sstables/tests-temporary";
        return _p;
    };

public:
    test_setup(file f, sstring path)
            : _f(std::move(f))
            , _path(path)
            , _listing(_f.list_directory([this] (directory_entry de) { return _remove(de); })) {
    }
    ~test_setup() {
        _f.close().finally([save = _f] {});
    }
protected:
    future<> _create_directory(sstring name) {
        return engine().make_directory(name);
    }

    future<> _remove(directory_entry de) {
        sstring t = _path + "/" + de.name;
        return engine().file_type(t).then([t] (std::experimental::optional<directory_entry_type> det) {
            auto f = make_ready_future<>();

            if (!det) {
                throw std::runtime_error("Can't determine file type\n");
            } else if (det == directory_entry_type::directory) {
                f = empty_test_dir(t);
            }
            return f.then([t] {
                return engine().remove_file(t);
            });
        });
    }
    future<> done() { return _listing.done(); }

    static future<> empty_test_dir(sstring p = path()) {
        return engine().open_directory(p).then([p] (file f) {
            auto l = make_lw_shared<test_setup>(std::move(f), p);
            return l->done().then([l] { });
        });
    }
public:
    static future<> create_empty_test_dir(sstring p = path()) {
        return engine().make_directory(p).then_wrapped([p] (future<> f) {
            try {
                f.get();
            // it's fine if the directory exists, just shut down the exceptional future message
            } catch (std::exception& e) {}
            return empty_test_dir(p);
        });
    }

    static future<> do_with_test_directory(std::function<future<> ()>&& fut, sstring p = path()) {
        return test_setup::create_empty_test_dir(p).then([fut = std::move(fut), p] () mutable {
            return fut();
        }).finally([p] {
            return test_setup::empty_test_dir(p).then([p] {
                return engine().remove_file(p);
            });
        });
    }
};
}
