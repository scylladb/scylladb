/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <ranges>
#include <seastar/util/memory-data-source.hh>

#include "sstables/mx/partition_codec.hh"
#include "sstables/mx/partition_decoder.hh"
#include "sstables/mx/partition_encoder.hh"
#include "sstables/column_translation.hh"
#include "sstables/key.hh"
#include "sstables/sstables.hh"
#include "schema/schema.hh"
#include "utils/on_internal_error.hh"

namespace sstables::mx {

struct codec_schema::impl {
    codec_params params;
    schema_ptr reader_schema;
    // Null when built from a column_mapping: such a codec_schema only decodes.
    schema_ptr writer_schema;
    // The encoder reads only the column lists, so the header minimums stay at their defaults.
    std::optional<mc::sstable_schema> sst_schema;
    // Owns the column names translation points to, when built from a column_mapping.
    std::optional<column_mapping> writer_columns;
    column_translation translation;
};

codec_schema::codec_schema(lw_shared_ptr<const impl> impl) : _impl(std::move(impl)) {}
codec_schema::codec_schema(const codec_schema&) = default;
codec_schema::codec_schema(codec_schema&&) noexcept = default;
codec_schema& codec_schema::operator=(const codec_schema&) = default;
codec_schema& codec_schema::operator=(codec_schema&&) noexcept = default;
codec_schema::~codec_schema() = default;

const codec_schema::impl& codec_schema::get() const {
    return *_impl;
}

const std::vector<std::optional<uint32_t>>& codec_schema::clustering_key_fixed_lengths() const {
    return _impl->translation.clustering_column_value_fix_legths();
}

namespace {

// Neither can be strongly consistent, and the codec does not handle the column
// order of compact tables.
void check_supported(const schema& s) {
    if (s.is_counter() || s.is_compact_table()) {
        utils::on_internal_error(format("mx partition codec does not support counter or compact table {}.{}", s.ks_name(), s.cf_name()));
    }
}

class codec_observer {
public:
    void on_partition_tombstone(tombstone, const deletion_time&, uint64_t) {}
    void on_static_row_written(uint64_t) {}
    void on_row_written(const clustering_row&, uint64_t) {}
    void on_range_tombstone_written(const mc::rt_marker&) {}
    void on_clustered_written(const clustering_key_prefix&, bound_kind_m, tombstone, uint64_t) {}
    void on_cell_written(const clustering_key_prefix*, const column_definition&, atomic_cell_view,
            gc_clock::duration, gc_clock::time_point, uint64_t) {}
    void on_collection_written(const clustering_key_prefix*, const column_definition&, uint64_t, uint64_t) {}
    void on_liveness_info(const row_marker&, gc_clock::duration, gc_clock::time_point) {}
    void on_tombstone(const tombstone&) {}

    void on_capped_deletion_time(tombstone t, int32_t ldt) {
        sstlog.warn("Capping tombstone local_deletion_time {} to max {} in an encoded partition", t.deletion_time.time_since_epoch().count(), ldt);
    }
    void on_clamped_ttl(gc_clock::duration ttl) {
        static thread_local logger::rate_limit rl(std::chrono::minutes(1));
        sstlog.log(log_level::warn, rl, "Clamping too big ttl {} to max_ttl ({}) in an encoded partition", ttl.count(), max_ttl.count());
    }
};

// Initial chunk size of the buffer a row body is assembled in. Encoded partitions
// are small; the buffer grows its chunks for a large row.
constexpr size_t tmp_buf_size = 512;

// encode_partition() is synchronous and may run outside a seastar thread.
struct no_yield {
    static void maybe_yield() {}
};

using encoder_type = mc::partition_encoder<codec_observer, no_yield>;

class encoding_consumer {
    encoder_type& _encoder;
    const schema& _schema;
    bytes_ostream& _out;
public:
    encoding_consumer(encoder_type& encoder, const schema& s, bytes_ostream& out)
        : _encoder(encoder), _schema(s), _out(out)
    { }

    void consume_new_partition(const dht::decorated_key& dk) {
        auto pk = key::from_partition_key(_schema, dk.key());
        _encoder.consume_new_partition(_out, bytes_view(pk));
    }
    void consume(tombstone t) {
        _encoder.consume(_out, t);
    }
    stop_iteration consume(static_row&& sr) {
        _encoder.consume(_out, sr);
        return stop_iteration::no;
    }
    stop_iteration consume(clustering_row&& cr) {
        _encoder.consume(_out, cr);
        return stop_iteration::no;
    }
    stop_iteration consume(range_tombstone_change&& rtc) {
        _encoder.consume(_out, rtc);
        return stop_iteration::no;
    }
    stop_iteration consume_end_of_partition() {
        _encoder.ensure_partition_header_is_written(_out);
        _encoder.write_end_of_partition(_out);
        return stop_iteration::no;
    }
    void consume_end_of_stream() {}
};

column_translation::writer_column to_writer_column(const column_definition& def) {
    return {&def.name(), def.type};
}

column_translation::writer_column to_writer_column(const column_mapping_entry& entry) {
    return {&entry.name(), entry.type()};
}

template <typename Columns>
std::vector<column_translation::writer_column> to_writer_columns(const Columns& columns) {
    std::vector<column_translation::writer_column> ret;
    for (const auto& c : columns) {
        ret.push_back(to_writer_column(c));
    }
    return ret;
}

} // anonymous namespace

codec_schema make_codec_schema(const schema& writer_schema, const schema& reader_schema, codec_params params) {
    check_supported(writer_schema);
    check_supported(reader_schema);
    auto impl = make_lw_shared<codec_schema::impl>();
    impl->params = params;
    impl->reader_schema = reader_schema.shared_from_this();
    impl->writer_schema = writer_schema.shared_from_this();
    impl->sst_schema.emplace(mc::make_sstable_schema(writer_schema, encoding_stats{}));
    auto ck_fixed_lengths = writer_schema.clustering_key_columns()
            | std::views::transform([] (const column_definition& def) { return def.type->value_length_if_fixed(); })
            | std::ranges::to<column_values_fixed_lengths>();
    impl->translation = column_translation::make(reader_schema, to_writer_columns(writer_schema.regular_columns()),
            to_writer_columns(writer_schema.static_columns()), std::move(ck_fixed_lengths));
    return codec_schema(std::move(impl));
}

codec_schema make_codec_schema(const column_mapping& writer_columns,
        const std::vector<std::optional<uint32_t>>& ck_fixed_lengths,
        const schema& reader_schema, codec_params params) {
    check_supported(reader_schema);
    // ALTER TABLE cannot change the clustering key, so the writer had the same columns.
    // The decoder reads this many key components per row, so a wrong count misparses every row.
    if (ck_fixed_lengths.size() != reader_schema.clustering_key_size()) {
        utils::on_internal_error(format("make_codec_schema(): {} clustering key fixed lengths for the {} clustering columns of {}.{}",
                ck_fixed_lengths.size(), reader_schema.clustering_key_size(), reader_schema.ks_name(), reader_schema.cf_name()));
    }
    auto impl = make_lw_shared<codec_schema::impl>();
    impl->params = params;
    impl->reader_schema = reader_schema.shared_from_this();
    // A column_mapping lists columns in column id order, the order the encoder writes them in.
    const auto& columns = impl->writer_columns.emplace(writer_columns).columns();
    auto statics = std::ranges::subrange(columns.begin(), columns.begin() + impl->writer_columns->n_static());
    auto regulars = std::ranges::subrange(columns.begin() + impl->writer_columns->n_static(), columns.end());
    impl->translation = column_translation::make(reader_schema, to_writer_columns(regulars), to_writer_columns(statics), ck_fixed_lengths);
    return codec_schema(std::move(impl));
}

void encode_partition(const mutation& m, const encoding_stats& bases, const codec_schema& cs, bytes_ostream& out) {
    const auto& impl = cs.get();
    if (!impl.writer_schema) {
        utils::on_internal_error("encode_partition(): the codec_schema was built from a column_mapping and cannot encode");
    }
    const schema& s = *impl.writer_schema;
    if (m.schema()->version() != s.version()) {
        utils::on_internal_error(format("encode_partition(): mutation schema version {} differs from codec_schema writer version {}",
                m.schema()->version(), s.version()));
    }
    codec_observer observer;
    encoder_type encoder(s, *impl.sst_schema, impl.params.version, bases, observer, tmp_buf_size);
    encoding_consumer consumer(encoder, s, out);
    mutation(m).consume(consumer, consume_in_reverse::no);
}

future<mutation> decode_partition(schema_ptr s, const encoding_stats& bases, const codec_schema& cs,
        bytes_ostream in, reader_permit permit) {
    // The translation's column names live in impl; keep impl alive until the decode ends.
    const codec_schema holder = cs;
    const auto& impl = holder.get();
    if (s->version() != impl.reader_schema->version()) {
        utils::on_internal_error(format("decode_partition(): schema version {} differs from codec_schema reader version {}",
                s->version(), impl.reader_schema->version()));
    }
    const data_source_descriptor desc{
        // The encoder clamps the TTL base, so the decoder must use the same value.
        .bases = encoder_type::clamp_enc_stats_ttl(bases),
        .translation = impl.translation,
        // The encoder writes shadowable row tombstones whenever a row has one.
        .has_shadowable_tombstones = true,
        .stats = nullptr,
    };
    const uint64_t size = in.size();
    // The buffers point into in, which the coroutine frame keeps alive past the decode.
    std::vector<temporary_buffer<char>> buffers;
    for (bytes_view fragment : in) {
        buffers.emplace_back(const_cast<char*>(reinterpret_cast<const char*>(fragment.data())), fragment.size(), deleter());
    }
    co_return co_await decode_partition(std::move(s), desc, seastar::util::as_input_stream(std::move(buffers)), size, std::move(permit));
}

} // namespace sstables::mx
