#include "database.hh"
#include "perf.hh"

static atomic_cell make_atomic_cell(bytes value) {
    return atomic_cell::make_live(0, ttl_opt{}, value);
};

int main(int argc, char* argv[]) {
    auto s = make_lw_shared(schema("ks", "cf",
        {{"p1", utf8_type}}, {{"c1", int32_type}}, {{"r1", int32_type}}, {}, utf8_type));

    column_family cf(s);

    std::cout << "Timing mutation of single column within one row...\n";

    auto key = partition_key::one::from_exploded(*s, {to_bytes("key1")});
    auto c_key = clustering_key::one::from_exploded(*s, {int32_type->decompose(2)});
    bytes value = int32_type->decompose(3);

    time_it([&] {
        mutation m(key, s);
        column_definition& col = *s->get_column_definition("r1");
        m.set_clustered_cell(c_key, col, make_atomic_cell(value));
        cf.apply(std::move(m));
    });
}
