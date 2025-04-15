use scylla_udf::export_udf;

#[export_udf]
fn plus42(i1: i16, i2: i16) -> i16 {
    i1 + i2 + 42
}
