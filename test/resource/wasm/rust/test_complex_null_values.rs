use scylla_udf::{export_udf, export_udt};
use std::collections::{BTreeMap, BTreeSet};

#[export_udf]
fn return_input_flist(x: Option<Vec<f64>>) -> Option<Vec<f64>> {
    x
}

#[export_udf]
fn return_input_fset(x: Option<BTreeSet<String>>) -> Option<BTreeSet<String>> {
    x
}

#[export_udf]
fn return_input_fmap(x: Option<BTreeMap<i32, bool>>) -> Option<BTreeMap<i32, bool>> {
    x
}

#[export_udf]
fn return_input_ftup(x: Option<(f64, String, i32, bool)>) -> Option<(f64, String, i32, bool)> {
    x
}

#[export_udt]
struct MyUdt {
    txt: String,
    i: i32,
}

#[export_udf]
fn return_input_fudt(x: Option<MyUdt>) -> Option<MyUdt> {
    x
}
