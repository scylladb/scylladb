use scylla_udf::{export_udf, export_udt};
use std::collections::{BTreeMap, BTreeSet};

#[export_udf]
fn sum_set(set: Option<BTreeSet<i32>>) -> Option<i32> {
    match set {
        Some(set) => Some(set.iter().sum()),
        None => Some(0),
    }
}

#[export_udf]
fn return_set(set: Option<BTreeSet<i32>>) -> Option<BTreeSet<i32>> {
    set
}

#[export_udf]
fn sum_list(list: Option<Vec<i32>>) -> Option<i32> {
    match list {
        Some(list) => Some(list.iter().sum()),
        None => Some(0),
    }
}

#[export_udf]
fn return_list(list: Option<Vec<i32>>) -> Option<Vec<i32>> {
    list
}

#[export_udf]
fn sum_map(map: Option<BTreeMap<i32, i32>>) -> Option<i32> {
    match map {
        Some(map) => Some(map.values().sum()),
        None => Some(0),
    }
}

#[export_udf]
fn return_map(map: Option<BTreeMap<i32, i32>>) -> Option<BTreeMap<i32, i32>> {
    map
}

#[export_udf]
fn return_tuple(tuple: Option<(Option<i32>, Option<i32>)>) -> Option<(Option<i32>, Option<i32>)> {
    tuple
}

#[export_udf]
fn tostring_tuple(tuple: Option<(Option<i32>, Option<i32>)>) -> Option<String> {
    Some(format!("{:?}", tuple))
}

#[export_udt]
#[derive(Debug)]
struct Udt {
    f: Option<i32>,
}

#[export_udf]
fn return_udt(udt: Option<Udt>) -> Option<Udt> {
    udt
}

#[export_udf]
fn tostring_udt(udt: Option<Udt>) -> Option<String> {
    Some(format!("{:?}", udt))
}
