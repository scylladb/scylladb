use scylla_udf::{export_udf, Time, Timestamp};
use chrono::NaiveDate;
use uuid::Uuid;

// The order of arguments must match type_defs in uf_types_test.py.
type FirstTypes = (
    Option<Timestamp>,
    Option<NaiveDate>,
    Option<Time>,
    Option<Uuid>,
    Option<Uuid>,
    Option<i8>,
    Option<i16>,
    Option<i32>,
    Option<i64>,
    Option<f32>,
    Option<f64>,
    Option<bool>,
);

type SecondTypes = (
    Option<String>,
    Option<String>,
    Option<(i32, String)>,
);

#[export_udf]
fn check_arg_and_return_1(
    ts: Option<Timestamp>,
    dt: Option<NaiveDate>,
    tim: Option<Time>,
    uu: Option<Uuid>,
    tu: Option<Uuid>,
    ti: Option<i8>,
    si: Option<i16>,
    i: Option<i32>,
    bi: Option<i64>,
    f: Option<f32>,
    d: Option<f64>,
    x: Option<bool>,
) -> FirstTypes {
    (ts, dt, tim, uu, tu, ti, si, i, bi, f, d, x)
}

#[export_udf]
fn check_arg_and_return_2(
    a: Option<String>,
    txt: Option<String>,
    tup: Option<(i32, String)>,
) -> SecondTypes {
    (a, txt, tup)
}

#[export_udf]
fn called_on_null(
    _ts: Option<Timestamp>,
    _dt: Option<NaiveDate>,
    _tim: Option<Time>,
    _uu: Option<Uuid>,
    _tu: Option<Uuid>,
    _ti: Option<i8>,
    _si: Option<i16>,
    _i: Option<i32>,
    _bi: Option<i64>,
    _f: Option<f32>,
    _d: Option<f64>,
    _x: Option<bool>,
    _a: Option<String>,
    _txt: Option<String>,
    _tup: Option<(i32, String)>,
) -> Option<String> {
    Some("called".to_string())
}

#[export_udf]
fn returns_null_on_null(
    _ts: Timestamp,
    _dt: NaiveDate,
    _tim: Time,
    _uu: Uuid,
    _tu: Uuid,
    _ti: i8,
    _si: i16,
    _i: i32,
    _bi: i64,
    _f: f32,
    _d: f64,
    _x: bool,
    _a: String,
    _txt: String,
    _tup: (i32, String),
) -> String {
    "called".to_string()
}
