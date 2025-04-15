use scylla_udf::{export_udf, Time, Timestamp};
use chrono::NaiveDate;
use uuid::Uuid;


#[export_udf]
fn check_arg_and_return_ts(x: Option<Timestamp>) -> Option<Timestamp> {
    x
}

#[export_udf]
fn called_on_null_ts(_: Option<Timestamp>) -> Option<String> {
    Some("called".to_string())
}

#[export_udf]
fn returns_null_on_null_ts(_: Timestamp) -> String {
    "called".to_string()
}

#[export_udf]
fn check_arg_and_return_dt(x: Option<NaiveDate>) -> Option<NaiveDate> {
    x
}

#[export_udf]
fn called_on_null_dt(_: Option<NaiveDate>) -> Option<String> {
    Some("called".to_string())
}

#[export_udf]
fn returns_null_on_null_dt(_: NaiveDate) -> String {
    "called".to_string()
}

#[export_udf]
fn check_arg_and_return_tim(x: Option<Time>) -> Option<Time> {
    x
}

#[export_udf]
fn called_on_null_tim(_: Option<Time>) -> Option<String> {
    Some("called".to_string())
}

#[export_udf]
fn returns_null_on_null_tim(_: Time) -> String {
    "called".to_string()
}

#[export_udf]
fn check_arg_and_return_uu(x: Option<Uuid>) -> Option<Uuid> {
    x
}

#[export_udf]
fn called_on_null_uu(_: Option<Uuid>) -> Option<String> {
    Some("called".to_string())
}

#[export_udf]
fn returns_null_on_null_uu(_: Uuid) -> String {
    "called".to_string()
}

#[export_udf]
fn check_arg_and_return_tu(x: Option<Uuid>) -> Option<Uuid> {
    x
}

#[export_udf]
fn called_on_null_tu(_: Option<Uuid>) -> Option<String> {
    Some("called".to_string())
}

#[export_udf]
fn returns_null_on_null_tu(_: Uuid) -> String {
    "called".to_string()
}


#[export_udf]
fn check_arg_and_return_ti(x: Option<i8>) -> Option<i8> {
    x
}

#[export_udf]
fn called_on_null_ti(_: Option<i8>) -> Option<String> {
    Some("called".to_string())
}

#[export_udf]
fn returns_null_on_null_ti(_: i8) -> String {
    "called".to_string()
}

#[export_udf]
fn check_arg_and_return_si(x: Option<i16>) -> Option<i16> {
    x
}

#[export_udf]
fn called_on_null_si(_: Option<i16>) -> Option<String> {
    Some("called".to_string())
}

#[export_udf]
fn returns_null_on_null_si(_: i16) -> String {
    "called".to_string()
}

#[export_udf]
fn check_arg_and_return_i(x: Option<i32>) -> Option<i32> {
    x
}

#[export_udf]
fn called_on_null_i(_: Option<i32>) -> Option<String> {
    Some("called".to_string())
}

#[export_udf]
fn returns_null_on_null_i(_: i32) -> String {
    "called".to_string()
}

#[export_udf]
fn check_arg_and_return_bi(x: Option<i64>) -> Option<i64> {
    x
}

#[export_udf]
fn called_on_null_bi(_: Option<i64>) -> Option<String> {
    Some("called".to_string())
}

#[export_udf]
fn returns_null_on_null_bi(_: i64) -> String {
    "called".to_string()
}

#[export_udf]
fn check_arg_and_return_f(x: Option<f32>) -> Option<f32> {
    x
}

#[export_udf]
fn called_on_null_f(_: Option<f32>) -> Option<String> {
    Some("called".to_string())
}

#[export_udf]
fn returns_null_on_null_f(_: f32) -> String {
    "called".to_string()
}

#[export_udf]
fn check_arg_and_return_d(x: Option<f64>) -> Option<f64> {
    x
}

#[export_udf]
fn called_on_null_d(_: Option<f64>) -> Option<String> {
    Some("called".to_string())
}

#[export_udf]
fn returns_null_on_null_d(_: f64) -> String {
    "called".to_string()
}

#[export_udf]
fn check_arg_and_return_x(x: Option<bool>) -> Option<bool> {
    x
}

#[export_udf]
fn called_on_null_x(_: Option<bool>) -> Option<String> {
    Some("called".to_string())
}

#[export_udf]
fn returns_null_on_null_x(_: bool) -> String {
    "called".to_string()
}

#[export_udf]
fn check_arg_and_return_a(x: Option<String>) -> Option<String> {
    x
}

#[export_udf]
fn called_on_null_a(_: Option<String>) -> Option<String> {
    Some("called".to_string())
}

#[export_udf]
fn returns_null_on_null_a(_: String) -> String {
    "called".to_string()
}

#[export_udf]
fn check_arg_and_return_txt(x: Option<String>) -> Option<String> {
    x
}

#[export_udf]
fn called_on_null_txt(_: Option<String>) -> Option<String> {
    Some("called".to_string())
}

#[export_udf]
fn returns_null_on_null_txt(_: String) -> String {
    "called".to_string()
}

// # TypesTestDef(type, f"frozen<{type}>", "u", null),

#[export_udf]
fn check_arg_and_return_tup(x: Option<(i32, String)>) -> Option<(i32, String)> {
    x
}

#[export_udf]
fn called_on_null_tup(_: Option<(i32, String)>) -> Option<String> {
    Some("called".to_string())
}

#[export_udf]
fn returns_null_on_null_tup(_: (i32, String)) -> String {
    "called".to_string()
}
