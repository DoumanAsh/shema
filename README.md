# shema

[![Rust](https://github.com/DoumanAsh/shema/actions/workflows/rust.yml/badge.svg)](https://github.com/DoumanAsh/shema/actions/workflows/rust.yml)
[![Crates.io](https://img.shields.io/crates/v/shema.svg)](https://crates.io/crates/shema)
[![Documentation](https://docs.rs/shema/badge.svg)](https://docs.rs/crate/shema/)

Derive macro to generate database schema code from Rust struct

All parameters are specified via `shema`

## Struct parameters

 - `firehose_schema` - Enables firehose schema generation
 - `firehose_partition_code` - Enables code generation to access partition information
 - `firehose_parquet_schema` - Enables parquet schema generation similar to AWS Glue's one

## Field parameters

- `json` - Specifies that field is to be encoded as json object (automatically derived for std's collections)
- `enumeration` - Specifies that field is to be encoded as enumeration (Depending on database, it will be encoded as string or object)
- `index` - Specifies that field is to be indexed by underlying database engine (e.g. to be declared a partition key in AWS glue schema)
- `firehose_date_index` - Specifies field to be used as timestamp within `firehose` schema which will produce `year`, `month` and `day` fields. Requires to be of `timestamp` type. E.g. [time::OffsetDateTime](https://docs.rs/time/0.3.44/time/struct.OffsetDateTime.html)
- `rename` - Tells to use different name for the field. Argument MUST be string specified as `rename = "new_name"`

### Firehose date index

If specified firehose output will expect RFC3339 encoded string as output during serialization

You should configure HIVE json deserializer with possible RFC3339 formats.

Terraform Reference: <https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/kinesis_firehose_delivery_stream#timestamp_formats-1>

## Schema output

### Following constants will be declared for affected structs:

- `SHEMA_TABLE_NAME` - table name in lower case
- `SHEMA_FIREHOSE_SCHEMA` - Firehose glue table schema. If enabled.
- `SHEMA_FIREHOSE_PARQUET_SCHEMA` - Partquet schema compatible with firehose data stream. If enabled.

### Following methods will be defined for affected structs

- `firehose_partition_keys_ref` - Returns tuple with references to partition keys
- `firehose_partition_keys` - Returns tuple with owned values of partition keys
- `firehose_s3_path_prefix` - Returns `fmt::Display` type that writes full path prefix for S3 destination object

### Firehose specifics

Firehose schema expects flat structure, so any complex struct or array must be serialized as strings

```rust
mod time {
    pub struct OffsetDateTime;
    impl OffsetDateTime {
        pub fn year(&self) -> i32 {
            2025
        }
        pub fn month(&self) -> i8 {
            12
        }
        pub fn day(&self) -> u8 {
            30
        }
    }
}
mod prost_wkt_types {
    pub struct Struct;
}


use std::fs;
use shema::Shema;

//Build context is relative to root of workspace so we point to crate's path
#[derive(Shema)]
#[shema(firehose_schema, firehose_parquet_schema, firehose_partition_code)]
pub(crate) struct Analytics<'a> {
    #[shema(index, firehose_date_index)]
    ///Special field that will be transformed in firehose as year,month,day
    r#client_time: time::OffsetDateTime,
    r#server_time: time::OffsetDateTime,
    r#user_id: Option<String>,
    #[shema(index)]
    ///Index key will go into firehose's partition_keys
    r#client_id: String,
    #[shema(index)]
    r#session_id: String,
    #[shema(json)]
    r#extras: Option<prost_wkt_types::Struct>,
    #[shema(json)]
    r#props: prost_wkt_types::Struct,
    r#name: String,

    byte: i8,
    short: i16,
    int: i32,
    long: i64,
    ptr: isize,

    float: f32,
    double: f64,
    boolean: bool,
    #[shema(rename = "stroka")]
    strka: &'a str,

    array: Vec<String>,
}

assert_eq!(Analytics::SHEMA_TABLE_NAME, "analytics");
```
