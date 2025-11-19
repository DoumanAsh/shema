mod prost_wkt_types {
    pub struct Struct;
}

use shema::Shema;

#[allow(unused)]
//Build context is relative to root of workspace so we point to crate's path
#[derive(Shema)]
#[shema(firehose_schema, firehose_parquet_schema)]
pub(crate) struct AnalyticsEvent<'a> {
    #[shema(index)]
    ///Index key will go into firehose's partition_keys
    r#client_id: String,
    #[shema(index)]
    #[shema(index, firehose_date_index)]
    ///Special field that will be transformed in firehose as year,month,day
    r#client_time: time::OffsetDateTime,
    r#server_time: time::OffsetDateTime,
    r#user_id: Option<String>,
    r#session_id: String,
    #[shema(json)]
    #[shema(rename = "extra")]
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

#[test]
fn should_verify_derive() {
    assert_eq!(AnalyticsEvent::SHEMA_TABLE_NAME, "analytics_event");

    println!("{}", AnalyticsEvent::SHEMA_FIREHOSE_PARQUET_SCHEMA);
    assert_eq!(
        AnalyticsEvent::SHEMA_FIREHOSE_PARQUET_SCHEMA,
        r#"message analytics_event {
  REQUIRED INT96 client_time;
  REQUIRED INT96 server_time;
  OPTIONAL BYTE_ARRAY user_id (UTF8);
  REQUIRED BYTE_ARRAY session_id (UTF8);
  OPTIONAL BYTE_ARRAY extra (UTF8);
  REQUIRED BYTE_ARRAY props (UTF8);
  REQUIRED BYTE_ARRAY name (UTF8);
  REQUIRED INT32 byte;
  REQUIRED INT32 short;
  REQUIRED INT32 int;
  REQUIRED INT64 long;
  REQUIRED INT64 ptr;
  REQUIRED FLOAT float;
  REQUIRED DOUBLE double;
  REQUIRED BOOLEAN boolean;
  REQUIRED BYTE_ARRAY stroka (UTF8);
  REQUIRED BYTE_ARRAY array (UTF8);
}"#);

    assert_eq!(
        AnalyticsEvent::SHEMA_FIREHOSE_SCHEMA,
        r#"{
  "name": "analytics_event",
  "partition_keys": [
    {
      "name": "year",
      "type": "string",
      "comment": "Extracted from 'client_time'",
      "mapping": "(.client_time|split(\"-\")[0])"
    },
    {
      "name": "month",
      "type": "string",
      "comment": "Extracted from 'client_time'",
      "mapping": "(.client_time|split(\"-\")[1])"
    },
    {
      "name": "day",
      "type": "string",
      "comment": "Extracted from 'client_time'",
      "mapping": "(.client_time|split(\"-\")[2]|split(\"T\")[0])"
    },
    {
      "name": "client_id",
      "type": "string",
      "comment": "Index key will go into firehose's partition_keys",
      "mapping": ".client_id"
    }
  ],
  "columns": [
    {
      "name": "client_time",
      "type": "timestamp",
      "comment": "Special field that will be transformed in firehose as year,month,day"
    },
    {
      "name": "server_time",
      "type": "timestamp",
      "comment": ""
    },
    {
      "name": "user_id",
      "type": "string",
      "comment": ""
    },
    {
      "name": "session_id",
      "type": "string",
      "comment": ""
    },
    {
      "name": "extra",
      "type": "string",
      "comment": ""
    },
    {
      "name": "props",
      "type": "string",
      "comment": ""
    },
    {
      "name": "name",
      "type": "string",
      "comment": ""
    },
    {
      "name": "byte",
      "type": "tinyint",
      "comment": ""
    },
    {
      "name": "short",
      "type": "smallint",
      "comment": ""
    },
    {
      "name": "int",
      "type": "int",
      "comment": ""
    },
    {
      "name": "long",
      "type": "bigint",
      "comment": ""
    },
    {
      "name": "ptr",
      "type": "bigint",
      "comment": ""
    },
    {
      "name": "float",
      "type": "float",
      "comment": ""
    },
    {
      "name": "double",
      "type": "double",
      "comment": ""
    },
    {
      "name": "boolean",
      "type": "boolean",
      "comment": ""
    },
    {
      "name": "stroka",
      "type": "string",
      "comment": ""
    },
    {
      "name": "array",
      "type": "string",
      "comment": ""
    }
  ]
}"#
    );
}
