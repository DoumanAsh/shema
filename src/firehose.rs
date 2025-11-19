use std::{fmt, io};
use std::borrow::Cow;

use crate::{TAB, TableSchema, Field, FieldType, FieldFlag};

impl FieldType {
    #[inline(always)]
    pub fn aws_glue_type(&self) -> &'static str {
        //Reference: https://github.com/apache/hive/blob/46f2783164584bddfef6efdc4d84586bb3114ba1/standalone-metastore/metastore-common/src/main/java/org/apache/hadoop/hive/metastore/ColumnType.java#L38
        match self {
            Self::Byte => "tinyint",
            Self::Short => "smallint",
            Self::Integer => "int",
            Self::Long => "bigint",
            Self::Float => "float",
            Self::Double => "double",
            Self::String => "string",
            Self::Boolean => "boolean",
            Self::TimestampZ => "timestamp",
            //Enout all arrays/objects as strings
            Self::Array | Self::Object | Self::Enum => "string",
        }
    }
}

#[derive(Copy, Clone)]
pub struct FirehoseInput<'a> {
    pub schema: &'a TableSchema,
    pub index_time_field: Option<&'a Field>
}

#[derive(serde_derive::Serialize)]
struct FirehoseType<'a> {
    name: &'a str,
    #[serde(rename = "type")]
    typ: &'a str,
    comment: Cow<'a, str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mapping: Option<String>,
}

#[derive(serde_derive::Serialize)]
struct FirehoseSchema<'a> {
    name: String,
    partition_keys: Vec<FirehoseType<'a>>,
    columns: Vec<FirehoseType<'a>>,
}

pub fn generate_firehose_schema<O: io::Write>(FirehoseInput { schema, index_time_field }: FirehoseInput<'_>, out: &mut O) -> io::Result<()> {
    let mut out_schema = FirehoseSchema {
        name: schema.lower_cased_table_name(),
        partition_keys: Vec::new(),
        columns: Vec::new(),
    };

    if let Some(field) =  index_time_field {
        let name = field.table_field_name();
        let comment = format!("Extracted from '{name}'");
        out_schema.partition_keys.push(FirehoseType {
            name: "year",
            typ: "string",
            comment: comment.clone().into(),
            mapping: Some(format!("(.{name}|split(\"-\")[0])")),
        });
        out_schema.partition_keys.push(FirehoseType {
            name: "month",
            typ: "string",
            comment: comment.clone().into(),
            mapping: Some(format!("(.{name}|split(\"-\")[1])")),
        });
        out_schema.partition_keys.push(FirehoseType {
            name: "day",
            typ: "string",
            comment: comment.into(),
            mapping: Some(format!("(.{name}|split(\"-\")[2]|split(\"T\")[0])")),
        });
    }

    for field in schema.fields.iter() {
        let name = field.table_field_name();
        let mut firehose_field = FirehoseType {
            name,
            typ: field.typ.aws_glue_type(),
            comment: field.docstring.as_str().into(),
            mapping: None,
        };

        //Firehose's date index field should be pushed to the column as it is to be preserved accurately
        if field.typ_flags.is_type_flag(FieldFlag::Index) && !field.typ_flags.is_type_flag(FieldFlag::FirehoseDateIndex) {
            //Put index into partition key by mapping field as it is
            firehose_field.mapping = Some(format!(".{name}"));
            out_schema.partition_keys.push(firehose_field);
        } else {
            out_schema.columns.push(firehose_field)
        }
    }

    serde_json::to_writer_pretty(&mut *out, &out_schema).map_err(|error| io::Error::other(error))?;
    out.flush()
}

pub fn generate_firehose_partition_accessor<O: fmt::Write>(FirehoseInput { schema, index_time_field }: FirehoseInput<'_>, out: &mut O) -> fmt::Result {
    use fmt::Write;

    let mut reference_type = String::new();
    reference_type.push('(');
    if index_time_field.is_some() {
        reference_type.push_str("i32,u8,u8,");
    }
    for field in schema.fields.iter() {
        if field.typ_flags.is_type_flag(FieldFlag::Index) && !field.typ_flags.is_type_flag(FieldFlag::FirehoseDateIndex) {
            write!(reference_type, "&'_int {},", field.original_type)?;
        }
    }
    reference_type.push(')');

    writeln!(out, "{TAB}///Returns tuple with reference to all partition keys")?;
    writeln!(out, "{TAB}pub fn partition_keys_ref<'_int>(&'_int self) -> {reference_type} {{")?;
    write!(out, "{TAB}{TAB}(")?;
    if let Some(time_field) = index_time_field {
        write!(out, "self.{time_field}.year(), self.{time_field}.month() as _, self.{time_field}.day(),", time_field=time_field.name)?;
    }
    for field in schema.fields.iter() {
        if field.typ_flags.is_type_flag(FieldFlag::Index) && !field.typ_flags.is_type_flag(FieldFlag::FirehoseDateIndex) {
            write!(out, "&self.{},", field.name)?;
        }
    }
    writeln!(out, ")")?;
    writeln!(out, "{TAB}}}\n")?;

    writeln!(out, "{TAB}///Returns owned tuple with reference to all partition keys")?;
    write!(out, "{TAB}pub fn partition_keys(&self) -> (")?;
    if index_time_field.is_some() {
        write!(out, "i32,u8,u8,")?;
    }
    for field in schema.fields.iter() {
        if field.typ_flags.is_type_flag(FieldFlag::Index) && !field.typ_flags.is_type_flag(FieldFlag::FirehoseDateIndex) {
            write!(out, "{},", field.original_type)?;
        }
    }
    writeln!(out, ") {{")?;
    write!(out, "{TAB}{TAB}(")?;
    if let Some(time_field) = index_time_field {
        write!(out, "self.{time_field}.year(), self.{time_field}.month() as _, self.{time_field}.day(),", time_field=time_field.name)?;
    }
    for field in schema.fields.iter() {
        if field.typ_flags.is_type_flag(FieldFlag::Index) && !field.typ_flags.is_type_flag(FieldFlag::FirehoseDateIndex) {
            write!(out, "self.{}.clone(),", field.name)?;
        }
    }
    writeln!(out, ")")?;
    writeln!(out, "{TAB}}}\n")?;

    writeln!(out, "{TAB}///Returns fmt::Display that can be used to write partitioned path prefix for s3 destination")?;
    writeln!(out, "{TAB}pub fn firehose_s3_path_prefix(&self) -> impl core::fmt::Display + '_ {{")?;

    writeln!(out, "{TAB}{TAB}pub struct DisplayImpl<'_int>({reference_type});")?;
    writeln!(out, "{TAB}{TAB}impl core::fmt::Display for DisplayImpl<'_> {{")?;
    writeln!(out, "{TAB}{TAB}{TAB}fn fmt(&self, fmt: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {{")?;
    write!(out, "{TAB}{TAB}{TAB}{TAB}fmt.write_fmt(format_args!(\"")?;
    if index_time_field.is_some() {
        write!(out, "year={{year:04}}/month={{month:02}}/day={{day:02}}/")?;
    }
    for field in schema.fields.iter() {
        if field.typ_flags.is_type_flag(FieldFlag::Index) && !field.typ_flags.is_type_flag(FieldFlag::FirehoseDateIndex) {
            write!(out, "{field_name}={{{field_name}}}/", field_name=field.table_field_name())?;
        }
    }
    write!(out, "\",")?;

    if index_time_field.is_some() {
        write!(out, "year=self.0.0, month=self.0.1 as u8, day=self.0.2,")?;
    }
    let mut idx = 3;
    for field in schema.fields.iter() {
        if field.typ_flags.is_type_flag(FieldFlag::Index) && !field.typ_flags.is_type_flag(FieldFlag::FirehoseDateIndex) {
            write!(out, "{}=self.0.{idx},", field.table_field_name())?;
            idx += 1;
        }
    }
    writeln!(out, "))")?;

    writeln!(out, "{TAB}{TAB}{TAB}}}\n")?;
    writeln!(out, "{TAB}{TAB}}}\n")?;
    writeln!(out, "{TAB}{TAB}DisplayImpl(self.partition_keys_ref())\n")?;
    writeln!(out, "{TAB}}}\n")?;

    Ok(())
}
