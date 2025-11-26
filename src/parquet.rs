use std::{fmt, io};
use crate::{TableSchema, Field, FieldType, FieldFlag};

const TAB: &'static str = "  ";

impl FieldType {
    #[inline(always)]
    pub fn is_aws_firehose_parquet_utf8_converted(&self) -> bool {
        matches!(self, FieldType::String | FieldType::Array | FieldType::Object | FieldType::Enum)
    }

    #[inline(always)]
    pub fn aws_firehose_parquet(&self) -> &'static str {
        match self {
            Self::Byte | Self::Short | Self::Integer => "INT32",
            Self::Long => "INT64",
            Self::Float => "FLOAT",
            Self::Double => "DOUBLE",
            Self::String => "BYTE_ARRAY",
            Self::Boolean => "BOOLEAN",
            //Firehose's Hive serializer encodes it as INT96
            Self::TimestampZ => "INT96",
            //Encode all arrays/objects as strings
            Self::Array | Self::Object | Self::Enum => "BYTE_ARRAY",
        }
    }
}

pub fn generate_parquet_schema<O: io::Write>(schema: &TableSchema, out: &mut O) -> io::Result<()> {
    writeln!(out, "message {} {{", schema.lower_cased_table_name())?;

    for field in schema.fields.iter() {
        if field.typ_flags.is_type_flag(FieldFlag::Index) && !field.typ_flags.is_type_flag(FieldFlag::FirehoseDateIndex) {
            //Partition keys are not written by Firehose data stream
            continue;
        }


        write!(out, "{TAB}")?;
        if field.typ_flags.is_type_flag(FieldFlag::Optional) {
            out.write_all(b"OPTIONAL ")?;
        } else {
            //Hive outputs everything as optional, confirm if `REQUIRED` is fine
            out.write_all(b"REQUIRED ")?;
        }
        write!(out, "{} {}", field.typ.aws_firehose_parquet(), field.table_field_name())?;
        if field.typ.is_aws_firehose_parquet_utf8_converted() {
            out.write_all(b" (UTF8)")?;
        }

        out.write_all(b";\n")?;
    }

    out.write_all(b"}")?;
    out.flush()
}

struct ParquetFieldWriter<'a>(&'a Field);

impl fmt::Display for ParquetFieldWriter<'_> {
    #[inline]
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        const ENCODE_HIVE_INT96_TIMESTAMP: &'static str = r#"
                let julian_day = record.to_julian_day() as u32;
                let time = time::Duration::hours(record.hour() as _) + time::Duration::minutes(record.minute() as _) + time::Duration::seconds(record.second() as _) + time::Duration::nanoseconds(record.nanosecond() as _);

                let time_nanos = time.whole_nanoseconds() as u64;
                let time_nanos: [u8; 8] = time_nanos.to_le_bytes();
                let mut timestamp = ::parquet::data_type::Int96::new();
                timestamp.set_data(
                    u32::from_ne_bytes([time_nanos[0], time_nanos[1], time_nanos[2], time_nanos[3]]),
                    u32::from_ne_bytes([time_nanos[4], time_nanos[5], time_nanos[6], time_nanos[7]]),
                    julian_day.to_le()
                );

                vals.push(timestamp);
        "#;
        //Parquet writes data in sequence, and uses definition_levels to determine if data is present
        if self.0.typ_flags.is_type_flag(FieldFlag::Optional) {
            fmt.write_fmt(format_args!("let definition_levels = records.iter().map(|rec| if rec.{field_name}.is_some() {{ 1 }} else {{ 0 }}).collect::<Vec<i16>>();", field_name=self.0.original_name))?;
            fmt.write_str("let definition_levels = Some(definition_levels.as_slice());\n")?;
        } else {
            fmt.write_str("let definition_levels = None::<&[i16]>;\n")?;
        }

        let column_writer = match self.0.typ {
            FieldType::Boolean => {
                if self.0.typ_flags.is_type_flag(FieldFlag::Optional) {
                    fmt.write_fmt(format_args!(r#"
            let vals = records.iter().filter_map(|rec| rec.{field_name}.map(|field| field as bool)).collect::<Vec<_>>();
                "#, field_name=self.0.original_name))?;
                } else {
                    fmt.write_fmt(format_args!(r#"
            let vals = records.iter().map(|rec| rec.{field_name} as bool).collect::<Vec<_>>();"#, field_name = self.0.original_name))?;
                }

                "BoolColumnWriter"
            },
            //All small integers are encoded as int32
            FieldType::Byte | FieldType::Short | FieldType::Integer => {
                if self.0.typ_flags.is_type_flag(FieldFlag::Optional) {
                    fmt.write_fmt(format_args!(r#"
            let vals = records.iter().filter_map(|rec| rec.{field_name}.map(|field| field as i32)).collect::<Vec<_>>();"#, field_name=self.0.original_name))?;
                } else {
                    fmt.write_fmt(format_args!(
                        r#"
            let vals = records.iter().map(|rec| rec.{field_name} as i32).collect::<Vec<_>>();"#, field_name = self.0.original_name))?;
                }

                "Int32ColumnWriter"
            },
            FieldType::Long => {
                if self.0.typ_flags.is_type_flag(FieldFlag::Optional) {
                    fmt.write_fmt(format_args!(r#"
            let vals = records.iter().filter_map(|rec| rec.{field_name}.map(|field| field as i64)).collect::<Vec<_>>();"#, field_name=self.0.original_name))?;
                } else {
                    fmt.write_fmt(format_args!(
                        r#"
            let vals = records.iter().map(|rec| rec.{field_name} as i64).collect::<Vec<_>>();"#, field_name = self.0.original_name))?;
                }

                "Int64ColumnWriter"
            },
            FieldType::Float => {
                if self.0.typ_flags.is_type_flag(FieldFlag::Optional) {
                    fmt.write_fmt(format_args!(r#"
            let vals = records.iter().filter_map(|rec| rec.{field_name}.map(|field| field as f32)).collect::<Vec<_>>();"#, field_name=self.0.original_name))?;
                } else {
                    fmt.write_fmt(format_args!(
                        r#"
            let vals = records.iter().map(|rec| rec.{field_name} as f32).collect::<Vec<_>>();"#, field_name = self.0.original_name))?;
                }

                "FloatColumnWriter"
            },
            FieldType::Double => {
                if self.0.typ_flags.is_type_flag(FieldFlag::Optional) {
                    fmt.write_fmt(format_args!(r#"
            let vals = records.iter().filter_map(|rec| rec.{field_name}.map(|field| field as f64)).collect::<Vec<_>>();"#, field_name=self.0.original_name))?;
                } else {
                    fmt.write_fmt(format_args!(
                        r#"
            let vals = records.iter().map(|rec| rec.{field_name} as f64).collect::<Vec<_>>();"#, field_name = self.0.original_name))?;
                }

                "DoubleColumnWriter"
            },
            FieldType::String => {
                if self.0.typ_flags.is_type_flag(FieldFlag::Optional) {
                    fmt.write_fmt(format_args!(r#"
            let vals = records.iter().filter_map(|rec| rec.{field_name}.as_ref().map(|field| field.as_bytes().into())).collect::<Vec<_>>();"#, field_name=self.0.original_name))?;
                } else {
                    fmt.write_fmt(format_args!(r#"
            let vals = records.iter().map(|rec| rec.{field_name}.as_bytes().into()).collect::<Vec<_>>();"#, field_name=self.0.original_name))?;
                }

                "ByteArrayColumnWriter"
            },
            //Firehose's Hive serializer encodes it as INT96
            FieldType::TimestampZ => {
                if self.0.typ_flags.is_type_flag(FieldFlag::Optional) {
                    fmt.write_fmt(format_args!(
                        r#"
            let mut vals = Vec::new();
            for record in records.iter() {{
                if let Some(record) = record.{field_name}.as_ref() {{
                    {ENCODE_HIVE_INT96_TIMESTAMP}
                }}
            }}"#, field_name = self.0.original_name))?;
                } else {
                    fmt.write_fmt(format_args!(
                        r#"
            let mut vals = Vec::new();
            for record in records.iter() {{
                let record = &record.{field_name};
                {ENCODE_HIVE_INT96_TIMESTAMP}
            }}"#, field_name = self.0.original_name))?;
                }

                "Int96ColumnWriter"
            },
            //Encode all arrays/objects as JSON strings
            FieldType::Array | FieldType::Object | FieldType::Enum => {
                if self.0.typ_flags.is_type_flag(FieldFlag::Optional) {
                    fmt.write_fmt(format_args!(r#"
            let mut vals = Vec::new();
            for record in records.iter() {{
                if let Some(record) = record.{field_name}.as_ref() {{
                    match serde_json::to_vec(record) {{
                        Ok(rec) => vals.push(rec.into()),
                        Err(error) => return Err(::parquet::errors::ParquetError::General(format!("Column '{field_name}' cannot be serialized: {{error}}").into()))
                    }}
                }}
            }}"#, field_name=self.0.original_name))?;
                } else {
                    fmt.write_fmt(format_args!(r#"
            let mut vals = Vec::new();
            for record in records.iter() {{
                match serde_json::to_vec(&record.{field_name}) {{
                    Ok(rec) => vals.push(rec.into()),
                    Err(error) => return Err(::parquet::errors::ParquetError::General(format!("Column '{field_name}' cannot be serialized: {{error}}").into()))
                }}
            }}"#, field_name=self.0.original_name))?;
                }

                "ByteArrayColumnWriter"
            }
        };

        fmt.write_fmt(format_args!(r#"
            if let ColumnWriter::{column_writer}(typed) = column_writer.untyped() {{
                typed.write_batch(&vals[..], definition_levels, None)?;
            }} else {{
                return Err(::parquet::errors::ParquetError::General("Column '{field_name}' expects {field_type:?} but got another type".into()));
            }}"#, field_name=self.0.original_name, field_type=self.0.typ))
    }
}

struct ParquetFieldSchema<'a>(&'a Field);

impl fmt::Display for ParquetFieldSchema<'_> {
    #[inline]
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        const LOGICAL_NONE: &str = "None";
        const LOGICAL_STRING: &str = "Some(::parquet::basic::LogicalType::String)";

        let (logical_type, physical_type) = match self.0.typ {
            FieldType::Byte => ("Some(::parquet::basic::LogicalType::Integer { bit_width: 8, is_signed: true })", "INT32"),
            FieldType::Short => ("Some(::parquet::basic::LogicalType::Integer { bit_width: 16, is_signed: true })", "INT32"),
            FieldType::Integer => ("Some(::parquet::basic::LogicalType::Integer { bit_width: 32, is_signed: true })", "INT32"),
            FieldType::Long => ("Some(::parquet::basic::LogicalType::Integer { bit_width: 64, is_signed: true })", "INT64"),
            FieldType::Float => (LOGICAL_NONE, "FLOAT"),
            FieldType::Double => (LOGICAL_NONE, "DOUBLE"),
            FieldType::String => (LOGICAL_STRING, "BYTE_ARRAY"),
            FieldType::Boolean => (LOGICAL_NONE, "BOOLEAN"),
            //Firehose's Hive serializer encodes it as INT96
            FieldType::TimestampZ => (LOGICAL_NONE, "INT96"),
            //Encode all arrays/objects as strings
            FieldType::Array | FieldType::Object | FieldType::Enum => (LOGICAL_STRING, "BYTE_ARRAY"),
        };

        let repetition = if self.0.typ_flags.is_type_flag(FieldFlag::Optional) {
            "::parquet::basic::Repetition::OPTIONAL"
        } else {
            "::parquet::basic::Repetition::REQUIRED"
        };

        let name = self.0.table_field_name();
        fmt.write_fmt(format_args!("::parquet::schema::types::Type::primitive_type_builder(\"{name}\", ::parquet::basic::Type::{physical_type}).with_logical_type({logical_type}).with_repetition({repetition}).build().unwrap().into()"))
    }
}

pub fn generate_parquet_writer_interface_code<O: fmt::Write>(
    schema: &TableSchema,
    out: &mut O,
) -> fmt::Result {
    //writer
    writeln!(
        out,
        r#"
    fn write_to_row_group<W: ::std::io::Write + Send>(&self, row_group_writer: &mut ::parquet::file::writer::SerializedRowGroupWriter<'_, W>) -> ::core::result::Result<(), ::parquet::errors::ParquetError> {{
        use ::parquet::column::writer::ColumnWriter;

        let mut row_group_writer = row_group_writer;
        let records = &self;
"#)?;

    for field in schema.fields.iter() {
        if field.typ_flags.is_type_flag(FieldFlag::Index) && !field.typ_flags.is_type_flag(FieldFlag::FirehoseDateIndex) {
            continue;
        }
        writeln!(
            out,
            r#"
        //write '{field_name}' column
        if let Some(mut column_writer) = row_group_writer.next_column()? {{
            {writer}
            column_writer.close()?;
        }} else {{
            return Err(::parquet::errors::ParquetError::General("Failed to get '{field_name}' column".into()));
        }}
        "#, field_name = field.table_field_name(), writer = ParquetFieldWriter(field))?;
    }

    writeln!(
        out,
        r#"
        Ok(())
    }}"#
    )?;

    use crate::TAB;
    //schema
    writeln!(out, "{TAB}fn schema(&self) -> ::core::result::Result<::parquet::schema::types::TypePtr, ::parquet::errors::ParquetError> {{")?;
    writeln!(out, "{TAB}{TAB}let mut fields = Vec::<::parquet::schema::types::TypePtr>::new();\n")?;
    for field in schema.fields.iter() {
        if field.typ_flags.is_type_flag(FieldFlag::Index) && !field.typ_flags.is_type_flag(FieldFlag::FirehoseDateIndex) {
            continue;
        }
        writeln!(out, "{TAB}{TAB}//write '{field_name}' column", field_name=field.table_field_name())?;
        writeln!(out, "{TAB}{TAB}fields.push({});", ParquetFieldSchema(field))?;
    }

    writeln!(out, "\n{TAB}{TAB}::parquet::schema::types::Type::group_type_builder(\"{name}\").with_fields(fields).build().map(Into::into)", name=schema.lower_cased_table_name())?;
    writeln!(out, "{TAB}}}")?;
    Ok(())
}
