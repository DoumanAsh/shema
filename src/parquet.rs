use std::io;
use crate::{TableSchema, FieldType, FieldFlag};

const TAB: &'static str = "  ";

impl FieldType {
    #[inline(always)]
    pub fn is_aws_firehose_parquet_utf8_converted(&self) -> bool {
        matches!(self, FieldType::String | FieldType::Array| FieldType::Object)
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
