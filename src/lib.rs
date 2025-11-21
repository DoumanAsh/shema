//!Derive macro to generate database schema code from Rust struct
//!
//!All parameters are specified via `shema`
//!
//!## Struct parameters
//!
//! - `firehose_schema` - Enables firehose schema generation
//! - `firehose_partition_code` - Enables code generation to access partition information
//! - `firehose_parquet_schema` - Enables parquet schema generation similar to AWS Glue's one
//! - `parquet_code` - Specifies to generate parquet code to write struct per schema. This requires `parquet` and `serde_json` crates to be added as dependencies
//!
//!## Field parameters
//!
//!- `json` - Specifies that field is to be encoded as json object (automatically derived for std's collections)
//!- `enumeration` - Specifies that field is to be encoded as enumeration (Depending on database, it will be encoded as string or object)
//!- `index` - Specifies that field is to be indexed by underlying database engine (e.g. to be declared a partition key in AWS glue schema)
//!- `firehose_date_index` - Specifies field to be used as timestamp within `firehose` schema which will produce `year`, `month` and `day` fields. Requires to be of `timestamp` type. E.g. [time::OffsetDateTime](https://docs.rs/time/0.3.44/time/struct.OffsetDateTime.html)
//!- `rename` - Tells to use different name for the field. Argument MUST be string specified as `rename = "new_name"`
//!
//!### Firehose date index
//!
//!If specified firehose output will expect RFC3339 encoded string as output during serialization
//!
//!You should configure HIVE json deserializer with possible RFC3339 formats.
//!
//!Terraform Reference: <https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/kinesis_firehose_delivery_stream#timestamp_formats-1>
//!
//!## Schema output
//!
//!### Following constants will be declared for affected structs:
//!
//!- `SHEMA_TABLE_NAME` - table name in lower case
//!- `SHEMA_FIREHOSE_SCHEMA` - Firehose glue table schema. If enabled.
//!- `SHEMA_FIREHOSE_PARQUET_SCHEMA` - Partquet schema compatible with firehose data stream. If enabled.
//!
//!### Following methods will be defined for affected structs
//!
//!- `firehose_partition_keys_ref` - Returns tuple with references to partition keys
//!- `firehose_partition_keys` - Returns tuple with owned values of partition keys
//!- `firehose_s3_path_prefix` - Returns `fmt::Display` type that writes full path prefix for S3 destination object
//!- `is_firehose_s3_path_prefix_valid` - Returns `true` if `firehose_s3_path_prefix` is valid or not (i.e. no string is empty among partitions)
//!
//!### Following [parquet](https://crates.io/crates/parquet)  crate traits are implemented:
//!
//!- [RecordWriter](https://docs.rs/parquet/57.0.0/parquet/record/trait.RecordWriter.html) - Enables write via [SerializedFileWriter](https://docs.rs/parquet/latest/parquet/file/writer/struct.SerializedFileWriter.html)
//!
//!### Firehose specifics
//!
//!Firehose schema expects flat structure, so any complex struct or array must be serialized as strings
//!
//!```rust
//!mod prost_wkt_types {
//!    pub struct Struct;
//!}
//!
//!use std::fs;
//!use shema::Shema;
//!
//!//Build context is relative to root of workspace so we point to crate's path
//!#[derive(Shema)]
//!#[shema(firehose_schema, firehose_parquet_schema, firehose_partition_code)]
//!pub(crate) struct Analytics<'a> {
//!    #[shema(index, firehose_date_index)]
//!    ///Special field that will be transformed in firehose as year,month,day
//!    r#client_time: time::OffsetDateTime,
//!    r#server_time: time::OffsetDateTime,
//!    r#user_id: Option<String>,
//!    #[shema(index)]
//!    ///Index key will go into firehose's partition_keys
//!    r#client_id: String,
//!    #[shema(index)]
//!    r#session_id: String,
//!    #[shema(json)]
//!    r#extras: Option<prost_wkt_types::Struct>,
//!    #[shema(json)]
//!    r#props: prost_wkt_types::Struct,
//!    r#name: String,
//!
//!    byte: i8,
//!    short: i16,
//!    int: i32,
//!    long: i64,
//!    ptr: isize,
//!
//!    float: f32,
//!    double: f64,
//!    boolean: bool,
//!    #[shema(rename = "stroka")]
//!    strka: &'a str,
//!
//!    array: Vec<String>,
//!}
//!
//!assert_eq!(Analytics::SHEMA_TABLE_NAME, "analytics");
//!```

#![allow(clippy::style)]

mod utils;
mod firehose;
mod parquet;

use core::fmt::{self, Write};

use proc_macro::TokenStream;

const ATTR_NAME: &str = "shema";
const TAB: &str = "    ";

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
enum FieldType {
    Byte,
    Short,
    Integer,
    Long,
    Float,
    Double,
    String,
    Boolean,
    TimestampZ,
    Array,
    Object,
    Enum,
}

impl FieldType {
    #[inline]
    pub const fn is_string_type(&self) -> bool {
        matches!(self, Self::String)
    }
}

#[repr(u8)]
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
enum FieldFlag {
    Optional = 1 << 0,
    Index = 1 << 1,
    FirehoseDateIndex = 1 << 2,
}

struct FieldFlagContainer(pub u8);

impl FieldFlagContainer {
    #[inline(always)]
    fn set_type_flag(&mut self, flag: FieldFlag) {
        self.0 |= flag as u8;
    }

    #[inline(always)]
    fn is_type_flag(&self, flag: FieldFlag) -> bool {
        let flag = flag as u8;
        self.0 & flag == flag
    }
}

struct Field {
    //Field's name
    name: String,
    typ: FieldType,
    original_name: String,
    original_type: String,
    typ_flags: FieldFlagContainer,
    //Optional documentation on field
    docstring: String,
}

impl Field {
    #[inline(always)]
    ///Returns field name suitable for serialization
    fn table_field_name(&self) -> &str {
        self.name.strip_prefix("r#").unwrap_or(self.name.as_str())
    }
}

struct Outputs {
    firehose_schema: bool,
    firehose_parquet_schema: bool,
    firehose_partition_code: bool,
    parquet_code: bool,
}

struct TableSchema {
    name: String,
    fields: Vec<Field>,
    outputs: Outputs,
}

impl TableSchema {
    #[inline(always)]
    fn lower_cased_table_name(&self) -> String {
        utils::to_lower_case_by_sep(&self.name, '_')
    }

    #[inline(always)]
    fn index_time_field(&self) -> Option<&Field> {
        self.fields.iter().find(|field| field.typ_flags.is_type_flag(FieldFlag::FirehoseDateIndex))
    }
}

#[cold]
#[inline(never)]
fn compile_error(input: &impl quote::ToTokens, error: impl fmt::Display) -> TokenStream {
    syn::Error::new_spanned(input, error).to_compile_error().into()
}

fn extract_type_path_segment(segment: &syn::PathSegment) -> Result<FieldType, TokenStream> {
    if segment.ident == "bool" {
        Ok(FieldType::Boolean)
    } else if segment.ident == "i8" {
        Ok(FieldType::Byte)
    } else if segment.ident == "i16" {
        Ok(FieldType::Short)
    } else if segment.ident == "i32" {
        Ok(FieldType::Integer)
    } else if segment.ident == "i64" || segment.ident == "isize" {
        Ok(FieldType::Long)
    } else if segment.ident == "f32" {
        Ok(FieldType::Float)
    } else if segment.ident == "f64" {
        Ok(FieldType::Double)
    } else if segment.ident == "OffsetDateTime" {
        Ok(FieldType::TimestampZ)
    } else if segment.ident == "String" || segment.ident == "str" {
        Ok(FieldType::String)
    } else if segment.ident == "Vec" || segment.ident == "HashSet" || segment.ident == " BTreeSet" {
        Ok(FieldType::Array)
    } else if segment.ident == "HashMap" || segment.ident == " BTreeMap" {
        Ok(FieldType::Object)
    } else if segment.ident == "u8" || segment.ident == "u16" ||
              segment.ident == "u32" || segment.ident == "u64" ||
              segment.ident == "usize" {
        Err(compile_error(segment, "Unsigned integers are not supported"))
    } else {
        Err(compile_error(segment, format_args!("Unrecognized type: {}", segment.ident)))
    }
}

//Returns flag indicating optionality and field type on success.
fn extract_type_path(ty: &syn::TypePath, type_override: Option<FieldType>) -> Result<(bool, FieldType), TokenStream> {
    let ty = ty.path.segments.last().expect("to have at least one segment");

    if let Some(type_override) = type_override {
        Ok((ty.ident == "Option", type_override))
    } else if ty.ident == "Option" {
        let ty = match &ty.arguments {
            syn::PathArguments::AngleBracketed(args) => match args.args.len() {
                0 => return Err(compile_error(&ty.ident, "Option without type argument. Fix it")),
                1 => match args.args.first().unwrap() {
                    syn::GenericArgument::Type(syn::Type::Path(ty)) => {
                        extract_type_path_segment(ty.path.segments.last().expect("to have at least on segment"))?
                    },
                    _ => return Err(compile_error(&ty.ident, "Option should have type argument, but got something unexpected. Fix it")),
                }
                _ => return Err(compile_error(&ty.ident, "Option has too many type arguments. Fix it")),
            },
            syn::PathArguments::None => return Err(compile_error(&ty.ident, "Option without type argument. Fix it")),
            syn::PathArguments::Parenthesized(_) => return Err(compile_error(&ty.ident, "You got wrong brackets for your Option. Fix it")),
        };
        Ok((true, ty))
    } else {
        let ty = extract_type_path_segment(ty)?;
        Ok((false, ty))
    }
}

fn from_struct(attributes: &[syn::Attribute], ident: &syn::Ident, generics: &syn::Generics, payload: &syn::DataStruct) -> TokenStream {
    let mut schema = TableSchema {
        name: ident.to_string(),
        fields: Vec::new(),
        outputs: Outputs {
            firehose_schema: false,
            firehose_parquet_schema: false,
            firehose_partition_code: false,
            parquet_code: false,
        }
    };

    for attr in attributes.iter() {
        match &attr.meta {
            syn::Meta::List(value) => {
                if value.path.is_ident("shema") {
                    let nested = attr.parse_args_with(
                        syn::punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated,
                    );
                    let nested = match nested {
                        Ok(nested) => nested,
                        Err(error) => return compile_error(&value, format_args!("shema input is not valid: {error}")),
                    };

                    for meta in nested {
                        let meta_path = meta.path();
                        match &meta {
                            syn::Meta::Path(value) => {
                                if value.is_ident("firehose_schema") {
                                    schema.outputs.firehose_schema = true;
                                } else if value.is_ident("firehose_parquet_schema") {
                                    schema.outputs.firehose_parquet_schema = true;
                                } else if value.is_ident("firehose_partition_code") {
                                    schema.outputs.firehose_partition_code = true;
                                } else if value.is_ident("parquet_code") {
                                    schema.outputs.parquet_code = true;
                                } else {
                                    return compile_error(meta_path, "Unknown attribute passed to shema");
                                }
                            }
                            invalid => return compile_error(invalid, format_args!("Unknown attribute passed to shema: {:?}", invalid)),
                        }
                    }
                }
            }
            _ => (),
        }
    }

    for field in payload.fields.iter() {
        let original_name = match field.ident.as_ref() {
            Some(ident) => ident.to_string(),
            None => return compile_error(field, "Field is missing name"),
        };

        let mut field_name = None;
        let mut docstring = String::new();
        let mut typ_flags = FieldFlagContainer(0);
        let mut type_override = None;

        for attr in field.attrs.iter() {
            match &attr.meta {
                syn::Meta::NameValue(value) if value.path.is_ident("doc") => {
                    let literal = match &value.value {
                        syn::Expr::Lit(literal) => &literal.lit,
                        _ => return compile_error(&value.value, "Docstring should be literal string"),
                    };
                    if let syn::Lit::Str(text) = literal {
                        docstring.push_str(&text.value());
                        docstring.push(' ');
                    }
                },
                syn::Meta::List(value) if value.path.is_ident(ATTR_NAME) => {
                    let nested = attr.parse_args_with(syn::punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated);
                    let nested = match nested {
                        Ok(nested) => nested,
                        Err(error) => return compile_error(value, format_args!("'{ATTR_NAME}' attribute input is not valid: {error}")),
                    };

                    for meta in nested {
                        let meta_path = meta.path();
                        match &meta {
                            syn::Meta::Path(value) => if value.is_ident("json") {
                                type_override = Some(FieldType::Object);
                            } else if value.is_ident("enumeration") {
                                type_override = Some(FieldType::Enum);
                            } else if value.is_ident("index") {
                                typ_flags.set_type_flag(FieldFlag::Index);
                            } else if value.is_ident("firehose_date_index") {
                                typ_flags.set_type_flag(FieldFlag::FirehoseDateIndex);
                            } else {
                                return compile_error(meta_path, "Unexpected path attribute specified for '{ATTR_NAME}'. Allowed: json, enumeration, index");
                            },
                            syn::Meta::NameValue(value) => if value.path.is_ident("rename") {
                                let literal = match &value.value {
                                    syn::Expr::Lit(literal) => match &literal.lit {
                                        syn::Lit::Str(literal) => literal,
                                        _ => return compile_error(&value.value, "'rename' should be literal string"),
                                    }
                                    _ => return compile_error(&value.value, "'rename' should be literal string"),
                                };
                                let new_name = literal.value();
                                let new_name = new_name.trim();
                                if new_name.is_empty() {
                                    return compile_error(literal, "'rename' requires non-empty string");
                                }

                                field_name = Some(new_name.to_owned());
                            } else {
                                return compile_error(meta_path, "Unexpected name value attribute specified for '{ATTR_NAME}'. Allowed: rename");
                            },
                            unexpected => return compile_error(unexpected, format_args!("Unexpected value provided to '{ATTR_NAME}': {unexpected:?}"))
                        }
                    }
                },
                _ => continue
            }
        } //attr

        let field_ty = &field.ty;
        let original_type = quote::quote!(#field_ty).to_string();

        let (is_optional, typ) = match field_ty {
            syn::Type::Path(ty) => match extract_type_path(ty, type_override) {
                Ok(result) => result,
                Err(error) => return error,
            },
            syn::Type::Reference(ty) => match &*ty.elem {
                syn::Type::Path(ty) => match extract_type_path(ty, type_override) {
                    Ok(result) => result,
                    Err(error) => return error,
                },
                unexpected => return compile_error(unexpected, "Field type should be type path"),
            }
            unexpected => return compile_error(unexpected, "Field type should be type path"),
        };

        docstring.pop();
        if is_optional {
            typ_flags.set_type_flag(FieldFlag::Optional);
        }
        if typ_flags.is_type_flag(FieldFlag::FirehoseDateIndex) && !matches!(typ, FieldType::TimestampZ) {
            return compile_error(&field.ty, format_args!("Firehose date index should be timestamp but got {:?}", typ));
        }

        schema.fields.push(Field {
            name: field_name.unwrap_or_else(|| original_name.clone()),
            typ,
            original_name,
            original_type,
            typ_flags,
            docstring
        })
    }

    let (impl_gen, type_gen, where_clause) = generics.split_for_impl();
    let mut code = String::new();

    let table_name = schema.lower_cased_table_name();
    //impl start
    let _ = writeln!(code, "impl{} {}{} {{", quote::quote!(#impl_gen), ident, quote::quote!(#type_gen #where_clause));
    let _ = writeln!(code, "{TAB}pub const SHEMA_TABLE_NAME: &'static str = \"{table_name}\";");

    if schema.outputs.firehose_schema || schema.outputs.firehose_partition_code {
        let schema = firehose::FirehoseInput {
            index_time_field: schema.index_time_field(),
            schema: &schema,
        };

        if schema.schema.outputs.firehose_schema {
            //Firehose schema
            let _ = write!(code, "{TAB}pub const SHEMA_FIREHOSE_SCHEMA: &'static str = r#\"");
            //json generates valid string always
            unsafe {
                firehose::generate_firehose_schema(schema, &mut code.as_mut_vec()).expect("to generate firehose schema");
            }
            let _ = writeln!(code, "\"#;");
        }

        if schema.schema.outputs.firehose_partition_code {
            //Generate partition index accessors
            let _ = firehose::generate_firehose_partition_accessor(schema, &mut code);
        }
    }

    if schema.outputs.firehose_parquet_schema {
        //Firehose's parquet schema
        let _ = write!(code, "{TAB}pub const SHEMA_FIREHOSE_PARQUET_SCHEMA: &'static str = r#\"");
        unsafe {
            parquet::generate_parquet_schema(&schema, &mut code.as_mut_vec()).expect("to generate parquet schema");
        }
        let _ = writeln!(code, "\"#;");
    }

    code.push('}'); //impl

    if schema.outputs.parquet_code {
        let _ = writeln!(
            code,
            "\nimpl{} parquet::record::RecordWriter<{ident}{generics}> for &[{ident}{generics}] {{",
            quote::quote!(#impl_gen),
            ident = ident,
            generics = quote::quote!(#type_gen #where_clause)
        );

        parquet::generate_parquet_writer_interface_code(&schema, &mut code)
            .expect("to generate parquet code");
        let _ = writeln!(code, "}}");
    }
    code.parse().expect("valid code")
}

#[proc_macro_derive(Shema, attributes(shema))]
pub fn shema(input: TokenStream) -> TokenStream {
    let ast: syn::DeriveInput = syn::parse(input).unwrap();
    match ast.data {
        syn::Data::Struct(data) => from_struct(&ast.attrs, &ast.ident, &ast.generics, &data),
        _ => compile_error(&ast.ident, "Unsupported type of input. Expected struct"),
    }
}
