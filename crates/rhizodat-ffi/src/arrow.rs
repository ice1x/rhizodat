//! An Arrow IPC stream (what ClickHouse answers with) as rows of the core's
//! values, for a caller that gets tagged JSON rather than Arrow.

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal128Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_ipc::reader::StreamReader;
use std::sync::Arc;

use arrow_schema::{DataType, TimeUnit};
use rhizodat_core::rows::RowValue;

/// The column names and the rows of `stream`.
pub(crate) fn ipc_rows(stream: &[u8]) -> Result<(Vec<String>, Vec<Vec<RowValue>>), String> {
    let reader = StreamReader::try_new(stream, None).map_err(|e| e.to_string())?;
    let columns: Vec<String> = reader
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let batches = reader
        .collect::<Result<Vec<RecordBatch>, _>>()
        .map_err(|e| e.to_string())?;
    Ok((columns, batch_rows(&batches)?))
}

/// The rows of `batches`, values in column order.
pub(crate) fn batch_rows(batches: &[RecordBatch]) -> Result<Vec<Vec<RowValue>>, String> {
    let mut rows = Vec::new();
    for batch in batches {
        for i in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|c| value(c, i))
                    .collect::<Result<_, _>>()?,
            );
        }
    }
    Ok(rows)
}

/// `rows` (values in `columns` order) as one record batch, each column of the
/// Arrow type its values have: integers `Int64` (`Float64` when floats are
/// mixed in), floats `Float64`, booleans, UUIDs as 16 bytes, bytes `Binary`,
/// and text — strings, decimals, dates, datetimes — as `Utf8`, which the
/// Iceberg branch casts to the table's type at stage. A column of nulls only
/// is `Null`. Lists and maps are refused.
pub(crate) fn record_batch(
    columns: &[String],
    rows: &[Vec<RowValue>],
) -> Result<RecordBatch, String> {
    use arrow_schema::{Field, Schema};

    let mut fields = Vec::with_capacity(columns.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    for (c, name) in columns.iter().enumerate() {
        let kind = column_kind(name, rows.iter().map(|r| &r[c]))?;
        let array = column_array(kind, rows.iter().map(|r| &r[c]), rows.len())?;
        fields.push(Field::new(name, array.data_type().clone(), true));
        arrays.push(array);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|e| e.to_string())
}

/// The Arrow type a column of values gets.
#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Null,
    Int,
    Float,
    Bool,
    Uuid,
    Bytes,
    Text,
}

fn column_kind<'a>(name: &str, values: impl Iterator<Item = &'a RowValue>) -> Result<Kind, String> {
    let mut kind = Kind::Null;
    for value in values {
        let k = match value {
            RowValue::Null => Kind::Null,
            RowValue::Int(_) => Kind::Int,
            RowValue::Float(_) => Kind::Float,
            RowValue::Bool(_) => Kind::Bool,
            RowValue::Uuid(_) => Kind::Uuid,
            RowValue::Bytes(_) => Kind::Bytes,
            RowValue::Str(_) | RowValue::Decimal(_) | RowValue::Date(_) | RowValue::DateTime(_) => {
                Kind::Text
            }
            RowValue::List(_) | RowValue::Map(_) => {
                return Err(format!(
                    "column {name:?}: a list or a map has no Iceberg column type here"
                ));
            }
        };
        kind = match (kind, k) {
            (any, Kind::Null) | (Kind::Null, any) => any,
            (a, b) if a == b => a,
            (Kind::Int, Kind::Float) | (Kind::Float, Kind::Int) => Kind::Float,
            _ => return Err(format!("column {name:?} mixes values of different types")),
        };
    }
    Ok(kind)
}

fn column_array<'a>(
    kind: Kind,
    values: impl Iterator<Item = &'a RowValue>,
    len: usize,
) -> Result<ArrayRef, String> {
    use arrow_array::builder::{
        BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder, Float64Builder, StringBuilder,
    };

    Ok(match kind {
        Kind::Null => Arc::new(arrow_array::NullArray::new(len)),
        Kind::Int => Arc::new(
            values
                .map(|v| match v {
                    RowValue::Int(i) => Some(*i),
                    _ => None,
                })
                .collect::<arrow_array::Int64Array>(),
        ),
        Kind::Float => {
            let mut b = Float64Builder::new();
            for v in values {
                #[allow(clippy::cast_precision_loss)]
                b.append_option(match v {
                    RowValue::Float(f) => Some(*f),
                    RowValue::Int(i) => Some(*i as f64),
                    _ => None,
                });
            }
            Arc::new(b.finish())
        }
        Kind::Bool => {
            let mut b = BooleanBuilder::new();
            for v in values {
                b.append_option(match v {
                    RowValue::Bool(x) => Some(*x),
                    _ => None,
                });
            }
            Arc::new(b.finish())
        }
        Kind::Uuid => {
            let mut b = FixedSizeBinaryBuilder::new(16);
            for v in values {
                match v {
                    RowValue::Uuid(u) => b.append_value(u.as_bytes()).map_err(|e| e.to_string())?,
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        Kind::Bytes => {
            let mut b = BinaryBuilder::new();
            for v in values {
                match v {
                    RowValue::Bytes(x) => b.append_value(x),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        Kind::Text => {
            let mut b = StringBuilder::new();
            for v in values {
                match v {
                    RowValue::Str(x)
                    | RowValue::Decimal(x)
                    | RowValue::Date(x)
                    | RowValue::DateTime(x) => b.append_value(x),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
    })
}

/// One cell as a core value.
pub(crate) fn value(column: &ArrayRef, i: usize) -> Result<RowValue, String> {
    if column.is_null(i) {
        return Ok(RowValue::Null);
    }
    let int = |v: i64| Ok(RowValue::Int(v));
    match column.data_type() {
        DataType::Null => Ok(RowValue::Null),
        DataType::Boolean => Ok(RowValue::Bool(column.as_boolean().value(i))),
        DataType::Int8 => int(column.as_primitive::<Int8Type>().value(i).into()),
        DataType::Int16 => int(column.as_primitive::<Int16Type>().value(i).into()),
        DataType::Int32 => int(column.as_primitive::<Int32Type>().value(i).into()),
        DataType::Int64 => int(column.as_primitive::<Int64Type>().value(i)),
        DataType::UInt8 => int(column.as_primitive::<UInt8Type>().value(i).into()),
        DataType::UInt16 => int(column.as_primitive::<UInt16Type>().value(i).into()),
        DataType::UInt32 => int(column.as_primitive::<UInt32Type>().value(i).into()),
        DataType::UInt64 => {
            let v = column.as_primitive::<UInt64Type>().value(i);
            i64::try_from(v)
                .map(RowValue::Int)
                .or_else(|_| Ok(RowValue::Decimal(v.to_string())))
        }
        DataType::Float32 => Ok(RowValue::Float(
            column.as_primitive::<Float32Type>().value(i).into(),
        )),
        DataType::Float64 => Ok(RowValue::Float(
            column.as_primitive::<Float64Type>().value(i),
        )),
        DataType::Utf8 => Ok(RowValue::Str(column.as_string::<i32>().value(i).to_owned())),
        DataType::LargeUtf8 => Ok(RowValue::Str(column.as_string::<i64>().value(i).to_owned())),
        // ClickHouse sends String as Binary unless told otherwise: text when it is
        // UTF-8, bytes when it is not.
        DataType::Binary => Ok(text_or_bytes(column.as_binary::<i32>().value(i))),
        DataType::LargeBinary => Ok(text_or_bytes(column.as_binary::<i64>().value(i))),
        DataType::FixedSizeBinary(16) => {
            let raw = column.as_fixed_size_binary().value(i);
            Ok(uuid::Uuid::from_slice(raw)
                .map_or_else(|_| RowValue::Bytes(raw.to_vec()), RowValue::Uuid))
        }
        DataType::FixedSizeBinary(_) => Ok(text_or_bytes(column.as_fixed_size_binary().value(i))),
        DataType::Date32 => {
            let days = column.as_primitive::<Date32Type>().value(i);
            Ok(RowValue::Date(date(days)))
        }
        DataType::Timestamp(unit, tz) => {
            let raw = match unit {
                TimeUnit::Second => column.as_primitive::<TimestampSecondType>().value(i),
                TimeUnit::Millisecond => column.as_primitive::<TimestampMillisecondType>().value(i),
                TimeUnit::Microsecond => column.as_primitive::<TimestampMicrosecondType>().value(i),
                TimeUnit::Nanosecond => column.as_primitive::<TimestampNanosecondType>().value(i),
            };
            Ok(RowValue::DateTime(datetime(raw, *unit, tz.is_some())))
        }
        DataType::Decimal128(_, scale) => {
            let raw = column.as_primitive::<Decimal128Type>().value(i);
            Ok(RowValue::Decimal(decimal(raw, *scale)))
        }
        DataType::List(_) => {
            let list = column.as_list::<i32>().value(i);
            (0..list.len())
                .map(|j| value(&list, j))
                .collect::<Result<_, _>>()
                .map(RowValue::List)
        }
        DataType::LargeList(_) => {
            let list = column.as_list::<i64>().value(i);
            (0..list.len())
                .map(|j| value(&list, j))
                .collect::<Result<_, _>>()
                .map(RowValue::List)
        }
        DataType::Dictionary(_, _) => {
            // LowCardinality: the value its key points at.
            let dictionary = column.as_any_dictionary();
            let key = dictionary.normalized_keys()[i];
            value(dictionary.values(), key)
        }
        other => Err(format!(
            "a column of Arrow type {other} has no rhizodat value; cast it in SQL"
        )),
    }
}

fn text_or_bytes(raw: &[u8]) -> RowValue {
    std::str::from_utf8(raw).map_or_else(
        |_| RowValue::Bytes(raw.to_vec()),
        |s| RowValue::Str(s.to_owned()),
    )
}

/// Days since 1970-01-01 as `YYYY-MM-DD` (proleptic Gregorian).
fn date(days: i32) -> String {
    let (y, m, d) = civil_from_days(i64::from(days));
    format!("{y:04}-{m:02}-{d:02}")
}

/// A timestamp as ISO 8601, with `+00:00` when it has a time zone (Arrow keeps
/// the instant in UTC).
fn datetime(raw: i64, unit: TimeUnit, zoned: bool) -> String {
    let per_second: i64 = match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    };
    let seconds = raw.div_euclid(per_second);
    let fraction = raw.rem_euclid(per_second);
    let (days, rest) = (seconds.div_euclid(86_400), seconds.rem_euclid(86_400));
    let (y, m, d) = civil_from_days(days);
    let (hh, mm, ss) = (rest / 3600, rest % 3600 / 60, rest % 60);
    let micros = fraction * 1_000_000 / per_second;
    let offset = if zoned { "+00:00" } else { "" };
    format!("{y:04}-{m:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}.{micros:06}{offset}")
}

/// Howard Hinnant's days-to-civil algorithm.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

/// An unscaled 128-bit value and its scale as exact decimal text.
fn decimal(raw: i128, scale: i8) -> String {
    let negative = raw < 0;
    let digits = raw.unsigned_abs().to_string();
    let text = match usize::try_from(scale) {
        Ok(0) | Err(_) => digits,
        Ok(scale) => {
            let padded = format!("{digits:0>width$}", width = scale + 1);
            let (int, frac) = padded.split_at(padded.len() - scale);
            format!("{int}.{frac}")
        }
    };
    if negative { format!("-{text}") } else { text }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dates_and_times_are_iso() {
        assert_eq!(date(0), "1970-01-01");
        assert_eq!(date(20_406), "2025-11-14");
        assert_eq!(date(-1), "1969-12-31");
        assert_eq!(
            datetime(1_763_112_615_123_456, TimeUnit::Microsecond, true),
            "2025-11-14T09:30:15.123456+00:00"
        );
        assert_eq!(
            datetime(-1, TimeUnit::Second, false),
            "1969-12-31T23:59:59.000000"
        );
    }

    #[test]
    fn rows_become_a_batch_of_their_values_types() {
        let id = uuid::Uuid::from_u128(1);
        let batch = record_batch(
            &[
                "n".into(),
                "x".into(),
                "id".into(),
                "at".into(),
                "gone".into(),
            ],
            &[
                vec![
                    RowValue::Int(1),
                    RowValue::Int(2),
                    RowValue::Uuid(id),
                    RowValue::DateTime("2026-11-14T09:30:00+00:00".into()),
                    RowValue::Null,
                ],
                vec![
                    RowValue::Null,
                    RowValue::Float(0.5),
                    RowValue::Null,
                    RowValue::Null,
                    RowValue::Null,
                ],
            ],
        )
        .unwrap();
        let types: Vec<DataType> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        assert_eq!(
            types,
            [
                DataType::Int64,
                DataType::Float64,
                DataType::FixedSizeBinary(16),
                DataType::Utf8,
                DataType::Null
            ]
        );
        let rows = batch_rows(&[batch]).unwrap();
        assert_eq!(rows[0][2], RowValue::Uuid(id));
        assert_eq!(rows[1][1], RowValue::Float(0.5));
        assert_eq!(rows[0][1], RowValue::Float(2.0));
        assert!(
            record_batch(
                &["a".into()],
                &[vec![RowValue::Int(1)], vec![RowValue::Str("x".into())]]
            )
            .is_err()
        );
        assert!(record_batch(&["a".into()], &[vec![RowValue::List(vec![])]]).is_err());
    }

    #[test]
    fn decimals_keep_their_scale() {
        assert_eq!(decimal(123_400, 4), "12.3400");
        assert_eq!(decimal(-5, 2), "-0.05");
        assert_eq!(decimal(42, 0), "42");
        assert_eq!(decimal(0, 3), "0.000");
    }
}
