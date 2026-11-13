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
    let mut rows = Vec::new();
    for batch in reader {
        let batch: RecordBatch = batch.map_err(|e| e.to_string())?;
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
    Ok((columns, rows))
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
    fn decimals_keep_their_scale() {
        assert_eq!(decimal(123_400, 4), "12.3400");
        assert_eq!(decimal(-5, 2), "-0.05");
        assert_eq!(decimal(42, 0), "42");
        assert_eq!(decimal(0, 3), "0.000");
    }
}
