use crate::write::storage::open_dataset;
use arrow::array::{
    Array, Date32Array, Date64Array, Decimal128Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{Duration, SecondsFormat};
use futures::StreamExt;
use std::time::Instant;
use tokio::runtime::Runtime;

pub type ColumnAggregateResult = (Option<String>, String, i64);

#[derive(Clone, Copy)]
enum Extremum {
    Min,
    Max,
}

pub fn lance_min_impl(
    uri: &str,
    column_name: &str,
    server_name: Option<&str>,
) -> Result<ColumnAggregateResult, String> {
    lance_extremum_impl(uri, column_name, server_name, Extremum::Min)
}

pub fn lance_max_impl(
    uri: &str,
    column_name: &str,
    server_name: Option<&str>,
) -> Result<ColumnAggregateResult, String> {
    lance_extremum_impl(uri, column_name, server_name, Extremum::Max)
}

fn lance_extremum_impl(
    uri: &str,
    column_name: &str,
    server_name: Option<&str>,
    op: Extremum,
) -> Result<ColumnAggregateResult, String> {
    let started = Instant::now();
    let rt = Runtime::new().map_err(|e| format!("failed to create tokio runtime: {}", e))?;

    let (value, data_type) = rt.block_on(async move {
        let dataset = open_dataset(uri, server_name).await?;
        let field = dataset
            .schema()
            .fields
            .iter()
            .find(|field| field.name == column_name)
            .ok_or_else(|| format!("column '{}' not found in Lance dataset", column_name))?;
        let data_type = field.data_type().clone();

        let mut scanner = dataset.scan();
        scanner
            .project(&[column_name])
            .map_err(|e| format!("failed to project column '{}': {}", column_name, e))?;
        let mut stream = scanner
            .try_into_stream()
            .await
            .map_err(|e| format!("failed to open Lance scan stream: {}", e))?;

        let mut state = ExtremumState::new(&data_type)?;
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|e| format!("failed to read Lance batch: {}", e))?;
            let column = batch.column(0);
            state.update(column.as_ref(), op)?;
        }

        Ok::<_, String>((state.finish()?, data_type.to_string()))
    })?;

    Ok((value, data_type, started.elapsed().as_millis() as i64))
}

enum ExtremumState {
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    UInt8(Option<u8>),
    UInt16(Option<u16>),
    UInt32(Option<u32>),
    UInt64(Option<u64>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    Date32(Option<i32>),
    Date64(Option<i64>),
    TimestampSecond(Option<i64>, bool),
    TimestampMillisecond(Option<i64>, bool),
    TimestampMicrosecond(Option<i64>, bool),
    TimestampNanosecond(Option<i64>, bool),
    Decimal128(Option<i128>, i8),
}

impl ExtremumState {
    fn new(data_type: &DataType) -> Result<Self, String> {
        match data_type {
            DataType::Int8 => Ok(Self::Int8(None)),
            DataType::Int16 => Ok(Self::Int16(None)),
            DataType::Int32 => Ok(Self::Int32(None)),
            DataType::Int64 => Ok(Self::Int64(None)),
            DataType::UInt8 => Ok(Self::UInt8(None)),
            DataType::UInt16 => Ok(Self::UInt16(None)),
            DataType::UInt32 => Ok(Self::UInt32(None)),
            DataType::UInt64 => Ok(Self::UInt64(None)),
            DataType::Float32 => Ok(Self::Float32(None)),
            DataType::Float64 => Ok(Self::Float64(None)),
            DataType::Date32 => Ok(Self::Date32(None)),
            DataType::Date64 => Ok(Self::Date64(None)),
            DataType::Timestamp(TimeUnit::Second, tz) => Ok(Self::TimestampSecond(None, tz.is_some())),
            DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                Ok(Self::TimestampMillisecond(None, tz.is_some()))
            }
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                Ok(Self::TimestampMicrosecond(None, tz.is_some()))
            }
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                Ok(Self::TimestampNanosecond(None, tz.is_some()))
            }
            DataType::Decimal128(_, scale) => Ok(Self::Decimal128(None, *scale)),
            other => Err(format!(
                "lance_min/lance_max do not support column type {}; supported types are integer, float, date, timestamp, and decimal128",
                other
            )),
        }
    }

    fn update(&mut self, array: &dyn Array, op: Extremum) -> Result<(), String> {
        match self {
            Self::Int8(current) => {
                update_ord(current, typed_array::<Int8Array>(array, "int8")?, op)
            }
            Self::Int16(current) => {
                update_ord(current, typed_array::<Int16Array>(array, "int16")?, op)
            }
            Self::Int32(current) => {
                update_ord(current, typed_array::<Int32Array>(array, "int32")?, op)
            }
            Self::Int64(current) => {
                update_ord(current, typed_array::<Int64Array>(array, "int64")?, op)
            }
            Self::UInt8(current) => {
                update_ord(current, typed_array::<UInt8Array>(array, "uint8")?, op)
            }
            Self::UInt16(current) => {
                update_ord(current, typed_array::<UInt16Array>(array, "uint16")?, op)
            }
            Self::UInt32(current) => {
                update_ord(current, typed_array::<UInt32Array>(array, "uint32")?, op)
            }
            Self::UInt64(current) => {
                update_ord(current, typed_array::<UInt64Array>(array, "uint64")?, op)
            }
            Self::Float32(current) => {
                update_float(current, typed_array::<Float32Array>(array, "float32")?, op)
            }
            Self::Float64(current) => {
                update_float(current, typed_array::<Float64Array>(array, "float64")?, op)
            }
            Self::Date32(current) => {
                update_ord(current, typed_array::<Date32Array>(array, "date32")?, op)
            }
            Self::Date64(current) => {
                update_ord(current, typed_array::<Date64Array>(array, "date64")?, op)
            }
            Self::TimestampSecond(current, _) => update_ord(
                current,
                typed_array::<TimestampSecondArray>(array, "timestamp second")?,
                op,
            ),
            Self::TimestampMillisecond(current, _) => update_ord(
                current,
                typed_array::<TimestampMillisecondArray>(array, "timestamp millisecond")?,
                op,
            ),
            Self::TimestampMicrosecond(current, _) => update_ord(
                current,
                typed_array::<TimestampMicrosecondArray>(array, "timestamp microsecond")?,
                op,
            ),
            Self::TimestampNanosecond(current, _) => update_ord(
                current,
                typed_array::<TimestampNanosecondArray>(array, "timestamp nanosecond")?,
                op,
            ),
            Self::Decimal128(current, _) => update_ord(
                current,
                typed_array::<Decimal128Array>(array, "decimal128")?,
                op,
            ),
        }
        Ok(())
    }

    fn finish(self) -> Result<Option<String>, String> {
        match self {
            Self::Int8(value) => Ok(value.map(|v| v.to_string())),
            Self::Int16(value) => Ok(value.map(|v| v.to_string())),
            Self::Int32(value) => Ok(value.map(|v| v.to_string())),
            Self::Int64(value) => Ok(value.map(|v| v.to_string())),
            Self::UInt8(value) => Ok(value.map(|v| v.to_string())),
            Self::UInt16(value) => Ok(value.map(|v| v.to_string())),
            Self::UInt32(value) => Ok(value.map(|v| v.to_string())),
            Self::UInt64(value) => Ok(value.map(|v| v.to_string())),
            Self::Float32(value) => Ok(value.map(|v| v.to_string())),
            Self::Float64(value) => Ok(value.map(|v| v.to_string())),
            Self::Date32(value) => value.map(format_date32).transpose(),
            Self::Date64(value) => value.map(format_date64).transpose(),
            Self::TimestampSecond(value, has_tz) => value
                .map(|v| format_timestamp_micros(v.saturating_mul(1_000_000), has_tz))
                .transpose(),
            Self::TimestampMillisecond(value, has_tz) => value
                .map(|v| format_timestamp_micros(v.saturating_mul(1_000), has_tz))
                .transpose(),
            Self::TimestampMicrosecond(value, has_tz) => value
                .map(|v| format_timestamp_micros(v, has_tz))
                .transpose(),
            Self::TimestampNanosecond(value, has_tz) => value
                .map(|v| format_timestamp_micros(v / 1_000, has_tz))
                .transpose(),
            Self::Decimal128(value, scale) => Ok(value.map(|v| format_decimal128(v, scale))),
        }
    }
}

fn typed_array<'a, T: 'static>(array: &'a dyn Array, label: &str) -> Result<&'a T, String> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| format!("invalid {} array", label))
}

fn update_ord<T, A>(current: &mut Option<T>, array: &A, op: Extremum)
where
    T: Copy + Ord,
    A: ArrayAccessor<T>,
{
    for row in 0..array.array_len() {
        if array.is_null_at(row) {
            continue;
        }
        let value = array.value_at(row);
        if should_replace(*current, value, op) {
            *current = Some(value);
        }
    }
}

fn update_float<T, A>(current: &mut Option<T>, array: &A, op: Extremum)
where
    T: Copy + PartialOrd + FloatValue,
    A: ArrayAccessor<T>,
{
    for row in 0..array.array_len() {
        if array.is_null_at(row) {
            continue;
        }
        let value = array.value_at(row);
        if value.is_nan() {
            continue;
        }
        if should_replace(*current, value, op) {
            *current = Some(value);
        }
    }
}

fn should_replace<T: PartialOrd>(current: Option<T>, candidate: T, op: Extremum) -> bool {
    match current {
        None => true,
        Some(current) => match op {
            Extremum::Min => candidate < current,
            Extremum::Max => candidate > current,
        },
    }
}

trait ArrayAccessor<T> {
    fn array_len(&self) -> usize;
    fn is_null_at(&self, row: usize) -> bool;
    fn value_at(&self, row: usize) -> T;
}

macro_rules! impl_array_accessor {
    ($array:ty, $value:ty) => {
        impl ArrayAccessor<$value> for $array {
            fn array_len(&self) -> usize {
                self.len()
            }

            fn is_null_at(&self, row: usize) -> bool {
                self.is_null(row)
            }

            fn value_at(&self, row: usize) -> $value {
                self.value(row)
            }
        }
    };
}

impl_array_accessor!(Int8Array, i8);
impl_array_accessor!(Int16Array, i16);
impl_array_accessor!(Int32Array, i32);
impl_array_accessor!(Int64Array, i64);
impl_array_accessor!(UInt8Array, u8);
impl_array_accessor!(UInt16Array, u16);
impl_array_accessor!(UInt32Array, u32);
impl_array_accessor!(UInt64Array, u64);
impl_array_accessor!(Float32Array, f32);
impl_array_accessor!(Float64Array, f64);
impl_array_accessor!(Date32Array, i32);
impl_array_accessor!(Date64Array, i64);
impl_array_accessor!(TimestampSecondArray, i64);
impl_array_accessor!(TimestampMillisecondArray, i64);
impl_array_accessor!(TimestampMicrosecondArray, i64);
impl_array_accessor!(TimestampNanosecondArray, i64);
impl_array_accessor!(Decimal128Array, i128);

trait FloatValue {
    fn is_nan(self) -> bool;
}

impl FloatValue for f32 {
    fn is_nan(self) -> bool {
        f32::is_nan(self)
    }
}

impl FloatValue for f64 {
    fn is_nan(self) -> bool {
        f64::is_nan(self)
    }
}

fn format_date32(days: i32) -> Result<String, String> {
    let base = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
        .ok_or_else(|| "invalid unix epoch date".to_string())?;
    let date = base
        .checked_add_signed(Duration::days(days as i64))
        .ok_or_else(|| "date32 value out of range".to_string())?;
    Ok(date.format("%Y-%m-%d").to_string())
}

fn format_date64(millis: i64) -> Result<String, String> {
    let timestamp = chrono::DateTime::from_timestamp_millis(millis)
        .ok_or_else(|| "date64 value out of range".to_string())?;
    Ok(timestamp.date_naive().format("%Y-%m-%d").to_string())
}

fn format_timestamp_micros(micros: i64, has_tz: bool) -> Result<String, String> {
    let timestamp = chrono::DateTime::from_timestamp_micros(micros)
        .ok_or_else(|| "timestamp value out of range".to_string())?;
    if has_tz {
        Ok(timestamp.to_rfc3339_opts(SecondsFormat::Micros, true))
    } else {
        Ok(timestamp
            .naive_utc()
            .format("%Y-%m-%d %H:%M:%S%.6f")
            .to_string())
    }
}

fn format_decimal128(value: i128, scale: i8) -> String {
    if scale <= 0 {
        let zeros = "0".repeat((-scale) as usize);
        return format!("{}{}", value, zeros);
    }

    let scale = scale as usize;
    let negative = value.is_negative();
    let mut digits = value.unsigned_abs().to_string();
    if digits.len() <= scale {
        let zeros = "0".repeat(scale + 1 - digits.len());
        digits = format!("{}{}", zeros, digits);
    }

    let point = digits.len() - scale;
    digits.insert(point, '.');
    if negative {
        format!("-{}", digits)
    } else {
        digits
    }
}
