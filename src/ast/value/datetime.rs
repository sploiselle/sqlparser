use std::error;
use std::fmt;
use std::time::Duration;

use super::ValueError;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IntervalValue {
    /// The raw `[value]` that was present in `INTERVAL '[value]'`
    pub value: String,
    /// the fully parsed date time
    pub parsed: ParsedDateTime,
    /// How much precision to keep track of
    ///
    /// If this is ommitted, then you are supposed to ignore all of the
    /// non-lead fields. If it is less precise than the final field, you
    /// are supposed to ignore the final field.
    ///
    /// For the following specifications:
    ///
    /// * `INTERVAL '1:1:1' HOURS TO SECONDS` the `last_field` gets
    ///   `Some(DateTimeField::Second)` and interpreters should generate an
    ///   interval equivalent to `3661` seconds.
    /// * In `INTERVAL '1:1:1' HOURS` the `last_field` gets `None` and
    ///   interpreters should generate an interval equivalent to `3600`
    ///   seconds.
    /// * In `INTERVAL '1:1:1' HOURS TO MINUTES` the interval should be
    ///   equivalent to `3660` seconds.
    pub precision_high: DateTimeField,
    pub precision_low: DateTimeField,
    /// The seconds precision can be specified in SQL source as
    /// `INTERVAL '__' SECOND(_, x)` (in which case the `leading_field`
    /// will be `Second` and the `last_field` will be `None`),
    /// or as `__ TO SECOND(x)`.
    pub fractional_seconds_precision: Option<u64>,
}

impl Default for IntervalValue {
    fn default() -> Self {
        Self {
            value: String::default(),
            parsed: ParsedDateTime::default(),
            precision_high: DateTimeField::Year,
            precision_low: DateTimeField::Second,
            fractional_seconds_precision: None,
        }
    }
}

impl IntervalValue {
    /// Get Either the number of Months or the Duration specified by this interval
    ///
    /// This computes the fiels permissively: it assumes that the leading field
    /// (i.e. the lead in `INTERVAL 'str' LEAD [TO LAST]`) is valid and parses
    /// all field in the `str` starting at the leading field, ignoring the
    /// truncation that should be specified by `LAST`.
    ///
    /// See also the related [`fields_match_precision`] function that will give
    /// an error if the interval string does not exactly match the `FROM TO
    /// LAST` spec.
    ///
    /// # Errors
    ///
    /// If a required field is missing (i.e. there is no value) or the `TO
    /// LAST` field is larger than the `LEAD`.
    pub fn computed_permissive(&self) -> Result<Interval, ValueError> {
        use DateTimeField::*;
        let mut months = 0i64;
        let mut seconds = 0i128;
        let mut nanos = 0i64;

        let mut add_field = |d: DateTimeField| match d {
            Year => {
                let (y, y_f) = self.units_of(Year);

                months += y.unwrap_or(0) as i64 * 12;
                months += y_f.unwrap_or(0) * 12 / 1_000_000_000;
            }
            Month => {
                let (m, m_f) = self.units_of(Month);

                months += m.unwrap_or(0) as i64;

                // Poatgres treats months as having 30 days.
                let s_n = m_f.unwrap_or(0) * 30 * seconds_multiplier(Day) as i64;

                seconds += s_n as i128 / 1_000_000_000;

                nanos += s_n % 1_000_000_000;
            }
            Second => {
                let (s, n) = self.units_of(Second);
                seconds += s.unwrap_or(0);
                nanos += n.unwrap_or(0);
            }
            d => {
                let (t, t_f) = self.units_of(d);

                seconds += t.unwrap_or(0) * seconds_multiplier(d);

                let s_n = t_f.unwrap_or(0) * (seconds_multiplier(d) as i64);

                seconds += s_n as i128 / 1_000_000_000;

                nanos += s_n % 1_000_000_000;
            }
        };

        add_field(self.precision_high);

        let min_field = &self.precision_low.clone();
        println!("parsed: {}", self.parsed);
        for field in self
            .precision_high
            .clone()
            .into_iter()
            .take_while(|f| f <= &Second)
        {
            add_field(field);
        }

        // Round fields based on min_field.
        match self.precision_low {
            Year => {
                months -= months % 12;
                seconds = 0;
                nanos = 0;
            }
            Month => {
                seconds = 0;
                nanos = 0;
            }
            // Round nanos
            Second => {
                let mut precision = 6;
                let mut remainder = 0;
                if let Some(p) = self.fractional_seconds_precision {
                    precision = p;
                }

                if precision > 6 {
                    return Err(ValueError(format!(
                        "Precision of nanoseconds must be (0, 6), have {}",
                        precision
                    )));
                }
                remainder = nanos % 10_i64.pow(9 - precision as u32);
                println!("{} remainder pre", remainder);

                println!(
                    "Rounding consideration {}",
                    remainder / 10_i64.pow(8 - precision as u32)
                );
                // Check for round up.
                if remainder / 10_i64.pow(8 - precision as u32) > 4 {
                    nanos += 10_i64.pow(9 - precision as u32);
                }
                println!("{} remainder post", remainder);

                nanos -= remainder;
            }
            dhm => {
                println!("Rounding to {}", dhm);
                println!("Before rounding {}", seconds);
                seconds -= seconds % seconds_multiplier(dhm);
                println!("After rounding {}", seconds);
                nanos = 0;
            }
        }

        if nanos < 0 && seconds > 0 {
            if let Some(n) = 1_000_000_000_i64.checked_add(nanos) {
                nanos = n;
                seconds -= 1;
            } else {
                return Err(ValueError(format!("Nanos in INTERVAL overflowed")));
            }
        } else if nanos > 0 && seconds < 0 {
            if let Some(n) = 1_000_000_000_i64.checked_sub(nanos) {
                nanos = -n;
                seconds += 1;
            } else {
                return Err(ValueError(format!("Nanos in INTERVAL overflowed")));
            }
        }

        println!("final seconds {}", seconds);

        Ok(Interval {
            months,
            duration: Duration::new(seconds.abs() as u64, nanos.abs() as u32),
            is_positive: seconds >= 0 && nanos >= 0,
        })
    }

    /// Retrieve the number that we parsed out of the literal string for the `field`
    fn units_of(&self, field: DateTimeField) -> (Option<i128>, Option<i64>) {
        match field {
            DateTimeField::Year => (self.parsed.year, self.parsed.year_frac),
            DateTimeField::Month => (self.parsed.month, self.parsed.month_frac),
            DateTimeField::Day => (self.parsed.day, self.parsed.day_frac),
            DateTimeField::Hour => (self.parsed.hour, self.parsed.hour_frac),
            DateTimeField::Minute => (self.parsed.minute, self.parsed.minute_frac),
            DateTimeField::Second => (self.parsed.second, self.parsed.nano),
        }
    }
}
/// Verify that the fields in me make sense
///
/// Returns Ok if the fields are fully specified, otherwise an error
///
/// # Examples
///
/// ```sql
/// INTERVAL '1 5' DAY TO HOUR -- Ok
/// INTERVAL '1 5' DAY         -- Err
/// INTERVAL '1:2:3' HOUR TO SECOND   -- Ok
/// INTERVAL '1:2:3' HOUR TO MINUTE   -- Err
/// INTERVAL '1:2:3' MINUTE TO SECOND -- Err
/// INTERVAL '1:2:3' DAY TO SECOND    -- Err
/// ```
// pub fn fields_match_precision(&self) -> Result<(), ValueError> {
//     let mut errors = vec![];
//     let last_field = self
//         .last_field
//         .as_ref()
//         .unwrap_or_else(|| &self.leading_field);
//     let mut extra_leading_fields = vec![];
//     let mut extra_trailing_fields = vec![];
//     // check for more data in the input string than was requested in <FIELD> TO <FIELD>
//     for field in std::iter::once(DateTimeField::Year).chain(DateTimeField::Year.into_iter()) {
//         if self.units_of(&field).is_none() {
//             continue;
//         }

//         if field < self.leading_field {
//             extra_leading_fields.push(field.clone());
//         }
//         if &field > last_field {
//             extra_trailing_fields.push(field.clone());
//         }
//     }

//     if !extra_leading_fields.is_empty() {
//         errors.push(format!(
//             "The interval string '{}' specifies {}s but the significance requested is {}",
//             self.value,
//             fields_msg(extra_leading_fields.into_iter()),
//             self.leading_field
//         ));
//     }
//     if !extra_trailing_fields.is_empty() {
//         errors.push(format!(
//             "The interval string '{}' specifies {}s but the requested precision would truncate to {}",
//             self.value, fields_msg(extra_trailing_fields.into_iter()), last_field
//         ));
//     }

//     // check for data requested by the <FIELD> TO <FIELD> that does not exist in the data
//     let missing_fields = match (
//         self.units_of(&self.leading_field),
//         self.units_of(&last_field),
//     ) {
//         (Some(_), Some(_)) => vec![],
//         (None, Some(_)) => vec![&self.leading_field],
//         (Some(_), None) => vec![last_field],
//         (None, None) => vec![&self.leading_field, last_field],
//     };

//     // if !missing_fields.is_empty() {
//     //     errors.push(format!(
//     //         "The interval string '{}' provides {} - which does not include the requested field(s) {}",
//     //         self.value, self.present_fields(), fields_msg(missing_fields.into_iter().cloned())));
//     // }

//     if !errors.is_empty() {
//         Err(ValueError(errors.join("; ")))
//     } else {
//         Ok(())
//     }
// }

// fn present_fields(&self) -> String {
//     fields_msg(
//         std::iter::once(DateTimeField::Year)
//             .chain(DateTimeField::Year.into_iter())
//             .filter(|field| self.units_of(&field).is_some()),
//     )
// }

// fn fields_msg(fields: impl Iterator<Item = DateTimeField>) -> String {
//     fields
//         .map(|field: DateTimeField| field.to_string())
//         .collect::<Vec<_>>()
//         .join(", ")
// }

fn seconds_multiplier(field: DateTimeField) -> i128 {
    match field {
        DateTimeField::Day => 60 * 60 * 24,
        DateTimeField::Hour => 60 * 60,
        DateTimeField::Minute => 60,
        DateTimeField::Second => 1,
        _other => unreachable!("Do not call with a non-duration field"),
    }
}

/// The result of parsing an `INTERVAL '<value>' <unit> [TO <precision>]`
///
/// Units of type `YEAR` or `MONTH` are semantically some multiple of months,
/// which are not well defined, and this parser normalizes them to some number
/// of months.
///
/// Intervals of unit [`DateTimeField::Day`] or smaller are semantically a
/// multiple of seconds.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Interval {
    /// A possibly negative number of months for field types like `YEAR`
    pub months: i64,
    /// An actual timespan, possibly negative, because why not
    pub duration: Duration,
    pub is_positive: bool,
}

/// The fields of a Date
///
/// This is not guaranteed to be a valid date
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ParsedDate {
    pub year: i64,
    pub month: u8,
    pub day: u8,
}

/// The fields in a `Timestamp`
///
/// Similar to a [`ParsedDateTime`], except that all the fields are required.
///
/// This is not guaranteed to be a valid date
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ParsedTimestamp {
    pub year: i64,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub nano: u32,
    pub timezone_offset_second: i64,
}

/// All of the fields that can appear in a literal `DATE`, `TIMESTAMP` or `INTERVAL` string
///
/// This is only used in an `Interval`, which can have any contiguous set of
/// fields set, otherwise you are probably looking for [`ParsedDate`] or
/// [`ParsedTimestamp`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ParsedDateTime {
    pub year: Option<i128>,
    pub year_frac: Option<i64>,
    pub month: Option<i128>,
    pub month_frac: Option<i64>,
    pub day: Option<i128>,
    pub day_frac: Option<i64>,
    pub hour: Option<i128>,
    pub hour_frac: Option<i64>,
    pub minute: Option<i128>,
    pub minute_frac: Option<i64>,
    pub second: Option<i128>,
    pub nano: Option<i64>,
    pub timezone_offset_second: Option<i64>,
}

impl Default for ParsedDateTime {
    fn default() -> ParsedDateTime {
        ParsedDateTime {
            year: None,
            year_frac: None,
            month: None,
            month_frac: None,
            day: None,
            day_frac: None,
            hour: None,
            hour_frac: None,
            minute: None,
            minute_frac: None,
            second: None,
            nano: None,
            timezone_offset_second: None,
        }
    }
}

impl fmt::Display for ParsedDateTime {
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(v) = self.year {
            write!(f, "{} year ", v)?;
        }
        if let Some(v) = self.month {
            write!(f, "{} month ", v)?;
        }
        if let Some(v) = self.day {
            write!(f, "{} day ", v)?;
        }
        if let Some(v) = self.hour {
            write!(f, "{} hour ", v)?;
        }
        if let Some(v) = self.minute {
            write!(f, "{} minute ", v)?;
        }
        if let Some(v) = self.second {
            write!(f, "{} second ", v)?;
        }
        if let Some(v) = self.nano {
            write!(f, "{} nanos ", v)?;
        }

        Ok(())
    }
}

impl ParsedDateTime {
    pub fn write_field_iff_none(
        &mut self,
        d: DateTimeField,
        v: Option<i128>,
        f: Option<i64>,
    ) -> Result<(), &'static str> {
        use DateTimeField::*;

        match d {
            Year if self.year.is_none() => {
                self.year = v;
                self.year_frac = f;
            }
            Month if self.month.is_none() => {
                self.month = v;
                self.month_frac = f;
            }
            Day if self.day.is_none() => {
                self.day = v;
                self.day_frac = f;
            }
            Hour if self.hour.is_none() => {
                self.hour = v;
                self.hour_frac = f;
            }
            Minute if self.minute.is_none() => {
                self.minute = v;
                self.minute_frac = f;
            }
            Second => {
                if v.is_some() {
                    if self.second.is_none() {
                        self.second = v;
                    } else {
                        return Err("Some field set twice");
                    }
                }

                if f.is_some() {
                    if self.nano.is_none() {
                        self.nano = f;
                    } else {
                        return Err("Some field set twice");
                    }
                }
            }
            _ => {
                return Err("Some field set twice");
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub enum DateTimeField {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

impl fmt::Display for DateTimeField {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            DateTimeField::Year => "YEAR",
            DateTimeField::Month => "MONTH",
            DateTimeField::Day => "DAY",
            DateTimeField::Hour => "HOUR",
            DateTimeField::Minute => "MINUTE",
            DateTimeField::Second => "SECOND",
        })
    }
}

/// Iterate over `DateTimeField`s in descending significance
impl IntoIterator for DateTimeField {
    type Item = DateTimeField;
    type IntoIter = DateTimeFieldIterator;
    fn into_iter(self) -> DateTimeFieldIterator {
        DateTimeFieldIterator(Some(self))
    }
}

/// An iterator over DateTimeFields
///
/// Always starts with the value smaller than the current one.
///
/// ```
/// use sqlparser::ast::DateTimeField::*;
/// let mut itr = Hour.into_iter();
/// assert_eq!(itr.next(), Some(Minute));
/// assert_eq!(itr.next(), Some(Second));
/// assert_eq!(itr.next(), None);
/// ```
pub struct DateTimeFieldIterator(Option<DateTimeField>);

/// Go through fields in descending significance order
impl Iterator for DateTimeFieldIterator {
    type Item = DateTimeField;
    fn next(&mut self) -> Option<Self::Item> {
        use DateTimeField::*;
        self.0 = match self.0 {
            Some(Year) => Some(Month),
            Some(Month) => Some(Day),
            Some(Day) => Some(Hour),
            Some(Hour) => Some(Minute),
            Some(Minute) => Some(Second),
            Some(Second) => None,
            None => None,
        };
        self.0.clone()
    }
}

/// Similar to a [`DateTimeField`], but with a few more options
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ExtractField {
    Millenium,
    Century,
    Decade,
    Year,
    /// The ISO Week-Numbering year
    ///
    /// See https://en.wikipedia.org/wiki/ISO_week_date
    IsoYear,
    Quarter,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    Milliseconds,
    Microseconds,
    // Weirder fields
    Timezone,
    TimezoneHour,
    TimezoneMinute,
    WeekOfYear,
    /// The day of the year (1 - 365/366)
    DayOfYear,
    /// The day of the week (0 - 6; Sunday is 0)
    DayOfWeek,
    /// The day of the week (1 - 7; Sunday is 7)
    IsoDayOfWeek,
    /// The number of seconds
    ///
    /// * for DateTime fields, the number of seconds since 1970-01-01 00:00:00-00
    /// * for intervals, the total number of seconds in the interval
    Epoch,
}

impl fmt::Display for ExtractField {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ExtractField::Millenium => f.write_str("MILLENIUM"),
            ExtractField::Century => f.write_str("CENTURY"),
            ExtractField::Decade => f.write_str("DECADE"),
            ExtractField::Year => f.write_str("YEAR"),
            ExtractField::IsoYear => f.write_str("ISOYEAR"),
            ExtractField::Quarter => f.write_str("QUARTER"),
            ExtractField::Month => f.write_str("MONTH"),
            ExtractField::Day => f.write_str("DAY"),
            ExtractField::Hour => f.write_str("HOUR"),
            ExtractField::Minute => f.write_str("MINUTE"),
            ExtractField::Second => f.write_str("SECOND"),
            ExtractField::Milliseconds => f.write_str("MILLISECONDS"),
            ExtractField::Microseconds => f.write_str("MICROSECONDS"),
            // Weirder fields
            ExtractField::Timezone => f.write_str("TIMEZONE"),
            ExtractField::TimezoneHour => f.write_str("TIMEZONE_HOUR"),
            ExtractField::TimezoneMinute => f.write_str("TIMEZONE_MINUTE"),
            ExtractField::WeekOfYear => f.write_str("WEEK"),
            ExtractField::DayOfYear => f.write_str("DOY"),
            ExtractField::DayOfWeek => f.write_str("DOW"),
            ExtractField::IsoDayOfWeek => f.write_str("ISODOW"),
            ExtractField::Epoch => f.write_str("EPOCH"),
        }
    }
}

use std::str::FromStr;

impl FromStr for ExtractField {
    type Err = ValueError;
    fn from_str(s: &str) -> Result<ExtractField, Self::Err> {
        Ok(match &*s.to_uppercase() {
            "MILLENIUM" => ExtractField::Millenium,
            "CENTURY" => ExtractField::Century,
            "DECADE" => ExtractField::Decade,
            "YEAR" => ExtractField::Year,
            "ISOYEAR" => ExtractField::IsoYear,
            "QUARTER" => ExtractField::Quarter,
            "MONTH" => ExtractField::Month,
            "DAY" => ExtractField::Day,
            "HOUR" => ExtractField::Hour,
            "MINUTE" => ExtractField::Minute,
            "SECOND" => ExtractField::Second,
            "MILLISECONDS" => ExtractField::Milliseconds,
            "MICROSECONDS" => ExtractField::Microseconds,
            // Weirder fields
            "TIMEZONE" => ExtractField::Timezone,
            "TIMEZONE_HOUR" => ExtractField::TimezoneHour,
            "TIMEZONE_MINUTE" => ExtractField::TimezoneMinute,
            "WEEK" => ExtractField::WeekOfYear,
            "DOY" => ExtractField::DayOfYear,
            "DOW" => ExtractField::DayOfWeek,
            "ISODOW" => ExtractField::IsoDayOfWeek,
            "EPOCH" => ExtractField::Epoch,
            _ => return Err(ValueError(format!("invalid EXTRACT specifier: {}", s))),
        })
    }
}
