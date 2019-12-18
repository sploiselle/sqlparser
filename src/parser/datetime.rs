use crate::ast::ParsedDateTime;
use crate::parser::{DateTimeField, ParserError};
use std::iter::Iterator;

pub(crate) fn tokenize_interval(value: &str) -> Result<Vec<IntervalToken>, ParserError> {
    let mut toks = vec![];
    let mut num_buf = String::with_capacity(4);
    let mut char_buf = String::with_capacity(7);
    println!("tokenize_interval(value: &str) str {}", value);
    fn parse_num(n: &str, idx: usize) -> Result<IntervalToken, ParserError> {
        Ok(IntervalToken::Num(n.parse().map_err(|e| {
            ParserError::ParserError(format!(
                "Unable to parse value as a number at index {}: {}",
                idx, e
            ))
        })?))
    };

    let tokenize_buffers = |i: usize| -> Result<(), ParserError> {
        if !num_buf.is_empty() {
            toks.push(parse_num(&num_buf, i)?);
            num_buf.clear();
        } else if !char_buf.is_empty() {
            toks.push(IntervalToken::TimeUnit(char_buf.clone()));
            char_buf.clear();
        }
        Ok(())
    };
    let mut last_field_is_frac = false;
    for (i, chr) in value.chars().enumerate() {
        match chr {
            '-' => {
                tokenize_buffers(i);
                toks.push(IntervalToken::Dash);
            }
            ' ' => {
                tokenize_buffers(i);
                toks.push(IntervalToken::Space);
            }
            ':' => {
                tokenize_buffers(i);
                toks.push(IntervalToken::Colon);
            }
            '.' => {
                tokenize_buffers(i);
                toks.push(IntervalToken::Dot);
                last_field_is_frac = true;
            }
            chr if chr.is_digit(10) => {
                if !char_buf.is_empty() {
                    return Err(ParserError::TokenizerError(format!(
                        "Invalid character at offset {} in {}: {:?}",
                        i, value, chr
                    )));
                }
                num_buf.push(chr)
            }
            chr if chr.is_ascii_alphabetic() => {
                if !num_buf.is_empty() {
                    return Err(ParserError::TokenizerError(format!(
                        "Invalid character at offset {} in {}: {:?}",
                        i, value, chr
                    )));
                }
                char_buf.push(chr)
            }
            chr => {
                return Err(ParserError::TokenizerError(format!(
                    "Invalid character at offset {} in {}: {:?}",
                    i, value, chr
                )));
            }
        }
    }
    if !num_buf.is_empty() {
        if !last_field_is_frac {
            toks.push(parse_num(&num_buf, 0)?);
        } else {
            let raw: u32 = num_buf.parse().map_err(|e| {
                ParserError::ParserError(format!(
                    "couldn't parse fraction of second {}: {}",
                    num_buf, e
                ))
            })?;
            // this is guaranteed to be ascii, so len is fine
            let chars = num_buf.len() as u32;
            let multiplicand = 1_000_000_000 / 10_u32.pow(chars);

            toks.push(IntervalToken::Nanos(raw * multiplicand));
        }
    }
    Ok(toks)
}

fn tokenize_timezone(value: &str) -> Result<Vec<IntervalToken>, ParserError> {
    let mut toks: Vec<IntervalToken> = vec![];
    let mut num_buf = String::with_capacity(4);
    // If the timezone string has a colon, we need to parse all numbers naively.
    // Otherwise we need to parse long sequences of digits as [..hhhhmm]
    let split_nums: bool = !value.contains(':');

    // Takes a string and tries to parse it as a number token and insert it into the
    // token list
    fn parse_num(
        toks: &mut Vec<IntervalToken>,
        n: &str,
        split_nums: bool,
        idx: usize,
    ) -> Result<(), ParserError> {
        if n.is_empty() {
            return Ok(());
        }

        let (first, second) = if n.len() > 2 && split_nums == true {
            let (first, second) = n.split_at(n.len() - 2);
            (first, Some(second))
        } else {
            (n, None)
        };

        toks.push(IntervalToken::Num(first.parse().map_err(|e| {
                ParserError::ParserError(format!(
                    "Error tokenizing timezone string: unable to parse value {} as a number at index {}: {}",
                    first, idx, e
                ))
            })?));

        if let Some(second) = second {
            toks.push(IntervalToken::Num(second.parse().map_err(|e| {
                ParserError::ParserError(format!(
                    "Error tokenizing timezone string: unable to parse value {} as a number at index {}: {}",
                    second, idx, e
                ))
            })?));
        }

        Ok(())
    };
    for (i, chr) in value.chars().enumerate() {
        match chr {
            '-' => {
                parse_num(&mut toks, &num_buf, split_nums, i)?;
                num_buf.clear();
                toks.push(IntervalToken::Dash);
            }
            ' ' => {
                parse_num(&mut toks, &num_buf, split_nums, i)?;
                num_buf.clear();
                toks.push(IntervalToken::Space);
            }
            ':' => {
                parse_num(&mut toks, &num_buf, split_nums, i)?;
                num_buf.clear();
                toks.push(IntervalToken::Colon);
            }
            '+' => {
                parse_num(&mut toks, &num_buf, split_nums, i)?;
                num_buf.clear();
                toks.push(IntervalToken::Plus);
            }
            chr if (chr == 'z' || chr == 'Z') && (i == value.len() - 1) => {
                parse_num(&mut toks, &num_buf, split_nums, i)?;
                num_buf.clear();
                toks.push(IntervalToken::Zulu);
            }
            chr if chr.is_digit(10) => num_buf.push(chr),
            chr if chr.is_ascii_alphabetic() => {
                parse_num(&mut toks, &num_buf, split_nums, i)?;
                let substring = &value[i..];
                toks.push(IntervalToken::TzName(substring.to_string()));
                return Ok(toks);
            }
            chr => {
                return Err(ParserError::TokenizerError(format!(
                    "Error tokenizing timezone string ({}): invalid character {:?} at offset {}",
                    value, chr, i
                )))
            }
        }
    }
    parse_num(&mut toks, &num_buf, split_nums, 0)?;
    Ok(toks)
}

/// Get the tokens that you *might* end up parsing starting with a most significant unit
///
/// For example, parsing `INTERVAL '9-5 4:3' MONTH` is *illegal*, but you
/// should interpret that as `9 months 5 days 4 hours 3 minutes`. This function
/// doesn't take any perspective on what things should be, it just teslls you
/// what the user might have meant.
fn potential_interval_tokens_ym(from: &DateTimeField) -> Result<Vec<IntervalToken>, ParserError> {
    use DateTimeField::*;
    use IntervalToken::*;

    let all_toks = [
        Num(0), // year
        Dash,
        Num(0), // month
        Space,
    ];
    let offset = match from {
        Year => 0,
        Month => 2,
        _ => {
            return Err(ParserError::ParserError(format!(
                "Error parsing ym tokens; invalid search key from {}",
                from
            )))
        }
    };

    Ok(all_toks[offset..].to_vec())
}

fn potential_interval_tokens_d(from: &DateTimeField) -> Result<Vec<IntervalToken>, ParserError> {
    use IntervalToken::*;

    let all_toks = [
        Num(0), // day
        Space,
    ];
    Ok(all_toks.to_vec())
}

fn potential_interval_tokens_hms(from: &DateTimeField) -> Result<Vec<IntervalToken>, ParserError> {
    use DateTimeField::*;
    use IntervalToken::*;

    let all_toks = [
        Num(0), // hour
        Colon,
        Num(0), // minute
        Colon,
        Num(0), // second
        Dot,
        Nanos(0), // Nanos
    ];
    let offset = match from {
        Hour => 0,
        Minute => 2,
        Second => 4,
        _ => {
            return Err(ParserError::ParserError(format!(
                "Error parsing hms tokens; invalid search key from {}",
                from
            )))
        }
    };
    Ok(all_toks[offset..].to_vec())
}

fn build_timezone_offset_second(tokens: &[IntervalToken], value: &str) -> Result<i64, ParserError> {
    use IntervalToken::*;
    let all_formats = [
        vec![Plus, Num(0), Colon, Num(0)],
        vec![Dash, Num(0), Colon, Num(0)],
        vec![Plus, Num(0), Num(0)],
        vec![Dash, Num(0), Num(0)],
        vec![Plus, Num(0)],
        vec![Dash, Num(0)],
        vec![TzName("".to_string())],
        vec![Zulu],
    ];

    let mut is_positive = true;
    let mut hour_offset: Option<i64> = None;
    let mut minute_offset: Option<i64> = None;

    for format in all_formats.iter() {
        let actual = tokens.iter();

        if actual.len() != format.len() {
            continue;
        }

        for (i, (atok, etok)) in actual.zip(format).enumerate() {
            match (atok, etok) {
                (Colon, Colon) | (Plus, Plus) => { /* Matching punctuation */ }
                (Dash, Dash) => {
                    is_positive = false;
                }
                (Num(val), Num(_)) => {
                    let val = *val;
                    match (hour_offset, minute_offset) {
                        (None, None) => if val <= 24 {
                            hour_offset = Some(val as i64);
                        } else {
                            // We can return an error here because in all the formats with numbers
                            // we require the first number to be an hour and we require it to be <= 24
                            return Err(ParserError::ParserError(format!(
                                "Error parsing timezone string ({}): timezone hour invalid {}",
                                value, val
                            )));
                        }
                        (Some(_), None) => if val <= 60 {
                            minute_offset = Some(val as i64);
                        } else {
                            return Err(ParserError::ParserError(format!(
                                "Error parsing timezone string ({}): timezone minute invalid {}",
                                value, val
                            )));
                        },
                        // We've already seen an hour and a minute so we should never see another number
                        (Some(_), Some(_)) => return Err(ParserError::ParserError(format!(
                            "Error parsing timezone string ({}): invalid value {} at token index {}", value,
                            val, i
                        ))),
                        (None, Some(_)) => unreachable!("parsed a minute before an hour!"),
                    }
                }
                (Zulu, Zulu) => return Ok(0 as i64),
                (TzName(val), TzName(_)) => {
                    // For now, we don't support named timezones
                    return Err(ParserError::ParserError(format!(
                        "Error parsing timezone string ({}): named timezones are not supported. \
                         Failed to parse {} at token index {}",
                        value, val, i
                    )));
                }
                (_, _) => {
                    // Theres a mismatch between this format and the actual token stream
                    // Stop trying to parse in this format and go to the next one
                    is_positive = true;
                    hour_offset = None;
                    minute_offset = None;
                    break;
                }
            }
        }

        // Return the first valid parsed result
        if let Some(hour_offset) = hour_offset {
            let mut tz_offset_second: i64 = hour_offset * 60 * 60;

            if let Some(minute_offset) = minute_offset {
                tz_offset_second += minute_offset * 60;
            }

            if !is_positive {
                tz_offset_second *= -1
            }
            return Ok(tz_offset_second);
        }
    }

    return Err(ParserError::ParserError(format!("It didnt work")));
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum IntervalToken {
    Dash,
    Space,
    Colon,
    Dot,
    Plus,
    Zulu,
    Num(i128),
    Nanos(u32),
    // String representation of a named timezone e.g. 'EST'
    TzName(String),
    // String representation of a unit of time e.g. 'YEAR'
    TimeUnit(String),
}

fn build_parsed_datetime_ym(
    actual: &mut std::iter::Peekable<std::slice::Iter<'_, IntervalToken>>,
    leading_field: &DateTimeField,
    value: &str,
    pdt: &mut ParsedDateTime,
) -> Result<(), ParserError> {
    use IntervalToken::*;

    let mut is_positive = true;

    if let Some(Dash) = actual.peek() {
        // If preceded by '-', assume negative.
        is_positive = false;
        actual.next();
    }

    let expected = potential_interval_tokens_ym(&leading_field)?;

    let mut current_field = leading_field.clone();
    for (i, (atok, etok)) in actual.zip(&expected).enumerate() {
        match (atok, etok) {
            (Dash, Dash) | (Space, Space) | (Colon, Colon) | (Dot, Dot) => {
                /* matching punctuation */
            }
            (Num(val), Num(_)) => {
                let val = *val;
                match current_field {
                    DateTimeField::Year => pdt.year = Some(val as i128),
                    DateTimeField::Month => pdt.month = Some(val as i128),
                    _ => {
                        return parser_err!("Invalid field {} in ym {}", current_field, val);
                    }
                }
                if current_field != DateTimeField::Month {
                    current_field = current_field
                        .into_iter()
                        .next()
                        .expect("Exhausted day iterator");
                }
            }
            // Allow exiting the loop early if encountering a space.
            // This more closely mirrors Postgres' interval parsing.
            (Space, Num(_)) => return Ok(()),
            (provided, expected) => {
                return parser_err!(
                    "Invalid interval part at offset {}: '{}' provided {:?} but expected {:?}",
                    i,
                    value,
                    provided,
                    expected,
                )
            }
        }
    }

    if let Some(Space) = actual.peek() {
        // Consume trailig space after Month, which is permissible.
        actual.next();
    }

    if !is_positive {
        if let Some(y) = pdt.year {
            pdt.year = Some(y * -1);
        }
        if let Some(m) = pdt.month {
            pdt.month = Some(m * -1);
        }
    }

    Ok(())
}

fn build_parsed_datetime_d(
    actual: &mut std::iter::Peekable<std::slice::Iter<'_, IntervalToken>>,
    leading_field: &DateTimeField,
    value: &str,
    pdt: &mut ParsedDateTime,
) -> Result<(), ParserError> {
    use IntervalToken::*;

    if let Some(Dash) = actual.peek() {
        // If preceded by '-', assume negative.
        pdt.is_positive_dur = false;
        actual.next();
    }

    let expected = potential_interval_tokens_d(&leading_field)?;

    let mut current_field = leading_field.clone();
    let mut seconds_seen = 0;
    let mut neg_num: bool;

    for (i, (atok, etok)) in actual.zip(&expected).enumerate() {
        match (atok, etok) {
            (Dash, Dash) | (Space, Space) | (Colon, Colon) | (Dot, Dot) => {
                /* matching punctuation */
            }
            (Num(val), Num(_)) => {
                let val = *val;
                match current_field {
                    DateTimeField::Day => pdt.day = Some(val),
                    DateTimeField::Hour => pdt.hour = Some(val),
                    DateTimeField::Minute => pdt.minute = Some(val),
                    DateTimeField::Second if seconds_seen == 0 => {
                        seconds_seen += 1;
                        pdt.second = Some(val);
                    }
                    DateTimeField::Second => {
                        return parser_err!("Too many numbers to parse as a second at {}", val)
                    }
                    _ => {
                        return parser_err!("Invalid field {} in dhms {}", current_field, val);
                    }
                }
                if current_field != DateTimeField::Second {
                    current_field = current_field
                        .into_iter()
                        .next()
                        .expect("Exhausted day iterator");
                }
            }
            (Nanos(val), Nanos(_)) if seconds_seen == 1 => pdt.nano = Some(*val),
            (provided, expected) => {
                return parser_err!(
                    "Invalid interval part at offset {}: '{}' provided {:?} but expected {:?}",
                    i,
                    value,
                    provided,
                    expected,
                )
            }
        }
    }

    Ok(())
}

fn build_parsed_datetime_hms(
    actual: &mut std::iter::Peekable<std::slice::Iter<'_, IntervalToken>>,
    leading_field: &DateTimeField,
    value: &str,
    pdt: &mut ParsedDateTime,
) -> Result<(), ParserError> {
    use IntervalToken::*;

    if let Some(Dash) = actual.peek() {
        // If preceded by '-', assume negative.
        pdt.is_positive_dur = false;
        actual.next();
    }

    let expected = potential_interval_tokens_dhms(&leading_field)?;

    let mut current_field = leading_field.clone();
    let mut seconds_seen = 0;
    for (i, (atok, etok)) in actual.zip(&expected).enumerate() {
        match (atok, etok) {
            (Dash, Dash) | (Space, Space) | (Colon, Colon) | (Dot, Dot) => {
                /* matching punctuation */
            }
            (Num(val), Num(_)) => {
                let val = *val;
                match current_field {
                    DateTimeField::Day => pdt.day = Some(val),
                    DateTimeField::Hour => pdt.hour = Some(val),
                    DateTimeField::Minute => pdt.minute = Some(val),
                    DateTimeField::Second if seconds_seen == 0 => {
                        seconds_seen += 1;
                        pdt.second = Some(val);
                    }
                    DateTimeField::Second => {
                        return parser_err!("Too many numbers to parse as a second at {}", val)
                    }
                    _ => {
                        return parser_err!("Invalid field {} in dhms {}", current_field, val);
                    }
                }
                if current_field != DateTimeField::Second {
                    current_field = current_field
                        .into_iter()
                        .next()
                        .expect("Exhausted day iterator");
                }
            }
            (Nanos(val), Nanos(_)) if seconds_seen == 1 => pdt.nano = Some(*val),
            (provided, expected) => {
                return parser_err!(
                    "Invalid interval part at offset {}: '{}' provided {:?} but expected {:?}",
                    i,
                    value,
                    provided,
                    expected,
                )
            }
        }
    }

    Ok(())
}

pub(crate) fn build_parsed_datetime_from_shorthand(
    tokens: &[IntervalToken],
    leading_field_ym: &Option<DateTimeField>,
    leading_field_dhms: &Option<DateTimeField>,
    value: &str,
) -> Result<ParsedDateTime, ParserError> {
    let mut actual = tokens.iter().peekable();

    let mut pdt = ParsedDateTime {
        ..Default::default()
    };

    if let Some(leading_field_ym) = leading_field_ym {
        build_parsed_datetime_ym(&mut actual, &leading_field_ym, value, &mut pdt)?;
    }

    if let Some(leading_field_dhms) = leading_field_dhms {
        build_parsed_datetime_dhms(&mut actual, &leading_field_dhms, value, &mut pdt)?;
    }

    Ok(pdt)
}

pub(crate) fn build_parsed_datetime_from_datetime_str(
    value: &str,
) -> Result<ParsedDateTime, ParserError> {
    let mut pdt = ParsedDateTime::default();
    let lc = value.to_lowercase();
    let split = lc.split(' ').collect::<Vec<&str>>();
    if split.len() % 2 == 0 {
        for i in 0..split.len() / 2 {
            let mut v = match split[i * 2].parse::<i128>() {
                Ok(v) => v,
                Err(_) => return parser_err!("Invalid INTERVAL: {:#?}", value),
            };
            match split[i * 2 + 1] {
                "year" | "years" => {
                    pdt.year = Some(v);
                }
                "month" | "months" => {
                    pdt.month = Some(v);
                }
                "day" | "days" => {
                    if v < 0 {
                        pdt.is_positive_dur = false;
                        v = v.abs()
                    }
                    pdt.day = Some(v as u64);
                }
                "hour" | "hours" => {
                    if v < 0 {
                        pdt.is_positive_dur = false;
                        v = v.abs()
                    }
                    pdt.hour = Some(v as u64);
                }
                "minute" | "minutes" => {
                    if v < 0 {
                        pdt.is_positive_dur = false;
                        v = v.abs()
                    }
                    pdt.minute = Some(v as u64);
                }
                "second" | "seconds" => {
                    if v < 0 {
                        pdt.is_positive_dur = false;
                        v = v.abs()
                    }
                    pdt.second = Some(v as u64);
                }
                _ => {
                    return parser_err!("Invalid INTERVAL: {:#?}", value);
                }
            }
        }
        Ok(pdt)
    } else {
        return parser_err!("Invalid INTERVAL: {:#?}", value);
    }
}

/// Takes a 'date timezone' 'date time timezone' string and splits
/// it into 'date {time}' and 'timezone' components
pub(crate) fn split_timestamp_string(value: &str) -> (&str, &str) {
    // First we need to see if the string contains " +" or " -" because
    // timestamps can come in a format YYYY-MM-DD {+|-}<tz> (where the
    // timezone string can have colons)
    let cut = value.find(" +").or_else(|| value.find(" -"));

    if let Some(cut) = cut {
        let (first, second) = value.split_at(cut);
        return (first.trim(), second.trim());
    }

    // If we have a hh:mm:dd component, we need to go past that to see if we can find a tz
    let colon = value.find(':');

    if let Some(colon) = colon {
        let substring = value.get(colon..);
        if let Some(substring) = substring {
            let tz = substring
                .find(|c: char| (c == '-') || (c == '+') || (c == ' ') || c.is_ascii_alphabetic());

            if let Some(tz) = tz {
                let (first, second) = value.split_at(colon + tz);
                return (first.trim(), second.trim());
            }
        }

        return (value.trim(), "");
    } else {
        // We don't have a time, so the only formats available are
        // YYY-mm-dd<tz> or YYYY-MM-dd <tz>
        // Numeric offset timezones need to be separated from the ymd by a space
        let cut = value.find(|c: char| (c == ' ') || c.is_ascii_alphabetic());

        if let Some(cut) = cut {
            let (first, second) = value.split_at(cut);
            return (first.trim(), second.trim());
        }

        return (value.trim(), "");
    }
}

pub(crate) fn parse_timezone_offset_second(value: &str) -> Result<i64, ParserError> {
    let toks = tokenize_timezone(value)?;
    Ok(build_timezone_offset_second(&toks, value)?)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::parser::*;

    #[test]
    fn test_potential_interval_tokens() {
        use DateTimeField::*;
        use IntervalToken::*;
        assert_eq!(
            potential_interval_tokens_ym(&Year).unwrap(),
            vec![Num(0), Dash, Num(0)]
        );

        assert_eq!(
            potential_interval_tokens_dhms(&Day).unwrap(),
            vec![
                Num(0),
                Space,
                Num(0),
                Colon,
                Num(0),
                Colon,
                Num(0),
                Dot,
                Nanos(0),
            ]
        );
    }

    #[test]
    fn test_split_timestamp_string() {
        let test_cases = [
            (
                "1969-06-01 10:10:10.410 UTC",
                "1969-06-01 10:10:10.410",
                "UTC",
            ),
            (
                "1969-06-01 10:10:10.410+4:00",
                "1969-06-01 10:10:10.410",
                "+4:00",
            ),
            (
                "1969-06-01 10:10:10.410-4:00",
                "1969-06-01 10:10:10.410",
                "-4:00",
            ),
            ("1969-06-01 10:10:10.410", "1969-06-01 10:10:10.410", ""),
            ("1969-06-01 10:10:10.410+4", "1969-06-01 10:10:10.410", "+4"),
            ("1969-06-01 10:10:10.410-4", "1969-06-01 10:10:10.410", "-4"),
            ("1969-06-01 10:10:10+4:00", "1969-06-01 10:10:10", "+4:00"),
            ("1969-06-01 10:10:10-4:00", "1969-06-01 10:10:10", "-4:00"),
            ("1969-06-01 10:10:10 UTC", "1969-06-01 10:10:10", "UTC"),
            ("1969-06-01 10:10:10", "1969-06-01 10:10:10", ""),
            ("1969-06-01 10:10+4:00", "1969-06-01 10:10", "+4:00"),
            ("1969-06-01 10:10-4:00", "1969-06-01 10:10", "-4:00"),
            ("1969-06-01 10:10 UTC", "1969-06-01 10:10", "UTC"),
            ("1969-06-01 10:10", "1969-06-01 10:10", ""),
            ("1969-06-01 UTC", "1969-06-01", "UTC"),
            ("1969-06-01 +4:00", "1969-06-01", "+4:00"),
            ("1969-06-01 -4:00", "1969-06-01", "-4:00"),
            ("1969-06-01 +4", "1969-06-01", "+4"),
            ("1969-06-01 -4", "1969-06-01", "-4"),
            ("1969-06-01", "1969-06-01", ""),
            ("1969-06-01 10:10:10.410Z", "1969-06-01 10:10:10.410", "Z"),
            ("1969-06-01 10:10:10.410z", "1969-06-01 10:10:10.410", "z"),
            ("1969-06-01Z", "1969-06-01", "Z"),
            ("1969-06-01z", "1969-06-01", "z"),
            ("1969-06-01 10:10:10.410   ", "1969-06-01 10:10:10.410", ""),
            (
                "1969-06-01     10:10:10.410   ",
                "1969-06-01     10:10:10.410",
                "",
            ),
            ("   1969-06-01 10:10:10.412", "1969-06-01 10:10:10.412", ""),
            (
                "   1969-06-01 10:10:10.413   ",
                "1969-06-01 10:10:10.413",
                "",
            ),
            (
                "1969-06-01 10:10:10.410 +4:00",
                "1969-06-01 10:10:10.410",
                "+4:00",
            ),
            (
                "1969-06-01 10:10:10.410+4 :00",
                "1969-06-01 10:10:10.410",
                "+4 :00",
            ),
            (
                "1969-06-01 10:10:10.410      +4:00",
                "1969-06-01 10:10:10.410",
                "+4:00",
            ),
            (
                "1969-06-01 10:10:10.410+4:00     ",
                "1969-06-01 10:10:10.410",
                "+4:00",
            ),
            (
                "1969-06-01 10:10:10.410  Z  ",
                "1969-06-01 10:10:10.410",
                "Z",
            ),
            ("1969-06-01    +4  ", "1969-06-01", "+4"),
            ("1969-06-01   Z   ", "1969-06-01", "Z"),
        ];

        for test in test_cases.iter() {
            let (ts, tz) = split_timestamp_string(test.0);

            assert_eq!(ts, test.1);
            assert_eq!(tz, test.2);
        }
    }

    #[test]
    fn test_parse_timezone_offset_second() {
        let test_cases = [
            ("+0:00", 0),
            ("-0:00", 0),
            ("+0:000000", 0),
            ("+000000:00", 0),
            ("+000000:000000", 0),
            ("+0", 0),
            ("+00", 0),
            ("+000", 0),
            ("+0000", 0),
            ("+00000000", 0),
            ("+0000001:000000", 3600),
            ("+0000000:000001", 60),
            ("+0000001:000001", 3660),
            ("+4:00", 14400),
            ("-4:00", -14400),
            ("+2:30", 9000),
            ("-5:15", -18900),
            ("+0:20", 1200),
            ("-0:20", -1200),
            ("+5", 18000),
            ("-5", -18000),
            ("+05", 18000),
            ("-05", -18000),
            ("+500", 18000),
            ("-500", -18000),
            ("+530", 19800),
            ("-530", -19800),
            ("+050", 3000),
            ("-050", -3000),
            ("+15", 54000),
            ("-15", -54000),
            ("+1515", 54900),
            ("+015", 900),
            ("-015", -900),
            ("+0015", 900),
            ("-0015", -900),
            ("+00015", 900),
            ("-00015", -900),
            ("+005", 300),
            ("-005", -300),
            ("+0000005", 300),
            ("+00000100", 3600),
            ("Z", 0),
            ("z", 0),
        ];

        for test in test_cases.iter() {
            match parse_timezone_offset_second(test.0) {
                Ok(tz_offset) => {
                    let expected: i64 = test.1 as i64;

                    println!("{} {}", expected, tz_offset);
                    assert_eq!(tz_offset, expected);
                }
                Err(e) => panic!(
                    "Test failed when expected to pass test case: {} error: {}",
                    test.0, e
                ),
            }
        }

        let failure_test_cases = [
            "+25:00", "+120:00", "+0:61", "+0:500", " 12:30", "+-12:30", "+2525", "+2561",
            "+255900", "+25", "+5::30", "+5:30:", "+5:30:16", "+5:", "++5:00", "--5:00", "UTC",
            " UTC", "a", "zzz", "ZZZ", "ZZ Top", " +", " -", " ", "1", "12", "1234",
        ];

        for test in failure_test_cases.iter() {
            match parse_timezone_offset_second(test) {
                Ok(t) => panic!("Test passed when expected to fail test case: {} parsed tz offset (seconds): {}", test, t),
                Err(e) => println!("{}", e),
            }
        }
    }
}
