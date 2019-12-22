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

    let mut last_field_is_frac = false;
    for (i, chr) in value.chars().enumerate() {
        if !num_buf.is_empty() && !char_buf.is_empty() {
            return Err(ParserError::TokenizerError(format!(
                "Invalid INTERVAL '{}': could not tokenize",
                value
            )));
        }
        match chr {
            '-' => {
                if !num_buf.is_empty() {
                    toks.push(parse_num(&num_buf, i)?);
                    num_buf.clear();
                } else if !char_buf.is_empty() {
                    toks.push(IntervalToken::TimeUnit(char_buf.clone()));
                    char_buf.clear();
                }
                toks.push(IntervalToken::Dash);
            }
            ' ' => {
                if !num_buf.is_empty() {
                    toks.push(parse_num(&num_buf, i)?);
                    num_buf.clear();
                } else if !char_buf.is_empty() {
                    toks.push(IntervalToken::TimeUnit(char_buf.clone()));
                    char_buf.clear();
                }
                toks.push(IntervalToken::Space);
            }
            ':' => {
                if !num_buf.is_empty() {
                    toks.push(parse_num(&num_buf, i)?);
                    num_buf.clear();
                } else if !char_buf.is_empty() {
                    toks.push(IntervalToken::TimeUnit(char_buf.clone()));
                    char_buf.clear();
                }
                toks.push(IntervalToken::Colon);
            }
            '.' => {
                if !num_buf.is_empty() {
                    toks.push(parse_num(&num_buf, i)?);
                    num_buf.clear();
                } else if !char_buf.is_empty() {
                    toks.push(IntervalToken::TimeUnit(char_buf.clone()));
                    char_buf.clear();
                }
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
            let raw: i64 = num_buf.parse().map_err(|e| {
                ParserError::ParserError(format!(
                    "couldn't parse fraction of second {}: {}",
                    num_buf, e
                ))
            })?;
            // this is guaranteed to be ascii, so len is fine
            let chars = num_buf.len() as u32;
            let multiplicand = 1_000_000_000 / 10_i64.pow(chars);

            toks.push(IntervalToken::Nanos(raw * multiplicand));
        }
    } else if !char_buf.is_empty() {
        toks.push(IntervalToken::TimeUnit(char_buf.clone()));
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

// determine_interval_parse_heads determines the "head" value of `interval_str`, i.e. where we should
// begin parsing using potential_interval_tokens_ym and potential_interval_tokens_dhms. It also tracks
// cases where `interval_str` is ambiguous and can be "moved" by the range declaration, e.g.
// `INTERVAL '1' MINUTE`.
//
// Why do we parse in this way?
//
// Interval shorthand uses the following format:
// Y-M D H:M:S.NS
//
// However, Postgres does not treat interval strings as expressing any continuous range, e.g.
// `INTERVAL '1-2 3` represents `1 year 2 mons 00:00:03`. To handle this, we treat interval strings
// as having three components:
// {Y-M }{D }{H:M:S.NS}
//
// Throughout interval parsing, these elements are referred to with a suffix of _ym for the former
// and _dhms for the latter.
pub(crate) fn determine_interval_parse_heads(
    interval_str: &str,
    ambiguous_resolver: Option<DateTimeField>,
) -> Result<IntervalDatetimeParseKey, ParserError> {
    let lead_hms_determ = |s: &str| -> Result<Option<DateTimeField>, ParserError> {
        let mut z = s.chars().peekable();

        if trim_leading_colons_sign_numbers(&mut z).is_err() {
            return parser_err!(format!(
                "invalid input syntax for type interval: {}",
                interval_str
            ));
        }

        match z.next() {
            // {?:...}
            Some(':') => {
                while let Some('0'..='9') = z.peek() {
                    z.next();
                }
                match z.peek() {
                    // Implies {H:M...}
                    Some(':') | None => Ok(Some(DateTimeField::Hour)),
                    // Implies {M:S.NS}
                    Some('.') => Ok(Some(DateTimeField::Minute)),
                    _ => {
                        return parser_err!(format!(
                            "invalid input syntax for type interval: {}",
                            interval_str
                        ))
                    }
                }
            }
            // Implies {S(.NS)?}
            Some('.') | None => Ok(Some(DateTimeField::Second)),
            _ => {
                return parser_err!(format!(
                    "invalid input syntax for type interval: {}",
                    interval_str
                ))
            }
        }
    };

    let v = interval_str
        .trim()
        .split_whitespace()
        .collect::<Vec<&str>>();

    match v.len() {
        // Implies {Y... }{D }{...}
        3 => Ok(IntervalDatetimeParseKey {
            ym: Some(DateTimeField::Year),
            d: Some(DateTimeField::Day),
            hms: lead_hms_determ(v[2])?,
        }),
        // Implies {?}{?}{...}
        2 => {
            let lead_ym;
            let lead_d;
            let mut z = v[0].chars().peekable();

            if trim_leading_colons_sign_numbers(&mut z).is_err() {
                return parser_err!(format!(
                    "invalid input syntax for type interval: {}",
                    interval_str
                ));
            }

            match z.next() {
                // Implies {Y-... }{}{...}
                Some('-') => {
                    lead_ym = Some(DateTimeField::Year);
                    lead_d = None;
                }
                // Implies {}{D }{...}
                None => {
                    lead_ym = None;
                    lead_d = Some(DateTimeField::Day);
                }
                _ => {
                    return parser_err!(format!(
                        "invalid input syntax for type interval: {}",
                        interval_str
                    ));
                }
            }

            Ok(IntervalDatetimeParseKey {
                ym: lead_ym,
                d: lead_d,
                hms: lead_hms_determ(v[1])?,
            })
        }
        1 => {
            let mut z = interval_str.chars().peekable();

            if trim_leading_colons_sign_numbers(&mut z).is_err() {
                return parser_err!(format!(
                    "invalid input syntax for type interval: {}",
                    interval_str
                ));
            }

            match z.next() {
                // Implies {Y-...}{}{}
                Some('-') => Ok(IntervalDatetimeParseKey {
                    ym: Some(DateTimeField::Year),
                    ..Default::default()
                }),
                // Implies {}{}{...}
                Some(_) => Ok(IntervalDatetimeParseKey {
                    hms: lead_hms_determ(interval_str)?,
                    ..Default::default()
                }),
                // Implies {?}{?}{?}, ambiguous case.
                None => {
                    let mut key = IntervalDatetimeParseKey {
                        ..Default::default()
                    };
                    match ambiguous_resolver {
                        Some(DateTimeField::Year) | Some(DateTimeField::Month) => {
                            key.ym = ambiguous_resolver
                        }
                        Some(DateTimeField::Day) => key.d = ambiguous_resolver,
                        Some(_) => key.hms = ambiguous_resolver,
                        None => {
                            return parser_err!(
                                "Cannot parse INTERVAL '{}' without ambiguous resolver",
                                interval_str
                            )
                        }
                    }
                    Ok(key)
                }
            }
        }
        _ => parser_err!(format!(
            "invalid input syntax for type interval: {}",
            interval_str
        )),
    }
}

// Trim values equivalent to the regex (:*-?[0-9]*).
fn trim_leading_colons_sign_numbers(
    z: &mut std::iter::Peekable<std::str::Chars<'_>>,
) -> Result<(), ParserError> {
    // PostgreSQL inexplicably trims all leading colons from interval sections.
    while let Some(':') = z.peek() {
        z.next();
    }

    // Consume leading negative sign from any field.
    if let Some('-') = z.peek() {
        z.next();
    }

    // Consume all following numbers.
    while let Some('0'..='9') = z.peek() {
        z.next();
    }

    Ok(())
}

/// Get the tokens that you *might* end up parsing starting with a most significant unit
///
/// For example, parsing `INTERVAL '9-5 4:3' MONTH` is *illegal*, but you
/// should interpret that as `9 months 5 days 4 hours 3 minutes`. This function
/// doesn't take any perspective on what things should be, it just teslls you
/// what the user might have meant.
fn potential_interval_tokens(from: DateTimeField) -> Vec<IntervalToken> {
    use DateTimeField::*;
    use IntervalToken::*;

    println!("Getting tokens starting at {}", from);

    let all_toks = [
        Num(0), // year
        Dash,
        Num(0), // month
        Space,
        Num(0), // day
        Space,
        Num(0), // hour
        Colon,
        Num(0), // minute
        Colon,
        Num(0), // second
        Dot,
        Nanos(0), // Nanos
    ];
    let (start, end) = match from {
        Year => (0, 4),
        Month => (2, 4),
        Day => (4, 6),
        Hour => (6, 13),
        Minute => (8, 13),
        Second => (10, 13),
    };
    all_toks[start..end].to_vec()
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
    Nanos(i64),
    // String representation of a named timezone e.g. 'EST'
    TzName(String),
    // String representation of a unit of time e.g. 'YEAR'
    TimeUnit(String),
}

fn build_parsed_datetime_component(
    actual: &mut std::iter::Peekable<std::slice::Iter<'_, IntervalToken>>,
    leading_field: DateTimeField,
    value: &str,
    pdt: &mut ParsedDateTime,
) -> Result<(), ParserError> {
    use DateTimeField::*;
    use IntervalToken::*;

    let mut is_positive = true;

    // PostgreSQL inexplicably trims all leading colons from interval parts.
    while let Some(Colon) = actual.peek() {
        actual.next();
    }

    if let Some(Dash) = actual.peek() {
        // If preceded by '-', assume negative.
        is_positive = false;
        actual.next();
    }

    let expected = potential_interval_tokens(leading_field);
    let mut expected = expected.iter().peekable();
    let mut seconds_seen = 0;

    println!("Tokens left {}", actual.len());

    let mut current_field = leading_field;

    let mut i = 0u8;

    while let Some(etok) = expected.peek() {
        if let Some(atok) = actual.peek() {
            match (atok, etok) {
                (Dash, Dash) | (Colon, Colon) => {
                    /* matching punctuation */
                    expected.next();
                    actual.next();

                    println!("Matching punctuation");
                }
                (Dot, Dot) => {
                    expected.next();
                    actual.next();
                    seconds_seen = 1;
                }
                (Num(val), Num(_)) => {
                    println!("Matching numbers");
                    expected.next();
                    actual.next();
                    let val = *val;
                    match current_field {
                        DateTimeField::Year => pdt.year = Some(val as i128),
                        DateTimeField::Month => pdt.month = Some(val as i128),
                        DateTimeField::Day => pdt.day = Some(val),
                        DateTimeField::Hour => pdt.hour = Some(val),
                        DateTimeField::Minute => pdt.minute = Some(val),
                        DateTimeField::Second if seconds_seen == 0 => {
                            println!("Seeing a second...");
                            seconds_seen += 1;
                            pdt.second = Some(val);
                        }
                        _ => {
                            return parser_err!(
                                "Invalid interval field {} in {}",
                                current_field,
                                val
                            );
                        }
                    }
                    // Advance to next field.
                    if current_field != DateTimeField::Second {
                        current_field = current_field
                            .into_iter()
                            .next()
                            .expect("Exhausted iterator");
                    }
                }
                (Nanos(val), Nanos(_)) if seconds_seen == 1 => {
                    expected.next();
                    actual.next();
                    pdt.nano = Some(*val)
                }
                // Break out of this component if you encounter a space.
                (Space, _) => {
                    println!("Exit early");
                    actual.next();
                    break;
                }
                // Allow skipping expexted numbers.
                (_, Num(_)) => {
                    println!("Skipping expected number");
                    expected.next();

                    // Advance to next field.
                    if current_field != DateTimeField::Second {
                        current_field = current_field
                            .into_iter()
                            .next()
                            .expect("Exhausted iterator");
                    }
                }
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
        } else {
            break;
        };

        i += 1;
    }

    if !is_positive {
        println!("is negative");
        match leading_field {
            Year | Month => {
                if let Some(y) = pdt.year {
                    pdt.year = Some(-y);
                }
                if let Some(m) = pdt.month {
                    pdt.month = Some(-m);
                }
            }
            Day => {
                println!("is negative day");
                if let Some(d) = pdt.day {
                    println!("negating day");
                    pdt.day = Some(-d);
                }
            }
            _ => {
                if let Some(h) = pdt.hour {
                    pdt.hour = Some(-h);
                }
                if let Some(m) = pdt.minute {
                    pdt.minute = Some(-m);
                }
                if let Some(s) = pdt.second {
                    pdt.second = Some(-s);
                }
                if let Some(n) = pdt.nano {
                    pdt.nano = Some(-n);
                }
            }
        }
    }

    Ok(())
}

pub struct IntervalDatetimeParseKey {
    pub ym: Option<DateTimeField>,
    pub d: Option<DateTimeField>,
    pub hms: Option<DateTimeField>,
}

impl Default for IntervalDatetimeParseKey {
    fn default() -> IntervalDatetimeParseKey {
        IntervalDatetimeParseKey {
            ym: None,
            d: None,
            hms: None,
        }
    }
}

pub(crate) fn build_parsed_datetime_shorthand(
    tokens: &[IntervalToken],
    key: IntervalDatetimeParseKey,
    value: &str,
) -> Result<ParsedDateTime, ParserError> {
    let mut actual = tokens.iter().peekable();

    let mut pdt = ParsedDateTime {
        ..Default::default()
    };

    if let Some(leading_field_ym) = key.ym {
        build_parsed_datetime_component(&mut actual, leading_field_ym, value, &mut pdt)?;
    }

    if let Some(leading_field_d) = key.d {
        build_parsed_datetime_component(&mut actual, leading_field_d, value, &mut pdt)?;
    }

    if let Some(leading_field_dhms) = key.hms {
        build_parsed_datetime_component(&mut actual, leading_field_dhms, value, &mut pdt)?;
    }

    Ok(pdt)
}

use std::collections::VecDeque;

pub(crate) fn build_parsed_datetime_from_datetime_str(
    tokens: &[IntervalToken],
    value: &str,
) -> Result<ParsedDateTime, ParserError> {
    use DateTimeField::*;
    use IntervalToken::*;

    let mut expected = VecDeque::with_capacity(7);
    expected.push_back(Dash);
    expected.push_back(Num(0));
    expected.push_back(Dot);
    expected.push_back(Nanos(0));
    expected.push_back(Space);
    expected.push_back(TimeUnit(String::default()));
    expected.push_back(Space);

    let mut expected_i = 0;

    let mut actual = tokens.iter().peekable();

    let mut is_negative = 1;
    let mut seen_int = false;
    let mut num_buf = 0i128;
    let mut nano_buf = 0i64;
    let mut pdt = ParsedDateTime::default();

    println!(
        "build_parsed_datetime_from_datetime_str\nactual.len() {}",
        actual.len()
    );

    while let Some(atok) = actual.peek() {
        if let Some(etok) = expected.get(expected_i) {
            println!("expected_i {}", expected_i);
            expected_i += 1;
            expected_i %= expected.len();
            match (atok, etok) {
                (Dash, Dash) => {
                    println!("Matching dash");
                    actual.next();
                    is_negative = -1;
                }
                (_, Dash) => {}
                (Dot, Dot) if seen_int => {
                    println!("Matching dot");
                    actual.next();
                }
                (Num(n), Num(_)) if !seen_int => {
                    println!("Got int {}", n);
                    actual.next();

                    seen_int = true;

                    match actual.peek() {
                        Some(Space) => expected_i += 2,
                        Some(Dot) => {}
                        _ => {
                            return parser_err!("Invalid INTERVAL: {:#?}", value);
                        }
                    }
                    num_buf = *n;
                }
                (Num(n), Nanos(_)) if seen_int => {
                    println!("Got nano {}", n);
                    actual.next();
                    let num_digit = (*n as f64).log10();
                    let multiplicand = 100_000_000 / 10_i64.pow(num_digit as u32);

                    nano_buf = (*n as i64) * multiplicand;
                }
                // Allow skipping past the first int
                (_, Num(_)) if !seen_int => {
                    seen_int = true;
                }
                (Space, Space) if seen_int => {
                    println!("Space seen_int");
                    actual.next();
                    seen_int = false;
                }
                (Space, Space) if !seen_int => {
                    println!("Space !seen_int; resetting tracking");
                    actual.next();
                    is_negative = 1;
                    num_buf = 0;
                    nano_buf = 0;
                }
                (TimeUnit(f), TimeUnit(_)) => {
                    println!("I think this TimeUnit is f {}", f);
                    actual.next();

                    match f.as_ref() {
                        "year" | "years" if pdt.year.is_none() => {
                            pdt.year = Some(num_buf * is_negative);
                        }
                        "month" | "months" if pdt.month.is_none() => {
                            pdt.month = Some(num_buf * is_negative);
                        }
                        "day" | "days" if pdt.day.is_none() => {
                            pdt.day = Some(num_buf * is_negative);
                        }
                        "hour" | "hours" if pdt.hour.is_none() => {
                            pdt.hour = Some(num_buf * is_negative);
                        }
                        "minute" | "minutes" if pdt.minute.is_none() => {
                            pdt.minute = Some(num_buf * is_negative);
                        }
                        "second" | "seconds" if pdt.second.is_none() => {
                            if num_buf > 0 {
                                pdt.second = Some(num_buf * is_negative);
                            }
                            if nano_buf > 0 {
                                pdt.nano = Some(nano_buf * is_negative as i64);
                            }
                        }
                        _ => {
                            return parser_err!("Invalid INTERVAL: {:#?}", value);
                        }
                    }
                }
                (_, _) => return parser_err!("Invalid INTERVAL: {:#?}", value),
            }
        }
    }

    Ok(pdt)
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
            potential_interval_tokens(Year).unwrap(),
            vec![Num(0), Dash, Num(0)]
        );

        assert_eq!(
            potential_interval_tokens(Day).unwrap(),
            vec![Num(0), Space,]
        );

        assert_eq!(
            potential_interval_tokens(Hour).unwrap(),
            vec![Num(0), Colon, Num(0), Colon, Num(0), Dot, Nanos(0)]
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
