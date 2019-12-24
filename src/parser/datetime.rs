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
                    toks.push(IntervalToken::TimeUnit(char_buf.to_uppercase().clone()));
                    char_buf.clear();
                }
                toks.push(IntervalToken::Dash);
            }
            ' ' => {
                if !num_buf.is_empty() {
                    toks.push(parse_num(&num_buf, i)?);
                    num_buf.clear();
                } else if !char_buf.is_empty() {
                    toks.push(IntervalToken::TimeUnit(char_buf.to_uppercase().clone()));
                    char_buf.clear();
                }
                toks.push(IntervalToken::Space);
            }
            ':' => {
                if !num_buf.is_empty() {
                    toks.push(parse_num(&num_buf, i)?);
                    num_buf.clear();
                } else if !char_buf.is_empty() {
                    toks.push(IntervalToken::TimeUnit(char_buf.to_uppercase().clone()));
                    char_buf.clear();
                }
                println!("Token colon");
                toks.push(IntervalToken::Colon);
            }
            '.' => {
                if !num_buf.is_empty() {
                    toks.push(parse_num(&num_buf, i)?);
                    num_buf.clear();
                } else if !char_buf.is_empty() {
                    toks.push(IntervalToken::TimeUnit(char_buf.to_uppercase().clone()));
                    char_buf.clear();
                }
                println!("Token dot");
                toks.push(IntervalToken::Dot);
                last_field_is_frac = true;
            }
            chr if chr.is_digit(10) => {
                if !char_buf.is_empty() {
                    toks.push(IntervalToken::TimeUnit(char_buf.to_uppercase().clone()));
                    char_buf.clear();
                }
                num_buf.push(chr)
            }
            chr if chr.is_ascii_alphabetic() => {
                if !num_buf.is_empty() {
                    toks.push(parse_num(&num_buf, i)?);
                    num_buf.clear();
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
        toks.push(IntervalToken::TimeUnit(char_buf.to_uppercase().clone()));
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

// IntervalShorthandParseKey expresses the greatest value present in each "section" of an interval string
// expressed in shorthand, e.g. `INTERVAL '1-2 3 4:5:6.7', which corresponds to the following format:
// {Y-M }{D }{H:M:S.NS}
//
// This structuring is necessary because Postgres does not necessarily treat interval strings as expressing
// continuous range, e.g. `INTERVAL '1-2 3` represents `1 year 2 mons 00:00:03`.
pub struct IntervalShorthandParseKey {
    pub ym: Option<DateTimeField>,
    pub d: Option<DateTimeField>,
    pub hms: Option<DateTimeField>,
}

impl Default for IntervalShorthandParseKey {
    fn default() -> IntervalShorthandParseKey {
        IntervalShorthandParseKey {
            ym: None,
            d: None,
            hms: None,
        }
    }
}

// determine_interval_parse_heads determines the "head" value of `interval_str`, i.e. the greatest
// value expressed in each "section" of the interval shorthand string.
pub(crate) fn determine_interval_parse_heads(
    interval_str: &str,
    ambiguous_resolver: Option<DateTimeField>,
) -> Result<IntervalShorthandParseKey, ParserError> {
    // Determining the {H:M:S.NS} portion of the string is always the same, so
    // this subroutine is useful in all conditions where you need to determine it.
    let lead_hms_determ = |s: &str| -> Result<Option<DateTimeField>, ParserError> {
        let mut z = s.chars().peekable();

        if trim_leading_colons_sign_numbers(&mut z).is_err() {
            return parser_err!(format!(
                "invalid input syntax for type interval: {}",
                interval_str
            ));
        }

        match z.next() {
            // Implies {?:...}
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
        3 => Ok(IntervalShorthandParseKey {
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

            Ok(IntervalShorthandParseKey {
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
                Some('-') => Ok(IntervalShorthandParseKey {
                    ym: Some(DateTimeField::Year),
                    ..Default::default()
                }),
                // Implies {}{}{...}
                Some(_) => Ok(IntervalShorthandParseKey {
                    hms: lead_hms_determ(interval_str)?,
                    ..Default::default()
                }),
                // Implies {?}{?}{?}, ambiguous case.
                None => {
                    let mut key = IntervalShorthandParseKey {
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

// pub(crate) fn build_parsed_datetime_shorthand(
//     tokens: &[IntervalToken],
//     key: IntervalShorthandParseKey,
//     value: &str,
// ) -> Result<ParsedDateTime, ParserError> {
//     let mut actual = tokens.iter().peekable();

//     let mut pdt = ParsedDateTime {
//         ..Default::default()
//     };

//     if let Some(ym_head) = key.ym {
//         build_parsed_datetime_component(&mut actual, ym_head, value, &mut pdt)?;
//     }

//     if let Some(d_head) = key.d {
//         build_parsed_datetime_component(&mut actual, d_head, value, &mut pdt)?;
//     }

//     if let Some(hms_head) = key.hms {
//         build_parsed_datetime_component(&mut actual, hms_head, value, &mut pdt)?;
//     }

//     Ok(pdt)
// }

fn determine_head(
    vt: &Vec<IntervalToken>,
    interval_str: &str,
) -> Result<Option<ParseBehaviour>, ParserError> {
    use DateTimeField::*;
    use IntervalToken::*;
    use ParseBehaviour::*;
    let lead_hms_determ = |v: &Vec<IntervalToken>| -> Result<Option<ParseBehaviour>, ParserError> {
        let mut z = v.iter().peekable();
        println!("lead_hms_determ");

        // PostgreSQL inexplicably trims all leading colons from interval sections.
        while let Some(Colon) = z.peek() {
            z.next();
        }

        // Consume leading negative sign from any field.
        if let Some(Dash) = z.peek() {
            z.next();
        }

        if let Some(Num(_)) = z.peek() {
            println!("lead_hms_determ clearing number 1");
            z.next();
        }

        match z.next() {
            // Implies {?:...}
            Some(Colon) => {
                if let Some(Num(_)) = z.peek() {
                    println!("lead_hms_determ clearing number 2");
                    z.next();
                }
                match z.peek() {
                    // Implies {H:M:...}
                    Some(Colon) | None => Ok(Some(Shorthand(Hour))),
                    // Implies {M:S.NS}
                    Some(Dot) => Ok(Some(Shorthand(Minute))),
                    _ => {
                        return parser_err!(format!(
                            "invalid input syntax for type interval: {}",
                            interval_str
                        ))
                    }
                }
            }
            // Ambiguous
            // Some(Dot) | None => Ok(None),
            _ => {
                return parser_err!(format!(
                    "invalid input syntax for type interval: {}",
                    interval_str
                ))
            }
        }
    };

    let mut z = vt.iter().peekable();

    // PostgreSQL inexplicably trims all leading colons from interval sections.
    while let Some(Colon) = z.peek() {
        z.next();
    }

    // Consume leading negative sign from any field.
    if let Some(Dash) = z.peek() {
        z.next();
    }

    match z.next() {
        Some(Num(_)) => {
            match z.next() {
                // Implies {Y-...}{}{}
                Some(Dash) => Ok(Some(Shorthand(Year))),
                // Implies {?}{?}{?}, ambiguous case.
                Some(Dot) | None => Ok(None),
                // Implies {}{}{...}
                Some(_) => Ok(lead_hms_determ(vt)?),
            }
        }
        Some(TimeUnit(f)) => match f.as_ref() {
            "YEAR" | "YEARS" => Ok(Some(DateTimeStr(Year))),
            "MONTH" | "MONTHS" => Ok(Some(DateTimeStr(Month))),
            "DAY" | "DAYS" => Ok(Some(DateTimeStr(Day))),
            "HOUR" | "HOURS" => Ok(Some(DateTimeStr(Hour))),
            "MINUTE" | "MINUTES" => Ok(Some(DateTimeStr(Minute))),
            "SECOND" | "SECONDS" => Ok(Some(DateTimeStr(Second))),
            _ => parser_err!("invalid input syntax for type interval: {}", interval_str),
        },
        _ => Ok(lead_hms_determ(vt)?),
    }
}

enum ParseBehaviour {
    Shorthand(DateTimeField),
    DateTimeStr(DateTimeField),
}

struct StrucutedTokens {
    pub head: ParseBehaviour,
    pub tokens: std::vec::Vec<IntervalToken>,
}

pub(crate) fn build_parsed_datetime_from_vvt(
    vvt: &std::vec::Vec<std::vec::Vec<IntervalToken>>,
    value: &str,
    ambiguous_resolver: Option<DateTimeField>,
) -> Result<ParsedDateTime, ParserError> {
    use DateTimeField::*;
    use IntervalToken::*;
    let mut pvvt = vvt.iter().peekable();
    let mut ambiguous_resolver = ambiguous_resolver;

    let mut pdt = ParsedDateTime::default();

    if pdt.day.is_some() {
        println!("DAY IS SOME!!!!");
    }

    let mut stv = Vec::new();

    while let Some(vt) = pvvt.next() {
        let mut vt = vt.clone();
        let mut head = determine_head(&vt, value)?;
        if head.is_none() {
            if let Some(nvt) = pvvt.peek() {
                let next_head = determine_head(&nvt, value)?;
                head = match next_head {
                    Some(ParseBehaviour::Shorthand(f)) => match f {
                        Year | Month | Day => None,
                        _ => Some(ParseBehaviour::Shorthand(Day)),
                    },
                    Some(ParseBehaviour::DateTimeStr(_)) => {
                        pvvt.next();
                        next_head
                    }
                    None => None,
                }
            }
            if head.is_none() && ambiguous_resolver.is_some() {
                head = Some(ParseBehaviour::Shorthand(ambiguous_resolver.unwrap()));
                ambiguous_resolver = None;
            } else if head.is_none() {
                return parser_err!("Cannot determine type of head");
            }
        }
        stv.push(StrucutedTokens {
            head: head.unwrap(),
            tokens: vt.clone(),
        });
    }

    if pdt.day.is_some() {
        println!("DAY IS SOME!!!!");
    }

    for st in stv {
        match st.head {
            ParseBehaviour::Shorthand(f) => {
                build_parsed_datetime_component(&st.tokens, f, value, &mut pdt)?
            }
            ParseBehaviour::DateTimeStr(f) => {
                build_parsed_datetime_from_datetime_str(&st.tokens, f, value, &mut pdt)?
            }
        }
    }

    if pdt.day.is_some() {
        println!("DAY IS SOME!!!!");
    }

    if let (None, None, None, None, None, None, None) = (
        pdt.year, pdt.month, pdt.day, pdt.hour, pdt.minute, pdt.second, pdt.nano,
    ) {
        return parser_err!("no valid fields in pdt");
    }
    Ok(pdt)
}

fn build_parsed_datetime_component(
    v: &Vec<IntervalToken>,
    leading_field: DateTimeField,
    value: &str,
    pdt: &mut ParsedDateTime,
) -> Result<(), ParserError> {
    use DateTimeField::*;
    use IntervalToken::*;

    if pdt.day.is_some() {
        println!("DAY IS SOME!!!!");
    }

    let mut actual = v.iter().peekable();

    let mut sign = 1;

    // PostgreSQL inexplicably trims all leading colons from interval parts.
    while let Some(Colon) = actual.peek() {
        actual.next();
    }

    if let Some(Dash) = actual.peek() {
        // If preceded by '-', assume negative.
        sign = -1;
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
                (Dash, Dash) | (Colon, Colon) | (Dot, Dot) => {
                    /* matching punctuation */
                    expected.next();
                    actual.next();

                    println!("Matching punctuation");
                }
                (Num(val), Num(_)) => {
                    println!("Matching numbers");
                    expected.next();
                    actual.next();
                    let val = *val;
                    let frac;
                    if let Some(Dot) = actual.peek() {
                        actual.next();
                        if let Some(Dot) = expected.peek() {
                            expected.next();
                        }
                        if let Some(Num(f)) = actual.peek() {
                            frac = Some((*f * sign) as i64);
                        } else {
                            frac = None;
                        }
                    } else {
                        frac = None;
                    }
                    match current_field {
                        DateTimeField::Year if pdt.year.is_none() => {
                            pdt.year = Some((val * sign) as i128)
                        }
                        DateTimeField::Month if pdt.month.is_none() => {
                            pdt.month = Some((val * sign) as i128)
                        }
                        DateTimeField::Day if pdt.day.is_none() => {
                            pdt.day = Some(val * sign);
                            pdt.day_frac = frac;
                        }
                        DateTimeField::Hour if pdt.hour.is_none() => {
                            pdt.hour = Some(val * sign);
                            pdt.hour_frac = frac;
                        }
                        DateTimeField::Minute if pdt.minute.is_none() => {
                            pdt.minute = Some(val * sign);
                            pdt.minute_frac = frac;
                        }
                        DateTimeField::Second if pdt.second.is_none() => {
                            pdt.second = Some(val * sign);
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
                        println!("(Num(val), Num(_)) Now at {}", current_field)
                    }
                }
                (Nanos(val), Nanos(_)) => {
                    expected.next();
                    actual.next();
                    pdt.nano = Some(*val * (sign as i64))
                }
                // Break out of this component if you encounter a space.
                (Space, _) => {
                    println!("Exit early");
                    actual.next();
                    break;
                }
                // Allow skipping expected numbers.
                (_, Num(_)) => {
                    println!("Skipping expected number");
                    expected.next();

                    // Advance to next field.
                    if current_field != DateTimeField::Second {
                        current_field = current_field
                            .into_iter()
                            .next()
                            .expect("Exhausted iterator");
                        println!("Skipping expected number Now at {}", current_field)
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
    println!("Exited loop in");

    // If using Shorthand, do not allow any further modification.
    match leading_field {
        Year | Month => {
            if pdt.year.is_none() {
                pdt.year = Some(0);
            }
            if pdt.month.is_none() {
                pdt.month = Some(0);
            }
        }
        Day => {
            println!("Filling day");
            if pdt.day.is_none() {
                pdt.day = Some(0);
            }
        }
        // Hour Minute Second
        _ => {
            if pdt.hour.is_none() {
                pdt.hour = Some(0);
            }
            if pdt.minute.is_none() {
                pdt.minute = Some(0);
            }
            if pdt.second.is_none() {
                pdt.second = Some(0);
            }
            if pdt.nano.is_none() {
                pdt.nano = Some(0);
            }
        }
    }

    Ok(())
}

fn build_parsed_datetime_from_datetime_str(
    tokens: &[IntervalToken],
    time_unit: DateTimeField,
    value: &str,
    pdt: &mut ParsedDateTime,
) -> Result<(), ParserError> {
    use IntervalToken::*;

    println!(
        "build_parsed_datetime_from_datetime_str time_unit {}",
        time_unit
    );
    if pdt.day.is_some() {
        println!("DAY IS SOME!!!!");
    }

    let mut actual = tokens.iter().peekable();

    let expected = vec![Dash, Num(0), Dot, Nanos(0)];

    // expected_i lets us skip around the values in expected.
    let mut expected_i = 0;

    // Temporary variables for parsing actual.
    let mut is_negative = 1;
    let mut num_buf = 0i128;
    let mut nano_buf = Some(0i64);

    while let Some(atok) = actual.peek() {
        if let Some(etok) = expected.get(expected_i) {
            match (atok, etok) {
                (Dash, Dash) => {
                    actual.next();
                    is_negative = -1;
                }
                (_, Dash) => {}
                (Num(n), Num(_)) => {
                    println!("Got int {}", n);
                    actual.next();
                    num_buf = *n;

                    match actual.peek() {
                        Some(Dot) => {
                            println!("I see a dot up next");
                        }
                        // Skip forward to TimeUnit.
                        None => break,
                        Some(_) => {
                            return parser_err!("Invalid char in interval {}", value);
                        }
                    }
                }
                // Allow skipping past the first int.
                (_, Num(_)) => {}
                (Dot, Dot) => {
                    println!("Got dot");
                    actual.next();
                }
                (Num(n), Nanos(_)) => {
                    println!("Got nano {}", n);
                    actual.next();
                    // Convert num to nanos.
                    let num_digit = (*n as f64).log10();
                    let multiplicand = 100_000_000 / 10_i64.pow(num_digit as u32);

                    nano_buf = Some((*n as i64) * multiplicand);
                }
                (Nanos(n), Nanos(_)) => {
                    println!("Got nano {}", n);
                    actual.next();

                    nano_buf = Some(*n);
                }
                (_, _) => return parser_err!("Invalid INTERVAL: while let Some(atok)  {}", value),
            }
        }
        // Advance expected_i to next token with circular behavior.
        expected_i += 1;
    }

    println!("timeunit {}", time_unit);

    match time_unit {
        DateTimeField::Year if pdt.year.is_none() => {
            pdt.year = Some(num_buf * is_negative);
        }
        DateTimeField::Month if pdt.month.is_none() => {
            pdt.month = Some(num_buf * is_negative);
        }
        DateTimeField::Day if pdt.day.is_none() => {
            pdt.day = Some(num_buf * is_negative);
        }
        DateTimeField::Hour if pdt.hour.is_none() => {
            pdt.hour = Some(num_buf * is_negative);
        }
        DateTimeField::Minute if pdt.minute.is_none() => {
            pdt.minute = Some(num_buf * is_negative);
        }
        DateTimeField::Second if pdt.second.is_none() => {
            if num_buf > 0 {
                pdt.second = Some(num_buf * is_negative);
            }
            if let Some(nano_buf) = nano_buf {
                pdt.nano = Some(nano_buf * is_negative as i64);
            }
        }
        _ => {
            return parser_err!("Invalid INTERVAL: {}", value);
        }
    }

    Ok(())
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
