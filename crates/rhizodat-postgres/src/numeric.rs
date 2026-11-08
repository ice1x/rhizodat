//! PostgreSQL's binary `numeric`, to and from the decimal text the core carries
//! (`RowValue::Decimal`): exact, with the scale kept (`7.00` stays `7.00`).
//!
//! The wire form is a header of four 16-bit fields (the number of digits, the
//! weight of the first digit, the sign, the display scale) followed by the
//! digits in base 10 000, most significant first; the value is
//! `sum(digit[i] * 10000^(weight - i))`. The sign also marks `NaN` and the
//! infinities (PostgreSQL 14 and later).

use std::fmt::Write as _;

const POSITIVE: u16 = 0x0000;
const NEGATIVE: u16 = 0x4000;
const NAN: u16 = 0xC000;
const PLUS_INFINITY: u16 = 0xD000;
const MINUS_INFINITY: u16 = 0xF000;

/// The decimal text of a binary `numeric`, as PostgreSQL prints it
/// (`-12.50`, `0.0001`, `NaN`, `Infinity`).
///
/// # Errors
///
/// A message when the bytes are not a `numeric`.
pub fn decode(raw: &[u8]) -> Result<String, String> {
    let field = |i: usize| -> Result<u16, String> {
        raw.get(2 * i..2 * i + 2)
            .map(|b| u16::from_be_bytes([b[0], b[1]]))
            .ok_or_else(|| format!("a numeric of {} bytes is cut short", raw.len()))
    };
    let ndigits = usize::from(field(0)?);
    let weight = i32::from(i16::from_be_bytes(field(1)?.to_be_bytes()));
    let sign = field(2)?;
    let dscale = usize::from(field(3)?);
    let digits = (0..ndigits)
        .map(|i| {
            let d = field(4 + i)?;
            if d < 10_000 {
                Ok(d)
            } else {
                Err(format!("{d} is not a base-10000 digit"))
            }
        })
        .collect::<Result<Vec<u16>, String>>()?;
    match sign {
        NAN => return Ok("NaN".into()),
        PLUS_INFINITY => return Ok("Infinity".into()),
        MINUS_INFINITY => return Ok("-Infinity".into()),
        POSITIVE | NEGATIVE => {}
        other => return Err(format!("{other:#06x} is not a numeric sign")),
    }
    // The digit at position `p` (weight `weight - p`); missing ones are zero.
    let digit = |p: i32| -> u16 {
        usize::try_from(p)
            .ok()
            .and_then(|p| digits.get(p))
            .copied()
            .unwrap_or(0)
    };
    let mut text = String::new();
    if sign == NEGATIVE && digits.iter().any(|&d| d != 0) {
        text.push('-');
    }
    if weight < 0 {
        text.push('0');
    } else {
        let _ = write!(text, "{}", digit(0));
        for p in 1..=weight {
            let _ = write!(text, "{:04}", digit(p));
        }
    }
    if dscale > 0 {
        let mut fraction = String::with_capacity(dscale + 4);
        let mut p = weight + 1;
        while fraction.len() < dscale {
            let _ = write!(fraction, "{:04}", digit(p));
            p += 1;
        }
        fraction.truncate(dscale);
        text.push('.');
        text.push_str(&fraction);
    }
    Ok(text)
}

/// The binary `numeric` of a decimal text: plain (`-12.50`) or with an
/// exponent as Python prints small and large decimals (`1.5E-7`, `1E+3`),
/// `NaN`, `Infinity` or `-Infinity`. The scale is the number of digits after
/// the point once the exponent is applied (`1.50` has 2, `1E+3` has 0).
///
/// # Errors
///
/// A message when the text is not a decimal or is beyond `numeric`'s range.
pub fn encode(text: &str) -> Result<Vec<u8>, String> {
    let bad = || format!("{text:?} is not a decimal");
    let special = match text.to_ascii_lowercase().as_str() {
        "nan" => Some(NAN),
        "infinity" | "inf" | "+infinity" | "+inf" => Some(PLUS_INFINITY),
        "-infinity" | "-inf" => Some(MINUS_INFINITY),
        _ => None,
    };
    if let Some(sign) = special {
        return Ok(header(0, 0, sign, 0));
    }
    let (negative, unsigned) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
        Some(at) => (
            &unsigned[..at],
            unsigned[at + 1..].parse::<i64>().map_err(|_| bad())?,
        ),
        None => (unsigned, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let is_digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
    if whole.len() + fraction.len() == 0 || !is_digits(whole) || !is_digits(fraction) {
        return Err(bad());
    }
    // All the digits, and where the point falls among them after the exponent.
    let coefficient: String = [whole, fraction].concat();
    let len = i64::try_from(coefficient.len()).map_err(|_| bad())?;
    let point = i64::try_from(whole.len()).map_err(|_| bad())? + exponent;
    let out_of_range = || format!("{text:?} is beyond the range of numeric");
    if !(-16_383..=131_072).contains(&point) || len - point > 16_383 {
        return Err(out_of_range());
    }
    let (int_part, frac_part) = if point <= 0 {
        (
            String::new(),
            "0".repeat(usize::try_from(-point).map_err(|_| bad())?) + &coefficient,
        )
    } else if point >= len {
        (
            coefficient.clone() + &"0".repeat(usize::try_from(point - len).map_err(|_| bad())?),
            String::new(),
        )
    } else {
        let at = usize::try_from(point).map_err(|_| bad())?;
        (coefficient[..at].to_owned(), coefficient[at..].to_owned())
    };
    let dscale = u16::try_from(frac_part.len()).map_err(|_| out_of_range())?;
    // Groups of four digits, aligned on the point.
    let int_pad = (4 - int_part.len() % 4) % 4;
    let int_aligned = "0".repeat(int_pad) + &int_part;
    let frac_pad = (4 - frac_part.len() % 4) % 4;
    let frac_aligned = frac_part + &"0".repeat(frac_pad);
    let group = |s: &str| -> u16 { s.parse().unwrap_or(0) };
    let mut groups: Vec<u16> = int_aligned
        .as_bytes()
        .chunks(4)
        .chain(frac_aligned.as_bytes().chunks(4))
        .map(|c| group(std::str::from_utf8(c).unwrap_or("0")))
        .collect();
    let mut weight = i32::try_from(int_aligned.len() / 4).map_err(|_| out_of_range())? - 1;
    let leading = groups.iter().take_while(|&&g| g == 0).count();
    groups.drain(..leading);
    weight -= i32::try_from(leading).map_err(|_| out_of_range())?;
    while groups.last() == Some(&0) {
        groups.pop();
    }
    if groups.is_empty() {
        return Ok(header(0, 0, POSITIVE, dscale));
    }
    let ndigits = u16::try_from(groups.len()).map_err(|_| out_of_range())?;
    let weight = i16::try_from(weight).map_err(|_| out_of_range())?;
    let mut out = header(
        ndigits,
        weight,
        if negative { NEGATIVE } else { POSITIVE },
        dscale,
    );
    for g in groups {
        out.extend_from_slice(&g.to_be_bytes());
    }
    Ok(out)
}

fn header(ndigits: u16, weight: i16, sign: u16, dscale: u16) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + 2 * usize::from(ndigits));
    out.extend_from_slice(&ndigits.to_be_bytes());
    out.extend_from_slice(&weight.to_be_bytes());
    out.extend_from_slice(&sign.to_be_bytes());
    out.extend_from_slice(&dscale.to_be_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wire(ndigits: u16, weight: i16, sign: u16, dscale: u16, digits: &[u16]) -> Vec<u8> {
        let mut out = header(ndigits, weight, sign, dscale);
        for d in digits {
            out.extend_from_slice(&d.to_be_bytes());
        }
        out
    }

    #[test]
    fn known_wire_forms_decode() {
        // 12345.678 = 1|2345|.6780, weight 1, scale 3
        assert_eq!(
            decode(&wire(3, 1, POSITIVE, 3, &[1, 2345, 6780])).unwrap(),
            "12345.678"
        );
        // -0.0001 = .0001, weight -1, scale 4
        assert_eq!(decode(&wire(1, -1, NEGATIVE, 4, &[1])).unwrap(), "-0.0001");
        // 0.00000001 = .0000|0001, weight -2
        assert_eq!(
            decode(&wire(1, -2, POSITIVE, 8, &[1])).unwrap(),
            "0.00000001"
        );
        // 10000 = 1|0000 with the trailing zero group dropped
        assert_eq!(decode(&wire(1, 1, POSITIVE, 0, &[1])).unwrap(), "10000");
        assert_eq!(decode(&wire(0, 0, POSITIVE, 2, &[])).unwrap(), "0.00");
        assert_eq!(decode(&wire(0, 0, NAN, 0, &[])).unwrap(), "NaN");
        assert_eq!(
            decode(&wire(0, 0, PLUS_INFINITY, 0, &[])).unwrap(),
            "Infinity"
        );
        assert_eq!(
            decode(&wire(0, 0, MINUS_INFINITY, 0, &[])).unwrap(),
            "-Infinity"
        );
    }

    #[test]
    fn decimal_texts_encode_to_the_wire_form() {
        assert_eq!(
            encode("12345.678").unwrap(),
            wire(3, 1, POSITIVE, 3, &[1, 2345, 6780])
        );
        assert_eq!(encode("-0.0001").unwrap(), wire(1, -1, NEGATIVE, 4, &[1]));
        assert_eq!(encode("1E+4").unwrap(), wire(1, 1, POSITIVE, 0, &[1]));
        assert_eq!(encode("1.5E-7").unwrap(), wire(1, -2, POSITIVE, 8, &[15]));
        assert_eq!(encode("0.00").unwrap(), wire(0, 0, POSITIVE, 2, &[]));
        assert_eq!(encode("-0").unwrap(), wire(0, 0, POSITIVE, 0, &[]));
        assert_eq!(encode("NaN").unwrap(), wire(0, 0, NAN, 0, &[]));
        assert_eq!(
            encode("-Infinity").unwrap(),
            wire(0, 0, MINUS_INFINITY, 0, &[])
        );
    }

    #[test]
    fn texts_round_trip_as_postgresql_prints_them() {
        for text in [
            "0",
            "7.00",
            "-1234.50",
            "98765432109876543210.0123456789012345678901",
            "0.000001",
            "100000000",
            "Infinity",
        ] {
            assert_eq!(decode(&encode(text).unwrap()).unwrap(), text);
        }
        for (python, postgres) in [("1E+3", "1000"), ("1.50E-5", "0.0000150"), ("+3", "3")] {
            assert_eq!(decode(&encode(python).unwrap()).unwrap(), postgres);
        }
    }

    #[test]
    fn what_is_not_a_numeric_is_refused() {
        for text in ["", "-", ".", "1.2.3", "abc", "1e", "1E+x", "sNaN", "1_000"] {
            assert!(encode(text).is_err(), "{text:?}");
        }
        assert!(encode("1E+200000").unwrap_err().contains("range"));
        assert!(encode("1E-20000").unwrap_err().contains("range"));
        assert!(decode(&[0, 1]).unwrap_err().contains("cut short"));
        assert!(decode(&wire(1, 0, POSITIVE, 0, &[10_000])).is_err());
        assert!(decode(&wire(0, 0, 0x1234, 0, &[])).is_err());
    }
}
