//! Row filters as pyiceberg spells them (`Table.scan(row_filter="...")`), parsed
//! into iceberg-rust predicates, so the Python adapter's `execute` parameters
//! work on the core.
//!
//! The grammar is pyiceberg's `pyiceberg.expressions.parser`: comparisons
//! (`<`, `<=`, `>`, `>=`, `=`, `==`, `!=`, `<>`) between a column and a literal
//! (either side), `BETWEEN a AND b`, `IS [NOT] NULL`, `IS [NOT] NAN`, `[NOT] IN
//! (…)`, `[NOT] LIKE 'prefix%'`, `true` / `false`, `NOT`, `AND`, `OR` (in that
//! precedence) and parentheses; keywords are case-insensitive. Literals are
//! single-quoted strings (`''` for a quote), integers, decimals and booleans;
//! columns are bare identifiers (`a.b` for a nested field) or double-quoted.
//! Literals are untyped here and cast to the column's type when the scan binds
//! the predicate, as pyiceberg does.

use std::fmt;

use iceberg::expr::{Predicate, Reference};
use iceberg::spec::Datum;

/// A filter the parser does not accept, with where it stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilterError(String);

impl fmt::Display for FilterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for FilterError {}

/// Parse `text`, a pyiceberg row filter, into a predicate.
///
/// # Errors
///
/// [`FilterError`] for anything outside the grammar.
pub fn parse_filter(text: &str) -> Result<Predicate, FilterError> {
    let tokens = lex(text)?;
    let mut parser = Parser { tokens, at: 0 };
    let predicate = parser.or()?;
    if parser.at < parser.tokens.len() {
        return Err(parser.unexpected("the end of the filter"));
    }
    Ok(predicate)
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    /// A bare or double-quoted identifier (a column, or a keyword before matching).
    Word(String),
    /// A single-quoted string.
    Str(String),
    /// An integer.
    Int(i64),
    /// A decimal number.
    Num(f64),
    /// `<`, `<=`, `>`, `>=`, `=`, `==`, `!=`, `<>`
    Op(&'static str),
    LParen,
    RParen,
    Comma,
    Dot,
}

fn lex(text: &str) -> Result<Vec<Token>, FilterError> {
    let chars: Vec<char> = text.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            c if c.is_whitespace() => i += 1,
            '(' => {
                tokens.push(Token::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(Token::RParen);
                i += 1;
            }
            ',' => {
                tokens.push(Token::Comma);
                i += 1;
            }
            '.' if !chars.get(i + 1).is_some_and(char::is_ascii_digit) => {
                tokens.push(Token::Dot);
                i += 1;
            }
            '\'' => {
                let (s, next) = quoted(&chars, i, '\'')?;
                tokens.push(Token::Str(s));
                i = next;
            }
            '"' => {
                let (s, next) = quoted(&chars, i, '"')?;
                tokens.push(Token::Word(s));
                i = next;
            }
            '<' | '>' | '=' | '!' => {
                let two: String = chars[i..(i + 2).min(chars.len())].iter().collect();
                let (op, len) = match two.as_str() {
                    "<=" => ("<=", 2),
                    ">=" => (">=", 2),
                    "==" => ("==", 2),
                    "!=" => ("!=", 2),
                    "<>" => ("<>", 2),
                    _ => match c {
                        '<' => ("<", 1),
                        '>' => (">", 1),
                        '=' => ("=", 1),
                        _ => return Err(FilterError(format!("unexpected {c:?} at {i}"))),
                    },
                };
                tokens.push(Token::Op(op));
                i += len;
            }
            c if c.is_ascii_digit()
                || ((c == '-' || c == '+' || c == '.')
                    && chars.get(i + 1).is_some_and(char::is_ascii_digit)) =>
            {
                let start = i;
                i += 1;
                while i < chars.len()
                    && (chars[i].is_ascii_digit()
                        || matches!(chars[i], '.' | 'e' | 'E')
                        || (matches!(chars[i], '+' | '-') && matches!(chars[i - 1], 'e' | 'E')))
                {
                    i += 1;
                }
                let raw: String = chars[start..i].iter().collect();
                tokens.push(match raw.parse::<i64>() {
                    Ok(n) => Token::Int(n),
                    Err(_) => Token::Num(
                        raw.parse::<f64>()
                            .map_err(|_| FilterError(format!("not a number: {raw:?}")))?,
                    ),
                });
            }
            c if c.is_alphabetic() || c == '_' => {
                let start = i;
                while i < chars.len()
                    && (chars[i].is_alphanumeric() || matches!(chars[i], '_' | '$'))
                {
                    i += 1;
                }
                tokens.push(Token::Word(chars[start..i].iter().collect()));
            }
            other => return Err(FilterError(format!("unexpected {other:?} at {i}"))),
        }
    }
    Ok(tokens)
}

/// The text inside a `quote`-delimited string starting at `start`; a doubled
/// quote stands for one (single quotes) and a backslash escapes (double quotes).
fn quoted(chars: &[char], start: usize, quote: char) -> Result<(String, usize), FilterError> {
    let mut out = String::new();
    let mut i = start + 1;
    while i < chars.len() {
        match chars[i] {
            c if c == quote && quote == '\'' && chars.get(i + 1) == Some(&quote) => {
                out.push(quote);
                i += 2;
            }
            c if c == quote => return Ok((out, i + 1)),
            '\\' if quote == '"' && i + 1 < chars.len() => {
                out.push(chars[i + 1]);
                i += 2;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    Err(FilterError(format!("unterminated string at {start}")))
}

struct Parser {
    tokens: Vec<Token>,
    at: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.at)
    }

    fn keyword(&self, word: &str) -> bool {
        matches!(self.peek(), Some(Token::Word(w)) if w.eq_ignore_ascii_case(word))
    }

    fn take_keyword(&mut self, word: &str) -> bool {
        if self.keyword(word) {
            self.at += 1;
            true
        } else {
            false
        }
    }

    fn expect_keyword(&mut self, word: &str) -> Result<(), FilterError> {
        if self.take_keyword(word) {
            Ok(())
        } else {
            Err(self.unexpected(word))
        }
    }

    fn expect(&mut self, token: &Token, what: &str) -> Result<(), FilterError> {
        if self.peek() == Some(token) {
            self.at += 1;
            Ok(())
        } else {
            Err(self.unexpected(what))
        }
    }

    fn unexpected(&self, wanted: &str) -> FilterError {
        match self.peek() {
            Some(t) => FilterError(format!("expected {wanted}, found {t:?}")),
            None => FilterError(format!("expected {wanted}, found the end of the filter")),
        }
    }

    fn or(&mut self) -> Result<Predicate, FilterError> {
        let mut left = self.and()?;
        while self.take_keyword("or") {
            left = left.or(self.and()?);
        }
        Ok(left)
    }

    fn and(&mut self) -> Result<Predicate, FilterError> {
        let mut left = self.not()?;
        while self.take_keyword("and") {
            left = left.and(self.not()?);
        }
        Ok(left)
    }

    fn not(&mut self) -> Result<Predicate, FilterError> {
        if self.take_keyword("not") {
            return Ok(self.not()?.negate());
        }
        self.atom()
    }

    fn atom(&mut self) -> Result<Predicate, FilterError> {
        match self.peek() {
            Some(Token::LParen) => {
                self.at += 1;
                let inner = self.or()?;
                self.expect(&Token::RParen, "')'")?;
                Ok(inner)
            }
            Some(Token::Word(w)) if w.eq_ignore_ascii_case("true") => {
                self.at += 1;
                Ok(Predicate::AlwaysTrue)
            }
            Some(Token::Word(w)) if w.eq_ignore_ascii_case("false") => {
                self.at += 1;
                Ok(Predicate::AlwaysFalse)
            }
            Some(Token::Word(_)) => self.column_predicate(),
            Some(Token::Str(_) | Token::Int(_) | Token::Num(_)) => {
                // literal <op> column
                let literal = self.literal()?;
                let op = self.comparison()?;
                let column = self.column()?;
                Ok(compare(column, flip(op), literal))
            }
            _ => Err(self.unexpected("a predicate")),
        }
    }

    fn column_predicate(&mut self) -> Result<Predicate, FilterError> {
        let column = self.column()?;
        if self.take_keyword("is") {
            let negated = self.take_keyword("not");
            let reference = Reference::new(column);
            return Ok(match (self.take_keyword("null"), negated) {
                (true, false) => reference.is_null(),
                (true, true) => reference.is_not_null(),
                (false, negated) => {
                    self.expect_keyword("nan")?;
                    if negated {
                        reference.is_not_nan()
                    } else {
                        reference.is_nan()
                    }
                }
            });
        }
        if self.take_keyword("between") {
            let low = self.literal()?;
            self.expect_keyword("and")?;
            let high = self.literal()?;
            return Ok(Reference::new(&column)
                .greater_than_or_equal_to(low)
                .and(Reference::new(&column).less_than_or_equal_to(high)));
        }
        let negated = self.take_keyword("not");
        if self.take_keyword("in") {
            self.expect(&Token::LParen, "'('")?;
            let mut literals = vec![self.literal()?];
            while self.peek() == Some(&Token::Comma) {
                self.at += 1;
                literals.push(self.literal()?);
            }
            self.expect(&Token::RParen, "')'")?;
            let reference = Reference::new(column);
            return Ok(if negated {
                reference.is_not_in(literals)
            } else {
                reference.is_in(literals)
            });
        }
        if self.take_keyword("like") {
            let Some(Token::Str(pattern)) = self.peek().cloned() else {
                return Err(self.unexpected("a quoted pattern"));
            };
            self.at += 1;
            return like(&column, &pattern, negated);
        }
        if negated {
            return Err(self.unexpected("IN or LIKE after NOT"));
        }
        let op = self.comparison()?;
        let literal = self.literal()?;
        Ok(compare(column, op, literal))
    }

    fn column(&mut self) -> Result<String, FilterError> {
        let Some(Token::Word(first)) = self.peek().cloned() else {
            return Err(self.unexpected("a column"));
        };
        self.at += 1;
        let mut name = first;
        while self.peek() == Some(&Token::Dot) {
            self.at += 1;
            let Some(Token::Word(part)) = self.peek().cloned() else {
                return Err(self.unexpected("a field after '.'"));
            };
            self.at += 1;
            name.push('.');
            name.push_str(&part);
        }
        Ok(name)
    }

    fn comparison(&mut self) -> Result<&'static str, FilterError> {
        match self.peek() {
            Some(Token::Op(op)) => {
                let op = *op;
                self.at += 1;
                Ok(op)
            }
            _ => Err(self.unexpected("a comparison")),
        }
    }

    fn literal(&mut self) -> Result<Datum, FilterError> {
        let datum = match self.peek() {
            Some(Token::Str(s)) => Datum::string(s.clone()),
            Some(Token::Int(n)) => Datum::long(*n),
            Some(Token::Num(x)) => Datum::double(*x),
            Some(Token::Word(w)) if w.eq_ignore_ascii_case("true") => Datum::bool(true),
            Some(Token::Word(w)) if w.eq_ignore_ascii_case("false") => Datum::bool(false),
            _ => return Err(self.unexpected("a literal")),
        };
        self.at += 1;
        Ok(datum)
    }
}

/// `column <op> literal`.
fn compare(column: String, op: &str, literal: Datum) -> Predicate {
    let reference = Reference::new(column);
    match op {
        "<" => reference.less_than(literal),
        "<=" => reference.less_than_or_equal_to(literal),
        ">" => reference.greater_than(literal),
        ">=" => reference.greater_than_or_equal_to(literal),
        "!=" | "<>" => reference.not_equal_to(literal),
        _ => reference.equal_to(literal),
    }
}

/// The operator of `literal <op> column` read as `column <op'> literal`.
fn flip(op: &'static str) -> &'static str {
    match op {
        "<" => ">",
        "<=" => ">=",
        ">" => "<",
        ">=" => "<=",
        other => other,
    }
}

/// `LIKE 'prefix%'` is a starts-with; a pattern without a trailing `%` is an
/// equality; `%` elsewhere (unescaped) is not supported, as in pyiceberg.
fn like(column: &str, pattern: &str, negated: bool) -> Result<Predicate, FilterError> {
    let (prefix, wildcard) = match pattern.strip_suffix('%') {
        Some(prefix) if !prefix.ends_with('\\') => (prefix.to_owned(), true),
        _ => (pattern.to_owned(), false),
    };
    let mut rest = prefix.replace("\\%", "\u{0}");
    if rest.contains('%') {
        return Err(FilterError(format!(
            "LIKE {pattern:?}: only a trailing % wildcard is supported"
        )));
    }
    rest = rest.replace('\u{0}', "%");
    let reference = Reference::new(column);
    Ok(match (wildcard, negated) {
        (true, false) => reference.starts_with(Datum::string(rest)),
        (true, true) => reference.not_starts_with(Datum::string(rest)),
        (false, false) => reference.equal_to(Datum::string(rest)),
        (false, true) => reference.not_equal_to(Datum::string(rest)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(filter: &str) -> String {
        parse_filter(filter).unwrap().to_string()
    }

    #[test]
    fn comparisons_read_either_way_round() {
        assert_eq!(text("qty >= 1"), text("1 <= qty"));
        assert_eq!(text("sku == 'a'"), text("sku = 'a'"));
        assert_eq!(text("sku != 'a'"), text("sku <> 'a'"));
        assert_eq!(
            text("dat_id == 'x' AND dat_tx_time <= 5"),
            Reference::new("dat_id")
                .equal_to(Datum::string("x"))
                .and(Reference::new("dat_tx_time").less_than_or_equal_to(Datum::long(5)))
                .to_string()
        );
    }

    #[test]
    fn keywords_are_case_insensitive_and_bind_not_and_or_in_order() {
        let p = parse_filter("not a > 1 or b < 2 and c IS NULL").unwrap();
        let wanted = Reference::new("a")
            .greater_than(Datum::long(1))
            .negate()
            .or(Reference::new("b")
                .less_than(Datum::long(2))
                .and(Reference::new("c").is_null()));
        assert_eq!(p.to_string(), wanted.to_string());
        assert_eq!(
            text("(a > 1 OR b < 2) AND c is not null"),
            text("(a > 1 or b < 2) and c IS NOT NULL")
        );
    }

    #[test]
    fn sets_ranges_nans_prefixes_and_quotes() {
        assert_eq!(
            text("kind IN ('a', 'b')"),
            Reference::new("kind")
                .is_in([Datum::string("a"), Datum::string("b")])
                .to_string()
        );
        assert_eq!(
            text("n NOT IN (1, 2.5)"),
            Reference::new("n")
                .is_not_in([Datum::long(1), Datum::double(2.5)])
                .to_string()
        );
        assert_eq!(text("n BETWEEN 1 AND 3"), text("n >= 1 AND n <= 3"));
        assert_eq!(text("x IS NAN"), Reference::new("x").is_nan().to_string());
        assert_eq!(
            text("x is not nan"),
            Reference::new("x").is_not_nan().to_string()
        );
        assert_eq!(
            text("sku LIKE 'SKU-%'"),
            Reference::new("sku")
                .starts_with(Datum::string("SKU-"))
                .to_string()
        );
        assert_eq!(text("sku NOT LIKE 'a'"), text("sku != 'a'"));
        assert_eq!(text("name == 'it''s'"), text("name == 'it''s'"));
        assert_eq!(
            text("\"odd name\" == 'x'"),
            Reference::new("odd name")
                .equal_to(Datum::string("x"))
                .to_string()
        );
        assert_eq!(
            text("a.b > 1"),
            Reference::new("a.b")
                .greater_than(Datum::long(1))
                .to_string()
        );
        assert_eq!(
            text("ok == true"),
            Reference::new("ok").equal_to(Datum::bool(true)).to_string()
        );
        assert_eq!(text("true"), Predicate::AlwaysTrue.to_string());
        assert_eq!(
            text("-2 < n"),
            Reference::new("n")
                .greater_than(Datum::long(-2))
                .to_string()
        );
    }

    #[test]
    fn what_is_outside_the_grammar_is_refused_with_the_place() {
        for (filter, needle) in [
            ("qty >", "a literal"),
            ("qty 1", "a comparison"),
            ("(a > 1", "')'"),
            ("a > 1 b", "the end of the filter"),
            ("a LIKE '%x'", "trailing %"),
            ("a IS", "nan"),
            ("a NOT b", "IN or LIKE"),
            ("'unterminated", "unterminated"),
            ("a # 1", "unexpected '#'"),
            ("", "a predicate"),
        ] {
            let err = parse_filter(filter).unwrap_err().to_string();
            assert!(err.contains(needle), "{filter:?}: {err}");
        }
    }
}
