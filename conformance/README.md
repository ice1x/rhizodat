# Conformance vectors

Language-neutral test data that every datorium implementation must pass bit for
bit: the Python reference (`tests/conformance/test_vectors.py`) and the planned
Rust core. A vector describes inputs (injected clocks, random values, strings)
and the exact expected output or a named error.

| File | What it pins down |
|---|---|
| `uuid7.json` | UUIDv7 (RFC 9562, 12-bit monotonic counter): bit layout, counter seeding and increments, clock going backwards, counter overflow borrowing the next millisecond, exhausting the 48-bit timestamp, invalid clocks. The random values a generator requests are part of the vector, with their bit widths. |
| `hlc.json` | Hybrid logical clock: `now` and `update(remote)` sequences, the int64 encoding (`wall_ms << 16 \| logical`), `clock_skew` (state unchanged) and `logical_overflow`. |
| `gid.json` | Branch gids `dat:<dat_txn>:<store>` and parsing of foreign gids. |
| `journal.json` | The journal states and the full 5 × 5 table of legal compare-and-set edges; `PREPARED` needs a `dat_tx_time`. |
| `dsn.json` | Connection strings: parsed fields, percent-decoding, the redacted form, and strings that must be rejected. |

Error names (`invalid_clock`, `timestamp_exhausted`, `clock_skew`,
`logical_overflow`) are part of the format; error messages are not. A DSN error
is only `"error": true`: messages differ by language, but must never echo a
secret.

The files are frozen. Changing a vector changes the protocol and must be a
deliberate, reviewed edit of the JSON, never a regeneration from an
implementation. Still to add: the crash matrix (failure point × outcome after
recovery) now covered by the Python property tests.
