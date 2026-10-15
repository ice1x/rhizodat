# Conformance vectors

Language-neutral test data that every rhizodat implementation must pass bit for
bit: the Python reference (`tests/conformance/test_vectors.py`) and the Rust
crates (`crates/rhizodat-conformance/tests/conformance.rs`). A vector describes inputs (injected clocks, random values, strings)
and the exact expected output or a named error.

| File | What it pins down |
|---|---|
| `uuid7.json` | UUIDv7 (RFC 9562, 12-bit monotonic counter): bit layout, counter seeding and increments, clock going backwards, counter overflow borrowing the next millisecond, exhausting the 48-bit timestamp, invalid clocks. The random values a generator requests are part of the vector, with their bit widths. |
| `hlc.json` | Hybrid logical clock: `now` and `update(remote)` sequences, the int64 encoding (`wall_ms << 16 \| logical`), `clock_skew` (state unchanged) and `logical_overflow`. |
| `gid.json` | Branch gids `dat:<dat_txn>:<store>` and parsing of foreign gids. |
| `journal.json` | The journal states and the full 5 × 5 table of legal compare-and-set edges; `PREPARED` needs a `dat_tx_time`; the text of the journal's `branches` column. |
| `dsn.json` | Connection strings: parsed fields, percent-decoding, the redacted form, and strings that must be rejected. |
| `postgres_schema.json` | The schema of a PostgreSQL store or journal: the `schema` query parameter or the argument (both must agree), the DSN handed to the driver without it, and invalid names. |
| `rows.json` | Row batches in a one-phase branch's journal payload: the versioned JSON with tagged values (uuid, decimal as text, bytes, date / datetime, non-finite floats, maps), described value by value; each implementation decodes the payload to those values and round-trips them, and rejects the listed bad payloads. |
| `staged_files.json` | Staged data files in a one-phase branch's journal payload (Iceberg): the versioned JSON with paths and optional `DataFile` objects, how entries of one table merge, and the bad payloads every decoder rejects. |
| `migrations.json` | Schema migration files (`NNNN_name.toml`): id and name from the file name, the steps (`store`, one body keyed by its kind), the canonical JSON form and its sha256 checksum, which the journal records for an applied migration; files every implementation rejects (floats and dates have no portable text form). |
| `crash_matrix.json` | The commit protocol under a crash at every step (two two-phase stores, one one-phase store, journal): the state the crash leaves (journal state, which stores hold a prepared transaction, which are applied) and what one recovery run does. Every case ends all-or-nothing. Implementations replay it with fake stores exposing the named fault points; branches are staged lock-free first, prepared in the listed order, and finished lock holders first. |

Error names (`invalid_clock`, `timestamp_exhausted`, `clock_skew`,
`logical_overflow`) are part of the format; error messages are not. A DSN error
is only `"error": true`: messages differ by language, but must never echo a
secret.

The files are frozen. Changing a vector changes the protocol and must be a
deliberate, reviewed edit of the JSON, never a regeneration from an
implementation.
