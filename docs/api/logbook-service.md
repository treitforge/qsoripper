# LogbookService Reference

The `LogbookService` is the core QSO lifecycle interface.
It covers local QSO storage, callsign enrichment, external sync, and ADIF transfer.

Proto definition: [`proto/services/logbook_service.proto`](../../proto/services/logbook_service.proto)

Domain payloads: [`proto/domain/qso_record.proto`](../../proto/domain/qso_record.proto), [`proto/domain/band.proto`](../../proto/domain/band.proto), [`proto/domain/mode.proto`](../../proto/domain/mode.proto), [`proto/domain/sync_status.proto`](../../proto/domain/sync_status.proto), [`proto/domain/station_snapshot.proto`](../../proto/domain/station_snapshot.proto)

Service envelopes and support types live in their own files under `proto/services/`. Every RPC uses a unique request/response envelope, including streamed items such as `ListQsosResponse` and `ExportAdifResponse`.

## Implementation Status

| RPC | Status | Notes |
|---|---|---|
| `LogQso` | Implemented | Saves locally through the configured backend. QRZ sync still reports unimplemented when requested |
| `UpdateQso` | Implemented | Updates local storage. QRZ sync still reports unimplemented when requested |
| `DeleteQso` | Implemented | Deletes from local storage. QRZ delete still reports unimplemented when requested |
| `GetQso` | Implemented | Loads a single local QSO by `local_id` |
| `ListQsos` | Implemented | Streams locally stored QSOs with filters, sorting, limit, and offset |
| `BackfillQsoEnrichment` | Implemented | Fills missing QSO enrichment fields through QRZ XML callsign lookup |
| `SyncWithQrz` | Planned | Contract defined. Returns `UNIMPLEMENTED` |
| `GetSyncStatus` | Implemented | Returns live local counts from storage. QRZ fields remain zero or absent until the engine implements sync. |
| `ImportAdif` | Implemented | Streams ADIF in, imports after client close, reports duplicates/fallback warnings |
| `ExportAdif` | Implemented | Streams filtered ADIF out in chronological order |

## RPCs

### LogQso

Log a new QSO (contact). Optionally syncs the new record to QRZ immediately.

```
rpc LogQso(LogQsoRequest) returns (LogQsoResponse)
```

> **Status:** Implemented for local storage.

**Request:** `LogQsoRequest`

| Field | Type | Description |
|---|---|---|
| `qso` | `QsoRecord` | The QSO to log. Keep `local_id` empty. The engine assigns a UUID. |
| `sync_to_qrz` | `bool` | If `true`, also upload to QRZ logbook immediately after logging locally. |

**Response:** `LogQsoResponse`

| Field | Type | Description |
|---|---|---|
| `local_id` | `string` | Engine-assigned UUID for the new QSO |
| `qrz_logid` | `string` (optional) | QRZ logbook record ID, set only when `sync_to_qrz` was `true` and sync succeeded |
| `sync_success` | `bool` | `true` after a local save without a QRZ failure. `false` when the request includes QRZ work without an implementation. |
| `sync_error` | `string` (optional) | Human-readable sync error message when `sync_to_qrz == true` and the QRZ step failed |

**Behavior:**
- The engine always assigns a new `local_id` (UUID). Do not set `QsoRecord.local_id` in the request.
- Required user input is `worked_callsign`, `utc_timestamp`, `band`, and `mode`, plus `station_callsign` unless the effective active station context already supplies the local station identity.
- When active station context is available, the server creates `station_snapshot` from it.
  The snapshot supplies default local-station values for the new record.
- If `sync_to_qrz == false`, the engine logs the QSO locally only. `sync_success` is `true`, and QRZ fields remain absent.
- A QRZ sync failure does not cause a local log failure. The engine keeps the local QSO. Check `sync_success` and `sync_error`.

**Notable status codes:**
- `INVALID_ARGUMENT` - missing required fields or invalid enum values.

---

### UpdateQso

Update an existing QSO identified by `local_id`.

```
rpc UpdateQso(UpdateQsoRequest) returns (UpdateQsoResponse)
```

> **Status:** Implemented for local storage.

**Request:** `UpdateQsoRequest`

| Field | Type | Description |
|---|---|---|
| `qso` | `QsoRecord` | Updated QSO. `local_id` must be set to identify the record. |
| `sync_to_qrz` | `bool` | If `true`, also update the record in QRZ logbook. |

**Response:** `UpdateQsoResponse`

| Field | Type | Description |
|---|---|---|
| `success` | `bool` | Whether the local update succeeded |
| `error` | `string` (optional) | Error message when `success == false` |
| `sync_success` | `bool` | Whether the optional QRZ sync succeeded |
| `sync_error` | `string` (optional) | Sync error message |

**Notable status codes:**
- `NOT_FOUND` - `local_id` does not exist in the local logbook.
- `INVALID_ARGUMENT` - the request is missing `local_id` or other required fields.

---

### DeleteQso

Delete a QSO from the local logbook. Optionally also deletes it from QRZ logbook.

```
rpc DeleteQso(DeleteQsoRequest) returns (DeleteQsoResponse)
```

> **Status:** Implemented for local storage.

**Request:** `DeleteQsoRequest`

| Field | Type | Description |
|---|---|---|
| `local_id` | `string` | UUID of the QSO to delete |
| `delete_from_qrz` | `bool` | If `true`, also delete the record from QRZ logbook (**permanent**, cannot be undone) |

**Response:** `DeleteQsoResponse`

| Field | Type | Description |
|---|---|---|
| `success` | `bool` | Whether the local delete succeeded |
| `error` | `string` (optional) | Error message when `success == false` |
| `qrz_delete_success` | `bool` | Whether the optional QRZ delete succeeded |
| `qrz_delete_error` | `string` (optional) | QRZ delete error message |

> **Warning:** Setting `delete_from_qrz = true` is **permanent and irreversible** on the QRZ side. Prompt the user to confirm before calling this with `delete_from_qrz = true`.

**Notable status codes:**
- `NOT_FOUND` - `local_id` does not exist.
- `INVALID_ARGUMENT` - `local_id` is blank.

---

### GetQso

Retrieve a single QSO by its local UUID.

```
rpc GetQso(GetQsoRequest) returns (GetQsoResponse)
```

> **Status:** Implemented for local storage.

**Request:** `GetQsoRequest`

| Field | Type | Description |
|---|---|---|
| `local_id` | `string` | UUID of the QSO to retrieve |

**Response:** `GetQsoResponse`

| Field | Type | Description |
|---|---|---|
| `qso` | `QsoRecord` | The retrieved QSO record |

**Notable status codes:**
- `NOT_FOUND` - `local_id` does not exist.
- `INVALID_ARGUMENT` - `local_id` is blank.

---

### ListQsos

List QSOs with optional filters, returning results as a server-streaming response.

```
rpc ListQsos(ListQsosRequest) returns (stream ListQsosResponse)
```

> **Status:** Implemented for local storage.

**Request:** `ListQsosRequest`

| Field | Type | Description |
|---|---|---|
| `after` | `Timestamp` (optional) | Include only QSOs with `utc_timestamp` after this time |
| `before` | `Timestamp` (optional) | Include only QSOs with `utc_timestamp` before this time |
| `callsign_filter` | `string` (optional) | Filter by `worked_callsign` (exact match) |
| `band_filter` | `Band` (optional) | Filter by band |
| `mode_filter` | `Mode` (optional) | Filter by mode |
| `contest_id` | `string` (optional) | Filter by contest ID |
| `limit` | `uint32` | Maximum records to return. `0` means no limit |
| `offset` | `uint32` | Skip this many records (for pagination) |
| `sort` | `QsoSortOrder` | `QSO_SORT_ORDER_NEWEST_FIRST` (default) or `QSO_SORT_ORDER_OLDEST_FIRST` |

**Response stream:** Zero or more `ListQsosResponse` messages, then stream close.

| Field | Type | Description |
|---|---|---|
| `qso` | `QsoRecord` | One matched QSO per streamed envelope |

**Behavior:**
- The engine streams each result when it produces the result. Clients must process each result.
- All filter fields are optional. Omitting all filters returns all QSOs (subject to `limit`/`offset`).

**Notable status codes:**
- `OK` - zero or more `ListQsosResponse` envelopes streamed back.

---

### BackfillQsoEnrichment

Fill missing enrichment fields on active local QSOs.

```
rpc BackfillQsoEnrichment(BackfillQsoEnrichmentRequest) returns (stream BackfillQsoEnrichmentResponse)
```

Preview mode is the default.
Apply mode uses atomic semantic compare-and-update writes; protobuf map wire order
does not affect concurrent-edit detection.
The engine keeps concurrent operator changes.
The engine does not change QRZ Logbook fields or sync state.

The request supports optional inclusive `after` and `before` UTC filters.
Both values must be valid protobuf timestamps, and `after` must not be later than
`before`; violations return `INVALID_ARGUMENT`.
The response contains cumulative scan, lookup, change, conflict, and storage counts.
The final response has `complete=true`.
Cancellation stops new work but waits for an active, independently bounded shared
provider lookup before releasing the one-run lease.
The CLI returns failure for a stream without a terminal complete response or for a
summary with lookup/storage errors; not-found records are normal success results.

The CLI command is:

```powershell
qsoripper-cli enrich --preview
qsoripper-cli enrich --apply --after 2026-08-01T00:00:00Z
```

**Notable status codes:**

- `RESOURCE_EXHAUSTED` - another backfill is active.
- `INVALID_ARGUMENT` - the mode or time range is invalid.

---

### SyncWithQrz

Start a full or incremental sync with the QRZ logbook. The engine streams progress to the client.

```
rpc SyncWithQrz(SyncWithQrzRequest) returns (stream SyncWithQrzResponse)
```

> **Status:** Planned. Currently returns `UNIMPLEMENTED`.

**Request:** `SyncWithQrzRequest`

| Field | Type | Description |
|---|---|---|
| `full_sync` | `bool` | `true` = re-fetch all records from QRZ. `false` = incremental (changes since last sync) |

**Response stream:** One or more `SyncWithQrzResponse` messages, terminated by a message with `complete == true`.

**`SyncWithQrzResponse` fields:**

| Field | Type | Description |
|---|---|---|
| `total_records` | `uint32` | Total records to process |
| `processed_records` | `uint32` | Records processed so far |
| `uploaded_records` | `uint32` | Records pushed to QRZ |
| `downloaded_records` | `uint32` | Records fetched from QRZ |
| `conflict_records` | `uint32` | Records with local/remote divergence |
| `current_action` | `string` (optional) | Human-readable status message |
| `complete` | `bool` | `true` on the final message - stream ends after this |
| `error` | `string` (optional) | Error message if sync failed |

**Behavior:**
- The server closes the stream after sending a message with `complete == true`.
- Clients must update the progress display after each message.
- A QRZ credentials error will produce an early terminal message with `complete == true` and `error` set.

**Notable status codes:**
- `UNIMPLEMENTED` - current server response.
- `UNAUTHENTICATED` - future: QRZ credentials not configured or invalid.

---

### GetSyncStatus

Get the current sync state and logbook statistics.

```
rpc GetSyncStatus(GetSyncStatusRequest) returns (GetSyncStatusResponse)
```

> **Status:** The engine implements local storage counts. QRZ metadata remains zero or absent until the engine implements QRZ sync.

**Request:** `GetSyncStatusRequest` - empty message, no fields.

**Response:** `GetSyncStatusResponse`

| Field | Type | Description |
|---|---|---|
| `local_qso_count` | `uint32` | Number of QSOs in the local logbook |
| `qrz_qso_count` | `uint32` | Number of QSOs reported by QRZ (from `STATUS` command) |
| `pending_upload` | `uint32` | Local QSOs not yet synced to QRZ |
| `last_sync` | `Timestamp` (optional) | Timestamp of the most recent successful sync |
| `qrz_logbook_owner` | `string` (optional) | QRZ logbook owner callsign |

**Current behavior:** The engine calculates `local_qso_count` and `pending_upload` from storage. Until QRZ sync exists, the other QRZ fields remain empty.

**Notable status codes:**
- `OK` - always returned. Check field values for substantive data.

---

### ImportAdif

Import QSOs from ADIF data. The client streams chunks of raw ADIF bytes. The server parses and imports them.

```
rpc ImportAdif(stream ImportAdifRequest) returns (ImportAdifResponse)
```

> **Status:** Implemented for local ADIF migration import.

**Request stream:** One or more `ImportAdifRequest` messages, each containing one `AdifChunk`.

| Field | Type | Description |
|---|---|---|
| `chunk` | `AdifChunk` | Wrapper envelope for one raw ADIF byte slice |

**Behavior:**
- Clients can split large ADIF files into multiple chunks to prevent large messages.
- The server accumulates chunks and parses the complete ADIF payload after the client closes the send side.
- The engine preserves imported `STATION_CALLSIGN`, `OPERATOR`, and `MY_*` fields through `station_snapshot`. The active station profile does **not** overwrite this history.
- If an ADIF record has no local-station context, the server uses the active station profile.
  The server adds a warning that describes this fallback.
- Duplicate policy: The engine skips a record that matches an existing QSO on the duplicate fields.
  These fields are `station_callsign`, `worked_callsign`, `utc_timestamp`, `band`, `mode`, and compatible `submode` or `frequency_hz`.
- The engine skips invalid core ADIF values and supplies warnings.
  It keeps raw ADIF values in `extra_fields` for later exports.

**Response:** `ImportAdifResponse`

| Field | Type | Description |
|---|---|---|
| `records_imported` | `uint32` | Number of QSOs successfully imported |
| `records_skipped` | `uint32` | Number of records skipped (duplicates or parse errors) |
| `warnings` | `repeated string` | Human-readable warnings for individual record issues |

**Notable status codes:**
- `OK` - import completed. Inspect counts and warnings for duplicates or skipped records.
- `INVALID_ARGUMENT` - malformed ADIF payload that the parser cannot parse.
- `INTERNAL` - storage failure during import.

---

### ExportAdif

Export QSOs to ADIF format. The server streams chunks of raw ADIF bytes back to the client.

```
rpc ExportAdif(ExportAdifRequest) returns (stream ExportAdifResponse)
```

> **Status:** Implemented for local ADIF export.

**Request:** `ExportAdifRequest`

| Field | Type | Description |
|---|---|---|
| `after` | `Timestamp` (optional) | Export only QSOs after this time |
| `before` | `Timestamp` (optional) | Export only QSOs before this time |
| `contest_id` | `string` (optional) | Export only QSOs for a specific contest |
| `include_header` | `bool` | Whether to include the ADIF file header with version/program info |

**Response stream:** One or more `ExportAdifResponse` messages containing one `AdifChunk`, then stream close.

| Field | Type | Description |
|---|---|---|
| `chunk` | `AdifChunk` | Wrapper envelope for one exported ADIF byte slice |

**Behavior:**
- Clients must concatenate the chunks to reconstruct the full ADIF payload.
- Omitting all filters exports all QSOs.
- Export order is chronological (`oldest first`) after applying the filters.
- `include_header=true` prepends an ADIF header with version/program metadata.

**Notable status codes:**
- `OK` - export stream opened successfully.
- `INTERNAL` - storage failure while enumerating records for export.

---

## QsoRecord Key Fields

| Field | Required? | Description |
|---|---|---|
| `local_id` | Assigned by engine | UUID - do not set in `LogQso` requests |
| `station_callsign` | Required | Local operator's callsign |
| `worked_callsign` | Required | Remote station's callsign |
| `utc_timestamp` | Required | UTC time of the contact |
| `utc_end_timestamp` | Optional | UTC end time of the contact when known |
| `band` | Required | Frequency band (see `Band` enum) |
| `mode` | Required | Operating mode (see `Mode` enum) |
| `frequency_hz` | Optional | Precise frequency in Hz (for example, 28075730 for 28.07573 MHz) |
| `submode` | Optional | ADIF submode string (for example, `"USB"`, `"PSK31"`) |
| `rst_sent` / `rst_received` | Optional | RST signal reports |
| `sync_status` | Set by engine | `LOCAL_ONLY → SYNCED → MODIFIED → CONFLICT` |
| `station_snapshot` | Optional | Immutable local-station metadata captured when the QSO was logged |
| `extra_fields` | Optional | ADIF fields with no dedicated proto field - preserved for lossless round-trip |

## QsoSortOrder Values

| Value | Description |
|---|---|
| `QSO_SORT_ORDER_NEWEST_FIRST` | Default (zero value) - most recent QSOs first |
| `QSO_SORT_ORDER_OLDEST_FIRST` | Oldest QSOs first |
