namespace QsoRipper.Cli;

internal static class CliHelpText
{
    public static string GetGeneralHelp()
    {
        return """
            QsoRipper CLI

            Usage: qsoripper-cli [options] <command> [arguments]

            Logbook:
              log <call> <band> <mode>         Log a QSO (e.g., log W1AW 20m FT8)
              get <local-id>                   Get a QSO by ID
              list [filters]                   List QSOs (--callsign, --band, --mode, --limit)
              enrich [--preview | --apply]     Backfill missing QSO data from QRZ XML
              update <local-id> [fields]       Update a QSO (--grid, --freq, --enrich, etc.)
              delete <local-id>                Soft-delete a QSO (recoverable via restore)
              restore <local-id>               Restore a soft-deleted QSO
              purge [options]                  Permanently delete soft-deleted QSOs

            ADIF:
              import <file>                    Import QSOs from an ADIF file
              export [--file out.adi]          Export QSOs to ADIF (stdout, file, or directory)

            Lookup:
              lookup <callsign>                Look up a callsign via QRZ
              stream-lookup <callsign>         Streaming lookup with progressive updates
              cache-check <callsign>           Check if a callsign is cached

            Engine:
              status                           Show sync status and QSO counts
              sync [--force]                   Sync with QRZ logbook (--force = full sync)
              sync-status                      Detailed sync status and scheduling info
              test-logbook [--api-key <key>]   Test QRZ logbook API key
              space-weather [--refresh]        Show current NOAA space weather snapshot
              contests active [filters]        Show active or upcoming contests
              contests refresh                 Refresh contest calendar cache
              config [--set KEY=VALUE]         View or modify runtime config
              setup [--status | --from-env]    Interactive setup wizard or headless config

            Rig Control:
              rig-status                       Show rig connection and current state
              cw <subcommand>                  Exercise engine-backed CW keying

            Options:
              --engine <profile>               Engine profile (for example: rust, dotnet, local-rust)
              --endpoint, -e <url>             Engine endpoint (default: rust=50051, dotnet=50052)
              --skip-cache                     Bypass cache for lookup commands
              --json                           Output as JSON (for piping to PowerShell)
              --help, -h                       Show this help
            """;
    }

    public static string GetCommandHelp(string command)
    {
        return command switch
        {
            "log" => """
                Usage: log <callsign> [band] [mode] [options]

                Log a new QSO. Auto-enriches with QRZ data (grid, country, etc.)

                  --from-rig           Auto-fill band, mode, and frequency from the rig
                  --band <band>        Override band (e.g., 20m)
                  --mode <mode>        Override mode (e.g., CW)
                  --station <call>     Your station callsign (if not set via setup)
                  --at <time>          QSO time (default: now). Relative (30.minutes) or absolute.
                  --rst-sent <rst>     RST sent (e.g., 59, 599)
                  --rst-rcvd <rst>     RST received
                  --freq <khz>         Frequency in kHz (e.g., 14074)
                  --comment <text>     Comment text
                  --notes <text>       Notes text
                  --no-enrich          Skip automatic QRZ lookup enrichment

                Without --from-rig, band and mode are required positional arguments.
                With --from-rig, omitted values are filled from the rig. Explicit values
                always take priority over rig-derived values.

                Examples:
                  log W1AW 20m FT8
                  log W1AW --from-rig
                  log W1AW 40m CW --from-rig --comment "Nice signal"
                  log K7ABV 20m SSB --at 30.minutes
                """,
            "get" => """
                Usage: get <local-id>

                Retrieve a QSO by its local ID (returned by the log command).
                """,
            "list" => """
                Usage: list [options]

                List QSOs with optional filters. By default shows only active (non-deleted) QSOs.

                  --callsign <call>    Filter by worked callsign
                  --band <band>        Filter by band (e.g., 20m)
                  --mode <mode>        Filter by mode (e.g., FT8)
                  --after <time>       QSOs after this time (e.g., 2.days, 3.hours, 2026-04-10)
                  --before <time>      QSOs before this time
                  --limit <n>          Max results (default: 20)
                  --show-id            Include the QSO local ID column
                  --show-rst           Include RST sent/received columns
                  --show-comment       Include comment/notes column (default)
                  --deleted            Show only soft-deleted QSOs (trash view)
                  --include-deleted    Show all QSOs including soft-deleted ones
                """,
            "update" => """
                Usage: update <local-id> [options]

                Update fields on an existing QSO.

                  --at <time>          Change timestamp. Relative (30.minutes) or absolute.
                  --grid <grid>        Set grid square (e.g., CN87)
                  --country <name>     Set country
                  --state <abbr>       Set state (e.g., WA)
                  --freq <khz>         Set frequency in kHz
                  --band <band>        Change band
                  --mode <mode>        Change mode
                  --rst-sent <rst>     Update RST sent
                  --rst-rcvd <rst>     Update RST received
                  --comment <text>     Set comment/notes
                  --enrich             Re-run QRZ lookup enrichment

                Examples:
                  update abc123 --grid CN87 --freq 14074
                  update abc123 --at "2026-04-12T01:51:00Z"
                  update abc123 --enrich
                """,
            "enrich" => """
                Usage: enrich [--preview | --apply] [options]

                Fill missing QSO enrichment fields from QRZ XML callsign data.
                Preview is the default. Existing local values are not changed or cleared.

                  --preview            Show the changes without storing them
                  --apply              Store the missing enrichment values
                  --after <utc>        Include QSOs at or after an ISO 8601 UTC time
                  --before <utc>       Include QSOs at or before an ISO 8601 UTC time

                Examples:
                  enrich
                  enrich --apply
                  enrich --preview --after 2026-08-01T00:00:00Z
                """,
            "delete" => """
                Usage: delete <local-id>

                Soft-delete a QSO by its local ID. The QSO is moved to trash and hidden from
                normal list output, but remains recoverable via the restore command until purged.
                Use --show-id on the list command to find local IDs.
                """,
            "cw" => """
                Usage: cw <subcommand> [options]

                Exercise engine-backed CW macro keying.

                CW speed must be 5 through 99 WPM. Hardware text transmission is disabled
                unless the engine starts with QSORIPPER_CW_TRANSMIT_ENABLED=true.

                  cw status
                  cw list
                  cw send <text> [--his-call <call>] [--rst <rst>] [--exchange <text>] [--nr <n>] [--speed <wpm>]
                  cw macro <name> [--his-call <call>] [--rst <rst>] [--exchange <text>] [--nr <n>] [--speed <wpm>]
                  cw speed <wpm>
                  cw abort

                Examples:
                  cw status
                  cw send "CQ TEST {MYCALL}"
                  cw macro exchange --his-call W1AW --exchange WA --nr 12
                """,
            "restore" => """
                Usage: restore <local-id>

                Restore a soft-deleted QSO by its local ID. Clears deleted_at and cancels any
                pending remote delete so the QSO reappears in normal list output.
                If the QRZ copy was already deleted, the QSO is re-queued for upload on the
                next sync. Use list --deleted --show-id to find local IDs of trashed QSOs.
                """,
            "purge" => """
                Usage: purge [options]

                Permanently remove soft-deleted QSOs from storage ("empty trash").
                This is non-recoverable. By default, prompts for confirmation.

                  --older-than <duration>    Only purge QSOs deleted before this duration
                                             (e.g., 7.days, 30.days, 1.hours)
                  --ids <id1,id2,...>         Only purge specific local IDs (comma-separated)
                  --include-pending-remote-deletes
                                             Include QSOs with pending QRZ remote deletes
                  --confirm                  Skip interactive confirmation prompt

                Examples:
                  purge                              Purge all deleted QSOs (with confirmation)
                  purge --older-than 30.days         Purge QSOs deleted more than 30 days ago
                  purge --ids abc123,def456          Purge specific QSOs
                  purge --confirm                    Skip confirmation prompt
                """,
            "import" => """
                Usage: import <file-path> [--refresh]

                Import QSOs from an ADIF (.adi) file.

                  --refresh            Update existing records instead of skipping duplicates.
                                       Non-empty import fields overwrite; fields absent from the
                                       import are preserved.
                """,
            "export" => """
                Usage: export [options]

                Export QSOs to ADIF format.

                  --file <path>        Write to file (default: stdout). If <path> is a
                                       directory, writes <logdbname>-yyyy-MM-dd.adi there.
                  --include-header     Include ADIF header
                  --after <time>       Only QSOs after this time. Relative (2.days, 3.hours)
                                       or absolute (2026-04-21, 2026-04-21T18:03:42Z).
                  --before <time>      Only QSOs before this time. Same formats as --after.
                  --contest <id>       Only QSOs tagged with this contest id (e.g., CQWWSSB).

                Examples:
                  export --file recent.adi --after 7.days
                  export --file cqww.adi --contest CQWWSSB
                  export --after 2026-04-21T18:03:42Z --file new-for-lotw.adi
                """,
            "lookup" => """
                Usage: lookup <callsign> [--skip-cache]

                Look up a callsign via QRZ.
                """,
            "stream-lookup" => """
                Usage: stream-lookup <callsign> [--skip-cache]

                Streaming lookup with progressive state updates.
                """,
            "cache-check" => """
                Usage: cache-check <callsign>

                Check if a callsign is in the engine's cache.
                """,
            "config" => """
                Usage: config [options]

                View or modify runtime configuration.

                  --set KEY=VALUE      Set a config value
                  --reset              Reset all overrides to defaults
                """,
            "setup" => """
                Usage: setup [--status] [--from-env]

                Interactive setup wizard for first-run configuration. Walks through
                persistence settings, station profile, and optional QRZ integration step by step.

                Modes:
                  (default)          Interactive wizard — prompts for each setting
                  --status           Show current setup status (read-only)
                  --from-env         Headless setup from environment variables (no prompts)

                Environment variables for --from-env:
                  QSORIPPER_LOG_FILE              Legacy alias for persistence.path
                  QSORIPPER_PERSISTENCE_<FIELD>   Persistence field values (for example PATH)
                  QSORIPPER_STATION_CALLSIGN      Station callsign (required)
                  QSORIPPER_OPERATOR_CALLSIGN     Operator callsign (defaults to station)
                  QSORIPPER_PROFILE_NAME          Profile name (defaults to "Default")
                  QSORIPPER_GRID                  Grid square
                  QSORIPPER_QRZ_XML_USERNAME      QRZ XML username (optional)
                  QSORIPPER_QRZ_XML_PASSWORD      QRZ XML password (optional)
                  QSORIPPER_WSJTX_INGEST_ENABLED  Enable WSJT-X QSO ingestion (optional)
                  QSORIPPER_WSJTX_INGEST_UDP_BIND WSJT-X UDP bind, e.g. 127.0.0.1:2237
                  QSORIPPER_WSJTX_INGEST_ADIF_TAIL_PATH
                                                   WSJT-X ADIF log path for tail recovery

                Examples:
                  setup                           Start the interactive wizard
                  setup --status                  Check if setup is complete
                  setup --from-env                Configure from environment variables
                """,
            "status" => """
                Usage: status

                Show engine identity, sync status, and QSO counts.
                """,
            "rig-status" => """
                Usage: rig-status

                Show rig connection status, endpoint, and current radio state
                (frequency, band, mode) when connected.
                """,
            "sync" => """
                Usage: sync [--force]

                Trigger a sync with the QRZ logbook. Shows streaming progress updates and
                a summary including downloaded, uploaded, conflict, and delete counters.

                  --force              Run a full sync instead of incremental
                """,
            "sync-status" => """
                Usage: sync-status

                Show detailed sync status: local/QRZ counts, pending uploads,
                auto-sync scheduling, and last error.
                """,
            "test-logbook" => """
                Usage: test-logbook [--api-key <key>]

                Test QRZ logbook API key by querying the logbook STATUS endpoint.
                Uses the configured key if --api-key is not provided.

                  --api-key <key>      API key to test (optional)
                """,
            "space-weather" => """
                Usage: space-weather [--refresh]

                Show the current engine-backed NOAA space weather snapshot.

                  --refresh          Force an immediate refresh before printing
                  --json             Output the snapshot as JSON
                """,
            "contests" => """
                Usage: contests active [--band 20m] [--mode CW] [--at <utc>] [--lookahead-hours N]
                       contests refresh

                Show engine-backed active contests from the contest calendar cache.
                Partial source matches are included by default because the initial
                WA7BNM RSS source does not publish band, mode, or exchange details.

                  --band <band>             Filter by band, such as 20m
                  --mode <mode>             Filter by mode, such as CW or SSB
                  --at <utc>                Query time, default engine current UTC
                  --lookahead-hours <n>     Include contests starting soon
                  --exact-matches           Exclude metadata-only partial matches
                  --include-partial         Include metadata-only partial matches
                  --refresh                 Force an immediate refresh before printing
                  --json                    Output the response as JSON
                """,
            _ => $"No help available for '{command}'."
        };
    }
}
