from __future__ import annotations

import json
import os
from typing import Any, Dict

from dotenv import load_dotenv

from etl.dim_chorister import (
    build_dim_chorister_assignment_from_raw,
    build_dim_chorister_from_raw,
)
from etl.dim_song import build_dim_song_from_raw
from etl.fact_attendance import build_fact_attendance_from_raw
from etl.fact_song_time import build_fact_song_time_from_raw
from etl.gsheets import (
    append_rows,
    build_sheets_service,
    get_sheet_titles,
    ensure_sheet_exists,
    get_values,
    overwrite_range,
    read_table,
    write_table_overwrite,
)
from etl.marts import (
    build_mart_attendance,
    build_mart_chorister_song,
    build_mart_song_rehearsal,
)


def _load_service_account_info() -> Dict[str, Any]:
    """Load service account credentials info from environment.

    Preferred option: GOOGLE_SERVICE_ACCOUNT_FILE points to a local JSON key file.
    Fallback: GOOGLE_SERVICE_ACCOUNT_JSON contains the JSON itself as a string.
    """
    sa_file = os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE")
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")

    if sa_file:
        with open(sa_file, "r", encoding="utf-8") as f:
            return json.load(f)

    if sa_json:
        return json.loads(sa_json)

    msg = (
        "Missing Google service account credentials. "
        "Set GOOGLE_SERVICE_ACCOUNT_FILE (recommended) or GOOGLE_SERVICE_ACCOUNT_JSON."
    )
    raise RuntimeError(msg)


REQUIRED_TABS_FOR_MARTS = [
    "dim_chorister",
    "dim_chorister_assignment",
    "dim_song",
    "fact_attendance",
    "fact_song_time",
]


def build_marts(service: Any, spreadsheet_id: str) -> None:
    """Read dim/fact tables from DB spreadsheet, build 3 marts, write them (full overwrite).

    Raises RuntimeError if any required source tab is missing.
    Empty facts produce marts with header and 0 rows.
    """
    titles = get_sheet_titles(service, spreadsheet_id)
    missing = [t for t in REQUIRED_TABS_FOR_MARTS if t not in titles]
    if missing:
        raise RuntimeError(
            f"Missing required worksheet(s) for marts: {', '.join(missing)}. "
            "Run full ETL first to create dim and fact tables."
        )

    dim_chorister = read_table(service, spreadsheet_id, "dim_chorister")
    dim_chorister_assignment = read_table(service, spreadsheet_id, "dim_chorister_assignment")
    dim_song = read_table(service, spreadsheet_id, "dim_song")
    fact_attendance = read_table(service, spreadsheet_id, "fact_attendance")
    fact_song_time = read_table(service, spreadsheet_id, "fact_song_time")

    header_a, rows_a = build_mart_attendance(
        dim_chorister, dim_chorister_assignment, fact_attendance
    )
    write_table_overwrite(service, spreadsheet_id, "mart_attendance", header_a, rows_a)

    header_s, rows_s = build_mart_song_rehearsal(dim_song, fact_song_time)
    write_table_overwrite(service, spreadsheet_id, "mart_song_rehearsal", header_s, rows_s)

    header_cs, rows_cs = build_mart_chorister_song(
        dim_chorister,
        dim_chorister_assignment,
        dim_song,
        fact_attendance,
        fact_song_time,
    )
    write_table_overwrite(service, spreadsheet_id, "mart_chorister_song", header_cs, rows_cs)


def main() -> None:
    """ETL entrypoint.

    - Loads environment variables from .env.
    - Connects to RAW and DB spreadsheets.
    - Writes a small test table to DB.members (for smoke-testing).
    - Builds dim_chorister, dim_chorister_assignment, dim_song from RAW and writes to DB.
    """
    load_dotenv()

    raw_spreadsheet_id = os.environ["RAW_SPREADSHEET_ID"]
    target_spreadsheet_id = os.environ["TARGET_SPREADSHEET_ID"]

    service_account_info = _load_service_account_info()
    service = build_sheets_service(service_account_info)

    status = "success"
    error_message = ""
    rows_dim_chorister = 0
    rows_dim_chorister_assignment = 0
    rows_dim_song = 0
    rows_fact_attendance = 0
    rows_fact_song_time = 0

    try:
        # Lightweight connectivity check to RAW (no data printed or logged).
        service.spreadsheets().get(
            spreadsheetId=raw_spreadsheet_id,
            fields="spreadsheetId",
        ).execute()

        # --- Smoke-test tab: members ---------------------------------------------
        ensure_sheet_exists(
            service=service,
            spreadsheet_id=target_spreadsheet_id,
            title="members",
        )

        members_header = ["member_id", "full_name", "is_active"]
        members_rows = [
            members_header,
            ["test_member_1", "Test Member", "TRUE"],
        ]
        overwrite_range(
            service=service,
            spreadsheet_id=target_spreadsheet_id,
            range_a1="members!A1:C2",
            rows=members_rows,
        )

        # --- dim_chorister -------------------------------------------------------
        raw_values = get_values(
            service=service,
            spreadsheet_id=raw_spreadsheet_id,
            range_a1="main!A:ZZ",
        )
        dim_chorister_rows, (chorister_id_by_key, normalized_to_chorister_id) = (
            build_dim_chorister_from_raw(raw_values)
        )
        rows_dim_chorister = max(len(dim_chorister_rows) - 1, 0)

        ensure_sheet_exists(
            service=service,
            spreadsheet_id=target_spreadsheet_id,
            title="dim_chorister",
        )
        overwrite_range(
            service=service,
            spreadsheet_id=target_spreadsheet_id,
            range_a1="dim_chorister!A1:F",
            rows=dim_chorister_rows,
        )

        # --- dim_chorister_assignment -------------------------------------------
        dim_assignment_rows = build_dim_chorister_assignment_from_raw(
            raw_values,
            chorister_id_by_key,
            normalized_to_chorister_id,
        )
        rows_dim_chorister_assignment = max(len(dim_assignment_rows) - 1, 0)

        ensure_sheet_exists(
            service=service,
            spreadsheet_id=target_spreadsheet_id,
            title="dim_chorister_assignment",
        )
        overwrite_range(
            service=service,
            spreadsheet_id=target_spreadsheet_id,
            range_a1="dim_chorister_assignment!A1:F",
            rows=dim_assignment_rows,
        )

        # --- dim_song -------------------------------------------------------------
        dim_song_rows, song_ids_ordered = build_dim_song_from_raw(raw_values)
        rows_dim_song = max(len(dim_song_rows) - 1, 0)

        ensure_sheet_exists(
            service=service,
            spreadsheet_id=target_spreadsheet_id,
            title="dim_song",
        )
        overwrite_range(
            service=service,
            spreadsheet_id=target_spreadsheet_id,
            range_a1="dim_song!A1:D",
            rows=dim_song_rows,
        )

        # --- fact_attendance ------------------------------------------------------
        fact_attendance_rows = build_fact_attendance_from_raw(
            raw_values,
            chorister_id_by_key,
        )
        rows_fact_attendance = max(len(fact_attendance_rows) - 1, 0)
        ensure_sheet_exists(
            service=service,
            spreadsheet_id=target_spreadsheet_id,
            title="fact_attendance",
        )
        overwrite_range(
            service=service,
            spreadsheet_id=target_spreadsheet_id,
            range_a1="fact_attendance!A1:E",
            rows=fact_attendance_rows,
        )

        # --- fact_song_time -------------------------------------------------------
        fact_song_time_rows = build_fact_song_time_from_raw(
            raw_values,
            song_ids_ordered,
        )
        rows_fact_song_time = max(len(fact_song_time_rows) - 1, 0)
        ensure_sheet_exists(
            service=service,
            spreadsheet_id=target_spreadsheet_id,
            title="fact_song_time",
        )
        overwrite_range(
            service=service,
            spreadsheet_id=target_spreadsheet_id,
            range_a1="fact_song_time!A1:D",
            rows=fact_song_time_rows,
        )

        # --- marts (from DB dim/fact tables) ------------------------------------
        build_marts(service, target_spreadsheet_id)

        print(
            "ETL finished: wrote test row to DB.members, "
            f"{rows_dim_chorister} dim_chorister, {rows_dim_chorister_assignment} dim_chorister_assignment, "
            f"{rows_dim_song} dim_song, {rows_fact_attendance} fact_attendance, {rows_fact_song_time} fact_song_time; "
            "marts: mart_attendance, mart_song_rehearsal, mart_chorister_song.",
        )
    except Exception as exc:  # noqa: BLE001
        status = "failed"
        # Store a short, non-secret error message.
        error_message = str(exc)[:500]
        print(f"ETL failed: {error_message}")

    # --- etl_log (append-only) ---------------------------------------------------
    ensure_sheet_exists(
        service=service,
        spreadsheet_id=target_spreadsheet_id,
        title="etl_log",
    )

    # Create header on first run if sheet is empty.
    existing = get_values(
        service=service,
        spreadsheet_id=target_spreadsheet_id,
        range_a1="etl_log!A1:A1",
    )
    rows_to_append = []
    if not existing:
        rows_to_append.append(
            [
                "run_ts",
                "status",
                "rows_dim_chorister",
                "rows_dim_chorister_assignment",
                "rows_dim_song",
                "rows_fact_attendance",
                "rows_fact_song_time",
                "error_message",
            ]
        )

    from datetime import datetime, timezone  # local import to avoid unused at module level

    run_ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows_to_append.append(
        [
            run_ts,
            status,
            rows_dim_chorister,
            rows_dim_chorister_assignment,
            rows_dim_song,
            rows_fact_attendance,
            rows_fact_song_time,
            error_message,
        ]
    )
    append_rows(
        service=service,
        spreadsheet_id=target_spreadsheet_id,
        range_a1="etl_log!A:H",
        rows=rows_to_append,
    )


if __name__ == "__main__":
    main()
