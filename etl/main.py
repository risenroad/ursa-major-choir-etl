from __future__ import annotations

import json
import os
from typing import Any, Dict

from dotenv import load_dotenv

from etl.dim_chorister import build_dim_chorister_from_raw
from etl.gsheets import build_sheets_service, ensure_sheet_exists, get_values, overwrite_range


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


def main() -> None:
    """ETL entrypoint.

    - Loads environment variables from .env.
    - Connects to RAW and DB spreadsheets.
    - Writes a small test table to DB.members (for smoke-testing).
    - Builds dim_chorister from RAW.raw_input and writes it to DB.dim_chorister.
    """
    load_dotenv()

    raw_spreadsheet_id = os.environ["RAW_SPREADSHEET_ID"]
    target_spreadsheet_id = os.environ["TARGET_SPREADSHEET_ID"]

    service_account_info = _load_service_account_info()
    service = build_sheets_service(service_account_info)

    # Lightweight connectivity check to RAW (no data printed or logged).
    service.spreadsheets().get(
        spreadsheetId=raw_spreadsheet_id,
        fields="spreadsheetId",
    ).execute()

    # --- Smoke-test tab: members -------------------------------------------------
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

    # --- dim_chorister -----------------------------------------------------------
    raw_values = get_values(
        service=service,
        spreadsheet_id=raw_spreadsheet_id,
        range_a1="main!A:ZZ",
    )
    dim_chorister_rows = build_dim_chorister_from_raw(raw_values)

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

    print(
        "ETL finished: wrote test row to DB.members and "
        f"{len(dim_chorister_rows) - 1} rows to DB.dim_chorister.",
    )


if __name__ == "__main__":
    main()
