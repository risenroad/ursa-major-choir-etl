from __future__ import annotations

from typing import Any, List, Sequence

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def build_sheets_service(service_account_info: dict) -> Any:
    """Build a Google Sheets API client from service account info."""
    creds = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


def append_rows(
    service: Any,
    spreadsheet_id: str,
    range_a1: str,
    rows: List[List[Any]],
) -> None:
    """Append rows to the given range (non-idempotent helper)."""
    body = {"values": rows}
    service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=range_a1,
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body=body,
    ).execute()


def get_sheet_titles(service: Any, spreadsheet_id: str) -> set[str]:
    """Return set of worksheet titles (tab names) in the spreadsheet."""
    metadata = (
        service.spreadsheets()
        .get(
            spreadsheetId=spreadsheet_id,
            fields="sheets.properties.title",
        )
        .execute()
    )
    return {
        sheet["properties"]["title"]
        for sheet in metadata.get("sheets", [])
        if "properties" in sheet and "title" in sheet["properties"]
    }


def ensure_sheet_exists(service: Any, spreadsheet_id: str, title: str) -> None:
    """Create a sheet (tab) with the given title if it does not exist."""
    existing_titles = get_sheet_titles(service, spreadsheet_id)
    if title in existing_titles:
        return
    sheets_api = service.spreadsheets()

    body = {
        "requests": [
            {
                "addSheet": {
                    "properties": {
                        "title": title,
                    }
                }
            }
        ]
    }
    sheets_api.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def overwrite_range(
    service: Any,
    spreadsheet_id: str,
    range_a1: str,
    rows: Sequence[Sequence[Any]],
) -> None:
    """Idempotently overwrite a rectangular range with the provided rows."""
    values = [list(row) for row in rows]
    body = {"values": values}

    values_api = service.spreadsheets().values()
    # Clear previous content in the range so repeated runs do not accumulate data.
    values_api.clear(
        spreadsheetId=spreadsheet_id,
        range=range_a1,
    ).execute()
    values_api.update(
        spreadsheetId=spreadsheet_id,
        range=range_a1,
        valueInputOption="USER_ENTERED",
        body=body,
    ).execute()


def get_values(
    service: Any,
    spreadsheet_id: str,
    range_a1: str,
) -> List[List[Any]]:
    """Read values from a given range. Returns [] if the range is empty."""
    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_a1)
        .execute()
    )
    return result.get("values", [])


def read_table(
    service: Any,
    spreadsheet_id: str,
    worksheet_name: str,
) -> List[dict]:
    """Read a worksheet as a table: first row = header, each other row = dict of column -> value.

    Returns list of dicts (one per data row). Empty sheet or only header -> [].
    Short rows are padded with None for missing columns.
    """
    values = get_values(
        service=service,
        spreadsheet_id=spreadsheet_id,
        range_a1=f"{worksheet_name}!A:ZZ",
    )
    if not values:
        return []
    header = [str(v).strip() if v is not None else "" for v in values[0]]
    result: List[dict] = []
    for row in values[1:]:
        d: dict = {}
        for i, key in enumerate(header):
            d[key] = row[i] if i < len(row) else None
        result.append(d)
    return result


def write_table_overwrite(
    service: Any,
    spreadsheet_id: str,
    worksheet_name: str,
    header: List[str],
    rows: List[List[Any]],
) -> None:
    """Idempotently overwrite a worksheet with a table: create tab if missing, clear, write header + rows."""
    ensure_sheet_exists(
        service=service,
        spreadsheet_id=spreadsheet_id,
        title=worksheet_name,
    )
    range_a1 = f"{worksheet_name}!A:ZZ"
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=range_a1,
    ).execute()
    all_rows: List[List[Any]] = [header] + rows
    if all_rows:
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{worksheet_name}!A1",
            valueInputOption="USER_ENTERED",
            body={"values": all_rows},
        ).execute()
