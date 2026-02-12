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


def ensure_sheet_exists(service: Any, spreadsheet_id: str, title: str) -> None:
    """Create a sheet (tab) with the given title if it does not exist."""
    sheets_api = service.spreadsheets()
    metadata = sheets_api.get(
        spreadsheetId=spreadsheet_id,
        fields="sheets.properties.title",
    ).execute()

    existing_titles = {
        sheet["properties"]["title"]
        for sheet in metadata.get("sheets", [])
        if "properties" in sheet and "title" in sheet["properties"]
    }
    if title in existing_titles:
        return

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
