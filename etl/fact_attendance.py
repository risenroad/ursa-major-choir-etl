from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Sequence, Tuple


FACT_ATTENDANCE_HEADER = [
    "rehearsal_date",
    "chorister_id",
    "hours_attended",
    "load_ts",
]

# Fixed columns in RAW: A=Tag, B=Joined, C=tgid, D=Who; date columns start at E (index 4).
DATE_COLUMNS_START_INDEX = 4


def _index_by_name(header_row: Sequence[str]) -> Dict[str, int]:
    return {name: idx for idx, name in enumerate(header_row)}


def _get_safe(row: Sequence[Any], idx: int) -> str:
    if idx < 0 or idx >= len(row):
        return ""
    value = row[idx]
    return "" if value is None else str(value).strip()


def _parse_hours(value: Any) -> float | None:
    """Return hours as float if cell has a number, else None."""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if not s:
        return None
    try:
        return float(s.replace(",", "."))
    except ValueError:
        return None


def build_fact_attendance_from_raw(
    values: List[List[Any]],
    chorister_id_by_key: Dict[Tuple[str, str], str],
) -> List[List[Any]]:
    """Unpivot RAW chorister rows × date columns into fact_attendance.

    One row per (chorister_id, rehearsal_date) where the cell has a number (hours).
    Skips empty cells. rehearsal_date is the column header (e.g. dd.mm.yy).
    """
    if not values:
        return [FACT_ATTENDANCE_HEADER]

    header = values[0]
    index = _index_by_name(header)
    tag_idx = index.get("Tag")
    joined_idx = index.get("Joined")
    who_idx = index.get("Who")

    if tag_idx is None or joined_idx is None or who_idx is None:
        return [FACT_ATTENDANCE_HEADER]

    # Date columns: from E onward, header = rehearsal_date string.
    date_columns: List[Tuple[int, str]] = []
    for idx in range(DATE_COLUMNS_START_INDEX, len(header)):
        date_str = _get_safe(header, idx)
        if date_str:
            date_columns.append((idx, date_str))

    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows: List[List[Any]] = []

    for row in values[1:]:
        tag = _get_safe(row, tag_idx)
        if not tag or tag == "Song":
            continue

        full_name = _get_safe(row, who_idx)
        if not full_name:
            continue

        joined_date = _get_safe(row, joined_idx)
        chorister_id = chorister_id_by_key.get((full_name, joined_date))
        if not chorister_id:
            continue

        for col_idx, rehearsal_date in date_columns:
            hours = _parse_hours(row[col_idx] if col_idx < len(row) else None)
            if hours is not None:
                rows.append([rehearsal_date, chorister_id, hours, now_iso])

    return [FACT_ATTENDANCE_HEADER, *rows]
