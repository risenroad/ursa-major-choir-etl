from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Sequence, Tuple


FACT_SONG_TIME_HEADER = [
    "rehearsal_date",
    "song_id",
    "minutes_spent",
    "load_ts",
]

DATE_COLUMNS_START_INDEX = 4


def _index_by_name(header_row: Sequence[str]) -> Dict[str, int]:
    return {name: idx for idx, name in enumerate(header_row)}


def _get_safe(row: Sequence[Any], idx: int) -> str:
    if idx < 0 or idx >= len(row):
        return ""
    value = row[idx]
    return "" if value is None else str(value).strip()


def _parse_minutes(value: Any) -> float | None:
    """Return minutes as float if cell has a number, else None."""
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


def build_fact_song_time_from_raw(
    values: List[List[Any]],
    song_ids_ordered: List[str],
) -> List[List[Any]]:
    """Unpivot RAW Song rows × date columns into fact_song_time.

    One row per (song_id, rehearsal_date) where the cell has a number (minutes).
    song_ids_ordered[i] = song_id for the i-th Song row in RAW (same order as dim_song).
    """
    if not values or not song_ids_ordered:
        return [FACT_SONG_TIME_HEADER]

    header = values[0]
    index = _index_by_name(header)
    tag_idx = index.get("Tag")
    who_idx = index.get("Who")

    if tag_idx is None or who_idx is None:
        return [FACT_SONG_TIME_HEADER]

    date_columns: List[Tuple[int, str]] = []
    for idx in range(DATE_COLUMNS_START_INDEX, len(header)):
        date_str = _get_safe(header, idx)
        if date_str:
            date_columns.append((idx, date_str))

    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows: List[List[Any]] = []
    song_index = 0

    for row in values[1:]:
        tag = _get_safe(row, tag_idx)
        if tag != "Song":
            continue

        if song_index >= len(song_ids_ordered):
            break
        song_id = song_ids_ordered[song_index]
        song_index += 1

        for col_idx, rehearsal_date in date_columns:
            minutes = _parse_minutes(row[col_idx] if col_idx < len(row) else None)
            if minutes is not None:
                rows.append([rehearsal_date, song_id, minutes, now_iso])

    return [FACT_SONG_TIME_HEADER, *rows]
