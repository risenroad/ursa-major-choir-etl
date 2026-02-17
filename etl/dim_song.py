from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Sequence, Tuple


DIM_SONG_HEADER = [
    "song_id",
    "song_name",
    "created_at",
    "updated_at",
]


def _index_by_name(header_row: Sequence[str]) -> Dict[str, int]:
    return {name: idx for idx, name in enumerate(header_row)}


def _get_safe(row: Sequence[Any], idx: int) -> str:
    if idx < 0 or idx >= len(row):
        return ""
    value = row[idx]
    return "" if value is None else str(value).strip()


def build_dim_song_from_raw(
    values: List[List[Any]],
) -> Tuple[List[List[Any]], List[str]]:
    """Build dim_song from RAW rows with Tag = Song; Who = song title.

    Returns (table_rows, song_ids_in_order) so fact_song_time can use same song_id per row.
    song_id = full title (readable); on duplicate add suffix " (2)", " (3)", etc.
    """
    if not values:
        return ([DIM_SONG_HEADER], [])

    header = values[0]
    index = _index_by_name(header)
    tag_idx = index.get("Tag")
    who_idx = index.get("Who")

    if tag_idx is None or who_idx is None:
        return ([DIM_SONG_HEADER], [])

    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows: List[List[Any]] = []
    song_ids_ordered: List[str] = []
    seen_titles: Dict[str, int] = {}

    for row in values[1:]:
        tag = _get_safe(row, tag_idx)
        if tag != "Song":
            continue

        song_name = _get_safe(row, who_idx)
        if not song_name:
            continue

        count = seen_titles.get(song_name, 0) + 1
        seen_titles[song_name] = count
        song_id = song_name if count == 1 else f"{song_name} ({count})"
        song_ids_ordered.append(song_id)
        rows.append([song_id, song_name, now_iso, now_iso])

    return ([DIM_SONG_HEADER, *rows], song_ids_ordered)
