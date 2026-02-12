from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Sequence


DIM_CHORISTER_HEADER = [
    "chorister_id",
    "tgid",
    "full_name",
    "joined_date",
    "created_at",
    "updated_at",
]


def _index_by_name(header_row: Sequence[str]) -> Dict[str, int]:
    """Build a mapping from column name to index."""
    return {name: idx for idx, name in enumerate(header_row)}


def _get_safe(row: Sequence[Any], idx: int) -> str:
    if idx < 0 or idx >= len(row):
        return ""
    value = row[idx]
    return "" if value is None else str(value).strip()


def _normalize_name(full_name: str) -> str:
    """Normalize name for ID generation (see data_contract.md)."""
    name = full_name.strip().lower()
    name = re.sub(r"\s+", "_", name)
    # Keep word characters (including Unicode letters) and underscore.
    name = re.sub(r"[^\w_]+", "", name, flags=re.UNICODE)
    return name


def _make_chorister_id(full_name: str, joined_date: str, tgid: str) -> str:
    if tgid:
        return f"tgid:{tgid}"
    normalized = _normalize_name(full_name)
    return f"name_joined:{normalized}:{joined_date}"


def build_dim_chorister_from_raw(values: List[List[Any]]) -> List[List[Any]]:
    """Transform RAW sheet `raw_input` values into dim_chorister rows.

    Expects the RAW header to contain at least: Tag, Joined, tgid, Who.
    Skips song rows (Tag == 'Song') and empty Tag rows.
    """
    if not values:
        return [DIM_CHORISTER_HEADER]

    header = values[0]
    index = _index_by_name(header)

    tag_idx = index.get("Tag")
    joined_idx = index.get("Joined")
    tgid_idx = index.get("tgid")
    who_idx = index.get("Who")

    if tag_idx is None or joined_idx is None or who_idx is None:
        # RAW schema is not what we expect; return only header to avoid
        # writing misleading data.
        return [DIM_CHORISTER_HEADER]

    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows: List[List[Any]] = []

    for row in values[1:]:
        tag = _get_safe(row, tag_idx)
        if not tag:
            continue
        if tag == "Song":
            # Song rows are handled in dim_song / fact_song_time later.
            continue

        full_name = _get_safe(row, who_idx)
        if not full_name:
            continue

        joined_date = _get_safe(row, joined_idx)
        tgid = _get_safe(row, tgid_idx) if tgid_idx is not None else ""

        chorister_id = _make_chorister_id(full_name=full_name, joined_date=joined_date, tgid=tgid)
        rows.append(
            [
                chorister_id,
                tgid,
                full_name,
                joined_date,
                now_iso,
                now_iso,
            ]
        )

    return [DIM_CHORISTER_HEADER, *rows]

