from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Sequence, Tuple


FACT_ATTENDANCE_HEADER = [
    "rehearsal_date",
    "chorister_id",
    "hours_attended",
    "missed_flag",
    "load_ts",
]

# Fixed columns in RAW: A=Tag, B=Joined, C=tgid, D=Who; date columns start at E (index 4).
DATE_COLUMNS_START_INDEX = 4


def _normalize_date_to_iso(val: Any) -> str:
    """Return date as YYYY-MM-DD string, or empty string if unparseable."""
    if val is None or val == "":
        return ""
    if isinstance(val, (int, float)):
        try:
            d = datetime(1899, 12, 30) + timedelta(days=int(float(val)))
            return d.strftime("%Y-%m-%d")
        except (ValueError, OverflowError):
            return ""
    s = str(val).strip()
    if not s:
        return ""
    if re.match(r"^\d{4}-\d{2}-\d{2}", s):
        return s[:10]
    m = re.match(r"^(\d{1,2})\.(\d{1,2})\.(\d{2,4})$", s)
    if m:
        day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if year < 100:
            year += 2000 if year < 50 else 1900
        try:
            d = datetime(year, month, day)
            return d.strftime("%Y-%m-%d")
        except ValueError:
            return ""
    return ""


def _index_by_name(header_row: Sequence[str]) -> Dict[str, int]:
    return {name: idx for idx, name in enumerate(header_row)}


def _get_safe(row: Sequence[Any], idx: int) -> str:
    if idx < 0 or idx >= len(row):
        return ""
    value = row[idx]
    return "" if value is None else str(value).strip()


def _parse_hours_strict(value: Any, chorister_id: str, rehearsal_date: str) -> float:
    """Parse hours; support 2,5 -> 2.5. Raise RuntimeError with context if unparseable."""
    if value is None or value == "":
        raise RuntimeError(
            f"Expected numeric hours for chorister_id={chorister_id!r}, "
            f"rehearsal_date={rehearsal_date!r}: got empty value (use missed_flag=1 row instead)."
        )
    if isinstance(value, (int, float)):
        v = float(value)
        if v < 0:
            raise RuntimeError(
                f"hours_attended must be >= 0 for chorister_id={chorister_id!r}, "
                f"rehearsal_date={rehearsal_date!r}: raw_value={value!r}"
            )
        return v
    s = str(value).strip()
    if not s:
        raise RuntimeError(
            f"Expected numeric hours for chorister_id={chorister_id!r}, "
            f"rehearsal_date={rehearsal_date!r}: raw_value={value!r}"
        )
    try:
        v = float(s.replace(",", "."))
        if v < 0:
            raise RuntimeError(
                f"hours_attended must be >= 0 for chorister_id={chorister_id!r}, "
                f"rehearsal_date={rehearsal_date!r}: raw_value={value!r}"
            )
        return v
    except ValueError as e:
        raise RuntimeError(
            f"Cannot parse hours_attended for chorister_id={chorister_id!r}, "
            f"rehearsal_date={rehearsal_date!r}, raw_value={value!r}: {e}"
        ) from e


def build_fact_attendance_from_raw(
    values: List[List[Any]],
    chorister_id_by_key: Dict[Tuple[str, str], str],
) -> List[List[Any]]:
    """Unpivot RAW chorister rows × date columns into fact_attendance.

    One row per (chorister_id, rehearsal_date) for every chorister and every date column.
    Empty cell -> hours_attended=0, missed_flag=1. Non-empty -> parse hours, missed_flag=0; invalid value -> RuntimeError.
    rehearsal_date is normalized to YYYY-MM-DD. Duplicate dates in headers after normalization -> RuntimeError.
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

    # Date columns: from E onward, normalize header to ISO; fail on duplicate dates.
    date_columns: List[Tuple[int, str]] = []
    seen_iso: Dict[str, int] = {}
    for idx in range(DATE_COLUMNS_START_INDEX, len(header)):
        date_str = _get_safe(header, idx)
        if not date_str:
            continue
        iso = _normalize_date_to_iso(date_str)
        if not iso:
            continue
        if iso in seen_iso:
            raise RuntimeError(
                f"Duplicate rehearsal_date after normalization: {iso!r} "
                f"(column indices {seen_iso[iso]} and {idx}, raw headers {date_str!r})"
            )
        seen_iso[iso] = idx
        date_columns.append((idx, iso))

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

        for col_idx, rehearsal_date_iso in date_columns:
            raw_val = row[col_idx] if col_idx < len(row) else None
            is_empty = raw_val is None or (isinstance(raw_val, str) and not str(raw_val).strip())
            if is_empty:
                rows.append([rehearsal_date_iso, chorister_id, 0.0, 1, now_iso])
            else:
                hours = _parse_hours_strict(raw_val, chorister_id, rehearsal_date_iso)
                rows.append([rehearsal_date_iso, chorister_id, hours, 0, now_iso])

    return [FACT_ATTENDANCE_HEADER, *rows]
