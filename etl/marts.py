"""Build BI marts from dim and fact tables (read from DB spreadsheet)."""
from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Any, List


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
    # Already ISO-like
    if re.match(r"^\d{4}-\d{2}-\d{2}", s):
        return s[:10]
    # dd.mm.yy or d.m.yy
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


def _safe_float(val: Any, default: float = 0.0) -> float:
    if val is None or val == "":
        return default
    if isinstance(val, (int, float)):
        return float(val)
    try:
        return float(str(val).replace(",", "."))
    except ValueError:
        return default


def _safe_str(val: Any) -> str:
    if val is None:
        return ""
    return str(val).strip()


def _get_voice_part_for_date(
    chorister_id: str,
    rehearsal_date_iso: str,
    assignments: List[dict],
) -> str:
    """Return voice_part for chorister on date. valid_from <= date and (valid_to is null or date <= valid_to). If multiple, pick assignment with max valid_from."""
    if not rehearsal_date_iso:
        return ""
    candidates = []
    for a in assignments:
        if _safe_str(a.get("chorister_id")) != chorister_id:
            continue
        vf = _normalize_date_to_iso(a.get("valid_from"))
        vt = _safe_str(a.get("valid_to") or "")
        if not vf:
            continue
        if rehearsal_date_iso < vf:
            continue
        if vt and _normalize_date_to_iso(vt) and rehearsal_date_iso > _normalize_date_to_iso(vt):
            continue
        candidates.append((vf, _safe_str(a.get("voice_part"))))
    if not candidates:
        return ""
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


# --- mart_attendance ----------------------------------------------------------

MART_ATTENDANCE_HEADER = [
    "rehearsal_date",
    "chorister_id",
    "full_name",
    "joined_date",
    "voice_part",
    "hours_attended",
    "attended_flag",
    "missed_flag",
    "available_flag",
]


def _joined_date_iso_for_available(ch: dict, chorister_id: str) -> str:
    """Return joined_date as ISO; empty if missing/invalid. Raise if value present but unparseable."""
    raw = ch.get("joined_date")
    s = _safe_str(raw)
    if not s:
        return ""
    iso = _normalize_date_to_iso(raw)
    if not iso and s:
        raise RuntimeError(
            f"Invalid joined_date for chorister_id={chorister_id!r}: {raw!r} (cannot normalize to YYYY-MM-DD)."
        )
    return iso


def build_mart_attendance(
    dim_chorister: List[dict],
    dim_chorister_assignment: List[dict],
    fact_attendance: List[dict],
) -> tuple[List[str], List[List[Any]]]:
    """Build mart_attendance: one row per chorister_id + rehearsal_date. Returns (header, rows).

    missed_flag from fact_attendance; available_flag = 1 if rehearsal_date >= joined_date else 0.
    """
    chorister_by_id: dict[str, dict] = {_safe_str(r.get("chorister_id")): r for r in dim_chorister if _safe_str(r.get("chorister_id"))}

    rows: List[List[Any]] = []
    for fa in fact_attendance:
        rehearsal_date_raw = fa.get("rehearsal_date")
        rehearsal_date_iso = _normalize_date_to_iso(rehearsal_date_raw)
        if not rehearsal_date_iso:
            rehearsal_date_iso = _safe_str(rehearsal_date_raw)
        chorister_id = _safe_str(fa.get("chorister_id"))
        hours = _safe_float(fa.get("hours_attended"))
        missed_flag_val = _safe_float(fa.get("missed_flag"), 0.0)
        missed_flag = 1 if missed_flag_val != 0 else 0

        ch = chorister_by_id.get(chorister_id) or {}
        full_name = _safe_str(ch.get("full_name"))
        joined_date_raw = ch.get("joined_date")
        joined_date_iso = _joined_date_iso_for_available(ch, chorister_id)
        joined_date_display = _normalize_date_to_iso(joined_date_raw) or _safe_str(joined_date_raw)

        voice_part = _get_voice_part_for_date(chorister_id, rehearsal_date_iso, dim_chorister_assignment)
        attended_flag = 1 if hours > 0 else 0
        available_flag = 1 if (joined_date_iso and rehearsal_date_iso >= joined_date_iso) else 0

        rows.append([
            rehearsal_date_iso,
            chorister_id,
            full_name,
            joined_date_display,
            voice_part,
            hours,
            attended_flag,
            missed_flag,
            available_flag,
        ])
    return (MART_ATTENDANCE_HEADER, rows)


# --- mart_song_rehearsal -------------------------------------------------------

MART_SONG_REHEARSAL_HEADER = [
    "rehearsal_date",
    "song_id",
    "song_name",
    "minutes_spent",
    "hours_spent",
]


def build_mart_song_rehearsal(
    dim_song: List[dict],
    fact_song_time: List[dict],
) -> tuple[List[str], List[List[Any]]]:
    """Build mart_song_rehearsal: one row per song_id + rehearsal_date. Returns (header, rows)."""
    song_by_id: dict[str, dict] = {_safe_str(r.get("song_id")): r for r in dim_song if _safe_str(r.get("song_id"))}

    rows = []
    for fs in fact_song_time:
        rehearsal_date_raw = fs.get("rehearsal_date")
        rehearsal_date_iso = _normalize_date_to_iso(rehearsal_date_raw)
        if not rehearsal_date_iso:
            rehearsal_date_iso = _safe_str(rehearsal_date_raw)
        song_id = _safe_str(fs.get("song_id"))
        minutes = _safe_float(fs.get("minutes_spent"))
        hours = minutes / 60.0
        song = song_by_id.get(song_id) or {}
        song_name = _safe_str(song.get("song_name"))
        rows.append([
            rehearsal_date_iso,
            song_id,
            song_name,
            minutes,
            hours,
        ])
    return (MART_SONG_REHEARSAL_HEADER, rows)


# --- mart_chorister_song -------------------------------------------------------

MART_CHORISTER_SONG_HEADER = [
    "rehearsal_date",
    "chorister_id",
    "full_name",
    "joined_date",
    "voice_part",
    "song_id",
    "song_name",
    "minutes_spent",
    "hours_spent",
]


def build_mart_chorister_song(
    dim_chorister: List[dict],
    dim_chorister_assignment: List[dict],
    dim_song: List[dict],
    fact_attendance: List[dict],
    fact_song_time: List[dict],
) -> tuple[List[str], List[List[Any]]]:
    """Build mart_chorister_song: one row per chorister + song + rehearsal_date when chorister attended and song was rehearsed. Returns (header, rows)."""
    chorister_by_id = {_safe_str(r.get("chorister_id")): r for r in dim_chorister if _safe_str(r.get("chorister_id"))}
    song_by_id = {_safe_str(r.get("song_id")): r for r in dim_song if _safe_str(r.get("song_id"))}

    # By date: set of chorister_ids who attended (hours > 0)
    attending_by_date: dict[str, set[str]] = {}
    for fa in fact_attendance:
        d = _normalize_date_to_iso(fa.get("rehearsal_date")) or _safe_str(fa.get("rehearsal_date"))
        if not d:
            continue
        if _safe_float(fa.get("hours_attended")) <= 0:
            continue
        if d not in attending_by_date:
            attending_by_date[d] = set()
        attending_by_date[d].add(_safe_str(fa.get("chorister_id")))

    # By date: list of (song_id, minutes_spent)
    songs_by_date: dict[str, List[tuple[str, float]]] = {}
    for fs in fact_song_time:
        d = _normalize_date_to_iso(fs.get("rehearsal_date")) or _safe_str(fs.get("rehearsal_date"))
        if not d:
            continue
        song_id = _safe_str(fs.get("song_id"))
        minutes = _safe_float(fs.get("minutes_spent"))
        if d not in songs_by_date:
            songs_by_date[d] = []
        songs_by_date[d].append((song_id, minutes))

    rows = []
    for rehearsal_date_iso, chorister_ids in attending_by_date.items():
        songs = songs_by_date.get(rehearsal_date_iso, [])
        for chorister_id in chorister_ids:
            ch = chorister_by_id.get(chorister_id) or {}
            full_name = _safe_str(ch.get("full_name"))
            joined_date = _safe_str(ch.get("joined_date"))
            voice_part = _get_voice_part_for_date(chorister_id, rehearsal_date_iso, dim_chorister_assignment)
            for song_id, minutes in songs:
                song = song_by_id.get(song_id) or {}
                song_name = _safe_str(song.get("song_name"))
                hours = minutes / 60.0
                rows.append([
                    rehearsal_date_iso,
                    chorister_id,
                    full_name,
                    joined_date,
                    voice_part,
                    song_id,
                    song_name,
                    minutes,
                    hours,
                ])
    return (MART_CHORISTER_SONG_HEADER, rows)
