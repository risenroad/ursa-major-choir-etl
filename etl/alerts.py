"""Telegram alerts: active choristers with 3+ consecutive misses in the lookback window."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, List

import requests

DASHBOARD_URL = (
    "https://lookerstudio.google.com/reporting/0a0ee2af-cb64-42ca-bc16-c312a3d27f1e"
    "/page/p_v5isym300d/edit?s=hs0hEMSZUdg"
)


def _flag(val: Any) -> int:
    """Normalize flag from mart (0/1, bool, string) to 0 or 1."""
    if val is None:
        return 0
    if isinstance(val, bool):
        return 1 if val else 0
    if isinstance(val, int):
        return 1 if val != 0 else 0
    if isinstance(val, float):
        return 1 if val != 0 else 0
    s = str(val).strip().upper()
    if s in ("1", "TRUE", "YES"):
        return 1
    return 0


def _date_iso(val: Any) -> str:
    """Return rehearsal_date as YYYY-MM-DD string for comparison."""
    if val is None or val == "":
        return ""
    if isinstance(val, str):
        return val.strip()[:10] if val.strip() else ""
    return str(val)[:10]


def _normalize_available_rows(mart_attendance_rows: List[dict]) -> List[dict]:
    """Filter to available_flag=1 and normalize to dicts with rehearsal_date, chorister_id, full_name, voice_part, tgid, missed_flag, attended_flag, is_active."""
    rows = []
    for r in mart_attendance_rows:
        if _flag(r.get("available_flag")) != 1:
            continue
        date_iso = _date_iso(r.get("rehearsal_date"))
        if not date_iso:
            continue
        rows.append({
            "rehearsal_date": date_iso,
            "chorister_id": str(r.get("chorister_id") or "").strip(),
            "full_name": str(r.get("full_name") or "").strip(),
            "voice_part": str(r.get("voice_part") or "").strip(),
            "tgid": str(r.get("tgid") or "").strip(),
            "missed_flag": _flag(r.get("missed_flag")),
            "attended_flag": _flag(r.get("attended_flag")),
            "is_active": _flag(r.get("is_active")) == 1,
        })
    return rows


def _get_window_dates(rows: List[dict], lookback_weeks: int) -> List[str] | None:
    """Return list of rehearsal dates in the lookback window (last lookback_weeks from latest date), or None if no data."""
    if not rows:
        return None
    all_dates = sorted(set(r["rehearsal_date"] for r in rows))
    if not all_dates:
        return None
    end_date = datetime.strptime(all_dates[-1], "%Y-%m-%d").date()
    start_date = end_date - timedelta(weeks=lookback_weeks)
    window_dates = [d for d in all_dates if start_date <= datetime.strptime(d, "%Y-%m-%d").date()]
    if not window_dates:
        return None
    return window_dates


def compute_current_missed_streak(
    mart_attendance_rows: List[dict],
    lookback_weeks: int,
    streak_threshold: int,
) -> List[dict]:
    """Find active choristers whose current consecutive miss streak is >= streak_threshold.

    - Only rows with available_flag=1 are considered (rehearsal_date >= joined_date).
    - Rehearsal dates are taken from the data; window = last lookback_weeks from the latest date.
    - For each chorister, streak is counted from the end of sorted dates (newest first):
      count consecutive missed_flag==1; stop at first attended_flag==1 or missed_flag==0.
    - Only choristers with is_active (on the most recent rehearsal in window) are included.

    Returns list of dicts: full_name, voice_part, streak_len, missed_dates (up to 10), chorister_id.
    """
    rows = _normalize_available_rows(mart_attendance_rows)
    if not rows:
        return []

    window_dates = _get_window_dates(rows, lookback_weeks)
    if not window_dates:
        return []

    # Последняя дата посещения по каждому хористу (по всем доступным репетициям)
    last_attended_by: dict[str, str] = {}
    for r in rows:
        if r["attended_flag"] != 1:
            continue
        cid = r["chorister_id"]
        d = r["rehearsal_date"]
        if cid not in last_attended_by or d > last_attended_by[cid]:
            last_attended_by[cid] = d

    # Per chorister: rows in window, sorted by date descending (newest first)
    by_chorister: dict[str, List[dict]] = {}
    for r in rows:
        if r["rehearsal_date"] not in window_dates:
            continue
        cid = r["chorister_id"]
        if cid not in by_chorister:
            by_chorister[cid] = []
        by_chorister[cid].append(r)

    result = []
    for chorister_id, crows in by_chorister.items():
        crows_sorted = sorted(crows, key=lambda x: x["rehearsal_date"], reverse=True)
        # is_active from the most recent rehearsal row
        is_active = crows_sorted[0].get("is_active", False)
        if not is_active:
            continue

        streak_len = 0
        missed_dates: List[str] = []
        for row in crows_sorted:
            if row["attended_flag"] == 1 or row["missed_flag"] == 0:
                break
            streak_len += 1
            missed_dates.append(row["rehearsal_date"])

        if streak_len < streak_threshold:
            continue

        # voice_part, full_name, tgid from the most recent date (first in crows_sorted)
        tgid_raw = crows_sorted[0].get("tgid") or ""
        result.append({
            "chorister_id": chorister_id,
            "full_name": crows_sorted[0]["full_name"] or "—",
            "voice_part": crows_sorted[0]["voice_part"] or "—",
            "tgid": tgid_raw,
            "last_attended_date": last_attended_by.get(chorister_id),
            "streak_len": streak_len,
            "missed_dates": missed_dates[:10],
        })

    return result


def compute_attendance_rate(
    mart_attendance_rows: List[dict],
    lookback_weeks: int,
) -> float | None:
    """Доходимость до репетиций: сумма посещённых (attended) / сумма доступных (available) в окне.

    Окно = последние lookback_weeks от максимальной даты в данных. Учитываются только
    строки с available_flag=1. Возвращает долю 0..1 или None, если нет данных.
    """
    rows = _normalize_available_rows(mart_attendance_rows)
    if not rows:
        return None
    window_dates = _get_window_dates(rows, lookback_weeks)
    if not window_dates:
        return None
    in_window = [r for r in rows if r["rehearsal_date"] in window_dates]
    total = len(in_window)
    if total == 0:
        return None
    attended = sum(r["attended_flag"] for r in in_window)
    return attended / total


def format_alert_message(
    violators: List[dict],
    lookback_weeks: int,
    streak_threshold: int = 3,
    attendance_rate: float | None = None,
) -> str:
    """Format Telegram message: сначала доходимость, затем пустая строка, затем список хористов (или успех)."""
    parts = []
    if attendance_rate is not None:
        pct = round(attendance_rate * 100)
        if pct < 50:
            icon = "🔴"
        elif pct <= 65:
            icon = "🟡"
        else:
            icon = "🟢"
        parts.append(f"{icon} Доходимость до репетиций (за последние {lookback_weeks} недели): {pct}%")
        parts.append("")  # отступ 1 строка
    if not violators:
        parts.append(
            f"✅ Нет хористов с пропусками {streak_threshold}+ подряд (за последние {lookback_weeks} недели)."
        )
    else:
        parts.append(
            f"⚠️ Хористы с {streak_threshold}+ пропусками подряд (за последние {lookback_weeks} недели):"
        )
        parts.append("")
        for i, v in enumerate(violators, 1):
            name = v.get("full_name", "—")
            part = v.get("voice_part", "—")
            streak = v.get("streak_len", 0)
            last_att = v.get("last_attended_date") or "—"
            tgid = (v.get("tgid") or "").strip().lstrip("@")
            tg_mention = f"@{tgid}" if tgid else "—"
            parts.append(f"{i}. {name} ({part}) — пропусков: {streak}, последняя явка: {last_att}, {tg_mention}")
    parts.append("")
    parts.append(f'<a href="{DASHBOARD_URL}">Ссылка на дашборд</a>')
    return "\n".join(parts)


def send_telegram_message(token: str, chat_id: str, text: str) -> None:
    """Send text to Telegram chat via Bot API. Raises on HTTP or API error."""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    resp = requests.post(url, json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram API error: {data}")
