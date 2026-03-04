"""Tests for alert logic (compute_current_missed_streak, format_alert_message). No Telegram calls."""
from __future__ import annotations

import unittest
from datetime import datetime, timedelta

# Run from repo root: PYTHONPATH=. python -m unittest tests.test_alerts
from etl.alerts import (
    compute_attendance_rate,
    compute_current_missed_streak,
    format_alert_message,
)


def _mart_row(
    rehearsal_date: str,
    chorister_id: str,
    full_name: str,
    voice_part: str,
    missed_flag: int,
    attended_flag: int,
    available_flag: int = 1,
    is_active: int = 1,
    tgid: str = "",
) -> dict:
    return {
        "rehearsal_date": rehearsal_date,
        "chorister_id": chorister_id,
        "full_name": full_name,
        "voice_part": voice_part,
        "tgid": tgid,
        "missed_flag": missed_flag,
        "attended_flag": attended_flag,
        "available_flag": available_flag,
        "is_active": is_active,
    }


class TestComputeCurrentMissedStreak(unittest.TestCase):
    def test_empty_mart_returns_empty(self) -> None:
        self.assertEqual(compute_current_missed_streak([], 12, 3), [])

    def test_three_or_more_misses_at_end_active_included(self) -> None:
        """3+ пропуска подряд до последней репетиции → попадает."""
        base = (datetime.now() - timedelta(days=7 * 4)).strftime("%Y-%m-%d")
        rows = []
        for i in range(5):
            d = (datetime.strptime(base, "%Y-%m-%d") + timedelta(weeks=i)).strftime("%Y-%m-%d")
            rows.append(_mart_row(d, "c1", "Иван", "Bass", 1, 0, 1, 1))
        out = compute_current_missed_streak(rows, lookback_weeks=12, streak_threshold=3)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["full_name"], "Иван")
        self.assertEqual(out[0]["streak_len"], 5)
        self.assertEqual(len(out[0]["missed_dates"]), 5)

    def test_attended_last_rehearsal_not_included(self) -> None:
        """Если на последней репетиции был → не попадает."""
        base = (datetime.now() - timedelta(days=7 * 4)).strftime("%Y-%m-%d")
        rows = []
        for i in range(5):
            d = (datetime.strptime(base, "%Y-%m-%d") + timedelta(weeks=i)).strftime("%Y-%m-%d")
            missed = 1 if i < 4 else 0
            attended = 0 if i < 4 else 1
            rows.append(_mart_row(d, "c1", "Иван", "Bass", missed, attended, 1, 1))
        out = compute_current_missed_streak(rows, lookback_weeks=12, streak_threshold=3)
        self.assertEqual(len(out), 0)

    def test_inactive_chorister_not_included(self) -> None:
        """Неактивный хорист не попадает в алерт."""
        base = (datetime.now() - timedelta(days=7 * 2)).strftime("%Y-%m-%d")
        rows = [
            _mart_row(base, "c1", "Иван", "Bass", 1, 0, 1, 0),  # is_active=0
            _mart_row(
                (datetime.strptime(base, "%Y-%m-%d") + timedelta(weeks=1)).strftime("%Y-%m-%d"),
                "c1", "Иван", "Bass", 1, 0, 1, 0,
            ),
            _mart_row(
                (datetime.strptime(base, "%Y-%m-%d") + timedelta(weeks=2)).strftime("%Y-%m-%d"),
                "c1", "Иван", "Bass", 1, 0, 1, 0,
            ),
            _mart_row(
                (datetime.strptime(base, "%Y-%m-%d") + timedelta(weeks=3)).strftime("%Y-%m-%d"),
                "c1", "Иван", "Bass", 1, 0, 1, 0,
            ),
        ]
        out = compute_current_missed_streak(rows, lookback_weeks=12, streak_threshold=3)
        self.assertEqual(len(out), 0)

    def test_available_flag_zero_ignored(self) -> None:
        """Репетиции до joined_date (available_flag=0) не считаются."""
        base = (datetime.now() - timedelta(days=7 * 2)).strftime("%Y-%m-%d")
        rows = []
        for i in range(6):
            d = (datetime.strptime(base, "%Y-%m-%d") + timedelta(weeks=i)).strftime("%Y-%m-%d")
            avail = 0 if i < 2 else 1  # first 2 dates "before joined"
            rows.append(_mart_row(d, "c1", "Иван", "Bass", 1, 0, avail, 1))
        out = compute_current_missed_streak(rows, lookback_weeks=12, streak_threshold=3)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["streak_len"], 4)

    def test_missed_dates_capped_at_10(self) -> None:
        base = (datetime.now() - timedelta(days=7 * 12)).strftime("%Y-%m-%d")
        rows = []
        for i in range(12):
            d = (datetime.strptime(base, "%Y-%m-%d") + timedelta(weeks=i)).strftime("%Y-%m-%d")
            rows.append(_mart_row(d, "c1", "Иван", "Bass", 1, 0, 1, 1))
        out = compute_current_missed_streak(rows, lookback_weeks=12, streak_threshold=3)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["streak_len"], 12)
        self.assertEqual(len(out[0]["missed_dates"]), 10)


class TestComputeAttendanceRate(unittest.TestCase):
    def test_empty_returns_none(self) -> None:
        self.assertIsNone(compute_attendance_rate([], 5))

    def test_rate_in_window(self) -> None:
        """Доходимость = посещённые / доступные в окне."""
        base = (datetime.now() - timedelta(days=7 * 3)).strftime("%Y-%m-%d")
        rows = []
        for i in range(4):
            d = (datetime.strptime(base, "%Y-%m-%d") + timedelta(weeks=i)).strftime("%Y-%m-%d")
            for cid, attended in [("c1", 1), ("c2", 1), ("c3", 0)]:
                rows.append({
                    "rehearsal_date": d,
                    "chorister_id": cid,
                    "available_flag": 1,
                    "attended_flag": attended,
                })
        rate = compute_attendance_rate(rows, lookback_weeks=5)
        self.assertIsNotNone(rate)
        # 4 dates * 3 choristers = 12 slots; 4*2 + 4*1 = 12 attended? No: each date 2 attended, 1 missed -> 8 attended, 4 missed. So 8/12
        self.assertAlmostEqual(rate, 8 / 12)


class TestFormatAlertMessage(unittest.TestCase):
    def test_no_violators_success_message(self) -> None:
        msg = format_alert_message([], lookback_weeks=12, streak_threshold=3)
        self.assertIn("Нет хористов", msg)
        self.assertIn("3+ подряд", msg)
        self.assertIn("12 недел", msg)  # "недели" или "недель"

    def test_with_violators_numbered_list(self) -> None:
        violators = [
            {
                "full_name": "Иван",
                "voice_part": "Bass",
                "streak_len": 4,
                "missed_dates": [],
                "last_attended_date": "2025-02-01",
                "tgid": "ivan_choir",
            },
        ]
        msg = format_alert_message(violators, lookback_weeks=12, streak_threshold=3)
        self.assertIn("3+ пропусками", msg)
        self.assertIn("1. Иван (Bass)", msg)
        self.assertIn("пропусков: 4", msg)
        self.assertIn("последняя явка: 2025-02-01", msg)
        self.assertIn("@ivan_choir", msg)

    def test_attendance_rate_in_message(self) -> None:
        msg = format_alert_message([], lookback_weeks=5, streak_threshold=3, attendance_rate=0.85)
        self.assertIn("Доходимость до репетиций", msg)
        self.assertIn("85%", msg)
        self.assertIn("🟢", msg)  # 85% — зелёная иконка
        msg2 = format_alert_message(
            [{"full_name": "X", "voice_part": "Y", "streak_len": 3}],
            lookback_weeks=5,
            streak_threshold=3,
            attendance_rate=0.0,
        )
        self.assertIn("Доходимость до репетиций", msg2)
        self.assertIn("0%", msg2)
        self.assertIn("🔴", msg2)  # 0% — красная иконка
