"""Tests for mart_attendance is_active with voice-part history overrides."""
from __future__ import annotations

from etl.dim_chorister import (
    build_dim_chorister_assignment_from_raw,
    build_dim_chorister_from_raw,
)
from etl.marts import build_mart_attendance


def _raw_row(tag: str, who: str = "Мария Дидуренко") -> list:
    return [tag, "16.06.24", "", who, "2", "", ""]


def _tables_from_raw(raw_values: list[list]) -> tuple[list[dict], list[dict]]:
    dim_rows, (by_key, norm_map) = build_dim_chorister_from_raw(raw_values)
    assign_rows = build_dim_chorister_assignment_from_raw(raw_values, by_key, norm_map)
    dim = [
        {
            "chorister_id": r[0],
            "tgid": r[1],
            "full_name": r[2],
            "joined_date": r[3],
        }
        for r in dim_rows[1:]
    ]
    assign = [
        {
            "assignment_id": r[0],
            "chorister_id": r[1],
            "voice_part": r[2],
            "is_active": r[3],
            "valid_from": r[4],
            "valid_to": r[5],
        }
        for r in assign_rows[1:]
    ]
    return dim, assign


def test_override_open_period_uses_raw_ex_tag_for_is_active() -> None:
    raw = [
        ["Tag", "Joined", "tgid", "Who", "16.06.24", "02.10.24", "01.11.24"],
        _raw_row("exAlto"),
    ]
    dim, assign = _tables_from_raw(raw)
    facts = [
        {
            "rehearsal_date": "2024-06-16",
            "chorister_id": "Мария Дидуренко",
            "hours_attended": 2,
            "missed_flag": 0,
        },
        {
            "rehearsal_date": "2024-11-01",
            "chorister_id": "Мария Дидуренко",
            "hours_attended": 0,
            "missed_flag": 1,
        },
    ]
    header, rows = build_mart_attendance(dim, assign, facts)
    by_date = {r[header.index("rehearsal_date")]: r for r in rows}

    assert by_date["2024-06-16"][header.index("is_active")] is True
    assert by_date["2024-06-16"][header.index("voice_part")] == "soprano"
    assert by_date["2024-11-01"][header.index("is_active")] is False
    assert by_date["2024-11-01"][header.index("voice_part")] == "alto"


def test_override_open_period_active_tag() -> None:
    raw = [
        ["Tag", "Joined", "tgid", "Who", "01.11.24"],
        _raw_row("Alto"),
    ]
    dim, assign = _tables_from_raw(raw)
    facts = [
        {
            "rehearsal_date": "2024-11-01",
            "chorister_id": "Мария Дидуренко",
            "hours_attended": 1,
            "missed_flag": 0,
        },
    ]
    header, rows = build_mart_attendance(dim, assign, facts)
    assert rows[0][header.index("is_active")] is True
    assert rows[0][header.index("voice_part")] == "alto"
