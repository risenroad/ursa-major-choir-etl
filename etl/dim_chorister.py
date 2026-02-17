from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Sequence, Tuple


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


def _make_chorister_id(
    full_name: str,
    joined_date: str,
    seen_names: Dict[str, int],
) -> str:
    """Return readable chorister_id (full name); on duplicate add disambiguator."""
    count = seen_names.get(full_name, 0) + 1
    seen_names[full_name] = count
    if count == 1:
        return full_name
    return f"{full_name} | {joined_date}"


def build_dim_chorister_from_raw(
    values: List[List[Any]],
) -> Tuple[
    List[List[Any]],
    Tuple[Dict[Tuple[str, str], str], Dict[str, str]],
]:
    """Transform RAW into dim_chorister rows; return rows and chorister_id lookups.

    Returns (table_rows, (chorister_id_by_key, normalized_to_chorister_id))
    for use in build_dim_chorister_assignment_from_raw.
    """
    if not values:
        return (
            [DIM_CHORISTER_HEADER],
            ({}, {}),
        )

    header = values[0]
    index = _index_by_name(header)

    tag_idx = index.get("Tag")
    joined_idx = index.get("Joined")
    tgid_idx = index.get("tgid")
    who_idx = index.get("Who")

    if tag_idx is None or joined_idx is None or who_idx is None:
        return ([DIM_CHORISTER_HEADER], ({}, {}))

    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows: List[List[Any]] = []
    seen_names: Dict[str, int] = {}
    chorister_id_by_key: Dict[Tuple[str, str], str] = {}
    normalized_to_chorister_id: Dict[str, str] = {}

    for row in values[1:]:
        tag = _get_safe(row, tag_idx)
        if not tag or tag == "Song":
            continue

        full_name = _get_safe(row, who_idx)
        if not full_name:
            continue

        joined_date = _get_safe(row, joined_idx)
        tgid = _get_safe(row, tgid_idx) if tgid_idx is not None else ""

        chorister_id = _make_chorister_id(
            full_name=full_name,
            joined_date=joined_date,
            seen_names=seen_names,
        )
        key = (full_name, joined_date)
        chorister_id_by_key[key] = chorister_id
        norm = _normalize_name(full_name)
        if norm not in normalized_to_chorister_id:
            normalized_to_chorister_id[norm] = chorister_id

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

    return (
        [DIM_CHORISTER_HEADER, *rows],
        (chorister_id_by_key, normalized_to_chorister_id),
    )


DIM_CHORISTER_ASSIGNMENT_HEADER = [
    "assignment_id",
    "chorister_id",
    "voice_part",
    "is_active",
    "valid_from",
    "valid_to",
]


# Hard-coded voice-part history overrides for rare cases.
# Keyed by normalized full_name (see _normalize_name).
CHORISTER_ASSIGNMENT_OVERRIDES: Dict[str, List[Dict[str, Any]]] = {
    # Мария Дидуренко: сначала Soprano, потом Alto
    "мария_дидуренко": [
        {"voice_part": "soprano", "valid_from": "16.06.24", "valid_to": "01.10.24"},
        {"voice_part": "alto", "valid_from": "02.10.24", "valid_to": ""},
    ],
    # Полина Калач: сначала Alto, потом Soprano
    "полина_калач": [
        {"voice_part": "alto", "valid_from": "16.06.24", "valid_to": "01.10.24"},
        {"voice_part": "soprano", "valid_from": "02.10.24", "valid_to": ""},
    ],
    # Митя Черняков: Bass до конца 2025, потом Tenor
    "митя_чернаков": [
        {"voice_part": "bass", "valid_from": "16.06.24", "valid_to": "31.12.25"},
        {"voice_part": "tenor", "valid_from": "01.01.26", "valid_to": ""},
    ],
}


def _extract_voice_part_and_active(tag: str) -> Dict[str, Any]:
    """Derive voice_part (lowercase) and is_active flag from Tag.

    Rules:
    - if Tag starts with 'ex' (e.g. 'exAlto', 'ex Tenor') -> is_active = False,
      voice_part = part after 'ex' in lower case.
    - otherwise -> is_active = True, voice_part = Tag in lower case.
    """
    raw = tag.strip()
    lower = raw.lower()

    is_active = True
    voice_part_src = raw

    if lower.startswith("ex"):
        is_active = False
        # Remove 'ex' prefix and separators like '-', space, '_'
        voice_part_src = raw[2:].lstrip(" -_")

    voice_part = voice_part_src.strip().lower()
    return {"voice_part": voice_part, "is_active": is_active}


def build_dim_chorister_assignment_from_raw(
    values: List[List[Any]],
    chorister_id_by_key: Dict[Tuple[str, str], str],
    normalized_to_chorister_id: Dict[str, str],
) -> List[List[Any]]:
    """Transform RAW into dim_chorister_assignment rows using same chorister_id as dim_chorister."""
    if not values:
        return [DIM_CHORISTER_ASSIGNMENT_HEADER]

    header = values[0]
    index = _index_by_name(header)

    tag_idx = index.get("Tag")
    joined_idx = index.get("Joined")
    tgid_idx = index.get("tgid")
    who_idx = index.get("Who")

    if tag_idx is None or joined_idx is None or who_idx is None:
        return [DIM_CHORISTER_ASSIGNMENT_HEADER]

    rows: List[List[Any]] = []

    for row in values[1:]:
        tag = _get_safe(row, tag_idx)
        if not tag or tag == "Song":
            continue

        full_name = _get_safe(row, who_idx)
        if not full_name:
            continue

        joined_date = _get_safe(row, joined_idx)

        chorister_id = chorister_id_by_key.get(
            (full_name, joined_date),
            full_name,  # fallback if key missing
        )
        normalized_name = _normalize_name(full_name)

        # Special cases: explicit voice-part history from a manual table.
        overrides = CHORISTER_ASSIGNMENT_OVERRIDES.get(normalized_name)
        if overrides:
            override_chorister_id = normalized_to_chorister_id.get(
                normalized_name,
                chorister_id,
            )
            for override in overrides:
                voice_part = override["voice_part"].strip().lower()
                valid_from = override["valid_from"]
                valid_to = override.get("valid_to", "")

                assignment_id = f"{override_chorister_id} | {voice_part} | {valid_from}"
                rows.append(
                    [
                        assignment_id,
                        override_chorister_id,
                        voice_part,
                        "TRUE",
                        valid_from,
                        valid_to,
                    ]
                )
            continue

        vp_info = _extract_voice_part_and_active(tag)
        voice_part = vp_info["voice_part"]
        is_active = vp_info["is_active"]
        valid_from = joined_date
        valid_to = ""

        assignment_id = f"{chorister_id} | {voice_part} | {valid_from}"

        rows.append(
            [
                assignment_id,
                chorister_id,
                voice_part,
                "TRUE" if is_active else "FALSE",
                valid_from,
                valid_to,
            ]
        )

    return [DIM_CHORISTER_ASSIGNMENT_HEADER, *rows]


