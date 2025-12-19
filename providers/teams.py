from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Mapping, Optional, Tuple

import yaml

DEFAULT_TEAM_MAPPING_PATH = Path("config/team_mapping.yaml")


def _norm_key(value: str) -> str:
    return " ".join((value or "").strip().lower().split())


@dataclass(frozen=True)
class TeamResolver:
    member_to_team: Mapping[str, Tuple[str, str]]  # member_identity -> (team_id, team_name)

    def resolve(self, identity: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
        if not identity:
            return None, None
        key = _norm_key(identity)
        team = self.member_to_team.get(key)
        if not team:
            return None, None
        return team[0], team[1]


def load_team_resolver(path: Optional[Path] = None) -> TeamResolver:
    raw_path = os.getenv("TEAM_MAPPING_PATH")
    if raw_path:
        path = Path(raw_path)
    path = path or DEFAULT_TEAM_MAPPING_PATH

    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}
    except FileNotFoundError:
        payload = {}

    member_to_team: Dict[str, Tuple[str, str]] = {}
    for entry in payload.get("teams") or []:
        team_id = str(entry.get("team_id") or "").strip()
        team_name = str(entry.get("team_name") or team_id).strip()
        if not team_id:
            continue
        for member in entry.get("members") or []:
            key = _norm_key(str(member))
            if not key:
                continue
            member_to_team[key] = (team_id, team_name)

    return TeamResolver(member_to_team=member_to_team)

