"""
ESPN unofficial API ingestion for live scores and upcoming matches.
No authentication required.
"""

import logging
import time
from datetime import datetime, date
from typing import Optional, List, Dict
import httpx
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert
from database.models import Match, Tournament, SyncLog
from utils.helpers import safe_int, normalize_surface

logger = logging.getLogger(__name__)

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/tennis"
TOURS = ["atp", "wta"]


async def fetch_espn_scoreboard(tour: str = "atp") -> Optional[Dict]:
    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            resp = await client.get(f"{ESPN_BASE}/{tour}/scoreboard")
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"ESPN scoreboard error ({tour}): {e}")
            return None


async def fetch_espn_schedule(tour: str = "atp") -> Optional[Dict]:
    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            resp = await client.get(f"{ESPN_BASE}/{tour}/schedule")
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"ESPN schedule error ({tour}): {e}")
            return None


def parse_espn_event(ev: Dict) -> Optional[Dict]:
    """Parse a single ESPN event into our match format."""
    try:
        eid = str(ev.get("id", ""))
        comps = ev.get("competitions", [{}])[0]
        competitors = comps.get("competitors", [])
        if len(competitors) < 2:
            return None

        # ESPN puts winner first when completed
        c1, c2 = competitors[0], competitors[1]

        def get_athlete(c):
            return c.get("athlete") or c.get("team") or {}

        a1, a2 = get_athlete(c1), get_athlete(c2)

        status_obj = ev.get("status", {})
        status_type = status_obj.get("type", {})
        status = status_type.get("name", "STATUS_UNKNOWN")

        # Map ESPN status to our status
        status_map = {
            "STATUS_SCHEDULED": "scheduled",
            "STATUS_IN_PROGRESS": "live",
            "STATUS_FINAL": "completed",
            "STATUS_POSTPONED": "cancelled",
        }
        our_status = status_map.get(status, "scheduled")

        # Get date
        dt_str = ev.get("date", "")
        match_date = None
        if dt_str:
            try:
                match_date = datetime.fromisoformat(dt_str.replace("Z", "+00:00")).date()
            except:
                match_date = date.today()

        # Score
        score_str = ""
        if c1.get("winner") is not None:
            winner = c1 if c1.get("winner") else c2
            loser = c2 if c1.get("winner") else c1
            linescores = winner.get("linescores", [])
            loser_scores = loser.get("linescores", [])
            parts = []
            for i, ls in enumerate(linescores):
                w_pts = ls.get("value", "")
                l_pts = loser_scores[i].get("value", "") if i < len(loser_scores) else ""
                parts.append(f"{w_pts}-{l_pts}")
            score_str = " ".join(parts)

        return {
            "espn_id": eid,
            "match_date": match_date,
            "status": our_status,
            "player1_name": a1.get("displayName", ""),
            "player1_id_espn": str(a1.get("id", "")),
            "player2_name": a2.get("displayName", ""),
            "player2_id_espn": str(a2.get("id", "")),
            "score": score_str,
            "tournament_name": ev.get("season", {}).get("slug", ""),
        }
    except Exception as e:
        logger.error(f"Error parsing ESPN event: {e}")
        return None


async def sync_live_scores(db: AsyncSession) -> dict:
    """Sync live/recent scores from ESPN."""
    stats = {"processed": 0, "updated": 0}

    data = await fetch_espn_scoreboard("atp")
    if not data:
        return stats

    events = data.get("events", [])
    for ev in events:
        stats["processed"] += 1
        parsed = parse_espn_event(ev)
        if not parsed or not parsed.get("espn_id"):
            continue

        # Find existing match by espn_id and update status/score
        from sqlalchemy import update as sa_update
        from database.models import Match as MatchModel

        stmt = (
            sa_update(MatchModel)
            .where(MatchModel.espn_id == parsed["espn_id"])
            .values(status=parsed["status"], score=parsed["score"])
        )
        result = await db.execute(stmt)
        if result.rowcount > 0:
            stats["updated"] += 1

    await db.commit()
    logger.info(f"sync_live_scores: {stats}")
    return stats


async def get_upcoming_matches(tour: str = "atp") -> List[Dict]:
    """Return upcoming matches from ESPN scoreboard (no DB write needed)."""
    data = await fetch_espn_scoreboard(tour)
    if not data:
        return []

    upcoming = []
    for ev in data.get("events", []):
        parsed = parse_espn_event(ev)
        if parsed and parsed["status"] in ("scheduled", "live"):
            upcoming.append(parsed)

    return upcoming
