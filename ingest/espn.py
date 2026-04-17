"""
ESPN unofficial API ingestion for live scores, upcoming matches,
and current ATP rankings.
No authentication required.
"""

import logging
import re
import time
import unicodedata
from datetime import datetime, date
from typing import Optional, List, Dict
import httpx
from sqlalchemy import select, update as sa_update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert
from database.models import Match, Tournament, Player, Ranking, SyncLog
from utils.helpers import safe_int, normalize_surface

logger = logging.getLogger(__name__)

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/tennis"
ESPN_RANKINGS_URL = f"{ESPN_BASE}/atp/rankings"
TOURS = ["atp", "wta"]


def _norm_name(name: str) -> str:
    """Lowercase, strip diacritics, collapse whitespace — for fuzzy matching."""
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_str = nfkd.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", ascii_str).strip().lower()


async def sync_espn_rankings(db: AsyncSession) -> dict:
    """
    Fetch live ATP rankings from ESPN and write them to the rankings table.
    ESPN updates weekly (usually Monday). Uses today's date as ranking_date.
    For players not yet in our DB, inserts a minimal Player record using
    'e{espn_id}' as the primary key to avoid collision with Sackmann IDs.
    """
    started = time.time()
    stats = {"processed": 0, "inserted": 0, "updated": 0, "new_players": 0}

    async with httpx.AsyncClient(timeout=20.0) as client:
        try:
            resp = await client.get(ESPN_RANKINGS_URL)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error(f"ESPN rankings fetch error: {e}")
            return stats

    ranks = data.get("rankings", [{}])[0].get("ranks", [])
    if not ranks:
        logger.warning("ESPN rankings: empty response")
        return stats

    # Parse the update date from ESPN (e.g. "2026-04-09T07:00Z")
    update_str = data.get("rankings", [{}])[0].get("update", "")
    try:
        rank_date = datetime.fromisoformat(update_str.replace("Z", "+00:00")).date()
    except Exception:
        rank_date = date.today()

    # Build name → player_id lookup from our existing players
    existing_res = await db.execute(select(Player.id, Player.name, Player.atp_code))
    name_to_id: dict[str, str] = {}   # normalised_name → player_id
    espn_id_to_id: dict[str, str] = {}  # espn_id (str) → player_id
    for pid, pname, atp_code in existing_res.all():
        if pname:
            name_to_id[_norm_name(pname)] = pid
        if atp_code and atp_code.startswith("e"):
            espn_id_to_id[atp_code[1:]] = pid  # strip 'e' prefix

    rows_to_insert: list[dict] = []

    for rank_entry in ranks:
        stats["processed"] += 1
        athlete = rank_entry.get("athlete", {})
        espn_id = str(athlete.get("id", ""))
        display_name = athlete.get("displayName", "").strip()
        rank_num = rank_entry.get("current")
        points = int(rank_entry.get("points", 0) or 0)

        if not espn_id or not display_name or not rank_num:
            continue

        # Resolve to our player_id
        player_id = (
            espn_id_to_id.get(espn_id)          # already matched by ESPN id
            or name_to_id.get(_norm_name(display_name))  # match by name
        )

        if player_id is None:
            # New player — insert minimal record with ESPN-prefixed id
            player_id = f"e{espn_id}"
            parts = display_name.rsplit(" ", 1)
            first = parts[0] if len(parts) == 2 else ""
            last = parts[-1]
            country_abbr = athlete.get("flag", {}).get("alt") or athlete.get("citizenship", "")
            new_player = {
                "id": player_id,
                "name": display_name,
                "first_name": first,
                "last_name": last,
                "country_code": country_abbr[:3].upper() if country_abbr else None,
                "atp_code": f"e{espn_id}",   # mark as ESPN-sourced
                "is_active": True,
            }
            p_stmt = pg_insert(Player).values(**new_player)
            p_stmt = p_stmt.on_conflict_do_update(
                index_elements=["id"],
                set_={"name": display_name, "is_active": True}
            )
            await db.execute(p_stmt)
            stats["new_players"] += 1
            name_to_id[_norm_name(display_name)] = player_id
            espn_id_to_id[espn_id] = player_id
        else:
            # Update atp_code to store ESPN id for future fast lookups
            await db.execute(
                sa_update(Player)
                .where(Player.id == player_id)
                .values(atp_code=f"e{espn_id}", is_active=True)
            )

        rows_to_insert.append({
            "player_id": player_id,
            "ranking_date": rank_date,
            "rank": rank_num,
            "points": points,
            "tour": "ATP",
        })

    if rows_to_insert:
        await db.commit()  # flush player upserts first
        stmt = pg_insert(Ranking).values(rows_to_insert)
        stmt = stmt.on_conflict_do_update(
            index_elements=["player_id", "ranking_date"],
            set_={"rank": stmt.excluded.rank, "points": stmt.excluded.points},
        )
        await db.execute(stmt)
        await db.commit()
        stats["inserted"] = len(rows_to_insert)

    logger.info(
        f"sync_espn_rankings: {stats} rank_date={rank_date} "
        f"({time.time()-started:.1f}s)"
    )
    return stats


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
