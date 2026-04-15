"""
The Odds API integration for sportsbook odds.
Sign up free at https://the-odds-api.com (500 free requests/month).
"""

import os
import logging
import time
from datetime import datetime
from typing import Optional, List, Dict
import httpx
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert
from database.models import OddsSnapshot, SyncLog

logger = logging.getLogger(__name__)

ODDS_BASE = "https://api.the-odds-api.com/v4"
API_KEY = os.getenv("ODDS_API_KEY", "")

SPORTS = ["tennis_atp", "tennis_wta"]
REGIONS = "us,eu,uk"
MARKETS = "h2h"


async def fetch_odds(sport: str = "tennis_atp") -> Optional[List[Dict]]:
    if not API_KEY:
        logger.warning("ODDS_API_KEY not set -- skipping odds sync")
        return None

    async with httpx.AsyncClient(timeout=20.0) as client:
        try:
            resp = await client.get(
                f"{ODDS_BASE}/sports/{sport}/odds",
                params={
                    "apiKey": API_KEY,
                    "regions": REGIONS,
                    "markets": MARKETS,
                    "oddsFormat": "american",
                }
            )
            resp.raise_for_status()
            remaining = resp.headers.get("x-requests-remaining", "?")
            logger.info(f"Odds API requests remaining: {remaining}")
            return resp.json()
        except Exception as e:
            logger.error(f"Odds API error ({sport}): {e}")
            return None


def american_to_implied(odds: float) -> float:
    """Convert American odds to implied probability."""
    if odds > 0:
        return 100 / (odds + 100)
    else:
        return abs(odds) / (abs(odds) + 100)


async def sync_odds(db: AsyncSession) -> dict:
    """Fetch current odds and store snapshots."""
    stats = {"processed": 0, "inserted": 0}
    started = time.time()

    log = SyncLog(job_name="odds_sync", status="running")
    db.add(log)
    await db.commit()

    try:
        for sport in SPORTS:
            events = await fetch_odds(sport)
            if not events:
                continue

            for ev in events:
                stats["processed"] += 1

                home = ev.get("home_team", "")
                away = ev.get("away_team", "")
                ev_id = ev.get("id", "")
                tourn = ev.get("sport_title", "")

                for bookmaker in ev.get("bookmakers", []):
                    bk_name = bookmaker.get("key", "")
                    for market in bookmaker.get("markets", []):
                        if market.get("key") != "h2h":
                            continue
                        outcomes = market.get("outcomes", [])
                        if len(outcomes) < 2:
                            continue

                        # Match by name
                        o1 = next((o for o in outcomes if o["name"] == home), None)
                        o2 = next((o for o in outcomes if o["name"] == away), None)
                        if not o1 or not o2:
                            continue

                        snapshot = {
                            "match_id": ev_id,
                            "player1_name": home,
                            "player2_name": away,
                            "bookmaker": bk_name,
                            "player1_odds": float(o1["price"]),
                            "player2_odds": float(o2["price"]),
                            "player1_implied_pct": american_to_implied(float(o1["price"])) * 100,
                            "player2_implied_pct": american_to_implied(float(o2["price"])) * 100,
                            "tournament_name": tourn,
                        }
                        db.add(OddsSnapshot(**snapshot))
                        stats["inserted"] += 1

        await db.commit()
        log.status = "success"
    except Exception as e:
        log.status = "failed"
        log.error_message = str(e)
        logger.error(f"Odds sync failed: {e}")
    finally:
        log.duration_seconds = time.time() - started
        log.completed_at = datetime.now()
        await db.commit()

    return stats
