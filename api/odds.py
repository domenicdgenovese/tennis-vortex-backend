"""Odds API — serve latest betting odds from DB."""
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc, func, and_
from database.connection import get_db
from database.models import OddsSnapshot

router = APIRouter(prefix="/api/odds", tags=["odds"])

@router.get("")
async def get_current_odds(db: AsyncSession = Depends(get_db)):
    """Return the most recent odds snapshot per match (within 60 min)."""
    cutoff = datetime.utcnow() - timedelta(hours=1)

    # Latest snapshot_at per (match_id, bookmaker)
    latest_sub = (
        select(
            OddsSnapshot.match_id,
            OddsSnapshot.bookmaker,
            func.max(OddsSnapshot.snapshot_at).label("max_ts"),
        )
        .where(OddsSnapshot.snapshot_at >= cutoff)
        .group_by(OddsSnapshot.match_id, OddsSnapshot.bookmaker)
        .subquery()
    )

    stmt = (
        select(OddsSnapshot)
        .join(
            latest_sub,
            and_(
                OddsSnapshot.match_id == latest_sub.c.match_id,
                OddsSnapshot.bookmaker == latest_sub.c.bookmaker,
                OddsSnapshot.snapshot_at == latest_sub.c.max_ts,
            ),
        )
        .order_by(OddsSnapshot.player1_name, OddsSnapshot.bookmaker)
    )

    rows = (await db.execute(stmt)).scalars().all()

    # Group by match, collect bookmakers
    matches: dict = {}
    for r in rows:
        key = r.match_id or f"{r.player1_name}__{r.player2_name}"
        if key not in matches:
            matches[key] = {
                "match_id": r.match_id,
                "player1": r.player1_name,
                "player2": r.player2_name,
                "tournament": r.tournament_name,
                "snapshot_at": r.snapshot_at.isoformat() if r.snapshot_at else None,
                "books": [],
            }
        matches[key]["books"].append({
            "bookmaker": r.bookmaker,
            "p1_odds": r.player1_odds,
            "p2_odds": r.player2_odds,
            "p1_implied_pct": round(r.player1_implied_pct, 1) if r.player1_implied_pct else None,
            "p2_implied_pct": round(r.player2_implied_pct, 1) if r.player2_implied_pct else None,
        })

    return {"count": len(matches), "matches": list(matches.values())}
