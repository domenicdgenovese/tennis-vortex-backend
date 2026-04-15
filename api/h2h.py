"""Head-to-head API."""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, desc, or_
from database.connection import get_db
from database.models import HeadToHead, Match, Tournament

router = APIRouter(prefix="/api/h2h", tags=["h2h"])

@router.get("")
async def get_h2h(p1: str = Query(...), p2: str = Query(...), db: AsyncSession = Depends(get_db)):
    # Normalise: player1_id is always the lexicographically smaller id
    p1c, p2c = min(p1, p2), max(p1, p2)
    flipped = (p1 != p1c)  # True when caller's p1 is the lex-larger id

    h2h = (await db.execute(
        select(HeadToHead).where(and_(HeadToHead.player1_id == p1c, HeadToHead.player2_id == p2c))
    )).scalar_one_or_none()

    matches_rows = (await db.execute(
        select(Match, Tournament)
        .outerjoin(Tournament, Match.tournament_id == Tournament.id)
        .where(or_(
            and_(Match.winner_id == p1, Match.loser_id == p2),
            and_(Match.winner_id == p2, Match.loser_id == p1),
        ))
        .order_by(desc(Match.match_date))
        .limit(10)
    )).all()

    # Swap p1/p2 win counts when the caller's perspective is flipped
    if h2h:
        p1w = h2h.p2_wins if flipped else h2h.p1_wins
        p2w = h2h.p1_wins if flipped else h2h.p2_wins

        def sw(surf):
            """Return (p1_wins, p2_wins) on a given surface from the caller's perspective."""
            raw_p1 = getattr(h2h, f"p1_{surf}_wins", 0) or 0
            raw_p2 = getattr(h2h, f"p2_{surf}_wins", 0) or 0
            return (raw_p2, raw_p1) if flipped else (raw_p1, raw_p2)
    else:
        p1w = p2w = 0
        def sw(surf): return (0, 0)

    total = p1w + p2w

    by_surface = {}
    for surf in ("hard", "clay", "grass"):
        sw1, sw2 = sw(surf)
        by_surface[surf.capitalize()] = {"p1_wins": sw1, "p2_wins": sw2}

    recent = [
        {
            "date": m.match_date.isoformat() if m.match_date else None,
            "tournament": t.name if t else None,
            "surface": m.surface,
            "round": m.round,
            "winner_id": m.winner_id,
            "score": m.score,
        }
        for m, t in matches_rows
    ]

    return {
        "p1_id": p1,
        "p2_id": p2,
        "p1_wins": p1w,
        "p2_wins": p2w,
        "total_matches": total,
        "surface_breakdown": by_surface,
        "last_match_date": h2h.last_match_date.isoformat() if h2h and h2h.last_match_date else None,
        "recent_matches": recent,
    }
