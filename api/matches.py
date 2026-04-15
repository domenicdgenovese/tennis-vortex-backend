"""Matches API."""
from typing import Optional
from datetime import date, timedelta
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc, and_
from database.connection import get_db
from database.models import Match, Tournament

router = APIRouter(prefix="/api/matches", tags=["matches"])

def _fmt(m: Match, t) -> dict:
    return {"id": m.id, "date": m.match_date.isoformat() if m.match_date else None,
            "round": m.round, "surface": m.surface, "best_of": m.best_of, "status": m.status,
            "winner_id": m.winner_id, "winner_name": m.winner_name, "winner_rank": m.winner_rank,
            "loser_id": m.loser_id, "loser_name": m.loser_name, "loser_rank": m.loser_rank,
            "score": m.score, "minutes": m.minutes,
            "tournament": {"id": t.id, "name": t.name, "level": t.level, "surface": t.surface, "location": t.location} if t else None}

@router.get("")
async def get_matches(status: Optional[str]=None, days: int=7, limit: int=Query(50,le=200), db: AsyncSession=Depends(get_db)):
    today = date.today()
    stmt = (select(Match, Tournament).outerjoin(Tournament, Match.tournament_id==Tournament.id)
            .where(and_(Match.match_date >= today-timedelta(days=days), Match.match_date <= today+timedelta(days=days))))
    if status: stmt = stmt.where(Match.status == status)
    rows = (await db.execute(stmt.order_by(desc(Match.match_date)).limit(limit))).all()
    return [_fmt(m, t) for m, t in rows]

@router.get("/live")
async def live_matches(db: AsyncSession=Depends(get_db)):
    rows = (await db.execute(select(Match, Tournament).outerjoin(Tournament, Match.tournament_id==Tournament.id).where(Match.status=="live"))).all()
    return [_fmt(m, t) for m, t in rows]

@router.get("/upcoming")
async def upcoming_matches(days: int=3, db: AsyncSession=Depends(get_db)):
    today = date.today()
    stmt = (select(Match, Tournament).outerjoin(Tournament, Match.tournament_id==Tournament.id)
            .where(and_(Match.status=="scheduled", Match.match_date>=today, Match.match_date<=today+timedelta(days=days)))
            .order_by(Match.match_date).limit(100))
    rows = (await db.execute(stmt)).all()
    return [_fmt(m, t) for m, t in rows]

@router.get("/{match_id}")
async def get_match(match_id: str, db: AsyncSession=Depends(get_db)):
    row = (await db.execute(select(Match, Tournament).outerjoin(Tournament, Match.tournament_id==Tournament.id).where(Match.id==match_id))).one_or_none()
    if not row: raise HTTPException(404, "Match not found")
    return _fmt(*row)
