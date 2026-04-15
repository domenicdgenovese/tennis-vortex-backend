"""Tournaments API."""
from typing import Optional
from datetime import date, timedelta
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_
from database.connection import get_db
from database.models import Tournament

router = APIRouter(prefix="/api/tournaments", tags=["tournaments"])

def _fmt(t: Tournament) -> dict:
    return {"id": t.id, "name": t.name, "short_name": t.short_name, "year": t.year,
            "start_date": t.start_date.isoformat() if t.start_date else None,
            "end_date": t.end_date.isoformat() if t.end_date else None,
            "surface": t.surface, "level": t.level, "draw_size": t.draw_size,
            "prize_money": t.prize_money, "location": t.location, "country": t.country,
            "indoor": t.indoor, "status": t.status}

@router.get("")
async def list_tournaments(year: Optional[int]=None, level: Optional[str]=None, status: Optional[str]=None, db: AsyncSession=Depends(get_db)):
    stmt = select(Tournament).order_by(Tournament.start_date)
    if year: stmt = stmt.where(Tournament.year == year)
    if level: stmt = stmt.where(Tournament.level == level)
    if status: stmt = stmt.where(Tournament.status == status)
    return [_fmt(t) for t in (await db.execute(stmt)).scalars().all()]

@router.get("/current")
async def current_tournaments(db: AsyncSession=Depends(get_db)):
    today = date.today()
    stmt = select(Tournament).where(and_(Tournament.start_date<=today+timedelta(days=14), Tournament.end_date>=today-timedelta(days=1))).order_by(Tournament.start_date)
    return [_fmt(t) for t in (await db.execute(stmt)).scalars().all()]
