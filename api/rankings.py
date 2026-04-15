"""Rankings API — live from PostgreSQL."""
from typing import Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, desc, and_
from database.connection import get_db
from database.models import Player, Ranking

router = APIRouter(prefix="/api/rankings", tags=["rankings"])

IOC_TO_ISO = {"SRB":"rs","USA":"us","ESP":"es","RUS":"ru","GER":"de","ITA":"it","FRA":"fr","GBR":"gb","AUS":"au","GRE":"gr","NOR":"no","BUL":"bg","CZE":"cz","POL":"pl","ARG":"ar","CAN":"ca","BEL":"be","SUI":"ch","SWE":"se","AUT":"at","DEN":"dk","NED":"nl","POR":"pt","CRO":"hr","HUN":"hu","ROU":"ro","FIN":"fi","UKR":"ua","KAZ":"kz","KOR":"kr","JPN":"jp","CHI":"cl","COL":"co","BRA":"br","MEX":"mx","RSA":"za","TUN":"tn","MON":"mc","CHN":"cn","IND":"in","QAT":"qa","UAE":"ae"}
def iso(c): return IOC_TO_ISO.get((c or "").upper(), (c or "xx").lower()[:2])

@router.get("")
async def get_rankings(tour: str = "ATP", limit: int = Query(100, le=250), db: AsyncSession = Depends(get_db)):
    latest = await db.execute(select(func.max(Ranking.ranking_date)).where(Ranking.tour == tour))
    latest_date = latest.scalar_one_or_none()
    if not latest_date:
        return {"date": None, "rankings": []}
    stmt = (select(Ranking, Player).join(Player, Ranking.player_id == Player.id)
            .where(and_(Ranking.ranking_date == latest_date, Ranking.tour == tour))
            .order_by(Ranking.rank).limit(limit))
    rows = (await db.execute(stmt)).all()
    return {
        "date": latest_date.isoformat(), "tour": tour,
        "rankings": [{"rank": r.rank, "points": r.points, "player_id": p.id, "name": p.name,
                      "country": p.country_code, "iso": iso(p.country_code), "hand": p.hand,
                      "dob": p.dob.isoformat() if p.dob else None, "height_cm": p.height_cm} for r, p in rows]
    }
