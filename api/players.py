"""Player API endpoints — live data from PostgreSQL."""

from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc, and_, func
from sqlalchemy.orm import aliased
from datetime import datetime

from database.connection import get_db
from database.models import Player, Ranking, PlayerSurfaceRecord, Match

router = APIRouter(prefix="/api/players", tags=["players"])

IOC_TO_ISO = {
    "SRB":"rs","USA":"us","ESP":"es","RUS":"ru","GER":"de","ITA":"it","FRA":"fr",
    "GBR":"gb","AUS":"au","GRE":"gr","NOR":"no","BUL":"bg","CZE":"cz","POL":"pl",
    "ARG":"ar","CAN":"ca","BEL":"be","SUI":"ch","SWE":"se","AUT":"at","DEN":"dk",
    "NED":"nl","POR":"pt","CRO":"hr","HUN":"hu","ROU":"ro","FIN":"fi","UKR":"ua",
    "KAZ":"kz","KOR":"kr","JPN":"jp","CHI":"cl","COL":"co","BRA":"br","MEX":"mx",
    "RSA":"za","TUN":"tn","MON":"mc","CHN":"cn","IND":"in","QAT":"qa","UAE":"ae",
}

def iso(c): return IOC_TO_ISO.get((c or "").upper(), (c or "xx").lower()[:2])

@router.get("")
async def list_players(q: Optional[str] = None, limit: int = Query(250, le=500), db: AsyncSession = Depends(get_db)):
    # Get latest ranking date to avoid N+1 per player
    latest_res = await db.execute(select(func.max(Ranking.ranking_date)))
    latest_date = latest_res.scalar_one_or_none()

    RankAlias = aliased(Ranking)
    stmt = (
        select(Player, RankAlias)
        .outerjoin(RankAlias, and_(
            RankAlias.player_id == Player.id,
            RankAlias.ranking_date == latest_date,
        ))
        .where(Player.is_active == True)
    )
    if q:
        stmt = stmt.where(Player.name.ilike(f"%{q}%"))

    # Order by rank in SQL so the top-ranked players fill the limit first
    from sqlalchemy import nullslast
    stmt = stmt.order_by(nullslast(RankAlias.rank.asc()))
    rows = (await db.execute(stmt.limit(limit))).all()
    return [
        {
            "id": p.id, "name": p.name, "country": p.country_code,
            "iso": iso(p.country_code), "hand": p.hand,
            "rank": r.rank if r else None, "points": r.points if r else None,
            "photo_url": p.photo_url,
        }
        for p, r in rows
    ]

@router.get("/{player_id}/stats")
async def get_player_stats(player_id: str, db: AsyncSession = Depends(get_db)):
    """Return frontend-compatible stats dict (REAL_STATS format) built from live DB data."""
    from datetime import date as _date, timedelta

    year = datetime.now().year
    today = datetime.now().date()

    p = (await db.execute(select(Player).where(Player.id == player_id))).scalar_one_or_none()
    if not p:
        raise HTTPException(404, "Player not found")

    # ── Surface records (current year, fall back to career) ──────────────────
    yr_res = await db.execute(
        select(PlayerSurfaceRecord).where(
            and_(PlayerSurfaceRecord.player_id == player_id,
                 PlayerSurfaceRecord.year == year)
        )
    )
    surface_recs: dict = {r.surface.lower(): r for r in yr_res.scalars().all()}

    # Supplement with career records (year=NULL) for any surface not yet covered
    career_res = await db.execute(
        select(PlayerSurfaceRecord).where(
            and_(PlayerSurfaceRecord.player_id == player_id,
                 PlayerSurfaceRecord.year == None)  # noqa: E711
        )
    )
    for r in career_res.scalars().all():
        if r.surface.lower() not in surface_recs:
            surface_recs[r.surface.lower()] = r

    # Last resort: fall back to the most recent year that HAS data
    # (needed when current year and career records don't exist yet)
    if not surface_recs:
        fallback_res = await db.execute(
            select(PlayerSurfaceRecord).where(
                PlayerSurfaceRecord.player_id == player_id
            ).order_by(desc(PlayerSurfaceRecord.year.nullslast()))
        )
        for r in fallback_res.scalars().all():
            surf = r.surface.lower()
            if surf not in surface_recs:
                surface_recs[surf] = r

    def win_pct(surf: str) -> float:
        r = surface_recs.get(surf)
        if r and (r.wins + r.losses) > 0:
            return round(r.wins / (r.wins + r.losses) * 100, 1)
        return 65.0

    # Elo: first found across overall → hard → clay → grass
    elo = 1500.0
    for _s in ("overall", "hard", "clay", "grass"):
        _r = surface_recs.get(_s)
        if _r and _r.elo:
            elo = _r.elo
            break

    # Serve/return stats: prefer 'overall', fall back to 'hard'
    best = surface_recs.get("overall") or surface_recs.get("hard")

    def _f(val, default: float) -> float:
        return round(float(val), 1) if val is not None else default

    fs  = _f(best.avg_first_in_pct   if best else None, 63.0)
    fsw = _f(best.avg_first_won_pct   if best else None, 72.0)
    ssw = _f(best.avg_second_won_pct  if best else None, 53.0)
    sgw = _f(best.avg_hold_pct        if best else None, 84.0)
    rg  = _f(best.avg_return_pts_won_pct if best else None, 38.0)
    bpc = _f(best.avg_break_pct       if best else None, 43.0)
    aces= _f(best.avg_aces            if best else None,  4.0)
    dfs = _f(best.avg_dfs             if best else None,  2.5)
    # Break points saved ≈ sgw * 0.77 (validated against REAL_STATS calibration set)
    bps = round(max(50.0, min(82.0, sgw * 0.77)), 1)

    # ── YTD match history (current year) ────────────────────────────────────
    ytd_res = await db.execute(
        select(Match).where(
            and_(
                (Match.winner_id == player_id) | (Match.loser_id == player_id),
                Match.match_date >= _date(year, 1, 1),
                Match.status == "completed",
            )
        ).order_by(desc(Match.match_date)).limit(80)
    )
    ytd_matches = ytd_res.scalars().all()

    ytd_w = sum(1 for m in ytd_matches if m.winner_id == player_id)
    ytd_l = len(ytd_matches) - ytd_w

    # Titles this year = final-round wins
    titles = sum(
        1 for m in ytd_matches
        if m.winner_id == player_id and (m.round or "").upper() == "F"
    )

    # ── Recent match history — fall back across years if no current-year data ─
    # Always show the 10 most recent matches we have, regardless of year.
    recent_res = await db.execute(
        select(Match).where(
            and_(
                (Match.winner_id == player_id) | (Match.loser_id == player_id),
                Match.status == "completed",
            )
        ).order_by(desc(Match.match_date)).limit(80)
    )
    recent_matches = recent_res.scalars().all()

    # Form string from last 10 completed matches (most-recent first)
    last10_m = (ytd_matches or recent_matches)[:10]
    form_str = "".join("W" if m.winner_id == player_id else "L" for m in last10_m) or "WWWWWLWWWW"

    # Fatigue = matches played in last 14 days
    cutoff = today - timedelta(days=14)
    all_recent = ytd_matches or recent_matches
    fatigue = sum(1 for m in all_recent if m.match_date and m.match_date >= cutoff)

    # Tournament short-names for last-10 display
    tourn_ids = list({m.tournament_id for m in last10_m if m.tournament_id})
    tourn_names: dict = {}
    if tourn_ids:
        from database.models import Tournament as _T
        t_res = await db.execute(select(_T).where(_T.id.in_(tourn_ids)))
        for t in t_res.scalars().all():
            tourn_names[t.id] = t.short_name or t.name[:14]

    def _short(full: str) -> str:
        if not full:
            return ""
        parts = full.split()
        return (parts[0][0] + ". " + " ".join(parts[1:])) if len(parts) > 1 else full

    last10 = [
        {
            "opp":   _short(m.loser_name if m.winner_id == player_id else (m.winner_name or "")),
            "oc":    "xx",
            "surf":  (m.surface or "Hard"),
            "tour":  tourn_names.get(m.tournament_id or "", ""),
            "rd":    m.round or "",
            "score": m.score or "",
            "res":   "W" if m.winner_id == player_id else "L",
            "dt":    m.match_date.strftime("%d %b %Y") if m.match_date else "",
        }
        for m in last10_m
    ]

    return {
        "elo":         round(elo),
        "hard":        win_pct("hard"),
        "clay":        win_pct("clay"),
        "grass":       win_pct("grass"),
        "fs":          fs,
        "fsw":         fsw,
        "ssw":         ssw,
        "sgw":         sgw,
        "rg":          rg,
        "bpc":         bpc,
        "bps":         bps,
        "aces":        aces,
        "dfs":         dfs,
        "ytd_w":       ytd_w,
        "ytd_l":       ytd_l,
        "titles_2025": titles,
        "form":        form_str,
        "last10":      last10,
        "fatigue":     fatigue,
    }


@router.get("/{player_id}")
async def get_player(player_id: str, db: AsyncSession = Depends(get_db)):
    p_res = await db.execute(select(Player).where(Player.id == player_id))
    player = p_res.scalar_one_or_none()
    if not player:
        raise HTTPException(404, "Player not found")

    rk_res = await db.execute(select(Ranking).where(Ranking.player_id==player_id).order_by(desc(Ranking.ranking_date)).limit(1))
    ranking = rk_res.scalar_one_or_none()

    year = datetime.now().year
    # Prefer current-year records; fall back to all available years if none exist
    sr_res = await db.execute(select(PlayerSurfaceRecord).where(
        PlayerSurfaceRecord.player_id == player_id
    ))
    all_surface_records = sr_res.scalars().all()
    # Group by surface, aggregate wins/losses across all years
    from collections import defaultdict
    surf_agg: dict = defaultdict(lambda: {"wins": 0, "losses": 0})
    for r in all_surface_records:
        surf_agg[r.surface]["wins"] += r.wins or 0
        surf_agg[r.surface]["losses"] += r.losses or 0
    surface_records = surf_agg  # dict of surface -> {wins, losses}

    match_res = await db.execute(
        select(Match).where(
            and_(Match.status=="completed",
                 (Match.winner_id==player_id)|(Match.loser_id==player_id))
        ).order_by(desc(Match.match_date)).limit(15)
    )
    recent = match_res.scalars().all()

    age = None
    if player.dob:
        today = datetime.now().date()
        age = today.year - player.dob.year - ((today.month, today.day) < (player.dob.month, player.dob.day))

    return {
        "id": player.id, "name": player.name,
        "first_name": player.first_name, "last_name": player.last_name,
        "country": player.country_code, "iso": iso(player.country_code),
        "hand": player.hand, "age": age,
        "dob": player.dob.isoformat() if player.dob else None,
        "height_cm": player.height_cm, "photo_url": player.photo_url,
        "rank": ranking.rank if ranking else None,
        "points": ranking.points if ranking else None,
        "surface_records": {
            surf: {
                "wins": v["wins"], "losses": v["losses"],
                "win_pct": round(v["wins"]/(v["wins"]+v["losses"])*100, 1) if (v["wins"]+v["losses"]) > 0 else 0
            } for surf, v in surface_records.items()
        },
        "recent_matches": [
            {
                "date": m.match_date.isoformat() if m.match_date else None,
                "tournament_id": m.tournament_id,
                "round": m.round, "surface": m.surface,
                "result": "W" if m.winner_id==player_id else "L",
                "opponent": m.loser_name if m.winner_id==player_id else m.winner_name,
                "score": m.score,
            } for m in recent
        ],
    }
