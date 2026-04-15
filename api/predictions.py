"""Prediction API — feeds live DB features into the full 18-factor model."""
from datetime import datetime, date
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc, and_

from database.connection import get_db
from database.models import Player, Ranking, PlayerSurfaceRecord, HeadToHead, Match
from models.predictor import PlayerFeatures, MatchContext, predict_match

router = APIRouter(prefix="/api/predictions", tags=["predictions"])


async def _build_features(pid: str, surface: str, db: AsyncSession) -> PlayerFeatures:
    year = datetime.now().year

    p = (await db.execute(select(Player).where(Player.id == pid))).scalar_one_or_none()
    if not p:
        return PlayerFeatures(player_id=pid, name=pid)

    rk = (await db.execute(
        select(Ranking).where(Ranking.player_id == pid).order_by(desc(Ranking.ranking_date)).limit(1)
    )).scalar_one_or_none()

    # Surface record for current year first, then career
    sr = (await db.execute(
        select(PlayerSurfaceRecord).where(
            and_(PlayerSurfaceRecord.player_id == pid,
                 PlayerSurfaceRecord.surface == surface,
                 PlayerSurfaceRecord.year == year)
        )
    )).scalar_one_or_none()
    if not sr:
        sr = (await db.execute(
            select(PlayerSurfaceRecord).where(
                and_(PlayerSurfaceRecord.player_id == pid,
                     PlayerSurfaceRecord.surface == surface,
                     PlayerSurfaceRecord.year == None)
            )
        )).scalar_one_or_none()

    # YTD matches
    matches = (await db.execute(
        select(Match).where(
            and_(
                (Match.winner_id == pid) | (Match.loser_id == pid),
                Match.match_date >= date(year, 1, 1),
                Match.status == "completed",
            )
        ).order_by(desc(Match.match_date)).limit(15)
    )).scalars().all()

    total = len(matches)
    wins = sum(1 for m in matches if m.winner_id == pid)
    l10 = matches[:10]
    l5  = matches[:5]
    l10_w = sum(1 for m in l10 if m.winner_id == pid)
    l5_w  = sum(1 for m in l5  if m.winner_id == pid)

    # Surface win %
    surf_pct = 50.0
    if sr and (sr.wins + sr.losses) > 0:
        surf_pct = round(sr.wins / (sr.wins + sr.losses) * 100, 1)

    # Fatigue: penalise if >60 matches in season
    fatigue = max(0.85, 1.0 - max(0, total - 60) * 0.005)

    # Rank-based Elo estimate when surface record has no Elo stored.
    # Calibrated so rank 1 ≈ 2150, rank 10 ≈ 2000, rank 50 ≈ 1850, rank 200 ≈ 1650.
    rank_val = rk.rank if rk else 999
    rank_elo = max(1400.0, 2200.0 - rank_val * 3.5)
    elo_val = (sr.elo if sr and sr.elo else None) or rank_elo

    return PlayerFeatures(
        player_id=pid,
        name=p.name,
        rank=rank_val,
        elo=elo_val,
        surface_win_pct=surf_pct,
        ytd_win_pct=round(wins / total * 100, 1) if total else 50.0,
        form_l10=round(l10_w / len(l10) * 100, 1) if l10 else 50.0,
        form_l5=round(l5_w  / len(l5)  * 100, 1) if l5  else 50.0,
        hold_pct=sr.avg_hold_pct or 70.0 if sr else 70.0,
        break_pct=sr.avg_break_pct or 30.0 if sr else 30.0,
        first_in_pct=sr.avg_first_in_pct or 60.0 if sr else 60.0,
        first_won_pct=sr.avg_first_won_pct or 72.0 if sr else 72.0,
        second_won_pct=sr.avg_second_won_pct or 50.0 if sr else 50.0,
        ace_rate=sr.avg_aces or 5.0 if sr else 5.0,
        df_rate=sr.avg_dfs or 2.5 if sr else 2.5,
        return_pts_won_pct=sr.avg_return_pts_won_pct or 40.0 if sr else 40.0,
        fatigue_score=fatigue,
    )


@router.get("")
async def predict(
    p1: str = Query(...),
    p2: str = Query(...),
    surface: str = "hard",
    best_of: int = 3,
    weather: str = "clear",
    altitude: str = "sea",
    crowd: str = "neutral",
    court_speed: str = "medium",
    db: AsyncSession = Depends(get_db),
):
    f1 = await _build_features(p1, surface, db)
    f2 = await _build_features(p2, surface, db)

    # H2H record
    p1c, p2c = min(p1, p2), max(p1, p2)
    flipped = (p1 != p1c)
    h2h_row = (await db.execute(
        select(HeadToHead).where(
            and_(HeadToHead.player1_id == p1c, HeadToHead.player2_id == p2c)
        )
    )).scalar_one_or_none()

    h2h: dict = {}
    if h2h_row:
        h2h = {
            "p1_wins":       h2h_row.p2_wins if flipped else h2h_row.p1_wins,
            "p2_wins":       h2h_row.p1_wins if flipped else h2h_row.p2_wins,
            f"p1_{surface}_wins": (h2h_row.p2_hard_wins if surface == "hard" else
                                   h2h_row.p2_clay_wins if surface == "clay" else
                                   h2h_row.p2_grass_wins) if flipped else
                                  (h2h_row.p1_hard_wins if surface == "hard" else
                                   h2h_row.p1_clay_wins if surface == "clay" else
                                   h2h_row.p1_grass_wins),
            f"p2_{surface}_wins": (h2h_row.p1_hard_wins if surface == "hard" else
                                   h2h_row.p1_clay_wins if surface == "clay" else
                                   h2h_row.p1_grass_wins) if flipped else
                                  (h2h_row.p2_hard_wins if surface == "hard" else
                                   h2h_row.p2_clay_wins if surface == "clay" else
                                   h2h_row.p2_grass_wins),
        }

    ctx = MatchContext(
        surface=surface,
        best_of=best_of,
        court_speed=court_speed,
        weather=weather,
        altitude=altitude,
        crowd=crowd,
    )

    result = predict_match(f1, f2, h2h, ctx, bo=best_of)

    return {
        "p1": {"id": p1, "name": f1.name, "rank": f1.rank, "elo": f1.elo},
        "p2": {"id": p2, "name": f2.name, "rank": f2.rank, "elo": f2.elo},
        "prediction": result,
        "context": {
            "surface": surface, "best_of": best_of,
            "weather": weather, "altitude": altitude,
            "crowd": crowd, "court_speed": court_speed,
        },
    }
