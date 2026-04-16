"""
Jeff Sackmann ATP Data Ingestion
Source: https://github.com/JeffSackmann/tennis_atp
Free, comprehensive ATP match data updated throughout the season.
"""

import io
import logging
import time
from datetime import date, datetime
from typing import Optional
import httpx
import pandas as pd
from sqlalchemy import select, update, delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert
from database.models import Player, Ranking, Match, MatchStats, Tournament, PlayerSurfaceRecord, HeadToHead, SyncLog
from utils.helpers import safe_int, safe_float, parse_sackmann_date, normalize_surface, get_h2h_key

logger = logging.getLogger(__name__)

BASE_URL = "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master"

CURRENT_YEAR = datetime.now().year
# Sackmann publishes match files per year; load the last 3 completed years
# (current year file may not exist yet — fetch_csv handles 404 gracefully)
YEARS_TO_LOAD = [CURRENT_YEAR, CURRENT_YEAR - 1, CURRENT_YEAR - 2]

# Surface normalization
SURFACE_MAP = {
    "Hard": "hard",
    "Clay": "clay",
    "Grass": "grass",
    "Carpet": "indoor",
    "Indoor": "indoor",
}

# Tournament level map
LEVEL_MAP = {
    "G": "G",   # Grand Slam
    "M": "M",   # Masters 1000
    "A": "A",   # ATP 500/250
    "D": "D",   # Davis Cup
    "F": "F",   # Finals
    "C": "C",   # Challenger
    "S": "S",   # Satellite/ITF
}


async def fetch_csv(url: str, client: httpx.AsyncClient) -> Optional[pd.DataFrame]:
    """Fetch a CSV from GitHub and return as DataFrame."""
    try:
        resp = await client.get(url, timeout=30.0)
        if resp.status_code == 404:
            logger.warning(f"CSV not found: {url}")
            return None
        resp.raise_for_status()
        return pd.read_csv(io.StringIO(resp.text), low_memory=False)
    except Exception as e:
        logger.error(f"Failed to fetch {url}: {e}")
        return None


async def sync_players(db: AsyncSession) -> dict:
    """
    Sync player profiles from match participants only (~5k rows, fast).
    Uses 2024 match CSV to find active player IDs rather than all 70k historical players.
    """
    started = time.time()
    stats = {"processed": 0, "inserted": 0, "updated": 0}

    async with httpx.AsyncClient() as client:
        players_df = await fetch_csv(f"{BASE_URL}/atp_players.csv", client)
        # Collect player IDs from recent match years (small CSVs)
        match_player_ids: set[str] = set()
        for yr in YEARS_TO_LOAD:
            m_df = await fetch_csv(f"{BASE_URL}/atp_matches_{yr}.csv", client)
            if m_df is not None:
                for col in ("winner_id", "loser_id"):
                    valid = m_df[col].dropna()
                    match_player_ids.update(str(int(v)) for v in valid)

    if players_df is None or not match_player_ids:
        return stats

    # Vectorized filter using pd.to_numeric (NaN → -1 → never in set) preserving index alignment
    pid_as_str = pd.to_numeric(players_df["player_id"], errors="coerce").fillna(-1).astype(int).astype(str)
    mask = pid_as_str.isin(match_player_ids)  # same length as players_df
    players_df = players_df[mask]

    rows_to_insert: list[dict] = []
    for _, row in players_df.iterrows():
        stats["processed"] += 1
        pid = str(int(row["player_id"]))

        dob = None
        if pd.notna(row.get("dob")) and str(row["dob"]).strip() not in ("", "nan", "0"):
            try:
                dob_str = str(int(row["dob"])) if isinstance(row["dob"], float) else str(row["dob"])
                dob = datetime.strptime(dob_str[:8], "%Y%m%d").date()
            except Exception:
                pass

        raw_cc = row.get("ioc") or row.get("country_code")
        cc = str(raw_cc).strip().upper()[:3] if pd.notna(raw_cc) else None

        rows_to_insert.append({
            "id": pid,
            "first_name": str(row.get("name_first", "")).strip() or None,
            "last_name":  str(row.get("name_last",  "")).strip() or None,
            "name": f"{str(row.get('name_first','')).strip()} {str(row.get('name_last','')).strip()}".strip(),
            "hand": str(row.get("hand", "")).strip()[:1].upper() or None,
            "dob": dob,
            "country_code": cc,
            "height_cm": safe_int(row.get("height")),
            "sackmann_id": safe_int(row.get("player_id")),
            "is_active": True,
        })
        stats["inserted"] += 1

    BATCH = 500
    for i in range(0, len(rows_to_insert), BATCH):
        chunk = rows_to_insert[i:i + BATCH]
        ins = pg_insert(Player).values(chunk)
        ins = ins.on_conflict_do_update(
            index_elements=["id"],
            set_={col: getattr(ins.excluded, col) for col in chunk[0] if col != "id"},
        )
        await db.execute(ins)
    if rows_to_insert:
        await db.commit()

    logger.info(f"sync_players: {stats} ({time.time()-started:.1f}s)")
    return stats


async def sync_rankings(db: AsyncSession) -> dict:
    """
    Sync current ATP rankings — only the LATEST ranking date (~1 500 rows).
    atp_rankings_current.csv has 92k rows (weekly history); we keep only the newest.
    """
    started = time.time()
    stats = {"processed": 0, "inserted": 0, "updated": 0}

    async with httpx.AsyncClient() as client:
        df = await fetch_csv(f"{BASE_URL}/atp_rankings_current.csv", client)

    if df is None:
        return stats

    player_col = "player" if "player" in df.columns else "player_id"

    # Keep only the most-recent ranking date (vectorised, no iterrows)
    df["ranking_date_parsed"] = pd.to_datetime(df["ranking_date"], format="%Y%m%d", errors="coerce")
    latest_date = df["ranking_date_parsed"].max()
    df = df[df["ranking_date_parsed"] == latest_date].copy()
    rank_date = latest_date.date() if pd.notna(latest_date) else date.today()

    # Build insert rows — deduplicate by player (keep lowest rank per player)
    valid = df[pd.to_numeric(df[player_col], errors="coerce").notna()].copy()
    valid["rank_num"] = pd.to_numeric(valid["rank"], errors="coerce")
    valid = valid.sort_values("rank_num").drop_duplicates(subset=[player_col], keep="first")
    stats["processed"] = len(valid)

    rows_to_insert = [
        {
            "player_id": str(int(row[player_col])),
            "ranking_date": rank_date,
            "rank": safe_int(row.get("rank")),
            "points": safe_int(row.get("points")),
            "tour": "ATP",
        }
        for _, row in valid.iterrows()
    ]
    stats["inserted"] = len(rows_to_insert)

    if rows_to_insert:
        # Single batch (≤2000 rows)
        stmt = pg_insert(Ranking).values(rows_to_insert)
        stmt = stmt.on_conflict_do_update(
            index_elements=["player_id", "ranking_date"],
            set_={"rank": stmt.excluded.rank, "points": stmt.excluded.points},
        )
        await db.execute(stmt)
        await db.commit()

        # Also mark those players as active in the players table
        pids = [r["player_id"] for r in rows_to_insert]
        from sqlalchemy import update as sa_update
        await db.execute(
            sa_update(Player).where(Player.id.in_(pids)).values(is_active=True)
        )
        await db.commit()

    logger.info(f"sync_rankings: {stats} latest_date={rank_date} ({time.time()-started:.1f}s)")
    return stats


async def sync_matches(db: AsyncSession, year: int) -> dict:
    """Sync match results for a given year from Sackmann."""
    stats = {"processed": 0, "inserted": 0, "updated": 0}

    async with httpx.AsyncClient() as client:
        # Stats are embedded in the main match CSV (columns: w_ace, w_df, w_svpt, etc.)
        matches_df = await fetch_csv(f"{BASE_URL}/atp_matches_{year}.csv", client)

    if matches_df is None:
        logger.warning(f"No match data for {year}")
        return stats

    from database.models import MatchStats

    BATCH = 500
    tourns_seen: dict = {}
    match_batch: list[dict] = []
    ms_batch: list[dict] = []

    async def _flush_matches(rows: list[dict]) -> None:
        if not rows:
            return
        stmt = pg_insert(Match).values(rows)
        skip = {"id", "tournament_id", "round", "winner_id", "loser_id"}
        stmt = stmt.on_conflict_do_update(
            index_elements=["tournament_id", "round", "winner_id", "loser_id"],
            set_={k: getattr(stmt.excluded, k) for k in rows[0] if k not in skip},
        )
        await db.execute(stmt)
        await db.commit()

    async def _flush_stats(rows: list[dict]) -> None:
        if not rows:
            return
        stmt = pg_insert(MatchStats).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["match_id"],
            set_={k: getattr(stmt.excluded, k) for k in rows[0] if k != "match_id"},
        )
        await db.execute(stmt)
        await db.commit()

    for idx, row in matches_df.iterrows():
        stats["processed"] += 1
        try:
            tid = row.get("tourney_id", "")
            tid_str = f"{year}_{str(tid).replace(str(year)+'_','')}"

            # Upsert tournament (one at a time, but only once per tournament)
            # IMPORTANT: commit immediately so the tournament is always in the DB
            # before any match row references it. A later rollback (for a bad
            # match row) must NOT undo the tournament insert, otherwise matches
            # that reference it will violate the FK constraint.
            if tid_str not in tourns_seen:
                t_date = parse_sackmann_date(row.get("tourney_date"))
                tourn_data = {
                    "id": tid_str,
                    "sackmann_id": safe_int(str(tid).split("_")[-1] if "_" in str(tid) else tid),
                    "name": str(row.get("tourney_name", "Unknown")).strip(),
                    "year": year,
                    "start_date": t_date,
                    "surface": normalize_surface(str(row.get("surface", ""))),
                    "level": str(row.get("tourney_level", "A")).strip(),
                    "draw_size": safe_int(row.get("draw_size")),
                    "status": "completed" if t_date and t_date < date.today() else "upcoming",
                }
                t_stmt = pg_insert(Tournament).values(**tourn_data)
                t_stmt = t_stmt.on_conflict_do_update(
                    index_elements=["id"],
                    set_={k: v for k, v in tourn_data.items() if k != "id"}
                )
                await db.execute(t_stmt)
                await db.commit()  # eager commit — survives any later rollback
                tourns_seen[tid_str] = True

            w_id = str(int(row["winner_id"])) if pd.notna(row.get("winner_id")) else None
            l_id = str(int(row["loser_id"])) if pd.notna(row.get("loser_id")) else None
            if not w_id or not l_id:
                continue

            match_date = parse_sackmann_date(row.get("tourney_date"))
            rnd = str(row.get("round", "")).strip()
            match_id = f"{tid_str}_{rnd}_{w_id}_{l_id}"

            match_batch.append({
                "id": match_id,
                "tournament_id": tid_str,
                "match_date": match_date,
                "round": rnd,
                "best_of": safe_int(row.get("best_of")) or 3,
                "surface": normalize_surface(str(row.get("surface", ""))),
                "winner_id": w_id,
                "loser_id": l_id,
                "winner_name": str(row.get("winner_name", "")).strip(),
                "loser_name": str(row.get("loser_name", "")).strip(),
                "winner_rank": safe_int(row.get("winner_rank")),
                "loser_rank": safe_int(row.get("loser_rank")),
                "winner_seed": safe_int(row.get("winner_seed")),
                "loser_seed": safe_int(row.get("loser_seed")),
                "score": str(row.get("score", "")).strip() or None,
                "minutes": safe_int(row.get("minutes")),
                "status": "completed",
            })
            stats["inserted"] += 1

            if any(pd.notna(row.get(c)) for c in ("w_ace", "w_df", "w_svpt")):
                ms_batch.append({
                    "match_id": match_id,
                    "w_aces":          safe_int(row.get("w_ace")),
                    "w_double_faults": safe_int(row.get("w_df")),
                    "w_serve_pts":     safe_int(row.get("w_svpt")),
                    "w_first_in":      safe_int(row.get("w_1stIn")),
                    "w_first_won":     safe_int(row.get("w_1stWon")),
                    "w_second_won":    safe_int(row.get("w_2ndWon")),
                    "w_serve_games":   safe_int(row.get("w_SvGms")),
                    "w_break_pts_saved": safe_int(row.get("w_bpSaved")),
                    "w_break_pts_faced": safe_int(row.get("w_bpFaced")),
                    "l_aces":          safe_int(row.get("l_ace")),
                    "l_double_faults": safe_int(row.get("l_df")),
                    "l_serve_pts":     safe_int(row.get("l_svpt")),
                    "l_first_in":      safe_int(row.get("l_1stIn")),
                    "l_first_won":     safe_int(row.get("l_1stWon")),
                    "l_second_won":    safe_int(row.get("l_2ndWon")),
                    "l_serve_games":   safe_int(row.get("l_SvGms")),
                    "l_break_pts_saved": safe_int(row.get("l_bpSaved")),
                    "l_break_pts_faced": safe_int(row.get("l_bpFaced")),
                })

            if len(match_batch) >= BATCH:
                await _flush_matches(match_batch)
                await _flush_stats(ms_batch)
                match_batch.clear()
                ms_batch.clear()

        except Exception as e:
            logger.error(f"Error processing match row {idx}: {e}")
            try:
                await db.rollback()  # recover session from error state
            except Exception:
                pass
            match_batch.clear()  # discard the failed batch so we don't retry it
            ms_batch.clear()
            continue

    await _flush_matches(match_batch)
    await _flush_stats(ms_batch)
    logger.info(f"sync_matches {year}: {stats}")
    return stats


async def compute_surface_records(db: AsyncSession) -> dict:
    """
    Compute and store win/loss surface records for all players from match data.
    Called after sync_matches to keep PlayerSurfaceRecord up to date.
    """
    stats = {"processed": 0}

    from sqlalchemy import text

    # Use raw SQL for efficiency -- aggregate match data + stats into surface records
    sql = text("""
        WITH wl AS (
            SELECT winner_id AS player_id, surface,
                   EXTRACT(YEAR FROM match_date)::int AS year,
                   1 AS wins, 0 AS losses,
                   m.id AS match_id, TRUE AS is_winner
            FROM matches m WHERE status = 'completed' AND match_date IS NOT NULL
            UNION ALL
            SELECT loser_id, surface,
                   EXTRACT(YEAR FROM match_date)::int,
                   0, 1,
                   m.id, FALSE
            FROM matches m WHERE status = 'completed' AND match_date IS NOT NULL
        ),
        stats_join AS (
            SELECT wl.player_id, wl.surface, wl.year,
                   SUM(wl.wins)    AS wins,
                   SUM(wl.losses)  AS losses,
                   COUNT(*)        AS n,
                   AVG(CASE WHEN wl.is_winner THEN ms.w_aces     ELSE ms.l_aces     END) AS avg_aces,
                   AVG(CASE WHEN wl.is_winner THEN ms.w_double_faults ELSE ms.l_double_faults END) AS avg_dfs,
                   AVG(CASE WHEN wl.is_winner AND ms.w_serve_pts > 0
                        THEN 100.0 * ms.w_first_in / ms.w_serve_pts
                        WHEN NOT wl.is_winner AND ms.l_serve_pts > 0
                        THEN 100.0 * ms.l_first_in / ms.l_serve_pts
                        ELSE NULL END) AS avg_first_in_pct,
                   AVG(CASE WHEN wl.is_winner AND ms.w_first_in > 0
                        THEN 100.0 * ms.w_first_won / ms.w_first_in
                        WHEN NOT wl.is_winner AND ms.l_first_in > 0
                        THEN 100.0 * ms.l_first_won / ms.l_first_in
                        ELSE NULL END) AS avg_first_won_pct,
                   AVG(CASE WHEN wl.is_winner AND (ms.w_serve_pts - ms.w_first_in) > 0
                        THEN 100.0 * ms.w_second_won / (ms.w_serve_pts - ms.w_first_in)
                        WHEN NOT wl.is_winner AND (ms.l_serve_pts - ms.l_first_in) > 0
                        THEN 100.0 * ms.l_second_won / (ms.l_serve_pts - ms.l_first_in)
                        ELSE NULL END) AS avg_second_won_pct,
                   AVG(CASE WHEN wl.is_winner AND ms.w_serve_games > 0
                        THEN 100.0 * (ms.w_serve_games - ms.w_break_pts_faced) / ms.w_serve_games
                        WHEN NOT wl.is_winner AND ms.l_serve_games > 0
                        THEN 100.0 * (ms.l_serve_games - ms.l_break_pts_faced) / ms.l_serve_games
                        ELSE NULL END) AS avg_hold_pct,
                   AVG(CASE WHEN wl.is_winner AND ms.l_break_pts_faced > 0
                        THEN 100.0 * (ms.l_break_pts_faced - ms.l_break_pts_saved) / ms.l_break_pts_faced
                        WHEN NOT wl.is_winner AND ms.w_break_pts_faced > 0
                        THEN 100.0 * (ms.w_break_pts_faced - ms.w_break_pts_saved) / ms.w_break_pts_faced
                        ELSE NULL END) AS avg_break_pct,
                   -- Return pts won = 100 - opponent's service pts won %
                   AVG(CASE WHEN wl.is_winner AND ms.l_serve_pts > 0
                        THEN 100.0 - 100.0 * (COALESCE(ms.l_first_won,0) + COALESCE(ms.l_second_won,0))::float / ms.l_serve_pts
                        WHEN NOT wl.is_winner AND ms.w_serve_pts > 0
                        THEN 100.0 - 100.0 * (COALESCE(ms.w_first_won,0) + COALESCE(ms.w_second_won,0))::float / ms.w_serve_pts
                        ELSE NULL END) AS avg_return_pts_won_pct
            FROM wl
            LEFT JOIN match_stats ms ON ms.match_id = wl.match_id
            GROUP BY wl.player_id, wl.surface, wl.year
        )
        INSERT INTO player_surface_records
            (player_id, surface, year, wins, losses,
             avg_aces, avg_dfs, avg_first_in_pct, avg_first_won_pct,
             avg_second_won_pct, avg_hold_pct, avg_break_pct, avg_return_pts_won_pct)
        SELECT player_id, surface, year, wins, losses,
               avg_aces, avg_dfs, avg_first_in_pct, avg_first_won_pct,
               avg_second_won_pct, avg_hold_pct, avg_break_pct, avg_return_pts_won_pct
        FROM stats_join
        ON CONFLICT (player_id, surface, year)
        DO UPDATE SET
            wins = EXCLUDED.wins, losses = EXCLUDED.losses,
            avg_aces = EXCLUDED.avg_aces, avg_dfs = EXCLUDED.avg_dfs,
            avg_first_in_pct = EXCLUDED.avg_first_in_pct,
            avg_first_won_pct = EXCLUDED.avg_first_won_pct,
            avg_second_won_pct = EXCLUDED.avg_second_won_pct,
            avg_hold_pct = EXCLUDED.avg_hold_pct,
            avg_break_pct = EXCLUDED.avg_break_pct,
            avg_return_pts_won_pct = EXCLUDED.avg_return_pts_won_pct,
            updated_at = NOW()
    """)
    await db.execute(sql)

    await db.commit()
    return stats


async def compute_h2h(db: AsyncSession) -> dict:
    """Compute head-to-head records from match data."""
    stats = {"processed": 0}

    from sqlalchemy import text

    sql = text("""
        WITH h2h_raw AS (
            SELECT
                LEAST(winner_id, loser_id) as p1_id,
                GREATEST(winner_id, loser_id) as p2_id,
                surface,
                match_date,
                winner_id,
                loser_id
            FROM matches
            WHERE status = 'completed'
        ),
        h2h_agg AS (
            SELECT
                p1_id, p2_id,
                SUM(CASE WHEN winner_id = p1_id THEN 1 ELSE 0 END) as p1_wins,
                SUM(CASE WHEN winner_id = p2_id THEN 1 ELSE 0 END) as p2_wins,
                SUM(CASE WHEN winner_id = p1_id AND surface = 'hard' THEN 1 ELSE 0 END) as p1_hard_wins,
                SUM(CASE WHEN winner_id = p2_id AND surface = 'hard' THEN 1 ELSE 0 END) as p2_hard_wins,
                SUM(CASE WHEN winner_id = p1_id AND surface = 'clay' THEN 1 ELSE 0 END) as p1_clay_wins,
                SUM(CASE WHEN winner_id = p2_id AND surface = 'clay' THEN 1 ELSE 0 END) as p2_clay_wins,
                SUM(CASE WHEN winner_id = p1_id AND surface = 'grass' THEN 1 ELSE 0 END) as p1_grass_wins,
                SUM(CASE WHEN winner_id = p2_id AND surface = 'grass' THEN 1 ELSE 0 END) as p2_grass_wins,
                MAX(match_date) as last_match_date
            FROM h2h_raw
            GROUP BY p1_id, p2_id
        )
        INSERT INTO head_to_head (player1_id, player2_id, p1_wins, p2_wins,
            p1_hard_wins, p2_hard_wins, p1_clay_wins, p2_clay_wins,
            p1_grass_wins, p2_grass_wins, last_match_date)
        SELECT p1_id, p2_id, p1_wins, p2_wins,
            p1_hard_wins, p2_hard_wins, p1_clay_wins, p2_clay_wins,
            p1_grass_wins, p2_grass_wins, last_match_date
        FROM h2h_agg
        ON CONFLICT (player1_id, player2_id)
        DO UPDATE SET
            p1_wins = EXCLUDED.p1_wins,
            p2_wins = EXCLUDED.p2_wins,
            p1_hard_wins = EXCLUDED.p1_hard_wins,
            p2_hard_wins = EXCLUDED.p2_hard_wins,
            p1_clay_wins = EXCLUDED.p1_clay_wins,
            p2_clay_wins = EXCLUDED.p2_clay_wins,
            p1_grass_wins = EXCLUDED.p1_grass_wins,
            p2_grass_wins = EXCLUDED.p2_grass_wins,
            last_match_date = EXCLUDED.last_match_date,
            updated_at = NOW()
    """)
    await db.execute(sql)
    await db.commit()
    return stats


async def run_full_sync(db: AsyncSession) -> dict:
    """Run all Sackmann sync jobs in order."""
    from database.connection import AsyncSessionLocal

    # Use a SEPARATE session just for the SyncLog so DB errors in sync
    # don't prevent the log from being updated (avoids "still running" ghosts).
    async with AsyncSessionLocal() as log_db:
        log = SyncLog(job_name="sackmann_full_sync", status="running")
        log_db.add(log)
        await log_db.commit()
        await log_db.refresh(log)
        log_id = log.id

    results = {}
    started = time.time()
    error_msg: str | None = None

    try:
        results["players"] = await sync_players(db)
        results["rankings"] = await sync_rankings(db)

        for year in YEARS_TO_LOAD:
            results[f"matches_{year}"] = await sync_matches(db, year)

        results["surface_records"] = await compute_surface_records(db)
        results["h2h"] = await compute_h2h(db)

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Full sync failed: {e}", exc_info=True)
        try:
            await db.rollback()
        except Exception:
            pass

    # Update the SyncLog via a fresh session — always succeeds
    async with AsyncSessionLocal() as log_db:
        log_row = await log_db.get(SyncLog, log_id)
        if log_row:
            log_row.status = "failed" if error_msg else "success"
            log_row.error_message = error_msg
            log_row.duration_seconds = time.time() - started
            log_row.completed_at = datetime.now()
            if not error_msg:
                log_row.records_processed = sum(r.get("processed", 0) for r in results.values() if isinstance(r, dict))
                log_row.records_inserted = sum(r.get("inserted", 0) for r in results.values() if isinstance(r, dict))
            await log_db.commit()

    return results
