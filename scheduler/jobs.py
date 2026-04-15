"""
APScheduler job definitions.
Jobs run within the FastAPI process using AsyncIOScheduler.
"""

import logging
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from database.connection import AsyncSessionLocal

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler(timezone="UTC")


async def _run_with_db(coro_fn, job_name: str):
    """Run an async job with a database session and log results."""
    async with AsyncSessionLocal() as db:
        try:
            logger.info(f"[{job_name}] Starting...")
            result = await coro_fn(db)
            logger.info(f"[{job_name}] Completed: {result}")
        except Exception as e:
            logger.error(f"[{job_name}] Failed: {e}", exc_info=True)


# -- HOURLY JOBS -------------------------------------------------------------

async def job_sync_rankings():
    from ingest.sackmann import sync_rankings
    await _run_with_db(sync_rankings, "hourly_rankings")

async def job_sync_live_scores():
    from ingest.espn import sync_live_scores
    await _run_with_db(sync_live_scores, "hourly_live_scores")

async def job_sync_odds():
    from ingest.odds import sync_odds
    await _run_with_db(sync_odds, "hourly_odds")

async def job_sync_recent_matches():
    """Sync current year matches to pick up latest results."""
    from ingest.sackmann import sync_matches
    from datetime import datetime
    year = datetime.now().year
    async with AsyncSessionLocal() as db:
        try:
            result = await sync_matches(db, year)
            logger.info(f"[hourly_recent_matches] {result}")
        except Exception as e:
            logger.error(f"[hourly_recent_matches] Failed: {e}")


# -- NIGHTLY JOBS ------------------------------------------------------------

async def job_nightly_full_sync():
    """Full nightly sync: players, rankings, matches, H2H, surface records."""
    from ingest.sackmann import run_full_sync
    await _run_with_db(run_full_sync, "nightly_full_sync")

async def job_nightly_compute_h2h():
    from ingest.sackmann import compute_h2h
    await _run_with_db(compute_h2h, "nightly_h2h")

async def job_nightly_surface_records():
    from ingest.sackmann import compute_surface_records
    await _run_with_db(compute_surface_records, "nightly_surface_records")


# -- SCHEDULER SETUP ---------------------------------------------------------

def setup_scheduler():
    """Register all jobs with the scheduler."""

    # Hourly jobs
    scheduler.add_job(
        job_sync_rankings,
        IntervalTrigger(hours=1),
        id="hourly_rankings",
        replace_existing=True,
        max_instances=1,
    )
    scheduler.add_job(
        job_sync_live_scores,
        IntervalTrigger(minutes=5),
        id="live_scores_5min",
        replace_existing=True,
        max_instances=1,
    )
    scheduler.add_job(
        job_sync_odds,
        IntervalTrigger(minutes=10),
        id="odds_10min",
        replace_existing=True,
        max_instances=1,
    )
    scheduler.add_job(
        job_sync_recent_matches,
        IntervalTrigger(hours=2),
        id="hourly_matches",
        replace_existing=True,
        max_instances=1,
    )

    # Nightly jobs (3 AM UTC)
    scheduler.add_job(
        job_nightly_full_sync,
        CronTrigger(hour=3, minute=0),
        id="nightly_full_sync",
        replace_existing=True,
        max_instances=1,
    )
    scheduler.add_job(
        job_nightly_compute_h2h,
        CronTrigger(hour=3, minute=30),
        id="nightly_h2h",
        replace_existing=True,
        max_instances=1,
    )
    scheduler.add_job(
        job_nightly_surface_records,
        CronTrigger(hour=4, minute=0),
        id="nightly_surface_records",
        replace_existing=True,
        max_instances=1,
    )

    scheduler.start()
    logger.info("Scheduler started with all jobs registered")
    return scheduler
