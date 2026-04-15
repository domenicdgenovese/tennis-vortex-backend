"""
Tennis Vortex API -- FastAPI Backend
Serves live ATP data to the Tennis Vortex frontend.
"""

import logging
import os
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("tennis_vortex")

FRONTEND_URL = os.getenv("FRONTEND_URL", "https://astonishing-travesseiro-141d6f.netlify.app")
ENVIRONMENT = os.getenv("ENVIRONMENT", "production")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    # Create database tables
    from database.connection import create_tables
    await create_tables()
    logger.info("Database tables ready")

    # Start scheduler
    from scheduler.jobs import setup_scheduler
    scheduler = setup_scheduler()
    logger.info("Scheduler running")

    # Run initial data sync in the background so the health check can pass immediately
    import asyncio

    async def _initial_sync():
        from database.connection import AsyncSessionLocal
        from sqlalchemy import select, func
        from database.models import Player
        async with AsyncSessionLocal() as db:
            count = await db.execute(select(func.count(Player.id)))
            player_count = count.scalar()
            if player_count == 0:
                logger.info("Empty database — running initial full sync in background...")
                from ingest.sackmann import run_full_sync
                try:
                    result = await run_full_sync(db)
                    logger.info(f"Initial sync complete: {result}")
                except Exception as e:
                    logger.error(f"Initial sync failed (will retry via scheduler): {e}")

    asyncio.create_task(_initial_sync())

    yield

    # Shutdown
    if scheduler.running:
        scheduler.shutdown()
    logger.info("Scheduler stopped")


app = FastAPI(
    title="Tennis Vortex API",
    description="Live ATP tennis analytics and prediction platform",
    version="2.0.0",
    lifespan=lifespan,
)

# CORS -- allow the Netlify frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if ENVIRONMENT == "development" else [
        FRONTEND_URL,
        "http://localhost:3000",
        "http://localhost:8080",
    ],
    allow_credentials=ENVIRONMENT != "development",
    allow_methods=["*"],
    allow_headers=["*"],
)


# -- Register routers --------------------------------------------------------
from api.players import router as players_router
from api.rankings import router as rankings_router
from api.matches import router as matches_router
from api.tournaments import router as tournaments_router
from api.h2h import router as h2h_router
from api.predictions import router as predictions_router
from api.odds import router as odds_router

app.include_router(players_router)
app.include_router(rankings_router)
app.include_router(matches_router)
app.include_router(tournaments_router)
app.include_router(h2h_router)
app.include_router(predictions_router)
app.include_router(odds_router)


# -- Health and admin endpoints ----------------------------------------------

@app.get("/health")
async def health():
    return {"status": "ok", "version": "2.0.0"}


@app.get("/api/sync-logs")
async def get_sync_logs(limit: int = 50):
    from database.models import SyncLog
    from sqlalchemy import select, desc
    from database.connection import AsyncSessionLocal
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(SyncLog).order_by(desc(SyncLog.started_at)).limit(limit)
        )
        logs = result.scalars().all()
        return [
            {
                "id": l.id,
                "job": l.job_name,
                "status": l.status,
                "records_processed": l.records_processed,
                "records_inserted": l.records_inserted,
                "duration_s": l.duration_seconds,
                "started_at": l.started_at.isoformat() if l.started_at else None,
                "error": l.error_message,
            }
            for l in logs
        ]


@app.post("/api/admin/sync")
async def trigger_sync(job: str = "full"):
    """Admin: manually trigger a sync job."""
    from database.connection import AsyncSessionLocal

    job_map = {
        "full": "ingest.sackmann.run_full_sync",
        "rankings": "ingest.sackmann.sync_rankings",
        "players": "ingest.sackmann.sync_players",
        "odds": "ingest.odds.sync_odds",
        "live": "ingest.espn.sync_live_scores",
    }

    if job not in job_map:
        return JSONResponse(status_code=400, content={"error": f"Unknown job: {job}. Valid: {list(job_map.keys())}"})

    import asyncio

    async def _run():
        module, fn = job_map[job].rsplit(".", 1)
        import importlib
        mod = importlib.import_module(module)
        fn_obj = getattr(mod, fn)
        async with AsyncSessionLocal() as db:
            return await fn_obj(db)

    asyncio.create_task(_run())
    return {"message": f"Sync job '{job}' triggered", "status": "running"}


@app.get("/api/scheduler/jobs")
async def get_scheduler_jobs():
    """List all scheduled jobs and their next run times."""
    from scheduler.jobs import scheduler
    jobs = []
    for job in scheduler.get_jobs():
        jobs.append({
            "id": job.id,
            "name": job.name,
            "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
            "trigger": str(job.trigger),
        })
    return {"jobs": jobs}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)), reload=True)
