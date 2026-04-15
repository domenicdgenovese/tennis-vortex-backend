"""SQLAlchemy ORM models for Tennis Vortex."""

from datetime import datetime, date
from typing import Optional
from sqlalchemy import (
    String, Integer, Float, Boolean, Date, DateTime,
    ForeignKey, Text, JSON, UniqueConstraint, Index
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func
from database.connection import Base


class Player(Base):
    __tablename__ = "players"

    id = mapped_column(String(20), primary_key=True)  # ATP player_id or sackmann_id
    name = mapped_column(String(100), nullable=False, index=True)
    first_name = mapped_column(String(50))
    last_name = mapped_column(String(50))
    country_code = mapped_column(String(3), index=True)  # IOC 3-letter
    iso_code = mapped_column(String(2))                   # ISO 2-letter for flags
    dob = mapped_column(Date)
    height_cm = mapped_column(Integer)
    hand = mapped_column(String(1))  # R, L, U
    turned_pro = mapped_column(Integer)
    photo_url = mapped_column(String(500))
    atp_code = mapped_column(String(10))  # ATP website 4-char code
    sackmann_id = mapped_column(Integer, index=True)  # Sackmann numeric ID
    is_active = mapped_column(Boolean, default=True)
    created_at = mapped_column(DateTime, server_default=func.now())
    updated_at = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    rankings = relationship("Ranking", back_populates="player", order_by="Ranking.ranking_date.desc()")
    surface_records = relationship("PlayerSurfaceRecord", back_populates="player")


class Ranking(Base):
    __tablename__ = "rankings"
    __table_args__ = (
        UniqueConstraint("player_id", "ranking_date", name="uq_ranking_player_date"),
        Index("ix_rankings_date", "ranking_date"),
    )

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    player_id = mapped_column(String(20), ForeignKey("players.id"), nullable=False, index=True)
    ranking_date = mapped_column(Date, nullable=False)
    rank = mapped_column(Integer, nullable=False)
    points = mapped_column(Integer)
    tour = mapped_column(String(5), default="ATP")  # ATP, WTA, CH (Challenger)
    created_at = mapped_column(DateTime, server_default=func.now())

    player = relationship("Player", back_populates="rankings")


class Tournament(Base):
    __tablename__ = "tournaments"
    __table_args__ = (
        UniqueConstraint("sackmann_id", "year", name="uq_tournament_sackmann_year"),
    )

    id = mapped_column(String(30), primary_key=True)  # e.g. "2026_AO"
    sackmann_id = mapped_column(Integer, index=True)
    name = mapped_column(String(200), nullable=False)
    short_name = mapped_column(String(50))
    year = mapped_column(Integer, nullable=False)
    start_date = mapped_column(Date)
    end_date = mapped_column(Date)
    surface = mapped_column(String(20))  # Hard, Clay, Grass, Carpet
    level = mapped_column(String(5))     # G (Grand Slam), M (Masters), A (ATP), C (Challenger)
    draw_size = mapped_column(Integer)
    prize_money = mapped_column(Integer)
    location = mapped_column(String(100))
    country = mapped_column(String(3))
    indoor = mapped_column(Boolean, default=False)
    espn_id = mapped_column(String(50))
    status = mapped_column(String(20), default="upcoming")  # upcoming, live, completed
    created_at = mapped_column(DateTime, server_default=func.now())
    updated_at = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    matches = relationship("Match", back_populates="tournament")


class Match(Base):
    __tablename__ = "matches"
    __table_args__ = (
        UniqueConstraint("tournament_id", "round", "winner_id", "loser_id", name="uq_match_tourn_round_players"),
        Index("ix_matches_date", "match_date"),
        Index("ix_matches_winner", "winner_id"),
        Index("ix_matches_loser", "loser_id"),
    )

    id = mapped_column(String(50), primary_key=True)
    tournament_id = mapped_column(String(30), ForeignKey("tournaments.id"), index=True)
    match_date = mapped_column(Date, index=True)
    round = mapped_column(String(10))   # R128, R64, R32, R16, QF, SF, F
    best_of = mapped_column(Integer, default=3)
    surface = mapped_column(String(20))
    indoor = mapped_column(Boolean, default=False)
    winner_id = mapped_column(String(20), ForeignKey("players.id"), index=True)
    loser_id = mapped_column(String(20), ForeignKey("players.id"), index=True)
    winner_name = mapped_column(String(100))
    loser_name = mapped_column(String(100))
    winner_rank = mapped_column(Integer)
    loser_rank = mapped_column(Integer)
    winner_seed = mapped_column(Integer)
    loser_seed = mapped_column(Integer)
    score = mapped_column(String(100))
    status = mapped_column(String(20), default="completed")  # scheduled, live, completed, cancelled
    minutes = mapped_column(Integer)
    espn_id = mapped_column(String(50))
    sackmann_match_num = mapped_column(Integer)
    created_at = mapped_column(DateTime, server_default=func.now())
    updated_at = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    tournament = relationship("Tournament", back_populates="matches")
    stats = relationship("MatchStats", back_populates="match", uselist=False)


class MatchStats(Base):
    __tablename__ = "match_stats"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    match_id = mapped_column(String(50), ForeignKey("matches.id"), unique=True, index=True)
    # Winner serve stats
    w_aces = mapped_column(Integer)
    w_double_faults = mapped_column(Integer)
    w_serve_pts = mapped_column(Integer)
    w_first_in = mapped_column(Integer)
    w_first_won = mapped_column(Integer)
    w_second_won = mapped_column(Integer)
    w_serve_games = mapped_column(Integer)
    w_break_pts_saved = mapped_column(Integer)
    w_break_pts_faced = mapped_column(Integer)
    # Loser serve stats
    l_aces = mapped_column(Integer)
    l_double_faults = mapped_column(Integer)
    l_serve_pts = mapped_column(Integer)
    l_first_in = mapped_column(Integer)
    l_first_won = mapped_column(Integer)
    l_second_won = mapped_column(Integer)
    l_serve_games = mapped_column(Integer)
    l_break_pts_saved = mapped_column(Integer)
    l_break_pts_faced = mapped_column(Integer)

    match = relationship("Match", back_populates="stats")


class PlayerSurfaceRecord(Base):
    __tablename__ = "player_surface_records"
    __table_args__ = (
        UniqueConstraint("player_id", "surface", "year", name="uq_player_surface_year"),
    )

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    player_id = mapped_column(String(20), ForeignKey("players.id"), index=True)
    surface = mapped_column(String(20), nullable=False)  # hard, clay, grass, indoor, overall
    year = mapped_column(Integer)  # NULL = career
    wins = mapped_column(Integer, default=0)
    losses = mapped_column(Integer, default=0)
    titles = mapped_column(Integer, default=0)
    # Rolling serve/return averages on this surface
    avg_aces = mapped_column(Float)
    avg_dfs = mapped_column(Float)
    avg_first_in_pct = mapped_column(Float)
    avg_first_won_pct = mapped_column(Float)
    avg_second_won_pct = mapped_column(Float)
    avg_hold_pct = mapped_column(Float)
    avg_break_pct = mapped_column(Float)
    avg_return_pts_won_pct = mapped_column(Float)
    elo = mapped_column(Float)
    updated_at = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    player = relationship("Player", back_populates="surface_records")


class HeadToHead(Base):
    __tablename__ = "head_to_head"
    __table_args__ = (
        UniqueConstraint("player1_id", "player2_id", name="uq_h2h_players"),
    )

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    # player1_id is always the lexicographically smaller ID for deduplication
    player1_id = mapped_column(String(20), ForeignKey("players.id"), index=True)
    player2_id = mapped_column(String(20), ForeignKey("players.id"), index=True)
    p1_wins = mapped_column(Integer, default=0)
    p2_wins = mapped_column(Integer, default=0)
    # Surface breakdown
    p1_hard_wins = mapped_column(Integer, default=0)
    p2_hard_wins = mapped_column(Integer, default=0)
    p1_clay_wins = mapped_column(Integer, default=0)
    p2_clay_wins = mapped_column(Integer, default=0)
    p1_grass_wins = mapped_column(Integer, default=0)
    p2_grass_wins = mapped_column(Integer, default=0)
    p1_indoor_wins = mapped_column(Integer, default=0)
    p2_indoor_wins = mapped_column(Integer, default=0)
    last_match_date = mapped_column(Date)
    last_match_winner_id = mapped_column(String(20))
    recent_matches = mapped_column(JSON)  # Last 10 matches as JSON
    updated_at = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())


class OddsSnapshot(Base):
    __tablename__ = "odds_snapshots"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    match_id = mapped_column(String(50), index=True)
    player1_id = mapped_column(String(20), index=True)
    player2_id = mapped_column(String(20), index=True)
    player1_name = mapped_column(String(100))
    player2_name = mapped_column(String(100))
    bookmaker = mapped_column(String(50))
    player1_odds = mapped_column(Float)   # American odds
    player2_odds = mapped_column(Float)
    player1_implied_pct = mapped_column(Float)
    player2_implied_pct = mapped_column(Float)
    tournament_name = mapped_column(String(200))
    snapshot_at = mapped_column(DateTime, server_default=func.now(), index=True)
    source = mapped_column(String(50), default="odds_api")


class Projection(Base):
    __tablename__ = "projections"
    __table_args__ = (
        UniqueConstraint("player1_id", "player2_id", "tournament_id", "round", name="uq_projection"),
    )

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    player1_id = mapped_column(String(20), ForeignKey("players.id"), index=True)
    player2_id = mapped_column(String(20), ForeignKey("players.id"), index=True)
    tournament_id = mapped_column(String(30))
    round = mapped_column(String(10))
    surface = mapped_column(String(20))
    best_of = mapped_column(Integer, default=3)
    p1_win_probability = mapped_column(Float)
    p2_win_probability = mapped_column(Float)
    confidence_score = mapped_column(Float)   # 0-100
    model_version = mapped_column(String(20))
    factors = mapped_column(JSON)             # Breakdown of contributing factors
    projected_score = mapped_column(String(100))
    created_at = mapped_column(DateTime, server_default=func.now())
    updated_at = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())


class SyncLog(Base):
    __tablename__ = "sync_logs"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_name = mapped_column(String(100), nullable=False, index=True)
    status = mapped_column(String(20), nullable=False)  # success, failed, running
    records_processed = mapped_column(Integer, default=0)
    records_inserted = mapped_column(Integer, default=0)
    records_updated = mapped_column(Integer, default=0)
    error_message = mapped_column(Text)
    duration_seconds = mapped_column(Float)
    started_at = mapped_column(DateTime, server_default=func.now(), index=True)
    completed_at = mapped_column(DateTime)
    metadata = mapped_column(JSON)
