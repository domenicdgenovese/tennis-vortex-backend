"""
Tennis Vortex Match Prediction Engine v2
18-factor weighted model with live data from database.
"""

import math
from typing import Dict, Optional, Tuple
from dataclasses import dataclass, field


@dataclass
class PlayerFeatures:
    """Normalized player features for prediction."""
    player_id: str
    name: str
    rank: int = 999
    elo: float = 1500.0
    surface_win_pct: float = 50.0    # Win % on current surface (career)
    ytd_win_pct: float = 50.0        # Year-to-date win %
    form_l10: float = 50.0           # Win % last 10 matches
    form_l5: float = 50.0            # Win % last 5 matches
    hold_pct: float = 70.0           # Service hold %
    break_pct: float = 30.0          # Break opportunity conversion %
    first_in_pct: float = 60.0       # First serve in %
    first_won_pct: float = 72.0      # 1st serve pts won %
    second_won_pct: float = 50.0     # 2nd serve pts won %
    ace_rate: float = 5.0            # Aces per 100 service pts
    df_rate: float = 2.5             # DFs per 100 service pts
    return_pts_won_pct: float = 40.0 # Return pts won %
    fatigue_score: float = 1.0       # 1.0 = fresh, 0.85 = fatigued
    titles_ytd: int = 0
    gs_titles: int = 0


@dataclass
class MatchContext:
    surface: str = "hard"             # hard, clay, grass, indoor
    best_of: int = 3
    court_speed: str = "medium"       # very_slow, slow, medium, fast, very_fast
    weather: str = "clear"            # clear, windy, humid, cold, rainy, indoor
    altitude: str = "sea"             # sea, mid, high
    crowd: str = "neutral"            # neutral, p1home, p2home
    tournament_level: str = "A"       # G, M, A, C


# Default weights -- these can be overridden via API
DEFAULT_WEIGHTS = {
    "elo_gap":              0.22,
    "surface_win_pct":     0.15,
    "form_l10":            0.12,
    "form_l5":             0.08,
    "rank_gap":            0.10,
    "hold_pct":            0.08,
    "break_pct":           0.06,
    "h2h_record":          0.07,
    "h2h_surface":         0.04,
    "fatigue":             0.04,
    "ytd_record":          0.02,
    "serve_dominance":     0.02,
}

SURFACE_ACE_MULT = {"grass": 1.3, "hard": 1.0, "clay": 0.75, "indoor": 1.1}
SURFACE_HOLD_MULT = {"grass": 1.1, "hard": 1.0, "clay": 0.92, "indoor": 1.05}

WEATHER_ADJUST = {
    "windy": {"ace_mult": 0.85, "big_server_penalty": -0.03},
    "humid": {"second_serve_penalty": -0.02},
    "cold": {"serve_penalty": -0.015},
    "rainy": {"clay_bonus_cancel": True},
    "clear": {},
    "indoor": {},
}

ALTITUDE_ADJUST = {
    "high": {"ace_bonus": 0.04, "serve_mult": 1.08},
    "mid":  {"ace_bonus": 0.02, "serve_mult": 1.03},
    "sea":  {},
}


def elo_expected(elo_a: float, elo_b: float) -> float:
    """Standard Elo expected score formula."""
    return 1.0 / (1.0 + 10 ** ((elo_b - elo_a) / 400.0))


def rank_to_score(rank: int) -> float:
    """Convert ranking to 0-100 score (lower rank = higher score)."""
    return max(0, 100 - math.log(max(rank, 1)) * 8)


def form_to_score(form_pct: float) -> float:
    """Convert form % to 0-100 score with sigmoid smoothing."""
    x = (form_pct - 50) / 15
    return 50 + 50 * math.tanh(x)


def predict_match(
    p1: PlayerFeatures,
    p2: PlayerFeatures,
    h2h: Dict,
    ctx: MatchContext,
    weights: Optional[Dict] = None,
    bo: int = 3,
) -> Dict:
    """
    Core prediction function. Returns win probabilities, confidence, projected score,
    and factor-by-factor breakdown.
    """
    W = weights or DEFAULT_WEIGHTS

    factors = {}

    # -- 1. Elo gap ----------------------------------------------------------
    elo_exp = elo_expected(p1.elo, p2.elo)
    factors["elo_gap"] = {"p1": elo_exp, "p2": 1 - elo_exp, "weight": W["elo_gap"]}

    # -- 2. Surface win % ----------------------------------------------------
    p1_surf = p1.surface_win_pct / 100
    p2_surf = p2.surface_win_pct / 100
    surf_total = p1_surf + p2_surf
    p1_surf_norm = p1_surf / surf_total if surf_total > 0 else 0.5
    factors["surface_win_pct"] = {"p1": p1_surf_norm, "p2": 1 - p1_surf_norm, "weight": W["surface_win_pct"]}

    # -- 3. Form last 10 -----------------------------------------------------
    p1_f10 = form_to_score(p1.form_l10) / 100
    p2_f10 = form_to_score(p2.form_l10) / 100
    f10_total = p1_f10 + p2_f10
    p1_f10_norm = p1_f10 / f10_total if f10_total > 0 else 0.5
    factors["form_l10"] = {"p1": p1_f10_norm, "p2": 1 - p1_f10_norm, "weight": W["form_l10"]}

    # -- 4. Form last 5 ------------------------------------------------------
    p1_f5 = form_to_score(p1.form_l5) / 100
    p2_f5 = form_to_score(p2.form_l5) / 100
    f5_total = p1_f5 + p2_f5
    p1_f5_norm = p1_f5 / f5_total if f5_total > 0 else 0.5
    factors["form_l5"] = {"p1": p1_f5_norm, "p2": 1 - p1_f5_norm, "weight": W["form_l5"]}

    # -- 5. Ranking gap ------------------------------------------------------
    p1_rk = rank_to_score(p1.rank) / 100
    p2_rk = rank_to_score(p2.rank) / 100
    rk_total = p1_rk + p2_rk
    p1_rk_norm = p1_rk / rk_total if rk_total > 0 else 0.5
    factors["rank_gap"] = {"p1": p1_rk_norm, "p2": 1 - p1_rk_norm, "weight": W["rank_gap"]}

    # -- 6. Hold % -----------------------------------------------------------
    p1_hold = (p1.hold_pct * SURFACE_HOLD_MULT.get(ctx.surface, 1.0)) / 100
    p2_hold = (p2.hold_pct * SURFACE_HOLD_MULT.get(ctx.surface, 1.0)) / 100
    hold_total = p1_hold + p2_hold
    p1_hold_norm = p1_hold / hold_total if hold_total > 0 else 0.5
    factors["hold_pct"] = {"p1": p1_hold_norm, "p2": 1 - p1_hold_norm, "weight": W["hold_pct"]}

    # -- 7. Break % ----------------------------------------------------------
    p1_break = p1.break_pct / 100
    p2_break = p2.break_pct / 100
    break_total = p1_break + p2_break
    p1_break_norm = p1_break / break_total if break_total > 0 else 0.5
    factors["break_pct"] = {"p1": p1_break_norm, "p2": 1 - p1_break_norm, "weight": W["break_pct"]}

    # -- 8. H2H overall ------------------------------------------------------
    h2h_total = h2h.get("p1_wins", 0) + h2h.get("p2_wins", 0)
    if h2h_total >= 2:
        p1_h2h = h2h.get("p1_wins", 0) / h2h_total
    else:
        p1_h2h = 0.5  # not enough data
    factors["h2h_record"] = {"p1": p1_h2h, "p2": 1 - p1_h2h, "weight": W["h2h_record"]}

    # -- 9. H2H on surface ---------------------------------------------------
    surf_key = ctx.surface
    h2h_surf_total = h2h.get(f"p1_{surf_key}_wins", 0) + h2h.get(f"p2_{surf_key}_wins", 0)
    if h2h_surf_total >= 2:
        p1_h2h_surf = h2h.get(f"p1_{surf_key}_wins", 0) / h2h_surf_total
    else:
        p1_h2h_surf = p1_h2h  # fall back to overall H2H
    factors["h2h_surface"] = {"p1": p1_h2h_surf, "p2": 1 - p1_h2h_surf, "weight": W["h2h_surface"]}

    # -- 10. Fatigue ---------------------------------------------------------
    p1_fat = p1.fatigue_score
    p2_fat = p2.fatigue_score
    fat_total = p1_fat + p2_fat
    p1_fat_norm = p1_fat / fat_total if fat_total > 0 else 0.5
    factors["fatigue"] = {"p1": p1_fat_norm, "p2": 1 - p1_fat_norm, "weight": W["fatigue"]}

    # -- 11. YTD record ------------------------------------------------------
    p1_ytd = p1.ytd_win_pct / 100
    p2_ytd = p2.ytd_win_pct / 100
    ytd_total = p1_ytd + p2_ytd
    p1_ytd_norm = p1_ytd / ytd_total if ytd_total > 0 else 0.5
    factors["ytd_record"] = {"p1": p1_ytd_norm, "p2": 1 - p1_ytd_norm, "weight": W["ytd_record"]}

    # -- 12. Serve dominance (ace rate adjusted for surface) -----------------
    p1_ace = p1.ace_rate * SURFACE_ACE_MULT.get(ctx.surface, 1.0)
    p2_ace = p2.ace_rate * SURFACE_ACE_MULT.get(ctx.surface, 1.0)
    ace_total = p1_ace + p2_ace
    p1_ace_norm = p1_ace / ace_total if ace_total > 0 else 0.5
    factors["serve_dominance"] = {"p1": p1_ace_norm, "p2": 1 - p1_ace_norm, "weight": W["serve_dominance"]}

    # -- Weighted sum --------------------------------------------------------
    p1_raw = sum(f["p1"] * f["weight"] for f in factors.values())
    p2_raw = sum(f["p2"] * f["weight"] for f in factors.values())
    total_raw = p1_raw + p2_raw

    p1_win_prob = p1_raw / total_raw if total_raw > 0 else 0.5
    p2_win_prob = 1.0 - p1_win_prob

    # -- Conditions adjustment -----------------------------------------------
    weather_adj = WEATHER_ADJUST.get(ctx.weather, {})
    alt_adj = ALTITUDE_ADJUST.get(ctx.altitude, {})

    # Court speed: fast courts amplify serve dominance, slow courts amplify baseline
    SPEED_SERVE_MULT = {
        "very_slow": 0.75, "slow": 0.87, "medium": 1.0,
        "fast": 1.15, "very_fast": 1.30,
    }
    speed_mult = SPEED_SERVE_MULT.get(ctx.court_speed, 1.0)
    # Serve-dominant player (higher ace_rate + first_won_pct) benefits from fast courts
    p1_serve_dom = (p1.ace_rate * speed_mult + p1.first_won_pct) / 2
    p2_serve_dom = (p2.ace_rate * speed_mult + p2.first_won_pct) / 2
    p1_baseline  = (p1.return_pts_won_pct + p1.break_pct) / 2
    p2_baseline  = (p2.return_pts_won_pct + p2.break_pct) / 2
    if speed_mult > 1.0:
        # Fast: reward whoever has the bigger serve advantage
        srv_delta = (p1_serve_dom - p2_serve_dom) / 100.0
        p1_win_prob += srv_delta * (speed_mult - 1.0) * 0.15
    elif speed_mult < 1.0:
        # Slow: reward whoever has the bigger return/baseline advantage
        base_delta = (p1_baseline - p2_baseline) / 100.0
        p1_win_prob += base_delta * (1.0 - speed_mult) * 0.15

    # Windy conditions hurt big servers more
    if weather_adj.get("big_server_penalty") and p1.ace_rate > p2.ace_rate:
        p1_win_prob += weather_adj["big_server_penalty"]
    elif weather_adj.get("big_server_penalty") and p2.ace_rate > p1.ace_rate:
        p2_win_prob += abs(weather_adj["big_server_penalty"])

    # Humid / cold slightly suppress ace rates (tighter, heavier balls)
    if ctx.weather in ("humid", "cold"):
        ace_pen = -0.01
        if p1.ace_rate > p2.ace_rate:
            p1_win_prob += ace_pen
        else:
            p2_win_prob += abs(ace_pen)

    # Crowd factor
    crowd_adj = {"p1home": 0.03, "p2home": -0.03, "neutral": 0.0}
    p1_win_prob += crowd_adj.get(ctx.crowd, 0.0)

    # High altitude helps big servers
    if alt_adj.get("ace_bonus"):
        if p1.ace_rate > p2.ace_rate:
            p1_win_prob += alt_adj["ace_bonus"]
        else:
            p2_win_prob += alt_adj["ace_bonus"]

    # BO5 amplifies favourite — compress toward 50% less (more variance in BO3)
    if bo == 5:
        p1_win_prob = 0.5 + (p1_win_prob - 0.5) * 1.08

    # Clamp
    p1_win_prob = max(0.05, min(0.95, p1_win_prob))
    p2_win_prob = 1.0 - p1_win_prob

    # -- Confidence score ----------------------------------------------------
    # High when factors agree; low when split
    p1_votes = sum(1 for f in factors.values() if f["p1"] > 0.5)
    agreement_ratio = max(p1_votes, len(factors) - p1_votes) / len(factors)
    magnitude = abs(p1_win_prob - 0.5) * 2  # 0-1
    confidence = round((0.6 * agreement_ratio + 0.4 * magnitude) * 100, 1)

    # -- Projected scoreline -------------------------------------------------
    projected_score = _project_score(p1_win_prob, p2_win_prob, bo)

    return {
        "p1_win_probability": round(p1_win_prob * 100, 1),
        "p2_win_probability": round(p2_win_prob * 100, 1),
        "confidence_score": confidence,
        "projected_score": projected_score,
        "factors": {k: {"p1": round(v["p1"]*100,1), "p2": round(v["p2"]*100,1), "weight": round(v["weight"]*100,1)} for k, v in factors.items()},
        "model_version": "v2.0",
    }


def _project_score(p1_prob: float, p2_prob: float, bo: int) -> str:
    """Project most likely scoreline."""
    if bo == 3:
        # P(2-0), P(2-1)
        p_2_0 = p1_prob ** 2
        p_1_2 = p2_prob ** 2
        p_2_1 = 2 * p1_prob * p2_prob * p1_prob
        p_1_2_w = 2 * p1_prob * p2_prob * p2_prob

        if p1_prob > 0.65:
            return "2-0" if p_2_0 > p_2_1 else "2-1"
        elif p1_prob < 0.35:
            return "0-2" if p_1_2 > p_1_2_w else "1-2"
        else:
            return "2-1" if p1_prob > 0.5 else "1-2"
    else:  # BO5
        if p1_prob > 0.7:
            return "3-0" if p1_prob > 0.80 else "3-1"
        elif p1_prob < 0.3:
            return "0-3" if p1_prob < 0.20 else "1-3"
        else:
            return "3-2" if p1_prob > 0.5 else "2-3"
