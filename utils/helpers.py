"""Shared utility functions."""

from datetime import date, datetime
from typing import Optional
import re


def safe_int(val) -> Optional[int]:
    try:
        if val is None or (isinstance(val, float) and str(val) == "nan"):
            return None
        return int(float(str(val)))
    except:
        return None


def safe_float(val) -> Optional[float]:
    try:
        if val is None or (isinstance(val, float) and str(val) == "nan"):
            return None
        return float(str(val))
    except:
        return None


def parse_sackmann_date(val) -> Optional[date]:
    """Parse Sackmann's YYYYMMDD date format."""
    try:
        if val is None:
            return None
        s = str(int(float(str(val))))
        if len(s) == 8:
            return datetime.strptime(s, "%Y%m%d").date()
    except:
        pass
    return None


SURFACE_MAP = {
    "hard": "hard",
    "clay": "clay",
    "grass": "grass",
    "carpet": "indoor",
    "indoor": "indoor",
    "hard (i)": "indoor",
}

def normalize_surface(s: str) -> str:
    """Normalize surface string to lowercase canonical form."""
    return SURFACE_MAP.get(s.lower().strip(), "hard")


IOC_TO_ISO = {
    "SRB": "rs", "USA": "us", "ESP": "es", "RUS": "ru", "GER": "de",
    "ITA": "it", "FRA": "fr", "GBR": "gb", "AUS": "au", "GRE": "gr",
    "NOR": "no", "BUL": "bg", "CZE": "cz", "POL": "pl", "ARG": "ar",
    "CAN": "ca", "BEL": "be", "SUI": "ch", "SWE": "se", "AUT": "at",
    "DEN": "dk", "NED": "nl", "POR": "pt", "CRO": "hr", "HUN": "hu",
    "ROU": "ro", "FIN": "fi", "UKR": "ua", "KAZ": "kz", "KOR": "kr",
    "JPN": "jp", "CHI": "cl", "COL": "co", "BRA": "br", "MEX": "mx",
    "RSA": "za", "TUN": "tn", "MON": "mc", "CHN": "cn", "IND": "in",
    "QAT": "qa", "UAE": "ae", "LAT": "lv", "LIT": "lt", "GEO": "ge",
    "MKD": "mk", "BIH": "ba",
}

def ioc_to_iso(code: str) -> str:
    if not code:
        return "xx"
    return IOC_TO_ISO.get(code.upper(), code.lower()[:2])


def get_h2h_key(p1_id: str, p2_id: str) -> tuple:
    """Return canonical (smaller_id, larger_id) tuple for H2H lookup."""
    return (min(p1_id, p2_id), max(p1_id, p2_id))


def calc_hold_pct(serve_games: int, breaks_faced: int) -> Optional[float]:
    if not serve_games or serve_games == 0:
        return None
    return round((serve_games - breaks_faced) / serve_games * 100, 1)


def calc_break_pct(break_pts: int, break_pts_faced: int) -> Optional[float]:
    if not break_pts_faced or break_pts_faced == 0:
        return None
    return round((break_pts - break_pts_faced) / break_pts_faced * 100, 1) if break_pts else 0.0
