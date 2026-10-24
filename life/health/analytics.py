# 健康数据周期聚合与个人基线比较（只做描述统计，不做医学判断）

from __future__ import annotations

import calendar
import math
from datetime import date, datetime, timedelta
from statistics import mean
from zoneinfo import ZoneInfo

from . import db

TZ = ZoneInfo("Asia/Shanghai")
METRICS = ("resting_hr", "hrv", "spo2", "stress")
METRIC_NAMES = {
    "resting_hr": "静息心率",
    "hrv": "HRV",
    "spo2": "血氧",
    "stress": "压力",
}
METRIC_UNITS = {"resting_hr": "bpm", "hrv": "ms", "spo2": "%", "stress": "分"}


def avg(values) -> float | None:
    values = [value for value in values if value is not None]
    return mean(values) if values else None


def fmt_min(minutes: float | None) -> str:
    if minutes is None:
        return "—"
    total = int(round(minutes))
    return f"{total // 60}:{total % 60:02d}"


def fmt_hm(minutes: float | None) -> str:
    return fmt_min(minutes)


def fmt_num(value, digits: int = 1, suffix: str = "") -> str:
    if value is None:
        return "—"
    text = f"{value:.{digits}f}" if digits else f"{value:.0f}"
    return f"{text}{suffix}"


def clock_stats(minutes_list: list[float]) -> tuple[str | None, float | None]:
    """返回钟点均值（HH:MM，环形）与环形离散度（分钟）。"""
    values = [value % 1440 for value in minutes_list if value is not None]
    if not values:
        return None, None
    angles = [2 * math.pi * value / 1440 for value in values]
    x = mean(math.cos(angle) for angle in angles)
    y = mean(math.sin(angle) for angle in angles)
    angle = math.atan2(y, x) % (2 * math.pi)
    center = int(round(angle * 1440 / (2 * math.pi))) % 1440
    radius = math.hypot(x, y)
    spread = math.sqrt(max(0.0, -2 * math.log(radius))) * 1440 / (2 * math.pi) if radius else None
    return f"{center // 60:02d}:{center % 60:02d}", spread


def _ms_clock(ms) -> float | None:
    if not ms:
        return None
    moment = datetime.fromtimestamp(ms / 1000, TZ)
    return moment.hour * 60 + moment.minute


def sleep_summary(conn, start: str, end: str) -> dict:
    rows = db.daily_rows(conn, start, end)
    nights = db.main_nights(conn, start, end)
    valid_rows = [row for row in rows if row["total_min"]]
    totals = [row["total_min"] for row in valid_rows]
    night_minutes = [row["night_min"] or 0 for row in valid_rows]
    nap_minutes = [row["nap_min"] or 0 for row in valid_rows]
    scores = [row["sleep_score"] for row in nights.values() if row["sleep_score"]]
    efficiency = [row["efficiency"] for row in nights.values() if row["efficiency"]]
    latency = [row["latency_min"] for row in nights.values() if row["latency_min"]]
    wakeups = [row["wakeup_count"] for row in nights.values() if row["wakeup_count"] is not None]

    deep = sum(row["deep_min"] or 0 for row in nights.values())
    dream = sum(row["dream_min"] or 0 for row in nights.values())
    light = sum(row["light_min"] or 0 for row in nights.values())
    awake = sum(row["awake_min"] or 0 for row in nights.values())
    stage_total = deep + dream + light + awake

    bed_times = [_ms_clock(row["go_bed_ms"]) for row in nights.values()]
    wake_times = [_ms_clock(row["wakeup_ms"]) for row in nights.values()]
    midpoints = []
    for row in nights.values():
        wake = _ms_clock(row["wakeup_ms"])
        if wake is not None and row["total_sleep_min"]:
            midpoints.append(wake - row["total_sleep_min"] / 2)
    bed_mean, bed_spread = clock_stats(bed_times)
    wake_mean, wake_spread = clock_stats(wake_times)
    _, mid_spread = clock_stats(midpoints)

    return {
        "days": len(totals),
        "avg_total": avg(totals),
        "avg_night": avg(night_minutes),
        "avg_nap": avg(nap_minutes),
        "avg_score": avg(scores),
        "avg_efficiency": avg(efficiency),
        "avg_latency": avg(latency),
        "avg_wakeups": avg(wakeups),
        "stage_pct": {
            "深睡": round(100 * deep / stage_total, 1) if stage_total else None,
            "浅睡": round(100 * light / stage_total, 1) if stage_total else None,
            "REM": round(100 * dream / stage_total, 1) if stage_total else None,
            "清醒": round(100 * awake / stage_total, 1) if stage_total else None,
        },
        "bed_mean": bed_mean,
        "wake_mean": wake_mean,
        "bed_spread": bed_spread,
        "wake_spread": wake_spread,
        "midpoint_spread": mid_spread,
        "records": list(nights.values()),
    }


def metric_summary(conn, metric: str, start: str, end: str) -> dict:
    rows = db.metric_rows(conn, start, end)
    values = [
        day[metric]["value"]
        for day in rows.values()
        if metric in day and day[metric]["value"] is not None
    ]
    lows = [day[metric]["min_value"] or day[metric]["value"] for day in rows.values()
            if metric in day and (day[metric]["min_value"] or day[metric]["value"]) is not None]
    highs = [day[metric]["max_value"] or day[metric]["value"] for day in rows.values()
             if metric in day and (day[metric]["max_value"] or day[metric]["value"]) is not None]
    return {
        "n": len(values),
        "avg": avg(values),
        "min": min(lows) if lows else None,
        "max": max(highs) if highs else None,
        "values": {
            day: entry[metric]["value"]
            for day, entry in rows.items()
            if metric in entry and entry[metric]["value"] is not None
        },
    }


def workouts_summary(conn, start: str, end: str) -> dict:
    rows = db.workouts(conn, start, end)
    minutes = [row["active_time_ms"] / 60000 for row in rows if row["active_time_ms"]]
    by_type: dict[int, int] = {}
    for row in rows:
        kind = row["activity_type"]
        by_type[kind] = by_type.get(kind, 0) + 1
    return {
        "n": len(rows),
        "minutes": sum(minutes),
        "distance_km": sum(row["distance_m"] or 0 for row in rows) / 1000,
        "calories": sum(row["calories"] or 0 for row in rows),
        "steps": sum(row["steps"] or 0 for row in rows),
        "avg_hr": avg([row["avg_hr"] for row in rows]),
        "max_hr": max((row["max_hr"] for row in rows if row["max_hr"]), default=None),
        "by_type": dict(sorted(by_type.items(), key=lambda item: -item[1])),
        "rows": rows,
    }


def emotion_summary(conn, start: str, end: str) -> dict:
    rows = db.emotions(conn, start, end)
    readings = sum(row["readings"] or 0 for row in rows.values())
    happy = sum(row["happy"] or 0 for row in rows.values())
    calm = sum(row["calm"] or 0 for row in rows.values())
    unhappy = sum(row["unhappy"] or 0 for row in rows.values())

    def pct(value: int) -> float | None:
        return round(100 * value / readings, 1) if readings else None

    return {
        "days": len(rows),
        "readings": readings,
        "happy": happy,
        "calm": calm,
        "unhappy": unhappy,
        "happy_pct": pct(happy),
        "calm_pct": pct(calm),
        "unhappy_pct": pct(unhappy),
        "rows": rows,
    }


def steps_summary(conn, start: str, end: str) -> dict:
    rows = conn.execute(
        "SELECT * FROM daily_steps WHERE date BETWEEN ? AND ? ORDER BY date", (start, end)
    ).fetchall()
    return {
        "days": len(rows),
        "avg": avg([row["steps"] for row in rows]),
        "max": max((row["steps"] for row in rows if row["steps"]), default=None),
        "rows": rows,
    }


def summarize(conn, start: str, end: str) -> dict:
    return {
        "start": start,
        "end": end,
        "sleep": sleep_summary(conn, start, end),
        "metrics": {metric: metric_summary(conn, metric, start, end) for metric in METRICS},
        "workouts": workouts_summary(conn, start, end),
        "emotion": emotion_summary(conn, start, end),
        "steps": steps_summary(conn, start, end),
    }


def date_range(start: str, end: str) -> list[str]:
    first = date.fromisoformat(start)
    last = date.fromisoformat(end)
    days = []
    while first <= last:
        days.append(first.isoformat())
        first += timedelta(days=1)
    return days


def missing_days(conn, start: str, end: str) -> list[str]:
    have = {row["date"] for row in db.daily_rows(conn, start, end)}
    return [day for day in date_range(start, end) if day not in have]


def iso_week_range(iso_year: int, week: int) -> tuple[str, str]:
    monday = date.fromisocalendar(iso_year, week, 1)
    return monday.isoformat(), (monday + timedelta(days=6)).isoformat()


def month_range(year: int, month: int) -> tuple[str, str]:
    return f"{year:04d}-{month:02d}-01", f"{year:04d}-{month:02d}-{calendar.monthrange(year, month)[1]:02d}"


def previous_month(year: int, month: int) -> tuple[int, int]:
    return (year - 1, 12) if month == 1 else (year, month - 1)


def delta_text(current, previous, digits: int = 1, suffix: str = "") -> str:
    if current is None or previous is None:
        return "—"
    diff = current - previous
    if abs(diff) < 10 ** (-digits) / 2:
        return "持平"
    return f"{diff:+.{digits}f}{suffix}"


def yearly_stats(conn) -> list[dict]:
    years = conn.execute(
        "SELECT DISTINCT substr(date,1,4) AS year FROM daily_sleep ORDER BY year"
    ).fetchall()
    result = []
    for row in years:
        year = row["year"]
        start, end = f"{year}-01-01", f"{year}-12-31"
        sleep = sleep_summary(conn, start, end)
        metrics = {metric: metric_summary(conn, metric, start, end) for metric in METRICS}
        workouts = workouts_summary(conn, start, end)
        emotion = emotion_summary(conn, start, end)
        result.append({
            "year": year,
            "days": sleep["days"],
            "avg_total": sleep["avg_total"],
            "avg_night": sleep["avg_night"],
            "avg_score": sleep["avg_score"],
            "avg_rhr": metrics["resting_hr"]["avg"],
            "avg_hrv": metrics["hrv"]["avg"],
            "avg_spo2": metrics["spo2"]["avg"],
            "avg_stress": metrics["stress"]["avg"],
            "workouts": workouts["n"],
            "workout_minutes": workouts["minutes"],
            "emotion": emotion,
        })
    return result
