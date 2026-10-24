# 健康报告生成与发布：稳定标题 + 正文 md5 幂等 + 图表资源原地更新

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
from configparser import ConfigParser
from datetime import date, datetime, timedelta
from pathlib import Path

from . import analytics, charts, db, memos

ROOT = Path(__file__).resolve().parents[2]
OLD_NOTE_ID = memos.OLD_NOTE_ID
HEALTH_NOTEBOOK_TITLE = "健康数据"
STATE_NAME = "health_notes.ini"
TOKEN_PATH = Path(os.environ.get(
    "HEALTH_TOKEN_PATH", "/data/codebase/joplin-health-bridge/data/token.json"))
WEEKDAYS = "一二三四五六日"
DISCLAIMER = ("> 本报告仅基于个人设备数据做描述统计，不构成医疗诊断、"
              "疾病风险预测或治疗建议；如有健康问题请咨询医生。")
CHART_RE = re.compile(r"\{\{chart:([a-z0-9_-]+)\}\}")
TS_RE = re.compile(r"((?:生成|更新时间)[：:]\s*)\d{4}-\d{2}-\d{2} \d{2}:\d{2}")
TYPE_NOTE = "运动类型编号为华为原始代码，未映射中文名称"

DAILY_TITLE = "健康数据日报"
OVERVIEW_TITLE = "健康数据总览"
MEMOS_TITLE = "健康生活随记汇编"
STATUS_TITLE = "健康数据系统状态"


# --- 通用 --------------------------------------------------------------------


def sanitize(body: str) -> str:
    return (body or "").replace("\x00", "")


def stable_digest(body: str) -> str:
    return hashlib.md5(TS_RE.sub(r"\1<ts>", body).encode("utf-8")).hexdigest()


def _day_label(day: str) -> str:
    parsed = date.fromisoformat(day)
    return f"{day}（周{WEEKDAYS[parsed.weekday()]}）"


def _table(headers: list[str], rows: list[list[str]]) -> list[str]:
    lines = ["| " + " | ".join(headers) + " |",
             "|" + "|".join(["------"] * len(headers)) + "|"]
    for row in rows:
        lines.append("| " + " | ".join(str(cell) for cell in row) + " |")
    return lines


def _metric(day_metrics: dict, metric: str, key: str = "value"):
    entry = day_metrics.get(metric)
    if entry is None:
        return None
    return entry[key]


def _num(value, digits: int = 0) -> str:
    if value is None:
        return "—"
    return f"{value:,.{digits}f}" if digits else f"{value:,.0f}"


def _metric_line(day_metrics: dict) -> str:
    parts = []
    for metric in ("resting_hr", "hrv", "spo2", "stress"):
        entry = day_metrics.get(metric)
        if entry is None:
            parts.append(f"{analytics.METRIC_NAMES[metric]} —")
            continue
        value = entry["value"]
        low, high = entry["min_value"], entry["max_value"]
        text = f"{analytics.METRIC_NAMES[metric]} {_num(value)} {analytics.METRIC_UNITS[metric]}"
        if metric in ("spo2", "stress") and low is not None and high is not None:
            text += f"（{_num(low)}–{_num(high)}）"
        parts.append(text)
    return " ｜ ".join(parts)


def _emotion_line(summary: dict) -> str:
    if not summary.get("readings"):
        return "无情绪读数"
    return (f"{summary['readings']} 次读数：愉快 {summary['happy_pct']:.1f}%、"
            f"平静 {summary['calm_pct']:.1f}%、低落 {summary['unhappy_pct']:.1f}%")


def _workout_line(rows) -> str:
    if not rows:
        return "无运动记录"
    parts = []
    for row in rows:
        minutes = (row["active_time_ms"] or 0) / 60000
        distance = f"，{row['distance_m'] / 1000:.1f} km" if row["distance_m"] else ""
        heart = f"，均心 {row['avg_hr']:.0f}" if row["avg_hr"] else ""
        parts.append(f"类型 {row['activity_type']} {minutes:.0f} 分钟{distance}{heart}")
    return "；".join(parts)


def _sleep_line(summary: dict) -> str:
    if not summary.get("days"):
        return "无睡眠记录"
    return (f"平均 {analytics.fmt_min(summary['avg_total'])}"
            f"（夜间 {analytics.fmt_min(summary['avg_night'])} + "
            f"小睡 {analytics.fmt_min(summary['avg_nap'])}）")


def _stage_line(summary: dict) -> str:
    stage = summary.get("stage_pct") or {}
    return " ｜ ".join(
        f"{name} {value:.1f}%" for name, value in stage.items() if value is not None
    ) or "—"


def _chart(out_dir: Path, label: str, func, *args, **kwargs) -> dict:
    path = Path(out_dir) / f"{label}.png"
    try:
        func(*args, path=path, **kwargs)
        return {label: path}
    except Exception as exc:  # 图表失败不阻塞正文
        print(f"chart {label} failed: {exc}", flush=True)
        return {}


def _window_stats(conn, start: str, end: str) -> dict:
    return {
        "sleep": analytics.sleep_summary(conn, start, end),
        "metrics": {metric: analytics.metric_summary(conn, metric, start, end)
                    for metric in analytics.METRICS},
        "workouts": analytics.workouts_summary(conn, start, end),
    }


def _memo_lines(rows, show_date: bool = False) -> list[str]:
    lines = []
    for entry in rows:
        text = re.sub(r"\s+", " ", entry["content"]).strip()
        beer = entry["beer_count"] or 0
        if beer and text:
            line = f"{text}（啤酒 {beer} 瓶）"
        elif beer:
            line = f"啤酒 {beer} 瓶"
        else:
            line = text
        if not line:
            continue
        if show_date:
            line = f"{entry['date'][5:]}：{line}"
        lines.append(f"- {line}")
    return lines


def _beer_stats_line(start: str, end: str) -> str:
    stats = memos.range_stats(start, end)
    return (f"- 饮酒登记：{stats['beer_days']} 天，共 {stats['beer_total']} 瓶 ｜ "
            f"随记 {stats['memos']} 条")


def _recovery_text(day_row, night, day_metrics: dict) -> str:
    if day_row:
        text = (f"睡眠 {analytics.fmt_min(day_row['total_min'])}"
                f"（夜间 {analytics.fmt_min(day_row['night_min'])}）")
    else:
        text = "睡眠无记录"
    if night is not None and night["sleep_score"] is not None:
        text += f"、分数 {_num(night['sleep_score'])}"
    rhr = _metric(day_metrics, "resting_hr")
    if rhr is not None:
        text += f"、静息心率 {_num(rhr)} bpm"
    hrv = _metric(day_metrics, "hrv")
    if hrv is not None:
        text += f"、HRV {_num(hrv)} ms"
    return text


def _beer_recovery_line(prev_beer: int, covered: bool, day_row, night,
                        day_metrics: dict) -> str:
    if not covered:
        return "- 前一晚饮酒：未登记（随记覆盖不足）"
    status = f"{prev_beer} 瓶" if prev_beer else "未饮酒"
    return f"- 前一晚饮酒：{status} → 今晨 {_recovery_text(day_row, night, day_metrics)}"


def _beer_comparison_block(conn, start: str, end: str, *,
                           title: str = "饮酒与次夜恢复（描述性）",
                           min_drink: int = 3, min_sober: int = 7) -> list[str]:
    result = memos.beer_comparison(conn, start, end, min_drink=min_drink,
                                   min_sober=min_sober)
    lines = [f"## {title}", ""]
    if result["coverage_pct"]:
        lines.append(f"> 覆盖：{result['covered_days']}/{result['range_days']} 天"
                     f"（{result['coverage_pct']:.0f}%）；"
                     "啤酒日按次日睡眠/晨间指标对齐。")
        lines.append("")
    if not result["ok"]:
        lines += [result["reason"] or "覆盖不足，暂不做对比。", ""]
        return lines
    rows = []
    for key, label in (("drink", "饮酒次夜"), ("sober", "非饮酒次夜")):
        item = result[key]
        rows.append([label, item["n"], analytics.fmt_min(item["sleep"]),
                     analytics.fmt_num(item["score"], 0),
                     analytics.fmt_num(item["rhr"], 0),
                     analytics.fmt_num(item["hrv"], 0)])
    lines += _table(["类别", "夜数", "睡眠均值", "分数", "静息心率", "HRV"], rows)
    lines += ["", "> 仅描述性统计，样本量小，不推断因果；"
                  "非饮酒次夜只在登记覆盖区间内认定。", ""]
    return lines


def _comparison_block(conn, start: str, end: str, prev_start: str, prev_end: str,
                      baseline_start: str | None = None) -> list[str]:
    current = _window_stats(conn, start, end)
    previous = _window_stats(conn, prev_start, prev_end)
    lines = ["## 与上一周期对比", ""]
    cur_sleep = current["sleep"]["avg_total"]
    prev_sleep = previous["sleep"]["avg_total"]
    lines.append(f"- 睡眠均值：{analytics.fmt_min(cur_sleep)}"
                 f"（上一周期 {analytics.fmt_min(prev_sleep)}，"
                 f"{analytics.delta_text(cur_sleep, prev_sleep, 0)} 分钟）")
    for metric in analytics.METRICS:
        cur = current["metrics"][metric]["avg"]
        prev = previous["metrics"][metric]["avg"]
        lines.append(f"- {analytics.METRIC_NAMES[metric]}：{analytics.fmt_num(cur, 1)} "
                     f"{analytics.METRIC_UNITS[metric]}（上一周期 "
                     f"{analytics.fmt_num(prev, 1)}，"
                     f"{analytics.delta_text(cur, prev, 1)}）")
    lines.append(f"- 运动：{current['workouts']['n']} 次 / "
                 f"{current['workouts']['minutes']:.0f} 分钟"
                 f"（上一周期 {previous['workouts']['n']} 次 / "
                 f"{previous['workouts']['minutes']:.0f} 分钟）")
    if baseline_start:
        baseline = _window_stats(conn, baseline_start, prev_end)
        lines += ["", "参考基线（此前四周日均）："]
        lines.append(f"- 睡眠 {analytics.fmt_min(baseline['sleep']['avg_total'])} ｜ "
                     f"静息心率 {analytics.fmt_num(baseline['metrics']['resting_hr']['avg'], 0)} ｜ "
                     f"HRV {analytics.fmt_num(baseline['metrics']['hrv']['avg'], 0)} ｜ "
                     f"压力 {analytics.fmt_num(baseline['metrics']['stress']['avg'], 0)}")
    lines.append("")
    return lines


# --- 日报 --------------------------------------------------------------------


def build_daily(conn, out_dir: Path, target: date | None = None) -> tuple[str | None, dict]:
    today = date.today()
    if target is not None:
        latest = target.isoformat()
    else:
        latest = db.latest_complete_day(conn, today)
        if latest is None:
            return None, {}
        lag = (today - date.fromisoformat(latest)).days
        if lag > 2:
            return None, {}

    start14 = (date.fromisoformat(latest) - timedelta(days=13)).isoformat()
    start30 = (date.fromisoformat(latest) - timedelta(days=29)).isoformat()
    latest_day = date.fromisoformat(latest)
    current_start = (latest_day - timedelta(days=6)).isoformat()
    previous_end = (latest_day - timedelta(days=7)).isoformat()
    previous_start = (latest_day - timedelta(days=13)).isoformat()

    rows14 = db.daily_rows(conn, start14, latest)
    by_day = {row["date"]: row for row in rows14}
    metrics14 = db.metric_rows(conn, start14, latest)
    nights14 = db.main_nights(conn, start14, latest)
    workouts14 = db.workouts(conn, start14, latest)
    workouts_by_day: dict[str, list] = {}
    for row in workouts14:
        workouts_by_day.setdefault(row["date"], []).append(row)

    day_row = by_day.get(latest)
    day_metrics = metrics14.get(latest, {})
    day_workouts = workouts_by_day.get(latest, [])
    day_emotion = db.emotions(conn, latest, latest).get(latest)
    day_steps = day_row["steps"] if day_row else None

    lines = [
        "# 健康数据日报",
        "",
        f"> 更新时间：{datetime.now():%Y-%m-%d %H:%M} ｜ 数据日：{_day_label(latest)}",
        "",
        "## 当日概览",
        "",
    ]
    if day_row:
        lines.append(f"- 睡眠：{analytics.fmt_min(day_row['total_min'])}"
                     f"（夜间 {analytics.fmt_min(day_row['night_min'])} + "
                     f"小睡 {analytics.fmt_min(day_row['nap_min'])}）")
    else:
        lines.append("- 睡眠：无记录")
    night = nights14.get(latest)
    if night:
        lines.append(f"- 睡眠质量：分数 {_num(night['sleep_score'])} ｜ 效率 "
                     f"{_num(night['efficiency'])}% ｜ 入睡潜伏期 "
                     f"{_num(night['latency_min'])} 分钟 ｜ 醒来 {_num(night['wakeup_count'])} 次")
    lines.append(f"- 恢复：{_metric_line(day_metrics)}")
    lines.append(f"- 运动：{_workout_line(day_workouts)}")
    if day_emotion:
        lines.append(f"- 情绪：{_emotion_line({'readings': day_emotion['readings'], 'happy_pct': day_emotion['happy_pct'] or 0, 'calm_pct': day_emotion['calm_pct'] or 0, 'unhappy_pct': day_emotion['unhappy_pct'] or 0})}")
    else:
        lines.append("- 情绪：无读数")
    if day_steps is not None:
        source = day_row["steps_source"] if day_row else "notification"
        suffix = "（通知来源，可能不完整）" if source == "notification" else ""
        lines.append(f"- 步数：{_num(day_steps)}{suffix}")
    else:
        lines.append("- 步数：无数据（历史步数接口不可用）")
    lines.append("")

    lines += ["## 近 14 天", ""]
    table_rows = []
    for row in rows14:
        night = nights14.get(row["date"])
        metrics = metrics14.get(row["date"], {})
        table_rows.append([
            row["date"][5:], analytics.fmt_min(row["total_min"]),
            analytics.fmt_min(row["night_min"]), analytics.fmt_min(row["nap_min"]),
            _num(night["sleep_score"]) if night else "—",
            _num(_metric(metrics, "resting_hr")), _num(_metric(metrics, "hrv")),
            _num(_metric(metrics, "spo2")), _num(_metric(metrics, "stress")),
            _num(row["steps"]), str(len(workouts_by_day.get(row["date"], []))),
        ])
    lines += _table(["日期", "睡眠", "夜间", "小睡", "分数", "静息心率", "HRV",
                     "血氧", "压力", "步数", "运动"], table_rows)
    lines.append("")

    rows30 = db.daily_rows(conn, start30, latest)
    chart_paths = {}
    chart_paths.update(_chart(out_dir, "sleep-30d", charts.sleep_trend, rows30,
                              title="近 30 天睡眠"))
    lines += ["## 近 30 天睡眠", "", "{{chart:sleep-30d}}", ""]

    lines += _comparison_block(conn, current_start, latest, previous_start, previous_end)

    lines += ["## 随记与饮酒", ""]
    previous_day = (latest_day - timedelta(days=1)).isoformat()
    coverage_start, coverage_end = memos.coverage_range()
    covered = bool(coverage_start and coverage_start <= previous_day <= coverage_end)
    previous_beer = memos.beer_by_day(previous_day, previous_day).get(previous_day, 0)
    lines.append(_beer_recovery_line(previous_beer, covered, day_row, night, day_metrics))
    today_memos = memos.memos_for_day(latest)
    if today_memos:
        lines += ["", "当天随记：", ""] + _memo_lines(today_memos)
    else:
        lines.append("- 当天随记：未登记")
    lines.append("")

    gaps = analytics.missing_days(conn, start14, latest)
    step_days = sum(1 for row in rows14 if row["steps"] is not None)
    lines += ["## 数据缺口", ""]
    lines.append(f"- 近 14 天睡眠缺失：{('、'.join(gaps)) if gaps else '无'}")
    lines.append(f"- 近 14 天步数覆盖：{step_days}/14 天（仅通知来源，可能不完整）")
    lines += ["", "---", DISCLAIMER, ""]
    return sanitize("\n".join(lines)), chart_paths


# --- 周报 --------------------------------------------------------------------


def _parse_week(value: str) -> tuple[int, int]:
    match = re.fullmatch(r"(\d{4})-?W(\d{1,2})", value)
    if not match:
        raise ValueError("周格式应为 YYYY-Www")
    return int(match.group(1)), int(match.group(2))


def default_week(today: date) -> tuple[int, int]:
    previous_monday = today - timedelta(days=today.weekday() + 7)
    iso = previous_monday.isocalendar()
    return iso.year, iso.week


def build_weekly(conn, out_dir: Path, iso_year: int | None = None,
                 week: int | None = None) -> tuple[str | None, dict]:
    if iso_year is None or week is None:
        iso_year, week = default_week(date.today())
    start, end = analytics.iso_week_range(iso_year, week)
    rows = db.daily_rows(conn, start, end)
    if not rows:
        return None, {}

    summary = analytics.summarize(conn, start, end)
    nights = db.main_nights(conn, start, end)
    metrics = db.metric_rows(conn, start, end)
    workouts_by_day: dict[str, list] = {}
    for row in summary["workouts"]["rows"]:
        workouts_by_day.setdefault(row["date"], []).append(row)

    prev_start = (date.fromisoformat(start) - timedelta(days=7)).isoformat()
    prev_end = (date.fromisoformat(start) - timedelta(days=1)).isoformat()
    base_start = (date.fromisoformat(start) - timedelta(days=28)).isoformat()

    title = f"健康数据周报｜{iso_year}年第{week}周"
    lines = [
        f"# {title}",
        "",
        f"> 更新时间：{datetime.now():%Y-%m-%d %H:%M} ｜ 周期：{start} .. {end}",
        "",
        "> 周归属按睡眠归日：周日夜间睡眠通常记到周一，跨周比较时请留意。",
        "",
        "## 本周概览",
        "",
        f"- 睡眠：{_sleep_line(summary['sleep'])}",
        f"- 睡眠结构（主夜）：{_stage_line(summary['sleep'])}",
        f"- 作息：平均入睡 {summary['sleep']['bed_mean'] or '—'} ｜ "
        f"平均醒来 {summary['sleep']['wake_mean'] or '—'} ｜ "
        f"睡眠中点波动 {analytics.fmt_num(summary['sleep']['midpoint_spread'], 0)} 分钟",
        f"- 睡眠分数均值：{analytics.fmt_num(summary['sleep']['avg_score'], 0)} ｜ "
        f"效率 {analytics.fmt_num(summary['sleep']['avg_efficiency'], 0)}%",
    ]
    for metric in analytics.METRICS:
        item = summary["metrics"][metric]
        lines.append(f"- {analytics.METRIC_NAMES[metric]}：{analytics.fmt_num(item['avg'], 0)} "
                     f"{analytics.METRIC_UNITS[metric]}（{analytics.fmt_num(item['n'], 0)} 天）")
    lines += [
        f"- 运动：{summary['workouts']['n']} 次，"
        f"{summary['workouts']['minutes']:.0f} 分钟，"
        f"{summary['workouts']['distance_km']:.1f} km，"
        f"{summary['workouts']['calories']:.0f} kcal",
        f"- 情绪：{_emotion_line(summary['emotion'])}",
        _beer_stats_line(start, end),
        "",
    ]

    table_rows = []
    for row in rows:
        night = nights.get(row["date"])
        day_metrics = metrics.get(row["date"], {})
        table_rows.append([
            row["date"][5:], analytics.fmt_min(row["total_min"]),
            _num(night["sleep_score"]) if night else "—",
            _num(_metric(day_metrics, "resting_hr")), _num(_metric(day_metrics, "hrv")),
            _num(_metric(day_metrics, "spo2")), _num(_metric(day_metrics, "stress")),
            _num(row["steps"]), str(len(workouts_by_day.get(row["date"], []))),
        ])
    lines += ["## 每日明细", ""]
    lines += _table(["日期", "睡眠", "分数", "静息心率", "HRV", "血氧", "压力",
                     "步数", "运动"], table_rows)
    lines.append("")

    lines += ["## 本周随记", ""]
    week_memos = memos.memos_by_range(start, end)
    if week_memos:
        lines += _memo_lines(week_memos, show_date=True) + [""]
    else:
        lines += ["本周无随记。", ""]

    chart_paths = {}
    chart_paths.update(_chart(out_dir, "week-sleep-structure", charts.sleep_structure,
                              summary["sleep"], title=f"{iso_year}年第{week}周睡眠结构"))
    chart_paths.update(_chart(out_dir, "week-recovery", charts.recovery_grid,
                              summary["metrics"], title=f"{iso_year}年第{week}周恢复指标"))
    lines += ["## 睡眠结构", "", "{{chart:week-sleep-structure}}", "",
              "## 恢复指标", "", "{{chart:week-recovery}}", ""]

    lines += _comparison_block(conn, start, end, prev_start, prev_end, base_start)

    lines += ["## 运动明细", ""]
    workouts = summary["workouts"]["rows"]
    if workouts:
        workout_rows = []
        for row in workouts:
            workout_rows.append([
                row["date"][5:], row["activity_type"],
                f"{(row['active_time_ms'] or 0) / 60000:.0f}",
                f"{(row['distance_m'] or 0) / 1000:.2f}",
                _num(row["calories"]), _num(row["avg_hr"]), _num(row["max_hr"]),
                f"{(row['avg_pace'] or 0):.0f}" if row["avg_pace"] else "—",
            ])
        lines += _table(["日期", "类型", "分钟", "公里", "kcal", "均心", "最高心", "配速(s/km)"],
                        workout_rows)
        lines += ["", f"*{TYPE_NOTE}。*", ""]
    else:
        lines += ["本周无运动记录。", ""]

    missing = analytics.missing_days(conn, start, end)
    cov_start, cov_end = memos.coverage_range()
    covered_days = sum(1 for day_text in analytics.date_range(start, end)
                       if cov_start and cov_start <= day_text <= cov_end)
    memo_coverage = (f"{covered_days}/7 天（《{memos.MANUAL_TITLE}》）"
                     if cov_start else "未建立登记覆盖")
    lines += ["## 数据缺口", "",
              f"- 睡眠缺失：{('、'.join(missing)) if missing else '无'}",
              f"- 情绪读数覆盖：{summary['emotion']['days']}/7 天",
              f"- 步数覆盖：{summary['steps']['days']}/7 天（通知来源，可能不完整）",
              f"- 随记覆盖：{memo_coverage}",
              "", "---", DISCLAIMER, ""]
    return sanitize("\n".join(lines)), chart_paths


# --- 月报 --------------------------------------------------------------------


def default_month(today: date) -> tuple[int, int]:
    first = today.replace(day=1) - timedelta(days=1)
    return first.year, first.month


def build_monthly(conn, out_dir: Path, year: int | None = None,
                  month: int | None = None) -> tuple[str | None, dict]:
    if year is None or month is None:
        year, month = default_month(date.today())
    start, end = analytics.month_range(year, month)
    rows = db.daily_rows(conn, start, end)
    if not rows:
        return None, {}

    summary = analytics.summarize(conn, start, end)
    prev_year, prev_month = analytics.previous_month(year, month)
    prev_start, prev_end = analytics.month_range(prev_year, prev_month)
    same_start, same_end = analytics.month_range(year - 1, month)

    title = f"健康数据月报｜{year}年{month:02d}月"
    lines = [
        f"# {title}",
        "",
        f"> 更新时间：{datetime.now():%Y-%m-%d %H:%M} ｜ 周期：{start} .. {end}",
        "",
        "> 月归属按睡眠归日：上月最后一晚的睡眠可能记入本月，跨月比较时请留意。",
        "",
        "## 本月概览",
        "",
        f"- 睡眠：{_sleep_line(summary['sleep'])}，共 {summary['sleep']['days']} 天",
        f"- 睡眠结构（主夜）：{_stage_line(summary['sleep'])}",
        f"- 作息：平均入睡 {summary['sleep']['bed_mean'] or '—'} ｜ "
        f"平均醒来 {summary['sleep']['wake_mean'] or '—'} ｜ "
        f"睡眠中点波动 {analytics.fmt_num(summary['sleep']['midpoint_spread'], 0)} 分钟",
        f"- 睡眠分数均值：{analytics.fmt_num(summary['sleep']['avg_score'], 0)} ｜ "
        f"效率 {analytics.fmt_num(summary['sleep']['avg_efficiency'], 0)}% ｜ "
        f"平均醒来 {analytics.fmt_num(summary['sleep']['avg_wakeups'], 1)} 次",
        f"- 运动：{summary['workouts']['n']} 次，"
        f"{summary['workouts']['minutes'] / 60:.1f} 小时，"
        f"{summary['workouts']['distance_km']:.1f} km，"
        f"{summary['workouts']['calories']:.0f} kcal",
        f"- 情绪：{_emotion_line(summary['emotion'])}",
        _beer_stats_line(start, end),
        "",
    ]
    for metric in analytics.METRICS:
        item = summary["metrics"][metric]
        lines.append(f"- {analytics.METRIC_NAMES[metric]}：{analytics.fmt_num(item['avg'], 0)} "
                     f"{analytics.METRIC_UNITS[metric]}（{item['n']} 天，"
                     f"区间 {analytics.fmt_num(item['min'], 0)}–{analytics.fmt_num(item['max'], 0)}）")
    lines.append("")

    week_rows = []
    cursor = date.fromisoformat(start)
    last = date.fromisoformat(end)
    while cursor <= last:
        week_start = cursor
        week_end = min(cursor + timedelta(days=6 - cursor.weekday()), last)
        segment = _window_stats(conn, week_start.isoformat(), week_end.isoformat())
        week_rows.append([
            f"{week_start.isoformat()[5:]}..{week_end.isoformat()[5:]}",
            analytics.fmt_min(segment["sleep"]["avg_total"]),
            segment["sleep"]["days"],
            segment["workouts"]["n"],
            analytics.fmt_num(segment["metrics"]["resting_hr"]["avg"], 0),
            analytics.fmt_num(segment["metrics"]["hrv"]["avg"], 0),
            analytics.fmt_num(segment["metrics"]["stress"]["avg"], 0),
        ])
        cursor = week_end + timedelta(days=1)
    lines += ["## 每周汇总", ""]
    lines += _table(["周期", "睡眠均值", "天数", "运动", "静息心率", "HRV", "压力"],
                    week_rows)
    lines.append("")

    lines += ["## 本月随记", ""]
    month_memos = memos.memos_by_range(start, end)
    if month_memos:
        lines += _memo_lines(month_memos, show_date=True) + [""]
    else:
        lines += ["本月无随记。", ""]

    lines += _beer_comparison_block(conn, start, end)

    chart_paths = {}
    chart_paths.update(_chart(out_dir, "month-sleep", charts.sleep_trend, rows,
                              title=f"{year}年{month:02d}月睡眠"))
    chart_paths.update(_chart(out_dir, "month-recovery", charts.recovery_grid,
                              summary["metrics"], title=f"{year}年{month:02d}月恢复指标"))
    chart_paths.update(_chart(out_dir, "month-workouts", charts.workout_bars,
                              summary["workouts"]["rows"],
                              title=f"{year}年{month:02d}月运动量"))
    lines += ["## 睡眠趋势", "", "{{chart:month-sleep}}", "",
              "## 恢复指标", "", "{{chart:month-recovery}}", "",
              "## 运动量", "", "{{chart:month-workouts}}", ""]

    lines += _comparison_block(conn, start, end, prev_start, prev_end)
    prev_rows = db.daily_rows(conn, prev_start, prev_end)
    same_rows = db.daily_rows(conn, same_start, same_end)
    if same_rows:
        lines += ["## 与去年同月对比", ""]
        same = analytics.summarize(conn, same_start, same_end)
        lines.append(f"- 睡眠均值：{analytics.fmt_min(summary['sleep']['avg_total'])}"
                     f"（去年同月 {analytics.fmt_min(same['sleep']['avg_total'])}）")
        for metric in ("resting_hr", "hrv", "spo2", "stress"):
            lines.append(f"- {analytics.METRIC_NAMES[metric]}："
                         f"{analytics.fmt_num(summary['metrics'][metric]['avg'], 0)}"
                         f"（去年同月 {analytics.fmt_num(same['metrics'][metric]['avg'], 0)}）")
        lines.append("")

    coverage = []
    for metric in analytics.METRICS:
        coverage.append(f"{analytics.METRIC_NAMES[metric]} {summary['metrics'][metric]['n']} 天")
    cov_start, cov_end = memos.coverage_range()
    covered_days = sum(1 for day_text in analytics.date_range(start, end)
                       if cov_start and cov_start <= day_text <= cov_end)
    memo_coverage = (f"{covered_days}/{len(analytics.date_range(start, end))} 天"
                     f"（《{memos.MANUAL_TITLE}》）" if cov_start
                     else "未建立登记覆盖")
    lines += ["## 数据覆盖", "",
              f"- 睡眠 {summary['sleep']['days']} 天 ｜ " + " ｜ ".join(coverage),
              f"- 情绪 {summary['emotion']['days']} 天 ｜ 步数 {summary['steps']['days']} 天"
              f"（仅通知来源，可能不完整）",
              f"- 随记覆盖：{memo_coverage}",
              f"- 上月有数据：{'是' if prev_rows else '否'}；去年同月有数据：{'是' if same_rows else '否'}",
              "", "---", DISCLAIMER, ""]
    return sanitize("\n".join(lines)), chart_paths


# --- 总览 --------------------------------------------------------------------


def build_overview(conn, out_dir: Path) -> tuple[str | None, dict]:
    first, last = db.range_bounds(conn)
    if not first:
        return None, {}
    summary = analytics.summarize(conn, first, last)
    years = analytics.yearly_stats(conn)
    workouts = summary["workouts"]["rows"]
    type_counts = summary["workouts"]["by_type"]
    memo_counts = memos.count_memos()
    memo_ranges = memos.source_ranges()

    title = OVERVIEW_TITLE
    lines = [
        f"# {title}",
        "",
        f"> 更新时间：{datetime.now():%Y-%m-%d %H:%M} ｜ 覆盖：{first} .. {last}"
        f" ｜ 睡眠 {summary['sleep']['days']} 天",
        "",
        "## 全历史概览",
        "",
        f"- 睡眠均值：{analytics.fmt_min(summary['sleep']['avg_total'])}"
        f"（夜间 {analytics.fmt_min(summary['sleep']['avg_night'])} + "
        f"小睡 {analytics.fmt_min(summary['sleep']['avg_nap'])}）",
        f"- 睡眠结构（主夜）：{_stage_line(summary['sleep'])}",
        f"- 运动：累计 {len(workouts)} 次，"
        f"{summary['workouts']['minutes'] / 60:.1f} 小时，"
        f"{summary['workouts']['distance_km']:.1f} km",
        f"- 情绪：{_emotion_line(summary['emotion'])}",
        f"- 步数：{summary['steps']['days']} 天有通知步数（2026-10-21 起，可能不完整）",
        "",
    ]

    year_rows = []
    for row in years:
        year_stats = memos.range_stats(f"{row['year']}-01-01", f"{row['year']}-12-31")
        year_rows.append([
            row["year"], row["days"], analytics.fmt_min(row["avg_total"]),
            analytics.fmt_min(row["avg_night"]),
            analytics.fmt_num(row["avg_score"], 0),
            analytics.fmt_num(row["avg_rhr"], 0), analytics.fmt_num(row["avg_hrv"], 0),
            analytics.fmt_num(row["avg_spo2"], 0), analytics.fmt_num(row["avg_stress"], 0),
            row["workouts"], f"{row['workout_minutes'] / 60:.0f}",
            f"{year_stats['beer_days']}/{year_stats['beer_total']}",
            year_stats["memos"],
        ])
    lines += ["## 年度汇总", ""]
    lines += _table(["年份", "睡眠天数", "睡眠均值", "夜间均值", "分数", "静息心率",
                     "HRV", "血氧", "压力", "运动次数", "运动小时",
                     "啤酒(天/瓶)", "随记(条)"], year_rows)
    lines.append("")

    chart_paths = _chart(out_dir, "overview-trends", charts.overview_figure, years,
                         title="全历史趋势")
    lines += ["## 长期趋势", "", "{{chart:overview-trends}}", ""]

    lines += ["## 运动类型分布", ""]
    if type_counts:
        for activity_type, count in type_counts.items():
            lines.append(f"- 类型 {activity_type}：{count} 次")
        lines += ["", f"*{TYPE_NOTE}。*", ""]
    else:
        lines += ["无运动记录。", ""]

    legacy_memos = memo_counts["legacy"]
    manual_memos = memo_counts["manual"]
    lines += ["## 生活方式与饮酒", "",
              f"- 全历史饮酒登记：{memo_counts['beer_days']} 天，共 "
              f"{memo_counts['beer_total']} 瓶 ｜ 随记 {memo_counts['memos']} 条",
              f"- 旧笔记：随记 {legacy_memos['memos']} 条，啤酒 "
              f"{legacy_memos['beer_days']} 天 {legacy_memos['beer_total']} 瓶"
              f"（{memo_ranges['legacy']['start'] or '—'} .. "
              f"{memo_ranges['legacy']['end'] or '—'}）",
              f"- 登记笔记：随记 {manual_memos['memos']} 条，啤酒 "
              f"{manual_memos['beer_days']} 天 {manual_memos['beer_total']} 瓶"
              f"（{memo_ranges['manual']['start'] or '—'} .. "
              f"{memo_ranges['manual']['end'] or '—'}）",
              ""]

    cov_start, cov_end = memos.coverage_range()
    if cov_start:
        lines += _beer_comparison_block(
            conn, cov_start, cov_end, title="饮酒与次夜恢复（登记期描述性）")
    else:
        lines += ["## 饮酒与次夜恢复（登记期描述性）", "",
                  "尚无《健康随记登记》覆盖，暂不做对比。", ""]

    lines += ["## 数据说明", "",
              "- 睡眠数据来自华为健康云端，归日规则：夜间睡眠记醒来日，小睡记入睡日。",
              "- 静息心率、HRV、血氧、压力为日聚合统计；HRV 可用历史较短，"
              "情绪读数从 2026-03 起才有较完整覆盖。",
              "- 步数仅自 2026-10-21 起从 HCX 通知库取得，历史缺失，"
              "不导入旧笔记里的手工步数。",
              "- 旧《健康运动笔记》中的睡眠、步数数字未被使用，仅生活随记只读提取。",
              "- 啤酒数记录在饮酒当天，睡眠/晨间指标按次日（D+1）对齐；"
              "非饮酒次夜只在《健康随记登记》覆盖区间内认定，区间外不推断。",
              "- 随记正文中的瓶数文字不自动计数，只读取标题下第一行的纯数字。",
              "", "---", DISCLAIMER, ""]
    return sanitize("\n".join(lines)), chart_paths


# --- 状态 --------------------------------------------------------------------


def _token_status() -> tuple[str, str]:
    if not TOKEN_PATH.exists():
        return "缺失", "需重新扫码登录"
    try:
        data = json.loads(TOKEN_PATH.read_text(encoding="utf-8"))
        expires_at = float(data["expires_at"])
    except (OSError, ValueError, KeyError, TypeError):
        return "不可读", "需检查 token.json"
    remaining = expires_at - datetime.now().timestamp()
    if remaining <= 0:
        return "已过期", "下次采集会尝试静默刷新"
    hours = remaining / 3600
    return f"有效（剩余 {hours:.1f} 小时）", data.get("region", "—")


def _state_rows() -> list[tuple[str, str, str]]:
    path = ROOT / "data" / STATE_NAME
    cfg = ConfigParser()
    if path.exists():
        cfg.read(path, encoding="utf-8")
    rows = []
    for section in cfg.sections():
        rows.append((section, cfg.get(section, "updated", fallback="—"),
                     cfg.get(section, "md5", fallback="—")[:8]))
    return rows


def build_status(conn, alert: bool = False) -> tuple[str, dict]:
    today = date.today()
    last_dates = db.table_last_dates(conn)
    metric_dates = db.metric_last_dates(conn)
    runs = db.sync_runs(conn, 12)
    token_text, token_note = _token_status()
    memo_counts = memos.count_memos()
    memo_ranges = memos.source_ranges()
    manual_status = memos.get_meta("manual_status", "absent") or "absent"
    memo_warnings = memos.get_meta_list("manual_warnings")
    overlaps = memos.overlap_beer_days()
    lag = None
    if last_dates.get("sleep"):
        lag = (today - date.fromisoformat(last_dates["sleep"])).days

    issues = []
    if lag is not None and lag > 2:
        issues.append(f"睡眠数据落后 {lag} 天")
    if not runs:
        issues.append("没有任何采集记录")
    else:
        latest_run = runs[0]
        if latest_run["status"] != "ok":
            issues.append(f"最近采集状态：{latest_run['status']}")
    if not token_text.startswith("有效"):
        issues.append(f"Token {token_text}")
    step_lag = None
    if last_dates.get("steps"):
        step_lag = (today - date.fromisoformat(last_dates["steps"])).days
        if step_lag > 1:
            issues.append(f"步数数据落后 {step_lag} 天")
    if not db.MEMOS_PATH.exists():
        issues.append("随记库尚未生成")
    if manual_status == "missing":
        issues.append("随记登记笔记缺失，库中数据已冻结")
    elif manual_status == "empty" and memo_counts["manual"]["total"]:
        issues.append("随记登记笔记为空，已保护库中数据")
    if memo_warnings:
        issues.append(f"随记解析告警 {len(memo_warnings)} 条")
    if overlaps:
        issues.append(f"随记来源重叠 {len(overlaps)} 天（统计以登记笔记为准）")

    lines = [
        f"# {STATUS_TITLE}",
        "",
        f"> 更新时间：{datetime.now():%Y-%m-%d %H:%M}",
        "",
    ]
    if alert and issues:
        lines += ["## 告警", ""] + [f"- {issue}" for issue in issues] + [""]
    lines += ["## 数据新鲜度", ""]
    lines.append(f"- 睡眠最新：{last_dates.get('sleep') or '—'}"
                 f"（滞后 {lag if lag is not None else '—'} 天）")
    for metric in analytics.METRICS:
        lines.append(f"- {analytics.METRIC_NAMES[metric]}最新：{metric_dates.get(metric, '—')}")
    lines.append(f"- 运动最新：{last_dates.get('workouts') or '—'}")
    lines.append(f"- 情绪最新：{last_dates.get('emotion') or '—'}")
    lines.append(f"- 步数最新：{last_dates.get('steps') or '—'}"
                 f"（滞后 {step_lag if step_lag is not None else '—'} 天，通知来源）")
    manual_text = {"ok": "正常", "missing": "缺失", "empty": "空",
                   "absent": "未创建"}.get(manual_status, manual_status)
    lines += ["", "## 采集与登录", "",
              f"- Token：{token_text} ｜ {token_note}",
              f"- 随记库：{'已生成' if db.MEMOS_PATH.exists() else '尚未生成'} ｜ "
              f"旧笔记 {memo_counts['legacy']['memos']} 条随记 / "
              f"{memo_counts['legacy']['beer_days']} 天啤酒 "
              f"{memo_counts['legacy']['beer_total']} 瓶 ｜ "
              f"登记笔记 {memo_counts['manual']['memos']} 条随记 / "
              f"{memo_counts['manual']['beer_days']} 天啤酒 "
              f"{memo_counts['manual']['beer_total']} 瓶",
              f"- 登记笔记：{manual_text} ｜ 最后随记 {memos.last_memo_date() or '—'} ｜ "
              f"覆盖 {memo_ranges['manual']['start'] or '—'} .. "
              f"{memo_ranges['manual']['end'] or '—'}",
              ""]
    if memo_warnings:
        lines += ["随记解析告警：", ""]
        lines += [f"- {warning}" for warning in memo_warnings[:5]]
        lines.append("")
    if overlaps:
        lines += [f"来源重叠（两边都记了啤酒，统计以登记笔记为准）："
                  f"{'、'.join(overlaps[:10])}", ""]
    if runs:
        lines += ["| 开始时间 | 模式 | 状态 | 睡眠记录 | 指标行 | 运动 | 情绪天 | 错误 |",
                  "|------|------|------|------|------|------|------|------|"]
        for run in runs:
            lines.append(f"| {run['started_at'] or '—'} | {run['mode']} | {run['status']} | "
                         f"{run['sleep_records']} | {run['metric_rows']} | "
                         f"{run['workouts']} | {run['emotion_days']} | "
                         f"{(run['error'] or '')[:80]} |")
        lines.append("")
    else:
        lines += ["无采集记录。", ""]

    state_rows = [row for row in _state_rows() if row[0] != STATUS_TITLE]
    lines += ["## 报告发布", ""]
    if state_rows:
        lines += _table(["报告", "更新时间", "md5 前缀"],
                        [[title, updated, digest] for title, updated, digest in state_rows])
    else:
        lines.append("尚未发布报告。")
    lines += ["", "## 告警检查", ""]
    lines += [f"- {issue}" for issue in issues] if issues else ["- 暂无明显异常"]
    lines += ["", "---", DISCLAIMER, ""]
    return sanitize("\n".join(lines)), {}


# --- Joplin 发布 -------------------------------------------------------------


class HealthPublisher:
    def __init__(self, state_path: Path | None = None):
        self.state_path = Path(state_path or ROOT / "data" / STATE_NAME)
        self.cfg = ConfigParser()
        if self.state_path.exists():
            self.cfg.read(self.state_path, encoding="utf-8")
        self._notebook_id_value = None

    def _save_state(self, title: str, note_id: str, digest: str) -> None:
        if not self.cfg.has_section(title):
            self.cfg.add_section(title)
        self.cfg.set(title, "note_id", note_id)
        self.cfg.set(title, "md5", digest)
        self.cfg.set(title, "updated", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.state_path, "w", encoding="utf-8") as fh:
            self.cfg.write(fh)

    def _notebook_id(self) -> str:
        if self._notebook_id_value:
            return self._notebook_id_value
        from func.jpfuncs import getnote, jpapi

        old_note = getnote(OLD_NOTE_ID)
        parent_id = old_note.parent_id
        for notebook in jpapi.get_all_notebooks(fields="id,title,parent_id"):
            if notebook.title == HEALTH_NOTEBOOK_TITLE and (notebook.parent_id or "") == parent_id:
                self._notebook_id_value = notebook.id
                return notebook.id
        self._notebook_id_value = jpapi.add_notebook(
            title=HEALTH_NOTEBOOK_TITLE, parent_id=parent_id)
        return self._notebook_id_value

    @staticmethod
    def _find_note(title: str, parent_id: str):
        from func.jpfuncs import jpapi

        for query in (f'title:"{title}"', f"title:{title}"):
            try:
                results = jpapi.search(query=query, fields="id,parent_id,title").items
            except Exception:
                continue
            for note in results:
                if note.title == title and note.parent_id == parent_id:
                    return note
        return None

    def _guard(self, note_id: str) -> None:
        if note_id == OLD_NOTE_ID:
            raise RuntimeError("拒绝对旧健康笔记写入：旧笔记为只读")
        manual_id = memos.get_meta("manual_note_id")
        if manual_id and note_id == manual_id:
            raise RuntimeError("拒绝对《健康随记登记》写入：登记笔记为用户所有，系统只读")

    def ensure_manual_note(self, adopt: bool = False,
                           dry_run: bool = False) -> tuple[str | None, str]:
        from func.jpfuncs import createnote, getnote

        stored = memos.get_meta("manual_note_id")
        if stored and stored != OLD_NOTE_ID:
            note = None
            try:
                note = getnote(stored)
            except Exception:
                note = None
            if note is not None and not getattr(note, "deleted_time", None):
                memos.set_meta("manual_status", "ok")
                return stored, "ok"
            if not adopt:
                memos.set_meta("manual_status", "missing")
                return None, "missing"
            memos.set_meta("manual_note_id", "")

        parent_id = self._notebook_id()
        found = self._find_note(memos.MANUAL_TITLE, parent_id)
        if found is not None and found.id != OLD_NOTE_ID:
            memos.set_meta("manual_note_id", found.id)
            memos.set_meta("manual_status", "ok")
            return found.id, "adopted"
        if dry_run:
            return None, "dry-run"
        note_id = createnote(memos.MANUAL_TITLE, memos.MANUAL_TEMPLATE,
                             parent_id=parent_id)
        memos.set_meta("manual_note_id", note_id)
        memos.set_meta("manual_status", "ok")
        return note_id, "created"

    def publish(self, title: str, body: str, chart_paths: dict | None = None,
                dry_run: bool = False, force: bool = False,
                out_dir: Path | None = None) -> str:
        chart_paths = chart_paths or {}
        body = sanitize(body)
        digest = stable_digest(body)
        stored_id = self.cfg.get(title, "note_id", fallback=None)
        stored_digest = self.cfg.get(title, "md5", fallback=None)

        if dry_run:
            target = Path(out_dir) if out_dir else Path.home() / "tmp" / "health_reports"
            target.mkdir(parents=True, exist_ok=True)
            filename = re.sub(r'[\\/:*?"<>|]', "_", title) + ".md"
            preview = body
            for label, path in chart_paths.items():
                preview = preview.replace(f"{{{{chart:{label}}}}}", f"![{label}]({Path(path).name})")
            preview = CHART_RE.sub(lambda m: f"_（图表缺失：{m.group(1)}）_", preview)
            (target / filename).write_text(preview, encoding="utf-8")
            return "dry-run"

        if not force and stored_id and stored_digest == digest:
            return "skipped"

        from func.jpfuncs import (
            createnote,
            getnote,
            jpapi,
            prune_removed_resources,
            updatenote_body,
            upsertresource,
        )

        parent_id = self._notebook_id()
        note = self._find_note(title, parent_id)
        old_body = ""
        created = False
        if note is None:
            skeleton = CHART_RE.sub(lambda m: f"_（图表上传中：{m.group(1)}）_", body)
            note_id = createnote(title, skeleton, parent_id=parent_id)
            created = True
        else:
            note_id = note.id
            old_body = getnote(note_id).body or ""
        self._guard(note_id)

        replacement = {}
        for label, path in chart_paths.items():
            try:
                resource_id = upsertresource(old_body, str(path), label,
                                             title=f"health_{label}.png")
                if f":/{resource_id}" not in old_body:
                    jpapi.add_resource_to_note(resource_id=resource_id, note_id=note_id)
                replacement[label] = f"![{label}](:/{resource_id})"
            except Exception as exc:
                print(f"chart upload {label} failed: {exc}", flush=True)
                replacement[label] = f"_（图表上传失败：{label}）_"
        final_body = CHART_RE.sub(
            lambda match: replacement.get(match.group(1),
                                          f"_（图表缺失：{match.group(1)}）_"),
            body,
        )
        updatenote_body(note_id, final_body)
        prune_removed_resources(old_body, final_body, exclude_note_id=note_id)
        self._save_state(title, note_id, digest)
        return "created" if created else "updated"
