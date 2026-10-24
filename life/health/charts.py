# 健康报告图表：Agg + 中文字体，输出 PNG

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib import font_manager

FONT_FAMILY = "Noto Sans CJK JP"
METRIC_TITLES = {"resting_hr": "静息心率 (bpm)", "hrv": "HRV (ms)",
                 "spo2": "血氧 (%)", "stress": "压力 (分)"}
COLORS = {"night": "#4C72B0", "nap": "#DD8452", "score": "#55A868",
          "resting_hr": "#C44E52", "hrv": "#8172B3", "spo2": "#4C72B0",
          "stress": "#CCB974"}


def _setup() -> None:
    families = {item.name for item in font_manager.fontManager.ttflist}
    family = next((name for name in (
        "Noto Sans CJK SC", "Noto Sans CJK JP", "WenQuanYi Micro Hei", "SimHei"
    ) if name in families), FONT_FAMILY)
    plt.rcParams["font.family"] = family
    plt.rcParams["axes.unicode_minus"] = False
    plt.rcParams["figure.dpi"] = 110


def _save(fig, path: str | Path) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(path, format="png", bbox_inches="tight")
    plt.close(fig)
    return path


def sleep_trend(rows, path: str | Path, title: str = "睡眠趋势") -> Path:
    _setup()
    dates = [date.fromisoformat(row["date"]) for row in rows]
    night = [(row["night_min"] or 0) / 60 for row in rows]
    nap = [(row["nap_min"] or 0) / 60 for row in rows]
    fig, ax = plt.subplots(figsize=(10, 3.2))
    ax.bar(dates, night, color=COLORS["night"], label="夜间睡眠", width=0.8)
    ax.bar(dates, nap, bottom=night, color=COLORS["nap"], label="小睡", width=0.8)
    ax.set_ylabel("小时")
    ax.set_title(title)
    ax.grid(axis="y", alpha=0.3)
    ax.legend(loc="upper left", fontsize=8, ncol=2)
    locator = mdates.AutoDateLocator(minticks=4, maxticks=10)
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
    fig.autofmt_xdate(rotation=30)
    return _save(fig, path)


def sleep_structure(summary: dict, path: str | Path, title: str = "睡眠结构") -> Path:
    _setup()
    parts = [(name, value or 0) for name, value in summary.get("stage_pct", {}).items()]
    fig, ax = plt.subplots(figsize=(9, 2.0))
    left = 0.0
    colors = {"深睡": "#3C5488", "浅睡": "#7FB3D5", "REM": "#8E44AD", "清醒": "#D5D8DC"}
    for name, value in parts:
        if value <= 0:
            continue
        ax.barh([0], [value], left=left, color=colors.get(name, "#999999"),
                edgecolor="white", label=name)
        if value >= 5:
            ax.text(left + value / 2, 0, f"{name}\n{value:.1f}%", ha="center",
                    va="center", fontsize=8, color="white")
        left += value
    ax.set_xlim(0, 100)
    ax.set_yticks([])
    ax.set_xlabel("夜间睡眠阶段占比（%）")
    ax.set_title(title)
    return _save(fig, path)


def _rolling(values: list[float | None], window: int = 7) -> list[float | None]:
    result: list[float | None] = []
    for index in range(len(values)):
        chunk = [value for value in values[max(0, index - window + 1): index + 1]
                 if value is not None]
        result.append(sum(chunk) / len(chunk) if chunk else None)
    return result


def recovery_grid(metrics: dict, path: str | Path, title: str = "恢复指标") -> Path:
    _setup()
    fig, axes = plt.subplots(2, 2, figsize=(10, 5.2))
    for ax, metric in zip(axes.flat, ("resting_hr", "hrv", "spo2", "stress")):
        values = metrics.get(metric, {}).get("values", {})
        days = sorted(values)
        ax.set_title(METRIC_TITLES[metric], fontsize=10)
        if not days:
            ax.text(0.5, 0.5, "无数据", ha="center", va="center", transform=ax.transAxes)
            ax.set_xticks([])
            ax.set_yticks([])
            continue
        x = [date.fromisoformat(day) for day in days]
        y = [values[day] for day in days]
        ax.plot(x, y, "o-", color=COLORS[metric], markersize=3, linewidth=1, alpha=0.55)
        smooth = _rolling(y)
        ax.plot(x, smooth, color=COLORS[metric], linewidth=1.8, label="7 日滑动均值")
        ax.grid(alpha=0.25)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
        ax.tick_params(axis="x", labelsize=8, labelrotation=30)
    fig.suptitle(title, fontsize=12)
    return _save(fig, path)


def workout_bars(workouts, path: str | Path, title: str = "运动量") -> Path:
    _setup()
    by_week: dict[str, list] = {}
    for row in workouts:
        day = date.fromisoformat(row["date"])
        monday = day - timedelta(days=day.weekday())
        key = monday.isoformat()
        bucket = by_week.setdefault(key, [0, 0.0])
        bucket[0] += 1
        bucket[1] += (row["active_time_ms"] or 0) / 60000 / 60
    fig, ax = plt.subplots(figsize=(10, 3.0))
    if not by_week:
        ax.text(0.5, 0.5, "无运动记录", ha="center", va="center", transform=ax.transAxes)
        ax.set_xticks([])
        ax.set_yticks([])
    else:
        keys = sorted(by_week)
        labels = [key[5:] for key in keys]
        counts = [by_week[key][0] for key in keys]
        hours = [by_week[key][1] for key in keys]
        ax.bar(labels, counts, color=COLORS["night"], label="次数")
        ax.set_ylabel("次数")
        ax2 = ax.twinx()
        ax2.plot(labels, hours, color=COLORS["nap"], marker="o", label="时长(小时)")
        ax2.set_ylabel("小时")
        ax.grid(axis="y", alpha=0.3)
        ax.set_title(title)
        handles1, labels1 = ax.get_legend_handles_labels()
        handles2, labels2 = ax2.get_legend_handles_labels()
        ax.legend(handles1 + handles2, labels1 + labels2, loc="upper left", fontsize=8)
        for label in ax.get_xticklabels():
            label.set_rotation(30)
            label.set_fontsize(8)
    return _save(fig, path)


def overview_figure(years: list[dict], path: str | Path,
                    title: str = "长期趋势") -> Path:
    _setup()
    fig, axes = plt.subplots(2, 2, figsize=(10, 5.6))
    labels = [row["year"] for row in years]

    ax = axes[0][0]
    hours = [(row["avg_total"] or 0) / 60 for row in years]
    ax.bar(labels, hours, color=COLORS["night"])
    ax.set_title("年均睡眠时长（小时）", fontsize=10)
    ax.grid(axis="y", alpha=0.25)

    ax = axes[0][1]
    ax.plot(labels, [row["avg_rhr"] for row in years], "o-",
            color=COLORS["resting_hr"], label="静息心率")
    ax2 = ax.twinx()
    ax2.plot(labels, [row["avg_hrv"] for row in years], "s--",
             color=COLORS["hrv"], label="HRV")
    ax.set_title("静息心率 / HRV", fontsize=10)
    ax.set_ylabel("bpm")
    ax2.set_ylabel("ms")
    ax.grid(alpha=0.25)

    ax = axes[1][0]
    ax.bar(labels, [row["workouts"] for row in years], color=COLORS["nap"])
    ax.set_title("年度运动次数", fontsize=10)
    ax.grid(axis="y", alpha=0.25)

    ax = axes[1][1]
    happy = [row["emotion"]["happy_pct"] or 0 for row in years]
    calm = [row["emotion"]["calm_pct"] or 0 for row in years]
    unhappy = [row["emotion"]["unhappy_pct"] or 0 for row in years]
    ax.bar(labels, happy, color="#55A868", label="愉快")
    ax.bar(labels, calm, bottom=happy, color="#4C72B0", label="平静")
    ax.bar(labels, unhappy, bottom=[a + b for a, b in zip(happy, calm)],
           color="#C44E52", label="低落")
    ax.set_ylim(0, 100)
    ax.set_title("情绪读数占比（%）", fontsize=10)
    ax.legend(fontsize=7, ncol=3)

    fig.suptitle(title, fontsize=12)
    return _save(fig, path)
