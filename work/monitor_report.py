# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     formats: ipynb,py:percent
#     notebook_metadata_filter: jupytext,-kernelspec,-jupytext.text_representation.jupytext_version
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
# ---

# %% [markdown]
# # 笔记监测 —— 报告&可视化层

# %% [markdown]
# ## 引入库

# %%
import hashlib
import random
import re
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import arrow
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
from matplotlib.ticker import MaxNLocator
from tzlocal import get_localzone

# %%
import pathmagic

with pathmagic.context():
    from func.first import getdirmain
    from func.jpfuncs import (
        createnote,
        getapi,
        getinivaluefromcloud,
        getnote,
        searchnotes,
        updatenote_body,
        updatenote_title,
    )
    from func.logme import log
    from func.sysfunc import not_IPython
    from work.monitor_store import (
        add_spark_log,
        cleanup_spark_log,
        clear_dirty,
        get_active_notes,
        get_config,
        get_daily_stats_by_person,
        get_dirty_persons,
        get_latest_snapshot,
        get_person_quote_today,
        get_person_set,
        get_snapshot_count,
        get_used_spark_hashes,
        init_db,
        set_config,
    )


# %% [markdown]
# ## 工具函数


# %%
def retry_jp(callable, *args, max_tries: int = 3, **kwargs):  # noqa: ANN001 ANN002 ANN003 ANN201
    """Joplin API 调用重试，带指数退避"""
    last_exc = None
    for attempt in range(1, max_tries + 1):
        try:
            return callable(*args, **kwargs)
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.ReadTimeout,
            requests.exceptions.ConnectTimeout,
            ConnectionRefusedError,
            ConnectionResetError,
            OSError,
        ) as e:
            last_exc = e
            if attempt < max_tries:
                wait = random.randint(2, 10) * attempt
                log.warning(f"Joplin API 调用失败（第{attempt}次），{wait}秒后重试: {e}")
                time.sleep(wait)
    raise last_exc


def ensure_monitor_note(title: str = "四件套笔记轮询结果") -> str:
    """查找或创建监控笔记，返回note_id"""
    note_id = get_config("monitor_note_id")
    if note_id and len(note_id) > 30:
        return str(note_id)

    results = searchnotes(f"{title}")
    if results:
        note_id = results[0].id
    else:
        note_id = createnote(title=title, body="监控笔记已创建。")
    set_config("monitor_note_id", note_id)
    return note_id


def ensure_heatmap_note(person: str) -> str:
    """查找或创建指定person的热图笔记，返回note_id"""
    note_id = get_config(f"heatmap_note_{person}")
    if note_id and len(note_id) > 30:
        return str(note_id)

    results = searchnotes(f"日更动态（{person}）")
    if results:
        note_id = results[0].id
    else:
        note_id = createnote(title=f"日更动态（{person}）", body="日更笔记已创建。")
    set_config(f"heatmap_note_{person}", note_id)
    return note_id


# %% [markdown]
# ## 文字报告生成


# %%
def generate_text_report() -> str:
    """生成文字监控报告，更新到「监控笔记」。

    Returns:
        monitor_note_id
    """
    init_db()
    notes = get_active_notes()
    if not notes:
        log.info("无活跃笔记，跳过文字报告")
        return ""

    monitor_note_id = ensure_monitor_note()

    body_parts = []
    sections_order = ["核心客户", "工作笔记", "个人成长", "他山之石"]
    notes_by_section = {}
    for n in notes:
        section = n.get("section", "其他")
        notes_by_section.setdefault(section, []).append(n)

    for section in sections_order:
        if section not in notes_by_section:
            continue
        body_parts.append(f"## {section}\n")

        for n in notes_by_section[section]:
            note_id = n["note_id"]
            title = n["title"]
            first_seen = n.get("first_seen", "")
            last_seen = n.get("last_seen", "")
            snap_count = get_snapshot_count(note_id)
            latest = get_latest_snapshot(note_id)

            if latest is None:
                body_parts.append(f"笔记ID: {note_id}\n")
                body_parts.append(f"### 笔记标题: {title}\n")
                body_parts.append("无快照记录\n\n")
                continue

            from work.monitor_store import get_daily_stats

            daily = get_daily_stats(note_id)
            valid_dates = [d for d in daily if d["word_count"] > 0]

            body_parts.append(f"笔记ID: {note_id}\n")
            body_parts.append(f"### 笔记标题: {title}\n")
            body_parts.append(f"抓取时间起止: {first_seen}，{last_seen}\n")
            body_parts.append(f"快照次数: {snap_count}\n")
            body_parts.append(f"最近快照时间: {latest.get('captured_at', '')}\n")
            body_parts.append(f"最近全文字数: {latest.get('word_count', 0)}\n")
            if daily:
                dates_sorted = sorted(d["entry_date"] for d in daily)
                body_parts.append(f"笔记有效内容起止日期: {dates_sorted[0]}，{dates_sorted[-1]}\n")
                body_parts.append(f"笔记内容有效日期数量: {len(valid_dates)}({len(daily)})\n")
            body_parts.append("\n")

        body_parts.append("---\n")

    body = "".join(body_parts)
    retry_jp(updatenote_body, monitor_note_id, body)
    log.info(f"文字报告已更新至笔记 {monitor_note_id}")
    return monitor_note_id


# %% [markdown]
# ## 热图生成


# %%
def plot_word_counts(daily_counts: dict, title: str) -> str:
    """生成热力图PNG，返回文件路径。

    Args:
        daily_counts: {date_str: (word_count, is_backfill), ...}
        title: 图表标题
    """
    img_dir = Path(getdirmain()) / "img"
    img_dir.mkdir(parents=True, exist_ok=True)
    img_path = img_dir / "heatmap.png"
    img_path_str = str(img_path.absolute())

    monthrange_str = getinivaluefromcloud("monitor", "monthrange")
    monthrange = int(monthrange_str) if monthrange_str else 3

    dfall = pd.DataFrame(
        [[k, v[0], v[1]] for k, v in daily_counts.items()],
        columns=["date", "count", "addedlater"],
    )
    dfall["date"] = pd.to_datetime(dfall["date"])

    # 将daily_counts的字符串key转为date对象，用于后续过滤
    daily_counts_for_filter = {}
    for k, v in daily_counts.items():
        try:
            dk = datetime.strptime(str(k), "%Y-%m-%d").date() if isinstance(k, str) else k
        except ValueError:
            continue
        daily_counts_for_filter[dk] = v

    current_day_identity = arrow.now(get_localzone()).replace(hour=7, minute=30, second=0, microsecond=0)
    now = arrow.now(get_localzone())
    if now.hour < 7 or (now.hour == 7 and now.minute < 30):
        current_day_identity = current_day_identity.shift(days=-1)
    current_date = pd.to_datetime(current_day_identity.date())
    three_months_ago = current_date - pd.DateOffset(months=monthrange)

    valid_dates = [
        dt
        for dt in daily_counts_for_filter.keys()
        if (dt >= three_months_ago.date())
        or (daily_counts_for_filter[dt][1] and dt >= (three_months_ago - pd.DateOffset(months=1)).date())
    ]
    if not valid_dates:
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(0.5, 0.5, "暂时没有有效数据", ha="center", va="center", fontsize=20)
        plt.title(title)
        plt.savefig(img_path_str)
        plt.close()
        return img_path_str

    min_date = max(min(dfall["date"]), three_months_ago)
    max_date = current_date

    start_date = min_date - pd.Timedelta(days=min_date.weekday())
    if start_date in list(dfall["date"]):
        min_date = start_date
    dfready = dfall[dfall.date >= min_date]
    end_date = max_date + pd.Timedelta(days=6 - max_date.weekday())
    all_dates = pd.date_range(start=start_date, end=end_date)
    all_dates_df = pd.DataFrame({"date": all_dates, "count": -1, "addedlater": False})

    df = pd.concat([dfready, all_dates_df], ignore_index=True)
    df = df.drop_duplicates(subset=["date"], keep="first")
    df = df.sort_values(by="date").reset_index(drop=True)
    # 有效日期区间内缺失的天 → 0字（黄色），而非无数据（白色）
    df.loc[(df["date"] >= start_date) & (df["date"] <= max_date) & (df["count"] == -1), "count"] = 0

    df["year"] = df["date"].dt.year
    df["week"] = df["date"].dt.isocalendar().week
    df["day_of_week"] = df["date"].dt.weekday
    df["week_number"] = ((df["date"] - start_date).dt.days // 7) + 1

    pivot_table = df.pivot_table(
        index="week_number",
        columns="day_of_week",
        values="count",
        aggfunc="sum",
        fill_value=-1,
    )

    white = "white"
    warning_color = "#FFD700"

    max_count = int(df["count"].max())
    num_bins = min(max_count + 2, 254)
    boundaries = [-1, 0] + list(np.linspace(1, max_count + 1, num_bins - 1))

    greens = list(plt.cm.Greens(np.linspace(0.3, 1, num_bins - 2)))
    colors = [white, warning_color] + greens
    cmap = mcolors.ListedColormap(colors)
    norm = mcolors.BoundaryNorm(boundaries=boundaries, ncolors=cmap.N, clip=True)

    if pivot_table.values.max() <= 0:
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(0.5, 0.5, "最近三个月无有效更新", ha="center", va="center", fontsize=20)
        plt.title(title)
        plt.savefig(img_path_str)
        plt.close()
        return img_path_str

    figsize_factor = max(1, len(pivot_table) // 10)
    fig, ax = plt.subplots(figsize=(15, 6 + figsize_factor))

    heatmap = ax.pcolor(pivot_table.values, cmap=cmap, norm=norm, edgecolors="white", linewidths=2)

    week_labels = []
    for week_num in pivot_table.index:
        week_start = start_date + pd.Timedelta(weeks=week_num - 1)
        week_labels.append(f"{week_start.strftime('%m-%d')}")

    ax.set_yticks(np.arange(len(pivot_table.index)) + 0.5)
    ax.set_yticklabels(week_labels)
    ax.set_xticks(np.arange(7) + 0.5, minor=False)
    ax.set_xticklabels(["一", "二", "三", "四", "五", "六", "日"], minor=False)

    # 月份分割线
    month_locs = []
    first_day_coords = []

    temp_date = start_date
    while temp_date <= end_date:
        if temp_date.day == 1:
            week_num = ((temp_date - start_date).days // 7) + 1
            month_locs.append(week_num - 0.5)
            try:
                date_row = df[df["date"] == temp_date]
                if not date_row.empty:
                    week_in_heatmap = date_row["week_number"].iloc[0]
                    weekday_in_heatmap = date_row["day_of_week"].iloc[0]
                    first_day_coords.append(
                        {
                            "week": week_in_heatmap,
                            "weekday": weekday_in_heatmap,
                            "date_str": temp_date.strftime("%m-%d"),
                        }
                    )
            except Exception:
                pass
        temp_date += pd.DateOffset(days=1)

    ax.hlines(month_locs, -0.5, 6.5, colors="gray", linestyles="dashed", linewidth=0.5)
    ax.invert_yaxis()

    cbar = plt.colorbar(heatmap)
    cbar.set_label("更新字数")
    cbar.locator = MaxNLocator(integer=True)
    cbar.update_ticks()

    plt.title(title)

    # 起始日期和当前日期标记
    min_date_week = df[df["date"] == min_date]["week_number"].iloc[0]
    min_date_weekday = df[df["date"] == min_date]["day_of_week"].iloc[0]
    current_date_week = df[df["date"] == current_date]["week_number"].iloc[0]
    current_date_weekday = df[df["date"] == current_date]["day_of_week"].iloc[0]

    ax.text(
        min_date_weekday + 0.5,
        min_date_week - min(pivot_table.index) + 0.5,
        min_date.strftime("%m-%d"),
        ha="center",
        va="center",
        color="red",
        fontsize=12,
    )
    ax.text(
        current_date_weekday + 0.5,
        current_date_week - min(pivot_table.index) + 0.5,
        current_date.strftime("%m-%d"),
        ha="center",
        va="center",
        color="red",
        fontsize=12,
    )
    ax.add_patch(
        plt.Rectangle(
            (current_date_weekday, current_date_week - min(pivot_table.index)),
            1,
            1,
            fill=False,
            edgecolor="red",
            linestyle="--",
            linewidth=2,
        )
    )

    # 补填标记
    for marked_date_str in df[df["addedlater"]]["date"]:
        m_row = df[df["date"] == marked_date_str]
        if m_row.empty:
            continue
        week = m_row["week_number"].values[0]
        day_of_week = m_row["day_of_week"].values[0]
        ax.add_patch(
            plt.Rectangle(
                (day_of_week, week - min(pivot_table.index)),
                1,
                1,
                fill=False,
                edgecolor="gray",
                linestyle="--",
                linewidth=2,
            )
        )

    # 每月第一天标记
    for coord in first_day_coords:
        text_x = coord["weekday"] + 0.5
        text_y = coord["week"] - min(pivot_table.index) + 0.5
        ax.text(text_x, text_y, coord["date_str"], ha="center", va="center", color="dimgray", fontsize=10, alpha=0.8)
        ax.add_patch(
            plt.Rectangle(
                (coord["weekday"], coord["week"] - min(pivot_table.index)),
                1,
                1,
                fill=True,
                facecolor="lightgray",
                alpha=0.1,
                zorder=1,
            )
        )

    plt.savefig(img_path_str)
    plt.close()
    return img_path_str


# %% [markdown]
# ## 火花语录 + 成就系统


# %%
_spark_cache: list | None = None


def _get_spark_candidates() -> list[dict]:
    """解析思想火花笔记，返回候选句子列表 [{text, source_date}]。模块级缓存。"""
    global _spark_cache
    if _spark_cache is not None:
        return _spark_cache

    max_len_str = getinivaluefromcloud("monitor", "spark_max_len")
    max_len = int(max_len_str) if max_len_str else 60

    try:
        results = searchnotes("思想火花-（白晔峰）")
        if not results:
            return []
        body = getattr(getnote(results[0].id), "body", "")
        if not body:
            return []

        ptn_date = re.compile(r"^###\s+(\d{4}\s*年\s*\d{1,2}\s*月\s*\d{1,2}\s*[日号])\s*$", re.M)
        sections = re.split(ptn_date, body.strip())
        date_matches = ptn_date.findall(body.strip())

        quotes = []
        for i, section in enumerate(sections[1:]):
            source_date_raw = date_matches[i] if i < len(date_matches) else ""
            source_date = re.sub(r"\s+", "", source_date_raw).replace("号", "日")
            items = re.findall(r"(?:^|\n)\d+[\.\、\)）]\s*(.+?)(?=\n\d+[\.\、\)）]|\n###|\Z)", section, re.S)
            for item in items:
                clean = item.strip()
                clean = re.sub(r'["""\']+', "", clean)
                clean = re.sub(r"（[^）]*$", "", clean)
                if 10 <= len(clean) <= max_len and not clean.startswith("+"):
                    quotes.append({"text": clean, "source_date": source_date})

        _spark_cache = quotes
        return quotes
    except Exception:
        return []


def _pick_spark_quote(person: str) -> dict:
    """为指定人员选取火花语录（同日固定、7天按人去重）。返回 {text, source_date} 或 {}。"""
    existing = get_person_quote_today(person)
    if existing:
        # 旧记录可能没有 source_date，从缓存中补查
        if not existing.get("source_date"):
            candidates = _get_spark_candidates()
            for q in candidates:
                if q["text"] == existing["text"]:
                    existing["source_date"] = q["source_date"]
                    break
        return existing

    candidates = _get_spark_candidates()
    if not candidates:
        return {}

    cleanup_spark_log(7)
    used = get_used_spark_hashes(person, 7)

    available = [q for q in candidates if hashlib.md5(q["text"].encode()).hexdigest() not in used]

    if not available:
        available = candidates

    chosen = random.choice(available)
    today_str = date.today().strftime("%Y-%m-%d")
    add_spark_log(today_str, person, hashlib.md5(chosen["text"].encode()).hexdigest(), chosen["text"], chosen["source_date"])
    return chosen


# %%
TITLES = [
    (30, "⭐ 铁打的日更人"),
    (14, "🏅 笔耕不辍"),
    (7, "📌 渐入佳境"),
    (1, "🌱 还在路上"),
    (0, "⏸️ 等你回来"),
]


def _get_person_title(streak: int) -> str:
    """根据连续更新天数返回称号。"""
    for days, title in TITLES:
        if streak >= days:
            return title
    return TITLES[-1][1]


# %%
def _compute_person_stats(person: str, active_note_ids: list[str]) -> dict:
    """计算指定人员的更新成就统计。"""
    data = get_daily_stats_by_person(person, active_note_ids)
    if not data:
        return {"streak": 0, "week_max": 0, "week_max_date": None, "month_total": 0, "eff_today": date.today()}

    daily_max: dict = {}
    for _title, dc in data.items():
        for d, (wc, *_) in dc.items():
            if isinstance(d, str):
                d = datetime.strptime(d, "%Y-%m-%d").date()
            daily_max[d] = max(daily_max.get(d, 0), wc)

    now_local = arrow.now(get_localzone())
    day_identity = now_local.replace(hour=7, minute=30, second=0, microsecond=0)
    if now_local.hour < 7 or (now_local.hour == 7 and now_local.minute < 30):
        day_identity = day_identity.shift(days=-1)
    eff_today = day_identity.date()

    streak = 0
    cursor = eff_today
    while cursor in daily_max and daily_max[cursor] > 0:
        streak += 1
        cursor -= timedelta(days=1)

    weekday = eff_today.weekday()
    week_start = eff_today - timedelta(days=weekday)
    week_max = 0
    week_max_date = None
    for i in range(7):
        d = week_start + timedelta(days=i)
        if d in daily_max and daily_max[d] > week_max:
            week_max = daily_max[d]
            week_max_date = d

    month_start = eff_today.replace(day=1)
    month_total = 0
    for i in range(31):
        d = month_start + timedelta(days=i)
        if d.month != eff_today.month:
            break
        month_total += daily_max.get(d, 0)

    return {
        "streak": streak,
        "week_max": week_max,
        "week_max_date": week_max_date,
        "month_total": month_total,
        "eff_today": eff_today,
    }


# %%
def _build_header(person: str, stats: dict) -> str:
    """构建热图笔记头部：称号 + 成就叙述 + 火花摘语。"""
    spark = _pick_spark_quote(person)
    title = _get_person_title(stats["streak"])
    weekday_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]

    if stats["streak"] == 0:
        streak_line = f"{title} —— 今天有望恢复\n\n"
    else:
        start = stats["eff_today"] - timedelta(days=stats["streak"] - 1)
        streak_line = f"{title} —— 已连续更新 **{stats['streak']}** 天，从 {start:%m-%d} 到 {stats['eff_today']:%m-%d}\n\n"

    if stats["week_max"] > 0 and stats["week_max_date"]:
        wd = weekday_names[stats["week_max_date"].weekday()]
        week_part = f"本周最高单日 **{stats['week_max']:,}** 字（{wd}）"
    else:
        week_part = "本周暂无记录"

    month_part = f"本月累计 **{stats['month_total']:,}** 字"

    parts = [
        f"# {person} · 日更动态\n\n",
        streak_line,
        f"{week_part} · {month_part}\n\n",
        "---\n\n",
    ]
    if spark and spark.get("text"):
        parts.append(f'> *"{spark["text"]}"*\n')
        parts.append(">\n")
        source = spark.get("source_date", "")
        if source:
            try:
                sd = datetime.strptime(source, "%Y年%m月%d日")
                source = sd.strftime("%m/%d")
            except ValueError:
                pass
        parts.append(f"> — 思想火花{f' · {source}' if source else ''}\n\n")
        parts.append("---\n\n")

    return "".join(parts)


# %%
def _build_backfill_summary(data: dict) -> str:
    """从 daily_stats 数据中提取延期补填条目，生成 markdown 汇总表。"""
    entries = []
    for title, daily_counts in data.items():
        for date_str, (wc, is_backfill, *rest) in daily_counts.items():
            if is_backfill:
                captured_at = rest[0] if rest else None
                entries.append((title, date_str, wc, captured_at))

    if not entries:
        return ""

    entries.sort(key=lambda x: x[1], reverse=True)

    lines = [
        "### 延期补填记录\n\n",
        "> 截止时间：次日 07:30，超时补填将记录于此。\n\n",
        "| 笔记 | 日期 | 应完成于 | 记录时间 | 字数 |\n",
        "|:--|:--|:--|:--|--:|\n",
    ]
    for title, date_str, wc, captured_at in entries:
        d = datetime.strptime(date_str, "%Y-%m-%d")
        date_display = d.strftime("%m/%d")
        deadline = (d + timedelta(days=1)).strftime("%m/%d") + " 07:30"
        if captured_at:
            if isinstance(captured_at, str):
                captured_at = datetime.fromisoformat(captured_at)
            captured_display = captured_at.strftime("%m/%d %H:%M")
        else:
            captured_display = "-"
        lines.append(f"| {title} | {date_display} | {deadline} | {captured_display} | {wc:,} |\n")

    return "".join(lines)


# %%
def _build_footer() -> str:
    """构建热图笔记尾部：折叠规则 + 更新时间。"""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    return (
        "<details>\n"
        "<summary>📋 统计规则</summary>\n\n"
        "- 每天 07:30 起算新一天 · 编辑完 30 分钟后生效\n"
        "- 按当天标题段落统计字数 · 同一人多篇取当日最高\n"
        "- 绿色越深字数越多 · 黄色为有标题无内容 · 灰色虚框为延迟补填\n"
        "\n</details>\n\n"
        f"*🤖 轻行动AI自动更新于 {now_str}*\n"
    )


# %% [markdown]
# ## 全量报告生成入口


# %%
def generate_all_reports(dirty_only: bool = True) -> dict:
    """生成文字报告 + 热图报告。

    Args:
        dirty_only: True=仅更新有变更的person, False=全量更新

    Returns:
        {'text_report_id': str, 'heatmap_persons': [str, ...]}
    """
    global _spark_cache
    _spark_cache = None
    init_db()

    # 文字报告总是更新（汇总所有笔记）
    text_id = generate_text_report()

    # 热图：确定需要更新的person（云端配置优先）
    if plst_str := getinivaluefromcloud("monitor", "person_list"):
        all_persons = [p.strip() for p in plst_str.split("，") if p.strip()]
    else:
        all_persons = list(get_person_set())

    if dirty_only:
        dirty_set = get_dirty_persons()
        persons = [p for p in all_persons if p in dirty_set] if dirty_set else []
        if not persons:
            log.info("无脏标记person，跳过热图更新")
            return {"text_report_id": text_id, "heatmap_persons": []}
    else:
        persons = all_persons

    log.info(f"将为以下人员生成热图: {persons}")

    # 获取最新的活跃笔记ID列表
    active_notes = {n["note_id"] for n in get_active_notes()}
    jpapi = retry_jp(getapi)

    heatmap_done = []
    for person in persons:
        try:
            data = get_daily_stats_by_person(person, list(active_notes))
            if not data:
                log.info(f"{person} 无有效数据，跳过热图")
                continue

            stats = _compute_person_stats(person, list(active_notes))
            header = _build_header(person, stats)
            heatmap_id = ensure_heatmap_note(person)
            old_note = getnote(heatmap_id)
            expected_title = f"日更动态（{person}）"
            if getattr(old_note, "title", "") != expected_title:
                retry_jp(updatenote_title, heatmap_id, expected_title)
            new_body_parts = [header]

            for title, daily_counts in data.items():
                img_path = plot_word_counts(daily_counts, f"{title}-{person}")
                try:
                    alt = f"{person} · {title}每日更新热图"
                    res_id = retry_jp(jpapi.add_resource, img_path, title=alt)
                    valid_days = sum(1 for (wc, *_) in daily_counts.values() if wc > 0)
                    total_wc = sum(wc for (wc, *_) in daily_counts.values() if wc > 0)
                    avg_wc = total_wc // valid_days if valid_days > 0 else 0
                    new_body_parts.append(f"## {title}\n\n")
                    new_body_parts.append(
                        f"近三月有效记录 **{valid_days}** 天，累计 **{total_wc:,}** 字，日均约 **{avg_wc:,}** 字\n\n"
                    )
                    new_body_parts.append(f"![{alt}](:/{res_id})\n\n")
                    new_body_parts.append("\n---\n\n")
                except Exception as e:
                    log.critical(f"上传热图资源失败（{title}-{person}）: {e}")

            if new_body_parts:
                new_body_parts.append(_build_backfill_summary(data))
                new_body_parts.append(_build_footer())
                new_body = "".join(new_body_parts)
                retry_jp(updatenote_body, heatmap_id, new_body)

                # 清理旧资源
                old_lines = getattr(old_note, "body").split()
                old_res_ids = [
                    re.search(r"\(:/(.+)\)", line).group(1) for line in old_lines if re.search(r"\(:/(.+)\)", line)
                ]
                for resid in old_res_ids:
                    try:
                        jpapi.delete_resource(resid)
                    except Exception:
                        pass

            heatmap_done.append(person)
            log.info(f"{person} 热图已更新至笔记 {heatmap_id}")

        except Exception as e:
            log.critical(f"生成 {person} 热图时出错: {e}")

    # 清除脏标记
    clear_dirty()

    return {"text_report_id": text_id, "heatmap_persons": heatmap_done}


# %% [markdown]
# ## 主函数，__main__

# %%
if __name__ == "__main__":
    if not_IPython():
        log.info(f"开始运行文件\t{__file__}")

    result = generate_all_reports(dirty_only=True)
    print(f"报告生成结果: {result}")

    if not_IPython():
        log.info(f"Done.结束执行文件\t{__file__}")
