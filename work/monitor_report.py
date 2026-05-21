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

    results = searchnotes(f"四件套更新热图（{person}）")
    if results:
        note_id = results[0].id
    else:
        note_id = createnote(title=f"四件套更新热图（{person}）", body="热图笔记已创建。")
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

    current_day_identity = arrow.now(get_localzone()).replace(hour=8, minute=0, second=0, microsecond=0)
    if arrow.now().hour < 8:
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
    all_dates_df = pd.DataFrame({"date": all_dates, "count": -1})

    df = pd.concat([dfready, all_dates_df], ignore_index=True)
    df = df.drop_duplicates(subset=["date"], keep="first")
    df = df.sort_values(by="date").reset_index(drop=True)

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
    for marked_date_str in df[df["addedlater"].fillna(False).infer_objects(copy=False)]["date"]:
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
_spark_cache: dict | None = None


def _get_spark_candidates() -> list[str]:
    """解析思想火花笔记，返回所有10~30字候选句子。模块级缓存避免重复API调用。"""
    global _spark_cache
    if _spark_cache is not None:
        return _spark_cache

    try:
        results = searchnotes("思想火花-（白晔峰）")
        if not results:
            return []
        body = getattr(getnote(results[0].id), "body", "")
        if not body:
            return []

        ptn_date = re.compile(r"^###\s+\d{4}\s*年\s*\d{1,2}\s*月\s*\d{1,2}\s*[日号]\s*$", re.M)
        sections = re.split(ptn_date, body.strip())

        quotes = []
        for section in sections[1:]:
            items = re.findall(
                r"(?:^|\n)\d+[\.\、\)）]\s*(.+?)(?=\n\d+[\.\、\)）]|\n###|\Z)", section, re.S
            )
            for item in items:
                clean = item.strip()
                clean = re.sub(r'["""\']+', "", clean)
                clean = re.sub(r"（[^）]*$", "", clean)
                if 10 <= len(clean) <= 30 and not clean.startswith("+"):
                    quotes.append(clean)

        _spark_cache = quotes
        return quotes
    except Exception:
        return []


def _pick_spark_quote() -> str:
    """选取一条火花语录，排除最近7天已用过的。返回空字符串表示无可选语录。"""
    candidates = _get_spark_candidates()
    if not candidates:
        return ""

    cleanup_spark_log(7)
    used = get_used_spark_hashes(7)

    available = [q for q in candidates if hashlib.md5(q.encode()).hexdigest() not in used]

    if not available:
        available = candidates  # 都用过了就重置

    chosen = random.choice(available)
    today_str = date.today().strftime("%Y-%m-%d")
    add_spark_log(today_str, hashlib.md5(chosen.encode()).hexdigest())
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
        for d, (wc, _) in dc.items():
            if isinstance(d, str):
                d = datetime.strptime(d, "%Y-%m-%d").date()
            daily_max[d] = max(daily_max.get(d, 0), wc)

    now_local = arrow.now(get_localzone())
    day_identity = now_local.replace(hour=8, minute=0, second=0, microsecond=0)
    if now_local.hour < 8:
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

    return {"streak": streak, "week_max": week_max, "week_max_date": week_max_date, "month_total": month_total, "eff_today": eff_today}


# %%
def _build_header(person: str, stats: dict) -> str:
    """构建热图笔记头部的称号+火花+成就表格。"""
    spark = _pick_spark_quote()
    title = _get_person_title(stats["streak"])

    streak_str = (
        f"今天有望恢复" if stats["streak"] == 0
        else f"{stats['eff_today'] - timedelta(days=stats['streak'] - 1):%m-%d} → {stats['eff_today']:%m-%d}"
    )

    week_max_str = (
        f"{stats['week_max']} 字" if stats["week_max"] > 0
        else "暂无"
    )
    week_date_str = (
        f"{stats['week_max_date']:%m-%d 周%a}" if stats["week_max_date"]
        else ""
    )
    week_date_str = week_date_str.replace("Mon", "一").replace("Tue", "二").replace("Wed", "三").replace("Thu", "四").replace("Fri", "五").replace("Sat", "六").replace("Sun", "日")

    parts = [f"> {title}\n"]
    if spark:
        parts.append(f'> 💡 *"{spark}"*\n')
    parts.append("\n")
    parts.append("| 🔥 连续更新 | 🏆 本周最高 | 📝 本月累计 |\n")
    parts.append("|:---:|:---:|:---:|\n")
    parts.append(f"| **{stats['streak']}** 天 | **{week_max_str}** | **{stats['month_total']:,}** 字 |\n")
    parts.append(f"| {streak_str} | {week_date_str} | {stats['eff_today']:%m}月至今 |\n")
    parts.append("\n---\n\n")

    return "".join(parts)


# %%
def _build_footer() -> str:
    """构建热图笔记底部规则说明。"""
    return (
        "\n---\n\n"
        "> 📋 **更新统计规则**\n>\n"
        "> - ⏰ **日界**：早上 08:00 前更新计入前一天，08:00 后计入当天\n"
        "> - ⏳ **延迟**：编辑完成后需稳定 30 分钟才会被系统确认，不会即时反映\n"
        "> - 📏 **计数**：按每日 `### YYYY年MM月DD日` 段落统计字数，同一人多篇取当日最高\n"
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

    # 热图：确定需要更新的person
    if dirty_only:
        persons = get_dirty_persons()
        if not persons:
            log.info("无脏标记person，跳过热图更新")
            return {"text_report_id": text_id, "heatmap_persons": []}
    else:
        persons = list(get_person_set())

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
            new_body_parts = [header]

            for title, daily_counts in data.items():
                img_path = plot_word_counts(daily_counts, f"{title}-{person}")
                try:
                    res_id = retry_jp(jpapi.add_resource, img_path, title=f"{title}-{person}")
                    new_body_parts.append(f"![{title}-{person}](:/{res_id})\n")
                except Exception as e:
                    log.critical(f"上传热图资源失败（{title}-{person}）: {e}")

            if new_body_parts:
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
