# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     notebook_metadata_filter: jupytext,-kernelspec,-jupytext.text_representation.jupytext_version
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
# ---

# %% [markdown]
# # 轮询监测结果图形化输出

# %% [markdown]
# ## 引入库

# %%
import base64
import io
import json
import re
from datetime import date, datetime, timedelta
from pathlib import Path

import arrow
import matplotlib.colors as mcolors
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# from memory_profiler import profile
from tzlocal import get_localzone

# %%
import pathmagic

with pathmagic.context():
    from func.configpr import getcfpoptionvalue, setcfpoptionvalue
    from func.datetimetools import normalize_timestamp
    from func.first import dirmainpath, getdirmain, touchfilepath2depth
    from func.jpfuncs import (
        content_hash,
        createnote,
        createresourcefromobj,
        getapi,
        getinivaluefromcloud,
        getnote,
        searchnotes,
        updatenote_body,
    )
    from func.logme import log

    # from func.termuxtools import termux_location, termux_telephony_deviceinfo
    # from func.nettools import ifttt_notify
    # from etc.getid import getdevicename, gethostuser
    from func.sysfunc import after_timeout, execcmd, not_IPython, set_timeout
    from work.monitor4 import NoteMonitor


# %% [markdown]
# ## 核心函数

# %% [markdown]
# ### stat2df()


# %%
def stat2df(person):
    """
    统计指定人员的相关笔记更新记录
    """
    note_monitor = NoteMonitor()
    # 筛选出指定person的数据字典
    targetdict = {
        k: v for k, v in note_monitor.monitored_notes.items() if person == v["person"]
    }
    # print(targetdict)
    # 过滤日期超过当天一天之内的日期数据对
    one_days_later = arrow.now(get_localzone()).shift(days=1).date()
    title_count_dict = {}

    for note_id, note_info in targetdict.items():
        daily_counts = {}  # 采用字典数据类型，确保日期唯一
        # 过滤掉可能的超纲日期
        for date_key, updates in [
            (date, updates)
            for date, updates in note_info["content_by_date"].items()
            if date < one_days_later
        ]:
            # 获取最后一次更新的字数
            last_update_time, word_count = updates[-1]
            # 检查 updates 的长度
            if len(updates) > 1:
                addedLater = True  # 标记该日期
            else:
                addedLater = False
            daily_counts[date_key] = (
                word_count,
                addedLater,
            )  # 使用日期作为键，字数和后补布林值作为值

        outlst = sorted(daily_counts.items(), key=lambda kv: kv[0])
        title_count_dict[note_info["title"]] = dict(outlst)
    return title_count_dict


# %% [markdown]
# ### plot_word_counts(daily_counts, title)


# %%
def plot_word_counts(daily_counts, title):
    """
    图形化输出函数，使用热图展示每天的字数统计。

    Args:
      daily_counts: 包含日期和对应字数的字典，例如：
        {'2023-10-26': 1200, '2023-10-27': 850, ...}
      title: 图表的标题。

    Returns:
      包含生成的 PNG 图像的文件路径。
    """
    # 准备输出图片的目录和文件名
    img_dir = Path(getdirmain()) / "img"
    img_dir.mkdir(parents=True, exist_ok=True)  # 自动创建目录
    img_heat_file_path = img_dir / "heatmap.png"
    img_heat_file_path_str = str(img_heat_file_path.absolute())

    if (monthrange := getinivaluefromcloud("monitor", "monthrange")) is None:
        monthrange = 3

    # 1. 数据预处理
    dfall = pd.DataFrame(
        [[k, v[0], v[1]] for k, v in daily_counts.items()],
        columns=["date", "count", "addedlater"],
    )
    # print(df)
    dfall["date"] = pd.to_datetime(dfall["date"])

    # 新2. 确定日期范围（最近三个月）
    current_date = pd.to_datetime(arrow.now(get_localzone()).date())
    three_months_ago = current_date - pd.DateOffset(months=monthrange)

    # 过滤有效数据（允许补填但限制范围）
    valid_dates = [
        dt
        for dt in daily_counts.keys()
        if (dt >= three_months_ago.date())
        or (
            daily_counts[dt][1]
            and dt >= (three_months_ago - pd.DateOffset(months=1)).date()
        )
    ]
    if not valid_dates:
        fig, ax = plt.subplots(figsize=(10, 6))  # 统一尺寸
        ax.text(0.5, 0.5, "暂时没有有效数据", ha="center", va="center", fontsize=20)
        plt.savefig(img_heat_file_path_str)
        plt.close()
        return img_heat_file_path_str  # 返回提示图而非空白

    # 强制从指定月数前开始
    min_date = max(min(dfall["date"]), three_months_ago)
    max_date = current_date

    # 新3. 创建完整日期范围（保证周完整性）
    start_date = min_date - pd.Timedelta(days=min_date.weekday())
    # 处理有效内容涵盖了起始周周一的情况
    if start_date in list(dfall["date"]):
        min_date = start_date
    # 按照日期截取dfall
    dfready = dfall[dfall.date >= min_date]
    end_date = max_date + pd.Timedelta(days=6 - max_date.weekday())
    # 全须全尾的周列表
    all_dates = pd.date_range(start=start_date, end=end_date)
    # 转换为DataFrame
    all_dates_df = pd.DataFrame({"date": all_dates, "count": -1})

    # 4. 合并数据，保留原始 count 值
    df = pd.concat([dfready, all_dates_df], ignore_index=True)
    df = df.drop_duplicates(subset=["date"], keep="first")
    df = df.sort_values(by="date").reset_index(drop=True)
    if getinivaluefromcloud("monitor", "debug"):
        print(df)
        log.info(
            f"数据记录最早日期为：{min(dfall['date'])}，最新日期为：{max(dfall['date'])}；最近{monthrange}个月的记录最早日期为：{min(dfready['date'])}，最新日期为：{max(dfready['date'])}；规整后全须全尾周的记录最早日期为：{min(df['date'])}，最新日期为：{max(df['date'])}"
        )

    # 5. 添加年份和星期几列
    df["year"] = df["date"].dt.year
    df["week"] = df["date"].dt.isocalendar().week
    df["day_of_week"] = df["date"].dt.weekday

    # 6. 计算自然周序号
    df["week_number"] = ((df["date"] - start_date).dt.days // 7) + 1

    # 7. 创建透视表
    dfcount = df[["year", "week", "week_number", "day_of_week", "date", "count"]]
    # print(dfcount)
    pivot_table = df.pivot_table(
        index="week_number",
        columns="day_of_week",
        values="count",
        aggfunc="sum",
        fill_value=-1,
    )

    # 8. 自定义颜色映射
    white = "white"
    warning_color = "#FFD700"

    # --- Dynamically determine the number of color bins ---
    max_count = int(df["count"].max())
    num_bins = min(max_count + 2, 254)  # Limit bins to a maximum of 254

    # --- CORRECTED BOUNDARIES CALCULATION ---
    boundaries = [-1, 0] + list(np.linspace(1, max_count + 1, num_bins - 1))
    # ----------------------------------------

    # 根据 count 值生成颜色
    greens = list(plt.cm.Greens(np.linspace(0.3, 1, num_bins - 2)))
    colors = [white, warning_color] + greens
    cmap = mcolors.ListedColormap(colors)

    # 9. 设置边界，确保 count 为零时使用警示颜色，count 为 -1 时使用白色
    norm = mcolors.BoundaryNorm(boundaries=boundaries, ncolors=cmap.N, clip=True)

    # 10. 创建图形
    if pivot_table.values.max() <= 0:
        fig, ax = plt.subplots(figsize=(10, 6))  # 统一尺寸
        ax.text(0.5, 0.5, "最近三个月无有效更新", ha="center", va="center", fontsize=20)
        plt.savefig(img_heat_file_path_str)
        plt.close()
        return img_heat_file_path_str  # 返回提示图而非空白
    # 设置动态分辨率
    figsize_factor = max(1, len(pivot_table) // 10)  # 每10周增加1英寸高度
    fig, ax = plt.subplots(figsize=(15, 6 + figsize_factor))

    # 11. 绘制热图
    heatmap = ax.pcolor(
        pivot_table.values, cmap=cmap, norm=norm, edgecolors="white", linewidths=2
    )

    # 12. 设置刻度和标签
    week_labels = []
    for week_num in pivot_table.index:
        week_start = start_date + pd.Timedelta(weeks=week_num - 1)
        week_str = f"{week_start.strftime('%m-%d')}"
        week_labels.append(week_str)

    ax.set_yticks(np.arange(len(pivot_table.index)) + 0.5)
    ax.set_yticklabels(week_labels)
    ax.set_xticks(np.arange(7) + 0.5, minor=False)
    ax.set_xticklabels(["一", "二", "三", "四", "五", "六", "日"], minor=False)
    # 新增月份分割线
    month_locs = []
    month_labels = []
    temp_date = start_date
    while temp_date <= end_date:
        if temp_date.day == 1:  # 每月第一天
            week_num = ((temp_date - start_date).days // 7) + 1
            month_locs.append(week_num - 0.5)
            month_labels.append(temp_date.strftime("%Y-%m"))
        temp_date += pd.DateOffset(days=1)

    ax.hlines(month_locs, -0.5, 6.5, colors="gray", linestyles="dashed", linewidth=0.5)

    # 13. 反转 y 轴
    ax.invert_yaxis()

    # 14. 添加颜色条
    cbar = plt.colorbar(heatmap)
    cbar.set_label("更新字数")

    # 15. 设置标题
    plt.title(title)

    # --- 添加起始日期和当前日期标记 ---
    # 找到最小日期和当前日期在热图中的位置
    min_date_week = df[df["date"] == min_date]["week_number"].iloc[0]
    # print(df[df['date'] == min_date])
    min_date_weekday = df[df["date"] == min_date]["day_of_week"].iloc[0]
    # print(df[df['date'] == current_date])
    current_date_week = df[df["date"] == current_date]["week_number"].iloc[0]
    current_date_weekday = df[df["date"] == current_date]["day_of_week"].iloc[0]

    # 在最小日期的格子中添加日期文本
    ax.text(
        min_date_weekday + 0.5,
        min_date_week - min(pivot_table.index) + 0.5,
        min_date.strftime("%m-%d"),
        ha="center",
        va="center",
        color="black",
        fontsize=8,
    )

    # 为当前日期的格子添加红色虚线边框
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
    # 在需要标记的日期上添加灰色虚线外框
    for marked_date in df[df.addedlater == True]["date"]:
        week = dfcount[dfcount["date"] == marked_date]["week_number"].values[0]
        day_of_week = dfcount[dfcount["date"] == marked_date]["day_of_week"].values[0]
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
    # --- 标记添加完成 ---

    # 16. 将图像保存到 BytesIO 对象并返回
    plt.savefig(img_heat_file_path_str)
    plt.close()
    # buffer = io.BytesIO()
    # plt.savefig(buffer, format='png')
    # buffer.seek(0)
    # plt.close(fig)

    return img_heat_file_path_str


# %% [markdown]
# ### get_heatmap_note_id(person)


# %%
def get_heatmap_note_id(person):
    # 查找指定人员热图笔记的id并返回
    if (
        person_heatmap_id := getcfpoptionvalue("happyjpmonitor", "person_ids", person)
    ) is None:
        results = searchnotes(f"四件套更新热图（{person}）")
        if results:
            person_heatmap_id = results[0].id
        else:
            person_heatmap_id = createnote(
                title=f"四件套更新热图（{person}）", body="热图笔记已创建。"
            )
        setcfpoptionvalue("happyjpmonitor", "person_ids", person, person_heatmap_id)

    return person_heatmap_id


# %% [markdown]
# ### get_refresh_id_list


# %%
def get_refresh_id_list():
    # 获取最新额《四件套笔记列表》中有效的笔记id列表
    title = "四件套笔记列表"
    results = searchnotes(f"{title}")
    if results:
        note_list_id = results[0].id
    else:
        log.critical(f"标题为：《{title}》的笔记不存在")
        return list()
    note = getnote(note_list_id)
    note_id_list_refresh = [
        re.search(r"\(:/(.+)\)", link).group(1)
        for link in note.body.split()
        if re.search(r"\(:/(.+)\)", link)
    ]

    return note_id_list_refresh


# %% [markdown]
# ### heatmap2note()


# %%
# @profile
def heatmap2note():
    # 监控笔记
    note_monitor = NoteMonitor()
    ptn = re.compile(r"[(（](\w+)[)）]")
    # 获取person列表
    if plststr := getinivaluefromcloud("monitor", "person_list"):
        person_lst = plststr.split("，")
    else:
        person_lst = list(
            set(
                [
                    re.findall(ptn, info["title"])[0]
                    for note_id, info in note_monitor.monitored_notes.items()
                ]
            )
        )
    print(person_lst)
    # 获取最新的被监控笔记id列表
    note_id_list_refresh = get_refresh_id_list()
    jpapi = getapi()
    for person in person_lst:
        # 筛选出指定person相关的{note_id:note_info}字典，同时确保只处理当前列表中的笔记
        targetdict = {
            k: v
            for k, v in note_monitor.monitored_notes.items()
            if person in v["title"]
        }
        should_plot = False
        refreshdict = {k: v for k, v in targetdict.items() if k in note_id_list_refresh}
        print(f">》{person}")
        for note_id, note_info in refreshdict.items():
            print(f"{note_id}\t{note_info['title']}")
            if len(note_info["content_by_date"]) == 0:
                log.info(f"笔记《{note_info['title']}》的有效日期内容为空，跳过")
                continue
            note_ini_time = normalize_timestamp(
                getcfpoptionvalue("happyjpmonitor", "note_update_time", note_id)
            )
            note_json_time = normalize_timestamp(note_info["note_update_time"])
            note_cloud_time = normalize_timestamp(
                getattr(getnote(note_id), "updated_time")
            )
            current_hash = content_hash(note_id)
            stored_hash = getcfpoptionvalue("happyjpmonitor", "content_hash", note_id)
            # 时间对不上内容也对不上然后才打开开关画图
            if (
                not note_ini_time
                or (note_ini_time != note_json_time)
                or (note_ini_time != note_cloud_time)
            ):
                if current_hash != stored_hash:
                    should_plot = True
                    log.debug(
                        f"[笔记ID:{note_id}]，本地存储时间: {note_ini_time}，监测爬取记录时间: {note_json_time}，云端新鲜时间: {note_cloud_time}，笔记内容哈希比对: {current_hash} vs {stored_hash}，触发条件: {should_plot}"
                    )
        if getinivaluefromcloud("monitor", "debug"):
            if person == "白晔峰":
                should_plot = True
        if should_plot:
            heatmap_id = get_heatmap_note_id(person)
            oldnote = getnote(heatmap_id)

            newbodystr = ""
            for k, v in stat2df(person).items():
                # buffer = plot_word_counts(v, k)
                img_heat_file_path = plot_word_counts(v, k)
                title = f"{k}-{person}"
                res_id = jpapi.add_resource(img_heat_file_path, title=title)
                newbodystr += f"![{title}](:/{res_id})" + "\n"

            updatenote_body(heatmap_id, newbodystr)
            # 操作成功后再删除原始笔记中的资源文件
            lines = getattr(oldnote, "body").split()
            res_ids = [
                re.search(r"\(:/(.+)\)", line).group(1)
                for line in lines
                if re.search(r"\(:/(.+)\)", line)
            ]
            print(
                f"笔记《{getattr(oldnote, 'title')}》中包含以下资源文件：{res_ids}，不出意外将被删除清理！"
            )
            for resid in res_ids:
                try:
                    jpapi.delete_resource(resid)
                except Exception as e:
                    print(e)
        # 操作成功后再setcfpoptionvalue
        for note_id, note_info in refreshdict.items():
            current_hash = content_hash(note_id)
            stored_hash = getcfpoptionvalue("happyjpmonitor", "content_hash", note_id)
            if current_hash != stored_hash:
                setcfpoptionvalue(
                    "happyjpmonitor", "content_hash", note_id, current_hash
                )
            note_time_with_zone = (
                arrow.get(note_info["note_update_time"])
                .to(get_localzone())
                .strftime("%Y-%m-%d %H:%M:%S")
            )
            if note_ini_time != note_json_time:
                setcfpoptionvalue(
                    "happyjpmonitor", "note_update_time", note_id, note_time_with_zone
                )


# %% [markdown]
# ## 主函数，__main__

# %%
if __name__ == "__main__":
    if not_IPython():
        log.info(f"开始运行文件\t{__file__}")

    heatmap2note()

    if not_IPython():
        log.info(f"Done.结束执行文件\t{__file__}")
