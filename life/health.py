# -*- coding: utf-8 -*-
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
# # 健康笔记

# %% [markdown]
# ## 引入库

# %% [markdown]
# ### 核心库

# %%
import base64
import calendar
import io
import re
from datetime import datetime, timedelta

import arrow
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# %%
import pathmagic

with pathmagic.context():
    from etc.getid import getdeviceid, gethostuser
    from func.configpr import getcfpoptionvalue, setcfpoptionvalue
    from func.datetimetools import datecn2utc
    from func.jpfuncs import (
        createnote,
        getinivaluefromcloud,
        getnote,
        noteid_used,
        searchnotebook,
        searchnotes,
        updatenote_body,
        updatenote_imgdata,
    )
    from func.logme import log
    from func.sysfunc import execcmd, not_IPython
    from func.wrapfuncs import timethis


# %% [markdown]
# ## 功能函数集

# %% [markdown]
#

# %% [markdown]
# ### debug_health_data(noteid)

# %%
def debug_health_data(noteid):
    """调试函数，用于检查健康数据提取过程中的问题"""
    healthnote = getnote(noteid)
    content = healthnote.body

    print("=== 原始笔记内容前500字符 ===")
    print(content[:500])
    print("\n=== 正则匹配测试 ===")

    # 测试正则表达式
    ptn = re.compile(
        r"###\s*(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日\s*\n"
        r"(\d+)\s*[,，]\s*(\d{1,2})\s*[:：]\s*(\d{1,2})\s*\n"
        r"([^#]*)"  # 备注部分（非#开头的内容）
    )

    matches = list(ptn.finditer(content))
    print(f"找到 {len(matches)} 条匹配记录")

    for i, match in enumerate(matches[:5]):  # 只显示前5条
        print(f"\n记录 {i + 1}:")
        print(f"  日期: {match.group(1)}年{match.group(2)}月{match.group(3)}日")
        print(f"  步数: {match.group(4)}")
        print(f"  睡眠: {match.group(5)}:{match.group(6)}")
        print(f"  备注: {match.group(7)[:50]}...")

    # 提取数据
    items = []
    for match in matches:
        year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
        steps = int(match.group(4))
        sleep_hour = int(match.group(5))
        sleep_minute = int(match.group(6))
        memo = match.group(7).strip()

        date_obj = datetime(year, month, day).date()
        sleep_total_minutes = sleep_hour * 60 + sleep_minute

        items.append({"日期": date_obj, "步数": steps, "睡眠时长": sleep_total_minutes, "随记": memo})

    if items:
        df = pd.DataFrame(items)
        print(f"\n=== 提取的DataFrame ===")
        print(f"形状: {df.shape}")
        print(f"列名: {df.columns.tolist()}")
        print(f"日期范围: {df['日期'].min()} 至 {df['日期'].max()}")

        # 检查重复日期
        duplicate_dates = df[df.duplicated(subset=["日期"], keep=False)]
        if not duplicate_dates.empty:
            print(f"\n⚠️ 发现重复日期:")
            for date in duplicate_dates["日期"].unique():
                date_records = df[df["日期"] == date]
                print(f"  日期 {date}: {len(date_records)} 条记录")
                for idx, row in date_records.iterrows():
                    print(f"    步数: {row['步数']}, 睡眠: {row['睡眠时长']}分钟")

        print(f"\n=== 前5条记录 ===")
        print(df.head())
    else:
        print("未提取到任何数据")

# %% [markdown]
# ### gethealthdatafromnote(noteid)
#


# %%
def gethealthdatafromnote(noteid):
    """从指定id的运动笔记获取数据，处理缺失日期，输出标准DataFrame"""
    healthnote = getnote(noteid)
    content = healthnote.body

    # 扩展的正则表达式，支持可选的啤酒瓶数字段
    # 格式1: 步数, 睡眠时长, 啤酒瓶数 (兼容旧格式)
    # 格式2: 步数, 睡眠时长, 啤酒:X (更易读)
    ptn = re.compile(
        r"###\s*(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日\s*\n"
        r"(\d+)\s*[,，]\s*(\d{1,2})\s*[:：]\s*(\d{1,2})"
        r"(?:\s*[,，]\s*(?:啤酒[:：]?\s*)?(\d+))?"  # 可选的啤酒瓶数字段
        r"\s*\n"
        r"([^#]*)"  # 备注部分
    )

    items = []
    for match in ptn.finditer(content):
        year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
        steps = int(match.group(4))
        sleep_hour = int(match.group(5))
        sleep_minute = int(match.group(6))

        # 啤酒瓶数（可选字段）
        beer_count = match.group(7)
        beer_count = int(beer_count) if beer_count else 0

        memo = match.group(8).strip()

        date_obj = datetime(year, month, day).date()
        sleep_total_minutes = sleep_hour * 60 + sleep_minute

        items.append(
            {"日期": date_obj, "步数": steps, "睡眠时长": sleep_total_minutes, "啤酒瓶数": beer_count, "随记": memo}
        )

    if not items:
        log.warning("未从笔记中提取到任何有效数据")
        return pd.DataFrame()

    # 创建DataFrame并按日期排序
    df = pd.DataFrame(items)

    # 检查是否有重复日期
    duplicate_dates = df[df.duplicated(subset=["日期"], keep=False)]
    if not duplicate_dates.empty:
        log.warning(f"发现重复日期记录: {duplicate_dates['日期'].unique().tolist()}")
        log.warning("将保留每个日期的最新记录")

        # 按日期分组，保留每个日期的最后一条记录（假设后面的记录是更新的）
        df = df.sort_values("日期").groupby("日期").last().reset_index()

    # 设置日期为索引
    df = df.set_index("日期").sort_index()

    # 处理缺失日期：填充完整日期范围
    if len(df) > 0:
        full_date_range = pd.date_range(start=df.index.min(), end=df.index.max(), freq="D")

        # 使用reindex填充缺失日期，但先确保索引是唯一的
        df = df[~df.index.duplicated(keep="last")]  # 再次确保没有重复

        # 重新索引，填充缺失日期
        df = df.reindex(full_date_range, fill_value=None)  # 缺失日期填充为NaN

        log.info(
            f"数据日期范围: {df.index.min()} 至 {df.index.max()}, 共{len(df)}天, 其中有效记录{df['步数'].count()}天"
        )
    else:
        log.warning("提取的数据为空")

    # 在返回前添加连续日期识别
    if not df.empty and "步数" in df.columns:
        # 找出最近的连续日期区间
        df["连续标记"] = df["步数"].notna().astype(int)

        # 识别连续段
        df["连续段"] = (df["连续标记"].diff() != 0).cumsum()

        # 找出最近的连续段
        recent_continuous = None
        for segment in sorted(df["连续段"].unique(), reverse=True):
            segment_data = df[df["连续段"] == segment]
            if segment_data["连续标记"].all():  # 全连续
                recent_continuous = segment_data
                break

        # 将连续段信息存储为DataFrame属性
        df.attrs["recent_continuous"] = recent_continuous

    return df


# %% [markdown]
# ### calds2ds(sds)


# %%
def calds2ds(sds):
    """根据输入的ds，按月合计并估算数据未满月的月份的整月值
    返回：月度合计ds、估算月度合计ds
    """
    # 使用'ME'代替'M'，避免FutureWarning
    sdsm_actual = sds.resample("ME").sum()

    # 创建估算的Series
    estimated_values = []
    estimated_dates = []

    # 对每个月份进行估算
    for month_start in sdsm_actual.index:
        # 获取该月的实际数据
        actual_value = sdsm_actual.loc[month_start]

        # 获取该月的所有数据点
        month_data = sds[sds.index.to_period("M") == month_start.to_period("M")]

        if len(month_data) == 0:
            # 如果该月没有数据，跳过
            continue

        # 获取该月的第一天和最后一天
        year = month_start.year
        month = month_start.month
        __, days_in_month = calendar.monthrange(year, month)

        # 获取该月数据覆盖的天数范围
        min_day_in_month = month_data.index.min().day
        max_day_in_month = month_data.index.max().day

        # 计算数据覆盖的天数
        days_covered = max_day_in_month - min_day_in_month + 1

        # 如果数据覆盖了整个月，则不需要估算
        if days_covered == days_in_month:
            estimated_value = actual_value
        else:
            # 估算整月值：实际值 ÷ 覆盖天数 × 当月总天数
            estimated_value = int(actual_value / days_covered * days_in_month)

        estimated_values.append(estimated_value)
        estimated_dates.append(month_start)

    # 创建估算的Series
    estimated_series = pd.Series(estimated_values, index=estimated_dates)

    return sdsm_actual, estimated_series


# %% [markdown]
# ### analyze_recent_continuous_data(hdf)

# %%
def analyze_recent_continuous_data(hdf):
    """分析最近的连续日期数据"""
    if "recent_continuous" not in hdf.attrs or hdf.attrs["recent_continuous"] is None:
        return None

    cont_df = hdf.attrs["recent_continuous"]

    if cont_df.empty:
        return None

    # 基本统计
    analysis = {
        "日期范围": f"{cont_df.index.min().strftime('%Y-%m-%d')} 至 {cont_df.index.max().strftime('%Y-%m-%d')}",
        "连续天数": len(cont_df),
        "步数统计": {
            "平均": cont_df["步数"].mean(),
            "总计": cont_df["步数"].sum(),
            "最高": cont_df["步数"].max(),
            "最低": cont_df["步数"].min(),
            "达标率": (cont_df["步数"] >= target).mean() * 100 if "target" in locals() else None,
        },
        "睡眠统计": {
            "平均小时": cont_df["睡眠时长"].mean() / 60 if "睡眠时长" in cont_df.columns else None,
            "总计小时": cont_df["睡眠时长"].sum() / 60 if "睡眠时长" in cont_df.columns else None,
        },
    }

    # 趋势分析（如果连续天数足够）
    if len(cont_df) >= 7:
        # 周对比
        if len(cont_df) >= 14:
            first_week = cont_df.iloc[:7]["步数"].mean()
            second_week = cont_df.iloc[7:14]["步数"].mean() if len(cont_df) >= 14 else None
            analysis["周对比"] = {
                "第一周平均": first_week,
                "第二周平均": second_week,
                "变化率": ((second_week - first_week) / first_week * 100) if second_week else None,
            }

    return analysis

# %% [markdown]
# ### hdf2imgbase64(hdf)


# %%
def hdf2imgbase64(hdf):
    """根据传入包含运动数据的DataFrame作图，处理缺失值，输出图形的bytes"""
    if hdf.empty or hdf["步数"].count() == 0:
        log.error("无有效数据可绘制图表")
        return create_error_image("无有效健康数据")

    # 从云端配置获取每日步数目标，获取不到则默认设置为8000步
    if not (target := getinivaluefromcloud("health", "step_day_target")):
        target = 8000
    else:
        target = int(target)

    # 从云端配置获取每日啤酒目标，获取不到则默认设置为2瓶
    if not (beer_target := getinivaluefromcloud("health", "beer_day_target")):
        beer_target = 2
    else:
        beer_target = int(beer_target)

    # 确保索引是DatetimeIndex
    if not isinstance(hdf.index, pd.DatetimeIndex):
        try:
            hdf.index = pd.to_datetime(hdf.index)
        except:
            log.error("无法将索引转换为日期时间格式")
            return create_error_image("日期格式错误")

    # 确保数据按日期排序
    hdf = hdf.sort_index()

    # 提取有效数据
    valid_steps = hdf["步数"].dropna()
    valid_sleep = hdf["睡眠时长"].dropna()

    # 提取啤酒数据（如果存在）
    valid_beer = None
    if "啤酒瓶数" in hdf.columns:
        valid_beer = hdf["啤酒瓶数"].dropna()

    # 使用calds2ds进行月度估算
    monthly_steps_actual = pd.Series()
    monthly_steps_estimated = pd.Series()
    monthly_sleep_actual = pd.Series()
    monthly_sleep_estimated = pd.Series()
    monthly_beer_actual = pd.Series()
    monthly_beer_estimated = pd.Series()

    if not valid_steps.empty:
        monthly_steps_actual, monthly_steps_estimated = calds2ds(valid_steps)

    if not valid_sleep.empty:
        monthly_sleep_actual, monthly_sleep_estimated = calds2ds(valid_sleep)

    if valid_beer is not None and not valid_beer.empty:
        monthly_beer_actual, monthly_beer_estimated = calds2ds(valid_beer)

    # 创建图表 - 6行布局（增加啤酒统计）
    fig = plt.figure(figsize=(15, 42), dpi=100)

    # ========== 1. 最近连续数据趋势图（支持三轴）==========
    ax1 = plt.subplot2grid((6, 2), (0, 0), colspan=2, rowspan=1)

    if "recent_continuous" in hdf.attrs and hdf.attrs["recent_continuous"] is not None:
        cont_df = hdf.attrs["recent_continuous"]

        # 提取步数和睡眠数据
        cont_steps = cont_df["步数"].dropna()
        cont_sleep = cont_df["睡眠时长"].dropna() if "睡眠时长" in cont_df.columns else pd.Series()

        # 检查是否有啤酒数据
        has_beer_data = "啤酒瓶数" in cont_df.columns and cont_df["啤酒瓶数"].notna().any()

        if not cont_steps.empty:
            if has_beer_data:
                # 三轴图表：步数（左）、睡眠（右1）、啤酒（右2）
                ax1_steps = ax1  # 左侧Y轴（步数）
                ax1_sleep = ax1.twinx()  # 右侧Y轴1（睡眠时长）
                ax1_beer = ax1.twinx()  # 右侧Y轴2（啤酒瓶数）

                # 调整啤酒Y轴位置，避免重叠
                ax1_beer.spines["right"].set_position(("outward", 60))

                # --- 绘制步数数据（左侧Y轴）---
                # 步数折线
                (line_steps,) = ax1_steps.plot(
                    cont_steps.index, cont_steps.values, "b-", lw=2, alpha=0.8, label="每日步数"
                )

                # 步数填充区域
                ax1_steps.fill_between(cont_steps.index, cont_steps.values, alpha=0.2, color="blue")

                # 步数移动平均（3日）
                if len(cont_steps) >= 3:
                    steps_ma = cont_steps.rolling(window=3, min_periods=1).mean()
                    (line_steps_ma,) = ax1_steps.plot(
                        steps_ma.index, steps_ma.values, "b--", lw=1.5, alpha=0.6, label="步数3日平均"
                    )

                # 步数目标线
                line_target_steps = ax1_steps.axhline(
                    y=target, color="orange", linestyle=":", alpha=0.7, label=f"步数目标({target}步)"
                )

                # 设置步数Y轴
                steps_min = max(0, cont_steps.min() * 0.8)
                steps_max = cont_steps.max() * 1.2
                ax1_steps.set_ylim(steps_min, steps_max)
                ax1_steps.set_ylabel("步数", color="blue", fontweight="bold")
                ax1_steps.tick_params(axis="y", labelcolor="blue")

                # --- 绘制睡眠数据（右侧Y轴1）---
                if not cont_sleep.empty:
                    # 转换为小时显示
                    sleep_hours = cont_sleep / 60

                    # 睡眠折线
                    (line_sleep,) = ax1_sleep.plot(
                        sleep_hours.index, sleep_hours.values, "g-", lw=2, alpha=0.8, label="睡眠时长"
                    )

                    # 睡眠填充区域
                    ax1_sleep.fill_between(sleep_hours.index, sleep_hours.values, alpha=0.2, color="green")

                    # 睡眠移动平均（3日）
                    if len(sleep_hours) >= 3:
                        sleep_ma = sleep_hours.rolling(window=3, min_periods=1).mean()
                        (line_sleep_ma,) = ax1_sleep.plot(
                            sleep_ma.index, sleep_ma.values, "g--", lw=1.5, alpha=0.6, label="睡眠3日平均"
                        )

                    # 睡眠目标线（7小时）
                    line_target_sleep = ax1_sleep.axhline(
                        y=7, color="orange", linestyle=":", alpha=0.7, label="睡眠目标(7小时)"
                    )

                    # 设置睡眠Y轴
                    sleep_min = max(0, sleep_hours.min() * 0.8)
                    sleep_max = sleep_hours.max() * 1.2
                    ax1_sleep.set_ylim(sleep_min, sleep_max)
                    ax1_sleep.set_ylabel("睡眠（小时）", color="green", fontweight="bold")
                    ax1_sleep.tick_params(axis="y", labelcolor="green")

                # --- 绘制啤酒数据（右侧Y轴2）---
                cont_beer = cont_df["啤酒瓶数"].dropna()
                if not cont_beer.empty:
                    # 啤酒柱状图
                    bars_beer = ax1_beer.bar(
                        cont_beer.index, cont_beer.values, width=0.6, alpha=0.5, color="gold", label="啤酒瓶数"
                    )

                    # 啤酒目标线
                    line_target_beer = ax1_beer.axhline(
                        y=beer_target, color="brown", linestyle="--", alpha=0.7, label=f"啤酒目标({beer_target}瓶)"
                    )

                    # 设置啤酒Y轴
                    beer_max = max(cont_beer.max() * 1.2, beer_target * 1.5)
                    ax1_beer.set_ylim(0, beer_max)
                    ax1_beer.set_ylabel("啤酒（瓶）", color="goldenrod", fontweight="bold")
                    ax1_beer.tick_params(axis="y", labelcolor="goldenrod")

                # 合并图例
                lines = [line_steps]
                labels = ["每日步数"]

                if "line_steps_ma" in locals():
                    lines.append(line_steps_ma)
                    labels.append("步数3日平均")

                lines.append(line_target_steps)
                labels.append(f"步数目标({target}步)")

                if not cont_sleep.empty:
                    lines.append(line_sleep)
                    labels.append("睡眠时长")

                    if "line_sleep_ma" in locals():
                        lines.append(line_sleep_ma)
                        labels.append("睡眠3日平均")

                    lines.append(line_target_sleep)
                    labels.append("睡眠目标(7小时)")

                if not cont_beer.empty:
                    lines.append(bars_beer)
                    labels.append("啤酒瓶数")
                    lines.append(line_target_beer)
                    labels.append(f"啤酒目标({beer_target}瓶)")

                # 添加图例（放在图表外部底部）
                ax1_steps.legend(lines, labels, loc="upper center", bbox_to_anchor=(0.5, -0.20), ncol=4, fontsize=8)

                # 添加数据统计标注
                stats_text = f"步数平均: {cont_steps.mean():.0f}步/天"
                if not cont_sleep.empty:
                    stats_text += f"\n睡眠平均: {sleep_hours.mean():.1f}小时/天"
                if not cont_beer.empty:
                    stats_text += f"\n啤酒平均: {cont_beer.mean():.1f}瓶/天"

                ax1_steps.text(
                    0.02,
                    0.98,
                    stats_text,
                    transform=ax1_steps.transAxes,
                    fontsize=9,
                    verticalalignment="top",
                    bbox=dict(boxstyle="round", facecolor="lightblue", alpha=0.7),
                )

            else:
                # 没有啤酒数据，使用原来的双轴图表
                # 创建双Y轴
                ax1_steps = ax1  # 左侧Y轴（步数）
                ax1_sleep = ax1.twinx()  # 右侧Y轴（睡眠时长）

                # --- 绘制步数数据（左侧Y轴）---
                (line_steps,) = ax1_steps.plot(
                    cont_steps.index, cont_steps.values, "b-", lw=2, alpha=0.8, label="每日步数"
                )

                # 步数填充区域
                ax1_steps.fill_between(cont_steps.index, cont_steps.values, alpha=0.2, color="blue")

                # 步数移动平均（3日）
                if len(cont_steps) >= 3:
                    steps_ma = cont_steps.rolling(window=3, min_periods=1).mean()
                    (line_steps_ma,) = ax1_steps.plot(
                        steps_ma.index, steps_ma.values, "b--", lw=1.5, alpha=0.6, label="步数3日平均"
                    )

                # 步数目标线
                line_target_steps = ax1_steps.axhline(
                    y=target, color="orange", linestyle=":", alpha=0.7, label=f"步数目标({target}步)"
                )

                # 设置步数Y轴
                steps_min = max(0, cont_steps.min() * 0.8)
                steps_max = cont_steps.max() * 1.2
                ax1_steps.set_ylim(steps_min, steps_max)
                ax1_steps.set_ylabel("步数", color="blue", fontweight="bold")
                ax1_steps.tick_params(axis="y", labelcolor="blue")

                # --- 绘制睡眠数据（右侧Y轴）---
                if not cont_sleep.empty:
                    # 转换为小时显示
                    sleep_hours = cont_sleep / 60

                    # 睡眠折线
                    (line_sleep,) = ax1_sleep.plot(
                        sleep_hours.index, sleep_hours.values, "g-", lw=2, alpha=0.8, label="睡眠时长"
                    )

                    # 睡眠填充区域
                    ax1_sleep.fill_between(sleep_hours.index, sleep_hours.values, alpha=0.2, color="green")

                    # 睡眠移动平均（3日）
                    if len(sleep_hours) >= 3:
                        sleep_ma = sleep_hours.rolling(window=3, min_periods=1).mean()
                        (line_sleep_ma,) = ax1_sleep.plot(
                            sleep_ma.index, sleep_ma.values, "g--", lw=1.5, alpha=0.6, label="睡眠3日平均"
                        )

                    # 睡眠目标线（7小时）
                    line_target_sleep = ax1_sleep.axhline(
                        y=7, color="orange", linestyle=":", alpha=0.7, label="睡眠目标(7小时)"
                    )

                    # 设置睡眠Y轴
                    sleep_min = max(0, sleep_hours.min() * 0.8)
                    sleep_max = sleep_hours.max() * 1.2
                    ax1_sleep.set_ylim(sleep_min, sleep_max)
                    ax1_sleep.set_ylabel("睡眠（小时）", color="green", fontweight="bold")
                    ax1_sleep.tick_params(axis="y", labelcolor="green")

                # 合并图例
                lines = [line_steps]
                labels = ["每日步数"]

                if "line_steps_ma" in locals():
                    lines.append(line_steps_ma)
                    labels.append("步数3日平均")

                lines.append(line_target_steps)
                labels.append(f"步数目标({target}步)")

                if not cont_sleep.empty:
                    lines.append(line_sleep)
                    labels.append("睡眠时长")

                    if "line_sleep_ma" in locals():
                        lines.append(line_sleep_ma)
                        labels.append("睡眠3日平均")

                    lines.append(line_target_sleep)
                    labels.append("睡眠目标(7小时)")

                # 添加图例（放在图表外部底部）
                ax1_steps.legend(lines, labels, loc="upper center", bbox_to_anchor=(0.5, -0.15), ncol=3, fontsize=9)

                # 添加数据统计标注
                stats_text = f"步数平均: {cont_steps.mean():.0f}步/天"
                if not cont_sleep.empty:
                    stats_text += f"\n睡眠平均: {sleep_hours.mean():.1f}小时/天"

                ax1_steps.text(
                    0.02,
                    0.98,
                    stats_text,
                    transform=ax1_steps.transAxes,
                    fontsize=10,
                    verticalalignment="top",
                    bbox=dict(boxstyle="round", facecolor="lightblue", alpha=0.7),
                )

        else:
            # 无连续步数数据的情况
            ax1.text(0.5, 0.5, "无连续步数数据", ha="center", va="center", transform=ax1.transAxes, fontsize=12)

    ax1.set_title("最近连续记录趋势", fontsize=14, fontweight="bold")
    ax1.tick_params(axis="x", rotation=45)

    # ========== 2. 步数动态图 ==========
    ax2 = plt.subplot2grid((6, 2), (1, 0), colspan=2, rowspan=1)

    if not valid_steps.empty:
        # 绘制步数折线图
        ax2.plot(valid_steps.index, valid_steps.values, "b-", lw=1.5, label="每日步数", alpha=0.7)

        # 绘制步数散点图
        ax2.scatter(valid_steps.index, valid_steps.values, s=30, c="blue", alpha=0.5)

        # 添加7天移动平均线
        if len(valid_steps) >= 7:
            moving_avg = valid_steps.rolling(window=7, min_periods=1).mean()
            ax2.plot(moving_avg.index, moving_avg.values, "r-", lw=2, label="7天移动平均")

        # 添加目标线
        ax2.axhline(y=target, color="orange", linestyle="--", alpha=0.5, label=f"目标线({target}步)")

        # 标注最高和最低步数
        if len(valid_steps) > 1:
            max_step_idx = valid_steps.idxmax()
            min_step_idx = valid_steps.idxmin()
            ax2.annotate(
                f"最高: {valid_steps.max()}",
                xy=(max_step_idx, valid_steps.max()),
                xytext=(max_step_idx, valid_steps.max() + 500),
                arrowprops=dict(arrowstyle="->", color="red"),
                fontsize=9,
                color="red",
            )

            ax2.annotate(
                f"最低: {valid_steps.min()}",
                xy=(min_step_idx, valid_steps.min()),
                xytext=(min_step_idx, valid_steps.min() - 500),
                arrowprops=dict(arrowstyle="->", color="green"),
                fontsize=9,
                color="green",
            )

    ax2.set_title("步数动态图（完整历史）", fontsize=14, fontweight="bold")
    ax2.set_xlabel("日期")
    ax2.set_ylabel("步数")
    ax2.legend(loc="upper left")
    ax2.grid(True, alpha=0.3)
    ax2.tick_params(axis="x", rotation=45)

    # ========== 3. 月度步数统计图 ==========
    ax3 = plt.subplot2grid((6, 2), (2, 0), colspan=2, rowspan=1)

    if not valid_steps.empty and not monthly_steps_actual.empty:
        # 创建柱状图
        months = [date.strftime("%Y-%m") for date in monthly_steps_actual.index]
        x_positions = range(len(months))

        # 绘制实际月度数据（实心柱体）
        bars_actual = ax3.bar(
            x_positions,
            monthly_steps_actual.values,
            width=0.6,
            color="skyblue",
            alpha=0.8,
            edgecolor="black",
            label="实际月度合计",
        )

        # 绘制估算月度数据（虚线边框）
        if not monthly_steps_estimated.empty:
            for i, (actual_val, month_date) in enumerate(zip(monthly_steps_actual.values, monthly_steps_actual.index)):
                if month_date in monthly_steps_estimated.index:
                    est_val = monthly_steps_estimated.loc[month_date]

                    # 如果估算值大于实际值，显示虚线边框
                    if est_val > actual_val:
                        # 绘制虚线边框表示估算值
                        ax3.plot(
                            [i - 0.3, i + 0.3, i + 0.3, i - 0.3, i - 0.3],
                            [actual_val, actual_val, est_val, est_val, actual_val],
                            "r--",  # 红色虚线
                            linewidth=2,
                            alpha=0.8,
                            label="估算整月值" if i == 0 else "",  # 只在第一个柱体显示图例
                        )

                        # 在柱体顶部添加估算值标签
                        ax3.text(
                            i,
                            est_val + (est_val * 0.01),
                            f"估算:{int(est_val):,}",
                            ha="center",
                            va="bottom",
                            fontsize=8,
                            color="red",
                        )

        # 添加实际值标签
        for i, actual_val in enumerate(monthly_steps_actual.values):
            ax3.text(i, actual_val + (actual_val * 0.01), f"{int(actual_val):,}", ha="center", va="bottom", fontsize=9)

        # 设置x轴标签
        ax3.set_xticks(x_positions)
        ax3.set_xticklabels(months, rotation=45, fontsize=10)

        # 添加趋势线（基于实际数据）
        if len(monthly_steps_actual) > 1:
            ax3.plot(
                x_positions,
                monthly_steps_actual.values,
                "r-",
                marker="o",
                markersize=6,
                linewidth=2,
                alpha=0.7,
                label="月度趋势",
            )

    ax3.set_title("月度步数统计（实心：实际值，虚线：估算值）", fontsize=14, fontweight="bold")
    ax3.set_xlabel("月份")
    ax3.set_ylabel("总步数")
    ax3.legend(loc="upper left")
    ax3.grid(True, alpha=0.3, axis="y")

    # ========== 4. 睡眠时长动态图 ==========
    ax4 = plt.subplot2grid((6, 2), (3, 0), colspan=2, rowspan=1)

    if not valid_sleep.empty:
        # 转换为小时
        sleep_hours = valid_sleep / 60

        # 绘制睡眠时长
        ax4.plot(sleep_hours.index, sleep_hours.values, "g-", lw=1.5, label="每日睡眠时长", alpha=0.7)
        ax4.scatter(sleep_hours.index, sleep_hours.values, s=30, c="green", alpha=0.5)

        # 添加7天移动平均
        if len(sleep_hours) >= 7:
            sleep_avg = sleep_hours.rolling(window=7, min_periods=1).mean()
            ax4.plot(sleep_avg.index, sleep_avg.values, "purple", lw=2, label="7天移动平均")

        # 添加目标线（7小时）
        ax4.axhline(y=7, color="orange", linestyle="--", alpha=0.5, label="目标线(7小时)")

        # 标注最高和最低睡眠时长
        if len(sleep_hours) > 1:
            max_sleep_idx = sleep_hours.idxmax()
            min_sleep_idx = sleep_hours.idxmin()
            ax4.annotate(
                f"{sleep_hours.max():.1f}h",
                xy=(max_sleep_idx, sleep_hours.max()),
                xytext=(max_sleep_idx, sleep_hours.max() + 0.5),
                arrowprops=dict(arrowstyle="->", color="red"),
                fontsize=9,
                color="red",
            )

            ax4.annotate(
                f"{sleep_hours.min():.1f}h",
                xy=(min_sleep_idx, sleep_hours.min()),
                xytext=(min_sleep_idx, sleep_hours.min() - 0.5),
                arrowprops=dict(arrowstyle="->", color="green"),
                fontsize=9,
                color="green",
            )

    ax4.set_title("睡眠时长动态图（小时）", fontsize=14, fontweight="bold")
    ax4.set_xlabel("日期")
    ax4.set_ylabel("睡眠时长（小时）")
    ax4.legend(loc="upper left")
    ax4.grid(True, alpha=0.3)
    ax4.tick_params(axis="x", rotation=45)

    # ========== 5. 月度睡眠统计 ==========
    ax5 = plt.subplot2grid((6, 2), (4, 0), colspan=2, rowspan=1)

    if not valid_sleep.empty and not monthly_sleep_actual.empty:
        # 转换为小时
        monthly_sleep_hours_actual = monthly_sleep_actual / 60
        monthly_sleep_hours_estimated = monthly_sleep_estimated / 60

        months = [date.strftime("%Y-%m") for date in monthly_sleep_hours_actual.index]
        x_positions = range(len(months))

        # 创建柱状图
        bars_actual_sleep = ax5.bar(
            x_positions,
            monthly_sleep_hours_actual.values,
            width=0.6,
            color="lightgreen",
            alpha=0.8,
            edgecolor="black",
            label="实际月度合计",
        )

        # 绘制估算月度数据（虚线边框）
        if not monthly_sleep_hours_estimated.empty:
            for i, (actual_val, month_date) in enumerate(
                zip(monthly_sleep_hours_actual.values, monthly_sleep_hours_actual.index)
            ):
                if month_date in monthly_sleep_hours_estimated.index:
                    est_val = monthly_sleep_hours_estimated.loc[month_date]

                    # 如果估算值大于实际值，显示虚线边框
                    if est_val > actual_val:
                        # 绘制虚线边框表示估算值
                        ax5.plot(
                            [i - 0.3, i + 0.3, i + 0.3, i - 0.3, i - 0.3],
                            [actual_val, actual_val, est_val, est_val, actual_val],
                            "r--",  # 红色虚线
                            linewidth=2,
                            alpha=0.8,
                            label="估算整月值" if i == 0 else "",  # 只在第一个柱体显示图例
                        )

                        # 在柱体顶部添加估算值标签
                        ax5.text(
                            i,
                            est_val + (est_val * 0.01),
                            f"估算:{est_val:.1f}h",
                            ha="center",
                            va="bottom",
                            fontsize=8,
                            color="red",
                        )

        # 添加实际值标签
        for i, actual_val in enumerate(monthly_sleep_hours_actual.values):
            ax5.text(i, actual_val + (actual_val * 0.01), f"{actual_val:.1f}h", ha="center", va="bottom", fontsize=9)

        # 设置x轴标签
        ax5.set_xticks(x_positions)
        ax5.set_xticklabels(months, rotation=45, fontsize=10)

        # 添加趋势线
        if len(monthly_sleep_hours_actual) > 1:
            ax5.plot(
                x_positions,
                monthly_sleep_hours_actual.values,
                "b-",
                marker="s",
                markersize=6,
                linewidth=2,
                alpha=0.7,
                label="月度趋势",
            )

    ax5.set_title("月度睡眠时长统计（实心：实际值，虚线：估算值）", fontsize=14, fontweight="bold")
    ax5.set_xlabel("月份")
    ax5.set_ylabel("总睡眠时长（小时）")
    ax5.legend(loc="upper left")
    ax5.grid(True, alpha=0.3, axis="y")

    # ========== 6. 啤酒消费统计图（新增）==========
    ax6 = plt.subplot2grid((6, 2), (5, 0), colspan=2, rowspan=1)

    # 检查是否有啤酒数据
    if valid_beer is not None and not valid_beer.empty and not monthly_beer_actual.empty:
        # 创建柱状图
        months = [date.strftime("%Y-%m") for date in monthly_beer_actual.index]
        x_positions = range(len(months))

        # 绘制实际月度数据
        bars_beer = ax6.bar(
            x_positions,
            monthly_beer_actual.values,
            width=0.6,
            color="gold",
            alpha=0.8,
            edgecolor="darkgoldenrod",
            label="月度啤酒消费",
        )

        # 绘制估算月度数据（虚线边框）
        if not monthly_beer_estimated.empty:
            for i, (actual_val, month_date) in enumerate(zip(monthly_beer_actual.values, monthly_beer_actual.index)):
                if month_date in monthly_beer_estimated.index:
                    est_val = monthly_beer_estimated.loc[month_date]

                    # 如果估算值大于实际值，显示虚线边框
                    if est_val > actual_val:
                        # 绘制虚线边框表示估算值
                        ax6.plot(
                            [i - 0.3, i + 0.3, i + 0.3, i - 0.3, i - 0.3],
                            [actual_val, actual_val, est_val, est_val, actual_val],
                            "r--",  # 红色虚线
                            linewidth=2,
                            alpha=0.8,
                            label="估算整月值" if i == 0 else "",
                        )

                        # 在柱体顶部添加估算值标签
                        ax6.text(
                            i,
                            est_val + (est_val * 0.01),
                            f"估算:{int(est_val)}",
                            ha="center",
                            va="bottom",
                            fontsize=8,
                            color="red",
                        )

        # 添加实际值标签
        for i, actual_val in enumerate(monthly_beer_actual.values):
            if actual_val > 0:
                ax6.text(i, actual_val + 0.1, f"{int(actual_val)}", ha="center", va="bottom", fontsize=9)

        # 设置x轴标签
        ax6.set_xticks(x_positions)
        ax6.set_xticklabels(months, rotation=45, fontsize=10)

        # 添加趋势线
        if len(monthly_beer_actual) > 1:
            ax6.plot(
                x_positions,
                monthly_beer_actual.values,
                "brown",
                marker="o",
                markersize=6,
                linewidth=2,
                alpha=0.7,
                label="消费趋势",
            )

        # 添加月度目标线
        days_in_month = 30  # 近似值
        monthly_target = beer_target * days_in_month
        ax6.axhline(
            y=monthly_target,
            color="red",
            linestyle=":",
            alpha=0.5,
            label=f"月度目标({monthly_target}瓶)",
        )

        ax6.set_title("月度啤酒消费统计", fontsize=14, fontweight="bold")
        ax6.set_xlabel("月份")
        ax6.set_ylabel("啤酒瓶数")
        ax6.legend(loc="upper left")
        ax6.grid(True, alpha=0.3, axis="y")
    else:
        # 没有啤酒数据的情况
        if valid_beer is None:
            ax6.text(0.5, 0.5, "未记录啤酒消费数据", ha="center", va="center", transform=ax6.transAxes, fontsize=12)
        elif valid_beer.empty:
            ax6.text(0.5, 0.5, "暂无啤酒消费记录", ha="center", va="center", transform=ax6.transAxes, fontsize=12)
        else:
            ax6.text(
                0.5, 0.5, "啤酒数据不足，无法生成统计", ha="center", va="center", transform=ax6.transAxes, fontsize=12
            )
        ax6.set_title("啤酒消费统计", fontsize=14, fontweight="bold")

    # 添加总体统计信息
    stats_text = ""
    if not valid_steps.empty:
        stats_text += f"步数统计（目标: {target}步）:\n"
        stats_text += f"• 平均: {valid_steps.mean():.0f}步/天\n"
        stats_text += f"• 总计: {valid_steps.sum():,}步\n"
        stats_text += f"• 达标率: {(valid_steps >= target).sum() / len(valid_steps) * 100:.1f}%\n"

    if not valid_sleep.empty:
        stats_text += f"\n睡眠统计（目标: 7小时）:\n"
        stats_text += f"• 平均: {valid_sleep.mean() / 60:.1f}小时/天\n"
        stats_text += f"• 总计: {valid_sleep.sum() / 60:.1f}小时\n"
        stats_text += f"• 达标率: {(valid_sleep >= 420).sum() / len(valid_sleep) * 100:.1f}%\n"

    if valid_beer is not None and not valid_beer.empty:
        stats_text += f"\n啤酒统计（目标: {beer_target}瓶）:\n"
        stats_text += f"• 平均: {valid_beer.mean():.1f}瓶/天\n"
        stats_text += f"• 总计: {valid_beer.sum():.0f}瓶\n"
        stats_text += f"• 超标率: {(valid_beer > beer_target).sum() / len(valid_beer) * 100:.1f}%\n"
        stats_text += f"• 饮酒天数: {(valid_beer > 0).sum()}天\n"

    stats_text += f"\n数据范围:\n"
    stats_text += f"{hdf.index.min().strftime('%Y-%m-%d')} 至 {hdf.index.max().strftime('%Y-%m-%d')}"

    # 添加月度估算说明
    if (
        not monthly_steps_estimated.empty
        or not monthly_sleep_estimated.empty
        or (valid_beer is not None and not monthly_beer_estimated.empty)
    ):
        stats_text += f"\n\n月度估算说明:\n"
        stats_text += f"• 实心柱体：实际月度合计\n"
        stats_text += f"• 红色虚线：估算整月值（数据不完整月份）\n"
        stats_text += f"• 估算值用于数据不完整月份的趋势参考"

    plt.figtext(
        0.02, 0.02, stats_text, fontsize=9, bbox=dict(boxstyle="round,pad=0.5", facecolor="lightyellow", alpha=0.8)
    )

    # 调整布局（为第一个图的图例留空间）
    plt.tight_layout(rect=[0, 0.12, 1, 0.95])

    # 转换为base64
    buffer = io.BytesIO()
    plt.savefig(buffer, format="png", dpi=120, bbox_inches="tight")
    buffer.seek(0)
    image_base64 = base64.b64encode(buffer.read()).decode()
    plt.close()

    log.info(f"图表生成成功，大小: {len(image_base64)} 字节")
    return image_base64


# %% [markdown]
# ### generate_health_report(hdf)

# %%
def generate_health_report(hdf):
    """生成健康数据的综合分析Markdown报告"""
    if hdf.empty:
        return "## 健康数据分析报告\n\n暂无有效数据。"

    # 确保索引是DatetimeIndex
    if not isinstance(hdf.index, pd.DatetimeIndex):
        try:
            hdf.index = pd.to_datetime(hdf.index)
        except:
            return "## 错误报告\n\n日期格式错误，无法生成分析报告。"

    # 从云端配置获取每日步数目标
    if not (target := getinivaluefromcloud("health", "step_day_target")):
        target = 8000
    else:
        target = int(target)

    # 从云端配置获取每日啤酒目标
    if not (beer_target := getinivaluefromcloud("health", "beer_day_target")):
        beer_target = 2
    else:
        beer_target = int(beer_target)

    valid_steps = hdf["步数"].dropna()
    valid_sleep = hdf["睡眠时长"].dropna()

    # 检查是否有啤酒数据
    has_beer_data = "啤酒瓶数" in hdf.columns
    if has_beer_data:
        valid_beer = hdf["啤酒瓶数"].dropna()
    else:
        valid_beer = pd.Series()

    report = "## 健康数据分析报告\n\n"

    # 在报告头部添加连续数据分析
    report += "\n### 0. 近期连续记录分析\n"

    recent_analysis = analyze_recent_continuous_data(hdf)

    if recent_analysis:
        report += f"- **连续记录区间**: {recent_analysis['日期范围']} ({recent_analysis['连续天数']}天)\n"

        if recent_analysis["步数统计"]["平均"]:
            report += f"- **连续期间平均步数**: {recent_analysis['步数统计']['平均']:.0f}步/天\n"
            report += f"- **连续期间总计步数**: {recent_analysis['步数统计']['总计']:,}步\n"

            if recent_analysis["步数统计"]["达标率"]:
                report += f"- **连续期间达标率**: {recent_analysis['步数统计']['达标率']:.1f}%\n"

        if recent_analysis.get("周对比"):
            report += f"- **周对比趋势**: "
            if recent_analysis["周对比"]["变化率"]:
                trend = "上升" if recent_analysis["周对比"]["变化率"] > 0 else "下降"
                report += f"{trend} {abs(recent_analysis['周对比']['变化率']):.1f}%\n"

        # 添加啤酒连续分析（如果存在）
        if has_beer_data and "啤酒瓶数" in recent_analysis:
            if recent_analysis["啤酒瓶数"]["平均"]:
                report += f"- **连续期间平均啤酒**: {recent_analysis['啤酒瓶数']['平均']:.1f}瓶/天\n"
                report += f"- **连续期间总计啤酒**: {recent_analysis['啤酒瓶数']['总计']:.0f}瓶\n"

        # 添加建议
        report += "\n**连续记录洞察**:\n"
        if recent_analysis["连续天数"] >= 30:
            report += "✅ 连续记录超过30天，习惯非常稳定！\n"
        elif recent_analysis["连续天数"] >= 14:
            report += "👍 连续记录超过2周，习惯正在养成中\n"
        else:
            report += "📝 连续记录较短，建议保持每日记录习惯\n"
    else:
        report += "- 暂无连续的近期记录数据\n"

    # 1. 近期趋势
    report += "### 1. 近期趋势\n"

    if not valid_steps.empty and len(valid_steps) >= 7:
        last_week = valid_steps.tail(7)
        report += f"- **最近7天平均步数**: {last_week.mean():.0f} 步\n"

        if len(valid_steps) >= 14:
            prev_week = valid_steps.iloc[-14:-7]
            if prev_week.mean() > 0:
                change = (last_week.mean() - prev_week.mean()) / prev_week.mean() * 100
                trend = "上升" if change > 0 else "下降"
                report += f"- **与前7天对比**: {trend} {abs(change):.1f}%\n"
            else:
                report += f"- **与前7天对比**: 数据不足\n"
        else:
            report += f"- **与前7天对比**: 数据不足\n"
    else:
        report += "- 数据不足，无法计算步数趋势\n"

    if not valid_sleep.empty and len(valid_sleep) >= 7:
        last_week_sleep = valid_sleep.tail(7) / 60  # 转换为小时
        report += f"- **最近7天平均睡眠**: {last_week_sleep.mean():.1f} 小时\n"
    else:
        report += "- 数据不足，无法计算睡眠趋势\n"

    if has_beer_data and not valid_beer.empty and len(valid_beer) >= 7:
        last_week_beer = valid_beer.tail(7)
        report += f"- **最近7天平均啤酒**: {last_week_beer.mean():.1f} 瓶\n"

        if len(valid_beer) >= 14:
            prev_week_beer = valid_beer.iloc[-14:-7]
            if prev_week_beer.mean() > 0:
                change = (last_week_beer.mean() - prev_week_beer.mean()) / prev_week_beer.mean() * 100
                trend = "上升" if change > 0 else "下降"
                report += f"- **与前7天对比**: {trend} {abs(change):.1f}%\n"
    elif has_beer_data:
        report += "- 数据不足，无法计算啤酒趋势\n"

    report += "\n"

    # 2. 健康建议
    report += "### 2. 健康建议\n"

    if not valid_steps.empty:
        avg_steps = valid_steps.mean()
        if avg_steps < 5000:
            report += "- **急需增加运动量**: 当前平均步数低于5000步，建议每天增加30分钟步行\n"
        elif avg_steps < target:
            report += f"- **适度增加运动**: 当前平均步数接近但未达到{target}步目标，建议每天增加15分钟步行\n"
        else:
            report += f"- **运动量良好**: 继续保持每日{target}步以上的运动习惯\n"

    if not valid_sleep.empty:
        avg_sleep = valid_sleep.mean() / 60
        if avg_sleep < 6:
            report += "- **急需改善睡眠**: 平均睡眠不足6小时，建议调整作息，保证睡眠质量\n"
        elif avg_sleep < 7:
            report += "- **适度增加睡眠**: 平均睡眠接近但未达到7小时，建议每天早睡30分钟\n"
        else:
            report += "- **睡眠充足**: 继续保持良好的睡眠习惯\n"

    if has_beer_data and not valid_beer.empty:
        avg_beer = valid_beer.mean()
        if avg_beer > 3:
            report += "- **饮酒过量**: 平均每日超过3瓶，建议减少饮酒频率\n"
        elif avg_beer > beer_target:
            report += f"- **适度控制**: 平均每日{avg_beer:.1f}瓶，略高于目标{beer_target}瓶\n"
        elif avg_beer > 0:
            report += f"- **饮酒适度**: 平均每日{avg_beer:.1f}瓶，在合理范围内\n"
        else:
            report += "- **无饮酒记录**: 保持健康生活习惯\n"

    # 3. 基本统计
    report += "\n### 3. 基本统计\n"
    report += f"- **数据日期范围**: {hdf.index.min().strftime('%Y-%m-%d')} 至 {hdf.index.max().strftime('%Y-%m-%d')}\n"
    report += f"- **总天数**: {len(hdf)} 天\n"

    if not valid_steps.empty:
        report += f"- **有效步数记录**: {valid_steps.count()} 天 ({valid_steps.count() / len(hdf) * 100:.1f}%)\n"
    else:
        report += f"- **有效步数记录**: 0 天 (0.0%)\n"

    if not valid_sleep.empty:
        report += f"- **有效睡眠记录**: {valid_sleep.count()} 天 ({valid_sleep.count() / len(hdf) * 100:.1f}%)\n"
    else:
        report += f"- **有效睡眠记录**: 0 天 (0.0%)\n"

    if has_beer_data and not valid_beer.empty:
        report += f"- **有效啤酒记录**: {valid_beer.count()} 天 ({valid_beer.count() / len(hdf) * 100:.1f}%)\n"
    elif has_beer_data:
        report += f"- **有效啤酒记录**: 0 天 (0.0%)\n"
    else:
        report += f"- **啤酒记录**: 未启用\n"

    report += "\n"

    # 4. 步数分析
    report += "### 4. 步数分析\n"
    if not valid_steps.empty:
        report += f"- **平均每日步数**: {valid_steps.mean():.0f} 步\n"
        report += f"- **最高步数**: {valid_steps.max():.0f} 步 ({valid_steps.idxmax().strftime('%Y-%m-%d')})\n"
        report += f"- **最低步数**: {valid_steps.min():.0f} 步 ({valid_steps.idxmin().strftime('%Y-%m-%d')})\n"

        # 达标分析
        达标天数 = (valid_steps >= target).sum()
        report += f"- **达标天数** (≥{target}步): {达标天数} 天 ({达标天数 / valid_steps.count() * 100:.1f}%)\n"

        # 步数分布
        if len(valid_steps) >= 5:
            quartiles = valid_steps.quantile([0.25, 0.5, 0.75])
            report += (
                f"- **步数分布**: Q1={quartiles[0.25]:.0f}, 中位数={quartiles[0.5]:.0f}, Q3={quartiles[0.75]:.0f}\n"
            )
    else:
        report += "- 暂无有效步数数据\n"

    report += "\n"

    # 5. 睡眠分析
    report += "### 5. 睡眠分析\n"
    if not valid_sleep.empty:
        avg_sleep_hours = valid_sleep.mean() / 60
        report += f"- **平均每日睡眠**: {avg_sleep_hours:.1f} 小时 ({valid_sleep.mean():.0f} 分钟)\n"
        report += f"- **最长睡眠**: {valid_sleep.max() / 60:.1f} 小时 ({valid_sleep.idxmax().strftime('%Y-%m-%d')})\n"
        report += f"- **最短睡眠**: {valid_sleep.min() / 60:.1f} 小时 ({valid_sleep.idxmin().strftime('%Y-%m-%d')})\n"

        # 达标分析（目标为7小时=420分钟）
        target_sleep = 420
        达标睡眠天数 = (valid_sleep >= target_sleep).sum()
        report += f"- **充足睡眠天数** (≥7小时): {达标睡眠天数} 天 ({达标睡眠天数 / valid_sleep.count() * 100:.1f}%)\n"

        # 睡眠分布
        if len(valid_sleep) >= 5:
            quartiles = valid_sleep.quantile([0.25, 0.5, 0.75])
            report += f"- **睡眠分布**: Q1={quartiles[0.25] / 60:.1f}h, 中位数={quartiles[0.5] / 60:.1f}h, Q3={quartiles[0.75] / 60:.1f}h\n"
    else:
        report += "- 暂无有效睡眠数据\n"

    report += "\n"

    # 6. 啤酒消费分析（新增）
    report += "### 6. 啤酒消费分析\n"
    if has_beer_data and not valid_beer.empty:
        report += f"- **平均每日啤酒**: {valid_beer.mean():.1f} 瓶\n"
        report += f"- **最高单日**: {valid_beer.max():.0f} 瓶 ({valid_beer.idxmax().strftime('%Y-%m-%d')})\n"
        report += f"- **总消费瓶数**: {valid_beer.sum():.0f} 瓶\n"

        # 超标分析
        超标天数 = (valid_beer > beer_target).sum()
        report += f"- **超标天数** (>{beer_target}瓶): {超标天数} 天 ({超标天数 / valid_beer.count() * 100:.1f}%)\n"

        # 饮酒频率
        饮酒天数 = (valid_beer > 0).sum()
        report += f"- **饮酒天数**: {饮酒天数} 天 ({饮酒天数 / valid_beer.count() * 100:.1f}%)\n"

        # 月度分析
        if len(valid_beer) >= 30:
            monthly_beer = valid_beer.resample("ME").sum()
            report += f"- **最高月度**: {monthly_beer.max():.0f} 瓶 ({monthly_beer.idxmax().strftime('%Y-%m')})\n"
            report += f"- **最低月度**: {monthly_beer.min():.0f} 瓶 ({monthly_beer.idxmin().strftime('%Y-%m')})\n"
    elif has_beer_data:
        report += "- 暂无啤酒消费记录\n"
    else:
        report += "- 未启用啤酒记录功能\n"

    report += "\n"

    # 数据完整性建议
    completeness = (
        (valid_steps.count() + valid_sleep.count() + (valid_beer.count() if has_beer_data else 0))
        / (3 * len(hdf))
        * 100
    )
    if completeness < 50:
        report += f"- **提高记录频率**: 当前数据完整度仅{completeness:.1f}%，建议每日记录\n"
    elif completeness < 80:
        report += f"- **保持记录习惯**: 当前数据完整度{completeness:.1f}%，继续努力\n"
    else:
        report += f"- **记录习惯良好**: 当前数据完整度{completeness:.1f}%，继续保持\n"

    # 添加备注信息
    if "随记" in hdf.columns:
        valid_notes = hdf["随记"].dropna()
        if not valid_notes.empty:
            interesting_notes = valid_notes[valid_notes.str.len() > 0]
            if len(interesting_notes) > 0:
                report += "\n### 7. 重要备注\n"
                for date, note in interesting_notes.head(5).items():  # 只显示前5条
                    report += f"- **{date.strftime('%Y-%m-%d')}**: {note}\n"

    return report


# %% [markdown]
# ### create_error_image(error_msg)

# %%
def create_error_image(error_msg="生成图表时出错"):
    """创建错误提示图片"""
    plt.figure(figsize=(10, 6))
    plt.text(
        0.5,
        0.5,
        f"⚠️ {error_msg}\n\n请检查数据格式或连续性",
        horizontalalignment="center",
        verticalalignment="center",
        fontsize=16,
        transform=plt.gca().transAxes,
    )
    plt.axis("off")

    buffer = io.BytesIO()
    plt.savefig(buffer, format="png", dpi=100)
    buffer.seek(0)
    image_base64 = base64.b64encode(buffer.read()).decode()
    plt.close()

    return image_base64

# %% [markdown]
# ### health2note()


# %%
@timethis
def health2note():
    """综合输出健康动态图并更新至笔记"""
    login_user = execcmd("whoami")
    namestr = "happyjp_life"
    section = f"health_{login_user}"
    notestat_title = f"健康动态日日升【{gethostuser()}】"

    # 1. 获取或查找健康笔记ID
    if not (health_id := getcfpoptionvalue(namestr, section, "health_cloud_id")):
        findhealthnotes = searchnotes("健康运动笔记")
        if len(findhealthnotes) == 0:
            log.critical("未找到标题为《健康运动笔记》的笔记")
            return
        healthnote = findhealthnotes
        health_id = healthnote.id
        setcfpoptionvalue(namestr, section, "health_cloud_id", f"{health_id}")

    # 2. 检查笔记是否有更新
    health_cloud_update_ts = getcfpoptionvalue(namestr, section, "health_cloud_updatetimestamp") or "0"
    note = getnote(health_id)
    noteupdatetimewithzone = arrow.get(note.updated_time, tzinfo="local")

    if float(health_cloud_update_ts) == noteupdatetimewithzone.timestamp() and not_IPython():
        log.info(f"健康运动笔记无更新（{noteupdatetimewithzone}），跳过")
        return

    # 3. 提取和处理数据
    try:
        hdf = gethealthdatafromnote(note.id)
        if hdf.empty:
            log.warning("提取的数据为空，使用错误图片")
            image_base64 = create_error_image("健康数据为空")
        else:
            # 使用修改后的hdf2imgbase64函数（包含月度估算）
            image_base64 = hdf2imgbase64(hdf)

        # 4. 生成分析报告（使用修改后的generate_health_report函数）
        report_content = generate_health_report(hdf)

    except Exception as e:
        log.error(f"数据处理失败: {str(e)}", exc_info=True)
        image_base64 = create_error_image(f"数据处理错误: {str(e)[:50]}")
        report_content = f"## 错误报告\n\n数据处理过程中出现错误：\n\n```\n{str(e)}\n```"

    # 5. 更新或创建笔记
    nbid = searchnotebook("康健") or searchnotebook("健康")
    if not nbid:
        nbid = createnote(title="健康记录", notebook=True)

    # 查找现有报告笔记
    existing_notes = searchnotes(notestat_title)
    if existing_notes:
        healthstat_cloud_id = existing_notes[0].id
        # 更新笔记内容和图片
        healthstat_cloud_id, res_lst = updatenote_imgdata(
            noteid=healthstat_cloud_id, parent_id=nbid, imgdata64=image_base64
        )
        # 更新笔记正文（分析报告）
        origin_content = getnote(healthstat_cloud_id).body
        new_content = "\n".join([report_content, origin_content])
        updatenote_body(healthstat_cloud_id, new_content)
    else:
        # 创建新笔记
        healthstat_cloud_id = createnote(
            title=notestat_title, parent_id=nbid, imgdata64=image_base64, body=report_content
        )

    # 6. 更新配置
    setcfpoptionvalue(namestr, section, "healthstat_cloud_id", f"{healthstat_cloud_id}")
    setcfpoptionvalue(namestr, section, "health_cloud_updatetimestamp", str(noteupdatetimewithzone.timestamp()))

    log.info(f"健康笔记更新完成，报告笔记ID: {healthstat_cloud_id}")


# %% [markdown]
# ## 主函数main()

# %%
if __name__ == "__main__":
    if not_IPython():
        log.info(f"开始运行文件\t{__file__}")

    health2note()

    if not_IPython():
        log.info(f"Done.结束执行文件\t{__file__}")
