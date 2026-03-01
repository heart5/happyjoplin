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

    # 更健壮的正则，匹配三级标题日期、步数、睡眠时长、可选备注
    # 允许中英文逗号和冒号，睡眠时长格式为"小时:分钟"或"小时：分钟"
    ptn = re.compile(
        r"###\s*(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日\s*\n"
        r"(\d+)\s*[,，]\s*(\d{1,2})\s*[:：]\s*(\d{1,2})\s*\n"
        r"([^#]*)"  # 备注部分（非#开头的内容）
    )

    items = []
    for match in ptn.finditer(content):
        year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
        steps = int(match.group(4))
        sleep_hour = int(match.group(5))
        sleep_minute = int(match.group(6))
        memo = match.group(7).strip()

        date_obj = datetime(year, month, day).date()
        sleep_total_minutes = sleep_hour * 60 + sleep_minute

        items.append({"日期": date_obj, "步数": steps, "睡眠时长": sleep_total_minutes, "随记": memo})

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

    return df


# %% [markdown]
# ### calds2ds(sds)


# %%
def calds2ds(sds):
    """根据输入的ds，按月合计并估算数据未满月的月份的整月值
    返回：月度合计ds、头尾估算合计ds
    """
    sdsm_actual = sds.resample("m").sum()

    dmin = sds.index.min()
    year = dmin.year
    month = dmin.month
    __, monthend = calendar.monthrange(year, month)
    estimatemin = int(sdsm_actual.iloc[0] / (monthend + 1 - dmin.day) * monthend)
    print(year, month, monthend, dmin.day, sdsm_actual.iloc[0], estimatemin)

    dmax = sds.index.max()
    year = dmax.year
    month = dmax.month
    __, monthend = calendar.monthrange(year, month)
    estimatemax = int(sdsm_actual.iloc[-1] / (dmax.day) * monthend)
    print(year, month, monthend, dmax.day, sdsm_actual.iloc[-1], estimatemax)

    estds = pd.Series([estimatemin, estimatemax], index=[dmin, dmax])

    estds_resample_full = estds.resample("m").sum()
    return sdsm_actual, estds_resample_full


# %% [markdown]
# ### hdf2imgbase64(hdf)


# %%
def hdf2imgbase64(hdf):
    """根据传入包含运动数据的DataFrame作图，处理缺失值，输出图形的bytes"""
    if hdf.empty or hdf["步数"].count() == 0:
        log.error("无有效数据可绘制图表")
        # 返回一个提示图片
        return create_error_image("无有效健康数据")

    # 确保索引是DatetimeIndex
    if not isinstance(hdf.index, pd.DatetimeIndex):
        try:
            hdf.index = pd.to_datetime(hdf.index)
        except:
            log.error("无法将索引转换为日期时间格式")
            return create_error_image("日期格式错误")

    # 确保数据按日期排序
    hdf = hdf.sort_index()

    # 创建图表
    fig = plt.figure(figsize=(15, 30), dpi=100)

    # 1. 步数动态图
    ax1 = plt.subplot2grid((4, 2), (0, 0), colspan=2, rowspan=1)

    # 提取有效步数数据
    valid_steps = hdf["步数"].dropna()

    if not valid_steps.empty:
        # 绘制步数折线图
        ax1.plot(valid_steps.index, valid_steps.values, "b-", lw=1.5, label="每日步数", alpha=0.7)

        # 绘制步数散点图
        ax1.scatter(valid_steps.index, valid_steps.values, s=30, c="blue", alpha=0.5)

        # 添加7天移动平均线
        if len(valid_steps) >= 7:
            moving_avg = valid_steps.rolling(window=7, min_periods=1).mean()
            ax1.plot(moving_avg.index, moving_avg.values, "r-", lw=2, label="7天移动平均")

        # 添加目标线（7000步）
        ax1.axhline(y=7000, color="orange", linestyle="--", alpha=0.5, label="目标线(7000步)")

        # 标注最高和最低步数
        if len(valid_steps) > 1:
            max_step_idx = valid_steps.idxmax()
            min_step_idx = valid_steps.idxmin()
            ax1.annotate(
                f"最高: {valid_steps.max()}",
                xy=(max_step_idx, valid_steps.max()),
                xytext=(max_step_idx, valid_steps.max() + 500),
                arrowprops=dict(arrowstyle="->", color="red"),
                fontsize=9,
                color="red",
            )

            ax1.annotate(
                f"最低: {valid_steps.min()}",
                xy=(min_step_idx, valid_steps.min()),
                xytext=(min_step_idx, valid_steps.min() - 500),
                arrowprops=dict(arrowstyle="->", color="green"),
                fontsize=9,
                color="green",
            )

    ax1.set_title("步数动态图", fontsize=14, fontweight="bold")
    ax1.set_xlabel("日期")
    ax1.set_ylabel("步数")
    ax1.legend(loc="upper left")
    ax1.grid(True, alpha=0.3)
    ax1.tick_params(axis="x", rotation=45)

    # 2. 月度步数统计图
    ax2 = plt.subplot2grid((4, 2), (1, 0), colspan=2, rowspan=1)

    if not valid_steps.empty:
        # 按月份汇总
        monthly_steps = valid_steps.resample("ME").sum()

        if not monthly_steps.empty:
            # 创建柱状图
            bars = ax2.bar(
                range(len(monthly_steps)), monthly_steps.values, color="skyblue", alpha=0.7, edgecolor="black"
            )

            # 添加数值标签
            for i, (date, value) in enumerate(monthly_steps.items()):
                ax2.text(i, value + (value * 0.01), f"{int(value):,}", ha="center", va="bottom", fontsize=9)

            # 设置x轴标签
            ax2.set_xticks(range(len(monthly_steps)))
            ax2.set_xticklabels([date.strftime("%Y-%m") for date in monthly_steps.index], rotation=45, fontsize=10)

            # 添加趋势线
            if len(monthly_steps) > 1:
                x_positions = range(len(monthly_steps))
                ax2.plot(
                    x_positions,
                    monthly_steps.values,
                    "r-",
                    marker="o",
                    markersize=6,
                    linewidth=2,
                    alpha=0.7,
                    label="月度趋势",
                )

    ax2.set_title("月度步数统计", fontsize=14, fontweight="bold")
    ax2.set_xlabel("月份")
    ax2.set_ylabel("总步数")
    ax2.legend(loc="upper left")
    ax2.grid(True, alpha=0.3, axis="y")

    # 3. 睡眠时长动态图
    ax3 = plt.subplot2grid((4, 2), (2, 0), colspan=2, rowspan=1)

    valid_sleep = hdf["睡眠时长"].dropna()

    if not valid_sleep.empty:
        # 转换为小时
        sleep_hours = valid_sleep / 60

        # 绘制睡眠时长
        ax3.plot(sleep_hours.index, sleep_hours.values, "g-", lw=1.5, label="每日睡眠时长", alpha=0.7)
        ax3.scatter(sleep_hours.index, sleep_hours.values, s=30, c="green", alpha=0.5)

        # 添加7天移动平均
        if len(sleep_hours) >= 7:
            sleep_avg = sleep_hours.rolling(window=7, min_periods=1).mean()
            ax3.plot(sleep_avg.index, sleep_avg.values, "purple", lw=2, label="7天移动平均")

        # 添加目标线（7小时）
        ax3.axhline(y=7, color="orange", linestyle="--", alpha=0.5, label="目标线(7小时)")

        # 标注最高和最低睡眠时长
        if len(sleep_hours) > 1:
            max_sleep_idx = sleep_hours.idxmax()
            min_sleep_idx = sleep_hours.idxmin()
            ax3.annotate(
                f"{sleep_hours.max():.1f}h",
                xy=(max_sleep_idx, sleep_hours.max()),
                xytext=(max_sleep_idx, sleep_hours.max() + 0.5),
                arrowprops=dict(arrowstyle="->", color="red"),
                fontsize=9,
                color="red",
            )

            ax3.annotate(
                f"{sleep_hours.min():.1f}h",
                xy=(min_sleep_idx, sleep_hours.min()),
                xytext=(min_sleep_idx, sleep_hours.min() - 0.5),
                arrowprops=dict(arrowstyle="->", color="green"),
                fontsize=9,
                color="green",
            )

    ax3.set_title("睡眠时长动态图（小时）", fontsize=14, fontweight="bold")
    ax3.set_xlabel("日期")
    ax3.set_ylabel("睡眠时长（小时）")
    ax3.legend(loc="upper left")
    ax3.grid(True, alpha=0.3)
    ax3.tick_params(axis="x", rotation=45)

    # 4. 月度睡眠统计
    ax4 = plt.subplot2grid((4, 2), (3, 0), colspan=2, rowspan=1)

    if not valid_sleep.empty:
        # 按月份汇总（转换为小时）
        monthly_sleep = valid_sleep.resample("ME").sum() / 60  # 转换为小时

        if not monthly_sleep.empty:
            # 创建柱状图
            bars = ax4.bar(
                range(len(monthly_sleep)), monthly_sleep.values, color="lightgreen", alpha=0.7, edgecolor="black"
            )

            # 添加数值标签
            for i, (date, value) in enumerate(monthly_sleep.items()):
                ax4.text(i, value + (value * 0.01), f"{value:.1f}h", ha="center", va="bottom", fontsize=9)

            # 设置x轴标签
            ax4.set_xticks(range(len(monthly_sleep)))
            ax4.set_xticklabels([date.strftime("%Y-%m") for date in monthly_sleep.index], rotation=45, fontsize=10)

            # 添加趋势线
            if len(monthly_sleep) > 1:
                x_positions = range(len(monthly_sleep))
                ax4.plot(
                    x_positions,
                    monthly_sleep.values,
                    "b-",
                    marker="s",
                    markersize=6,
                    linewidth=2,
                    alpha=0.7,
                    label="月度趋势",
                )

    ax4.set_title("月度睡眠时长统计（小时）", fontsize=14, fontweight="bold")
    ax4.set_xlabel("月份")
    ax4.set_ylabel("总睡眠时长（小时）")
    ax4.legend(loc="upper left")
    ax4.grid(True, alpha=0.3, axis="y")

    # 添加总体统计信息
    stats_text = ""
    if not valid_steps.empty:
        stats_text += f"步数统计:\n"
        stats_text += f"• 平均: {valid_steps.mean():.0f}步/天\n"
        stats_text += f"• 总计: {valid_steps.sum():,}步\n"
        stats_text += f"• 达标率: {(valid_steps >= 7000).sum() / len(valid_steps) * 100:.1f}%\n"

    if not valid_sleep.empty:
        stats_text += f"\n睡眠统计:\n"
        stats_text += f"• 平均: {valid_sleep.mean() / 60:.1f}小时/天\n"
        stats_text += f"• 总计: {valid_sleep.sum() / 60:.1f}小时\n"
        stats_text += f"• 达标率: {(valid_sleep >= 420).sum() / len(valid_sleep) * 100:.1f}%\n"

    stats_text += f"\n数据范围:\n"
    stats_text += f"{hdf.index.min().strftime('%Y-%m-%d')} 至 {hdf.index.max().strftime('%Y-%m-%d')}"

    plt.figtext(
        0.02, 0.02, stats_text, fontsize=10, bbox=dict(boxstyle="round,pad=0.5", facecolor="lightyellow", alpha=0.8)
    )

    plt.tight_layout(rect=[0, 0.05, 1, 0.98])

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

    # 从云端配置获取每日步数目标，获取不到则默认设置为8000步
    if not (target := getinivaluefromcloud("health", "step_day_target")):
        target = 8000
    valid_steps = hdf["步数"].dropna()
    valid_sleep = hdf["睡眠时长"].dropna()

    report = "## 📊 健康数据分析报告\n\n"

    # 1. 基本统计
    report += "### 1. 基本统计\n"
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

    report += "\n"

    # 2. 步数分析
    report += "### 2. 步数分析\n"
    if not valid_steps.empty:
        report += f"- **平均每日步数**: {valid_steps.mean():.0f} 步\n"
        report += f"- **最高步数**: {valid_steps.max():.0f} 步 ({valid_steps.idxmax().strftime('%Y-%m-%d')})\n"
        report += f"- **最低步数**: {valid_steps.min():.0f} 步 ({valid_steps.idxmin().strftime('%Y-%m-%d')})\n"

        # 达标分析（目标为从云端配置获取的步数）
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

    # 3. 睡眠分析
    report += "### 3. 睡眠分析\n"
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

    # 4. 近期趋势
    report += "### 4. 近期趋势\n"

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

    report += "\n"

    # 5. 健康建议
    report += "### 5. 健康建议\n"

    if not valid_steps.empty:
        avg_steps = valid_steps.mean()
        if avg_steps < 5000:
            report += "- 🚶 **急需增加运动量**: 当前平均步数低于5000步，建议每天增加30分钟步行\n"
        elif avg_steps < target:
            report += f"- 🚶 **适度增加运动**: 当前平均步数接近但未达到{target}步目标，建议每天增加15分钟步行\n"
        else:
            report += f"- ✅ **运动量良好**: 继续保持每日{target}步以上的运动习惯\n"

    if not valid_sleep.empty:
        avg_sleep = valid_sleep.mean() / 60
        if avg_sleep < 6:
            report += "- 😴 **急需改善睡眠**: 平均睡眠不足6小时，建议调整作息，保证睡眠质量\n"
        elif avg_sleep < 7:
            report += "- 😴 **适度增加睡眠**: 平均睡眠接近但未达到7小时，建议每天早睡30分钟\n"
        else:
            report += "- ✅ **睡眠充足**: 继续保持良好的睡眠习惯\n"

    # 数据完整性建议
    completeness = (valid_steps.count() + valid_sleep.count()) / (2 * len(hdf)) * 100
    if completeness < 50:
        report += f"- 📝 **提高记录频率**: 当前数据完整度仅{completeness:.1f}%，建议每日记录\n"
    elif completeness < 80:
        report += f"- 📝 **保持记录习惯**: 当前数据完整度{completeness:.1f}%，继续努力\n"
    else:
        report += f"- ✅ **记录习惯良好**: 当前数据完整度{completeness:.1f}%，继续保持\n"

    # 添加备注信息
    if "随记" in hdf.columns:
        valid_notes = hdf["随记"].dropna()
        if not valid_notes.empty:
            interesting_notes = valid_notes[valid_notes.str.len() > 0]
            if len(interesting_notes) > 0:
                report += "\n### 6. 重要备注\n"
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
            image_base64 = hdf2imgbase64(hdf)

        # 4. 生成分析报告
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
