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
# # 位置数据展示与分析系统
#
# ## 功能：从Joplin加载规整位置数据，生成可视化报告

# %%
import base64
import os
import re
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from geopy.distance import great_circle
from sklearn.cluster import DBSCAN

# %%
import pathmagic

with pathmagic.context():
    from func.configpr import getcfpoptionvalue
    from func.first import getdirmain
    from func.jpfuncs import createnote, jpapi, searchnotes, searchnotebook, updatenote_body
    from func.logme import log

# %% [markdown]
# ## 配置参数

# %%
# 报告层级
REPORT_LEVELS = {"monthly": 1, "quarterly": 3, "yearly": 12}

# 可视化参数
PLOT_WIDTH = 10
PLOT_HEIGHT = 8
DPI = 150

# %% [markdown]
# ## 数据加载函数

# %% [markdown]
# ### `load_location_data(scope)`
# 加载指定范围的位置数据


# %%
def load_location_data(scope):
    """
    加载指定范围的位置数据
    """
    # 计算时间范围
    end_date = datetime.now()
    months = REPORT_LEVELS[scope]
    start_date = end_date - timedelta(days=30 * months)

    # 获取月份列表
    date_range = pd.date_range(start_date, end_date, freq="MS")
    monthly_dfs = []

    for date in date_range:
        month_str = date.strftime("%Y%m")

        # 从Joplin获取位置数据笔记
        note_title = f"位置数据_{month_str}"
        notes = searchnotes(f"title:{note_title}")

        if not notes:
            log.warning(f"未找到{month_str}的位置数据笔记")
            continue

        note = notes[0]
        resources = jpapi.get_resources(note.id).items

        # 查找位置数据附件
        location_resource = None
        for res in resources:
            if res.title.endswith(".xlsx"):
                location_resource = res
                break

        if not location_resource:
            log.warning(f"未找到{month_str}的位置数据附件")
            continue

        # 读取Excel数据
        res_data = jpapi.get_resource_file(location_resource.id)
        df = pd.read_excel(BytesIO(res_data))

        # 添加月份标记
        df["month"] = month_str
        monthly_dfs.append(df)

    if not monthly_dfs:
        log.warning(f"未找到{scope}的位置数据")
        return pd.DataFrame()

    return pd.concat(monthly_dfs).reset_index(drop=True)


# %% [markdown]
# ## 数据分析函数

# %% [markdown]
# ### `analyze_location_data(df)`
# 分析位置数据，返回统计结果


# %%

def analyze_location_data(df):
    """
    分析位置数据，返回统计结果
    """
    if df.empty:
        return {}
    
    # 基础统计
    start_time = df["time"].min()
    end_time = df["time"].max()
    total_points = len(df)
    unique_days = df["time"].dt.date.nunique()
    
    # 设备使用统计
    device_stats = df["device_id"].value_counts().to_dict()
    
    # 活动范围计算（核心修复）
    min_lat, max_lat = df["latitude"].min(), df["latitude"].max()
    min_lon, max_lon = df["longitude"].min(), df["longitude"].max()
    distance_km = great_circle(
        (min_lat, min_lon), 
        (max_lat, max_lon)
    ).kilometers
    
    # 计算位置范围（修复scope未定义错误）
    scope = (
        f"纬度: {min_lat:.6f}° - {max_lat:.6f}°, "
        f"经度: {min_lon:.6f}° - {max_lon:.6f}°, "
        f"跨度: {distance_km:.2f}公里"
    )
    
    # 时间跳跃分析
    if "big_gap" in df.columns:
        big_gaps = df[df["big_gap"]]
        gap_stats = {
            "count": len(big_gaps),
            "longest_gap": df["time_diff"].max() if "time_diff" in df.columns else 0
        }
    else:
        gap_stats = {"count": 0, "longest_gap": 0}
    
    # 位置精度分析
    accuracy_stats = {
        "min": df["accuracy"].min(),
        "max": df["accuracy"].max(),
        "mean": df["accuracy"].mean()
    }
    
    # 每日活动模式
    df["hour"] = df["time"].dt.hour
    hourly_distribution = df["hour"].value_counts().sort_index().to_dict()
    
    # 重要地点识别
    important_places = identify_important_places(df)
    
    return {
        "time_range": (start_time, end_time),
        "total_points": total_points,
        "unique_days": unique_days,
        "device_stats": device_stats,
        "distance_km": distance_km,
        "gap_stats": gap_stats,
        "accuracy_stats": accuracy_stats,
        "hourly_distribution": hourly_distribution,
        "important_places": important_places,
        "scope": scope  # 使用已定义的变量
    }


# %% [markdown]
# ### handle_time_jumps(df)

# %%
def handle_time_jumps(df):
    if df.empty:
        return df

    # 确保按时间排序
    df = df.sort_values("time")

    # 计算时间差（分钟）
    df["time_diff"] = df["time"].diff().dt.total_seconds() / 60

    # 标记大时间间隔（>4小时）
    df["big_gap"] = df["time_diff"] > 4 * 60

    # 添加连续段标记
    df["segment"] = df["big_gap"].cumsum()

    return df

# %% [markdown]
# ## 重要地点识别

# %% [markdown]
# ### `identify_important_places(df, radius_km=0.5, min_points=3)`
# 识别重要地点（停留点）


# %%
def identify_important_places(df, radius_km=0.5, min_points=3):
    """
    识别重要地点（停留点）
    """
    if df.empty or "time_diff" not in df.columns:
        return pd.DataFrame()
    required_cols = ["latitude", "longitude", "time_diff"]
    if not all(col in df.columns for col in required_cols):
        return pd.DataFrame()

    # 转换坐标为弧度
    coords = df[["latitude", "longitude"]].values
    kms_per_radian = 6371.0088
    epsilon = radius_km / kms_per_radian

    # DBSCAN聚类
    db = DBSCAN(
        eps=epsilon, min_samples=min_points, algorithm="ball_tree", metric="haversine"
    ).fit(np.radians(coords))
    df["cluster"] = db.labels_

    # 过滤噪声点
    clustered = df[df["cluster"] != -1]

    if clustered.empty:
        return pd.DataFrame()

    # 计算聚类中心
    cluster_centers = (
        clustered.groupby("cluster")
        .agg({"latitude": "mean", "longitude": "mean", "time": "count"})
        .rename(columns={"time": "visit_count"})
        .reset_index()
    )

    # 添加停留时间（近似）
    cluster_centers["avg_stay_min"] = (
        clustered.groupby("cluster")["time_diff"].mean().values
    )
    if "time_diff" in clustered.columns:
        cluster_centers["avg_stay_min"] = (
            clustered.groupby("cluster")["time_diff"].mean().values
        )
    else:
        cluster_centers["avg_stay_min"] = 0  # 默认值
    return cluster_centers.sort_values("visit_count", ascending=False).head(10)


# %% [markdown]
# ## 可视化函数

# %% [markdown]
# ### `generate_visualizations(df, analysis_results)`
# 生成位置数据的可视化图表


# %%
def generate_visualizations(df, analysis_results):
    """
    生成位置数据的可视化图表
    """
    images = {}

    # 1. 轨迹图
    plt.figure(figsize=(PLOT_WIDTH, PLOT_HEIGHT))

    # 按连续段绘制不同颜色
    if "segment" in df.columns:
        for segment in df["segment"].unique():
            seg_df = df[df["segment"] == segment]
            plt.plot(
                seg_df["longitude"],
                seg_df["latitude"],
                alpha=0.7,
                linewidth=1.5,
                label=f"段 {segment}",
            )
    else:
        plt.plot(df["longitude"], df["latitude"], "b-", alpha=0.5, linewidth=1)

    plt.title(f"{analysis_results['scope'].capitalize()}位置轨迹")
    plt.xlabel("经度")
    plt.ylabel("纬度")
    plt.grid(True)
    if "segment" in df.columns:
        plt.legend(loc="best")

    buf = BytesIO()
    plt.savefig(buf, format="png", dpi=DPI)
    plt.close()
    images["trajectory"] = base64.b64encode(buf.getvalue()).decode("utf-8")

    # 2. 时间分布图
    plt.figure(figsize=(PLOT_WIDTH, PLOT_HEIGHT - 2))
    hour_counts = analysis_results["hourly_distribution"]
    plt.bar(list(hour_counts.keys()), list(hour_counts.values()), width=0.8)
    plt.title(f"{analysis_results['scope'].capitalize()}位置记录时间分布")
    plt.xlabel("小时")
    plt.ylabel("记录数量")
    plt.xticks(range(0, 24))
    plt.grid(True)

    buf = BytesIO()
    plt.savefig(buf, format="png", dpi=DPI)
    plt.close()
    images["time_dist"] = base64.b64encode(buf.getvalue()).decode("utf-8")

    # 3. 精度分布图
    plt.figure(figsize=(PLOT_WIDTH, PLOT_HEIGHT - 2))
    plt.hist(df["accuracy"].dropna(), bins=50, alpha=0.7)
    plt.title(f"{analysis_results['scope'].capitalize()}位置精度分布")
    plt.xlabel("精度 (米)")
    plt.ylabel("记录数量")
    plt.grid(True)

    buf = BytesIO()
    plt.savefig(buf, format="png", dpi=DPI)
    plt.close()
    images["accuracy"] = base64.b64encode(buf.getvalue()).decode("utf-8")

    return images


# %% [markdown]
# ## 构建报告内容

# %% [markdown]
# ### `build_report_content(analysis_results, images)`
# 构建Markdown报告内容


# %%
def build_report_content(analysis_results, images):
    """
    构建Markdown报告内容
    """
    scope = analysis_results["scope"]
    start_time, end_time = analysis_results["time_range"]
    device_stats = analysis_results["device_stats"]
    gap_stats = analysis_results["gap_stats"]
    accuracy_stats = analysis_results["accuracy_stats"]
    hourly_distribution = analysis_results["hourly_distribution"]
    important_places = analysis_results["important_places"]

    # 设备使用统计表
    device_table = "| 设备ID | 记录数 | 占比 |\n|--------|--------|------|\n"
    total = analysis_results["total_points"]
    for device, count in device_stats.items():
        percent = (count / total) * 100
        device_table += f"| {device} | {count} | {percent:.1f}% |\n"

    # 时间分布表
    time_table = "| 小时 | 记录数 |\n|------|--------|\n"
    for hour in sorted(hourly_distribution.keys()):
        time_table += f"| {hour} | {hourly_distribution[hour]} |\n"

    # 重要地点表
    places_table = ""
    if not important_places.empty:
        places_table = "| 纬度 | 经度 | 访问次数 | 平均停留(分) |\n|------|------|----------|------------|\n"
        for _, row in important_places.iterrows():
            places_table += f"| {row['latitude']:.5f} | {row['longitude']:.5f} | {row['visit_count']} | {row['avg_stay_min']:.1f} |\n"

    # 构建报告
    report = f"""
# 📍 {scope.capitalize()}位置分析报告 
## 时间范围: {start_time.strftime("%Y-%m-%d")} 至 {end_time.strftime("%Y-%m-%d")}

### 概览统计
- **总记录数**: {analysis_results["total_points"]}
- **覆盖天数**: {analysis_results["unique_days"]}
- **活动范围**: {analysis_results["distance_km"]:.2f}公里
- **时间跳跃次数**: {gap_stats["count"]} (最长{gap_stats["longest_gap"]:.1f}小时)

### 设备使用情况
{device_table}

### 位置精度
- **最小精度**: {accuracy_stats["min"]:.1f}米
- **最大精度**: {accuracy_stats["max"]:.1f}米
- **平均精度**: {accuracy_stats["mean"]:.1f}米

### 时间分布
{time_table}

### 重要地点
{places_table}

### 可视化分析
#### 位置轨迹
![轨迹图](data:image/png;base64,{images["trajectory"]})

#### 时间分布
![时间分布](data:image/png;base64,{images["time_dist"]})

#### 精度分布
![精度分布](data:image/png;base64,{images["accuracy"]})
"""
    return report


# %% [markdown]
# ## 更新Joplin笔记

# %% [markdown]
# ### `update_joplin_report(report_content, scope)`
# 更新Joplin位置分析报告


# %%
def update_joplin_report(report_content, scope):
    """
    更新Joplin位置分析报告
    """
    note_title = f"位置分析报告_{scope}"
    existing_notes = searchnotes(f"title:{note_title}")

    if existing_notes:
        note_id = existing_notes[0].id
        updatenote_body(note_id, report_content)
        log.info(f"更新位置分析报告: {note_title}")
    else:
        parent_id = searchnotebook("ewmobile")
        if not parent_id:
            parent_id = createnote(title="ewmobile", notebook=True)
        note_id = createnote(title=note_title, parent_id=parent_id, body=report_content)
        log.info(f"创建位置分析报告: {note_title}")


# %% [markdown]
# ## 主函数

# %% [markdown]
# ### `generate_location_reports()`
# 生成三个层级的报告：月报、季报、年报


# %%
def generate_location_reports():
    """
    生成三个层级的报告：月报、季报、年报
    """
    for scope in REPORT_LEVELS.keys():
        log.info(f"开始生成 {scope} 位置报告...")

        # 1. 加载数据
        df = load_location_data(scope)
        if df.empty:
            log.warning(f"跳过 {scope} 报告，无数据")
            continue

        # 2. 分析数据
        analysis_results = analyze_location_data(df)

        # 3. 生成可视化
        images = generate_visualizations(df, analysis_results)

        # 4. 构建报告
        report_content = build_report_content(analysis_results, images)

        # 5. 更新Joplin笔记
        update_joplin_report(report_content, scope)

        log.info(f"{scope.capitalize()}位置报告生成完成")


# %% [markdown]
# ## 主入口

# %% [markdown]
# ### `main()`
# 脚本主入口

# %%
if __name__ == "__main__":
    log.info("开始生成位置分析报告...")
    generate_location_reports()
    log.info("位置分析报告生成完成")
