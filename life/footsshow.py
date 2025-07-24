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
import seaborn as sns

# %%
import pathmagic
from geopy.distance import great_circle
from sklearn.cluster import DBSCAN

with pathmagic.context():
    from func.configpr import getcfpoptionvalue
    from func.first import getdirmain
    from func.jpfuncs import (
        add_resource_from_bytes,
        createnote,
        getinivaluefromcloud,
        jpapi,
        searchnotes,
        updatenote_body,
    )
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
    end_date = datetime.now()
    months = REPORT_LEVELS[scope]
    start_date = end_date - timedelta(days=30 * months)

    date_range = pd.date_range(start_date, end_date, freq="MS")
    monthly_dfs = []

    for date in date_range:
        month_str = date.strftime("%Y%m")
        note_title = f"位置数据_{month_str}"
        notes = searchnotes(f"title:{note_title}")

        if not notes:
            log.warning(f"未找到{month_str}的位置数据笔记")
            continue

        note = notes[0]
        resources = jpapi.get_resources(note.id).items

        location_resource = None
        for res in resources:
            if res.title.endswith(".xlsx"):
                location_resource = res
                break

        if not location_resource:
            log.warning(f"未找到{month_str}的位置数据附件")
            continue

        res_data = jpapi.get_resource_file(location_resource.id)
        df = pd.read_excel(BytesIO(res_data))
        df["month"] = month_str
        monthly_dfs.append(df)

    if not monthly_dfs:
        log.warning(f"未找到{scope}的位置数据")
        return pd.DataFrame()

    return pd.concat(monthly_dfs).reset_index(drop=True)


# %% [markdown]
# ## 数据分析函数

# %% [markdown]
# ### `analyze_location_data(df, scope)`
# 分析位置数据，返回统计结果


# %%
def analyze_location_data(indf, scope):
    """
    分析位置数据，返回统计结果
    修复列名问题并添加数据预处理
    """
    # 1. 数据预处理
    df = indf.copy()
    df = fuse_device_data(df)
    df = handle_time_jumps(df)
    # 确保时间戳和时间差列存在
    df["timestamp"] = df["time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df["time_diff"] = df["time"].diff().dt.total_seconds().fillna(0) / 60

    # 2. 记录调试信息
    log.debug(f"分析启动时数据列为: {df.columns.tolist()}")

    # 3. 时间范围分析
    start_time = df["time"].min().strftime("%Y-%m-%d")
    end_time = df["time"].max().strftime("%Y-%m-%d")

    # 4. 基本统计
    total_points = len(df)
    unique_days = df["time"].dt.date.nunique()

    # 5. 设备分析
    device_stats = df["device_id"].value_counts().to_dict()

    # 6. 距离
    min_lat, max_lat = df["latitude"].min(), df["latitude"].max()
    min_lon, max_lon = df["longitude"].min(), df["longitude"].max()
    distance_km = great_circle((min_lat, min_lon), (max_lat, max_lon)).kilometers
    # 7. 大跨越
    if "big_gap" in df.columns:
        big_gaps = df[df["big_gap"]]
        gap_stats = {
            "count": len(big_gaps),
            "longest_gap": df["time_diff"].max() if "time_diff" in df.columns else 0,
        }
    else:
        gap_stats = {"count": 0, "longest_gap": 0}

    # 8. 小时分布
    df["hour"] = df["time"].dt.hour
    hourly_distribution = df["hour"].value_counts().sort_index().to_dict()

    # 9. 精度分析
    accuracy_stats = {
        "min": df["accuracy"].min(),
        "max": df["accuracy"].max(),
        "mean": df["accuracy"].mean(),
    }

    # 10. 重要地点分析
    clustered = identify_important_places(df)
    important_places = (
        clustered.groupby("cluster")
        .agg(
            {
                "latitude": "mean",
                "longitude": "mean",
            }
        )
        .assign(visit_count=1)
        .assign(avg_stay_min=0)
        .sort_values("visit_count", ascending=False)
        .head(5)
    )

    log.debug(f"分析结束时数据列为: {df.columns.tolist()}")
    return {
        "scope": scope,
        "time_range": (start_time, end_time),
        "total_points": total_points,
        "unique_days": unique_days,
        "device_stats": device_stats,
        "distance_km": distance_km,
        "gap_stats": gap_stats,
        "accuracy_stats": accuracy_stats,
        "hourly_distribution": hourly_distribution,
        "important_places": important_places.to_dict("records"),
    }


# %% [markdown]
# ### `handle_time_jumps(df)`


# %%
def handle_time_jumps(df):
    if df.empty:
        return df

    df = df.sort_values("time")
    df["time_diff"] = df["time"].diff().dt.total_seconds() / 60
    df["big_gap"] = df["time_diff"] > 4 * 60
    df["segment"] = df["big_gap"].cumsum()

    return df


# %% [markdown]
# ### `calc_device_activity(df, device_id)`


# %%
def calc_device_activity(df, device_id):
    """计算设备活跃度评分（0-100）"""
    device_data = df[df["device_id"] == device_id]
    if len(device_data) < 2:
        return 0

    total_dist = 0
    prev = None
    for _, row in device_data.iterrows():
        if prev is not None:
            dist = great_circle(
                (prev.latitude, prev.longitude), (row.latitude, row.longitude)
            ).m
            total_dist += dist
        prev = row

    time_span = (
        device_data["time"].max() - device_data["time"].min()
    ).total_seconds() / 3600
    lat_var = device_data["latitude"].var()
    lon_var = device_data["longitude"].var()

    activity_score = min(
        100,
        (total_dist / max(1, time_span)) * 0.7 + (lat_var + lon_var) * 10000 * 0.3,
    )
    return activity_score


# %% [markdown]
# ### `check_spatiotemporal_consistency(point1, point2)`


# %%
def check_spatiotemporal_consistency(point1, point2):
    """检查两点时空一致性"""
    time_diff = abs((point1["time"] - point2["time"]).total_seconds())
    dist = great_circle(
        (point1.latitude, point1.longitude), (point2.latitude, point2.longitude)
    ).m
    max_allowed_dist = min(100, time_diff * 0.5)  # 0.5m/s移动速度
    return dist < max_allowed_dist and time_diff < 300


# %% [markdown]
# ### `fuse_device_data(df, window_size="2h")`


# %%
def fuse_device_data(df, window_size="2h"):
    """多设备数据智能融合"""
    print(
        f"开始多设备数据智能融合……\n传入汇总数据大小为：{df.shape[0]}，传入的列名称列表为：{list(df.columns)}"
    )
    device_activity = {
        device_id: calc_device_activity(df, device_id)
        for device_id in df["device_id"].unique()
    }
    df["time_window"] = df["time"].dt.floor(window_size)
    fused_points = []

    for window, group in df.groupby("time_window"):
        active_devices = [
            did
            for did, score in device_activity.items()
            if score > 50 and did in group["device_id"].values
        ]
        if active_devices:
            active_group = group[group["device_id"].isin(active_devices)]
            candidate = active_group.loc[active_group["accuracy"].idxmin()]
        else:
            candidate = group.loc[group["accuracy"].idxmin()]

        if fused_points:
            last_point = fused_points[-1]
            if not check_spatiotemporal_consistency(last_point, candidate):
                group["dist_to_last"] = group.apply(
                    lambda row: great_circle(
                        (last_point.latitude, last_point.longitude),
                        (row.latitude, row.longitude),
                    ).m,
                    axis=1,
                )
                candidate = group.loc[group["dist_to_last"].idxmin()]

        fused_points.append(candidate)
    outdf = pd.DataFrame(fused_points)
    print(
        f"按照时间窗口{window_size}判断并处理活跃设备，整合完毕数据大小为{outdf.shape[0]}，列名称列表为：{list(outdf.columns)}"
    )

    return outdf


# %% [markdown]
# ### `detect_static_devices(df, var_threshold=0.00001)`


# %%
def detect_static_devices(df, var_threshold=0.00001):
    """识别并过滤静态设备"""
    static_devices = []
    for device_id, device_data in df.groupby("device_id"):
        lat_var = device_data["latitude"].var()
        lon_var = device_data["longitude"].var()

        if lat_var < var_threshold and lon_var < var_threshold:
            static_devices.append(device_id)
            log.info(f"设备 {device_id} 被识别为静态设备")

    return df[~df["device_id"].isin(static_devices)]


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
    required_cols = ["latitude", "longitude", "time_diff"]
    if not all(col in df.columns for col in required_cols):
        return pd.DataFrame()

    coords = df[["latitude", "longitude"]].values
    kms_per_radian = 6371.0088
    epsilon = radius_km / kms_per_radian

    db = DBSCAN(
        eps=epsilon, min_samples=min_points, algorithm="ball_tree", metric="haversine"
    ).fit(np.radians(coords))
    df["cluster"] = db.labels_

    clustered = df[df["cluster"] != -1]

    if clustered.empty:
        return pd.DataFrame()

    cluster_centers = (
        clustered.groupby("cluster")
        .agg({"latitude": "mean", "longitude": "mean", "time": "count"})
        .rename(columns={"time": "visit_count"})
        .reset_index()
    )

    cluster_centers["avg_stay_min"] = (
        clustered.groupby("cluster")["time_diff"].mean().values
    )
    print(cluster_centers.columns)
    return cluster_centers.sort_values("visit_count", ascending=False).head(10)


# %%
# 在footsshow.py中修改
def generate_visualizations(df, analysis_results, scope):
    """生成位置数据的可视化图表并返回资源ID"""
    resource_ids = {}

    # 1. 轨迹图
    plt.figure(figsize=(PLOT_WIDTH, PLOT_HEIGHT))
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

    plt.title(f"{scope.capitalize()}位置轨迹")
    plt.xlabel("经度")
    plt.ylabel("纬度")
    plt.grid(True)
    plt.legend(loc="best")

    buf = BytesIO()
    plt.savefig(buf, format="png", dpi=DPI)
    plt.close()
    resource_ids["trajectory"] = add_resource_from_bytes(
        buf.getvalue(), title=f"轨迹图_{scope}.png"
    )

    # 2. 时间分布图
    plt.figure(figsize=(PLOT_WIDTH, PLOT_HEIGHT - 2))
    hourly_distribution = analysis_results["hourly_distribution"]
    plt.bar(
        list(hourly_distribution.keys()), list(hourly_distribution.values()), width=0.8
    )
    plt.title(f"{scope.capitalize()}位置记录时间分布")
    plt.xlabel("小时")
    plt.ylabel("记录数量")
    plt.xticks(range(0, 24))
    plt.grid(True)

    buf = BytesIO()
    plt.savefig(buf, format="png", dpi=DPI)
    plt.close()
    resource_ids["time_dist"] = add_resource_from_bytes(
        buf.getvalue(), title=f"时间分布_{scope}.png"
    )

    # 3. 精度分布图
    if "accuracy" in df.columns:
        plt.figure(figsize=(PLOT_WIDTH, PLOT_HEIGHT - 2))
        plt.hist(df["accuracy"].dropna(), bins=50, alpha=0.7)
        plt.title(f"{scope.capitalize()}位置精度分布")
        plt.xlabel("精度 (米)")
        plt.ylabel("记录数量")
        plt.grid(True)

        buf = BytesIO()
        plt.savefig(buf, format="png", dpi=DPI)
        plt.close()
        resource_ids["accuracy"] = add_resource_from_bytes(
            buf.getvalue(), title=f"精度分布_{scope}.png"
        )

    return resource_ids


# %% [markdown]
# ### generate_device_pie_chart(device_stats)

# %%
def generate_device_pie_chart(device_stats):
    """生成设备分布饼图"""
    plt.figure(figsize=(6, 6))
    # labels = [f"设备{i + 1}" for i in range(len(device_stats))]
    labels = [
        getinivaluefromcloud("device", str(device_id)) for device_id in device_stats
    ]
    sizes = list(device_stats.values())
    plt.pie(sizes, labels=labels, autopct="%1.1f%%", startangle=90)
    plt.axis("equal")

    buf = BytesIO()
    plt.savefig(buf, format="png", dpi=120)
    plt.close()
    return add_resource_from_bytes(buf.getvalue(), "设备分布.png")


# %% [markdown]
# ### generate_time_heatmap(hourly_distribution)

# %%
def generate_time_heatmap(hourly_distribution):
    """生成24小时热力图"""
    hours = list(range(24))
    values = [hourly_distribution.get(h, 0) for h in hours]

    plt.figure(figsize=(10, 3))
    plt.bar(hours, values, color="#4c72b0")
    plt.xticks(hours)
    plt.xlabel("小时")
    plt.ylabel("记录数")
    plt.grid(axis="y", alpha=0.3)

    buf = BytesIO()
    plt.savefig(buf, format="png", dpi=120)
    plt.close()
    return add_resource_from_bytes(buf.getvalue(), "时间分布热力图.png")


# %% [markdown]
# ### generate_geo_link(lat, lon)

# %%
def generate_geo_link(lat, lon):
    """生成地图链接"""
    return f" https://www.openstreetmap.org/?mlat= {lat}&mlon={lon}&zoom=15"

# %% [markdown]
# ## 可视化函数

# %% [markdown]
# ### `generate_visualizations(df, analysis_results)`
# 生成位置数据的可视化图表


# %%
def generate_visualizations(df, analysis_results, scope):
    """生成位置数据的可视化图表并返回资源ID"""
    resource_ids = {}

    # 1. 轨迹图
    plt.figure(figsize=(PLOT_WIDTH, PLOT_HEIGHT))
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

    plt.title(f"{scope.capitalize()}位置轨迹")
    plt.xlabel("经度")
    plt.ylabel("纬度")
    plt.grid(True)
    plt.legend(loc="best")

    buf = BytesIO()
    plt.savefig(buf, format="png", dpi=DPI)
    plt.close()
    resource_ids["trajectory"] = add_resource_from_bytes(
        buf.getvalue(), title=f"轨迹图_{scope}.png"
    )
    # 2. 新增设备分布饼图
    resource_ids["device_dist"] = generate_device_pie_chart(
        analysis_results["device_stats"]
    )

    # 3. 新增时间分布热力图
    resource_ids["time_heatmap"] = generate_time_heatmap(
        analysis_results["hourly_distribution"]
    )

    # 4. 精度分布图（优化展示）
    if "accuracy" in df.columns:
        plt.figure(figsize=(PLOT_WIDTH, PLOT_HEIGHT - 2))
        sns.histplot(df["accuracy"].dropna(), bins=30, kde=True, color="#55a868")
        plt.title(f"{scope.capitalize()}定位精度分布")
        plt.xlabel("精度 (米)")
        plt.grid(True, alpha=0.3)
        resource_ids["accuracy"] = add_resource_from_bytes(
            buf.getvalue(), f"精度分布_{scope}.png"
        )

    return resource_ids


# %% [markdown]
# ## 构建报告内容

# %% [markdown]
# ### `build_report_content(analysis_results, resource_ids, scope)`
# 构建Markdown报告内容


# %%
def build_report_content(analysis_results, resource_ids, scope):
    """优化后的报告结构"""
    # 核心指标卡片式布局
    content = f"""
# 📍 {scope.capitalize()}位置分析报告  
**{analysis_results["time_range"][0]} 至 {analysis_results["time_range"][1]}**  

## 📊 核心指标
| 指标 | 值 | 说明 |
|------|----|------|
| **总记录** | {analysis_results["total_points"]} | 位置点数量 |
| **覆盖天数** | {analysis_results["unique_days"]} | 数据完整度 |
| **活动半径** | {analysis_results["distance_km"]:.2f}km | 最大移动距离 |
| **时间断层** | {analysis_results["gap_stats"]["count"]} | 最长间隔 {analysis_results["gap_stats"]["longest_gap"]:.1f}h |
"""

    # 设备使用饼图替代表格
    device_chart = generate_device_pie_chart(analysis_results["device_stats"])
    content += f"""
## 📱 设备分布
![](:/{resource_ids["device_dist"]})
"""

    # 精度指标卡片
    content += f"""
## 🎯 定位精度
| 指标 | 值 |
|------|----|
| **最佳精度** | {analysis_results["accuracy_stats"]["min"]:.1f}m |
| **最差精度** | {analysis_results["accuracy_stats"]["max"]:.1f}m |
| **平均精度** | {analysis_results["accuracy_stats"]["mean"]:.1f}m |
"""

    # 时间分布热力图
    content += f"""
## 🕒 时间分布
![](:/{resource_ids["time_heatmap"]})
"""

    # 精选重要地点（前3）
    content += """
## 🌍 关键地点
| 位置 | 访问 | 停留 | 坐标 |
|------|------|------|------|"""
    for i, place in enumerate(analysis_results["important_places"][:3]):
        visit_count = int(place["visit_count"])
        lat = place["latitude"]
        lon = place["longitude"]
        content += f"""
| **地点{i + 1}** | {visit_count}次 | {place["avg_stay_min"]:.1f}分 | [{lat}, {lon}]({generate_geo_link(lat, lon)}) |"""

    # 可视化分析
    content += f"""
## 📈 空间分析
### 移动轨迹
![](:/{resource_ids["trajectory"]})

### 精度分布
![](:/{resource_ids["accuracy"]})
"""
    return content


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
        # 更新笔记内容
        updatenote_body(note_id, report_content)
    else:
        parent_id = searchnotebook("ewmobile")
        if not parent_id:
            parent_id = createnote(title="ewmobile", notebook=True)

        # 创建新笔记
        note_id = createnote(title=note_title, parent_id=parent_id, body=report_content)


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

        # 分析数据
        print(f"进入汇总输出时数据列命令列表为：{df.columns.tolist()}")
        analysis_results = analyze_location_data(df, scope)

        # 生成可视化并获取资源ID
        resource_ids = generate_visualizations(df, analysis_results, scope)

        # 构建报告
        report_content = build_report_content(analysis_results, resource_ids, scope)

        # 更新笔记并附加资源
        update_joplin_report(report_content, scope)


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
