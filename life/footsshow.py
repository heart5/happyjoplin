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
# ## 引入库

# %%
import base64
import os
import re
from dataclasses import dataclass
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
        searchnotebook,
        searchnotes,
        updatenote_body,
    )
    from func.logme import log
    from func.wrapfuncs import timethis


# %% [markdown]
# ## 配置参数

# %%
@dataclass
class Config:
    REPORT_LEVELS: dict = None
    PLOT_WIDTH: int = 10
    PLOT_HEIGHT: int = 8
    DPI: int = 300
    TIME_WINDOW: str = "30min"  # 默认2h，可以为30min等数值
    STAY_DIST_THRESH: int = 200  # 默认200米
    STAY_TIME_THRESH: int = 600  # 默认600秒，十分钟

    def __post_init__(self):
        if self.REPORT_LEVELS is None:
            self.REPORT_LEVELS = {
                "monthly": 1,
                "quarterly": 3,
                "yearly": 12,
                "two_yearly": 24,
            }

# %% [markdown]
# ## 数据加载函数

# %% [markdown]
# ### load_location_data(scope, config: Config)
# 加载指定范围的位置数据


# %%
def load_location_data(scope, config: Config):
    """
    加载指定范围的位置数据
    """
    # 获取包含当前月份第一天日期的列表
    end_date = datetime.now()
    months = config.REPORT_LEVELS[scope]
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
# ### analyze_location_data(df, scope)


# %%
def analyze_location_data(indf, scope):
    """
    分析位置数据，返回统计结果
    修复列名问题并添加数据预处理
    """
    config = Config()
    df = indf.copy()
    # 1. 数据预处理
    # 1.1 设备融合
    print(
        f"融合设备数据前大小为：{df.shape[0]}；起自{df['time'].min()}，止于{df['time'].max()}。"
    )
    print(df.groupby("device_id").count()["time"])
    df = fuse_device_data(df, config)
    print(
        f"融合设备数据后大小为：{df.shape[0]}；起自{df['time'].min()}，止于{df['time'].max()}。"
    )
    print(df.groupby("device_id").count()["time"])

    # 1.2. 处理时间跳跃
    df = handle_time_jumps(df)

    # 1.3. 位置平滑
    df = smooth_coordinates(df)
    print(
        f"处理融合设备、时间跳跃和位置平滑后设备数据后大小为：{df.shape[0]}；起自{df['time'].min()}，止于{df['time'].max()}。"
    )
    print(df.groupby("device_id").count()["time"])

    # 1.4. 添加必要的时间差列
    # df["time_diff"] = df["time"].diff().dt.total_seconds().fillna(0)
    # df["timestamp"] = df["time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    # df["time_diff"] = df["time"].diff().dt.total_seconds().fillna(0) / 60

    # 2. 记录调试信息
    print(f"分析启动时数据列为: {df.columns.tolist()}")

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

    # 11. 停留点分析
    df = identify_stay_points(df, dist_threshold=350, time_threshold=600)
    # 计算停留点统计
    stay_stats = {
        "total_stays": df["is_stay"].sum(),
        "avg_duration": df[df["is_stay"]]["duration"].mean() / 60
        if "duration" in df
        else 0,
        "top_locations": df[df["is_stay"]]
        .groupby("cluster")
        .size()
        .nlargest(3)
        .to_dict(),
    }
    stay_stats["resource_id"] = generate_stay_points_map(df, scope, config)

    print(f"分析完成后数据列为: {df.columns.tolist()}")
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
        "stay_stats": stay_stats,
    }

# %% [markdown]
# ### fuse_device_data(df, config: Config)


# %%
@timethis
def fuse_device_data(df, config: Config):
    """多设备数据智能融合"""
    print(f"多设备数据智能融合时间窗口为：{config.TIME_WINDOW}")
    df["time_window"] = df["time"].dt.floor(config.TIME_WINDOW)
    # print(df.tail())
    fused_points = []

    for window, group in df.groupby("time_window"):
        # 给设备活跃度赋权，基于时间窗口涵盖的数据
        device_activity = {
            device_id: calc_device_activity(group, device_id)
            for device_id in group["device_id"].unique()
        }
        # 添加位置稳定性检测
        if len(group) > 1:
            # 计算组内位置标准差
            lat_std = group["latitude"].std()
            lon_std = group["longitude"].std()

            # 如果位置变化很小（稳定状态），选择精度最高的点
            if lat_std < 0.002 and lon_std < 0.002:  # 约200米精度
                candidate = group.loc[group["accuracy"].idxmin()]
            else:
                # 原有选择逻辑
                candidate = group.loc[group["accuracy"].idxmin()]
        else:
            candidate = group.iloc[0]

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

    return outdf


# %% [markdown]
# ### calc_device_activity(df, device_id)


# %%
def calc_device_activity(df, device_id):
    """计算设备活跃度评分（0-100）"""
    device_data = df[df["device_id"] == device_id].copy()
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
# ### calc_device_activity_optimized(df, device_id)

# %%
def calc_device_activity_optimized(df, device_id):
    """优化版设备活跃度评分"""
    device_data = df[df["device_id"] == device_id].copy()

    # 基础校验
    if len(device_data) < 2:
        return 0

    # 向量化距离计算（效率提升10倍+）
    coords = device_data[["latitude", "longitude"]].values
    dists = [great_circle(coords[i - 1], coords[i]).m for i in range(1, len(coords))]
    total_dist = sum(dists)

    # 时间跨度计算（添加最小阈值）
    time_min = device_data["time"].min()
    time_max = device_data["time"].max()
    time_span = max(0.1, (time_max - time_min).total_seconds() / 3600)  # 至少0.1小时

    # 位置变化计算（转换为米制单位）
    lat_deg_to_m = 111000  # 1纬度≈111km
    mean_lat = np.radians(device_data["latitude"].mean())
    lon_deg_to_m = 111000 * np.cos(mean_lat)  # 经度距离随纬度变化

    lat_std_m = device_data["latitude"].std() * lat_deg_to_m
    lon_std_m = device_data["longitude"].std() * lon_deg_to_m
    pos_variation = (lat_std_m**2 + lon_std_m**2) ** 0.5  # 综合位置变化

    # 改进评分公式
    distance_score = min(100, total_dist / time_span) * 0.7  # 米/小时
    variation_score = min(100, pos_variation / 1000) * 0.3  # 千米级变化

    return min(100, distance_score + variation_score)


# %% [markdown]
# ### smooth_coordinates(df, window_size=5)

# %%
def smooth_coordinates(df, window_size=5):
    """
    使用滑动窗口平均法平滑经纬度坐标
    参数:
        window_size: 滑动窗口大小（奇数）
    """
    # 确保按时间排序
    df = df.sort_values("time")

    # 使用滚动窗口计算平均位置
    df["smoothed_lat"] = (
        df["latitude"].rolling(window=window_size, center=True, min_periods=1).mean()
    )

    df["smoothed_lon"] = (
        df["longitude"].rolling(window=window_size, center=True, min_periods=1).mean()
    )

    # 对于边缘点，使用原始值
    df["smoothed_lat"] = df["smoothed_lat"].fillna(df["latitude"])
    df["smoothed_lon"] = df["smoothed_lon"].fillna(df["longitude"])

    return df

# %% [markdown]
# ### handle_time_jumps(df)


# %%
def handle_time_jumps(df):
    if df.empty:
        return df

    df = df.sort_values("time")
    df["time_diff"] = df["time"].diff().dt.total_seconds() / 60
    df["big_gap"] = df["time_diff"] > 2 * 60
    df["segment"] = df["big_gap"].cumsum()

    return df


# %% [markdown]
# ### check_spatiotemporal_consistency(point1, point2)


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
# ### detect_static_devices(df, var_threshold=0.0002)


# %%
def detect_static_devices(df, var_threshold=0.0002):
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
# ### identify_stay_points(df, dist_threshold=350, time_threshold=600)

# %%
def identify_stay_points(df, dist_threshold=350, time_threshold=600):
    # 确保数据按时间排序
    df = df.sort_values("time").reset_index(drop=True)

    # 添加前一位置列
    df["prev_lat"] = df["smoothed_lat"].shift(1)
    df["prev_lon"] = df["smoothed_lon"].shift(1)

    # 计算距离
    df["dist_to_prev"] = df.apply(
        lambda row: great_circle(
            (row["smoothed_lat"], row["smoothed_lon"]),
            (row["prev_lat"], row["prev_lon"]),
        ).meters
        if not pd.isna(row["prev_lat"])
        else 0,
        axis=1,
    )

    # 添加时间差列（如果不存在）
    if "time_diff" not in df.columns:
        df["time_diff"] = df["time"].diff().dt.total_seconds().fillna(0)

    # 标记停留点
    df["is_stay"] = (df["dist_to_prev"] < dist_threshold) & (
        df["time_diff"] > time_threshold
    )

    # 分组连续停留点
    df["stay_group"] = (df["is_stay"] != df["is_stay"].shift(1)).cumsum()

    # 计算每组停留时间
    stay_groups = df[df["is_stay"]].groupby("stay_group")
    df["duration"] = stay_groups["time_diff"].transform("sum")

    # print(df.tail(10))
    return df

# %% [markdown]
# ### `identify_important_places(df, radius_km=0.5, min_points=3)`
# 识别重要地点（停留点）


# %%
def identify_important_places(df, radius_km=0.5, min_points=3):
    """
    识别重要地点（停留点）
    减小聚类半径以处理位置扰动
    """
    # 使用平滑后的坐标
    if "smoothed_lat" in df.columns and "smoothed_lon" in df.columns:
        coords = df[["smoothed_lat", "smoothed_lon"]].values
    else:
        coords = df[["latitude", "longitude"]].values

    # 将半径从米转换为度（近似）
    kms_per_radian = 6371.0088
    epsilon = radius_km / kms_per_radian

    # 使用DBSCAN聚类
    db = DBSCAN(
        eps=epsilon, min_samples=min_points, algorithm="ball_tree", metric="haversine"
    ).fit(np.radians(coords))

    df["cluster"] = db.labels_

    # 只保留有效聚类（排除噪声点）
    clustered = df[df["cluster"] >= 0]

    return clustered


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
    return f" https://www.openstreetmap.org/?mlat={lat}&mlon={lon}&zoom=15"


# %% [markdown]
# ### generate_stay_points_map(df, scope, config)

# %%
def generate_stay_points_map(df, scope, config: Config):
    """生成停留点分布图"""
    # import matplotlib.pyplot as plt

    plt.figure(figsize=(config.PLOT_WIDTH, config.PLOT_HEIGHT))

    # 绘制所有轨迹点
    plt.scatter(
        df["longitude"], df["latitude"], c="gray", alpha=0.3, s=5, label="轨迹点"
    )

    # 突出显示停留点
    stay_df = df[df["is_stay"]]
    plt.scatter(
        stay_df["longitude"], stay_df["latitude"], c="red", s=50, label="停留点"
    )

    # 标注高频停留点
    top_stays = stay_df.groupby("cluster").size().nlargest(5).index
    for cluster_id in top_stays:
        cluster_df = stay_df[stay_df["cluster"] == cluster_id]
        center_lon = cluster_df["longitude"].mean()
        center_lat = cluster_df["latitude"].mean()
        # 用emoji符号，绘图时字体貌似不支持
        # plt.text(
        #     center_lon,
        #     center_lat,
        #     f"📍{cluster_id}",
        #     fontsize=12,
        #     ha="center",
        #     va="bottom",
        # )
        # 先绘制标记，再添加文本
        plt.plot(
            center_lon, center_lat, "o", markersize=8, color="red"
        )  # 绘制一个圆点标记
        plt.text(
            center_lon,
            center_lat + 0.001,  # 稍微偏移以避免重叠
            str(cluster_id),
            fontsize=10,
            ha="center",
            va="bottom",
        )
        # 绘制Latex倒三角形，空心
        # plt.text(
        #     center_lon,
        #     center_lat,
        #     r"$\triangledown$" + f"{cluster_id}",
        #     fontsize=12,
        #     ha="center",
        #     va="bottom",
        # )

    plt.title(f"{scope.capitalize()}停留点分布")
    plt.xlabel("经度")
    plt.ylabel("纬度")
    plt.legend()

    # 保存为图片资源
    buf = BytesIO()

    # plt.rcParams["font.sans-serif"] = [
    #     "SimHei",
    #     "DejaVu Sans",
    #     "Noto Sans CJK JP",
    # ]  # 搞了半天应该那个糖葫芦emoji字体导致的问题，特意多放几个字体尝试尝试
    # plt.rcParams["axes.unicode_minus"] = False  # 解决负号显示问题
    plt.savefig(buf, format="png", dpi=config.DPI)
    plt.close()
    # buf.seek(0)
    return add_resource_from_bytes(buf.getvalue(), f"停留点分布_{scope}.png")

# %% [markdown]
# ## 可视化函数

# %% [markdown]
# ### `generate_visualizations(df, analysis_results)`
# 生成位置数据的可视化图表


# %%
def generate_visualizations(df, analysis_results, scope, config: Config):
    """生成位置数据的可视化图表并返回资源ID"""
    resource_ids = {}

    # 1. 轨迹图
    plt.figure(figsize=(config.PLOT_WIDTH, config.PLOT_HEIGHT))
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
    # plt.legend(loc="best")

    buf = BytesIO()
    plt.savefig(buf, format="png", dpi=config.DPI)
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
        plt.figure(figsize=(config.PLOT_WIDTH, config.PLOT_HEIGHT - 2))
        sns.histplot(df["accuracy"].dropna(), bins=30, kde=True, color="#55a868")
        plt.title(f"{scope.capitalize()}定位精度分布")
        plt.xlabel("精度 (米)")
        plt.grid(True, alpha=0.3)
        buf_acc = BytesIO()
        plt.savefig(buf_acc, format="png", dpi=config.DPI)
        plt.close()
        resource_ids["accuracy"] = add_resource_from_bytes(
            buf_acc.getvalue(), f"精度分布_{scope}.png"
        )
    # 5. 停留点地图
    resource_ids["stay_points_map"] = analysis_results["stay_stats"]["resource_id"]

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
![设备分布](:/{resource_ids["device_dist"]})
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
![时间分布](:/{resource_ids["time_heatmap"]})
"""

    # 新增停留点分析部分
    content += f"""
## 🛑 停留点分析

| 指标 | 值 | 说明 |
|---|---|---|
| 总停留次数 | {analysis_results["stay_stats"]["total_stays"]} | 识别到的停留点数量 |
| 平均停留时长 | {analysis_results["stay_stats"]["avg_duration"]:.1f}分钟 | 每次停留的平均时间 |
| 高频停留点 | {len(analysis_results["stay_stats"]["top_locations"])}处 | 访问最频繁的地点 |
"""

    # 添加停留点分布图
    content += f"""
### 停留点分布图
![停留点分布](:/{resource_ids["stay_points_map"]})
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
![移动轨迹](:/{resource_ids["trajectory"]})

### 位置精度分布
![位置进度分布](:/{resource_ids["accuracy"]})
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
@timethis
def generate_location_reports(config: Config):
    """
    生成三个层级的报告：月报、季报、年报
    """
    for scope in list(config.REPORT_LEVELS.keys())[:]:
        log.info(f"开始生成 {scope} 位置报告...")

        # 1. 加载数据
        df = load_location_data(scope, config)
        if df.empty:
            log.warning(f"跳过 {scope} 报告，无数据")
            continue

        # 分析数据
        print(f"进入汇总输出时数据列命令列表为：{df.columns.tolist()}")
        analysis_results = analyze_location_data(df, scope)

        # 生成可视化并获取资源ID
        resource_ids = generate_visualizations(df, analysis_results, scope, config)

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
    generate_location_reports(Config())
    log.info("位置分析报告生成完成")
