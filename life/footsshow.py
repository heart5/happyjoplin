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

import dask.dataframe as dd
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
                "two_year": 24,
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
        notes = searchnotes(f"{note_title}")

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
@timethis
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
    # df = fuse_device_data_dask(df, config)
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

    # 2. 计算分析结果
    print(f"分析启动时数据列为: {df.columns.tolist()}")

    # 2.1 时间范围分析
    start_time = df["time"].min().strftime("%Y-%m-%d")
    end_time = df["time"].max().strftime("%Y-%m-%d")

    # 2.2 基本统计
    total_points = len(df)
    unique_days = df["time"].dt.date.nunique()

    # 2.3 设备分析
    device_stats = df["device_id"].value_counts().to_dict()

    # 2.4 距离
    min_lat, max_lat = df["latitude"].min(), df["latitude"].max()
    min_lon, max_lon = df["longitude"].min(), df["longitude"].max()
    distance_km = great_circle((min_lat, min_lon), (max_lat, max_lon)).kilometers
    # 2.5 大跨越
    if "big_gap" in df.columns:
        big_gaps = df[df["big_gap"]]
        gap_stats = {
            "count": len(big_gaps),
            "longest_gap": df["time_diff"].max() if "time_diff" in df.columns else 0,
        }
    else:
        gap_stats = {"count": 0, "longest_gap": 0}

    # 2.6 小时分布
    df["hour"] = df["time"].dt.hour
    hourly_distribution = df["hour"].value_counts().sort_index().to_dict()

    # 2.7 精度分析
    accuracy_stats = {
        "min": df["accuracy"].min(),
        "max": df["accuracy"].max(),
        "mean": df["accuracy"].mean(),
    }

    # 2.8 重要地点分析
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

    # 2.9 停留点分析
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

    # 3. 生成所有可视化资源
    analysis_results = {
        "scope": scope,
        "time_range": (start_time, end_time),
        "total_points": total_points,
        "unique_days": unique_days,
        "distance_km": distance_km,
        "device_stats": device_stats,
        "hourly_distribution": hourly_distribution,
        "gap_stats": gap_stats,
        "accuracy_stats": accuracy_stats,
        "important_places": important_places.to_dict("records"),
        "stay_stats": stay_stats,
    }
    resource_ids = {}
    # 3.1 轨迹图
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
    buf = BytesIO()
    plt.savefig(buf, format="png", dpi=config.DPI)
    plt.close()
    resource_ids["trajectory"] = add_resource_from_bytes(
        buf.getvalue(), title=f"轨迹图_{scope}.png"
    )

    # 3.2 设备分布饼图
    resource_ids["device_dist"] = generate_device_pie_chart(
        analysis_results["device_stats"]
    )

    # 3.3 时间分布热力图
    resource_ids["time_heatmap"] = generate_time_heatmap(
        analysis_results["hourly_distribution"]
    )

    # 3.4 精度分布图
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

    # 3.5 停留点地图（已在前面计算，这里直接使用）
    resource_ids["stay_points_map"] = analysis_results["stay_stats"]["resource_id"]

    # 3.6 交互式地图
    resource_ids["interactive_map"] = generate_interactive_map(df, scope, config)

    # 3.7 时间序列分析
    resource_ids["time_series"] = generate_time_series_analysis(df, scope, config)

    # 3.8 深度停留分析
    resource_ids["enhanced_stays"] = enhanced_stay_points_analysis(df, scope, config)

    # 3.9 移动模式识别
    resource_ids["movement_patterns"] = movement_pattern_analysis(df, scope, config)

    # 3.10 数据质量监控
    resource_ids["data_quality"] = data_quality_dashboard(df, scope, config)

    # 将资源 ID 添加到分析结果中
    analysis_results["resource_ids"] = resource_ids

    return analysis_results

# %% [markdown]
# ### fuse_device_data(df, config: Config)


# %%
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
# ### fuse_device_data_dask(df, config: Config)

# %%
@timethis
def fuse_device_data_dask(df, config: Config):
    """使用Dask进行并行处理 - 修复版"""
    # 如果数据量小，直接使用单进程
    if len(df) < 10000:  # 小于1万行直接用单进程
        print("数据量较小，使用单进程处理...")
        return fuse_device_data_optimized(df, config)

    # 确保df是Pandas DataFrame
    if not isinstance(df, pd.DataFrame):
        df = df.compute() if hasattr(df, "compute") else pd.DataFrame(df)

    # 获取原始列名
    original_columns = df.columns.tolist()

    # 转换为Dask DataFrame，分区数根据数据大小调整
    n_partitions = max(1, min(os.cpu_count(), len(df) // 5000))  # 每分区约5000行
    ddf = dd.from_pandas(df, npartitions=n_partitions)

    # 定义处理函数 - 确保返回与原始数据相同的列
    def process_partition(partition):
        # 调用优化后的fuse_device_data函数
        result = fuse_device_data_optimized(partition, config)

        # 确保返回的DataFrame只包含原始列
        # 如果结果中有额外列，只保留原始列
        result_columns = set(result.columns)
        original_columns_set = set(original_columns)

        if result_columns != original_columns_set:
            # 找出缺失的列并添加（填充NaN）
            missing_cols = original_columns_set - result_columns
            for col in missing_cols:
                result[col] = np.nan

            # 移除多余的列
            extra_cols = result_columns - original_columns_set
            result = result.drop(columns=list(extra_cols))

            # 确保列顺序一致
            result = result[original_columns]

        return result

    # 提供正确的元数据
    meta = df.iloc[:0].copy()

    try:
        # 应用处理函数
        results = ddf.map_partitions(process_partition, meta=meta).compute()
        return results
    except Exception as e:
        print(f"Dask处理失败: {e}")
        # 回退到单进程处理
        print("回退到单进程处理...")
        return fuse_device_data_optimized(df, config)


# %% [markdown]
# ### fuse_device_data_optimized(df, config: Config)

# %%
# 优化后的fuse_device_data函数
def fuse_device_data_optimized(df, config: Config):
    """优化版的多设备数据智能融合"""
    print(f"多设备数据智能融合时间窗口为：{config.TIME_WINDOW}")

    # 预先计算时间窗口
    df["time_window"] = df["time"].dt.floor(config.TIME_WINDOW)

    # 使用更高效的分组和聚合方法
    fused_points = []
    prev_candidate = None

    # 预先计算所有时间窗口
    time_windows = df["time_window"].unique()

    for window in time_windows:
        group = df[df["time_window"] == window]

        # 计算设备活跃度
        device_activity = {}
        for device_id in group["device_id"].unique():
            device_activity[device_id] = calc_device_activity_fast(group, device_id)

        # 选择最佳候选点
        if len(group) > 1:
            lat_std = group["latitude"].std()
            lon_std = group["longitude"].std()

            if lat_std < 0.002 and lon_std < 0.002:
                candidate = group.loc[group["accuracy"].idxmin()].copy()
            else:
                candidate = group.loc[group["accuracy"].idxmin()].copy()
        else:
            candidate = group.iloc[0].copy()

        # 筛选活跃设备
        active_devices = [
            did
            for did, score in device_activity.items()
            if score > 50 and did in group["device_id"].values
        ]

        if active_devices:
            active_group = group[group["device_id"].isin(active_devices)]
            candidate = active_group.loc[active_group["accuracy"].idxmin()].copy()

        # 时空一致性检查
        if prev_candidate is not None and not check_spatiotemporal_consistency(
            prev_candidate, candidate
        ):
            # 计算所有点到上一个点的距离
            distances = []
            for _, row in group.iterrows():
                dist = great_circle(
                    (prev_candidate.latitude, prev_candidate.longitude),
                    (row.latitude, row.longitude),
                ).m
                distances.append(dist)

            # 添加距离列
            group = group.copy()
            group["dist_to_last"] = distances

            # 选择距离最小的点
            candidate = group.loc[group["dist_to_last"].idxmin()].copy()
            # 移除临时列
            candidate = (
                candidate.drop("dist_to_last")
                if "dist_to_last" in candidate
                else candidate
            )

        fused_points.append(candidate)
        prev_candidate = candidate

    # 创建结果DataFrame，确保只包含原始列
    outdf = pd.DataFrame(fused_points)

    # 移除可能添加的临时列
    original_columns = [col for col in df.columns if col != "time_window"]
    outdf = outdf[original_columns]

    return outdf


# %% [markdown]
# ### calc_device_activity_fast(group, device_id)

# %%
# 快速计算设备活跃度的函数
def calc_device_activity_fast(group, device_id):
    """快速计算设备活跃度"""
    device_data = group[group["device_id"] == device_id]
    if len(device_data) < 2:
        return 0

    # 简化计算逻辑
    time_diff = (device_data["time"].max() - device_data["time"].min()).total_seconds()
    if time_diff == 0:
        return 0

    activity_score = min(100, len(device_data) * 10 / (time_diff / 3600))
    return activity_score

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


# %% [markdown]
# ## 可视化函数

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
# ### generate_interactive_map(df, scope, config)

# %%
def generate_interactive_map(df, scope, config):
    """生成交互式Leaflet地图"""
    import folium

    # 创建基础地图
    center_lat = df["latitude"].mean()
    center_lon = df["longitude"].mean()
    m = folium.Map(location=[center_lat, center_lon], zoom_start=12)

    # 添加轨迹线
    points = list(zip(df["latitude"], df["longitude"]))
    folium.PolyLine(points, color="blue", weight=2, opacity=0.7).add_to(m)

    # 添加停留点标记
    stay_df = df[df["is_stay"]]
    for _, row in stay_df.iterrows():
        folium.CircleMarker(
            location=[row["latitude"], row["longitude"]],
            radius=8,
            color="red",
            fill=True,
            popup=f"停留时间: {row.get('duration', 0) / 60:.1f}分钟",
        ).add_to(m)

    # 保存为HTML文件
    map_path = f"/tmp/interactive_map_{scope}.html"
    m.save(map_path)

    with open(map_path, "rb") as f:
        map_data_bytes = f.read()
    os.remove(map_path)

    return add_resource_from_bytes(map_data_bytes, "交互地图.html")


# %% [markdown]
# ### generate_time_series_analysis(df, scope, config)

# %%
def generate_time_series_analysis(df, scope, config):
    """生成时间序列分析图表"""
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))

    # 每日记录数量趋势
    daily_counts = df.resample("D", on="time").size()
    ax1.plot(daily_counts.index, daily_counts.values)
    ax1.set_title("每日记录数量趋势")
    ax1.tick_params(axis="x", rotation=45)

    # 周内分布热力图
    df["weekday"] = df["time"].dt.dayofweek
    df["hour"] = df["time"].dt.hour
    weekday_hour = df.groupby(["weekday", "hour"]).size().unstack()
    sns.heatmap(weekday_hour, ax=ax2, cmap="YlOrRd")
    ax2.set_title("周内时间分布热力图")

    # 移动速度分析（如果有时间差和距离数据）
    if "dist_to_prev" in df.columns and "time_diff" in df.columns:
        df["speed"] = df["dist_to_prev"] / (df["time_diff"] / 3600)  # km/h
        ax3.hist(df["speed"].dropna(), bins=50, alpha=0.7)
        ax3.set_title("移动速度分布")
        ax3.set_xlabel("速度 (km/h)")

    # 记录间隔分布
    if "time_diff" in df.columns:
        ax4.hist(df["time_diff"].dropna(), bins=50, alpha=0.7)
        ax4.set_title("记录时间间隔分布")
        ax4.set_xlabel("时间间隔 (分钟)")

    plt.tight_layout()
    buf = BytesIO()
    plt.savefig(buf, format="png", dpi=config.DPI)
    plt.close()

    return add_resource_from_bytes(buf.getvalue(), f"时间序列分析_{scope}.png")


# %% [markdown]
# ### enhanced_stay_points_analysis(df, scope, config)

# %%
def enhanced_stay_points_analysis(df, scope, config):
    """增强版停留点分析"""
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(18, 6))

    # 停留时长分布
    stay_durations = df[df["is_stay"]]["duration"] / 60  # 转换为分钟
    ax1.hist(stay_durations, bins=30, alpha=0.7, color="skyblue")
    ax1.set_title("停留时长分布")
    ax1.set_xlabel("停留时间 (分钟)")
    ax1.set_ylabel("频次")

    # 停留点访问频次
    stay_counts = df[df["is_stay"]].groupby("cluster").size()
    ax2.bar(range(len(stay_counts)), sorted(stay_counts.values, reverse=True))
    ax2.set_title("停留点访问频次排名")
    ax2.set_xlabel("停留点排名")
    ax2.set_ylabel("访问次数")

    # 停留点时间分布（日/夜）
    if "hour" in df.columns:
        day_stays = df[df["is_stay"] & (df["hour"].between(6, 18))]
        night_stays = df[df["is_stay"] & (~df["hour"].between(6, 18))]
        ax3.bar(
            ["白天", "夜晚"],
            [len(day_stays), len(night_stays)],
            color=["orange", "navy"],
        )
        ax3.set_title("停留点时间分布")

    plt.tight_layout()
    buf = BytesIO()
    plt.savefig(buf, format="png", dpi=config.DPI)
    plt.close()

    return add_resource_from_bytes(buf.getvalue(), f"增强停留点分析_{scope}.png")


# %% [markdown]
# ### data_quality_dashboard(df, scope, config)

# %%
def data_quality_dashboard(df, scope, config):
    """数据质量监控仪表板"""
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))

    # 数据完整性时间序列
    daily_completeness = (
        df.resample("D", on="time").count()["latitude"] / 1440
    )  # 假设完整数据为1440条/天
    axes[0, 0].plot(daily_completeness.index, daily_completeness.values)
    axes[0, 0].set_title("每日数据完整性")
    axes[0, 0].set_ylim(0, 1)

    # 精度随时间变化
    if "accuracy" in df.columns:
        daily_accuracy = df.resample("D", on="time")["accuracy"].mean()
        axes[0, 1].plot(daily_accuracy.index, daily_accuracy.values)
        axes[0, 1].set_title("日均定位精度")
        axes[0, 1].set_ylabel("精度 (米)")

    # 设备数据贡献比例
    device_contrib = df["device_id"].value_counts()
    axes[1, 0].pie(
        device_contrib.values, labels=device_contrib.index, autopct="%1.1f%%"
    )
    axes[1, 0].set_title("设备数据贡献比例")

    # 时间间隔分布
    if "time_diff" in df.columns:
        axes[1, 1].hist(df["time_diff"].dropna(), bins=50, alpha=0.7)
        axes[1, 1].set_title("记录时间间隔分布")
        axes[1, 1].set_xlabel("时间间隔 (分钟)")

    plt.tight_layout()
    buf = BytesIO()
    plt.savefig(buf, format="png", dpi=config.DPI)
    plt.close()

    return add_resource_from_bytes(buf.getvalue(), f"数据质量仪表板_{scope}.png")


# %% [markdown]
# ### movement_pattern_analysis(df, scope)

# %%
def movement_pattern_analysis(df, scope, config):
    """移动模式识别和分析"""
    # 使用聚类算法识别移动模式
    from sklearn.cluster import KMeans

    # 提取移动特征：速度、方向变化等
    movement_features = []
    for i in range(1, len(df)):
        point1 = df.iloc[i - 1]
        point2 = df.iloc[i]

        # 计算移动特征
        distance = great_circle(
            (point1["latitude"], point1["longitude"]),
            (point2["latitude"], point2["longitude"]),
        ).km

        time_diff = (point2["time"] - point1["time"]).total_seconds() / 3600
        speed = distance / time_diff if time_diff > 0 else 0

        movement_features.append([distance, speed])

    # 聚类分析
    kmeans = KMeans(n_clusters=3, random_state=42)
    clusters = kmeans.fit_predict(movement_features)

    # 可视化聚类结果
    plt.figure(figsize=(10, 6))
    scatter = plt.scatter(
        [f[0] for f in movement_features],
        [f[1] for f in movement_features],
        c=clusters,
        cmap="viridis",
        alpha=0.6,
    )
    plt.colorbar(scatter)
    plt.title("移动模式聚类分析")
    plt.xlabel("移动距离 (km)")
    plt.ylabel("移动速度 (km/h)")

    buf = BytesIO()
    plt.savefig(buf, format="png", dpi=config.DPI)
    plt.close()

    return add_resource_from_bytes(buf.getvalue(), f"移动模式分析_{scope}.png")

# %% [markdown]
# ### `generate_visualizations(df, analysis_results)`
# 生成位置数据的可视化图表


# %%
def generate_visualizations(analysis_results, scope):
    """从分析结果中提取可视化资源ID"""
    # 直接返回 analysis_results 中的 resource_ids
    return analysis_results.get("resource_ids", {})


# %% [markdown]
# ## 构建报告内容

# %% [markdown]
# ### `build_report_content(analysis_results, resource_ids, scope)`
# 构建Markdown报告内容


# %%
def build_report_content(analysis_results, resource_ids, scope):
    """构建Markdown报告内容"""
    # 使用 analysis_results 和 resource_ids 构建报告
    # 现有代码基本不变，但确保所有资源 ID 来自 resource_ids
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

## 📱 设备分布
![设备分布](:/{resource_ids["device_dist"]})

## 🎯 定位精度
| 指标 | 值 |
|------|----|
| **最佳精度** | {analysis_results["accuracy_stats"]["min"]:.1f}m |
| **最差精度** | {analysis_results["accuracy_stats"]["max"]:.1f}m |
| **平均精度** | {analysis_results["accuracy_stats"]["mean"]:.1f}m |

## 🕒 时间分布
![时间分布](:/{resource_ids["time_heatmap"]})

## 🛑 停留点分析
| 指标 | 值 | 说明 |
|---|---|---|
| 总停留次数 | {analysis_results["stay_stats"]["total_stays"]} | 识别到的停留点数量 |
| 平均停留时长 | {analysis_results["stay_stats"]["avg_duration"]:.1f}分钟 | 每次停留的平均时间 |
| 高频停留点 | {len(analysis_results["stay_stats"]["top_locations"])}处 | 访问最频繁的地点 |

### 停留点分布图
![停留点分布](:/{resource_ids["stay_points_map"]})

## 🌍 关键地点
| 位置 | 访问 | 停留 | 坐标 |
|------|------|------|------|
"""
    for i, place in enumerate(analysis_results["important_places"][:3]):
        visit_count = int(place["visit_count"])
        lat = place["latitude"]
        lon = place["longitude"]
        content += f"""| **地点{i + 1}** | {visit_count}次 | {place["avg_stay_min"]:.1f}分 | [{lat}, {lon}]({generate_geo_link(lat, lon)}) |"""

    content += f"""
## 📈 空间分析
### 移动轨迹
![移动轨迹](:/{resource_ids["trajectory"]})

### 位置精度分布
![位置精度分布](:/{resource_ids["accuracy"]})

## 🗺️ 交互式地图
[查看交互式地图](:/{resource_ids["interactive_map"]})

## 📈 时间模式分析
![时间序列分析](:/{resource_ids["time_series"]})

## 🛑 深度停留分析
![停留点分析](:/{resource_ids["enhanced_stays"]})

## 🚶 移动模式识别
![移动模式](:/{resource_ids["movement_patterns"]})

## ✅ 数据质量监控
![数据质量](:/{resource_ids["data_quality"]})
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
    existing_notes = searchnotes(f"{note_title}")

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
    """生成三个层级的报告：月报、季报、年报"""
    for scope in list(config.REPORT_LEVELS.keys())[:]:
        log.info(f"开始生成 {scope} 位置报告...")

        # 1. 加载数据
        df = load_location_data(scope, config)
        if df.empty:
            log.warning(f"跳过 {scope} 报告，无数据")
            continue

        # 2. 分析数据并生成可视化资源
        analysis_results = analyze_location_data(df, scope)

        # 3. 从分析结果中获取资源ID
        resource_ids = generate_visualizations(analysis_results, scope)

        # 4. 构建报告
        report_content = build_report_content(analysis_results, resource_ids, scope)

        # 5. 更新笔记
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
