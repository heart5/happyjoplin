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
# ## 位置数据展示与分析系统

# %% [markdown]
#
# ## 引入库

# %%
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from io import BytesIO
from typing import Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


# %%
from geopy.distance import great_circle
from sklearn.cluster import DBSCAN

import pathmagic

with pathmagic.context():
    from func.jpfuncs import (
        add_resource_from_bytes,
        createnote,
        createresource,
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
    """参数配置类"""

    REPORT_LEVELS: Optional[dict] = None
    PLOT_WIDTH: int = 8 # 图像宽度默认8英寸
    PLOT_HEIGHT: int = 12 # 图像高度默认12英寸
    DPI: int = 300 # 图像分辨率默认300
    TIME_WINDOW: str = "2h"  # 判断设备活跃的时间窗口，默认2h，可以为30min等数值
    STAY_DIST_THRESH: int = 200  # 停留点距离阈值（米），默认200米
    SAMPLE_FOR_IMPORTANT_POINTS: int = 10000  # 重要地点采样数，默认10000
    RADIUS_KM: float = 1.5  # 识别重要地点时的半径，单位为公里
    MIN_POINTS: int = 100  # 最小点数
    TIME_JUMP_DAY_THRESH: int = 30  # 时间跳跃，白天阈值（分钟）
    TIME_JUMP_NIGHT_THRESH: int = 240  # 时间跳跃，夜间阈值（分钟）

    def __post_init__(self) -> None:
        """从配置读取阈值，如果读取不到则使用默认值"""
        self.TIME_WINDOW = getinivaluefromcloud("foots", "time_window") or self.TIME_WINDOW
        self.STAY_DIST_THRESH = getinivaluefromcloud("foots", "stay_dist_thresh") or self.STAY_DIST_THRESH
        self.SAMPLE_FOR_IMPORTANT_POINTS = getinivaluefromcloud("foots", "sample_for_important_points") or self.SAMPLE_FOR_IMPORTANT_POINTS
        self.RADIUS_KM = getinivaluefromcloud("foots", "radius_km") or self.RADIUS_KM
        self.MIN_POINTS = getinivaluefromcloud("foots", "min_points") or self.MIN_POINTS
        self.TIME_JUMP_DAY_THRESH = int(getinivaluefromcloud("foots", "time_jump_day_thresh") or self.TIME_JUMP_DAY_THRESH)
        self.TIME_JUMP_NIGHT_THRESH = int(getinivaluefromcloud("foots", "time_jump_night_thresh") or self.TIME_JUMP_NIGHT_THRESH)

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
def load_location_data(scope: str, config: Config) -> pd.DataFrame:
    """加载指定范围的位置数据"""
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
def analyze_location_data(indf: pd.DataFrame, scope: str) -> dict:
    """分析位置数据，返回统计结果

    修复列名问题并添加数据预处理
    """
    config = Config()
    df = indf.copy()

    # 1. 数据预处理
    # 1.1. 按设备和时间列去重
    df = df.sort_values(by=["device_id", "time"]).drop_duplicates(subset=["device_id", "time"])
    print(
        f"去重后大小为：{df.shape[0]}；起自{df['time'].min()}，止于{df['time'].max()}。"
    )
    print(df.groupby("device_id").count()["time"])

    # 1.2 设备融合
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

    # 1.3. 处理时间跳跃，添加time_diff列，big_gap列和segment列
    df = handle_time_jumps(df, config)

    # 1.4. 位置平滑
    df = smooth_coordinates(df)
    print(
        f"处理融合设备、时间跳跃和位置平滑后设备数据后大小为：{df.shape[0]}；起自{df['time'].min()}，止于{df['time'].max()}。"
    )
    print(df.groupby("device_id").count()["time"])

    # 1.5. 重要地点分析
    clustered = identify_important_places(df, config)
    if "cluster" in clustered.columns:
        df["cluster"] = clustered["cluster"]
    print(f"重要地点分析后数据列为: {df.columns.tolist()}")

    # 2. 计算分析结果

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
    if "cluster" in df.columns:
        important_places = (
            df[df["cluster"] >= 0]
            .groupby("cluster")
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
    else:
        important_places = pd.DataFrame()

    # 2.9 停留点分析
    df = identify_stay_points(df, config)
    # 计算停留点统计
    stay_stats = {
        "total_stays": df["stay_group"].nunique(),  # 调整total_stays的计算逻辑
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

    # 随机选择一个stay_group值，并打印该值第一次出现的前五条记录和后五条记录
    if df["stay_group"].isna().all():
        print("没有找到stay_group的记录。")
    else:
        stay_groups = df[df["stay_group"].notna()]["stay_group"].unique()
        random_stay_group = np.random.choice(stay_groups)
        first_occurrence_index = df[df["stay_group"] == random_stay_group].index[0]
        print(f"随机选取的stay_group为: {random_stay_group}")
        print("该stay_group第一次出现的前五条记录和后五条记录如下：")
        print(df.iloc[max(0, first_occurrence_index-5):first_occurrence_index+6])

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
    resource_ids["trajectory_with_map"] = generate_trajectory_map(df, scope, config)

    # 保留原始轨迹图作为备选
    # resource_ids["trajectory"] = generate_trajectory_map_fallback(df, scope, config)

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
def fuse_device_data(df: pd.DataFrame, config: Config) -> pd.DataFrame:
    """多设备数据智能选择：每个时间窗口选择最佳设备的数据"""
    print(f"多设备数据智能选择时间窗口为：{config.TIME_WINDOW}")

    # 1. 创建时间窗口
    df["time_window"] = df["time"].dt.floor(config.TIME_WINDOW)

    # 2. 存储最终选择的数据点
    selected_points = []

    for window, group in df.groupby("time_window"):
        if len(group) == 0:
            continue

        # 3. 计算每个设备的综合评分
        device_scores = {}
        for device_id, device_data in group.groupby("device_id"):
            # 3.1 计算设备活跃度
            activity = calc_device_activity_optimized(device_data, device_id)

            # 3.2 计算设备平均精度（精度越高越好）
            avg_accuracy = device_data["accuracy"].mean()

            # 3.3 计算设备位置稳定性
            lat_std = device_data["latitude"].std()
            lon_std = device_data["longitude"].std()
            stability = 1 / (lat_std + lon_std + 1e-6)  # 避免除零

            # 3.4 综合评分 = 活跃度 * 稳定性 * (1/平均精度)
            score = activity * stability * (1 / max(avg_accuracy, 1))
            device_scores[device_id] = score

        # 4. 选择评分最高的设备
        best_device = max(device_scores, key=device_scores.get)

        # 5. 获取该设备在本时间窗口的所有数据点
        best_device_data = group[group["device_id"] == best_device]

        # 6. 添加元数据
        best_device_data = best_device_data.copy()
        best_device_data["selected_device"] = best_device
        best_device_data["selection_score"] = device_scores[best_device]

        selected_points.append(best_device_data)

    # 7. 合并所有选择的数据点
    result_df = pd.concat(selected_points)
    return result_df

# %% [markdown]
# ### calc_device_activity(df, device_id)


# %%
def calc_device_activity(df: pd.DataFrame, device_id: str) -> int:
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
        int((total_dist / max(1, time_span)) * 0.7 + (lat_var + lon_var) * 10000 * 0.3),
    )
    return activity_score


# %% [markdown]
# ### calc_device_activity_optimized(df, device_id)

# %%
def calc_device_activity_optimized(df: pd.DataFrame, device_id: str) -> int:
    """优化版设备活跃度评分"""
    device_data = df[df["device_id"] == device_id].copy()

    # 基础校验
    if len(device_data) < 2:
        return 0

    # 向量化距离计算
    coords = device_data[["latitude", "longitude"]].values
    dists = np.zeros(len(coords) - 1)
    for i in range(1, len(coords)):
        dists[i - 1] = great_circle(coords[i - 1], coords[i]).m
    total_dist = np.sum(dists)

    # 时间跨度计算
    time_min = device_data["time"].min()
    time_max = device_data["time"].max()
    time_span = max(0.1, (time_max - time_min).total_seconds() / 3600)

    # 位置变化计算
    lat_deg_to_m = 111000
    mean_lat = np.radians(device_data["latitude"].mean())
    lon_deg_to_m = 111000 * np.cos(mean_lat)

    lat_std_m = device_data["latitude"].std() * lat_deg_to_m
    lon_std_m = device_data["longitude"].std() * lon_deg_to_m
    pos_variation = np.sqrt(lat_std_m**2 + lon_std_m**2)

    # 改进评分公式
    distance_score = min(100, total_dist / time_span) * 0.7
    variation_score = min(100, pos_variation / 1000) * 0.3

    return min(100, int(distance_score + variation_score))


# %% [markdown]
# ### smooth_coordinates(df, window_size=5)

# %%
def smooth_coordinates(df: pd.DataFrame, window_size: int=5) -> pd.DataFrame:
    """使用滑动窗口平均法平滑经纬度坐标

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
# ### handle_time_jumps(df, config: Config)


# %%
def handle_time_jumps(df: pd.DataFrame, config: Config) -> pd.DataFrame:
    """智能处理时间跳跃，考虑位置变化和设备切换

    避免过度分割连续轨迹
    """
    if df.empty:
        return df

    # 1. 排序并计算时间差
    df = df.sort_values("time")
    df["time_diff"] = df["time"].diff().dt.total_seconds().fillna(0) / 60

    # 2. 动态阈值计算（基于活动模式）
    # 工作日白天阈值较低（30分钟），夜间阈值较高（4小时）
    hour = df["time"].dt.hour
    is_weekday = df["time"].dt.dayofweek < 6
    day_threshold = config.TIME_JUMP_DAY_THRESH  # 30分钟
    night_threshold = config.TIME_JUMP_NIGHT_THRESH  # 4小时

    # 动态阈值：白天工作时间阈值低，夜间阈值高
    df["dynamic_threshold"] = np.where(
        (hour >= 8) & (hour <= 20) & is_weekday, day_threshold, night_threshold
    )

    # 3. 智能跳跃检测（结合时间和位置变化）
    df["prev_lat"] = df["latitude"].shift(1)
    df["prev_lon"] = df["longitude"].shift(1)

    # 计算位置变化（米）
    df["dist_change"] = df.apply(
        lambda row: great_circle(
            (row["prev_lat"], row["prev_lon"]), (row["latitude"], row["longitude"])
        ).m
        if not pd.isna(row["prev_lat"])
        else 0,
        axis=1,
    )

    # 4. 跳跃条件：时间差超过阈值且位置变化小（可能为设备切换或静止）
    df["big_gap"] = (df["time_diff"] > df["dynamic_threshold"]) & (
        df["dist_change"] < config.STAY_DIST_THRESH
    )

    # 5. 设备切换检测（额外标记）
    df["device_change"] = df["device_id"] != df["device_id"].shift(1)

    # 6. 智能分段逻辑
    # 组合时间跳跃和设备切换作为分段点
    df["segment_point"] = df["big_gap"] | df["device_change"]
    df["segment"] = df["segment_point"].cumsum()

    # 清理临时列
    df.drop(
        ["prev_lat", "prev_lon", "dynamic_threshold", "segment_point", "device_change"],
        axis=1,
        inplace=True,
        errors="ignore",
    )

    return df


# %% [markdown]
# ### check_spatiotemporal_consistency(point1, point2)


# %%
def check_spatiotemporal_consistency(point1: pd.Series, point2: pd.Series) -> bool:
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
def detect_static_devices(df: pd.DataFrame, var_threshold: float=0.0002) -> pd.DataFrame:
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
# ### identify_stay_points(df, config)

# %%
def identify_stay_points(df: pd.DataFrame, config: Config) -> pd.DataFrame:
    """识别停留点并做相应处理，增加数据列is_stay、stay_group、duration"""
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

    # 初始化is_stay列为False
    df["is_stay"] = False

    # 初始化stay_group列
    df["stay_group"] = None

    # 初始化duration列
    df["duration"] = None

    # 标记停留点
    stay_group_counter = 0
    current_stay_group = None

    for i in range(1, len(df)):
        if (df.loc[i, "dist_to_prev"] < config.STAY_DIST_THRESH):
            if current_stay_group is None:
                stay_group_counter += 1
                current_stay_group = stay_group_counter
            df.loc[i, "is_stay"] = True
            df.loc[i, "stay_group"] = current_stay_group
        else:
            current_stay_group = None

    # 计算每组停留时间
    stay_groups = df[df["is_stay"]].groupby("stay_group")
    df.loc[df["is_stay"], "duration"] = stay_groups["time_diff"].transform("sum")

    # 删除过程数据列prev_lat和prev_lon
    df.drop(columns=["prev_lat", "prev_lon"], inplace=True)

    return df



# %% [markdown]
# ### identify_important_places(df, radius_km=1.5, min_points=200)
# 识别重要地点（停留点）


# %%
@timethis
def identify_important_places(df: pd.DataFrame, config: Config, radius_km: float=1.5, min_points: int=200) -> pd.DataFrame:
    """识别重要地点

    1.5公里半径内的点数量大于200个，则认为是重要地点。

    Args:
        df (pd.DataFrame): 原始数据
        config (Config): 配置信息
        radius_km (float, optional): 半径，单位为公里. Defaults to 1.5.
        min_points (int, optional): 最小点数. Defaults to 200.

    Returns:
        pd.DataFrame: 重要地点数据
    """
    # log.info(f"识别重要地点初始数据记录数为：\t{df.shape[0]}")
    # 使用平滑后的坐标
    if "smoothed_lat" in df.columns and "smoothed_lon" in df.columns:
        coords = df[["smoothed_lat", "smoothed_lon"]].values
    else:
        coords = df[["latitude", "longitude"]].values

    # 优化1：对数据进行采样，减少处理量
    sample_size = min(config.SAMPLE_FOR_IMPORTANT_POINTS, len(coords))
    if len(coords) > sample_size:
        indices = np.random.choice(len(coords), sample_size, replace=False)
        coords = coords[indices]
    # log.info(f"识别重要地点初始数据记录数抽样后为：\t{len(coords)}")

    kms_per_radian = 6371.0088
    epsilon = radius_km / kms_per_radian  # 1500米半径

    # 优化3：使用更高效的算法参数
    db = DBSCAN(
        eps=epsilon,
        min_samples=min_points,
        algorithm="ball_tree",
        metric="haversine",
        n_jobs=-1,  # 使用所有CPU核心并行计算
    ).fit(np.radians(coords))

    # 为原始数据添加聚类标签
    df["cluster"] = -1  # 默认-1表示噪声点
    if len(coords) < len(df):
        # 只更新采样点的聚类标签
        df.iloc[indices, df.columns.get_loc("cluster")] = db.labels_
    else:
        df["cluster"] = db.labels_

    # 只保留有效聚类（排除噪声点）
    clustered = df[df["cluster"] >= 0]

    return clustered


# %% [markdown]
# ### identify_important_places_before(df, radius_km=0.5, min_points=3)

# %%
def identify_important_places_before(df: pd.DataFrame, radius_km: float=0.5, min_points: int=3) -> pd.DataFrame:
    """识别重要地点（停留点）

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
def generate_device_pie_chart(device_stats: dict) -> str:
    """生成设备分布饼图

    return res_id: str
    """
    plt.figure(figsize=(6, 6))
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
def generate_time_heatmap(hourly_distribution: dict) -> str:
    """生成24小时热力图

    return res_id: str
    """
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
def generate_geo_link(lat: float, lon: float) -> str:
    """生成地图链接"""
    return f" https://www.openstreetmap.org/?mlat={lat}&mlon={lon}&zoom=15"


# %% [markdown]
# ### generate_trajectory_map(df, scope, config)

# %%
def generate_trajectory_map(df: pd.DataFrame, scope: str, config: Config) -> str:
    """生成带地图底图的轨迹图（优化版）- 显示分段起始日期

    scope: str，日期范围名称
    """
    try:
        import contextily as ctx

        fig, ax = plt.subplots(figsize=(config.PLOT_WIDTH, config.PLOT_HEIGHT))

        # 1. 优化图例处理 - 只显示最新的6个分段，并显示起始日期
        max_legend_items = 6  # 最多显示6个图例项

        if "segment" in df.columns:
            # 获取所有分段并按起始时间排序（最新的在前）
            segments = df["segment"].unique()

            # 计算每个分段的起始时间
            segment_start_time = {}
            for segment in segments:
                seg_df = df[df["segment"] == segment]
                segment_start_time[segment] = seg_df[
                    "time"
                ].min()  # 使用min()获取起始时间

            # 按起始时间排序（最新的在前）
            sorted_segments = sorted(
                segments, key=lambda x: segment_start_time[x], reverse=True
            )

            # 只保留前6个最新分段
            segments_to_show = sorted_segments[:max_legend_items]

            # 绘制所有分段但只显示最新6个的图例
            for segment in segments:
                seg_df = df[df["segment"] == segment]
                if segment in segments_to_show:
                    # 格式化日期为"25年9月1日"的格式
                    start_date_str = segment_start_time[segment].strftime(
                        "%y年%-m月%-d日"
                    )

                    ax.plot(
                        seg_df["longitude"],
                        seg_df["latitude"],
                        alpha=0.7,
                        linewidth=2.0,
                        label=f"{start_date_str}",
                    )
                else:
                    ax.plot(
                        seg_df["longitude"],
                        seg_df["latitude"],
                        alpha=0.7,
                        linewidth=2.0,
                        color="gray",  # 使用灰色表示不显示图例的分段
                    )
        else:
            # 没有分段数据
            ax.plot(df["longitude"], df["latitude"], "b-", alpha=0.7, linewidth=2.0)

        # 2. 优化边界计算
        min_lon, max_lon = df["longitude"].min(), df["longitude"].max()
        min_lat, max_lat = df["latitude"].min(), df["latitude"].max()

        lon_range = max_lon - min_lon
        lat_range = max_lat - min_lat

        # 动态计算边距 - 基于数据范围的比例
        if lon_range < 0.1 or lat_range < 0.1:  # 小范围数据
            margin_factor = 0.15  # 15%的边距
        else:  # 大范围数据
            margin_factor = 0.05  # 5%的边距

        lon_margin = lon_range * margin_factor
        lat_margin = lat_range * margin_factor

        # 确保最小边距（避免数据点太靠近边缘）
        min_abs_margin = 0.005  # 最小绝对边距（度）
        lon_margin = max(lon_margin, min_abs_margin)
        lat_margin = max(lat_margin, min_abs_margin)

        ax.set_xlim(min_lon - lon_margin, max_lon + lon_margin)
        ax.set_ylim(min_lat - lat_margin, max_lat + lat_margin)

        # 3. 计算合适的缩放级别
        lon_range = max_lon - min_lon
        lat_range = max_lat - min_lat
        max_range = max(lon_range, lat_range)

        # 根据数据范围动态确定缩放级别
        # 更精确的缩放级别映射
        if max_range < 0.001:  # 非常小的范围（约100米）
            zoom_level = 18
        elif max_range < 0.005:  # 约500米
            zoom_level = 16
        elif max_range < 0.01:  # 约1公里
            zoom_level = 15
        elif max_range < 0.05:  # 约5公里
            zoom_level = 14
        elif max_range < 0.1:  # 约10公里
            zoom_level = 13
        elif max_range < 0.5:  # 约50公里
            zoom_level = 12
        else:  # 大范围
            zoom_level = 10

        # 4. 使用高分辨率地图源
        try:
            # 尝试使用Stamen Terrain背景，通常提供较高清晰度
            ctx.add_basemap(
                ax,
                crs="EPSG:4326",
                source=ctx.providers.Stamen.Terrain,
                zoom=zoom_level,  # 指定缩放级别
                alpha=0.8,
            )
        except Exception:
            # 备用方案：使用OpenStreetMap但指定缩放级别
            ctx.add_basemap(
                ax,
                crs="EPSG:4326",
                source=ctx.providers.OpenStreetMap.Mapnik,
                zoom=zoom_level,
                alpha=0.8,
            )

        # 5. 设置标题和标签
        ax.set_title(f"{scope.capitalize()}位置轨迹（带地图底图）", fontsize=14)
        ax.set_xlabel("经度")
        ax.set_ylabel("纬度")
        ax.grid(True, alpha=0.3)

        # 6. 只显示最新6个分段的图例
        if "segment" in df.columns and len(segments_to_show) > 0:
            ax.legend(
                loc="upper left",
                bbox_to_anchor=(0, 1),
                fontsize="small",
                ncol=min(2, len(segments_to_show)),  # 最多2列
                title="行程起始日期",  # 添加图例标题
            )

        # 7. 提高保存图像的质量
        buf = BytesIO()
        plt.savefig(
            buf,
            format="png",
            dpi=config.DPI,
            bbox_inches="tight",
            facecolor="white",
        )
        plt.close()

        return add_resource_from_bytes(
            buf.getvalue(), title=f"轨迹图_{scope}_带地图.png"
        )

    except ImportError:
        log.warning("未安装contextily库，无法添加地图底图")
        return generate_trajectory_map_fallback(df, scope, config)


# %% [markdown]
# ### generate_trajectory_map_fallback(df, scope, config)

# %%
def generate_trajectory_map_fallback(df: pd.DataFrame, scope: str, config: Config) -> str:
    """生成不带地图底图的轨迹图（备用）- 显示分段起始日期"""
    plt.figure(figsize=(config.PLOT_WIDTH, config.PLOT_HEIGHT))

    # 只显示最新6个分段
    max_legend_items = 6

    if "segment" in df.columns:
        # 获取所有分段并按起始时间排序（最新的在前）
        segments = df["segment"].unique()

        # 计算每个分段的起始时间
        segment_start_time = {}
        for segment in segments:
            seg_df = df[df["segment"] == segment]
            segment_start_time[segment] = seg_df["time"].min()  # 使用min()获取起始时间

        # 按起始时间排序（最新的在前）
        sorted_segments = sorted(
            segments, key=lambda x: segment_start_time[x], reverse=True
        )

        # 只保留前6个最新分段
        segments_to_show = sorted_segments[:max_legend_items]

        # 绘制所有分段但只显示最新6个的图例
        for segment in segments:
            seg_df = df[df["segment"] == segment]
            if segment in segments_to_show:
                # 格式化日期为"25年9月1日"的格式
                start_date_str = segment_start_time[segment].strftime("%y年%-m月%-d日")

                plt.plot(
                    seg_df["longitude"],
                    seg_df["latitude"],
                    alpha=0.7,
                    linewidth=1.5,
                    label=f"{start_date_str}",
                )
            else:
                plt.plot(
                    seg_df["longitude"],
                    seg_df["latitude"],
                    alpha=0.7,
                    linewidth=1.5,
                    color="gray",
                )

        plt.legend(
            loc="upper left",
            fontsize="small",
            ncol=min(2, len(segments_to_show)),
            title="行程起始日期",  # 添加图例标题
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

    return add_resource_from_bytes(buf.getvalue(), title=f"轨迹图_{scope}.png")


# %% [markdown]
# ### generate_stay_points_map(df, scope, config)

# %%
def generate_stay_points_map(df: pd.DataFrame, scope: str, config: Config) -> str:
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

    plt.savefig(buf, format="png", dpi=config.DPI)
    plt.close()
    return add_resource_from_bytes(buf.getvalue(), f"停留点分布_{scope}.png")


# %% [markdown]
# ### generate_interactive_map(df, scope, config)

# %%
def generate_interactive_map(df: pd.DataFrame, scope: str, config: Config) -> str:
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

    # 保存为html文件
    map_path = f"/tmp/interactive_map_{scope}.html"
    m.save(map_path)
    res_id = createresource(map_path, title="交互地图.html")
    os.remove(map_path)

    return res_id


# %% [markdown]
# ### generate_time_series_analysis(df, scope, config)

# %%
def generate_time_series_analysis(df: pd.DataFrame, scope: str, config: Config) -> str:
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
        valid_speeds = df["speed"][np.isfinite(df["speed"])]  # 过滤inf/nan

        if not valid_speeds.empty:
            ax3.hist(valid_speeds, bins=50, alpha=0.7)
            ax3.set_title("速度分布")
            ax3.set_xlabel("速度 (公里/小时)")
            ax3.set_ylabel("数量")
        else:
            ax3.text(
                0.5,
                0.5,
                "无有效速度数据",
                horizontalalignment="center",
                verticalalignment="center",
                transform=ax3.transAxes,
            )
            ax3.set_title("速度分布 (无数据)")

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
def enhanced_stay_points_analysis(df: pd.DataFrame, scope: str, config: Config) -> str:
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
def data_quality_dashboard(df: pd.DataFrame, scope: str, config: Config) -> str:
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
def movement_pattern_analysis(df: pd.DataFrame, scope: str, config: Config) -> str:
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
def generate_visualizations(analysis_results: str, scope: str) -> dict:
    """从分析结果中提取可视化资源ID"""
    # 直接返回 analysis_results 中的 resource_ids
    return analysis_results.get("resource_ids", {})


# %% [markdown]
# ## 构建报告内容

# %% [markdown]
# ### `build_report_content(analysis_results, resource_ids, scope)`
# 构建Markdown报告内容


# %%
def build_report_content(analysis_results: dict, resource_ids:str, scope: str) -> str:
    """构建Markdown报告内容"""
    # 使用 analysis_results 和 resource_ids 构建报告
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
        content += f"""| **地点{i + 1}** | {visit_count}次 | {place["avg_stay_min"]:.1f}分 | [{lat}, {lon}]({generate_geo_link(lat, lon)}) |\n"""

    content += f"""
## 📈 空间分析
### 移动轨迹
![移动轨迹](:/{resource_ids["trajectory_with_map"]})

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
def update_joplin_report(report_content: str, scope: str) -> None:
    """更新Joplin位置分析报告"""
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
def generate_location_reports(config: Config) -> None:
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
