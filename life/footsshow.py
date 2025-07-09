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
# # 足迹展示

# %% [markdown]
# ## 库引入

# %%
import os
import re
import arrow
from tzlocal import get_localzone
from pathlib import Path
from math import radians, cos, sin, asin, sqrt

import pandas as pd
import numpy as np
import folium
from typing import List, Tuple
from pylab import plt
# import plotly.express as px
# from plotly.subplots import make_subplots
# import plotly.graph_objects as go
# import plotly.io as pio

# %%
# 自定义函数
import pathmagic
with pathmagic.context():
    from func.configpr import getcfpoptionvalue, setcfpoptionvalue
    from func.first import getdirmain, dirmainpath, touchfilepath2depth
    from func.datatools import readfromtxt, write2txt
    from func.jpfuncs import (searchnotes, createnote, updatenote_imgdata,
                                noteid_used, searchnotebook, updatenote_title,
                                updatenote_body, getinivaluefromcloud,
                                createresource, deleteresourcesfromnote)
    from func.logme import log
    from func.wrapfuncs import timethis, ift2phone
    from func.termuxtools import (termux_telephony_deviceinfo,
                                termux_telephony_cellinfo, termux_location)
    from etc.getid import getdeviceid, gethostuser
    from func.sysfunc import not_IPython, set_timeout, after_timeout


# %% [markdown]
# ## 功能函数

# %% [markdown]
# ### geodistance(lng1, lat1, lng2, lat2)

# %%
def geodistance(lng1: float, lat1: float, lng2: float, lat2: float) -> float:
    """
    计算两点之间的距离并返回（公里，千米）
    """
    lng1, lat1, lng2, lat2 = map(radians, [lng1, lat1, lng2, lat2])
    dlon = lng2 - lng1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    # 地球半径为：6371千米
    dis = 4 * asin(sqrt(a)) * 6371 * 1000
    return dis


# %% [markdown]
# ### chuli_datasource()

# %%
@timethis
def chuli_datasource() -> pd.DataFrame:
    """
    展示足迹
    """
    namestr = 'happyjp_life'
    section = 'hjloc'
    if not (device_id := str(getcfpoptionvalue(namestr, section, 'device_id'))):
        device_id = getdeviceid()
        setcfpoptionvalue(namestr, section, 'device_id', device_id)

    txtfilename = str(dirmainpath / 'data' / 'ifttt' / f'location_{device_id}.txt')
    print(txtfilename)
    itemread = readfromtxt(txtfilename)
    numlimit = 9    # 显示项目数
    print(itemread[:numlimit])
    itemsrc = [x.split('\t') for x in itemread if not 'False' in x]
    itemnotfine = [x for x in itemsrc if len(x) < 3]
    print(f"有问题的数据共有{len(itemnotfine)}行：{itemnotfine}")
#     itemfine = [x for x in itemsrc if len(x) >= 3][:10000]
    itemfine = [x for x in itemsrc if len(x) >= 3]
    # print(itemfine)
    if len(itemfine) < 2:
        print('gps数据量不足，暂时无法输出移动距离信息')
        return
    timesr = list()
    dissr = list()
    outlst = list()
    # speedsr = list()
    highspeed = getinivaluefromcloud('life', 'highspeed')
    print(f"{highspeed}\t{type(highspeed)}")
    for i in range(len(itemfine) - 1):
        if (len(itemfine[i]) < 5 ) | (len(itemfine[i + 1]) < 5):
            print(itemfine[i], itemfine[i + 1])
        time1, lat1, lng1, alt1, *others, pro1 = itemfine[i]
        time2, lat2, lng2, alt2, *others, pro2 = itemfine[i + 1]
        # print(f'{lng1}\t{lat1}\t\t{lng2}\t{lat2}')
        dis = round(geodistance(eval(lng1), eval(lat1), eval(lng2), eval(lat2)) / 1000, 3)
#         dis = round(geodistance(eval(lng1), eval(lat1), eval(lng2), eval(lat2)), 3)
        try:
            itemtime = pd.to_datetime(time1)
            itemtimeend = pd.to_datetime(time2)
            timedelta = itemtime - itemtimeend
        except ValueErrors as eep:
            log.critical(f"{time1}\t{time2}，处理此时间点处数据出现问题。跳过此组（两个）数据条目！！！{eep}")
            continue
        while timedelta.seconds == 0:
            log.info(f"位置记录时间戳相同：{itemtime}\t{itemtimeend}")
            i = i + 1
            time2, lng2, lat2, *others = itemfine[i + 1]
            dis = round(geodistance(float(lng1), float(lat1), float(lng2), float(lat2)) / 1000, 3)
            itemtime = pd.to_datetime(time1)
            itemtimeend = pd.to_datetime(time2)
            timedelta = itemtime - itemtimeend
        timedeltahour = timedelta.seconds / 60 / 60
        itemspeed = round(dis / timedeltahour, 2)
        if itemspeed >= highspeed * 1000:
            log.info(f"时间起点：{itemtimeend}，时间截止点：{itemtime}，时长：{round(timedeltahour, 3)}小时，距离：{dis}公里，速度：{itemspeed}码")
            i += 1
            continue
        timesr.append(itemtime)
        dissr.append(round(dis, 3))
        outlst.append([pd.to_datetime(time1), float(lng1), float(lat1), float(alt1), pro1])

    df = pd.DataFrame(outlst, columns=['time', 'longi', 'lati', 'alti', 'provider']).sort_values(['time'])
    df['jiange'] = df['time'].diff()
    df['longi1'] = df['longi'].shift()
    df['lati1'] = df['lati'].shift()
    df['distance'] = df.apply(lambda x: round(geodistance(x.longi1, x.lati1, x.longi, x.lati) / 1000, 3), axis=1)
#     df['distance'] = df.apply(lambda x: round(geodistance(x.longi1, x.lati1, x.longi, x.lati), 3), axis=1)

    log.info(f"位置数据大小为：{df.shape[0]}")
    return df.set_index(['time'])[['longi', 'lati', 'alti', 'provider', 'jiange', 'distance']]


# %% [markdown]
# ### foot2show(df4dis)

# %%
@set_timeout(360, after_timeout)
@timethis
def foot2show(df4dis):
    """
    展示足迹
    """
    namestr = 'happyjp_life'
    section = 'hjloc'
    if (device_id := getcfpoptionvalue(namestr, section, 'device_id')) is None:
        device_id = getdeviceid()
        setcfpoptionvalue(namestr, section, 'device_id', device_id)
    device_id = str(device_id)

    noteloc_title = f"轨迹动态_【{gethostuser()}】"
    nbid = searchnotebook("ewmobile")
    if not (loc_cloud_id := getcfpoptionvalue(namestr, section, "loc_cloud_id")):
        ipnotefindlist = searchnotes(f"title:{noteloc_title}")
        if (len(ipnotefindlist) == 0):
            loc_cloud_id = createnote(title=noteloc_title, parent_id=nbid)
            log.info(f"新的轨迹动态图笔记“{loc_cloud_id}”新建成功！")
        else:
            loc_cloud_id = ipnotefindlist[-1].id
        setcfpoptionvalue(namestr, section, 'loc_cloud_id', f"{loc_cloud_id}")

    imglst = []
    ds = df4dis['distance']
    today = arrow.now(get_localzone())
    start_time = pd.Timestamp(today.date().strftime("%F"))
    end_time = pd.Timestamp(today.shift(days=1).date().strftime("%F"))
    try:
        dstoday = ds.loc[start_time:end_time].sort_index().cumsum()
        if dstoday.empty:
            raise KeyError
    except KeyError:
        log.warning(f"{today}无有效数据")
        dstoday = pd.Series(dtype=float)

    if dstoday.shape[0] > 1:
        plt.figure(figsize=(10, 5))
        dstoday.plot()
        imgpathtoday = dirmainpath / 'img' / 'gpstoday.png'
        touchfilepath2depth(imgpathtoday)
        plt.title('今日移动距离')
        plt.xlabel('时间')
        plt.ylabel('累计距离 (km)')
        plt.tight_layout()
        plt.savefig(str(imgpathtoday))
        plt.close()
        res_title = str(imgpathtoday).split("/")[-1]
        res_id = createresource(str(imgpathtoday), title=res_title)
        imglst.append([res_title, res_id])

    dsdays = ds.resample('D').sum()
    if not dsdays.empty:
        plt.figure(figsize=(10, 5))
        dsdays.plot()
        imgpathdays = dirmainpath / 'img' / 'gpsdays.png'
        touchfilepath2depth(imgpathdays)
        plt.title('每日移动距离')
        plt.xlabel('日期')
        plt.ylabel('移动距离 (km)')
        plt.tight_layout()
        plt.savefig(str(imgpathdays))
        plt.close()
        res_title = str(imgpathdays).split("/")[-1]
        res_id = createresource(str(imgpathdays), title=res_title)
        imglst.append([res_title, res_id])

    bodystr = "\n".join(f"![{son[0]}](:/{son[1]})" for son in imglst)
    deleteresourcesfromnote(loc_cloud_id)
    updatenote_body(loc_cloud_id, bodystr)


# %% [markdown]
# ### enhanced_visualization(df)

# %%
def enhanced_visualization(dfin: pd.DataFrame) -> pd.DataFrame:
    """综合可视化仪表盘"""

    # print("输入数据的前几行：")
    # print(dfin.head())
    # print("数据框信息：")
    # print(dfin.info())

    df = dfin.reset_index()
    # 数据清洗
    df.dropna(subset=['longi', 'lati'], inplace=True)
    df = df[(df['longi'].between(73.0, 135.0)) & (df['lati'].between(18.0, 54.0))]

    print("经过数据清洗后的数据：")
    print(df)

    if df.empty:
        print("数据框为空，无法生成图形。")
        return None, None

    # 计算移动速度和距离
    df['distance'] = df['longi'].shift().combine_first(df['longi']).diff()**2 + df['lati'].shift().combine_first(df['lati']).diff()**2
    df['distance'] = (df['distance'].pow(0.5) * 111.32)
    df['speed'] = df['distance'] / (df['jiange'].dt.seconds / 3600).replace(0, np.nan)

    # 处理时间数据
    df['hour'] = df['time'].dt.hour

    fig, axs = plt.subplots(3, 2, figsize=(15, 15))
    fig.suptitle('移动数据综合可视化仪表盘', fontsize=20)

    # 轨迹热力图
    hb = axs[0, 0].hexbin(df['longi'], df['lati'], gridsize=30, cmap='Blues')
    axs[0, 0].set_title('移动轨迹热力图')
    plt.colorbar(hb, ax=axs[0, 0], label='频率')

    # 时段活跃度
    hour_dist = df.groupby('hour').size().reset_index(name='counts')
    axs[0, 1].bar(hour_dist['hour'], hour_dist['counts'], color='orange')
    axs[0, 1].set_title('时段活跃度')

    # 移动距离分布
    axs[1, 0].hist(df['distance'], bins=20, color='green', alpha=0.7)
    axs[1, 0].set_title('单次移动距离分布')

    # 常去区域
    stay_points = df[df['distance'] < 0.1]
    axs[1, 1].scatter(stay_points['longi'], stay_points['lati'], c='red', alpha=0.5)
    axs[1, 1].set_title('常去区域')

    # 移动速度趋势
    axs[2, 0].plot(df['time'], df['speed'], color='purple')
    axs[2, 0].set_title('移动速度变化')

    axs[2, 1].axis('off')  # 隐藏最后一个子图

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])

    # 定义图像保存路径
    img_file = os.path.abspath(getdirmain() / 'img' / "location_dashboard.png")
    plt.savefig(img_file)  # 这里保存图像
    plt.close(fig)

    return df.set_index('time')


# %% [markdown]
# ### create_interactive_map(df)

# %%
def create_interactive_map(df: pd.DataFrame) -> folium.Map:
    """生成可交互的轨迹地图"""
    df = df.reset_index()
    m = folium.Map(
        location=[df['lati'].mean(), df['longi'].mean()],
        zoom_start=14,
        tiles='OpenStreetMap'  # 添加 tiles 参数，设置地图样式
    )
    
    # 添加轨迹线
    points = list(zip(df['lati'], df['longi']))
    folium.PolyLine(points, color='blue', weight=2.5, opacity=0.7).add_to(m)
    
    # 添加重要标记
    for idx, row in df[df['distance'] > 5].iterrows():  # 长距离移动点
        folium.Marker(
            [row['lati'], row['longi']],
            popup=f"时间：{row['time']}<br>距离：{row['distance']}km",
            icon=folium.Icon(color='red')
        ).add_to(m)
    
    # 生成热力图层
    from folium.plugins import HeatMap
    HeatMap(points, radius=15).add_to(m)
    
    outfile = os.path.abspath(getdirmain() / 'img' / 'trail_map.html')
    m.save(outfile)
    return m


# %% [markdown]
# ### calculate_metrics(df)

# %%
def calculate_metrics(df):
    """生成多维统计指标"""
    print("计算统计数据，检查输入数据：")
    print(df.head())

    stats = {
        'total_distance': df['distance'].sum(),
        'daily_avg': df.resample('D')['distance'].sum().mean(),
        'frequent_hour': df['hour'].mode()[0],
        'max_speed': df['speed'].max(),
        'stay_points': len(df[df['speed'] < 1])  # 速度<1km/h视为停留
    }

    # 生成统计面板
    stats_markdown = f"""
### 移动数据统计
- 总移动距离：{stats['total_distance']:.1f} km
- 日均移动：{stats['daily_avg']:.1f} km
- 最活跃时段：{stats['frequent_hour']:02d}:00-{stats['frequent_hour']+1:02d}:00
- 最高移动速度：{stats['max_speed']:.1f} km/h
- 重要停留点：{stats['stay_points']} 处
    """
    print("统计数据：")
    print(stats)

    return stats, stats_markdown


# %% [markdown]
# ### process_last_month_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, str]

# %%
# 增加一个处理最近一个月数据的函数
def process_last_month_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    """处理最近一个月的数据并生成统计信息和可视化"""
    # 计算最近一个月的时间范围
    # today = pd.Timestamp.today()
    # start_date = today - pd.DateOffset(months=1)
    start_date = df.index.max() - pd.DateOffset(months=1)

    # 过滤出最近一个月的数据
    last_month_data = df[(df.index >= start_date) & (df.index <= df.index.max())]
    
    # 计算统计信息
    if last_month_data.empty:
        print("最近一个月没有数据！")
        return None, "最近一个月没有数据。"

    stats = {
        'total_distance': last_month_data['distance'].sum(),
        'daily_avg': last_month_data.resample('D')['distance'].sum().mean(),
        'frequent_hour': last_month_data['hour'].mode()[0],
        'max_speed': last_month_data['speed'].max(),
        'stay_points': len(last_month_data[last_month_data['speed'] < 1])  # 速度<1km/h视为停留
    }

    stats_markdown = f"""
### 最近一个月的数据统计
- 总移动距离：{stats['total_distance']:.1f} km
- 日均移动：{stats['daily_avg']:.1f} km
- 最活跃时段：{stats['frequent_hour']:02d}:00-{stats['frequent_hour']+1:02d}:00
- 最高移动速度：{stats['max_speed']:.1f} km/h
- 重要停留点：{stats['stay_points']} 处
    """

    # 生成可视化
    fig, axs = plt.subplots(3, 2, figsize=(15, 15))
    fig.suptitle('最近一个月移动数据综合可视化', fontsize=20)

    # 轨迹热力图
    hb = axs[0, 0].hexbin(last_month_data['longi'], last_month_data['lati'], gridsize=30, cmap='Blues')
    axs[0, 0].set_title('最近一个月移动轨迹热力图')
    plt.colorbar(hb, ax=axs[0, 0], label='频率')

    # 时段活跃度
    hour_dist = last_month_data.groupby('hour').size().reset_index(name='counts')
    axs[0, 1].bar(hour_dist['hour'], hour_dist['counts'], color='orange')
    axs[0, 1].set_title('最近一个月时段活跃度')

    # 移动距离分布
    axs[1, 0].hist(last_month_data['distance'], bins=20, color='green', alpha=0.7)
    axs[1, 0].set_title('最近一个月单次移动距离分布')

    # 常去区域
    stay_points = last_month_data[last_month_data['distance'] < 0.1]
    axs[1, 1].scatter(stay_points['longi'], stay_points['lati'], c='red', alpha=0.5)
    axs[1, 1].set_title('最近一个月常去区域')

    # 移动速度趋势
    axs[2, 0].plot(last_month_data.index, last_month_data['speed'], color='purple')
    axs[2, 0].set_title('最近一个月移动速度变化')

    axs[2, 1].axis('off')  # 隐藏最后一个子图

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])

    # 定义图像保存路径
    img_file = os.path.abspath(getdirmain() / 'img' / "last_month_dashboard.png")
    plt.savefig(img_file)  # 这里保存图像
    plt.close(fig)

    return last_month_data, stats_markdown


# %% [markdown]
# ### publish_to_joplin(df)

# %%
def publish_to_joplin(df):
    """将分析结果发布到Joplin"""
    updated_df = enhanced_visualization(df)

    # 处理最近一个月的数据
    last_month_data, last_month_stats = process_last_month_data(updated_df)

    img_ids = []
    for img_file in ['location_dashboard.png', 'trail_map.html', 'last_month_dashboard.png']:
        res_id = createresource(str((getdirmain() / "img" / img_file).absolute()), img_file)
        img_ids.append(res_id)

    # 计算统计数据
    stats, stats_markdown = calculate_metrics(updated_df)  # 计算总体统计数据

    # 构建笔记内容
    body = f"""
{last_month_stats}
![最近一个月综合仪表盘](:/{img_ids[2]})

{stats_markdown}
![综合仪表盘](:/{img_ids[0]})
<iframe src=":/{img_ids[1]}" width="100%" height="500"></iframe>
    """
    
    # 更新或创建笔记
    note_id = getcfpoptionvalue('happyjp_life', 'hjloc', 'analytics_note')
    if not note_id:
        note_id = createnote(title="高级位置分析报告")
        setcfpoptionvalue('happyjp_life', 'hjloc', 'analytics_note', note_id)

    updatenote_body(note_id, body)


# %% [markdown]
# ## 主函数main

# %%
if __name__ == '__main__':
    """
    主函数：处理数据源，展示足迹，发布到 Joplin。
    """
    if not_IPython():
        log.info(f'运行文件\t{__file__}……')
    df = chuli_datasource()
    foot2show(df)
    publish_to_joplin(df)
    # showdis()
    if not_IPython():
        log.info(f"完成文件{__file__}\t的运行")
