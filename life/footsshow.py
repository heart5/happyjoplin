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
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import plotly.io as pio

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
    print(dstoday)
    if dstoday.shape[0] > 1:
        dstoday.plot()
        imgpathtoday = dirmainpath / 'img' / 'gpstoday.png'
        touchfilepath2depth(imgpathtoday)
        plt.tight_layout() # 紧缩排版，缩小默认的边距
        plt.savefig(str(imgpathtoday))
        plt.close()
        res_title = str(imgpathtoday).split("/")[-1]
        res_id = createresource(str(imgpathtoday), title=res_title)
        imglst.append([res_title, res_id])
    dsdays = ds.resample('D').sum()
    print(dsdays)
    dsdays.plot()
    imgpathdays = dirmainpath / 'img' / 'gpsdays.png'
    touchfilepath2depth(imgpathdays)
    plt.tight_layout() # 紧缩排版，缩小默认的边距
    plt.savefig(str(imgpathdays))
    plt.close()
    res_title = str(imgpathdays).split("/")[-1]
    res_id = createresource(str(imgpathdays), title=res_title)
    imglst.append([res_title, res_id])
    print(imglst)

    bodystr = "\n".join(f"![{son[0]}](:/{son[1]})" for son in imglst)
    deleteresourcesfromnote(loc_cloud_id)
    updatenote_body(loc_cloud_id, bodystr)


# %% [markdown]
# ### enhanced_visualization(df)

# %%
def enhanced_visualization(dfin: pd.DataFrame) -> Tuple[go.Figure, pd.DataFrame]:
    """综合可视化仪表盘"""

    df = dfin.reset_index()
    # 数据清洗: 去掉缺失值和不合理的记录
    df.dropna(subset=['longi', 'lati'], inplace=True)  # 去除缺失经纬度或时间的记录
    df = df[(df['longi'].between(73.0, 135.0)) & (df['lati'].between(18.0, 54.0))]  # 只保留在中国范围内的坐标

    # 计算移动速度和距离
    df['distance'] = df['longi'].shift().combine_first(df['longi']).diff()**2 + df['lati'].shift().combine_first(df['lati']).diff()**2
    df['distance'] = (df['distance'].pow(0.5) * 111.32)  # 将弧度转换为公里
    df['speed'] = df['distance'] / (df['jiange'].dt.seconds / 3600).replace(0, np.nan)  # 小时速度

    # 处理时间数据
    df['hour'] = df['time'].dt.hour
    df['weekday'] = df['time'].dt.weekday

    # 创建仪表盘
    fig = make_subplots(
        rows=3, cols=2,
        specs=[[{"type": "xy"}, {"type": "xy"}],
               [{"type": "xy"}, {"type": "scattergeo"}],
               [{"colspan": 2}, None]],
        subplot_titles=('移动轨迹热力图', '时段活跃度', '移动距离分布', '常去区域', '移动速度趋势')
    )

    # 轨迹热力图
    fig.add_trace(px.density_heatmap(
        df, x='longi', y='lati', z='hour',
        nbinsx=30, nbinsy=30,
        title='移动轨迹热力图'
    ).data[0], row=1, col=1)

    # 时段活跃度
    hour_dist = df.groupby('hour').size().reset_index(name='counts')
    fig.add_trace(px.bar(
        hour_dist, x='hour', y='counts',
        color='counts', title='时段分布'
    ).data[0], row=1, col=2)

    # 移动距离分布
    fig.add_trace(px.histogram(
        df, x='distance', nbins=20,
        title='单次移动距离分布'
    ).data[0], row=2, col=1)

    # 常去区域（中国地图）
    stay_points = df[df['distance'] < 0.1]
    fig.add_trace(px.scatter_geo(
        stay_points, lat='lati', lon='longi',
        color='hour', size='hour',
        projection="natural earth",
        title='常去区域',
        scope='asia',  # 设置为亚洲地图
        center={"lat": 35.0, "lon": 105.0},  # 中心点为中国
        # projection_type="natural earth",  # 设置投影类型
    ).data[0], row=2, col=2)

    # 移动速度趋势
    df['speed'] = df['distance'] / (df['jiange'].dt.seconds / 3600).replace(0, np.nan)  # 小时速度
    fig.add_trace(px.line(
        df, x='time', y='speed',
        title='移动速度变化'
    ).data[0], row=3, col=1)

    fig.update_layout(height=1200)
    outfile = os.path.abspath(getdirmain() / 'img' / "location_dashboard.html")
    pio.write_html(fig, outfile)
    return fig, df.set_index('time')


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
    return stats, stats_markdown


# %% [markdown]
# ### publish_to_joplin(df)

# %%
def publish_to_joplin(df):
    """将分析结果发布到Joplin"""
    # 生成所有可视化内容
    fig, updated_df = enhanced_visualization(df)
    create_interactive_map(updated_df)
    stats, stats_markdown = calculate_metrics(updated_df)
    
    # 转换图表为图片
    img_file = (getdirmain() / 'img' / 'dashboard.png').absolute() # Only include the Plotly image
    fig.write_image(img_file)
    img_ids = []
    for img_file in ['dashboard.png', 'trail_map.html']:
        res_id = createresource(str((getdirmain() /"img" / img_file).absolute()), img_file)
        img_ids.append(res_id)
    
    # 构建笔记内容
    body = f"""
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
