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
import re
import json
import base64
import io
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.dates as mdates
from pathlib import Path
from datetime import datetime, timedelta, date

# %%
import pathmagic
with pathmagic.context():
    from func.first import getdirmain
    from func.configpr import getcfpoptionvalue, setcfpoptionvalue
    from func.logme import log
    from work.monitor4 import NoteMonitor
    # from func.termuxtools import termux_location, termux_telephony_deviceinfo
    # from func.nettools import ifttt_notify
    # from etc.getid import getdevicename, gethostuser
    from func.sysfunc import not_IPython, set_timeout, after_timeout, execcmd
    from func.jpfuncs import getapi, getnote, searchnotes, createnote, updatenote_body, createresourcefromobj


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
    targetdict = {k: v for k, v in note_monitor.monitored_notes.items() if person == v['person']}
    # print(targetdict)
    title_count_dict = {}
    for note_id, note_info in targetdict.items():
        daily_counts = {} # 采用字典数据类型，确保日期唯一
        for date_key, updates in note_info['content_by_date'].items():
            # 获取最后一次更新的字数
            last_update_time, word_count = updates[-1]
            daily_counts[date_key] = word_count  # 使用日期作为键，字数作为值
        outlst = sorted(daily_counts.items(), key=lambda kv: kv[0])
        title_count_dict[note_info['title']] = dict(outlst)
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
      包含生成的 PNG 图像的 io.BytesIO 对象。
    """

    # 1. 数据预处理
    df = pd.DataFrame(list(daily_counts.items()), columns=['date', 'count'])
    df['date'] = pd.to_datetime(df['date'])

    # 2. 确定最小日期和当前日期所在周的第一天和最后一天
    min_date = df['date'].min()
    current_date = pd.to_datetime(datetime.now().date())
    # print(min_date, type(min_date), current_date, type(current_date))
    min_week_start = min_date - timedelta(days=min_date.weekday())
    current_week_end = current_date + timedelta(days=6 - current_date.weekday())

    # 3. 创建完整日期范围，包含补齐的日期
    all_dates = pd.date_range(start=min_week_start, end=current_week_end)
    all_dates_df = pd.DataFrame({'date': all_dates, 'count': -1})

    # 4. 合并数据，保留原始 count 值
    df = pd.concat([df, all_dates_df], ignore_index=True)
    df = df.drop_duplicates(subset=['date'], keep='first')
    df = df.sort_values(by='date').reset_index(drop=True)

    # 5. 添加年份和星期几列
    df['year'] = df['date'].dt.year
    df['week'] = df['date'].dt.isocalendar().week
    df['day_of_week'] = df['date'].dt.weekday

    # 6. 确保数据正确处理跨年周数
    df.loc[(df['week'] == 1) & (df['date'].dt.month == 12), 'week'] = 53

    # 7. 创建透视表
    pivot_table = df.pivot_table(
        index='week', columns='day_of_week', values='count', aggfunc='sum', fill_value=-1
    )

    # 8. 自定义颜色映射
    white = 'white'
    warning_color = '#FFD700' 

    # --- Dynamically determine the number of color bins ---
    max_count = int(df['count'].max())
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
    fig, ax = plt.subplots(figsize=(15, 10))

    # 11. 绘制热图
    heatmap = ax.pcolor(
        pivot_table.values, cmap=cmap, norm=norm, edgecolors='white', linewidths=2
    )

    # 12. 设置刻度和标签
    ax.set_xticks(np.arange(7) + 0.5, minor=False)
    ax.set_yticks(np.arange(len(pivot_table.index)) + 0.5, minor=False)
    ax.set_xticklabels(['一', '二', '三', '四', '五', '六', '日'], minor=False)
    ax.set_yticklabels(pivot_table.index, minor=False)

    # 13. 反转 y 轴
    ax.invert_yaxis()

    # 14. 添加颜色条
    cbar = plt.colorbar(heatmap)
    cbar.set_label('更新字数')

    # 15. 设置标题
    plt.title(title)

    # --- 添加起始日期和当前日期标记 ---
    # 找到最小日期和当前日期在热图中的位置
    min_date_week = df[df['date'] == min_date]['week'].iloc[0]
    print(df[df['date'] == min_date])
    min_date_weekday = df[df['date'] == min_date]['day_of_week'].iloc[0]
    print(df[df['date'] == current_date])
    current_date_week = df[df['date'] == current_date]['week'].iloc[0]
    current_date_weekday = df[df['date'] == current_date]['day_of_week'].iloc[0]

    # 在最小日期的格子中添加日期文本
    ax.text(
        min_date_weekday + 0.5,
        min_date_week - min(pivot_table.index) + 0.5,
        min_date.strftime('%m-%d'),
        ha='center',
        va='center',
        color='black',
        fontsize=8,
    )

    # 为当前日期的格子添加红色虚线边框
    ax.add_patch(
        plt.Rectangle(
            (current_date_weekday, current_date_week - min(pivot_table.index)),
            1,
            1,
            fill=False,
            edgecolor='red',
            linestyle='--',
            linewidth=2,
        )
    )
    # --- 标记添加完成 ---

    # 16. 将图像保存到 BytesIO 对象并返回
    buffer = io.BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)
    plt.close(fig)

    return buffer


# %% [markdown]
# ### get_heatmap_note_id(person)

# %%
def get_heatmap_note_id(person):
    # 查找指定人员热图笔记的id并返回
    if (person_heatmap_id := getcfpoptionvalue("happyjpmonitor", "person_ids", person)) is None:
        results = searchnotes(f"title:四件套更新热图（{person}）")
        if results:
            person_heatmap_id = results[0].id
        else:
            person_heatmap_id = createnote(title=f"四件套更新热图（{person}）", body="热图笔记已创建。")
        setcfpoptionvalue('happyjpmonitor', 'person_ids', person, person_heatmap_id)

    return person_heatmap_id


# %% [markdown]
# ### heatmap2note()

# %%
def heatmap2note():
    # 监控笔记
    note_monitor = NoteMonitor()
    ptn = re.compile(r"[(（](\w+)[)）]")
    person_lst = list(set([re.findall(ptn, info['title'])[0] for note_id, info in note_monitor.monitored_notes.items()]))
    print(person_lst)
    jpapi = getapi()
    for person in person_lst:
        # 筛选出指定person相关的{note_id:note_info}字典
        targetdict = {k: v for k, v in note_monitor.monitored_notes.items() if person in v['title']}
        should_plot = False
        for note_id, note_info in targetdict.items():
            if len(note_info['content_by_date']) == 0:
                log.info(f"笔记《{note_info['title']}》的有效日期内容为空，跳过")
                continue
            if (person_note_update_time := getcfpoptionvalue("happyjpmonitor", "note_update_time", note_id)) != note_info["note_update_time"].strftime('%Y-%m-%d %H:%M:%S') or (person_note_update_time != getattr(getnote(note_id), "updated_time").strftime('%Y-%m-%d %H:%M:%S')):
                should_plot = True
        if should_plot:
            heatmap_id = get_heatmap_note_id(person)
            oldnote = getnote(heatmap_id)

            newbodystr = ""
            for k, v in stat2df(person).items():
                buffer = plot_word_counts(v, k)
                title=f"{k}-{person}"
                res_id = createresourcefromobj(buffer, title=title)
                newbodystr += f"![{title}](:/{res_id})" + "\n"

            updatenote_body(heatmap_id, newbodystr)
            # 操作成功后再删除原始笔记中的资源文件
            res_ids = [re.search(r'\(:/(.+)\)', link).group(1) for link in getattr(oldnote, "body").split()]
            print(f"笔记《{getattr(oldnote, 'title')}》中包含以下资源文件：{res_ids}，不出意外将被删除清理！")
            for resid in res_ids:
                try:
                    jpapi.delete_resource(resid)
                except Exception as e:
                    print(e)
        # 操作成功后再setcfpoptionvalue
        for note_id, note_info in targetdict.items():
            if (person_note_update_time := getcfpoptionvalue("happyjpmonitor", "note_update_time", note_id)) != note_info["note_update_time"].strftime('%Y-%m-%d %H:%M:%S'):
                setcfpoptionvalue('happyjpmonitor', 'note_update_time', note_id, note_info["note_update_time"].strftime('%Y-%m-%d %H:%M:%S'))


# %% [markdown]
# ## 主函数，__main__

# %%
if __name__ == '__main__':
    if not_IPython():
        log.info(f'开始运行文件\t{__file__}')

    heatmap2note()

    if not_IPython():
        log.info(f'Done.结束执行文件\t{__file__}')

