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
    targetdict = {k: v for k, v in note_monitor.monitored_notes.items() if person in v['title']}
    # print(targetdict)
    title_count_dict = {}
    for note_id, note_info in targetdict.items():
        daily_counts = {}
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
    图形化输出函数
    """
    # 将字典转换为 DataFrame，方便处理
    df = pd.DataFrame(list(daily_counts.items()), columns=['date', 'count'])
    df['date'] = pd.to_datetime(df['date'])

    # 添加年份和星期几列
    df['year'] = df['date'].dt.year
    df['week'] = df['date'].dt.isocalendar().week
    df['day_of_week'] = df['date'].dt.weekday

    # 确保数据正确处理跨年周数
    df.loc[(df['week'] == 1) & (df['date'].dt.month == 12), 'week'] = 53

    # 获取最小日期和当前日期
    min_date = df['date'].min()
    current_date = pd.to_datetime(datetime.now().date())

    # 创建一个透视表，确保缺失值填充为零
    pivot_table = df.pivot_table(index='week', columns='day_of_week', values='count', aggfunc='sum', fill_value=0)

    # 自定义颜色映射
    white = 'white'
    warning_color = '#FFD700'  # 暗黄色
    greens = list(plt.cm.Greens(np.linspace(0.3, 1, 254)))
    colors = [white, warning_color] + greens
    cmap = mcolors.ListedColormap(colors)

    # 设置边界，确保 count 为零时使用警示颜色
    boundaries = [0, 1] + list(np.linspace(1.01, df['count'].max(), 255))
    norm = mcolors.BoundaryNorm(boundaries=boundaries, ncolors=cmap.N, clip=True)

    # 创建图形
    fig, ax = plt.subplots(figsize=(15, 10))
    
    # 绘制热图
    heatmap = ax.pcolor(pivot_table.values, cmap=cmap, norm=norm, edgecolors='white', linewidths=2)

    # 设置刻度
    ax.set_xticks(np.arange(7) + 0.5, minor=False)
    ax.set_yticks(np.arange(len(pivot_table.index)) + 0.5, minor=False)

    # 标签
    ax.set_xticklabels(['一', '二', '三', '四', '五', '六', '日'], minor=False)
    ax.set_yticklabels(pivot_table.index, minor=False)

    # 反转y轴
    ax.invert_yaxis()

    # 添加颜色条
    cbar = plt.colorbar(heatmap)
    cbar.set_label('更新字数')
    
    plt.title(title)

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
    # person = '耿华忠'
    for person in person_lst:
        targetdict = {k: v for k, v in note_monitor.monitored_notes.items() if person in v['title']}
        should_plot = False
        for note_id, note_info in targetdict.items():
            if getattr(getnote(note_id), 'updated_time') != note_info['note_update_time']:
                should_plot = True
                break
        if should_plot:
            heatmap_id = get_heatmap_note_id(person)
            oldnote = getnote(heatmap_id)
            res_ids = [re.search(r'\(:/(.+)\)', link).group(1) for link in getattr(oldnote, "body").split()]
            print(f"笔记《{getattr(oldnote, 'title')}》中包含以下资源文件：{res_ids}，不出意外将被删除清理！")
            jpapi = getapi()
            for resid in res_ids:
                try:
                    jpapi.delete_resource(resid)
                except Exception as e:
                    print(e)

            newbodystr = ""
            for k, v in stat2df(person).items():
                buffer = plot_word_counts(v, k)
                title=f"{k}-{person}"
                res_id = createresourcefromobj(buffer, title=title)
                newbodystr += f"![{title}](:/{res_id})" + "\n"
            updatenote_body(heatmap_id, newbodystr)


# %% [markdown]
# ## 主函数，__main__

# %%
if __name__ == '__main__':
    if not_IPython():
        log.info(f'开始运行文件\t{__file__}')

    heatmap2note()

    if not_IPython():
        log.info(f'Done.结束执行文件\t{__file__}')

