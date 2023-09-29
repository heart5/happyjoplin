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

# %%
import os
import re
import arrow
import pandas as pd
import matplotlib
import base64
import io
from tzlocal import get_localzone
from threading import Timer

from pylab import plt, FuncFormatter, mpl
# 设置显示中文字体
# mpl.rcParams["font.sans-serif"] = ["SimHei"]
mpl.rcParams["font.sans-serif"] = ["Microsoft YaHei"]

# %%
import pathmagic
with pathmagic.context():
    from func.first import getdirmain
    from func.datetimetools import datecn2utc
    from func.configpr import getcfpoptionvalue, setcfpoptionvalue
    from func.jpfuncs import getinivaluefromcloud, searchnotes, getnote, createnote, updatenote_imgdata, updatenote_body, updatenote_title
    from func.logme import log
    from func.wrapfuncs import timethis, ift2phone
    # from func.termuxtools import termux_location, termux_telephony_deviceinfo
    # from func.nettools import ifttt_notify
    from etc.getid import getdeviceid
    from func.sysfunc import not_IPython, set_timeout, after_timeout, execcmd


# %% [markdown]
# ## 功能函数集

# %% [markdown]
# ### gethealthdatafromnote(noteid)

# %%
def gethealthdatafromnote(noteid):
    healthnote = getnote(noteid)
    content = healthnote.body

    ptn = re.compile("(?###\s+)(\d{4}年\d{,2}月\d{,2}日)\n+(\d+)[,，](.+)\n*([^#]+)")
    itemslist = re.findall(ptn, content)
    itemslist0 = [[x.strip("\n") for x in item] for item in itemslist]

    def timestr2minutes(timestr):
        lst = re.split("[:：,，]", timestr)
        if len(lst) == 1:
            log.critical(f"时长字符串“{timestr}”格式有误，默认返回时长值为零")
            return 0
        else:
            return int(lst[0]) * int(lst[1])

    itemslist = [[datecn2utc(item[0]), int(item[1]), timestr2minutes(item[2]), item[3]] for item in itemslist0]

    columns=["date", "step", "sleep", "memo"]
    columns=["日期", "步数", "睡眠时长", "随记"]
    df = pd.DataFrame(itemslist, columns=columns).set_index("日期")
    print(df.dtypes)
    print(type(df.index))

    return df


# %% [markdown]
# ### hdf2imgbase64(hdf)

# %%
def hdf2imgbase64(hdf):
    plt.figure(figsize=(16, 20))
    ax1 = plt.subplot2grid((4, 2), (0, 0), colspan=2, rowspan=2)
    ax1.plot(hdf['步数'], lw=0.6, label=u'每天步数')
    junhdf = hdf['步数'].resample("7D").mean()
    ax1.plot(junhdf, lw=1, label=u'七天日均')
    # 标注数据点
    for i in range(len(junhdf.index)):
        plt.annotate(f'({int(junhdf.iloc[i])})', (junhdf.index[i], junhdf.iloc[i]), textcoords="offset points", xytext=(0,10), ha='center')
    plt.legend(loc=1)
    plt.title("步数动态图")

    # Convert the plot to a base64 encoded image
    buffer = io.BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)
    image_base64 = base64.b64encode(buffer.read()).decode()
    # Now, 'image_base64' contains the base64 encoded image
    # Close the plot to free up resources
    plt.close()

    # from IPython.display import Image
    # buffer.seek(0)
    # img1 = Image(data=buffer.read())
    # img1

    return image_base64


# %% [markdown]
# ### health2note()

# %%
def health2note():
    namestr = "happyjp_life"
    section = "health"
    if not (health_id := getcfpoptionvalue(namestr, section, 'health_cloud_id')):
        findhealthnotes = searchnotes("title:健康运动笔记")
        if len(findhealthnotes) == 0:
            log.critical(f"标题为《健康运动笔记》的笔记貌似不存在，请按照规定格式构建之！退出先！！！")
            exit(1)
        healthnote = findhealthnotes[0]
        health_id = healthnote.id
        setcfpoptionvalue(namestr, section, 'health_cloud_id', f"{health_id}")
    # 在happyjp_life配置文件中查找health_cloud_updatetimestamp，找不到则表示首次运行，置零
    if not (health_cloud_updatetimestamp := getcfpoptionvalue(namestr, section, 'health_cloud_updatetimestamp')):
        health_cloud_updatetimestamp = 0
    note = getnote(health_id)
    noteupdatetimewithzone = arrow.get(note.updated_time, tzinfo=get_localzone())
    if noteupdatetimewithzone.timestamp() == health_cloud_updatetimestamp:
        log.info(f'健康运动笔记无更新【最新更新时间为：{noteupdatetimewithzone}】，跳过本次轮询和相应动作。')
        return

    hdf = gethealthdatafromnote(note.id)
    image_base64 = hdf2imgbase64(hdf)
    if not (healthstat_id := getcfpoptionvalue(namestr, section, 'healthstat_cloud_id')):
        healthnotefindlist = searchnotes("title:健康动态") 
        if (len(healthnotefindlist) == 0):
            healthstat_note_id = createnote(title="健康动态新生活", imgdata64=image_base64)
            log.info(f"新的健康动态笔记“{ealthstat_note_id}”新建成功！")
        else:
            healthstat_note_id = healthnotefindlist[-1].id
        setcfpoptionvalue(namestr, section, 'healthstat_cloud_id', f"{healthstat_id}")

    updatenote_imgdata(noteid=healthstat_note_id, imgdata=image_base64)
    setcfpoptionvalue(namestr, section, 'health_cloud_updatetimestamp', str(noteupdatetimewithzone.timestamp()))


# %% [markdown]
# ## 主函数main()

# %%
if __name__ == '__main__':
    if not_IPython():
        log.info(f'开始运行文件\t{__file__}')
    # log2notes()
    
    health2note()
    if not_IPython():
        log.info(f'Done.结束执行文件\t{__file__}')