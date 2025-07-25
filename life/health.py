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

# %% [markdown]
# ### 核心库

# %%
import re
import arrow
import pandas as pd
import base64
import io
import calendar

# %%
import pathmagic

with pathmagic.context():
    from func.datetimetools import datecn2utc
    from func.configpr import getcfpoptionvalue, setcfpoptionvalue
    from func.jpfuncs import searchnotes, getnote, createnote, updatenote_imgdata, noteid_used, searchnotebook
    from func.logme import log
    from func.wrapfuncs import timethis
    from etc.getid import getdeviceid, gethostuser
    from func.sysfunc import not_IPython, execcmd

# %% [markdown]
# ### 中文显示预置

# %%
# from pylab import plt, FuncFormatter, mpl
# 设置显示中文字体
# mpl.rcParams["font.sans-serif"] = ["SimHei"]
# mpl.rcParams["font.sans-serif"] = ["Microsoft YaHei"]
import matplotlib.pyplot as plt
# plt.rcParams["font.family"] = "sans-serif"

# %% [markdown]
# ## 功能函数集

# %% [markdown]
# ### gethealthdatafromnote(noteid)
#


# %%
def gethealthdatafromnote(noteid):
    """
    从指定id的运动笔记获取数据，处理，并输出标准DataFrame
    """
    healthnote = getnote(noteid)
    content = healthnote.body

    ptn = re.compile(r"(?###\s+)(\d{4}年\d{,2}月\d{,2}日)\n+(\d+)[,，](.+)\n*([^#]+)")
    itemslist = re.findall(ptn, content)
    itemslist0 = [[x.strip("\n") for x in item] for item in itemslist]

    def timestr2minutes(timestr):
        lst = re.split("[:：,，]", timestr)
        if len(lst) == 1:
            log.critical(f"时长字符串“{timestr}”格式有误，默认返回时长值为零")
            return 0
        else:
            if lst[0].isdecimal() & lst[1].isdecimal():
                return int(lst[0]) * 60 + int(lst[1])
            else:
                log.critical(f"时长字符串“{timestr}”格式有误，默认返回时长值为零")
                return 0

    itemslist = [[datecn2utc(item[0]), int(item[1]), timestr2minutes(item[2]), item[3]] for item in itemslist0]

    columns = ["date", "step", "sleep", "memo"]
    columns = ["日期", "步数", "睡眠时长", "随记"]
    df = pd.DataFrame(itemslist, columns=columns).set_index("日期")
    print(df.dtypes)
    print(type(df.index))

    return df


# %% [markdown]
# ### calds2ds(sds)


# %%
def calds2ds(sds):
    """
    根据输入的ds，按月合计并估算数据未满月的月份的整月值
    返回：月度合计ds、头尾估算合计ds
    """
    sdsm_actual = sds.resample("m").sum()

    dmin = sds.index.min()
    year = dmin.year
    month = dmin.month
    __, monthend = calendar.monthrange(year, month)
    estimatemin = int(sdsm_actual.iloc[0] / (monthend + 1 - dmin.day) * monthend)
    print(year, month, monthend, dmin.day, sdsm_actual.iloc[0], estimatemin)

    dmax = sds.index.max()
    year = dmax.year
    month = dmax.month
    __, monthend = calendar.monthrange(year, month)
    estimatemax = int(sdsm_actual.iloc[-1] / (dmax.day) * monthend)
    print(year, month, monthend, dmax.day, sdsm_actual.iloc[-1], estimatemax)

    estds = pd.Series([estimatemin, estimatemax], index=[dmin, dmax])

    estds_resample_full = estds.resample("m").sum()
    return sdsm_actual, estds_resample_full


# %% [markdown]
# ### hdf2imgbase64(hdf)


# %%
def hdf2imgbase64(hdf):
    """
    根据传入包含运动数据的DataFrame作图，并输出图形的bytes
    """

    plt.figure(figsize=(15, 30), dpi=100)

    ax1 = plt.subplot2grid((4, 2), (0, 0), colspan=2, rowspan=1)
    ax1.plot(hdf["步数"], lw=0.6, label="每天步数")
    junhdf = hdf["步数"].resample("7D").mean()
    ax1.plot(junhdf, lw=1, label="七天日均")
    # 标注数据点
    for i in range(len(junhdf.index)):
        ax1.annotate(
            f"({int(junhdf.iloc[i])})",
            (junhdf.index[i], junhdf.iloc[i]),
            textcoords="offset points",
            xytext=(0, 10),
            ha="center",
        )
    # ax1.legend(loc=1)
    ax1.legend()
    ax1.set_title("步数动态图")

    ax2 = plt.subplot2grid((4, 2), (1, 0), colspan=2, rowspan=1)
    sdsm_actual, sdsm_estimate_full = calds2ds(hdf["步数"])
    axsub = sdsm_actual.plot(kind="bar")
    sdsm_estimate_full.plot(kind="bar", linestyle="-.", edgecolor="green", fill=False, ax=axsub)
    # 标注数据点
    for i, v in enumerate(sdsm_actual):
        axsub.text(i, v, str(v), ha="center", va="bottom")
        if (val := sdsm_estimate_full.iloc[i]) != 0:
            axsub.text(i, val, str(val), ha="center", va="bottom")
    # 设置横轴刻度显示
    axsub.set_xticklabels([x.strftime("%Y-%m") for x in sdsm_actual.index], rotation=20)
    # ax2.legend(loc=1)
    ax2.legend(["步数", "整月估算"])
    ax2.set_title("月度步数图")

    ax3 = plt.subplot2grid((4, 2), (2, 0), colspan=2, rowspan=1)
    ax3.plot(hdf["睡眠时长"], lw=0.6, label="睡眠时长")
    sleepjundf = hdf["睡眠时长"].resample("7D").mean()
    ax3.plot(sleepjundf, lw=1, label="七天平均")
    # 标注数据点
    for i in range(len(sleepjundf.index)):
        plt.annotate(
            f"({int(sleepjundf.iloc[i] / 60)}钟{int(sleepjundf.iloc[i] % 60)}分)",
            (sleepjundf.index[i], sleepjundf.iloc[i]),
            textcoords="offset points",
            xytext=(0, 10),
            ha="center",
        )
    # ax3.legend(loc=1)
    ax3.legend()
    ax3.set_title("睡眠时长动态图（分钟）")

    ax4 = plt.subplot2grid((4, 2), (3, 0), colspan=2, rowspan=1)
    sdsm_actual, sdsm_estimate_full = calds2ds(hdf["睡眠时长"])
    axsub = sdsm_actual.plot(kind="bar")
    sdsm_estimate_full.plot(kind="bar", linestyle="-.", edgecolor="green", fill=False, ax=axsub)
    # 标注数据点
    for i, v in enumerate(sdsm_actual):
        axsub.text(i, v, str(v), ha="center", va="bottom")
        if (val := sdsm_estimate_full.iloc[i]) != 0:
            axsub.text(i, val, str(val), ha="center", va="bottom")
    # 设置横轴刻度显示
    axsub.set_xticklabels([x.strftime("%Y-%m") for x in sdsm_actual.index], rotation=20)
    # ax4.legend(loc=1)
    ax4.legend(["睡眠时长", "整月估算"])
    ax4.set_title("月度睡眠时长图（分钟）")

    # convert the plot to a base64 encoded image
    buffer = io.BytesIO()
    plt.savefig(buffer, format="png")
    buffer.seek(0)
    image_base64 = base64.b64encode(buffer.read()).decode()
    # now, 'image_base64' contains the base64 encoded image
    # close the plot to free up resources
    plt.show()
    plt.close()

    # from IPython.display import Image
    # buffer.seek(0)
    # img1 = Image(data=buffer.read())
    # img1
    log.info(f"生成图片的大小为\t{len(image_base64)}\t字节")

    return image_base64


# %% [markdown]
# ### health2note()


# %%
@timethis
def health2note():
    """
    综合输出健康动态图并更新至笔记
    """

    login_user = execcmd("whoami")
    namestr = "happyjp_life"
    section = f"health_{login_user}"
    notestat_title = f"健康动态日日升【{gethostuser()}】"
    if not (health_id := getcfpoptionvalue(namestr, section, "health_cloud_id")):
        findhealthnotes = searchnotes("title:健康运动笔记")
        if len(findhealthnotes) == 0:
            log.critical("标题为《健康运动笔记》的笔记貌似不存在，请按照规定格式构建之！退出先！！！")
            exit(1)
        healthnote = findhealthnotes[0]
        health_id = healthnote.id
        setcfpoptionvalue(namestr, section, "health_cloud_id", f"{health_id}")
    # 在happyjp_life配置文件中查找health_cloud_updatetimestamp，找不到则表示首次运行，置零
    if not (health_cloud_update_ts := getcfpoptionvalue(namestr, section, "health_cloud_updatetimestamp")):
        health_cloud_update_ts = 0
    note = getnote(health_id)
    noteupdatetimewithzone = arrow.get(note.updated_time, tzinfo="local")
    # IPyton环境无视对比判断，强行执行后续操作；非IPython环境则正常逻辑推进
    if (noteupdatetimewithzone.timestamp() == health_cloud_update_ts) and (not_IPython()):
        log.info(f"健康运动笔记无更新【最新更新时间为：{noteupdatetimewithzone}】，跳过本次轮询和相应动作。")
        return

    hdf = gethealthdatafromnote(note.id)
    image_base64 = hdf2imgbase64(hdf)
    nbid = searchnotebook("康健")
    if not (healthstat_cloud_id := getcfpoptionvalue(namestr, section, "healthstat_cloud_id")):
        healthnotefindlist = searchnotes(f"title:{notestat_title}")
        if len(healthnotefindlist) == 0:
            healthstat_cloud_id = createnote(title=notestat_title, parent_id=nbid, imgdata64=image_base64)
            log.info(f"新的健康动态笔记“{healthstat_cloud_id}”新建成功！")
        else:
            healthstat_cloud_id = healthnotefindlist[-1].id
        setcfpoptionvalue(namestr, section, "healthstat_cloud_id", f"{healthstat_cloud_id}")

    if not noteid_used(healthstat_cloud_id):
        healthstat_cloud_id = createnote(title=notestat_title, parent_id=nbid, imgdata64=image_base64)
    else:
        healthstat_cloud_id, res_lst = updatenote_imgdata(
            noteid=healthstat_cloud_id, parent_id=nbid, imgdata64=image_base64
        )
    setcfpoptionvalue(namestr, section, "healthstat_cloud_id", f"{healthstat_cloud_id}")
    setcfpoptionvalue(namestr, section, "health_cloud_updatetimestamp", str(noteupdatetimewithzone.timestamp()))
    log.info(
        f"健康运动笔记【更新时间：{arrow.get(health_cloud_update_ts, tzinfo='local')}-》{noteupdatetimewithzone}】。"
    )


# %% [markdown]
# ## 主函数main()

# %%
if __name__ == "__main__":
    if not_IPython():
        log.info(f"开始运行文件\t{__file__}")

    health2note()

    if not_IPython():
        log.info(f"Done.结束执行文件\t{__file__}")
