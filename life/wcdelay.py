# encoding:utf-8
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
# # 微信聊天信息延迟管理

# %%
"""
微信延迟管理文件
"""

# %% [markdown]
# ## 引入重要库

# %%
import base64
import io
import os

# import datetime
import sqlite3 as lite
import time

import matplotlib.pyplot as plt
import pandas as pd
from pandas.plotting import register_matplotlib_converters

# %%
import pathmagic

with pathmagic.context():
    from func.configpr import getcfpoptionvalue, setcfpoptionvalue
    from func.first import getdirmain, touchfilepath2depth
    from func.jpfuncs import createnote, updatenote_imgdata
    from func.litetools import ifnotcreate
    from func.logme import log
    from func.sysfunc import not_IPython


# %% [markdown]
# ## 功能函数集

# %% [markdown]
# ### def checkdelaytable(dbname, tablename)


# %%
def checkwcdelaytable(dbname: str, tablename: str):
    """
    检查和dbname（绝对路径）相对应的延时数据表是否已经构建，设置相应的ini值避免重复打开关闭数据库文件进行检查
    """
    if (
        wcdelaycreated := getcfpoptionvalue(
            "everwebchat", os.path.abspath(dbname), tablename
        )
    ) is None:
        print(wcdelaycreated)
        csql = f"create table if not exists {tablename} (id INTEGER PRIMARY KEY AUTOINCREMENT, msgtime int, delay int)"
        ifnotcreate(tablename, csql, dbname)
        setcfpoptionvalue("everwebchat", os.path.abspath(dbname), tablename, str(True))
        logstr = f"数据表{tablename}在数据库{dbname}中构建成功"
        log.info(logstr)


# %% [markdown]
# ### def inserttimeitem2db(dbname, timestampinput)


# %%
def inserttimeitem2db(dbname: str, timestampinput: int):
    """
    insert timestamp to wcdelay db whose table name is wcdelay
    """
    tablename = "wcdelaynew"
    checkwcdelaytable(dbname, tablename)

    # timetup = time.strptime(timestr, "%Y-%m-%d %H:%M:%S")
    # timest = time.mktime(timetup)
    elsmin = (int(time.time()) - timestampinput) // 60
    conn = False
    try:
        conn = lite.connect(dbname)
        cursor = conn.cursor()
        cursor.execute(
            f"insert into {tablename} (msgtime, delay) values(?, ?)",
            (timestampinput, elsmin),
        )
        #         print(f"数据成功写入{dbname}\t{(timestampinput, elsmin)}")
        conn.commit()
    except Exception as e:
        logstr = f"数据库文件{dbname}存取错误！{e}"
        log.critical(logstr)
    finally:
        if conn:
            conn.close()


# %% [markdown]
# ### def getdelaydb(dbname, tablename)


# %%
def getdelaydb(dbname: str, tablename="wcdelaynew"):
    """
    从延时数据表提取数据（DataFrame），返回最近延时值和df
    """
    #     tablename = "wcdelaynew"
    checkwcdelaytable(dbname, tablename)

    conn = lite.connect(dbname)
    cursor = conn.cursor()
    cursor.execute(f"select * from {tablename}")
    table = cursor.fetchall()
    conn.close()

    tmpdf = pd.DataFrame(table)
    if len(tmpdf.columns) == 3:
        timedf = pd.DataFrame(table, columns=["id", "time", "delay"])
        timedf = timedf.set_index("id")
        timedf["time"] = timedf["time"].apply(
            lambda x: pd.to_datetime(
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(x))
            )
        )
        timedfgrp = timedf.groupby("time").sum()
    #     timedf.set_index("time", inplace=True)
    elif len(tmpdf.columns) == 2:
        timedf = pd.DataFrame(table, columns=["time", "delay"])
        timedf["time"] = timedf["time"].apply(
            lambda x: pd.to_datetime(
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(x))
            )
        )
        timedfgrp = timedf.set_index("time")
    else:
        return 0, tmpdf

    if (tdfsize := timedfgrp.shape[0]) != 0:
        print(f"延时记录{type(timedfgrp)}共有{tdfsize}条")
        # 增加当前时间，延时值引用最近一次的值，用于做图形展示的右边栏
        #         nowtimestamp = time.ctime()
        #         timedf = timedf.append(pd.DataFrame([timedf.iloc[-1]],
        # index=[pd.to_datetime(time.ctime())]))
        timedfgrp = pd.concat(
            [
                timedfgrp,
                pd.DataFrame(
                    [timedfgrp.iloc[-1]], index=[pd.to_datetime(time.ctime())]
                ),
            ]
        )
        jujinmins = int(
            (timedfgrp.index[-1] - timedfgrp.index[-2]).total_seconds() / 60
        )
    else:
        jujinmins = 0
        logstr = f"数据表{tablename}还没有数据呢"
        log.info(logstr)

    timedfgrp.loc[timedfgrp.delay < 0] = 0
    # print(timedf.iloc[:2])
    print(timedf.iloc[-3:])

    return jujinmins, timedfgrp


# %% [markdown]
# ### def showdelayimg(dbname, jingdu)


# %%
def showdelayimg(dbname: str, jingdu: int = 300):
    """
    show the img for wcdelay
    """
    jujinm, timedf = getdelaydb(dbname)
    #     timedf.iloc[-1]
    print(f"记录新鲜度：出炉了{jujinm}分钟")

    register_matplotlib_converters()

    plt.figure(figsize=(36, 12))
    plt.style.use("ggplot")  # 使得作图自带色彩，这样不用费脑筋去考虑配色什么的；

    def drawdelayimg(pos, timedfinner, title):
        # 画出左边界
        tmin = timedfinner.index.min()
        tmax = timedfinner.index.max()
        shicha = tmax - tmin
        bianjie = int(shicha.total_seconds() / 40)
        print(f"左边界：{bianjie}秒，也就是大约{int(bianjie / 60)}分钟")
        # plt.xlim(xmin=tmin-pd.Timedelta(f'{bianjie}s'))
        plt.subplot(pos)
        plt.xlim(xmin=tmin)
        plt.xlim(xmax=tmax + pd.Timedelta(f"{bianjie}s"))
        # plt.vlines(tmin, 0, int(timedf.max() / 2))
        plt.vlines(tmax, 0, int(timedfinner.max() / 2))

        # 绘出主图和标题
        plt.scatter(timedfinner.index, timedfinner, s=timedfinner)
        plt.scatter(
            timedfinner[timedfinner == 0].index, timedfinner[timedfinner == 0], s=0.5
        )
        plt.title(title, fontsize=40)
        plt.tick_params(labelsize=20)
        plt.tight_layout()

    drawdelayimg(
        211,
        timedf[timedf.index > timedf.index.max() + pd.Timedelta("-2d")],
        "信息频率和延时（分钟，最近两天）",
    )
    drawdelayimg(212, timedf, "信息频率和延时（分钟，全部）")
    fig1 = plt.gcf()

    # convert the plot to a base64 encoded image
    buffer = io.BytesIO()
    plt.savefig(buffer, format="png", dpi=jingdu)
    buffer.seek(0)
    image_base64 = base64.b64encode(buffer.read()).decode()
    # now, 'image_base64' contains the base64 encoded image
    # close the plot to free up resources

    # plt.show()
    plt.close()

    imgwcdelaypath = touchfilepath2depth(
        getdirmain() / "img" / "webchat" / "wcdelay.png"
    )

    with open(imgwcdelaypath, "wb") as f:
        buffer.seek(0)
        f.write(buffer.read())
    # fig1.savefig(imgwcdelaypath, dpi=jingdu)
    print(os.path.relpath(imgwcdelaypath))

    return imgwcdelaypath, image_base64


# %% [markdown]
# ### delayimg2note(image_base64)


# %%
def delayimg2note(owner):
    dbnameouter = touchfilepath2depth(
        getdirmain() / "data" / "db" / f"wcdelay_{owner}.db"
    )
    imgpath, image_base64 = showdelayimg(dbnameouter)
    if (delayid := getcfpoptionvalue("happyjpwebchat", "delay", "noteid")) is None:
        delayid = createnote(
            title=f"微信信息延迟动态图（{owner}）", imgdata64=image_base64
        )
        setcfpoptionvalue("happyjpwebchat", "delay", "noteid", str(delayid))
        return
    noteid, residlst = updatenote_imgdata(noteid=delayid, imgdata64=image_base64)
    setcfpoptionvalue("happyjpwebchat", "delay", "noteid", str(noteid))


# %% [markdown]
# ## 主函数main

# %%
if __name__ == "__main__":
    if not_IPython():
        logstrouter = "运行文件\t%s" % __file__
        log.info(logstrouter)
    # owner = 'heart5'
    owner = "白晔峰"
    # dbnameouter = touchfilepath2depth(getdirmain() / "data" / "db" / f"wcdelay_{owner}.db")
    # xinxian, tdf = getdelaydb(dbnameouter)
    # print(tdf.sort_index(ascending=False))
    # imgpath, image_base64 = showdelayimg(dbnameouter)
    delayimg2note(owner)
    if not_IPython():
        logstrouter = "文件%s运行结束" % __file__
        log.info(logstrouter)
