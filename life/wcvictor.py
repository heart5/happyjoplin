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
# # 微信记录综合应用

# %% [markdown]
# ## 库导入

# %%
import os
import base64
import io
import re
import xlsxwriter
import arrow
import psutil
import pandas as pd
import numpy as np
import sqlite3 as lite
from pathlib import Path
from datetime import datetime
from PIL import Image  # 读取图片的包
from wordcloud import WordCloud, ImageColorGenerator  # 做词云图
import matplotlib.font_manager as fm
import matplotlib.pyplot as plt  # 作图

# %%
import pathmagic

with pathmagic.context():
    from func.first import getdirmain, touchfilepath2depth
    from func.logme import log
    from etc.getid import getdevicename
    from func.wrapfuncs import timethis
    from func.sysfunc import not_IPython, execcmd
    from func.configpr import setcfpoptionvalue, getcfpoptionvalue
    from func.litetools import ifnotcreate, showtablesindb
    from func.jpfuncs import (
        getapi,
        getinivaluefromcloud,
        searchnotes,
        searchnotebook,
        createnote,
        getreslst,
        updatenote_body,
        updatenote_title,
        getnote,
    )
    from filedatafunc import getfilemtime as getfltime
    from life.wc2note import items2df

# %% [markdown]
# ## 功能函数集

# %%
# 获取CPU的使用率
cpu_usage = psutil.cpu_percent()
print("CPU使用率：", cpu_usage)

# 获取CPU核心数量
cpu_count = psutil.cpu_count()
print("CPU核心数量：", cpu_count)

# 获取每个进程的CPU利用率
for proc in psutil.process_iter(["pid", "name", "cpu_percent"]):
    print(f"进程ID: {proc.info['pid']}, 进程名: {proc.info['name']}, CPU利用率: {proc.info['cpu_percent']}")

# 获取内存使用情况
memory_usage = psutil.virtual_memory()
print("内存使用情况：", memory_usage)

# 获取可用内存
available_memory = memory_usage.available
print("可用内存：", available_memory)

# 获取已使用的内存百分比
memory_percent = memory_usage.percent
print("已使用的内存百分比：", memory_percent)


# %%
import psutil

connections = psutil.net_connections()
for conn in connections:
    print(conn)

import psutil

interfaces = psutil.net_if_addrs()
for interface_name, interface_addresses in interfaces.items():
    for address in interface_addresses:
        print(f"Interface: {interface_name}")
        print(f"  Address: {address.address}")
        print(f"  Netmask: {address.netmask}")


net_io = psutil.net_io_counters()
print(f"Bytes Sent: {net_io.bytes_sent}")
print(f"Bytes Received: {net_io.bytes_recv}")

stats = psutil.net_if_stats()
for interface_name, interface_stats in stats.items():
    print(f"Interface: {interface_name}")
    print(f"  Packets Sent: {interface_stats.packets_sent}")
    print(f"  Packets Received: {interface_stats.packets_recv}")
    print(f"  Errors Out: {interface_stats.errout}")

# %%
psutil.cpu_count()

# %%
psutil.cpu_count(logical=False)

# %%
psutil.cpu_freq()

# %%
psutil.users()


# %% [markdown]
# ### all2df(name, wcdatapath)


# %%
@timethis
def all2df(name, wcdatapath):
    """
    获取所有聊天记录并以Dataframe的格式返回
    从最新的文本和数据库中读取聊天记录，合并，去重
    为了和sqlite3数据格式统一，将time字段转换为timestamp(int类型)
    """
    wc_txt_df = items2df(wcdatapath / f"chatitems({owner}).txt")
    # 为了适应sqlite3的存储类型，将bool转换为01，将日期时间转换为整数
    wc_txt_df["send"] = wc_txt_df["send"].apply(lambda x: 1 if x else 0)
    wc_txt_df["time"] = wc_txt_df["time"].apply(lambda x: (int(arrow.get(x, tzinfo="local").timestamp())))
    log.info(f"文本数据的最新记录时间为：{datetime.fromtimestamp(wc_txt_df['time'].max())}")
    loginstr = "" if (whoami := execcmd("whoami")) and (len(whoami) == 0) else f"{whoami}"
    dbfilename = f"wcitemsall_({getdevicename()})_({loginstr}).db".replace(" ", "_")
    dbname = os.path.abspath(wcdatapath / dbfilename)
    # db_all_df = pd.DataFrame(data=None, columns=['time', 'send', 'sender', 'type', 'content'])
    db_all_df = pd.DataFrame()
    with lite.connect(dbname) as conn:
        tablename = f"wc_{name}"
        sql_query = pd.read_sql_query(f"select * from {tablename}", conn)
        db_all_df = pd.DataFrame(sql_query, columns=["time", "send", "sender", "type", "content"])
        last_from_db = db_all_df.iloc[-1, 0]
        if type(last_from_db) == int:
            log.info(f"数据库数据的最后一条记录时间为：{datetime.fromtimestamp(last_from_db)}")
        else:
            log.info(f"数据库数据的最后一条记录时间为：{last_from_db}")
        # 找到time为字符串类型的记录并做转换
        df_tmp = db_all_df[db_all_df["time"].apply(lambda x: type(x) != int)]
        log.critical(
            f"从数据库{dbfilename}中读取数据time字段为【文本】的数据共有{df_tmp.shape[0]}条，\
              最大值为：{df_tmp['time'].max()}，最小值为：{df_tmp['time'].min()}"
        )
        db_all_df["time"] = db_all_df["time"].apply(
            lambda x: int(arrow.get(x, tzinfo="local").timestamp()) if type(x) != int else x
        )
    dfcombine = pd.concat([db_all_df, wc_txt_df], ignore_index=True)
    items_all_num = dfcombine.shape[0]
    dfdone = dfcombine.drop_duplicates()
    if items_all_num != dfdone.shape[0]:
        df_dup = dfcombine[dfcombine.duplicated()]
        log.critical(
            f"从数据库{dbfilename}中读取数据并和文本中记录合并后，\
                     重复的数据记录共有{df_dup.shape[0]}条，\
                     最大值为：{datetime.fromtimestamp(int(df_dup['time'].max()))}，\
                     最小值为：{datetime.fromtimestamp(int(df_dup['time'].min()))}"
        )
        log.critical(f"合并数据记录共有{items_all_num}条，去重后有效数据有{dfdone.shape[0]}条！")
    dfcombine.drop_duplicates(inplace=True)
    print(dfcombine.dtypes)
    dfcombine.sort_values(["time"], ascending=False, inplace=True)
    return dfcombine


# %%
wcdatapath = getdirmain() / "data" / "webchat"
owner = "白晔峰"

# %%
wc_all_df = all2df(owner, wcdatapath)

# %%
wc_all_df


# %% [markdown]
# ### all2spdf(wc_all_df)


# %%
def all2spdf(wc_all_df):
    sport_df = wc_all_df[wc_all_df.sender.str.contains("微信运动")]
    sport_df.loc[:, "time"] = sport_df["time"].apply(lambda x: datetime.fromtimestamp(x))
    spdf = sport_df.loc[:, ["time", "content"]]
    spdf.loc[:, "content"] = spdf["content"].apply(lambda x: re.sub("(\[\w+前\]|\[刚才\])?", "", x))
    num4all = spdf.shape[0]
    print(spdf[spdf.duplicated()])
    spdf.drop_duplicates(inplace=True)
    print(f"数据有{num4all}条，去重后有{spdf.shape[0]}条")
    return spdf


# %% [markdown]
# ### spdf2liked(spdf)


# %%
def spdf2liked(spdf):
    sp_liked_df = spdf[spdf.content.str.contains("just liked|刚刚赞了", regex=True)]

    # 以time为基准，找到重复的内容，去掉那个短的记录，从而获得完整准确的数据集
    duptimelst = sp_liked_df[sp_liked_df.time.duplicated()]["time"].values
    dup_mul_df = sp_liked_df[sp_liked_df["time"].apply(lambda x: x in duptimelst)]
    dupindexshortlist = dup_mul_df[
        dup_mul_df["content"].apply(lambda x: x.endswith("just liked your ranking"))
    ].index.values
    right_index_lst = [x for x in sp_liked_df.index.values if x not in dupindexshortlist]
    sp_liked_df = sp_liked_df.loc[right_index_lst, :]

    # 依据content新增两列：friend、wcid（微信id号）
    sp_liked_df.loc[:, "friend"] = sp_liked_df["content"].apply(lambda x: re.split("\W|刚刚赞了", x)[0])
    sp_liked_df.loc[:, "wcid"] = sp_liked_df["content"].apply(lambda x: re.split("\W", x)[-2])

    return sp_liked_df


# %%
sp_liked_df = spdf2liked(all2spdf(wc_all_df))

# %%
sp_liked_df.groupby("friend").count().sort_values("time", ascending=False)

# %%
newesttime = arrow.get(sp_liked_df["time"].max(), tzinfo="local")
monthago = pd.to_datetime(newesttime.shift(days=-60).strftime("%F %T"))
mydf = sp_liked_df[sp_liked_df.time > monthago]

# %%
mydf = sp_liked_df

# %%
mywcdict = mydf.groupby("friend").count().sort_values("time", ascending=False)["time"].to_dict()

# %%
bgimg = wcdatapath / "../../img" / "fengye.jpg"


# %% [markdown]
# ### makecloudimg(mywcdict, bgimg)


# %%
def makecloudimg(mywcdict, bgimg):
    font_path = fm.findfont(fm.FontProperties())
    # 读取背景图片
    background_Image = np.array(Image.open(bgimg))
    # 提取背景图片颜色
    img_colors = ImageColorGenerator(background_Image)

    # 创建画板
    plt.figure(figsize=(10, 8), dpi=600)  # 创建画板 ,定义图形大小及分辨率
    mask = plt.imread(bgimg)  # 自定义背景图片
    # 设置词云图相关参数
    wc = WordCloud(
        mask=mask, font_path=font_path, width=800, height=500, scale=2, mode="RGBA", background_color="white"
    )
    wc = wc.generate_from_frequencies(mywcdict)  # 利用生成的dict文件制作词云图
    # 根据图片色设置背景色
    # wc.recolor(color_func=img_colors)
    plt.imshow(wc, interpolation="bilinear")
    plt.axis("off")
    # convert the plot to a base64 encoded image
    buffer = io.BytesIO()
    plt.savefig(buffer, format="png")
    buffer.seek(0)
    image_base64 = base64.b64encode(buffer.read()).decode()
    plt.close()

    return image_base64


# %%
allbase64 = makecloudimg(mywcdict, bgimg)


# %% [markdown]
# ### wcliked2note()


# %%
@timethis
def wcliked2note():
    """
    综合输出微信运动点赞好友云图并更新至笔记
    """

    login_user = execcmd("whoami")
    namestr = "happyjp_life"
    section = f"health_{login_user}"
    notestat_title = f"微信运动点赞好友云图【{login_user}】"

    # 在happyjp_life配置文件中查找health_cloud_updatetimestamp，找不到则表示首次运行，置零
    if not (wc_sp_liked_items_num := getcfpoptionvalue(namestr, section, "wc_sp_liked_items_num")):
        wc_sp_liked_items_num = 0
    note = getnote(health_id)
    noteupdatetimewithzone = arrow.get(note.updated_time, tzinfo="local")
    # IPyton环境无视对比判断，强行执行后续操作；非IPython环境则正常逻辑推进
    if (noteupdatetimewithzone.timestamp() == wc_sp_liked_items_num) and (not_IPython()):
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
        f"健康运动笔记【更新时间：{arrow.get(wc_sp_liked_items_num, tzinfo='local')}-》{noteupdatetimewithzone}】。"
    )


# %%
spdf_champion = spdf[spdf.content.str.contains("Champion on \d{4}|夺得\d{2}月\d{2}日", regex=True)]
spdf_champion.iloc[15:30, :]

# %%
spdf.iloc[-10:]

# %%
spdf[spdf.content.str.contains("Champion on \d{4}|夺得\d{2}月\d{2}日", regex=True)].iloc[1, -1]

# %%
spdf[spdf.content.str.contains("Champion on \d{4}|夺得\d{2}月\d{2}日", regex=True)].iloc[2, -1]


# %% [markdown]
# ### getaccountowner(fn)


# %%
def getownerfromfilename(fn):
    """
    从文件名中获取账号
    文件名称示例：chatitems(heart5).txt.1
    """
    ptn = re.compile("\((\w*)\)")
    ac = ac if (ac := re.search(ptn, fn).groups()[0]) not in ["", "None"] else "白晔峰"
    return ac


# %% [markdown]
# ### txtfiles2dfdict(wcdatapath, newfileonly=False)


# %%
@timethis
def txtfiles2dfdict(dpath, newfileonly=False):
    """
    读取传入目录下符合标准（固定格式文件名）所有文本文件并提取融合分账号的df，
    返回字典{name:dict}
    """

    fllst = [f for f in os.listdir(dpath) if f.startswith("chatitems")]
    names = list(set([getownerfromfilename(nm) for nm in fllst]))
    print(names)
    # 如果设置为new，则找到每个账号的最新文本文件处理，否则是全部文本文件
    if newfileonly:
        fl3lst = [[getownerfromfilename(fl), fl, getfltime(dpath / fl)] for fl in fllst]
        fllstout = list()
        for nm in names:
            fllstinner = [item for item in fl3lst if item[0] == nm]
            fllstout4name = sorted(fllstinner, key=lambda x: x[2])
            fllstout.extend(fllstout4name[-2:])
        fllst = [item[1] for item in fllstout]

    #     print(fllst)
    dfdict = dict()
    for fl in fllst[::-1]:
        rs1 = re.search("\((\w*)\)", fl)
        if rs1 is None:
            log.critical(f"记录文件《{fl}》的文件名不符合规范，跳过")
            continue
        account = getownerfromfilename(fl)
        dfin = items2df(dpath / fl)
        print(f"{fl}\t{getfltime(dpath / fl).strftime('%F %T')}\t {account}\t{dfin.shape[0]}", end="\t")
        if account in dfdict.keys():
            dfall = pd.concat([dfdict[account], dfin])
            dfall = dfall.drop_duplicates().sort_values(["time"], ascending=False)
            print(f"{dfall.shape[0]}")
            dfdict.update({account: dfall})
        else:
            dfall = dfin.drop_duplicates().sort_values(["time"], ascending=False)
            print(f"{dfall.shape[0]}")
            dfdict[account] = dfall

    return dfdict


# %% [markdown]
# ### getdaterange(start, end)


# %%
def getdaterange(start, end):
    """
    根据输入的起止时间按照月尾分割生成时间点列表返回
    """
    start = start + pd.Timedelta(-1, "sec")
    if start.strftime("%Y-%m") == end.strftime("%Y-%m"):
        drlst = [start, end]
    else:
        dr = pd.date_range(pd.to_datetime(start.strftime("%F 23:59:59")), end, freq="M")
        drlst = list(dr)
        #         drlst.pop()
        drlst.insert(0, start)
        drlst.append(end)

    return drlst


# %% [markdown]
# ### txtdfsplit2xlsx(name, df, dpath, newfileonly=False):


# %%
def txtdfsplit2xlsx(name, df, dpath, newfileonly=False):
    """
    按月份拆分指定账号的数据记录df，如果尚不存在本地相应资源文件，直接写入并更新ini中登记
    数量；如果存在相应本地资源文件，则读取并融合df中的记录，存入对应格式化名称的excel表格
    中，相应更新ini中登记数量
    """
    dftimestart = df["time"].min()
    dftimeend = df["time"].max()
    dr = getdaterange(dftimestart, dftimeend)
    if newfileonly:
        dr = dr[-2:]
    log.info(f"时间范围横跨{len(dr) - 1}个月")

    outlst = list()
    for i in range(len(dr) - 1):
        print(f"{'-' * 15}\t{name}\t【{i + 1}/{len(dr) - 1}】\tBegin\t{'-' * 15}")
        dfp = df[(df.time >= dr[i]) & (df.time <= dr[i + 1])]
        if dfp.shape[0] != 0:
            ny = dfp["time"].iloc[0].strftime("%y%m")
            fn = f"wcitems_{name}_{ny}.xlsx"  # 纯文件名称
            fn_all = dpath / fn
            fn_all = touchfilepath2depth(fn_all)
            fna = os.path.abspath(fn_all)  # 全路径文件名（绝对路径）
            if not os.path.exists(fna):
                logstr = f"创建文件{fn}，记录共有{dfp.shape[0]}条。"
                log.info(logstr)
                dfp.to_excel(fna, engine="xlsxwriter", index=False)
                setcfpoptionvalue("happyjpwcitems", fn, "itemsnumfromtxt", f"{dfp.shape[0]}")
            else:
                if (oldnum := getcfpoptionvalue("happyjpwcitems", fn, "itemsnumfromtxt")) is None:
                    oldnum = 0
                if oldnum != dfp.shape[0]:
                    dftmp = pd.read_excel(fna)
                    dfpall = pd.concat([dfp, dftmp]).drop_duplicates().sort_values(["time"], ascending=False)
                    logstr = (
                        f"{fn}\t本地（文本文件）登记的记录数量为（{oldnum}），但新文本文件中"
                        f"记录数量（{dfp.shape[0]}）条记录，"
                        f"融合本地excel文件后记录数量为({dfpall.shape[0]})。覆盖写入所有新数据！"
                    )
                    log.info(logstr)
                    dfpall.to_excel(fna, engine="xlsxwriter", index=False)
                    setcfpoptionvalue("happyjpwcitems", fn, "itemsnumfromtxt", f"{dfp.shape[0]}")
                else:
                    print(f"{fn}已经存在，且文本文件中记录数量没有变化。")
            print(i, ny, dr[i], dr[i + 1], dfp.shape[0])
        print(f"{'-' * 15}\t{name}\t【{i + 1}/{len(dr) - 1}】\tDone!\t{'-' * 15}")


# %% [markdown]
# ### df2db(name, df4name, wcpath)


# %%
@timethis
def df2db(name, wcpath):
    """
    把指定微信账号的记录df写入db相应表中
    """
    loginstr = "" if (whoami := execcmd("whoami")) and (len(whoami) == 0) else f"{whoami}"
    dbfilename = f"wcitemsall_({getdevicename()})_({loginstr}).db".replace(" ", "_")
    dbname = os.path.abspath(wcpath / dbfilename)
    dfout = pd.DataFrame()
    with lite.connect(dbname) as conn:
        tablename = f"wc_{name}"
        sql_query = pd.read_sql_query(f"select * from {tablename}", conn)
        dfout = pd.DataFrame(sql_query, columns=["time", "send", "sender", "type", "content"])
        # dfout = pd.DataFrame(sql_query, index=None)
    dfout["time"] = pd.to_datetime(dfout["time"].apply(lambda x: arrow.get(x, tzinfo="local").format()))
    # dfout['time'] = pd.to_datetime(dfout['time'])
    dfout["send"] = dfout["send"].apply(lambda x: True if x == 1 else False)
    return dfout


# %%
dfout

# %%
dfout = df2db(owner, wcdatapath)
wc_db_df_copy = dfout.copy(deep=True)
dfout.dtypes

# %%
dfout = df2db(owner, wcdatapath)
wc_db_df_copy = dfout.copy(deep=True)
dfout.dtypes

# %%
dfout = df2db(owner, wcdatapath)
wc_db_df_copy = dfout.copy(deep=True)
dfout.dtypes

# %%
dfout["time"] = dfout["time"].apply(lambda x: arrow.get(x, tzinfo="local").format())
dfout["send"] = dfout["send"].apply(lambda x: True if x == 1 else False)

# %%
wc_df = dfout.sort_values(["time"], ascending=False)

# %%
wc_df[wc_df.sender.str.contains("微信运动")]

# %%
test_df = dfout[:100]

# %%
pd.to_datetime(test_df["time"])


# %% [markdown]
# ### updatewcitemsxlsx2note(name, df4name, wcpath, notebookguid)


# %%
def updatewcitemsxlsx2note(name, df4name, wcpath, notebookguid):
    """
    处理从本地资源文件读取生成的df，如果和ini登记数量相同，则返回；如果不同，则从笔记端读取相应登记
    数量再次对比，相同，则跳过，如果不同，则拉取笔记资源文件和本地资源文件融合，更新笔记端资源文件并
    更新ini登记数量（用融合后的记录数量）
    """
    ny = df4name["time"].iloc[0].strftime("%y%m")
    dftfilename = f"wcitems_{name}_{ny}.xlsx"
    dftallpath = wcpath / dftfilename
    dftallpathabs = os.path.abspath(dftallpath)
    print(dftallpathabs)
    loginstr = "" if (whoami := execcmd("whoami")) and (len(whoami) == 0) else f"，登录用户：{whoami}"
    timenowstr = pd.to_datetime(datetime.now()).strftime("%F %T")
    first_note_tail = f"\n本笔记创建于{timenowstr}，来自于主机：{getdevicename()}{loginstr}"

    if (dftfileguid := getcfpoptionvalue("happyjpwcitems", dftfilename, "guid")) is None:
        # findnotelst = findnotefromnotebook(notebookguid, dftfilename, notecount=1)
        findnotelst = searchnotes(f"title:{dftfilename}", parent_id=notebookguid)
        if len(findnotelst) == 1:
            dftfileguid = findnotelst[0].id
            log.info(f"数据文件《{dftfilename}》的笔记已经存在，取用")
        else:
            first_note_desc = f"### 账号\t{None}\n### 记录数量\t-1"  # 初始化内容头部，和正常内容头部格式保持一致
            first_note_body = "\n\n---\n".join([first_note_desc, first_note_tail])
            # dftfileguid = makenote2(dftfilename, notebody=first_note_body, parentnotebookguid=notebookguid).guid
            dftfileguid = createnote(title=dftfilename, body=first_note_body, parent_id=notebookguid)
        setcfpoptionvalue("happyjpwcitems", dftfilename, "guid", str(dftfileguid))

    df2db(name, df4name, wcpath)
    if (itemsnum_old := getcfpoptionvalue("happyjpwcitems", dftfilename, "itemsnum")) is None:
        itemsnum_old = 0
    itemnum = df4name.shape[0]
    if itemnum == itemsnum_old:
        log.info(f"笔记《{dftfilename}》的记录数量（{itemnum}）和本地登记数量相同，跳过")
        return

    # print(dftfileguid)
    if (itemsnum4net := getcfpoptionvalue("happyjpwcitems", dftfilename, "itemsnum4net")) is None:
        itemsnum4net = 0
    # oldnotecontent = getnotecontent(dftfileguid).find("pre").text
    if oldnotecontent := getnote(dftfileguid).body:
        # print(oldnotecontent)
        nrlst = oldnotecontent.split("\n\n---\n")
        itemsnumfromnet = int(re.search("记录数量\t(-?\d+)", nrlst[0]).groups()[0])
    else:
        nrlst = list()
        itemsnumfromnet = 0
    if itemsnum4net == itemsnumfromnet == itemnum:
        log.info(
            f"本地资源的记录数量（{itemnum}），本地登记的记录数量（{itemsnum4net}）"
            f"和笔记中登记的记录数量（{itemsnumfromnet}）相同，跳过"
        )
        return
    log.info(
        f"本地资源的记录数量（{itemnum}），登记的记录数量（{itemsnum4net}）"
        f"和笔记中登记的记录数量（{itemsnumfromnet}）三不相同，从笔记端拉取融合"
    )
    reslst = getreslst(dftfileguid)
    # reslst = getnoteresource(dftfileguid)
    if len(reslst) != 0:
        dfromnote = pd.DataFrame()
        filetmp = wcpath / "wccitems_from_net.xlsx"
        for res in reslst:
            fh = open(filetmp, "wb")
            fh.write(res.get("contentb"))
            fh.close()
            dfromnote = pd.concat([dfromnote, pd.read_excel(filetmp)])
        dfcombine = pd.concat([dfromnote, df4name]).drop_duplicates().sort_values(["time"], ascending=False)
        if dfcombine.shape[0] == itemsnumfromnet:
            log.info(
                f"本地数据文件记录有{itemnum}条，笔记中资源文件记录数为{itemsnumfromnet}条，合并后总记录数量{dfcombine.shape[0]}没变化，跳过"
            )
            setcfpoptionvalue("happyjpwcitems", dftfilename, "itemsnum", str(itemsnumfromnet))
            setcfpoptionvalue("happyjpwcitems", dftfilename, "itemsnum4net", str(itemsnumfromnet))
            return
        log.info(
            f"本地数据文件记录数有{itemnum}条，笔记资源文件记录数为{itemsnumfromnet}条"
            f"，合并后记录总数为：\t{dfcombine.shape[0]}"
        )
        df4name = dfcombine
    df2db(name, df4name, wcpath)
    note_desc = f"### 账号\t{name}\n### 记录数量\t{df4name.shape[0]}"
    df4name_desc = (
        f"更新时间：{timenowstr}\t"
        f"记录时间自{df4name['time'].min()}至{df4name['time'].max()}，"
        f"共有{df4name.shape[0]}条，来自主机：{getdevicename()}{loginstr}"
    )
    if len(nrlst) == 2:
        nrlst[0] = note_desc
        nrlst[1] = f"{df4name_desc}\n{nrlst[1]}"
    else:
        nrlst = [note_desc, first_note_tail]
    resultstr = "\n\n---\n".join(nrlst)
    df4name.to_excel(dftallpathabs, engine="xlsxwriter", index=False)
    api, url, port = getapi()
    res_id = api.add_resource(dftallpathabs)
    link_desc = f"[{dftallpathabs}](:/{res_id})\n\n"
    resultstr = link_desc + resultstr

    for res in reslst:
        api.delete_resource(res.get("id"))
        log.critical(f"资源文件《{res.get('title')}》（id：{res.get('id')}）被从系统中删除！")

    updatenote_body(noteid=dftfileguid, bodystr=resultstr)
    # updatereslst2note([dftallpathabs], dftfileguid, \
    #                   neirong=resultstr, filenameonly=True, parentnotebookguid=notebookguid)
    setcfpoptionvalue("happyjpwcitems", dftfilename, "itemsnum", str(df4name.shape[0]))
    setcfpoptionvalue("happyjpwcitems", dftfilename, "itemsnum4net", str(df4name.shape[0]))


# %% [markdown]
# ### getnotelist(name, wcpath, notebookguid)


# %%
@timethis
def getnotelist(name, wcpath, notebookguid):
    """
    根据传入的微信账号名称获得云端记录笔记列表
    """
    notelisttitle = f"微信账号（{name}）记录笔记列表"
    loginstr = "" if (whoami := execcmd("whoami")) and (len(whoami) == 0) else f"，登录用户：{whoami}"
    timenowstr = pd.to_datetime(datetime.now()).strftime("%F %T")
    if (notelistguid := getcfpoptionvalue("happyjpwcitems", "common", f"{name}_notelist_guid")) is None:
        findnotelst = searchnotes(f"title:{notelisttitle}", parent_id=notebookguid)
        if len(findnotelst) == 1:
            notelistguid = findnotelst[0].id
            log.info(f"文件列表《{notelisttitle}》的笔记已经存在，取用")
        else:
            nrlst = list()
            nrlst.append(f"### 账号\t{name}\n### 笔记数量\t-1")  # 初始化内容头部，和正常内容头部格式保持一致
            nrlst.append("")
            nrlst.append(f"\n本笔记创建于{timenowstr}，来自于主机：{getdevicename()}{loginstr}")
            note_body_str = "\n\n---\n".join(nrlst)
            note_body = f"{note_body_str}"
            notelistguid = createnote(title=notelisttitle, body=note_body, parent_id=notebookguid)
            # notelistguid = makenote2(notelisttitle, notebody=note_body, parentnotebookguid=notebookguid).guid
            log.info(f"文件列表《{notelisttitle}》被首次创建！")
        setcfpoptionvalue("happyjpwcitems", "common", f"{name}_notelist_guid", str(notelistguid))

    ptn = f"wcitems_{name}_" + "\d{4}.xlsx"  # wcitems_heart5_2201.xlsx
    xlsxfllstfromlocal = [fl for fl in os.listdir(wcpath) if re.search(ptn, fl)]
    numatlocal_actual = len(xlsxfllstfromlocal)
    if (numatlocal := getcfpoptionvalue("happyjpwcitems", "common", f"{name}_num_at_local")) is None:
        numatlocal = numatlocal_actual
        setcfpoptionvalue("happyjpwcitems", "common", f"{name}_num_at_local", str(numatlocal))
        log.info(
            f"首次运行getnotelist函数，统计微信账户《{name}》的本地资源文件数量({numatlocal_actual})存入本地ini变量中"
        )
    elif numatlocal != numatlocal_actual:
        setcfpoptionvalue("happyjpwcitems", "common", f"{name}_num_at_local", str(numatlocal_actual))
        log.info(f"微信账户《{name}》的本地资源文件数量{numatlocal_actual}和ini数据{numatlocal}不同，更新ini")
        numatlocal = numatlocal_actual
    else:
        numatlocal = numatlocal_actual

    # api, url, port = getapi()
    notent = getnote(notelistguid).body
    nrlst = notent.split("\n\n---\n")
    # print(notent)
    if len(nrlst) != 3:
        nrlst = list()
        nrlst.append(f"### 账号\t{name}\n### 笔记数量\t-1")  # 初始化内容头部，和正常内容头部格式保持一致
        nrlst.append("")
        nrlst.append(f"\n本笔记创建于{timenowstr}，来自于主机：{getdevicename()}{loginstr}")
        log.info(f"《{notelisttitle}》笔记内容不符合规范，重构之。【{nrlst}】")

    #     print(nrlst)
    numinnotedesc = int(re.findall("\t(-?\d+)", nrlst[0])[0])
    #     ptn = f"(wcitems_{name}_\d\d\d\d\.xlsx)\t(\S+)"
    ptn = f"(wcitems_{name}_" + "\d{4}.xlsx)\t(\S+)"
    #     print(ptn)
    finditems = re.findall(ptn, nrlst[1])
    finditems = sorted(finditems, key=lambda x: x[0], reverse=True)
    #     print(finditems)
    print(numinnotedesc, numatlocal, len(finditems))
    if numinnotedesc == numatlocal == len(finditems):
        log.info(f"《{notelisttitle}》中数量无更新，跳过。")
        return finditems
    findnotelst = searchnotes(f"title:wcitems_{name}_", parent_id=notebookguid)
    findnotelst = [[nt.title, nt.id, re.findall("记录数量\t(-?\d+)", nt.body)[0]] for nt in findnotelst]
    # findnotelst = [[nt.get("title"), note.get("id"), re.findall("记录数量\t(-?\d+)", nt.get("body"))[0]] for nt in findnotelst]
    findnotelst = sorted(findnotelst, key=lambda x: x[0], reverse=True)
    nrlstnew = list()
    nrlstnew.append(re.sub("\t(-?\d+)", "\t" + f"{len(findnotelst)}", nrlst[0]))
    nrlstnew.append("\n".join(["\t".join(sonlst) for sonlst in findnotelst]))
    nrlstnew.append(f"更新于{timenowstr}，来自于主机：{getdevicename()}{loginstr}" + f"\n{nrlst[2]}")

    updatenote_body(notelistguid, bodystr="\n\n---\n".join(nrlstnew))
    # imglist2note(get_notestore(), [], notelistguid, notelisttitle,
    #              neirong="<pre>" + "\n---\n".join(nrlst) + "</pre>", parentnotebookguid=notebookguid)
    numatlocal = len(finditems)
    setcfpoptionvalue("happyjpwcitems", "common", f"{name}_num_at_local", str(numatlocal))

    return findnotelst


# %% [markdown]
# ### merge2note(dfdict, wcpath, notebookguid, newfileonly=False)


# %%
# @timethis
def merge2note(dfdict, wcpath, notebookguid, newfileonly=False):
    """
    处理从文本文件读取生成的dfdict，分账户读取本地资源文件和笔记进行对照，并做相应更新或跳过
    """
    for name in dfdict.keys():
        fllstfromnote = getnotelist(name, wcpath, notebookguid=notebookguid)
        ptn = f"wcitems_{name}_" + "\d{4}.xlsx"  # wcitems_heart5_2201.xlsx
        xlsxfllstfromlocal = [fl for fl in os.listdir(wcpath) if re.search(ptn, fl)]
        if len(fllstfromnote) != len(xlsxfllstfromlocal):
            print(
                f"{name}的数据文件本地数量\t{len(xlsxfllstfromlocal)}，云端笔记列表中为\t{len(fllstfromnote)}，"
                "两者不等，先把本地缺的从网上拉下来"
            )
            misslstfromnote = [fl for fl in fllstfromnote if fl[0] not in xlsxfllstfromlocal]
            for fl, guid, num in misslstfromnote:
                reslst = getreslst(guid)
                # reslst = getnoteresource(guid)
                if len(reslst) != 0:
                    for res in reslst:
                        flfull = wcpath / fl
                        fh = open(flfull, "wb")
                        fh.write(res.contentb)
                        fh.close()
                        dftest = pd.read_excel(flfull)
                        setcfpoptionvalue("happyjpwcitems", fl, "guid", guid)
                        setcfpoptionvalue("happyjpwcitems", fl, "itemsnum", str(dftest.shape[0]))
                        setcfpoptionvalue("happyjpwcitems", fl, "itemsnum4net", str(dftest.shape[0]))
                        log.info(f"文件《{fl}》在本地不存在，从云端获取存入并更新ini（section：{fl}，guid：{guid}）")

        xlsxfllst = sorted([fl for fl in os.listdir(wcpath) if re.search(ptn, fl)])
        print(f"{name}的数据文件数量\t{len(xlsxfllst)}", end="，")
        if newfileonly:
            xlsxfllst = xlsxfllst[-2:]
        xflen = len(xlsxfllst)
        print(f"本次处理的数量为\t{xflen}")
        for xfl in xlsxfllst:
            print(f"{'-' * 15}\t{name}\t【{xlsxfllst.index(xfl) + 1}/{xflen}】\tBegin\t{'-' * 15}")
            dftest = pd.read_excel(wcpath / xfl).drop_duplicates()
            updatewcitemsxlsx2note(name, dftest, wcpath, notebookguid)
            print(f"{'-' * 15}\t{name}\t【{xlsxfllst.index(xfl) + 1}/{xflen}】\tDone!\t{'-' * 15}")


# %% [markdown]
# ### refreshres(wcpath)


# %%
@timethis
def refreshres(wcpath):
    notebookname = "微信记录数据仓"
    notebookguid = searchnotebook(notebookname)
    if (new := getinivaluefromcloud("wcitems", "txtfilesonlynew")) is None:
        new = False
    #     new = True
    print(f"是否只处理新的文本文件：\t{new}")
    dfdict = txtfiles2dfdict(wcpath, newfileonly=new)
    for k in dfdict:
        dfinner = dfdict[k]
        print(f"{k}\t{dfinner.shape[0]}", end="\n\n")
        txtdfsplit2xlsx(k, dfinner, wcpath, newfileonly=new)

    merge2note(dfdict, wcpath, notebookguid, newfileonly=new)


# %% [markdown]
# ### alldfdesc2note(name)


# %%
@timethis
def alldfdesc2note(wcpath):
    """
    读取本地所有资源文件的聊天记录到DataFrame中，输出描述性信息到相应笔记中
    """
    ptn4name = "wcitems_(\w+)_(\d{4}.xlsx)"
    names = list(set([re.search(ptn4name, fl).groups()[0] for fl in os.listdir(wcpath) if re.search(ptn4name, fl)]))
    print(names)
    loginstr = "" if (whoami := execcmd("whoami")) and (len(whoami) == 0) else f"{whoami}"
    dbfilename = f"wcitemsall_({getdevicename()})_({loginstr}).db".replace(" ", "_")
    dbname = wcpath / dbfilename
    resultdict = dict()
    for name in names[:]:
        with lite.connect(dbname) as conn:
            tbname = f"wc_{name}"
            sql = f"select * from {tbname}"
            finnaldf = pd.read_sql(
                sql,
                conn,
                index_col=["id"],
                parse_dates=["time"],
                columns=["id", "time", "send", "sender", "type", "content"],
            )
            finnaldf["send"] = finnaldf["send"].astype(bool)
            resultdict[name] = finnaldf
            print(f"{name}\t{finnaldf.shape[0]}")

    return resultdict


# %% [markdown]
# ## main，主函数

# %%
if __name__ == "__main__":
    if not_IPython():
        log.info(f"运行文件\t{__file__}")

    wcpath = getdirmain() / "data" / "webchat"
    refreshres(wcpath)
    mydict = alldfdesc2note(wcpath)

    if not_IPython():
        log.info(f"文件\t{__file__}\t运行结束。")


# %%
def explodedf():
    mydf = mydict["heart5"]
    mydf[mydf.time >= "2022-10-01"].sort_values("time")
