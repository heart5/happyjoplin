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
# # 微信聊天记录文本文件智能远程存储

# %% [markdown]
# - 文本文件：跑程序生成的txt存储的记录原始文件
# - 资源文件：提取并排序后按月拆分的记录存储文件，excel表格
# - 笔记：记录了分月统计和更新信息的云端笔记，附件为相应的资源文件

# %% [markdown]
# ## 库导入

# %%
import os
import re
import sqlite3 as lite
from datetime import datetime
from pathlib import Path

import arrow
import pandas as pd
# import xlsxwriter

# %%
import pathmagic

with pathmagic.context():
    from etc.getid import getdevicename
    from func.configpr import getcfpoptionvalue, setcfpoptionvalue
    from func.filedatafunc import getfilemtime as getfltime
    from func.first import getdirmain, touchfilepath2depth
    from func.jpfuncs import (
        createnote,
        getinivaluefromcloud,
        getnote,
        getreslst,
        jpapi,
        searchnotebook,
        searchnotes,
        updatenote_body,
        # updatenote_title,
    )
    from func.litetools import ifnotcreate
    from func.logme import log
    from func.sysfunc import execcmd, not_IPython
    from func.wrapfuncs import timethis



# %% [markdown]
# ## 功能函数集

# %% [markdown]
# ### items2df(fl: Path) -> pd.DataFrame


# %%
def items2df(fl: Path) -> pd.DataFrame:
    """读取txt记录文件，格式化拆分并存储至DataFrame返回"""
    try:
        content = open(fl, "r").read()
        # print(fl, content[:100])
    except Exception as e:
        log.critical(f"文件{fl}读取时出现错误，返回空的pd.DataFrame.\n{e}")
        return pd.DataFrame()
    ptn = re.compile(
        r"(^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\t(True|False)\t([^\t]+)\t(\w+)\t", re.M
    )
    itemlst = re.split(ptn, content)
    # print(itemlst[:5])
    itemlst = [im.strip() for im in itemlst if len(im) > 0]
    # print(itemlst[:5])
    step = 5
    itemlst4pd1 = [itemlst[i : i + step] for i in range(0, len(itemlst), step)]
    # print(itemlst4pd1[:5])
    df2 = pd.DataFrame(
        itemlst4pd1, columns=["time", "send", "sender", "type", "content"]
    )
    df2["time"] = pd.to_datetime(df2["time"])
    df2["send"] = df2["send"].apply(lambda x: True if x == "True" else False)
    df2["content"] = df2["content"].apply(
        lambda x: re.sub(r"(\[\w+前\]|\[刚才\])?", "", x)
    )
    # 处理成相对路径，逻辑是准备把所有音频等文件集中到主运行环境
    ptn = re.compile(r"^/.+happyjoplin/")
    df2.loc[:, "content"] = df2["content"].apply(
        lambda x: re.sub(ptn, "", x) if ptn.match(x) else x
    )
    dfout = df2.drop_duplicates().sort_values("time")

    return dfout


# %% [markdown]
# ### getownerfromfilename(fn: str) -> str


# %%
def getownerfromfilename(fn: str) -> str:
    """从文件名中获取账号

    文件名格式：chatitems(账号).txt.1

    Args:
        fn (str): 文件名

    Returns:
        str: 账号
    """
    ptn = re.compile(r"\((\w*)\)")
    ac = ac if (ac := re.search(ptn, fn).groups()[0]) not in ["", "None"] else "白晔峰"
    return ac



# %% [markdown]
# ### txtfiles2dfdict(dpath: Path, newfileonly: bool=False) -> dict


# %%
@timethis
def txtfiles2dfdict(dpath: Path, newfileonly: bool=False) -> dict:
    """读取传入目录下符合标准（固定格式文件名）的文本文件并提取融合分账号的df，

    Args:
        dpath: 文本文件所在目录
        newfileonly: 是否只处理最新两个文本文件，默认为False

    Returns:
        dfdict: 字典，key为账号名，value为DataFrame，包含该账号的所有聊天记录
    """
    fllst = [f for f in os.listdir(dpath) if f.startswith("chatitems")]
    names = list(set([getownerfromfilename(nm) for nm in fllst]))
    print(names)
    # 如果设置为new，则找到每个账号的两个最新文本文件处理，否则则处理全部文本文件
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
        rs1 = re.search(r"\((\w*)\)", fl)
        if rs1 is None:
            log.critical(f"记录文件《{fl}》的文件名不符合规范，跳过")
            continue
        account = getownerfromfilename(fl)
        dfin = items2df(dpath / fl)
        print(
            f"{fl}\t{getfltime(dpath / fl).strftime('%F %T')}\t {account}\t{dfin.shape[0]}",
            end="\t",
        )
        if account in dfdict.keys():
            dfall = pd.concat([dfdict[account], dfin])
            dfall = dfall.drop_duplicates().sort_values(["time"])
            print(f"{dfall.shape[0]}")
            dfdict.update({account: dfall})
        else:
            dfall = dfin
            print(f"{dfall.shape[0]}")
            dfdict[account] = dfall

    return dfdict


# %% [markdown]
# ### getdaterange(start: datetime, end: datetime) -> list


# %%
def getdaterange(start: datetime,end: datetime) -> list:
    """根据输入的起止时间按照月尾分割生成时间点列表返回"""
    """
    start = pd.to_datetime("2024-1-9")
    end = pd.to_datetime("2024-11-18")
    output:
    [Timestamp('2024-01-31 23:59:59'), Timestamp('2024-02-29 23:59:59'), Timestamp('2024-03-31 23:59:59'), Timestamp('2024-04-30 23:59:59'), Timestamp('2024-05-31 23:59:59'), Timestamp('2024-06-30 23:59:59'), Timestamp('2024-07-31 23:59:59'), Timestamp('2024-08-31 23:59:59'), Timestamp('2024-09-30 23:59:59'), Timestamp('2024-10-31 23:59:59')]
    """
    if start > end:
        log.critical("start time is later than end time")
        return []

    start_s = pd.to_datetime(start.strftime("%Y-%m-%d 00:00:00"))
    end_s = pd.to_datetime(end.strftime("%Y-%m-%d 23:59:59"))

    if start_s.strftime("%Y-%m") == end_s.strftime("%Y-%m"):
        drlst = [start_s, end_s]
    else:
        dr = pd.date_range(start_s, end_s, freq="MS")
        drlst = list(dr)
        if start.strftime("%d") != "01":
            drlst.insert(0, start)
        else:
            drlst[0] = start
        drlst.append(end)

    return drlst

# %% [markdown]
# ### txtdfsplit2xlsx(name: str, df: pd.DataFrame, dpath: Path, newfileonly: bool=False) -> None


# %%
def txtdfsplit2xlsx(name: str, df: pd.DataFrame, dpath: Path, newfileonly: bool=False) -> None:
    """按月份拆分指定账号的数据记录df，如果尚不存在本地相应资源文件，直接写入并更新ini中登记数量；如果存在相应本地资源文件，则读取并融合df中的记录，存入对应格式化名称的excel表格中，相应更新ini中登记数量

    Args:
        name: 账号名称
        df: 待处理的数据记录df
        dpath: 本地资源文件路径
        newfileonly: 是否只处理最新两个月的数据，默认为False
    Returns:
        None
    """
    try:
        dftimestart = df["time"].min()
        dftimeend = df["time"].max()
        dr = getdaterange(dftimestart, dftimeend)
        if newfileonly:
            dr = dr[-3:]
        log.info(f"时间范围横跨{len(dr) - 1}个月")

        for i in range(len(dr) - 1):
            log.info(f"{'-' * 15}\t{name}\t【{i + 1}/{len(dr) - 1}】\tBegin\t{'-' * 15}")
            log.info(f"宣称的处理时间范围：{dr[i]} - {dr[i + 1]}")
            dfp = df[(df.time >= dr[i]) & (df.time < dr[i + 1])]
            log.info(f"数据实际时间跨度范围：{dfp.time.min()} - {dfp.time.max()}，共{len(dfp)}条记录")
            if not dfp.empty:
                ny = dfp["time"].iloc[0].strftime("%y%m")
                fn = f"wcitems_{name}_{ny}.xlsx"  # 纯文件名称
                fn_all = touchfilepath2depth(dpath / fn)
                fna = os.path.abspath(fn_all)  # 全路径文件名（绝对路径）

                if not os.path.exists(fna):
                    logstr = f"创建文件{fn}，记录共有{len(dfp)}条。"
                    log.info(logstr)
                    dfp.to_excel(fna, engine="xlsxwriter", index=False)
                    setcfpoptionvalue("happyjpwcitems", fn, "itemsnumfromtxt", str(len(dfp)))
                else:
                    oldnum = getcfpoptionvalue("happyjpwcitems", fn, "itemsnumfromtxt")
                    if oldnum != len(dfp):
                        dftmp = pd.read_excel(fna)
                        dfpall = pd.concat([dfp, dftmp]).drop_duplicates().sort_values(["time"])
                        # 去除时间范围外的记录
                        dfpall = dfpall[dfpall.time < dr[i + 1]]
                        logstr = (
                            f"{fn}\t本地（文本文件）登记的记录数量为（{oldnum}），但新文本文件中"
                            f"记录数量（{len(dfp)}）条记录，"
                            f"融合本地excel文件后记录数量为({len(dfpall)})。覆盖写入所有新数据！"
                        )
                        log.info(logstr)
                        dfpall.to_excel(fna, engine="xlsxwriter", index=False)
                        setcfpoptionvalue("happyjpwcitems", fn, "itemsnumfromtxt", str(len(dfp)))
                    else:
                        print(f"{fn}已经存在，且文本文件中记录数量没有变化。")
                print(i, ny, dr[i], dr[i + 1], len(dfp))
            print(f"{'-' * 15}\t{name}\t【{i + 1}/{len(dr) - 1}】\tDone!\t{'-' * 15}")

    except Exception as e:
        log.error(f"在处理 {name} 的数据时发生错误: {e}")
        raise

# %% [markdown]
# ### df2db(name: str, df4name: pd.DataFrame, wcpath: Path) -> None


# %%
def df2db(name: str, df4name: pd.DataFrame, wcpath: Path) -> None:
    """把指定微信账号的记录df写入db相应表中"""
    ny = df4name["time"].iloc[0].strftime("%y%m")
    dftfilename = f"wcitems_{name}_{ny}.xlsx"
    if (
        itemsnum_db := getcfpoptionvalue("happyjpwcitems", dftfilename, "itemsnum_db")
    ) is None:
        itemsnum_db = 0
    itemnum = df4name.shape[0]
    if itemnum != itemsnum_db:
        df4name = df4name.sort_values("time")
        starttime = df4name["time"].min().strftime("%F %T")
        endtime = df4name["time"].max().strftime("%F %T")
        loginstr = (
            "" if (whoami := execcmd("whoami")) and (len(whoami) == 0) else f"{whoami}"
        )
        dbfilename = f"wcitemsall_({getdevicename()})_({loginstr}).db".replace(" ", "_")
        dbname = str((wcpath / dbfilename).resolve())
        with lite.connect(dbname) as conn:
            tablename = f"wc_{name}"
            csql = (
                f"create table if not exists {tablename} "
                + "(id INTEGER PRIMARY KEY AUTOINCREMENT, time DATETIME, send BOOLEAN, sender TEXT, type TEXT, content TEXT)"
            )
            ifnotcreate(tablename, csql, dbname)
            cursor = conn.cursor()
            sql = f"select * from {tablename} where datetime(time, 'unixepoch', 'localtime') between '{starttime}' and '{endtime}';"
            tb = cursor.execute(sql).fetchall()
            if len(tb) != itemnum:
                sqldel = f"delete from {tablename} where datetime(time, 'unixepoch', 'localtime') between '{starttime}' and '{endtime}';"
                cursor.execute(sqldel)
                conn.commit()
                if cursor.rowcount != 0:
                    print(sqldel)
                    log.info(
                        f"从数据库文件《{dbname}》的表《{tablename}》中删除{cursor.rowcount}条记录"
                    )
                df4name.to_sql(tablename, conn, if_exists="append", index=False)
                setcfpoptionvalue(
                    "happyjpwcitems", dftfilename, "itemsnum_db", str(itemnum)
                )
                log.info(
                    f"{dftfilename}的数据写入数据库文件（{dbname}）的（{tablename}）表中，并在ini登记数量（{itemnum}）"
                )


# %% [markdown]
# ### updatewcitemsxlsx2note(name: str, df4name: pd.DataFrame, wcpath: Path, notebookguid: str) -> None


# %%
def updatewcitemsxlsx2note(name: str, df4name: pd.DataFrame, wcpath: Path, notebookguid: str) -> None:
    """处理从本地资源文件读取生成的df，如果和ini登记数量相同，则返回；如果不同，则从笔记端读取相应登记数量再次对比，相同，则跳过，如果不同，则拉取笔记资源文件和本地资源文件融合，更新笔记端资源文件并更新ini登记数量（用融合后的记录数量）"""
    # global jpapi

    forcerefresh = getinivaluefromcloud("wcitems", "forcerefresh")
    ny = df4name["time"].iloc[0].strftime("%y%m")
    dftfilename = f"wcitems_{name}_{ny}.xlsx"
    dftallpath = wcpath / dftfilename
    dftallpathabs = os.path.abspath(dftallpath)
    print(dftallpathabs)
    loginstr = (
        ""
        if (whoami := execcmd("whoami")) and (len(whoami) == 0)
        else f"，登录用户：{whoami}"
    )
    timenowstr = pd.to_datetime(datetime.now()).strftime("%F %T")
    first_note_tail = (
        f"\n本笔记创建于{timenowstr}，来自于主机：{getdevicename()}{loginstr}"
    )

    if (
        dftfileguid := getcfpoptionvalue("happyjpwcitems", dftfilename, "guid")
    ) is None:
        # findnotelst = findnotefromnotebook(notebookguid, dftfilename, notecount=1)
        findnotelst = searchnotes(f"{dftfilename}", parent_id=notebookguid)
        if len(findnotelst) == 1:
            dftfileguid = findnotelst[0].id
            log.info(f"数据文件《{dftfilename}》的笔记已经存在，取用")
        else:
            first_note_desc = f"### 账号\t{None}\n### 记录数量\t-1"  # 初始化内容头部，和正常内容头部格式保持一致
            first_note_body = "\n\n---\n".join([first_note_desc, first_note_tail])
            # dftfileguid = makenote2(dftfilename, notebody=first_note_body, parentnotebookguid=notebookguid).guid
            dftfileguid = createnote(
                title=dftfilename, body=first_note_body, parent_id=notebookguid
            )
        setcfpoptionvalue("happyjpwcitems", dftfilename, "guid", str(dftfileguid))

    df2db(name, df4name, wcpath)
    if (
        itemsnum_old := getcfpoptionvalue("happyjpwcitems", dftfilename, "itemsnum")
    ) is None:
        itemsnum_old = 0
    itemnum = df4name.shape[0]
    if itemnum == itemsnum_old:
        if forcerefresh:
            log.info(
                f"笔记《{dftfilename}》的记录数量（{itemnum}）和本地登记数量相同，但是强制更新！！！"
            )
        else:
            log.info(
                f"笔记《{dftfilename}》的记录数量（{itemnum}）和本地登记数量相同，跳过"
            )
            return

    # print(dftfileguid)
    if (
        itemsnum4net := getcfpoptionvalue("happyjpwcitems", dftfilename, "itemsnum4net")
    ) is None:
        itemsnum4net = 0
    # oldnotecontent = getnotecontent(dftfileguid).find("pre").text
    if oldnotecontent := getnote(dftfileguid).body:
        # print(oldnotecontent)
        nrlst = oldnotecontent.split("\n\n---\n")
        itemsnumfromnet = int(re.search(r"记录数量\t(-?\d+)", nrlst[0]).groups()[0])
    else:
        nrlst = list()
        itemsnumfromnet = 0
    if itemsnum4net == itemsnumfromnet == itemnum:
        if forcerefresh:
            log.info(
                f"本地资源的记录数量（{itemnum}），本地登记的记录数量（{itemsnum4net}）"
                f"和笔记中登记的记录数量（{itemsnumfromnet}）相同，但是要强制更新！！！"
            )
        else:
            log.info(
                f"本地资源的记录数量（{itemnum}），本地登记的记录数量（{itemsnum4net}）"
                f"和笔记中登记的记录数量（{itemsnumfromnet}）相同，跳过"
            )
            return
    else:
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
        dfcombine = pd.concat([dfromnote, df4name])
        ptn = re.compile(r"^/.+happyjoplin/")
        dfcombine.loc[:, "content"] = dfcombine["content"].apply(
            lambda x: re.sub(ptn, "", x) if isinstance(x, str) and ptn.match(x) else x
        )
        dfcombinedone = dfcombine.drop_duplicates().sort_values(["time"])
        if dfcombine.shape[0] != dfcombinedone.shape[0]:
            log.info(
                f"云端笔记《{getnote(dftfileguid).title}》资源文件存在重复记录，从{dfcombine.shape[0]}去重后降至{dfcombinedone.shape[0]}"
            )
        if dfcombinedone.shape[0] == itemsnumfromnet:
            setcfpoptionvalue(
                "happyjpwcitems", dftfilename, "itemsnum", str(itemsnumfromnet)
            )
            setcfpoptionvalue(
                "happyjpwcitems", dftfilename, "itemsnum4net", str(itemsnumfromnet)
            )
            if forcerefresh:
                log.info(
                    f"本地数据文件记录有{itemnum}条，笔记中资源文件记录数为{itemsnumfromnet}条，合并后总记录数量{dfcombinedone.shape[0]}没变化，但是要强制更新！！！"
                )
            else:
                log.info(
                    f"本地数据文件记录有{itemnum}条，笔记中资源文件记录数为{itemsnumfromnet}条，合并后总记录数量{dfcombinedone.shape[0]}没变化，跳过"
                )
                return
        log.info(
            f"本地数据文件记录数有{itemnum}条，笔记资源文件记录数为{itemsnumfromnet}条"
            f"，合并后记录总数为：\t{dfcombinedone.shape[0]}"
        )
        df4name = dfcombinedone
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
    res_id = jpapi.add_resource(dftallpathabs)
    link_desc = f"[{dftallpathabs}](:/{res_id})\n\n"
    resultstr = link_desc + resultstr

    for res in reslst:
        jpapi.delete_resource(res.get("id"))
        log.critical(
            f"资源文件《{res.get('title')}》（id：{res.get('id')}）被从系统中删除！"
        )

    updatenote_body(noteid=dftfileguid, bodystr=resultstr)
    # updatereslst2note([dftallpathabs], dftfileguid, \
    #                   neirong=resultstr, filenameonly=True, parentnotebookguid=notebookguid)
    setcfpoptionvalue("happyjpwcitems", dftfilename, "itemsnum", str(df4name.shape[0]))
    setcfpoptionvalue(
        "happyjpwcitems", dftfilename, "itemsnum4net", str(df4name.shape[0])
    )


# %% [markdown]
# ### getnotelist(name: str, wcpath: Path, notebookguid: str) -> list


# %%
@timethis
def getnotelist(name: str, wcpath: Path, notebookguid: str) -> list:
    """根据传入的微信账号名称获得云端记录笔记列表"""
    notelisttitle = f"微信账号（{name}）记录笔记列表"
    loginstr = (
        ""
        if (whoami := execcmd("whoami")) and (len(whoami) == 0)
        else f"，登录用户：{whoami}"
    )
    timenowstr = pd.to_datetime(datetime.now()).strftime("%F %T")
    if (
        notelistguid := getcfpoptionvalue(
            "happyjpwcitems", "common", f"{name}_notelist_guid"
        )
    ) is None:
        findnotelst = searchnotes(f"{notelisttitle}", parent_id=notebookguid)
        if len(findnotelst) == 1:
            notelistguid = findnotelst[0].id
            log.info(f"文件列表《{notelisttitle}》的笔记已经存在，取用")
        else:
            nrlst = list()
            nrlst.append(
                f"### 账号\t{name}\n### 笔记数量\t-1"
            )  # 初始化内容头部，和正常内容头部格式保持一致
            nrlst.append("")
            nrlst.append(
                f"\n本笔记创建于{timenowstr}，来自于主机：{getdevicename()}{loginstr}"
            )
            note_body_str = "\n\n---\n".join(nrlst)
            note_body = f"{note_body_str}"
            notelistguid = createnote(
                title=notelisttitle, body=note_body, parent_id=notebookguid
            )
            log.info(f"文件列表《{notelisttitle}》被首次创建！")
        setcfpoptionvalue(
            "happyjpwcitems", "common", f"{name}_notelist_guid", str(notelistguid)
        )

    ptn = f"wcitems_{name}_" + r"\d{4}.xlsx"  # wcitems_heart5_2201.xlsx
    xlsxfllstfromlocal = [fl for fl in os.listdir(wcpath) if re.search(ptn, fl)]
    numatlocal_actual = len(xlsxfllstfromlocal)
    if (
        numatlocal := getcfpoptionvalue(
            "happyjpwcitems", "common", f"{name}_num_at_local"
        )
    ) is None:
        numatlocal = numatlocal_actual
        setcfpoptionvalue(
            "happyjpwcitems", "common", f"{name}_num_at_local", str(numatlocal)
        )
        log.info(
            f"首次运行getnotelist函数，统计微信账户《{name}》的本地资源文件数量({numatlocal_actual})存入本地ini变量中"
        )
    elif numatlocal != numatlocal_actual:
        setcfpoptionvalue(
            "happyjpwcitems", "common", f"{name}_num_at_local", str(numatlocal_actual)
        )
        log.info(
            f"微信账户《{name}》的本地资源文件数量{numatlocal_actual}和ini数据{numatlocal}不同，更新ini"
        )
        numatlocal = numatlocal_actual
    else:
        numatlocal = numatlocal_actual

    # api, url, port = getapi()
    notent = getnote(notelistguid).body
    nrlst = notent.split("\n\n---\n")
    # print(notent)
    if len(nrlst) != 3:
        nrlst = list()
        nrlst.append(
            f"### 账号\t{name}\n### 笔记数量\t-1"
        )  # 初始化内容头部，和正常内容头部格式保持一致
        nrlst.append("")
        nrlst.append(
            f"\n本笔记创建于{timenowstr}，来自于主机：{getdevicename()}{loginstr}"
        )
        log.info(f"《{notelisttitle}》笔记内容不符合规范，重构之。【{nrlst}】")

    #     print(nrlst)
    numinnotedesc = int(re.findall(r"\t(-?\d+)", nrlst[0])[0])
    ptn = f"(wcitems_{name}_" + r"\d{4}.xlsx)\t(\S+)"
    finditems = re.findall(ptn, nrlst[1])
    finditems = sorted(finditems, key=lambda x: x[0], reverse=True)
    #     print(finditems)
    print(numinnotedesc, numatlocal, len(finditems))
    if numinnotedesc == numatlocal == len(finditems):
        log.info(f"《{notelisttitle}》中数量无更新，跳过。")
        return finditems
    # 没有严格限定笔记名称规范，导致笔内容结构不对而出错，修正之
    findnotelst = searchnotes(f"wcitems_{name}_", parent_id=notebookguid)
    findnotelst = [
        [nt.title, nt.id, re.findall(r"记录数量\t(-?\d+)", nt.body)[0]]
        for nt in findnotelst
    ]
    # findnotelst = [[nt.get("title"), note.get("id"), re.findall("记录数量\t(-?\d+)", nt.get("body"))[0]] for nt in findnotelst]
    # 使用字典去重
    unique_findnotelst = {item[0]: item for item in findnotelst}.values()
    # 转换为列表
    unique_findnotelst = list(unique_findnotelst)
    findnotelst = sorted(
        unique_findnotelst, key=lambda x: (x[0], int(x[2])), reverse=True
    )
    nrlstnew = list()
    nrlstnew.append(re.sub(r"\t(-?\d+)", "\t" + f"{len(findnotelst)}", nrlst[0]))
    nrlstnew.append("\n".join(["\t".join(sonlst) for sonlst in findnotelst]))
    nrlstnew.append(
        f"更新于{timenowstr}，来自于主机：{getdevicename()}{loginstr}" + f"\n{nrlst[2]}"
    )

    updatenote_body(notelistguid, bodystr="\n\n---\n".join(nrlstnew))
    # imglist2note(get_notestore(), [], notelistguid, notelisttitle,
    #              neirong="<pre>" + "\n---\n".join(nrlst) + "</pre>", parentnotebookguid=notebookguid)
    numatlocal = len(finditems)
    setcfpoptionvalue(
        "happyjpwcitems", "common", f"{name}_num_at_local", str(numatlocal)
    )

    return findnotelst


# %% [markdown]
# ### merge2note(dfdict: dict, wcpath: Path, notebookguid: str, newfileonly: bool=False) -> None


# %%
# @timethis
def merge2note(dfdict: dict, wcpath: Path, notebookguid: str, newfileonly: bool=False) -> None:
    """处理从文本文件读取生成的dfdict，分账户读取本地资源文件和笔记进行对照，并做相应更新或跳过"""
    for name in dfdict.keys():
        fllstfromnote = getnotelist(name, wcpath, notebookguid=notebookguid)
        ptn = f"wcitems_{name}_" + r"\d{4}.xlsx"  # wcitems_heart5_2201.xlsx
        xlsxfllstfromlocal = [fl for fl in os.listdir(wcpath) if re.search(ptn, fl)]
        if len(fllstfromnote) != len(xlsxfllstfromlocal):
            print(
                f"{name}的数据文件本地数量\t{len(xlsxfllstfromlocal)}，云端笔记列表中为\t{len(fllstfromnote)}，"
                "两者不等，先把本地缺的从网上拉下来"
            )
            misslstfromnote = [
                fl for fl in fllstfromnote if fl[0] not in xlsxfllstfromlocal
            ]
            for fl, guid, num in misslstfromnote:
                reslst = getreslst(guid)
                # reslst = getnoteresource(guid)
                if len(reslst) != 0:
                    for res in reslst:
                        flfull = wcpath / fl
                        fh = open(flfull, "wb")
                        fh.write(res["contentb"])
                        fh.close()
                        dftest = pd.read_excel(flfull)
                        setcfpoptionvalue("happyjpwcitems", fl, "guid", guid)
                        setcfpoptionvalue(
                            "happyjpwcitems", fl, "itemsnum", str(dftest.shape[0])
                        )
                        setcfpoptionvalue(
                            "happyjpwcitems", fl, "itemsnum4net", str(dftest.shape[0])
                        )
                        log.info(
                            f"文件《{fl}》在本地不存在，从云端获取存入并更新ini（section：{fl}，guid：{guid}）"
                        )

        xlsxfllst = sorted([fl for fl in os.listdir(wcpath) if re.search(ptn, fl)])
        print(f"{name}的数据文件数量\t{len(xlsxfllst)}", end="，")
        if newfileonly:
            xlsxfllst = xlsxfllst[-2:]
        xflen = len(xlsxfllst)
        print(f"本次处理的数量为\t{xflen}")
        for xfl in xlsxfllst:
            print(
                f"{'-' * 15}\t{name}\t【{xlsxfllst.index(xfl) + 1}/{xflen}】\tBegin\t{'-' * 15}"
            )
            dftest = pd.read_excel(wcpath / xfl, engine="openpyxl").drop_duplicates()
            updatewcitemsxlsx2note(name, dftest, wcpath, notebookguid)
            print(
                f"{'-' * 15}\t{name}\t【{xlsxfllst.index(xfl) + 1}/{xflen}】\tDone!\t{'-' * 15}"
            )


# %% [markdown]
# ### refreshres(wcpath: Path) -> None


# %%
@timethis
def refreshres(wcpath: Path) -> None:
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
# ### alldfdesc2note(wcpath: Path) -> dict


# %%
@timethis
def alldfdesc2note(wcpath: Path) -> dict:
    """读取本地所有资源文件的聊天记录到DataFrame中，输出描述性信息到相应笔记中"""
    ptn4name = r"wcitems_(\w+)_(\d{4}.xlsx)"
    names = list(
        set(
            [
                re.search(ptn4name, fl).groups()[0]
                for fl in os.listdir(wcpath)
                if re.search(ptn4name, fl)
            ]
        )
    )
    print(names)
    loginstr = (
        "" if (whoami := execcmd("whoami")) and (len(whoami) == 0) else f"{whoami}"
    )
    dbfilename = f"wcitemsall_({getdevicename()})_({loginstr}).db".replace(" ", "_")
    dbname = wcpath / dbfilename
    resultdict = dict()
    for name in names[:]:
        with lite.connect(dbname) as conn:
            tbname = f"wc_{name}"
            # dbname中可能不存在指定表，没有则创建之。2025年8月22日，pixel termux发现的bug
            csql = (
                f"create table if not exists {tbname} "
                + "(id INTEGER PRIMARY KEY AUTOINCREMENT, time DATETIME, send BOOLEAN, sender TEXT, type TEXT, content TEXT)"
            )
            ifnotcreate(tbname, csql, dbname)
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
def explodedf() -> None:
    mydf = mydict["heart5"]
    mydf[mydf.time >= "2022-10-01"].sort_values("time")
