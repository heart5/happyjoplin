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
# # 微信聊天记录深度利用

# %%
"""聊天记录挖掘与提炼：步数分析、词云、转账、红包、活跃度等"""

# %%
import base64
import io
import os
import re
import sqlite3 as lite
from datetime import datetime

import arrow
import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import pandas as pd
from wordcloud import WordCloud

# %%
import pathmagic

with pathmagic.context():
    from func.configpr import getcfpoptionvalue, setcfpoptionvalue
    from func.first import getdirmain
    from func.getid import getdevicename
    from func.jpfuncs import createnote, getnote, searchnotebook, searchnotes
    from func.logme import log
    from func.sysfunc import execcmd, not_IPython
    from func.wrapfuncs import timethis
    from life.wc2note import items_to_df


# %% [markdown]
# ## all2df(name, wcdatapath) — 合并聊天记录


# %%
@timethis
def all2df(name, wcdatapath):
    """获取全部聊天记录并返回DataFrame。

    从最新文本文件和SQLite数据库中读取，合并去重，时间字段统一为int(timestamp)。
    """
    wc_txt_df = items_to_df(wcdatapath / f"chatitems({name}).txt")
    wc_txt_df["send"] = wc_txt_df["send"].apply(lambda x: 1 if x else 0)
    wc_txt_df["time"] = wc_txt_df["time"].apply(lambda x: (int(arrow.get(x, tzinfo="local").timestamp())))
    log.info(f"文本数据的最新记录时间为：{datetime.fromtimestamp(wc_txt_df['time'].max())}")
    loginstr = "" if (whoami := execcmd("whoami")) and (len(whoami) == 0) else f"{whoami}"
    dbfilename = f"wcitemsall_({getdevicename()})_({loginstr}).db".replace(" ", "_")
    dbname = os.path.abspath(wcdatapath / dbfilename)
    db_all_df = pd.DataFrame()
    with lite.connect(dbname) as conn:
        tablename = f"wc_{name}"
        sql_query = pd.read_sql_query(f"select * from {tablename}", conn)
        db_all_df = pd.DataFrame(sql_query, columns=["time", "send", "sender", "type", "content"])
        last_from_db = db_all_df.iloc[-1, 0]
        if isinstance(last_from_db, int):
            log.info(f"数据库数据的最后一条记录时间为：{datetime.fromtimestamp(last_from_db)}")
        else:
            log.info(f"数据库数据的最后一条记录时间为：{last_from_db}")
        df_tmp = db_all_df[db_all_df["time"].apply(lambda x: not isinstance(x, int))]
        log.critical(
            f"从数据库{dbfilename}中读取数据time字段为【文本】的数据共有{df_tmp.shape[0]}条，"
            f"最大值为：{df_tmp['time'].max()}，最小值为：{df_tmp['time'].min()}"
        )
        db_all_df["time"] = db_all_df["time"].apply(
            lambda x: int(arrow.get(x, tzinfo="local").timestamp()) if not isinstance(x, int) else x
        )
    dfcombine = pd.concat([db_all_df, wc_txt_df], ignore_index=True)
    items_all_num = dfcombine.shape[0]
    dfdone = dfcombine.drop_duplicates()
    if items_all_num != dfdone.shape[0]:
        df_dup = dfcombine[dfcombine.duplicated()]
        log.critical(
            f"从数据库{dbfilename}中读取数据并和文本中记录合并后，"
            f"重复的数据记录共有{df_dup.shape[0]}条，"
            f"最大值为：{datetime.fromtimestamp(int(df_dup['time'].max()))}，"
            f"最小值为：{datetime.fromtimestamp(int(df_dup['time'].min()))}"
        )
        log.critical(f"合并数据记录共有{items_all_num}条，去重后有效数据有{dfdone.shape[0]}条！")
    dfcombine.drop_duplicates(inplace=True)
    print(dfcombine.dtypes)
    dfcombine.sort_values(["time"], ascending=False, inplace=True)
    return dfcombine


# %% [markdown]
# ## all2spdf(wc_all_df) — 筛出微信运动记录


# %%
def all2spdf(wc_all_df):
    """从全部聊天记录中筛出微信运动步数排行消息。"""
    sport_df = wc_all_df[wc_all_df.sender.str.contains("微信运动")]
    sport_df.loc[:, "time"] = sport_df["time"].apply(lambda x: datetime.fromtimestamp(x))
    spdf = sport_df.loc[:, ["time", "content"]]
    spdf.loc[:, "content"] = spdf["content"].apply(lambda x: re.sub(r"(\[\w+前\]|\[刚才\])?", "", x))
    num4all = spdf.shape[0]
    print(spdf[spdf.duplicated()])
    spdf.drop_duplicates(inplace=True)
    print(f"数据有{num4all}条，去重后有{spdf.shape[0]}条")
    return spdf


# %% [markdown]
# ## spdf2liked(spdf) — 提取点赞记录


# %%
def spdf2liked(spdf):
    """从微信运动记录中提取点赞数据，解析出点赞人和微信ID。"""
    sp_liked_df = spdf[spdf.content.str.contains("just liked|刚刚赞了", regex=True)]

    duptimelst = sp_liked_df[sp_liked_df.time.duplicated()]["time"].values
    dup_mul_df = sp_liked_df[sp_liked_df["time"].apply(lambda x: x in duptimelst)]
    dupindexshortlist = dup_mul_df[
        dup_mul_df["content"].apply(lambda x: x.endswith("just liked your ranking"))
    ].index.values
    right_index_lst = [x for x in sp_liked_df.index.values if x not in dupindexshortlist]
    sp_liked_df = sp_liked_df.loc[right_index_lst, :]

    sp_liked_df.loc[:, "friend"] = sp_liked_df["content"].apply(lambda x: re.split(r"\W|刚刚赞了", x)[0])
    sp_liked_df.loc[:, "wcid"] = sp_liked_df["content"].apply(lambda x: re.split(r"\W", x)[-2])

    return sp_liked_df


# %% [markdown]
# ## makecloudimg(mywcdict, bgimg) — 生成词云图


# %%
def makecloudimg(mywcdict, bgimg):
    """根据频率字典和背景图生成词云，返回base64编码的PNG。"""
    font_path = fm.findfont(fm.FontProperties())

    plt.figure(figsize=(10, 8), dpi=600)
    mask = plt.imread(bgimg)
    wc = WordCloud(
        mask=mask,
        font_path=font_path,
        width=800,
        height=500,
        scale=2,
        mode="RGBA",
        background_color="white",
    )
    wc = wc.generate_from_frequencies(mywcdict)
    plt.imshow(wc, interpolation="bilinear")
    plt.axis("off")
    buffer = io.BytesIO()
    plt.savefig(buffer, format="png")
    buffer.seek(0)
    image_base64 = base64.b64encode(buffer.read()).decode()
    plt.close()

    return image_base64


# %% [markdown]
# ## wcliked2note() — 点赞词云上云（实验性）


# %%
@timethis
def wcliked2note():
    """综合输出微信运动点赞好友云图并更新至笔记。

    注意：此函数为前期实验代码，依赖的健康数据处理链路（health_id、
    gethealthdatafromnote、hdf2imgbase64等）尚未完整实现，暂时不可直接调用。
    """
    login_user = execcmd("whoami")
    namestr = "happyjp_life"
    section = f"health_{login_user}"
    notestat_title = f"微信运动点赞好友云图【{login_user}】"

    if not (wc_sp_liked_items_num := getcfpoptionvalue(namestr, section, "wc_sp_liked_items_num")):
        wc_sp_liked_items_num = 0
    note = getnote(health_id)  # noqa: F821 — 待实现的健康笔记ID
    noteupdatetimewithzone = arrow.get(note.updated_time, tzinfo="local")
    if (noteupdatetimewithzone.timestamp() == wc_sp_liked_items_num) and (not_IPython()):
        log.info(f"健康运动笔记无更新【最新更新时间为：{noteupdatetimewithzone}】，跳过本次轮询和相应动作。")
        return

    hdf = gethealthdatafromnote(note.id)  # noqa: F821 — 待实现
    image_base64 = hdf2imgbase64(hdf)  # noqa: F821 — 待实现
    nbid = searchnotebook("康健")
    if not (healthstat_cloud_id := getcfpoptionvalue(namestr, section, "healthstat_cloud_id")):
        healthnotefindlist = searchnotes(f"{notestat_title}")
        if len(healthnotefindlist) == 0:
            healthstat_cloud_id = createnote(title=notestat_title, parent_id=nbid, imgdata64=image_base64)
            log.info(f"新的健康动态笔记“{healthstat_cloud_id}”新建成功！")
        else:
            healthstat_cloud_id = healthnotefindlist[-1].id
        setcfpoptionvalue(namestr, section, "healthstat_cloud_id", f"{healthstat_cloud_id}")

    if not noteid_used(healthstat_cloud_id):  # noqa: F821 — 待实现
        healthstat_cloud_id = createnote(title=notestat_title, parent_id=nbid, imgdata64=image_base64)
    else:
        healthstat_cloud_id, res_lst = updatenote_imgdata(  # noqa: F821 — 待实现
            noteid=healthstat_cloud_id, parent_id=nbid, imgdata64=image_base64
        )
    setcfpoptionvalue(namestr, section, "healthstat_cloud_id", f"{healthstat_cloud_id}")
    setcfpoptionvalue(
        namestr,
        section,
        "health_cloud_updatetimestamp",
        str(noteupdatetimewithzone.timestamp()),
    )
    log.info(
        f"健康运动笔记【更新时间：{arrow.get(wc_sp_liked_items_num, tzinfo='local')}-》{noteupdatetimewithzone}】。"
    )


# %% [markdown]
# ## 主函数

# %%
if __name__ == "__main__":
    log.info(f"运行文件\t{__file__}")

    wcdatapath = getdirmain() / "data" / "webchat"
    owner = "白晔峰"
    wc_all_df = all2df(owner, wcdatapath)
    spdf = all2spdf(wc_all_df)
    sp_liked_df = spdf2liked(spdf)
    print(f"步数记录: {spdf.shape[0]}条, 点赞记录: {sp_liked_df.shape[0]}条")

    log.info(f"文件\t{__file__}\t运行结束。")
