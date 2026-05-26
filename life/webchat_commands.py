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
# # 微信指令处理

# %%
"""轻行动 AI 问答 + 真元宝 工具查询的指令派发"""

# %%
import math
import os
import time
from datetime import datetime

import itchat

# %%
import pathmagic

with pathmagic.context():
    from func.first import getdirmain, touchfilepath2depth
    from func.logme import log
    from func.pdtools import db2img
    from joplin_qa_client import client, qa4joplin
    from life.wcdelay import delayimg2note, showdelayimg


# %% [markdown]
# ## _archive_reply(innermsg, inputtext='')

# %%
def _archive_reply(innermsg, inputtext=""):
    """构造合成消息并归档，等价于原 makemsg2write"""
    from life.webchat import writefmmsg2txtandmaybeevernotetoo

    nowtuple = time.time()
    nowdatetime = datetime.fromtimestamp(nowtuple)
    finnalmsg = {
        "fmId": math.floor(nowtuple),
        "fmTime": nowdatetime.strftime("%Y-%m-%d %H:%M:%S"),
        "fmSend": True,
        "fmSender": innermsg["fmSender"],
        "fmType": "Text",
        "fmText": f"{inputtext}",
    }
    writefmmsg2txtandmaybeevernotetoo(finnalmsg)


# %% [markdown]
# ## _handle_qingxingdong(msg, innermsg, qrylst)

# %%
def _handle_qingxingdong(msg, innermsg, qrylst):
    """处理"轻行动"AI 问答指令：清空/统计/提问"""
    diyihang = qrylst[0].split()
    if len(diyihang) == 1:
        response = (
            "轻行动为提示词，加空格后直接跟提问内容，不要在内容中有空格或换行"
        )
        itchat.send_msg(response, toUserName=msg["FromUserName"])
        _archive_reply(innermsg, response)
        return
    if diyihang[1] == "清空":
        result = client.clear_history()
        response = result.get("message") if result.get("success") else result.get("error")
        itchat.send_msg(response, toUserName=msg["FromUserName"])
        _archive_reply(innermsg, response)
        return
    if diyihang[1] == "统计":
        result = client.get_statistics()
        if result.get("success"):
            stats = result["statistics"]
            response = f"数据库笔记数: {stats.get('total_notes_in_db')}"
            response += f"对话历史数: {stats.get('conversation_history_count')}"
            response += f"使用模型: {stats.get('config', {}).get('chat_model')}"
        else:
            response = f"获取统计失败: {result.get('error')}"
        itchat.send_msg(response, toUserName=msg["FromUserName"])
        _archive_reply(innermsg, response)
        return
    response = qa4joplin(diyihang[1])
    itchat.send_msg(response, toUserName=msg["FromUserName"])
    _archive_reply(innermsg, response)


# %% [markdown]
# ## _handle_zhenyuanbao(msg, innermsg, men_wc, qrylst)

# %%
def _handle_zhenyuanbao(msg, innermsg, men_wc, qrylst):
    """处理"真元宝"工具查询指令：延时图/电量图/联系人/连更/连显/默认搜索"""
    diyihang = qrylst[0].split()
    if len(diyihang) == 1:
        if len(qrylst) == 1 or qrylst[1].strip() == "":
            from work.zymessage import searchcustomer

            rstfile, rst = searchcustomer()
        else:
            from work.zymessage import searchcustomer

            qrystr = qrylst[1].strip()
            rstfile, rst = searchcustomer(qrystr.split())
    elif diyihang[1] == "延时图":
        delaydbname = getdirmain() / "data" / "db" / f"wcdelay_{men_wc}.db"
        imgwcdelay, _ = showdelayimg(delaydbname)
        imgwcdelay = os.path.abspath(imgwcdelay)
        itchat.send_image(imgwcdelay, toUserName=msg["FromUserName"])
        delayimg2note(men_wc)
        _archive_reply(innermsg, imgwcdelay)
        return
    elif diyihang[1] == "电量图":
        from etc.battery_manage import showbattinfoimg

        delaydbname = touchfilepath2depth(getdirmain() / "data" / "db" / "batteryinfo.db")
        imgbattinfo = showbattinfoimg(delaydbname)
        imgbattinforel = os.path.relpath(imgbattinfo)
        itchat.send_image(imgbattinforel, toUserName=msg["FromUserName"])
        _archive_reply(innermsg, imgbattinforel)
        return
    elif diyihang[1] == "联系人":
        from life.phonecontact import showphoneinfoimg

        contactinfo = showphoneinfoimg()
        imgcontactinforel = os.path.relpath(contactinfo)
        itchat.send_image(imgcontactinforel, toUserName=msg["FromUserName"])
        _archive_reply(innermsg, imgcontactinforel)
        return
    elif diyihang[1] == "连更":
        from life.wccontact import updatectdf

        updatectdf()
        return
    elif diyihang[1] == "连显":
        from life.wccontact import getctdf, showwcsimply

        frddfread = getctdf()
        imgwc = db2img(showwcsimply(frddfread))
        imgwcrel = os.path.relpath(imgwc)
        itchat.send_image(imgwcrel, toUserName=msg["FromUserName"])
        _archive_reply(innermsg, imgwcrel)
        return

    rst = "未知指令。可用子命令：延时图/电量图/联系人/连更/连显，或不带子命令直接搜索客户"
    itchat.send_msg(rst, toUserName=msg["FromUserName"])
    _archive_reply(innermsg, rst)


# %% [markdown]
# ## dispatch(msg, innermsg, men_wc) -> bool

# %%
def dispatch(msg, innermsg, men_wc):
    """解析消息首行，匹配指令并执行。返回 True 表示已处理，False 表示非指令。

    split() 自动合并连续空白字符，兼容"轻行动  清空"等多空格输入。
    """
    text = msg["Text"]
    qrylst = [x.strip() for x in text.split("\n")]
    qrylst = [x for x in qrylst if x]
    if not qrylst:
        return False
    log.debug(f"{qrylst}")

    first_word = qrylst[0].split()[0] if qrylst[0].split() else ""
    if first_word == "轻行动":
        _handle_qingxingdong(msg, innermsg, qrylst)
        return True
    if first_word == "真元宝":
        _handle_zhenyuanbao(msg, innermsg, men_wc, qrylst)
        return True
    return False
