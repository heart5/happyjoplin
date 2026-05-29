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
    from func.first import getdirmain
    from func.logme import log
    from func.pdtools import db2img
    from joplin_qa_client import client, qa4joplin
    from life.wcdelay import delay_img_to_note, showdelayimg


# %% [markdown]
# ## _archive_reply(innermsg, inputtext='')

# %%
def _archive_reply(innermsg, inputtext=""):
    """构造合成消息并归档，等价于原 makemsg2write"""
    from life.webchat import _archive_msg

    nowtuple = time.time()
    nowdatetime = datetime.fromtimestamp(nowtuple)
    final_msg = {
        "fmId": math.floor(nowtuple),
        "fmTime": nowdatetime.strftime("%Y-%m-%d %H:%M:%S"),
        "fmSend": True,
        "fmSender": innermsg["fmSender"],
        "fmType": "Text",
        "fmText": f"{inputtext}",
    }
    _archive_msg(final_msg)


# %% [markdown]
# ## _handle_qingxingdong(msg, innermsg, query_parts)

# %%
def _handle_qingxingdong(msg, innermsg, query_parts):
    """处理"轻行动"AI 问答指令：清空/统计/提问"""
    first_word = query_parts[0].split()
    if len(first_word) == 1:
        response = (
            "轻行动为提示词，加空格后直接跟提问内容，不要在内容中有空格或换行"
        )
        itchat.send_msg(response, toUserName=msg["FromUserName"])
        _archive_reply(innermsg, response)
        return
    if first_word[1] == "清空":
        result = client.clear_history()
        response = result.get("message") if result.get("success") else result.get("error")
        itchat.send_msg(response, toUserName=msg["FromUserName"])
        _archive_reply(innermsg, response)
        return
    if first_word[1] == "统计":
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
    response = qa4joplin(first_word[1])
    itchat.send_msg(response, toUserName=msg["FromUserName"])
    _archive_reply(innermsg, response)


# %% [markdown]
# ## _handle_zhenyuanbao(msg, innermsg, men_wc, query_parts)

# %%
def _handle_zhenyuanbao(msg, innermsg, men_wc, query_parts):
    """处理"真元宝"工具查询指令：延时图/联系人/连更/连显"""
    first_word = query_parts[0].split()
    if len(first_word) == 1:
        rst = "真元宝可用子命令：延时图/联系人/连更/连显/退出"
        itchat.send_msg(rst, toUserName=msg["FromUserName"])
        _archive_reply(innermsg, rst)
        return
    elif first_word[1] == "延时图":
        delaydbname = getdirmain() / "data" / "db" / f"wcdelay_{men_wc}.db"
        imgwcdelay, _ = showdelayimg(delaydbname)
        imgwcdelay = os.path.abspath(imgwcdelay)
        itchat.send_image(imgwcdelay, toUserName=msg["FromUserName"])
        delay_img_to_note(men_wc)
        _archive_reply(innermsg, imgwcdelay)
        return
    elif first_word[1] == "联系人":
        from life.phonecontact import showphoneinfoimg

        contactinfo = showphoneinfoimg()
        imgcontactinforel = os.path.relpath(contactinfo)
        itchat.send_image(imgcontactinforel, toUserName=msg["FromUserName"])
        _archive_reply(innermsg, imgcontactinforel)
        return
    elif first_word[1] == "连更":
        from life.wccontact import updatectdf

        updatectdf()
        return
    elif first_word[1] == "连显":
        from life.wccontact import getctdf, showwcsimply

        frddfread = getctdf()
        imgwc = db2img(showwcsimply(frddfread))
        imgwcrel = os.path.relpath(imgwc)
        itchat.send_image(imgwcrel, toUserName=msg["FromUserName"])
        _archive_reply(innermsg, imgwcrel)
        return

    elif first_word[1] == "退出":
        response = "真元宝系统正在退出…"
        itchat.send_msg(response, toUserName=msg["FromUserName"])
        _archive_reply(innermsg, response)
        log.info("根据指令「真元宝 退出」登出itchat微信web协议")
        itchat.logout()
        return

    rst = "未知指令。可用子命令：延时图/联系人/连更/连显/退出"
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
    query_parts = [x.strip() for x in text.split("\n")]
    query_parts = [x for x in query_parts if x]
    if not query_parts:
        return False
    log.debug(f"{query_parts}")

    first_word = query_parts[0].split()[0] if query_parts[0].split() else ""
    if first_word == "轻行动":
        _handle_qingxingdong(msg, innermsg, query_parts)
        return True
    if first_word == "真元宝":
        _handle_zhenyuanbao(msg, innermsg, men_wc, query_parts)
        return True
    return False
