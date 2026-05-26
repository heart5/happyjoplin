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
# # 微信分享/公众号解析

# %%
"""分享消息和公众号推送的 XML 内容解析"""

# %%
import re

import itchat.storage
from bs4 import BeautifulSoup

# %%
import pathmagic

with pathmagic.context():
    from func.jpfuncs import getinivaluefromcloud
    from func.logme import log


# %% [markdown]
# ## soupclean2item(msgcontent)

# %%
def soupclean2item(msgcontent):
    rpcontent = msgcontent.replace("<![CDATA[", "").replace("]]>", "")
    if isinstance(rpcontent, str) and not rpcontent.strip().startswith("<"):
        soup = BeautifulSoup(rpcontent, "html.parser")
    else:
        soup = BeautifulSoup(rpcontent, "lxml")
    category = soup.category
    if category:
        items = category.find_all("item")
        if not items:
            items = []
    else:
        items = []

    return soup, items


# %% [markdown]
# ## parse_sharing_content(soup, items, innermsg, msg)

# %%
def parse_sharing_content(soup, items, innermsg, msg):
    """解析分享/公众号消息的 XML 内容，更新 innermsg['fmText']"""
    impimlst = re.split("[，,]", getinivaluefromcloud("webchat", "impmplist"))
    cleansender = re.split("\\(群\\)", innermsg["fmSender"])[0]

    inmtxt = innermsg["fmText"]
    if cleansender in impimlst:
        if cleansender == "微信支付" and inmtxt.endswith("转账收款汇总通知"):
            itms = soup.opitems.find_all("opitem")
            userfre = [
                f"{x.weapp_username.string}\t{x.hint_word.string}"
                for x in itms
                if x.word.string.find("收款记录") >= 0
            ][0]
            innermsg["fmText"] += f"[{soup.des.string}\n[{userfre}]]"
        elif cleansender == "微信运动" and (
            inmtxt.endswith("刚刚赞了你") or inmtxt.endswith("just liked your ranking")
        ):
            innermsg["fmText"] += f"[{soup.rankid.string}\t{soup.displayusername.string}]"
        elif cleansender == "微信运动" and (
            inmtxt.endswith("排行榜冠军") or inmtxt.startswith("Champion on")
        ):
            ydlst = []
            mni = soup.messagenodeinfo
            minestr = f"heart57479\t{mni.rankinfo.rankid.string}\t{mni.rankinfo.rank.rankdisplay.string}"
            ydlst.append(minestr)
            ril = soup.rankinfolist.find_all("rankinfo")
            for item in ril:
                istr = f"{item.username.string}\t{item.rank.rankdisplay.string}\t{item.score.scoredisplay.string}"
                ydlst.append(istr)
            yundong = "\n".join(ydlst)
            innermsg["fmText"] += f"[{yundong}]"
        elif soup.des or soup.digest:
            valuepart = soup.des or soup.digest
            innermsg["fmText"] += f"[{valuepart.string}]"
        else:
            from life.webchat import showmsgexpanddictetc

            showmsgexpanddictetc(msg)
    elif len(items) > 0:
        itemstr = "\n"
        for item in [x for x in items if x]:
            if titlestr := item.title.string:
                itemstr += titlestr + "\n"
        itemstr = itemstr[:-1]
        innermsg["fmText"] += itemstr
    elif type(msg["User"]) == itchat.storage.MassivePlatform:
        log.info(f"公众号信息\t{msg['User']}")
        from life.webchat import showmsgexpanddictetc

        showmsgexpanddictetc(msg)
