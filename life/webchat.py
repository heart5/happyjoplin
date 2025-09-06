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
# # 微信大观园

# %%
"""
微信大观园，工作优先，娱乐生活
"""

# %% [markdown]
# ## 库引入

# %%
import datetime
import logging
import math
import os

# import arrow
import re
import sys
import time
from collections import deque

import itchat
import itchat.storage

# from itchat.content import *
from bs4 import BeautifulSoup
from itchat.content import (
    ATTACHMENT,
    CARD,
    FRIENDS,
    MAP,
    NOTE,
    PICTURE,
    RECORDING,
    SHARING,
    TEXT,
    VIDEO,
)

# %%
import pathmagic

with pathmagic.context():
    # from work.weixinzhang import showjinzhang, showshoukuan
    # import evernote.edam.type.ttypes as ttypes
    # from work.zymessage import searchcustomer, searchqiankuan, searchpinxiang
    from etc.getid import getdeviceid, getdevicename
    from func.configpr import getcfpoptionvalue, setcfpoptionvalue
    from func.datatools import readfromtxt, write2txt
    from func.datetimetools import gethumantimedelay
    from func.first import dirmainpath, getdirmain, touchfilepath2depth
    from func.jpfuncs import (
        createnote,
        getinivaluefromcloud,
        updatenote_body,
        updatenote_title,
    )
    from func.logme import log
    from func.nettools import trycounttimes2

    # from life.wccontact import updatectdf, getctdf, showwcsimply
    # from life.phonecontact import showphoneinfoimg
    # from etc.battery_manage import showbattinfoimg
    from func.pdtools import db2img
    from func.sysfunc import execcmd, listallloghander, not_IPython, uuid3hexstr
    from func.termuxtools import termux_sms_send
    from life.wcdelay import delayimg2note, inserttimeitem2db, showdelayimg


# %% [markdown]
# ## 功能函数

# %% [markdown]
# ### get_response(msg)


# %%
@trycounttimes2("微信服务器")
def get_response(msg):
    txt = msg["Text"]
    # print(msg)
    return txt


# %% [markdown]
# ### showmsgexpanddicttetc()


# %%
def showmsgexpanddictetc(msg):
    """
    列印dict中的所有属性和值，对于dict类型的子元素，则再展开一层
    """
    # print(msg)
    for item in msg:
        # print(item)
        # if item.lower().find('name') < 0:
        # continue
        print(f"{item}\t{type(msg[item])}", end="\t")
        if type(msg[item]) in [
            dict,
            itchat.storage.templates.Chatroom,
            itchat.storage.templates.User,
        ]:
            print(len(msg[item]))
            for child in msg[item]:
                childmsg = msg[item][child]
                print(f"\t{child}\t{type(childmsg)}", end="\t")
                if type(childmsg) in [
                    dict,
                    itchat.storage.templates.User,
                    itchat.storage.templates.ContactList,
                ]:
                    lenchildmsg = len(childmsg)
                    print(lenchildmsg)
                    lmt = getinivaluefromcloud("webchat", "itemshowinmsg")
                    shownum = lmt if lenchildmsg > lmt else lenchildmsg
                    print(f"\t\t{childmsg[:shownum]}")
                    # print(f'\t\t{childmsg}')
                else:
                    print(f"\t\t{childmsg}")

        else:
            print(msg[item])


# %% [markdown]
# ### formatmsg(msg)


# %%
def formatmsg(msg):
    """
    格式化并重构msg,获取合适用于直观显示的用户名，对公众号和群消息特别处置
    """
    timetuple = time.localtime(msg["CreateTime"])
    timestr = time.strftime("%Y-%m-%d %H:%M:%S", timetuple)
    # print(msg['CreateTime'], timetuple, timestr)
    men_wc = getcfpoptionvalue("happyjpwebchat", get_host_uuid(), "host_nickname")
    # 信息延时登记入专门数据库文件
    dbname = touchfilepath2depth(getdirmain() / "data" / "db" / f"wcdelay_{men_wc}.db")
    inserttimeitem2db(dbname, msg["CreateTime"])
    # owner = itchat.web_init()
    meu_wc = getcfpoptionvalue("happyjpwebchat", get_host_uuid(), "host_username")
    send = msg["FromUserName"] == meu_wc
    if "NickName" in msg["User"].keys():
        showname = msg["User"]["NickName"]
        if len(msg["User"]["RemarkName"]) > 0:
            showname = msg["User"]["RemarkName"]
    elif "UserName" in msg["User"].keys():
        showname = msg["User"]["UserName"]
    elif "userName" in msg["User"].keys():
        showname = msg["User"]["userName"]
    else:
        showname = ""
        log.warning("NickName或者UserName或者userName键值不存在哦")
        showmsgexpanddictetc(msg)

    # 过滤掉已经研究过属性公众号信息，对于尚未研究过的显示详细信息
    ignoredmplist = getinivaluefromcloud("webchat", "ignoredmplist")
    imlst = re.split("[，,]", ignoredmplist)
    ismp = type(msg["User"]) == itchat.storage.MassivePlatform
    if ismp and (showname not in imlst):
        showmsgexpanddictetc(msg)
        # print(f"{showname}\t{imlst}")

    # 处理群消息
    if type(msg["User"]) == itchat.storage.templates.Chatroom:
        isfrom = msg["FromUserName"].startswith("@@")
        isto = msg["ToUserName"].startswith("@@")
        # qunmp = isfrom or isto
        # showmsgexpanddictetc(msg)
        if isfrom:
            # print(f"（群)\t{msg['ActualNickName']}", end='')
            showname += f"(群){msg['ActualNickName']}"
        elif isto:
            # print(f"（群）\t{msg['User']['Self']['NickName']}", end='')
            showname += f"(群){msg['User']['Self']['NickName']}"
        # print(f"\t{msg['Type']}\t{msg['MsgType']}\t{msg['Text']}")
        # print(f"\t{send}\t{msg['Type']}\t{msg['Text']}")
    fmtext = msg["Text"]

    finnalmsg = {
        "fmId": msg["MsgId"],
        "fmTime": timestr,
        "fmSend": send,
        "fmSender": showname,
        "fmType": msg["Type"],
        "fmText": fmtext,
    }

    return finnalmsg


# %% [markdown]
# ### deque2dict()


# %%
def deque2dict(inputdeque):
    sondict = {}
    for item in inputdeque:
        sondict.update(item)
    return sondict


# %% [markdown]
# ### def writefmmsg2txtandmaybeevernotetoo(inputformatmsg):


# %%
def writefmmsg2txtandmaybeevernotetoo(inputformatmsg):
    """
    把格式化好的微信聊天记录写入文件，并根据云端ini设定更新相应账号的聊天笔记
    """
    # 把聊天记录以dict的格式缓存入队列，fmId提取出来做key
    global recentmsg_deque
    onedict = {}
    onedict[inputformatmsg["fmId"]] = {
        item: inputformatmsg[item] for item in inputformatmsg if item != "fmId"
    }
    recentmsg_deque.append(onedict)
    if (recentnum := len(recentmsg_deque)) != 30:
        print(
            f"缓存聊天记录数量为：\t{recentnum}，fmId号列表\t{list(deque2dict(recentmsg_deque))}"
        )
    # 判断是否延时并增加提示到条目内容中
    if humantimestr := gethumantimedelay(inputformatmsg["fmTime"]):
        inputformatmsg["fmText"] = f"[{humantimestr}]" + inputformatmsg["fmText"]
    msgcontent = ""
    for item in inputformatmsg:
        if item == "fmId":
            continue
        msgcontent += f"{inputformatmsg[item]}\t"
    # 去除最后一个\t
    msgcontent = msgcontent[:-1]
    print(f"{msgcontent}")

    men_wc = getcfpoptionvalue("happyjpwebchat", get_host_uuid(), "host_nickname")
    chattxtfilename = str(
        getdirmain() / "data" / "webchat" / f"chatitems({men_wc}).txt"
    )
    chatitems = readfromtxt(chattxtfilename)
    # 倒叙插入，最新的显示在最上面
    chatitems.insert(0, msgcontent)
    write2txt(chattxtfilename, chatitems)

    # if inputformatmsg['fmText'].startswith('收到转账'):
    # showjinzhang()

    # if inputformatmsg['fmText'].startswith('微信支付收款'):
    # showshoukuan()

    if (men_wc is None) or (len(men_wc) == 0):
        log.critical(f"登录名{men_wc}为空！！！")
        return
    notetitle = f"微信记录【{men_wc}】 -（{getdevicename()}-{execcmd('whoami')}））"
    wcnote_title = men_wc + f"_{getdeviceid()}"
    if (chatnoteid := getinivaluefromcloud("webchat", wcnote_title)) is None:
        if (
            chatnoteid := getcfpoptionvalue("happyjpwebchat", "webchat", wcnote_title)
        ) is None:
            chatnoteid = createnote(title=notetitle)
            setcfpoptionvalue(
                "happyjpwebchat", "webchat", wcnote_title, str(chatnoteid)
            )
    updatefre = getinivaluefromcloud("webchat", "updatefre")
    showitemscount = getinivaluefromcloud("webchat", "showitems")
    # print(f"{type(showitemscount)}\t{showitemscount}")
    neirong = "\n".join(chatitems[:showitemscount])
    # neirongplain = neirong.replace('<', '《').replace('>', '》') \
    #     .replace('=', '等于').replace('&', '并或')
    # global note_store
    if (len(chatitems) % updatefre) == 0:
        # init_info = itchat.web_init()
        # print(f"每{updatefre}条信息，再次初始化一次，返回值为：\t{init_info.keys()}")
        updatenote_title(noteid=chatnoteid, titlestr=notetitle)
        updatenote_body(noteid=chatnoteid, bodystr=neirong)


# %%
def getsendernick(msg):
    """
    获取发送者昵称并返回
    """
    # sendernick = itchat.search_friends(userName=msg['FromUserName'])
    if msg["FromUserName"].startswith("@@"):
        qun = itchat.search_chatrooms(userName=msg["FromUserName"])
        sendernick = qun["NickName"] + "(群)" + msg["ActualNickName"]
    else:
        senderuser = itchat.search_friends(userName=msg["FromUserName"])
        if senderuser is None:
            return "self?"
        if len(senderuser["RemarkName"]) == 0:
            sendernick = senderuser["NickName"]
        else:
            sendernick = senderuser["RemarkName"]
    # sendernick = itchat.search_friends(userName=msg['FromUserName'])['NickName']
    return sendernick


# %% [markdown]
# ### @itchat.msg_register([CARD, FRIENDS], isFriendChat=True, isGroupChat=True, isMpChat=True)


# %%
@itchat.msg_register(
    [CARD, FRIENDS], isFriendChat=True, isGroupChat=True, isMpChat=True
)
def tuling_reply(msg):
    # showmsgexpanddictetc(msg)
    writefmmsg2txtandmaybeevernotetoo(formatmsg(msg))


# %% [markdown]
# ### @itchat.msg_register([NOTE], isFriendChat=True, isGroupChat=True, isMpChat=True)


# %%
@itchat.msg_register([NOTE], isFriendChat=True, isGroupChat=True, isMpChat=True)
def note_reply(msg):
    """
    处理note类型信息，对微信转账记录深入处置
    """
    global recentmsg_deque
    # showmsgexpanddictetc(msg)
    innermsg = formatmsg(msg)
    if ("撤回了一条消息" in msg["Content"]) or ("recalled a message" in msg["Content"]):
        old_msg_id = re.search(r"\<msgid\>(.*?)\<\/msgid\>", msg["Content"]).group(1)
        msg_information = deque2dict(recentmsg_deque)
        old_msg = msg_information.get(old_msg_id)
        print(old_msg)
        if old_msg is None:
            print(f"未找到有效msg_id，直接返回。原始信息如下：")
            print(msg)
            return

        msg_body = f"{old_msg.get('fmSender')}撤回了 {old_msg.get('fmType')} 消息\n{old_msg.get('fmTime')}\
            \n ⇣ \n{old_msg.get('fmText')}"

        # if old_msg['fmType'] == "Sharing":
        #     msg_body += "\n就是这个链接➣ " + old_msg.get('fmText')
        itchat.send_msg(msg_body, toUserName="filehelper")

        tpdict = {
            "Picture": "img",
            "Video": "vid",
            "Recording": "fil",
            "Attachment": "fil",
        }
        if (fileprefix := tpdict.get(old_msg["fmType"])) is not None:
            fileabspath = os.path.abspath(getdirmain() / f"{old_msg['fmText']}")
            file = "@%s@%s" % (fileprefix, fileabspath)
            response_send = itchat.send(file, toUserName="filehelper")
            log.critical(f"发送文件的返回值:\t{response_send}")
            log.critical(f"撤回的文件【{fileprefix}\t{fileabspath}】发送给文件助手")
        log.critical(f"处理了撤回的消息：{old_msg}")
        msg_information.pop(old_msg_id)
        recentmsg_deque.clear()
        for k, v in msg_information.items():
            recentmsg_deque.append({k: v})
        print(
            f"处理该撤回记录后缓存记录数量为：\t{len(recentmsg_deque)}，fmId列表\t{list(deque2dict(recentmsg_deque))}"
        )

    if msg["FileName"] == "微信转账":
        ptn = re.compile("<pay_memo><!\\[CDATA\\[(.*)\\]\\]></pay_memo>")
        pay = re.search(ptn, msg["Content"])[1]
        innermsg["fmText"] += f"[{pay}]"
    if msg["FileName"].find("红包") >= 0:
        showmsgexpanddictetc(msg)
    writefmmsg2txtandmaybeevernotetoo(innermsg)


# %% [markdown]
# ### @itchat.msg_register([MAP], isFriendChat=True, isGroupChat=True, isMpChat=True)


# %%
@itchat.msg_register([MAP], isFriendChat=True, isGroupChat=True, isMpChat=True)
def map_reply(msg):
    # showmsgexpanddictetc(msg)
    innermsg = formatmsg(msg)
    gps = msg["Url"].split("=")[1]
    # print(f"[{gps}]")
    innermsg["fmText"] = innermsg["fmText"] + f"[{gps}]"
    writefmmsg2txtandmaybeevernotetoo(innermsg)


# %% [markdown]
# ### @itchat.msg_register([PICTURE, RECORDING, ATTACHMENT, VIDEO], isFriendChat=True, isGroupChat=True, isMpChat=True)


# %%
@itchat.msg_register(
    [PICTURE, RECORDING, ATTACHMENT, VIDEO],
    isFriendChat=True,
    isGroupChat=True,
    isMpChat=True,
)
def fileetc_reply(msg):
    innermsg = formatmsg(msg)
    createtimestr = time.strftime("%Y%m%d", time.localtime(msg["CreateTime"]))
    filepath = getdirmain() / "img" / "webchat" / createtimestr
    filepath = filepath / f"{innermsg['fmSender']}_{msg['FileName']}"
    touchfilepath2depth(filepath)
    # log.info(f"保存文件（{innermsg['fmType']}）：\t{str(filepath)}")
    print(f"保存文件（{innermsg['fmType']}）：\t{str(filepath)}")
    msg["Text"](str(filepath))
    innermsg["fmText"] = str(filepath)

    writefmmsg2txtandmaybeevernotetoo(innermsg)


# %% [markdown]
# ### soupclean2item(msgcontent)


# %%
def soupclean2item(msgcontent):
    rpcontent = msgcontent.replace("<![CDATA[", "").replace("]]>", "")
    # rpcontent = msgcontent
    if isinstance(rpcontent, str) and not rpcontent.strip().startswith("<"):
        # Handle the case where the input is not HTML
        # log.info("Warning: Input does not appear to be valid HTML.")
        # Optionally, you can still parse it as plain text
        soup = BeautifulSoup(rpcontent, "html.parser")  # or just handle it differently
    else:
        soup = BeautifulSoup(rpcontent, "lxml")
    # soup = BeautifulSoup(rpcontent, 'lxml')
    category = soup.category
    if category:
        items = category.find_all("item")
        if not items:
            items = []
    else:
        items = []

    return soup, items


# %% [markdown]
# ### @itchat.msg_register([SHARING], isFriendChat=True, isGroupChat=True, isMpChat=True)


# %%
@itchat.msg_register([SHARING], isFriendChat=True, isGroupChat=True, isMpChat=True)
def sharing_reply(msg):
    sendernick = getsendernick(msg)
    print(sendernick)
    innermsg = formatmsg(msg)

    # showmsgexpanddictetc(msg)
    # 处理火界麻将战绩网页
    # http://zj.lgmob.com/h5_whmj_qp/fks0_eb81c193dea882941fe13dfa5be24a11.html
    # ptn = re.compile("h5_whmj_qp/fks0_")
    # http://s0.lgmob.com/h5_whmj_qp/zhanji/index.php?id=fks0_eb81c193dea882941fe13dfa5be24a11
    # ptn = re.compile("h5_whmj_qp/zhanji/index.php\\?id=")
    # men_wc = getcfpoptionvalue('happyjpwebchat', get_host_uuid(), 'host_nickname')
    #     msgurl = msg['Url']
    #     print(msgurl)
    #     if (ulst := splitmjurlfromtext(msgurl)) and (len(ulst) > 0):
    #         if getinivaluefromcloud("game", "forceupdateexcel"):
    #             descstr = ''
    #             for sp in ulst:
    #                 tmpurl, tmpdescstr = geturlcontentwrite2excel(men_wc, sp)
    #                 descstr += tmpdescstr
    #         else:
    #             descstr = updateurllst(men_wc, ulst)
    #         outstr = f"【Text】信息中发现新的火界麻将战绩网页链接并处理：\t{descstr}"
    #         itchat.send_msg(f'({sendernick})' + outstr)
    #         makemsg2write(innermsg, outstr)
    #         makemsg2write(innermsg, msgurl)

    #     # 处理开房链接
    #     # http://s0.lgmob.com/h5_whmj_qp/?d=217426
    #     ptnfang = re.compile("s0.lgmob.com/h5_whmj_qp/\\?d=(\d+)")
    #     if re.findall(ptnfang, msgurl):
    #         if msgurl.startswith('http'):
    #             outstr = f"【Sharing】信息中发现新的火界麻将开房链接：\t{re.findall(ptnfang, msgurl)[0]}: {msg['Text']}"
    #             # log.info(outstr)
    #             itchat.send_msg(f'({sendernick})'  +outstr)
    #             makemsg2write(innermsg, outstr)

    soup, items = soupclean2item(msg["Content"])

    # 过滤掉已经研究过属性公众号信息，对于尚未研究过的显示详细信息
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
        # elif innermsg["fmText"].endswith("微信支付凭证"):
        # innermsg['fmText'] = innermsg['fmText']+f"[{soup.des.string}]"
        elif cleansender == "微信运动" and (
            inmtxt.endswith("刚刚赞了你") or inmtxt.endswith("just liked your ranking")
        ):
            innermsg["fmText"] += (
                f"[{soup.rankid.string}\t{soup.displayusername.string}]"
            )
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
            showmsgexpanddictetc(msg)
    elif len(items) > 0:
        itemstr = "\n"
        for item in [x for x in items if x]:
            if titlestr := item.title.string:
                itemstr += titlestr + "\n"
        # 去掉尾行的回车
        itemstr = itemstr[:-1]
        innermsg["fmText"] += itemstr
    elif type(msg["User"]) == itchat.storage.MassivePlatform:
        log.info(f"公众号信息\t{msg['User']}")
        showmsgexpanddictetc(msg)

    writefmmsg2txtandmaybeevernotetoo(innermsg)


# %% [markdown]
# ### makemsg2write(innermsg, inputext='')


# %%
def makemsg2write(innermsg, inputtext=""):
    """
    make record then write to items for chatitems
    """
    nowtuple = time.time()
    nowdatetime = datetime.datetime.fromtimestamp(nowtuple)
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
# ### @itchat.msg_register([TEXT], isFriendChat=True, isGroupChat=True, isMpChat=True)


# %%
@itchat.msg_register([TEXT], isFriendChat=True, isGroupChat=True, isMpChat=True)
def text_reply(msg):
    sendernick = getsendernick(msg)
    innermsg = formatmsg(msg)
    soup, items = soupclean2item(msg["Content"])

    # 是否在清单中
    mp4txtlist = re.split("[，,]", getinivaluefromcloud("webchat", "mp4txtlist"))
    cleansender = re.split("\\(群\\)", innermsg["fmSender"])[0]
    if cleansender in mp4txtlist:
        itemstr = "\n"
        for item in items:
            itemstr += item.title.string + "\n"
        # 去掉尾行的回车
        itemstr = itemstr[:-1]
        innermsg["fmText"] = itemstr

    writefmmsg2txtandmaybeevernotetoo(innermsg)

    # 特定指令则退出
    if msg["Text"] == "退出小元宝系统":
        log.info("根据指令退出小元宝系统")
        itchat.logout()

    # 处理火界麻将战绩网页
    #     men_wc = getcfpoptionvalue('happyjpwebchat', get_host_uuid(), 'host_nickname')
    # #     ptn = re.compile("h5_whmj_qp/(zhanji/index.php\\?id=|fks0_)")
    #     msgtxt = msg['Text']
    #     if (ulst := splitmjurlfromtext(msgtxt)) and (len(ulst) > 0):
    #         if getinivaluefromcloud("game", "forceupdateexcel"):
    #             descstr = ''
    #             for sp in ulst:
    #                 tmpurl, tmpdescstr = geturlcontentwrite2excel(men_wc, sp)
    #                 descstr += tmpdescstr
    #         else:
    #             descstr = updateurllst(men_wc, ulst)
    #         outstr = f"【Text】信息中发现新的火界麻将战绩网页链接并处理：\t{descstr}"
    #         # log.info(outstr)
    #         itchat.send_msg(sendernick + outstr)
    #         makemsg2write(innermsg, outstr)
    #         makemsg2write(innermsg, msgtxt)

    # 如何不是指定的数据分析中心，则不进行语义分析
    thisid = getdeviceid()
    # print(f"type:{type(thisid)}\t{thisid}")
    houseid = getinivaluefromcloud("webchat", "datahouse")
    mainaccount = getinivaluefromcloud("webchat", "mainaccount")
    # print(f"type:{type(houseid)}\t{houseid}")
    men_wc = getcfpoptionvalue("happyjpwebchat", get_host_uuid(), "host_nickname")
    if thisid != str(houseid) or (men_wc != mainaccount):
        print(f"不是数据分析中心也不是主账号【{mainaccount}】，指令咱不管哦")
        return

    #     # 根据口令显示火界麻将战绩综合统计结果
    #     if msg['Text'].startswith('火界麻将战果统计') or msg['Text'].startswith('麻果'):
    #         log.info(f"根据口令显示火界麻将战绩综合统计结果")
    #         msgtxt = msg['Text']
    #         recentday = ""
    #         if msgtxt.find('日') != -1:
    #             recentday = "日"
    #         elif msgtxt.find('周') != -1:
    #             recentday = "周"
    #         elif msgtxt.find('旬') != -1:
    #             recentday = "旬"
    #         elif msgtxt.find('月') != -1:
    #             recentday = "月"
    #         elif msgtxt.find('年') != -1:
    #             recentday = "年"
    #         elif msgtxt.find('全部') != -1:
    #             recentday = "全部"
    #         else:
    #             recentday = '周'

    #         simpledesc = True
    #         if msgtxt.find('综合') != -1:
    #             simpledesc = False
    #         zhanji = zhanjidesc(men_wc, recentday, simpledesc)
    #         # 发回给查询者
    #         itchat.send_msg(f"{zhanji}", toUserName=msg['FromUserName'])
    #         makemsg2write(innermsg, zhanji)
    #         imgzhanji = lststr2img(zhanji)
    #         imgwcrel = os.path.relpath(imgzhanji)
    #         itchat.send_image(imgwcrel, toUserName=msg['FromUserName'])
    #         makemsg2write(innermsg, imgwcrel)
    #         outstr = f"{sendernick}\t查询信息：\n{msgtxt}"
    #         # 查询记录发给自己一份备档
    #         itchat.send_msg(outstr)
    #         makemsg2write(innermsg, outstr)

    #         if msgtxt.find('折线图') != -1:
    #             _, imgzhanji = showzhanjiimg(men_wc, recentday)
    #             imgzhanjirel = os.path.relpath(imgzhanji)
    #             itchat.send_image(imgzhanjirel, toUserName=msg['FromUserName'])
    #             # 折线图发送记录备档
    #             makemsg2write(innermsg, imgzhanjirel)

    if msg["Text"].find("真元信使") >= 0:
        qrylst = msg["Text"].split("\n")
        # 去除某行首位空格
        qrylst = [x.strip() for x in qrylst]
        # 去掉空行
        qrylst = [x.strip() for x in qrylst if len(x.strip()) != 0]
        print(f"{qrylst}")
        diyihang = qrylst[0].split()
        if diyihang[0].strip() == "真元信使":
            if len(diyihang) == 1:
                if (len(qrylst) == 1) or (qrylst[1].strip == ""):
                    rstfile, rst = searchcustomer()
                else:
                    qrystr = qrylst[1].strip()
                    rstfile, rst = searchcustomer(qrystr.split())
            elif diyihang[1] == "延时图":
                delaydbname = getdirmain() / "data" / "db" / f"wcdelay_{men_wc}.db"
                imgwcdelay, image_base64 = showdelayimg(delaydbname)
                # imgwcdelayrel = os.path.relpath(imgwcdelay)
                imgwcdelay = os.path.abspath(imgwcdelay)
                itchat.send_image(imgwcdelay, toUserName=msg["FromUserName"])
                delayimg2note(men_wc)
                makemsg2write(innermsg, imgwcdelay)
                # 延时图发送记录备档
                return
            elif diyihang[1] == "电量图":
                delaydbname = touchfilepath2depth(
                    getdirmain() / "data" / "db" / "batteryinfo.db"
                )
                imgbattinfo = showbattinfoimg(delaydbname)
                imgbattinforel = os.path.relpath(imgbattinfo)
                itchat.send_image(imgbattinforel, toUserName=msg["FromUserName"])
                makemsg2write(innermsg, imgbattinforel)
                # 延时图发送记录备档
                return
            elif diyihang[1] == "联系人":
                contactinfo = showphoneinfoimg()
                imgcontactinforel = os.path.relpath(contactinfo)
                itchat.send_image(imgcontactinforel, toUserName=msg["FromUserName"])
                makemsg2write(innermsg, imgcontactinforel)
                # 延时图发送记录备档
                return
            elif diyihang[1] == "连更":
                updatectdf()
                return
            elif diyihang[1] == "连显":
                frddfread = getctdf()
                imgwc = db2img(showwcsimply(frddfread))
                imgwcrel = os.path.relpath(imgwc)
                itchat.send_image(imgwcrel, toUserName=msg["FromUserName"])
                makemsg2write(innermsg, imgwcrel)
                return
            elif diyihang[1] == "欠款":
                qrystr = qrylst[1].strip()
                rstfile, rst = searchqiankuan(qrystr.split())
            elif diyihang[1] == "品项":
                qrystr = qrylst[1].strip()
                rstfile, rst = searchpinxiang(qrystr.split())
            else:
                rstfile, rst = None, None

            itchat.send_msg(rst, toUserName=msg["FromUserName"])
            # 查询结果文件路径备档
            makemsg2write(innermsg, rst)

            if rstfile:
                # rstfile必须是绝对路径，并且不能包含中文字符
                itchat.send_file(rstfile, toUserName=msg["FromUserName"])
                # 发给自己一份存档
                makemsg2write(
                    innermsg, rstfile.replace(os.path.abspath(dirmainpath), "")
                )
                itchat.send_file(rstfile)
                infostr = f"成功发送查询结果文件：{os.path.split(rstfile)[1]}给{innermsg['fmSender']}"
                itchat.send_msg(infostr)
                makemsg2write(innermsg, infostr)
                log.info(infostr)
            # return rst


# %% [markdown]
# ### listfriends(num=-10)


# %%
def listfriends(num=-10):
    friends = itchat.get_friends(update=True)
    for fr in friends[num:]:
        print(fr)


# %% [markdown]
# ### listchatrooms()


# %%
def listchatrooms():
    chatrooms = itchat.get_chatrooms(update=True)
    for cr in chatrooms:
        print(cr)


# %% [markdown]
# ### @itchat.msg_register(FRIENDS)


# %%
@itchat.msg_register(FRIENDS)
def add_friend(msg):
    # 如何不是指定的数据分析中心和主账户，则不打招呼
    thisid = getdeviceid()
    houseid = getinivaluefromcloud("webchat", "datahouse")
    mainaccount = getinivaluefromcloud("webchat", "mainaccount")
    helloword1 = getinivaluefromcloud("webchat", "helloword1")
    helloword2 = getinivaluefromcloud("webchat", "helloword2")
    men_wc = getcfpoptionvalue("happyjpwebchat", get_host_uuid(), "host_nickname")
    if thisid != str(houseid) or (men_wc != mainaccount):
        print(f"不是数据分析中心也不是主账号【{mainaccount}】，不用打招呼哟")
        return
    msg.user.verify()
    msg.user.send(f"Nice to meet you!\n{helloword1}\n{helloword2}")
    writefmmsg2txtandmaybeevernotetoo(msg)
    log.info(msg)


# %% [markdown]
# ### after_login()


# %%
def after_login():
    men_wc = getcfpoptionvalue("happyjpwebchat", get_host_uuid(), "host_nickname")
    log.info(f"登入《{men_wc}》的微信服务")


# %% [markdown]
# ### after_logout()


# %%
def after_logout():
    men_wc = getcfpoptionvalue("happyjpwebchat", get_host_uuid(), "host_nickname")
    try:
        termux_sms_send(f"微信({men_wc})登录已退出，如有必要请重新启动")
    except Exception as e:
        log.critical(f"尝试发送退出提醒短信失败。{e}")
        log.error("", exc_info=True)
    log.critical(f"退出微信({men_wc})登录")


# %% [markdown]
# ### get_host_uuid()


# %%
def get_host_uuid():
    hotdir = itchat.originInstance.hotReloadDir
    #     print(hotdir) # itchat.pkl
    return uuid3hexstr(os.path.abspath(hotdir))


# %% [markdown]
# ### def keepliverun()


# %%
@trycounttimes2("微信服务器", 200, 50)
def keepliverun():
    # 为了让实验过程更加方便（修改程序不用多次扫码），我们使用热启动
    status4login = itchat.check_login()
    log.critical(f"微信登录状态为：\t{status4login}")
    if status4login == "200":
        log.info("已处于成功登录状态")
        return
    itchat.auto_login(
        hotReload=True, loginCallback=after_login, exitCallback=after_logout
    )
    # itchat.auto_login(hotReload=True)

    # 设定获取信息时重试的次数，默认是5，设定为50，不知道是否能够起作用
    itchat.originInstance.receivingRetryCount = 50

    init_info = itchat.web_init()
    # showmsgexpanddictetc(init_info)
    if init_info["BaseResponse"]["Ret"] == 0:
        logstr = "微信初始化信息成功返回，获取登录用户信息"
        log.info(logstr)
        host_nickname = init_info["User"]["NickName"]
        host_username = init_info["User"]["UserName"]
        log.critical(
            f"函数《{sys._getframe().f_code.co_name}》中用户变量为：\t{(host_nickname, host_username)}"
        )
        if (host_username is not None) and (len(host_username) > 0):
            setcfpoptionvalue(
                "happyjpwebchat", get_host_uuid(), "host_nickname", host_nickname
            )
            setcfpoptionvalue(
                "happyjpwebchat", get_host_uuid(), "host_username", host_username
            )
        else:
            log.critical("username is None")
    elif itchat.originInstance.loginInfo:
        log.info("从itchat.originInstance.loginInfo中获取登录用户信息")
        host_nickname = dict(itchat.originInstance.loginInfo["User"])["NickName"]
        host_username = dict(itchat.originInstance.loginInfo["User"])["UserName"]
        log.critical(
            f"函数《{sys._getframe().f_code.co_name}》中用户变量为：\t{(host_nickname, host_username)}"
        )
        if (host_username is not None) and (len(host_username) > 0):
            setcfpoptionvalue(
                "happyjpwebchat", get_host_uuid, "host_nickname", host_username
            )
            setcfpoptionvalue(
                "happyjpwebchat", get_host_uuid, "host_username", host_username
            )
        else:
            log.critical("username is None")
    else:
        log.critical(
            f"函数《{sys._getframe().f_code.co_name}》中用户变量为：\t{(host_nickname, host_username)}"
        )

    # itchat.get_mps(update=True)
    # listchatrooms()
    # listfriends()
    itchat.run()
    # raise Exception


# %% [markdown]
# ## main 主函数

# %%
if __name__ == "__main__":
    if not_IPython():
        log.info(f"运行文件\t{__file__}")

    # listallloghander()
    # console = logging.StreamHandler()
    # console.setLevel(logging.DEBUG)
    # itloger = logging.getLogger('itchat')
    # itloger.addHandler(console)
    # print(itloger, itloger.handlers)

    recentmsg_deque = deque(maxlen=30)
    # face_bug=None  #针对表情包的内容
    keepliverun()

    if not_IPython():
        log.info(f"{__file__}\t运行结束！")


# %% [markdown]
# ### tst4itchat


# %%
def tst4itchat():
    itchat.auto_login(hotReload=True, statusStorageDir="../itchat.pkl")

    itchat_web_init_dict = itchat.web_init()
    print(itchat_web_init_dict.keys())
    wcfriends = itchat.get_friends()
    print(wcfriends[:19])

    mps_lst = itchat.get_mps(update=True)
    mp_meituan = itchat.search_mps(
        userName="@111b2a64da3fcd28f194226313bf9a342191a947d0bcac29e2e58c3c1e2a6d79"
    )
    print(mp_meituan)
    print(mps_lst[-20:])

    fileprefix = "img"
    fpath = "img/webchat/20240918/（多神家园）MS五群(群)西尼—破晓_240918-174115.png"
    fileabspath = os.path.abspath(getdirmain() / fpath)
    print(fileabspath)

    file = "@%s@%s" % (fileprefix, fileabspath)
    print(file)
    # re_info = itchat.send(file, toUserName='filehelper')
    # return re_info


# %%
if (not not_IPython()) and True:
    tst4itchat()
