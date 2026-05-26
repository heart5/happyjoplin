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
"""微信大观园，工作优先，娱乐生活"""


# %% [markdown]
# ## 库引入


# %%
import os

# %%
# import arrow
import re
import sys
import time
from collections import deque
from datetime import datetime

# %%
import itchat
import itchat.storage
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
    from func.configpr import getcfpoptionvalue, setcfpoptionvalue
    from func.datatools import readfromtxt, write2txt
    from func.datetimetools import gethumantimedelay
    from func.first import getdirmain, touchfilepath2depth
    from func.getid import getdeviceid, getdevicename
    from func.jpfuncs import (
        add_resource_from_bytes,
        createnote,
        deleteresourcesfromnote,
        getinivaluefromcloud,
        jpapi,
        searchnotes,
        updatenote_body,
        updatenote_title,
    )
    from func.logme import log
    from func.nettools import trycounttimes2
    from func.sysfunc import execcmd, not_IPython, uuid3hexstr
    from func.termuxtools import termux_sms_send
    from life.wcdelay import inserttimeitem2db
    from life.webchat_sharing import parse_sharing_content, soupclean2item


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


# %%
### showmsgexpanddicttetc()


# %%
def showmsgexpanddictetc(msg):
    """列印dict中的所有属性和值，对于dict类型的子元素，则再展开一层"""
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
    """格式化并重构msg,获取合适用于直观显示的用户名，对公众号和群消息特别处置"""
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
        log.info(f"待配置公众号（不在ignoredmplist）: {showname}")
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
    if not isinstance(fmtext, str):
        fmtext = str(fmtext)

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
# %%
def writefmmsg2txtandmaybeevernotetoo(inputformatmsg):
    """把格式化好的微信聊天记录写入文件，并根据云端ini设定更新相应账号的聊天笔记"""
    # 把聊天记录以dict的格式缓存入队列，fmId提取出来做key
    global recentmsg_deque
    onedict = {}
    onedict[inputformatmsg["fmId"]] = {item: inputformatmsg[item] for item in inputformatmsg if item != "fmId"}
    recentmsg_deque.append(onedict)
    if (recentnum := len(recentmsg_deque)) != 30:
        log.debug(f"缓存聊天记录数量为：\t{recentnum}，fmId号列表\t{list(deque2dict(recentmsg_deque))}")
    # 判断是否延时并增加提示到条目内容中
    if humantimestr := gethumantimedelay(inputformatmsg["fmTime"]):
        inputformatmsg["fmText"] = f"[{humantimestr}]" + inputformatmsg["fmText"]
    from life.chat_schema import format_tsv

    msgcontent = format_tsv(inputformatmsg)
    print(f"{msgcontent}")

    men_wc = getcfpoptionvalue("happyjpwebchat", get_host_uuid(), "host_nickname")
    chattxtfilename = str(getdirmain() / "data" / "webchat" / f"chatitems({men_wc}).txt")
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
        if (chatnoteid := getcfpoptionvalue("happyjpwebchat", "webchat", wcnote_title)) is None:
            chatnoteid = createnote(title=notetitle)
            setcfpoptionvalue("happyjpwebchat", "webchat", wcnote_title, str(chatnoteid))
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

    _check_session_age()


# %%
def _check_session_age():
    """检查会话年龄，25天/28天时通过SMS提醒续期（同日不重复）。每50条消息触发一次实际检查。"""
    # 每50条消息才真正检查一次，降低IO开销
    msg_count = getattr(_check_session_age, "_msg_count", 0) + 1
    _check_session_age._msg_count = msg_count
    if msg_count % 50 != 0:
        return
    from func.configpr import getcfpoptionvalue, setcfpoptionvalue

    login_date_str = getcfpoptionvalue("happyjpwebchat", "session", "login_date")
    if not login_date_str:
        return
    try:
        login_date = datetime.strptime(login_date_str, "%Y-%m-%d")
    except ValueError:
        return
    age = (datetime.now() - login_date).days
    if age < 25:
        return

    today = datetime.now().strftime("%Y-%m-%d")
    if age < 28:
        key = "last_sms_day25"
        msg = "微信pkl会话已25天，请抽空在Termux运行: python life/webchat.py --renew"
    elif age < 30:
        key = "last_sms_day28"
        msg = f"微信pkl已{age}天即将过期，请尽快运行 --renew 续期"
    else:
        return  # 30天及以上由 after_logout 通知

    last_sms = getcfpoptionvalue("happyjpwebchat", "session", key) or ""
    if last_sms == today:
        return  # 今天已发过，节流
    try:
        from func.termuxtools import termux_sms_send

        termux_sms_send(msg)
        setcfpoptionvalue("happyjpwebchat", "session", key, today)
        log.info(f"会话年龄{age}天，已发送SMS续期提醒")
    except Exception:
        log.debug(f"会话年龄{age}天，SMS发送失败（非Termux环境）")


# %% [markdown]
# ### getsendernick(msg)

# %%
def getsendernick(msg):
    """获取发送者昵称并返回"""
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
@itchat.msg_register([CARD, FRIENDS], isFriendChat=True, isGroupChat=True, isMpChat=True)
def tuling_reply(msg):
    # showmsgexpanddictetc(msg)
    writefmmsg2txtandmaybeevernotetoo(formatmsg(msg))


# %% [markdown]
# ### @itchat.msg_register([NOTE], isFriendChat=True, isGroupChat=True, isMpChat=True)


# %%
@itchat.msg_register([NOTE], isFriendChat=True, isGroupChat=True, isMpChat=True)
def note_reply(msg):
    """处理note类型信息，对微信转账记录深入处置。"""
    global recentmsg_deque
    # showmsgexpanddictetc(msg)
    innermsg = formatmsg(msg)
    if ("撤回了一条消息" in msg["Content"]) or ("recalled a message" in msg["Content"]):
        if (msgid_match := re.search(r"\<msgid\>(.*?)\<\/msgid\>", msg["Content"])) is None:
            log.warning(f"撤回消息中未找到msgid，原始信息：{msg}")
            return
        old_msg_id = msgid_match.group(1)
        msg_information = deque2dict(recentmsg_deque)
        old_msg = msg_information.get(old_msg_id)
        log.warning(f"撤回消息缓存查找结果：{old_msg}")
        if old_msg is None:
            log.warning(f"未找到有效msg_id，直接返回。原始信息如下：{msg}")
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
            log.warning(f"发送文件的返回值:\t{response_send}")
            log.warning(f"撤回的文件【{fileprefix}\t{fileabspath}】发送给文件助手")
        log.warning(f"处理了撤回的消息：{old_msg}")
        msg_information.pop(old_msg_id)
        recentmsg_deque.clear()
        for k, v in msg_information.items():
            recentmsg_deque.append({k: v})
        log.warning(
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
    log.info(f"保存文件（{innermsg['fmType']}）：\t{str(filepath)}")
    msg["Text"](str(filepath))
    innermsg["fmText"] = str(filepath)

    writefmmsg2txtandmaybeevernotetoo(innermsg)


# %% [markdown]
# ### @itchat.msg_register([SHARING], isFriendChat=True, isGroupChat=True, isMpChat=True)


# %%
@itchat.msg_register([SHARING], isFriendChat=True, isGroupChat=True, isMpChat=True)
def sharing_reply(msg):
    sendernick = getsendernick(msg)
    log.debug(sendernick)
    innermsg = formatmsg(msg)
    soup, items = soupclean2item(msg["Content"])
    parse_sharing_content(soup, items, innermsg, msg)
    writefmmsg2txtandmaybeevernotetoo(innermsg)


# %% [markdown]
# ### @itchat.msg_register([TEXT], isFriendChat=True, isGroupChat=True, isMpChat=True)


# %%
@itchat.msg_register([TEXT], isFriendChat=True, isGroupChat=True, isMpChat=True)
def text_reply(msg):
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

    # 如何不是指定的数据分析中心，则不进行语义分析
    thisid = getdeviceid()
    # print(f"type:{type(thisid)}\t{thisid}")
    houseid = getinivaluefromcloud("webchat", "datahouse")
    mainaccount = getinivaluefromcloud("webchat", "mainaccount")
    # print(f"type:{type(houseid)}\t{houseid}")
    men_wc = getcfpoptionvalue("happyjpwebchat", get_host_uuid(), "host_nickname")
    if thisid != str(houseid) or (men_wc != mainaccount):
        log.debug(f"不是数据分析中心也不是主账号【{mainaccount}】，指令咱不管哦")
        return

    from life.webchat_commands import dispatch

    if dispatch(msg, innermsg, men_wc):
        return


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
        log.debug(f"不是数据分析中心也不是主账号【{mainaccount}】，不用打招呼哟")
        return
    msg.user.verify()
    greeted = getcfpoptionvalue("happyjpwebchat", "greeted_friends", "list") or ""
    greeted_set = set(greeted.split(",")) if greeted else set()
    friend_name = msg.user.NickName or msg.user.UserName
    if friend_name in greeted_set:
        log.info(f"已打过招呼，跳过: {friend_name}")
    else:
        msg.user.send(f"Nice to meet you!\n{helloword1}\n{helloword2}")
        greeted_set.add(friend_name)
        setcfpoptionvalue("happyjpwebchat", "greeted_friends", "list", ",".join(greeted_set))
    writefmmsg2txtandmaybeevernotetoo(msg)
    log.info(msg)


# %% [markdown]
# ### after_login()


# %%
def after_login():
    men_wc = getcfpoptionvalue("happyjpwebchat", get_host_uuid(), "host_nickname")
    log.info(f"登入《{men_wc}》的微信服务")
    today = datetime.now().strftime("%Y-%m-%d")
    setcfpoptionvalue("happyjpwebchat", "session", "login_date", today)
    # 重置SMS哨兵
    setcfpoptionvalue("happyjpwebchat", "session", "last_sms_day25", "")
    setcfpoptionvalue("happyjpwebchat", "session", "last_sms_day28", "")
    setcfpoptionvalue("happyjpwebchat", "session", "last_sms_expired", "")


# %% [markdown]
# ### after_logout()


# %%
def after_logout():
    men_wc = getcfpoptionvalue("happyjpwebchat", get_host_uuid(), "host_nickname")
    today = datetime.now().strftime("%Y-%m-%d")
    last_sms = getcfpoptionvalue("happyjpwebchat", "session", "last_sms_expired") or ""
    if last_sms != today:
        try:
            termux_sms_send(f"微信({men_wc})web协议已退出，请运行 --renew 重新扫码")
            setcfpoptionvalue("happyjpwebchat", "session", "last_sms_expired", today)
        except Exception as e:
            log.critical(f"尝试发送退出提醒短信失败。{e}")
            log.error("", exc_info=True)
    log.critical(f"itchat微信web协议登录已退出({men_wc})")


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
    itchat.auto_login(hotReload=True, loginCallback=after_login, exitCallback=after_logout)
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
        log.critical(f"函数《{sys._getframe().f_code.co_name}》中用户变量为：\t{(host_nickname, host_username)}")
        if (host_username is not None) and (len(host_username) > 0):
            setcfpoptionvalue("happyjpwebchat", get_host_uuid(), "host_nickname", host_nickname)
            setcfpoptionvalue("happyjpwebchat", get_host_uuid(), "host_username", host_username)
        else:
            log.critical("username is None")
    elif itchat.originInstance.loginInfo:
        log.info("从itchat.originInstance.loginInfo中获取登录用户信息")
        host_nickname = dict(itchat.originInstance.loginInfo["User"])["NickName"]
        host_username = dict(itchat.originInstance.loginInfo["User"])["UserName"]
        log.critical(f"函数《{sys._getframe().f_code.co_name}》中用户变量为：\t{(host_nickname, host_username)}")
        if (host_username is not None) and (len(host_username) > 0):
            setcfpoptionvalue("happyjpwebchat", get_host_uuid, "host_nickname", host_username)
            setcfpoptionvalue("happyjpwebchat", get_host_uuid, "host_username", host_username)
        else:
            log.critical("username is None")
    else:
        log.critical(f"函数《{sys._getframe().f_code.co_name}》中用户变量为：\t{(host_nickname, host_username)}")

    # itchat.get_mps(update=True)
    # listchatrooms()
    # listfriends()
    itchat.run()
    # raise Exception


# %% [markdown]
# ### _run_renew() —— 一键续期


# %%
def _run_renew():
    """--renew 模式：生成QR→上传Joplin→同步tc→SMS通知→等扫码→分发pkl→重启双机"""
    import subprocess
    import threading
    from pathlib import Path

    qr_path = str(getdirmain() / "img" / "qrcode.png")
    _tmp = os.environ.get('TMPDIR') or '/tmp'
    if not os.path.exists(_tmp):
        _tmp = str(Path.home() / 'tmp')
    guard_file = Path(_tmp) / "webchat_renewing"

    # 1. 设置续期守卫，防止 startwebchatprocess.sh 干预
    guard_file.touch()

    # 2. 停掉现有 webchat 进程（$锚定避免误杀 --renew 自身）
    log.info("停止现有webchat进程…")
    subprocess.run("pkill -f 'python.*life/webchat\\.py$' 2>/dev/null", shell=True)
    time.sleep(2)

    # 3. QR回调：在itchat生成QR码后立即上传Joplin并通知
    qr_event = threading.Event()

    def _qr_callback(uuid=None, status=None, qrcode=None):
        if qrcode and not qr_event.is_set():
            _upload_qr_to_joplin(qrcode, qr_path)
            _sync_tc_joplin()
            _notify_renew_qr()
            qr_event.set()

    log.info("生成二维码，等待扫码…")
    try:
        itchat.auto_login(
            hotReload=False,
            picDir=qr_path,
            qrCallback=_qr_callback,
        )
    except Exception as e:
        log.error(f"auto_login 异常: {e}")
        # 恢复：用旧pkl重启
        _restart_local_webchat()
        guard_file.unlink()
        print(f"[--renew] 登录失败: {e}")
        return

    # 4. 扫码成功，保存新pkl
    itchat.dump_login_status()
    log.info("扫码成功，新pkl已保存")

    # 5. 分发pkl到腾讯云
    pkl_src = str(getdirmain() / "itchat.pkl")
    pkl_dst = "tc:~/codebase/happyjoplin/itchat.pkl"
    result = subprocess.run(
        f"scp {pkl_src} {pkl_dst}",
        shell=True, capture_output=True, text=True, timeout=60,
    )
    if result.returncode == 0:
        log.info("pkl已上传至腾讯云")
    else:
        log.error(f"pkl上传tc失败: {result.stderr}")

    # 6. 远程重启tc上的webchat（委托保活脚本处理python路径和日志）
    restart_cmd = (
        "pkill -f webchat.py 2>/dev/null; "
        "sleep 2; "
        "sh ~/codebase/happyjoplin/life/startwebchatprocess.sh"
    )
    result = subprocess.run(
        f"ssh tc '{restart_cmd}'",
        shell=True, capture_output=True, text=True, timeout=30,
    )
    if result.returncode == 0:
        log.info("tc webchat 已重启")
    else:
        log.error(f"tc webchat 重启失败: {result.stderr}")

    # 7. 更新会话日期，重置SMS哨兵
    today = datetime.now().strftime("%Y-%m-%d")
    setcfpoptionvalue("happyjpwebchat", "session", "login_date", today)
    setcfpoptionvalue("happyjpwebchat", "session", "last_sms_day25", "")
    setcfpoptionvalue("happyjpwebchat", "session", "last_sms_day28", "")
    setcfpoptionvalue("happyjpwebchat", "session", "last_sms_expired", "")

    # 8. 删除守卫，重启本地webchat
    guard_file.unlink()
    _restart_local_webchat()
    log.info("--renew 续期完成")
    print("[--renew] 续期完成！新pkl已分发至tc，双机均已重启。")


def _upload_qr_to_joplin(qrcode_bytes, local_path=None):
    """将二维码上传到Joplin笔记「微信扫码登录」"""
    from pathlib import Path

    try:
        if local_path:
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            with open(local_path, "wb") as f:
                f.write(qrcode_bytes)

        notes = searchnotes("微信扫码登录")
        if notes:
            note_id = notes[0].id
        else:
            note_id = createnote(title="微信扫码登录", body="")
        try:
            deleteresourcesfromnote(note_id)
        except Exception:
            pass
        res_id = add_resource_from_bytes(qrcode_bytes, "微信扫码登录二维码.png")
        jpapi.add_resource_to_note(resource_id=res_id, note_id=note_id)
        updatenote_body(
            noteid=note_id,
            bodystr=(
                "## 微信扫码登录\n\n"
                "请使用 **Pixel 6 Pro 微信** 扫描下方二维码\n\n"
                f"![](:/{res_id})\n\n"
                "---\n\n"
                "**二维码有效期：5分钟**\n\n"
                "> 此笔记由 `--renew` 自动更新"
            ),
        )
        log.info(f"QR已上传Joplin笔记 {note_id}")
        return note_id
    except Exception as e:
        log.error(f"QR上传Joplin失败: {e}")
        return None


def _sync_tc_joplin():
    """SSH到tc同步Joplin"""
    import subprocess

    try:
        subprocess.run(
            "ssh tc 'source /usr/miniconda3/bin/activate newlsp && "
            "conda activate newlsp && joplin sync'",
            shell=True, timeout=30, capture_output=True,
        )
        log.info("tc Joplin同步完成")
    except Exception as e:
        log.error(f"tc Joplin同步失败: {e}")


def _notify_renew_qr():
    """SMS通知用户扫码"""
    try:
        termux_sms_send("微信续期二维码已更新至Joplin笔记，请5分钟内打开Joplin扫码")
        log.info("已发送扫码SMS通知")
    except Exception:
        log.debug("SMS发送失败（非Termux环境？）")


def _restart_local_webchat():
    """后台启动本地webchat"""
    import subprocess
    from pathlib import Path

    _tmp = os.environ.get('TMPDIR') or '/tmp'
    if not os.path.exists(_tmp):
        _tmp = str(Path.home() / 'tmp')
    log_file = str(Path(_tmp) / "lifewebchat.out")
    subprocess.Popen(
        ["nohup", "python", str(getdirmain() / "life" / "webchat.py")],
        stdout=open(log_file, "a"),
        stderr=subprocess.STDOUT,
        cwd=str(getdirmain()),
        start_new_session=True,
    )


# %% [markdown]
# ## main 主函数

# %%
if __name__ == "__main__":
    if not_IPython():
        log.info(f"运行文件\t{__file__}")

    # --renew 一键续期模式
    if len(sys.argv) > 1 and sys.argv[1] == "--renew":
        _run_renew()
        sys.exit(0)

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
    mp_meituan = itchat.search_mps(userName="@111b2a64da3fcd28f194226313bf9a342191a947d0bcac29e2e58c3c1e2a6d79")
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
