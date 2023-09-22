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
# # 日志信息更新至云端笔记

# %% [markdown]
# ## 引入库

# %%
import os
import re
import pandas as pd
from threading import Timer
import pathmagic

# %%
with pathmagic.context():
    from func.first import getdirmain
    from func.configpr import getcfpoptionvalue, setcfpoptionvalue
    # from func.evernttest import get_notestore, imglist2note, readinifromnote, evernoteapijiayi, makenote, getinivaluefromnote
    from func.jpfuncs import getinivaluefromcloud, createnote, updatenote_body, updatenote_title
    from func.logme import log
    from func.wrapfuncs import timethis, ift2phone
    # from func.termuxtools import termux_location, termux_telephony_deviceinfo
    # from func.nettools import ifttt_notify
    from etc.getid import getdeviceid
    from func.sysfunc import not_IPython, set_timeout, after_timeout, execcmd


# %% [markdown]
# ## 功能函数集

# %% [markdown]
# ### log2note(notetuid, loglimit, levelstr='', notetitle='happyjp日志信息')

# %%
@timethis
# @ift2phone()
# @profile
def log2note(noteid, loglimit, levelstr='', notetitle='happyjp日志信息'):
    namestr = 'happyjplog'

    if levelstr == 'CRITICAL':
        levelstrinner = levelstr + ':'
        levelstr4title = '严重错误'
        countnameinini = 'happyjplogcc'
    else:
        levelstrinner = levelstr
        levelstr4title = ''
        countnameinini = 'happyjplogc'

    # 查找log目录下所有有效日志文件并根据levelstrinner集合相应行
    pathlog = getdirmain() / 'log'
    files = [f for f in os.listdir(str(pathlog)) if not f.startswith(".")]
    loglines = []
    for fname in files[::-1]:
        # log.info(fname)
        if not fname.startswith('happyjoplin.log'):
            log.warning(f'文件《{fname}》不是合法的日志文件，跳过。')
            continue
        with open(pathlog / fname, 'r', encoding='utf-8') as flog:
            charsnum2showinline = getinivaluefromcloud(namestr,
                                                      'charsnum2showinline')
            # print(f"log行最大显示字符数量为：\t{charsnum2showinline}")
            loglines = loglines + [line.strip()[:charsnum2showinline]
                                   for line in flog if line.find(levelstrinner) >= 0]

    ptn = re.compile('\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}')
    tmlst = [pd.to_datetime(re.match(ptn, x).group())
             for x in loglines if re.match(ptn, x)]
    loglines = [x for x in loglines if re.match(ptn, x)]
    logsr = pd.Series(loglines, index=tmlst)
    logsr = logsr.sort_index()
    # print(logsr.index)
    # print(logsr)
    loglines = list(logsr)
    # log.info(loglines[:20])
    # print(len(loglines))
    print(f'日志的{levelstr4title}记录共有{len(loglines)}条，只取时间最近的{loglimit}条')
    if not (everlogc := getcfpoptionvalue(namestr, namestr, countnameinini)):
        everlogc = 0
    # log.info(everlogc)
    if len(loglines) == everlogc:  # <=调整为==，用来应对log文件崩溃重建的情况
        print(f'暂无新的{levelstr4title}记录，不更新“happyjoplin的{levelstr}日志笔记”。')
    else:
        loglinesloglimit = loglines[(-1 * loglimit):]
        loglinestr = '\n'.join(loglinesloglimit[::-1])
        # loglinestr = loglinestr.replace('<', '《').replace('>',
        #                                                   '》').replace('=', '等于').replace('&', '并或')
        # loglinestr = "<pre>" + loglinestr + "</pre>"
        log.info(f"日志字符串长度为：\t{len(loglinestr)}")
        # log.info(loglinestr[:100])
        try:
            updatenote_title(noteid, notetitle)
            updatenote_body(noteid, loglinestr)
            setcfpoptionvalue(namestr, namestr, countnameinini, f'{len(loglines)}')
            print(f'新的log{levelstr4title}信息成功更新入笔记《{notetitle}》')
        except Exception as eeee:
            errmsg = f'处理新的log{levelstr4title}信息到笔记《{notetitle}》时出现未名错误。{eeee}'
            log.critical(errmsg)


# %% [markdown]
# ### log2notes()

# %%
@set_timeout(360, after_timeout)
def log2notes():
    namestr = 'happyjplog'
    device_id = getdeviceid()
    loginname = execcmd("whoami")

    # token = getcfpoptionvalue('everwork', 'evernote', 'token')
    # log.info(token)
    if not (logid := getcfpoptionvalue(namestr, device_id, 'logid')):
        logid = createnote(f'服务器_{device_id}_{loginname}_日志信息', "")
#         note_store = get_notestore()
#         parentnotebook = note_store.getNotebook(
#             '4524187f-c131-4d7d-b6cc-a1af20474a7f')
#         evernoteapijiayi()
#         note = ttypes.Note()
#         note.title = f'服务器_{device_id}_日志信息'

#         notelog = makenote(token, note_store, note.title,
#                            notebody='', parentnotebook=parentnotebook)
#         logguid = notelog.guid
        setcfpoptionvalue(namestr, device_id, 'logid', logid)

    if not (logcid := getcfpoptionvalue(namestr, device_id, 'logcid')):
        logcid = createnote(f'服务器_{device_id}_{loginname}_严重错误日志信息', "")
        # note_store = get_notestore()
        # parentnotebook = note_store.getNotebook(
        #     '4524187f-c131-4d7d-b6cc-a1af20474a7f')
        # evernoteapijiayi()
        # note = ttypes.Note()
        # note.title = f'服务器_{device_id}_严重错误日志信息'
        # notelog = makenote(token, note_store, note.title,
        #                    notebody='', parentnotebook=parentnotebook)
        # logcguid = notelog.guid
        setcfpoptionvalue(namestr, device_id, 'logcid', logcid)

    if not (loglimitc := getinivaluefromcloud(namestr, 'loglimit')):
        loglimitc = 500

    if not (servername := getinivaluefromcloud('device', device_id)):
        servername = device_id

    if getinivaluefromcloud(namestr, 'critical') == 1:
        levelstrc = 'CRITICAL'
        # noteguidc = cfpeverwork.get('evernote', 'lognotecriticalguid')
        log2note(logcid, loglimitc, levelstrc,
                 notetitle=f'服务器_{servername}_{loginname}_严重错误日志信息')

    # noteguidn = cfpeverwork.get('evernote', 'lognoteguid')
    log2note(noteid=logid, loglimit=loglimitc,
             notetitle=f'服务器_{servername}_{loginname}_日志信息')

    # locinfo = termux_location()
    # print(locinfo)


# %% [markdown]
# ## 主函数main()

# %%
if __name__ == '__main__':
    if not_IPython():
        log.info(f'开始运行文件\t{__file__}')
    log2notes()
    if not_IPython():
        log.info(f'Done.结束执行文件\t{__file__}')
