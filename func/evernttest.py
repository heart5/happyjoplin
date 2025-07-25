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
# # evernote相关功能函数集

# %%
"""
evernote或印象笔记相关功能函数
"""

# %% [markdown]
# ## 引入重要库

# %%
import os
import sys
import binascii
import datetime
import hashlib
import mimetypes
import re
import time
import traceback
import inspect
import numpy as np
import pandas as pd
import http
import ssl
from bs4 import BeautifulSoup
from evernote.api.client import EvernoteClient
from evernote.edam.error.ttypes import EDAMNotFoundException, EDAMSystemException, EDAMUserException, EDAMErrorCode
from evernote.edam.notestore.NoteStore import NoteFilter, NotesMetadataResultSpec
from evernote.edam.type.ttypes import Note, NoteAttributes, Resource, ResourceAttributes, Data, Notebook
from evernote.edam.userstore.constants import EDAM_VERSION_MAJOR, EDAM_VERSION_MINOR

# %%
import pathmagic

# %%
with pathmagic.context():
    from func.configpr import getcfpoptionvalue, setcfpoptionvalue, removesection
    from func.first import dirlog, dirmainpath, touchfilepath2depth
    from func.logme import log
    from func.nettools import trycounttimes2
    from func.sysfunc import convertframe2dic, not_IPython, extract_traceback4exception, set_timeout, after_timeout
    from func.datetimetools import timestamp2str
    # from etc.getid import getid

# %% [markdown]
# print(f"{__file__} is loading now...")

# %% [markdown]
# ## 函数集合

# %% [markdown]
# ### def gettoken():


# %%
def gettoken():
    if china := getcfpoptionvalue("everwork", "evernote", "china"):
        # print(f"china value:\t{china}")
        auth_token = getcfpoptionvalue("everwork", "evernote", "tokenchina")  # 直接提取，唯一使用
    else:
        # print(f"china value:\t{china}")
        auth_token = getcfpoptionvalue("everwork", "evernote", "token")  # 直接提取，唯一使用

    return auth_token


# %% [markdown]
# ###  def imglist2note(notestore, reslist, noteguid, notetitle, neirong='', parentnotebookguid=None):


# %%
def imglist2note(notestore, reslist, noteguid, notetitle, neirong="", parentnotebookguid=None):
    """
    更新note内容，可以包含图片等资源类文件列表
    :param notestore:
    :param reslist:
    :param noteguid:
    :param notetitle:
    :param neirong:object
    :return:
    """
    note = Note()
    noteattrib = NoteAttributes()
    global en_username
    if en_username is not None:
        noteattrib.author = en_username
        print(f"I'm here while creating the note, for evernote user {en_username}")
    note.attributes = noteattrib
    note.guid = noteguid.lower()
    note.title = notetitle
    if (parentnotebookguid is not None) and (re.search("\w{8}(-\w{4}){3}-\w{12}", parentnotebookguid) is not None):
        note.notebookGuid = parentnotebookguid

    # To include an attachment such as an image in a note, first create a Resource
    # for the attachment. At a minimum, the Resource contains the binary attachment
    # data, an MD5 hash of the binary data, and the attachment MIME type.
    # It can also include attributes such as filename and location.

    # Now, add the new Resource to the note's list of resources
    # print(len(note.resources))
    # print(noteguid)
    # note.resources = notestore.getNote(token, noteguid, True, True, True,True).resources
    # evernoteapijiayi()
    # if not note.resources:
    #     note.resources = []

    note.resources = []
    # print(len(note.resources))
    # for img, imgtitle in imglist:
    for res in reslist:
        """
        必须要重新构建一个Data（），否则内容不会变化
        Data只有三个域：bodyHash（用MD5进行hash得到的值）、size（body的字节长度）
        和body（字节形式的内容本身）
        """
        resactual = open(res, "rb").read()
        md5 = hashlib.md5()
        md5.update(resactual)
        reshash = md5.digest()
        data = Data()
        data.size = len(resactual)
        data.bodyHash = reshash
        data.body = resactual
        """
        Resource需要常用的域：guid、noteGuid、data（指定上面的Data）、
        mime（需要设定）、attributes（可以设定附件的原文件名）
        """
        resource = Resource()
        #         resource.mime = 'image/png'
        if (mtype := mimetypes.guess_type(res)[0]) is None:
            logstr = f"文件《{res}》的类型无法判断"
            log.critical(logstr)
            print(logstr)
            mtype = "file/unkonwn"
        #             continue
        resource.mime = mtype
        #         print(mtype)
        resource.data = data
        """
        NoteAttributes常用的域：sourceURL、fileName和经纬度、照相机等信息
        """
        resattrib = ResourceAttributes()
        resattrib.fileName = res
        resource.attributes = resattrib
        note.resources.append(resource)

    # The content of an Evernote note is represented using Evernote Markup Language
    # (ENML). The full ENML specification can be found in the Evernote API Overview
    # at http://dev.evernote.com/documentation/cloud/chapters/ENML.php
    nbody = '<?xml version="1.0" encoding="UTF-8"?>'
    nbody += '<!DOCTYPE en-note SYSTEM "http://xml.evernote.com/pub/enml2.dtd">'
    nbody += "<en-note>"
    if note.resources:
        # To display the Resource as part of the note's content, include an <en-media>
        # tag in the note's ENML content. The en-media tag identifies the corresponding
        # Resource using the MD5 hash.
        # nBody += "<br />" * 2
        for resource in note.resources:
            #             print(resource.guid)
            if resource.mime.startswith("image") or True:
                hexhash = binascii.hexlify(resource.data.bodyHash)
                str1 = "%s" % hexhash  # b'cd34b4b6c8d9279217b03c396ca913df'
                # print (str1)
                str1 = str1[2:-1]  # cd34b4b6c8d9279217b03c396ca913df
                print(resource.mime)
                nbody += '<en-media type="%s" hash="%s" align="center" longdesc="%s" /><br />%s<hr />' % (
                    resource.mime,
                    str1,
                    resource.attributes.fileName,
                    resource.attributes.fileName,
                )
    # neirong= "<pre>" + neirong + "</pre>"

    # 去除控制符
    neirong = re.sub(r"[\x00-\x08\x0b-\x0c\x0e-\x1f]", "", neirong)
    neirong = re.sub("&", "and连接符", neirong)

    nbody += neirong
    nbody += "</en-note>"

    # ！！！严重错误，过滤\x14时把回车等符号都杀了！！！
    # nbodynotasciilst = [hex(ord(x)) for x in nbody if ord(x) < 32]
    # print(f"存在不可显示字符串：{''.join(nbodynotasciilst)}")
    # nbodylst = [x for x in nbody if ord(x) >= 32]
    # nbody = ''.join(nbodylst)
    note.content = nbody
    # log.info(f"新构笔记文字部分长度为：\t{len(nbody)}")
    # print(note.content[:100])

    # Finally, send the new note to Evernote using the updateNote method
    # The new Note object that is returned will contain server-generated
    # attributes such as the new note's unique GUID.
    @trycounttimes2("evernote服务器，更新笔记。")
    def updatenote(notesrc):
        nsinner = get_notestore()
        token = gettoken()
        updated_note = nsinner.updateNote(token, notesrc)
        evernoteapijiayi()
        log.info("成功更新了笔记《%s》，guid：%s。" % (updated_note.title, updated_note.guid))

    updatenote(note)


# %% [markdown]
# ###  def updatereslst2note(reslist, guidinput, title=None, neirong=None, filenameonly=False, parentnotebookguid=None):


# %%
def updatereslst2note(reslist, guidinput, title=None, neirong=None, filenameonly=False, parentnotebookguid=None):
    """
    更新note附件和文字内容，附件只更新或添加，不影响其它附件，可以包含图片等资源类文件列表
    :param notestore:
    :param reslist:
    :param noteguid:
    :param notetitle:
    :param neirong:object
    :return:
    """
    noteattrib = NoteAttributes()
    #     global en_username
    #     if en_username is not None:
    #         noteattrib.author = en_username
    #         print(f"I'm here while updating the note for special res, for evernote user {en_username}")

    resfnonlylist = [os.path.basename(innerpath) for innerpath in reslist]  # 只取用文件名，保证名称唯一
    #     print(f"输入资源短文件名列表：\t{resfnonlylist}")
    reslist = [os.path.abspath(innerpath) for innerpath in reslist]  # 取用绝对路径，保证名称唯一
    #     print(f"输入资源长文件名列表：\t{reslist}")

    noteinput = getnoteall(guidinput)
    note = Note()
    note.attributes = noteattrib
    note.guid = guidinput

    if (parentnotebookguid is not None) and (re.search("\w{8}(-\w{4}){3}-\w{12}", parentnotebookguid) is not None):
        note.notebookGuid = parentnotebookguid
    #         print(parentnotebookguid)
    if title is None:
        note.title = noteinput.title
    else:
        note.title = title

    #     print(f"inputnote's resources is {noteinput.resources}")
    if (nirs := noteinput.resources) is not None:
        if filenameonly:
            notereslstclean = [res for res in nirs if res.attributes.fileName not in resfnonlylist]
        else:
            notereslstclean = [res for res in nirs if res.attributes.fileName not in reslist]
    else:
        notereslstclean = list()
    print(f"待更新笔记中的资源文件有{len(notereslstclean)}个", end="，")
    """
    必须重新构建note.resources，否则内容不会改变
    """
    note.resources = []
    for res1 in notereslstclean:
        note.resources.append(res1)

    # To include an attachment such as an image in a note, first create a Resource
    # for the attachment. At a minimum, the Resource contains the binary attachment
    # data, an MD5 hash of the binary data, and the attachment MIME type.
    # It can also include attributes such as filename and location.

    # Now, add the new Resource to the note's list of resources
    for res in reslist:
        """
        必须要重新构建一个Data（），否则内容不会变化
        Data只有三个域：bodyHash（用MD5进行hash得到的值）、size（body的字节长度）和body（字节形式的内容本身）
        """
        resactual = open(res, "rb").read()
        md5 = hashlib.md5()
        md5.update(resactual)
        reshash = md5.digest()
        data = Data()
        data.size = len(resactual)
        data.bodyHash = reshash
        data.body = resactual
        """
        Resource需要常用的域：guid、noteGuid、data（指定上面的Data）、mime（需要设定）、attributes（可以设定附件的原文件名）
        """
        resource = Resource()
        #         resource.mime = 'image/png'
        if (mtype := mimetypes.guess_type(res)[0]) is None:
            logstr = f"文件《{res}》的类型无法判断"
            log.critical(logstr)
            print(logstr)
            mtype = "file/unkonwn"
        #             continue
        resource.mime = mtype
        #         print(mtype)
        resource.data = data
        """
        NoteAttributes常用的域：sourceURL、fileName和经纬度、照相机等信息
        """
        resattrib = ResourceAttributes()
        if filenameonly:
            resattrib.fileName = os.path.basename(res)
        else:
            resattrib.fileName = res
        resource.attributes = resattrib
        note.resources.append(resource)

    print(f"处理后共有{len(note.resources)}个。")
    # The content of an Evernote note is represented using Evernote Markup Language
    # (ENML). The full ENML specification can be found in the Evernote API Overview
    # at http://dev.evernote.com/documentation/cloud/chapters/ENML.php
    nbody = '<?xml version="1.0" encoding="UTF-8"?>'
    nbody += '<!DOCTYPE en-note SYSTEM "http://xml.evernote.com/pub/enml2.dtd">'
    nbody += "<en-note>"
    if ((nss := note.resources) is not None) & (len(nss) != 0):
        # To display the Resource as part of the note's content, include an <en-media>
        # tag in the note's ENML content. The en-media tag identifies the corresponding
        # Resource using the MD5 hash.
        # nBody += "<br />" * 2
        for resource in nss:
            #             print(resource.guid)
            if resource.mime.startswith("image") or True:
                hexhash = binascii.hexlify(resource.data.bodyHash)
                str1 = "%s" % hexhash  # b'cd34b4b6c8d9279217b03c396ca913df'
                # print (str1)
                str1 = str1[2:-1]  # cd34b4b6c8d9279217b03c396ca913df
                #                 print(resource.mime)
                nbody += '<en-media type="%s" hash="%s" align="center" longdesc="%s" /><br />%s<hr />' % (
                    resource.mime,
                    str1,
                    resource.attributes.fileName,
                    resource.attributes.fileName,
                )
    if neirong is not None:
        neirong = "<pre>" + neirong + "</pre>"
        # 去除控制符
        neirong = re.sub(r"[\x00-\x08\x0b-\x0c\x0e-\x1f]", "", neirong)
        neirong = re.sub("&", "and连接符", neirong)
        nbody += neirong

    nbody += "</en-note>"

    # ！！！严重错误，过滤\x14时把回车等符号都杀了！！！
    # nbodynotasciilst = [hex(ord(x)) for x in nbody if ord(x) < 32]
    # print(f"存在不可显示字符串：{''.join(nbodynotasciilst)}")
    # nbodylst = [x for x in nbody if ord(x) >= 32]
    # nbody = ''.join(nbodylst)
    note.content = nbody
    #     print(nbody)
    # log.info(f"新构笔记文字部分长度为：\t{len(nbody)}")
    # print(note.content[:100])

    # Finally, send the new note to Evernote using the updateNote method
    # The new Note object that is returned will contain server-generated
    # attributes such as the new note's unique GUID.
    #     print(f"I'm here while note'updating is ready.\t{note.guid}")
    #     p_noteattributeundertoken(noteinput)
    @trycounttimes2("evernote服务器，更新笔记。")
    def updatenote(notesrc):
        nsinner = get_notestore()
        token = gettoken()
        updated_note = nsinner.updateNote(token, notesrc)
        evernoteapijiayi()
        log.info("成功更新了笔记《%s》，guid：%s。资源列表为：%s" % (updated_note.title, updated_note.guid, reslist))

    #         if updated_note:
    #             if updated_note.resources:
    #                 print(f"笔记res更新后共有{len(updated_note.resources)}个")
    #                 print([res.attributes.fileName for res in updated_note.resources])

    updatenote(note)


# %% [markdown]
# ###  def tablehtml2evernote(dataframe, tabeltitle='表格标题', withindex=True, setwidth=True):


# %%
def tablehtml2evernote(dataframe, tabeltitle="表格标题", withindex=True, setwidth=True):
    colwidth = pd.get_option("max_colwidth")
    if setwidth:
        pd.set_option("max_colwidth", 200)
    else:
        # print(colwidth)
        pass
    df = pd.DataFrame(dataframe)
    outstr = (
        df.to_html(justify="center", index=withindex)
        .replace('class="dataframe">', 'align="center">')
        .replace("<table", '\n<h3 align="center">%s</h3>\n<table' % tabeltitle)
        .replace("<th></th>", "<th>&nbsp;</th>")
    )
    # print(outstr)
    if setwidth:
        pd.set_option("max_colwidth", colwidth)
    return outstr


# %% [markdown]
# ###  def findnotefromnotebook(notebookguid, titlefind='', notecount=10000):


# %%
def findnotefromnotebook(notebookguid, titlefind="", notecount=10000):
    """
    列出笔记本中包含某关键词的笔记信息
    :param tokenfnfn: token
    :param notebookguid: 笔记本的guid
    :param titlefind: 关键词
    :param notecount: 搜索结果数量限值
    :return: 列表，包含形如[noteguid, notetitle, note.updateSequenceNum]的list
    """
    global note_store
    note_store = get_notestore()
    notefilter = NoteFilter()
    notefilter.notebookGuid = notebookguid
    notemetaspec = NotesMetadataResultSpec(
        includeTitle=True,
        includeContentLength=True,
        includeCreated=True,
        includeUpdated=True,
        includeDeleted=True,
        includeUpdateSequenceNum=True,
        includeNotebookGuid=True,
        includeTagGuids=True,
        includeAttributes=True,
        includeLargestResourceMime=True,
        includeLargestResourceSize=True,
    )

    @trycounttimes2("evernote服务器")
    def findnote(startnum: int = 0, maxnum: int = 250):
        tokenfnfn = gettoken()
        # log.info("I'm here now too.")
        notelist = note_store.findNotesMetadata(tokenfnfn, notefilter, startnum, maxnum, notemetaspec)
        # log.info("I'm here now three.")
        evernoteapijiayi()
        return notelist

    width = 250
    items = list()
    ournotelist = findnote()
    print(ournotelist.totalNotes)
    items.extend(
        [
            [note.guid, note.title, note.updateSequenceNum]
            for note in ournotelist.notes
            if note.title.find(titlefind) >= 0
        ]
    )

    if ournotelist.totalNotes > notecount:
        numtobesplit = notecount
    else:
        numtobesplit = ournotelist.totalNotes

    spllst = [
        (i * width, (width, numtobesplit - width * i)[numtobesplit - width * (i + 1) < 0], numtobesplit)
        for i in range((numtobesplit // width) + 1)
    ]
    if len(spllst) >= 1:
        print(spllst)
        for numbt in spllst[1:]:
            print(numbt)
            ournotelist = findnote(numbt[0], numbt[1])
            items.extend(
                [
                    [note.guid, note.title, note.updateSequenceNum]
                    for note in ournotelist.notes
                    if note.title.find(titlefind) >= 0
                ]
            )

    return items


# %% [markdown]
# ###  def getnoteall(guid: str):


# %%
@trycounttimes2("evernote服务器")
def getnoteall(guid: str):
    """
    获取笔记
    :param guid:
    :return:note
    """
    nost = get_notestore()
    note = nost.getNote(gettoken(), guid.lower(), True, True, False, False)
    #     print(note)
    evernoteapijiayi()

    return note


# %% [markdown]
# ###  def getnotecontent(guid: str):


# %%
@trycounttimes2("evernote服务器")
def getnotecontent(guid: str):
    """
    获取笔记内容
    :param guid:
    :return:
    """
    ns = get_notestore()
    soup = BeautifulSoup(ns.getNoteContent(guid), "html.parser")
    # print(soup)

    return soup


# %% [markdown]
# ###  def getnoteresource(guid: str):


# %%
@trycounttimes2("evernote服务器")
def getnoteresource(guid: str):
    """
    获取笔记附件
    :param guid:
    :return:
    """
    ns = get_notestore()
    note = ns.getNote(gettoken(), guid, True, True, False, False)
    evernoteapijiayi()
    resultlst = list()
    if note.resources is None:
        log.critical(f"笔记{guid}中没有包含资源文件，返回空列表")
        return list()
    for resitem in note.resources:
        sonlst = list()
        sonlst.append(resitem.attributes.fileName)
        #         sonlst.append(resitem.data.body.decode())
        sonlst.append(resitem.data.body)
        resultlst.append(sonlst)
    # print(soup)

    return resultlst


# %% [markdown]
# ###  def createnotebook(nbname: str, stack='fresh'):


# %%
def createnotebook(nbname: str, stack="fresh"):
    notebook = Notebook()
    notebook.name = nbname
    notebook.stack = stack

    return get_notestore().createNotebook(gettoken(), notebook)


# %% [markdown]
# ###  def makenote(tokenmn, notestore, notetitle, notebody='真元商贸——休闲食品经营专家', parentnotebook=None):


# %%
def makenote(tokenmn, notestore, notetitle, notebody="真元商贸——休闲食品经营专家", parentnotebook=None):
    """
    创建一个note
    :param tokenmn:
    :param notestore:
    :param notetitle:
    :param notebody:
    :param parentnotebook:
    :return:
    """
    # global log
    nbody = '<?xml version="1.0" encoding="UTF-8"?>'
    nbody += '<!DOCTYPE en-note SYSTEM "http://xml.evernote.com/pub/enml2.dtd">'
    nbody += "<en-note>%s</en-note>" % notebody

    # Create note object
    ournote = Note()
    ournote.title = notetitle
    ournote.content = nbody

    # parentNotebook is optional; if omitted, default notebook is used
    if type(parentnotebook) is str:
        parentnotebook = notestore.getNotebook(gettoken(), parentnotebook)
    if parentnotebook and hasattr(parentnotebook, "guid"):
        ournote.notebookGuid = parentnotebook.guid

    # Attempt to create note in Evernote account
    try:
        note = notestore.createNote(tokenmn, ournote)
        evernoteapijiayi()
        if parentnotebook and hasattr(parentnotebook, "name"):
            bkname = f"<{parentnotebook.name}>"
        else:
            bkname = "默认"
        log.info(f"笔记《{notetitle}》在\t{bkname}\t笔记本中创建成功。")
        return note
    except EDAMUserException as usere:
        # Something was wrong with the note data
        # See EDAMErrorCode enumeration for error code explanation
        # http://dev.evernote.com/documentation/reference/Errors.html#Enum_EDAMErrorCode
        log.critical("用户错误！%s" % str(usere))
    except EDAMNotFoundException as notfounde:
        # Parent Notebook GUID doesn't correspond to an actual notebook
        print("无效的笔记本guid（识别符）！%s" % str(notfounde))
    except EDAMSystemException as systeme:
        if systeme.errorCode == EDAMErrorCode.RATE_LIMIT_REACHED:
            log.critical("API达到调用极限，需要 %d 秒后重来" % systeme.rateLimitDuration)
            exit(1)
        else:
            log.critical("创建笔记时出现严重错误：" + str(systeme))
            exit(2)


# %% [markdown]
# ### def makenote2(notetitle, notebody='真元商贸——休闲食品经营专家', parentnotebookguid=None):


# %%
def makenote2(notetitle, notebody="真元商贸——休闲食品经营专家", parentnotebookguid=None):
    """
    创建note，封装token和notestore
    :param notetitle:
    :param notebody:
    :param parentnotebook:
    :return:
    """

    notestore = get_notestore()
    nbody = '<?xml version="1.0" encoding="UTF-8"?>'
    nbody += '<!DOCTYPE en-note SYSTEM "http://xml.evernote.com/pub/enml2.dtd">'
    nbody += "<en-note>%s</en-note>" % notebody

    # Create note object
    ournote = Note()
    ournote.title = notetitle
    ournote.content = nbody

    # parentNotebook is optional; if omitted, default notebook is used
    if type(parentnotebookguid) is str:
        try:
            parentnotebook = notestore.getNotebook(gettoken(), parentnotebookguid)
        except:
            log.critical(f"新建笔记的笔记本guid属性无效，设置为默认")
            parentnotebook = None
    else:
        parentnotebook = None
    if parentnotebook and hasattr(parentnotebook, "guid"):
        ournote.notebookGuid = parentnotebook.guid

    # Attempt to create note in Evernote account
    try:
        note = notestore.createNote(gettoken(), ournote)
        evernoteapijiayi()
        if parentnotebook and hasattr(parentnotebook, "name"):
            bkname = f"<{parentnotebook.name}>"
        else:
            bkname = "默认"
        log.info(f"笔记《{notetitle}》在\t{bkname}\t笔记本中创建成功。")
        return note
    except EDAMUserException as usere:
        # Something was wrong with the note data
        # See EDAMErrorCode enumeration for error code explanation
        # http://dev.evernote.com/documentation/reference/Errors.html#Enum_EDAMErrorCode
        log.critical("用户错误！%s" % str(usere))
    except EDAMNotFoundException as notfounde:
        # Parent Notebook GUID doesn't correspond to an actual notebook
        print("无效的笔记本guid（识别符）！%s" % str(notfounde))
    except EDAMSystemException as systeme:
        if systeme.errorCode == EDAMErrorCode.RATE_LIMIT_REACHED:
            log.critical("API达到调用极限，需要 %d 秒后重来" % systeme.rateLimitDuration)
            exit(1)
        else:
            log.critical("创建笔记时出现严重错误：" + str(systeme))
            exit(2)


# %% [markdown]
# ### def evernoteapijiayi():


# %%
def evernoteapijiayi():
    """
    evernote api调用次数加一。结合api调用限制，整点或达到限值（貌似是300次每小时）则重构一个继续干。
    """
    cfpapiname = "everapi"
    nssectionname = "notestore"
    note_store = get_notestore()
    nsstr4ini = hex(id(note_store))
    nowtime = datetime.datetime.now()
    nowmin = nowtime.minute
    try:
        nowhourini = getcfpoptionvalue(cfpapiname, "apitimes", "hour")
        # ns首次启动和整点重启（用小时判断）
        if not (apitimes := getcfpoptionvalue(cfpapiname, nssectionname, nsstr4ini)) or (
            (nowmin == 0) and (nowhourini != nowtime.hour)
        ):
            if nowmin == 0:
                log.info(f"Evernote API\t{nsstr4ini} 调用次数整点重启^_^")
            else:
                log.info(f"Evernote API\t{nsstr4ini} 新生^_^{inspect.stack()[-1]}")
            #             log.critical(f"Evernote API\t{nsstr4ini} 新生^_^{inspect.stack()[-1]}")
            apitimes = 0
            #         print(nowhourini, nowtime.hour)
        if nowhourini != nowtime.hour:
            setcfpoptionvalue(cfpapiname, "apitimes", "hour", str(nowtime.hour))
        apitimes += 1
        log.debug(f"动用Evernote API({note_store})次数：\t {apitimes} ")
        setcfpoptionvalue(cfpapiname, nssectionname, nsstr4ini, str(apitimes))
    except Exception as e:
        log.critical(
            f"{cfpapiname}配置文件存取出现严重错误，试图清除《{nssectionname}》小节下的所有内容。跳过一次api调用计数！"
        )
        log.critical(e)
        removesection(cfpapiname, nssectionname)
        return
    if apitimes >= 290:
        sleepsecs = np.random.randint(0, 50)
        time.sleep(sleepsecs)
        note_store = None
        note_store = get_notestore(forcenew=True)
        log.critical(f"休息{sleepsecs:d}秒，重新构造了一个服务器连接{note_store}继续干……")


# %% [markdown]
# ### def evernoteapijiayi_test():


# %%
def evernoteapijiayi_test():
    calllink = [
        re.findall("^<FrameSummary file (.+), line (\d+) in (.+)>$", str(line)) for line in traceback.extract_stack()
    ]
    if len(calllink) > 0:
        calllinks = str(calllink[-1])
    #         print(calllinks)
    else:
        calllinks = ""
    note_store = get_notestore()
    nsstr4ini = str(id(note_store))
    nowtime = datetime.datetime.now()
    nowmin = nowtime.minute
    nowhourini = getcfpoptionvalue("everapi", "apitimes", "hour")
    # ns首次启动和整点重启（用小时判断）
    if not (apitimes := getcfpoptionvalue("everapi", "apitimes", nsstr4ini)) or (
        (nowmin == 0) and (nowhourini != nowtime.hour)
    ):
        if nowmin == 0:
            log.critical(f"Evernote API\t{nsstr4ini} 调用次数整点重启^_^{calllinks}")
        else:
            log.critical(f"Evernote API\t{nsstr4ini} 新生^_^{calllinks}")
        apitimes = 0
    if nowhourini != nowtime.hour:
        setcfpoptionvalue("everapi", "apitimes", "hour", str(nowtime.hour))
    apitimes += 1
    log.debug(f"动用Evernote API({note_store})次数：\t {apitimes} ")
    setcfpoptionvalue("everapi", "apitimes", nsstr4ini, str(apitimes))
    if apitimes >= 290:
        sleepsecs = np.random.randint(0, 50)
        time.sleep(sleepsecs)
        note_store = None
        note_store = get_notestore(forcenew=True)
        log.critical(f"休息{sleepsecs:d}秒，重新构造了一个服务器连接{note_store}继续干……{calllinks}")


# %% [markdown]
# ### def p_notebookattributeundertoken(notebook):


# %%
# @use_logging()
def p_notebookattributeundertoken(notebook):
    """
    测试笔记本（notebook）数据结构每个属性的返回值,开发口令（token）的方式调用返回如下
    :param notebook:
    :return:dict
    """
    rstdict = dict()
    rstdict["名称"] = notebook.name  # phone
    rstdict["guid"] = notebook.guid  # f64c3076-60d1-4f0d-ac5c-f0e110f3a69a
    rstdict["更新序列号"] = notebook.updateSequenceNum  # 8285
    rstdict["默认笔记本"]: bool = notebook.defaultNotebook  # False
    # print(type(rstdict['默认笔记本']), rstdict['默认笔记本'])
    if china := getcfpoptionvalue("everwork", "evernote", "china"):
        shijianchushu = 1
    else:
        shijianchushu = 1000
    ntsct = notebook.serviceCreated / 1000
    ntsut = notebook.serviceUpdated / 1000
    # print(ntsct, ntsut, timestamp2str(ntsct), timestamp2str(ntsut))
    rstdict["创建时间"] = pd.to_datetime(timestamp2str(ntsct))  # 2010-09-15 11:37:43
    rstdict["更新时间"] = pd.to_datetime(timestamp2str(ntsut))  # 2016-08-29 19:38:24
    rstdict["笔记本组"] = notebook.stack  # 手机平板

    # print('发布中\t', notebook.publishing)     # 这种权限的调用返回None
    # print('发布过\t', notebook.published)      # 这种权限的调用返回None

    # print '共享笔记本id\t', notebook.sharedNotebookIds  #这种权限的调用返回None
    # print '共享笔记本\t', notebook.sharedNotebooks  #这种权限的调用返回None
    # print '商务笔记本\t', notebook.businessNotebook  #这种权限的调用返回None
    # print '联系人\t', notebook.contact  #这种权限的调用返回None
    # print '限定\t', notebook.restrictions  #NotebookRestrictions(noSetDefaultNotebook=None,
    # noPublishToBusinessLibrary=True, noCreateTags=None, noUpdateNotes=None,
    # expungeWhichSharedNotebookRestrictions=None,
    # noExpungeTags=None, noSetNotebookStack=None, noCreateSharedNotebooks=None, noExpungeNotebook=None,
    # noUpdateTags=None, noPublishToPublic=None, noUpdateNotebook=None, updateWhichSharedNotebookRestrictions=None,
    # noSetParentTag=None, noCreateNotes=None, noEmailNotes=True, noReadNotes=None, noExpungeNotes=None,
    # noShareNotes=None, noSendMessageToRecipients=None)
    # print '接受人设定\t', notebook.recipientSettings  #这种权限的调用没有返回这个值，报错

    # print(rstdict)

    return rstdict


# %% [markdown]
# ### def p_noteattributeundertoken(note):


# %%
def p_noteattributeundertoken(note):
    """
    测试笔记（note）数据结构每个属性的返回值,通过findNotesMetadata函数获取，开发口令（token）的方式调用返回如下:
    :param note:
    :return:
    """
    print("guid\t%s" % note.guid)  #
    print("标题\t%s" % note.title)  #
    print(f"内容长度\t{note.contentLength}")  # 762
    # 这种权限的调用没有返回这个值，报错；NoteStore.getNoteContent()也无法解析
    print("内容\t" + note.content)
    print("内容哈希值\t%s" % note.contentHash)  # 8285
    if note.created:
        # 2017-09-04 22:39:51
        print("创建时间\t%s" % timestamp2str(int(note.created / 1000)))
    if note.updated:
        # 2017-09-07 06:38:47
        print("更新时间\t%s" % timestamp2str(int(note.updated / 1000)))
    if note.deleted:
        print("删除时间\t%s" % note.deleted)  # 这种权限的调用返回None
    print("活跃\t%s" % note.active)  # True
    if note.updateSequenceNum:
        print("更新序列号\t%d" % note.updateSequenceNum)  # 173514
    # 2c8e97b5-421f-461c-8e35-0f0b1a33e91c
    print("所在笔记本的guid\t%s" % note.notebookGuid)
    print("标签的guid\t%s" % note.tagGuids)  # 这种权限的调用返回None
    print("资源表\t%s" % note.resources)  # 这种权限的调用返回None
    print("属性\t%s" % note.attributes)
    # NoteAttributes(lastEditorId=139947593, placeName=None, sourceURL=None, classifications=None,
    # creatorId=139947593, author=None, reminderTime=None, altitude=0.0, reminderOrder=None, shareDate=None,
    # reminderDoneTime=None, longitude=114.293, lastEditedBy='\xe5\x91\xa8\xe8\x8e\x89 <305664756@qq.com>',
    # source='mobile.android', applicationData=None, sourceApplication=None, latitude=30.4722, contentClass=None,
    # subjectDate=None)
    print("标签名称表\t%s" % note.tagNames)  # 这种权限的调用返回None
    # print ('共享的笔记表\t%s' % note.sharedNotes)
    # 这种权限的调用没有返回这个值，报错AttributeError: 'Note' object has no attribute 'sharedNotes'
    # print ('限定\t%s' % note.restrictions)
    # 这种权限的调用没有返回这个值，报错AttributeError: 'Note' object has no attribute 'restrictions'
    # print ('范围\t%s' % note.limits) #这种权限的调用没有返回这个值，报错AttributeError: 'Note' object has no attribute 'limits'


# %% [markdown]
# ### def findnotebookfromevernote(ntname=None):


# %%
def findnotebookfromevernote(ntname=None):
    """
    列出所有笔记本
    :return: rstdf，
    DataFrame格式，dtypes
    创建时间     datetime64[ns]
    名称               object
    更新序列号           float64
    更新时间     datetime64[ns]
    笔记本组             object
    默认笔记本              bool
    dtype: object
    """
    global note_store
    note_store = get_notestore()
    notebooks = note_store.listNotebooks()
    # p_notebookattributeundertoken(notebooks[-1])

    rstdf = pd.DataFrame()
    for x in notebooks:
        # rstdf = rstdf.append(pd.Series(p_notebookattributeundertoken(x)), ignore_index=True)
        ds = pd.Series(p_notebookattributeundertoken(x))
        ds1 = ds.to_frame()
        if rstdf.shape[0] == 0:
            rstdf = ds1.T
        else:
            rstdf = pd.concat([rstdf, pd.DataFrame(ds1.values.T, columns=ds1.index)], ignore_index=True)

    # print(rstdf)

    rstdf["默认笔记本"] = rstdf["默认笔记本"].astype(bool)
    rstdf.set_index("guid", inplace=True)

    if ntname is not None:
        rstdf = rstdf[rstdf.名称 == ntname]

    return rstdf


# %% [markdown]
# ### expungenotes(inputguidlst)


# %%
def expungenotes(inputguidlst):
    """
    删除传入的笔记列表
    """

    @trycounttimes2("evernote服务器，删除笔记", maxtimes=8)
    def innerexpungenote(intoken, nost, guid):
        evernoteapijiayi()
        nost.expungeNote(intoken, guid)
        return True

    token = gettoken()
    nost = get_notestore()
    for son in inputguidlst:
        log.info("\t".join([f"（{inputguidlst.index(son) + 1}/{len(inputguidlst)}）", son[0], son[1]]))
        if done := innerexpungenote(token, nost, son[0]):
            log.info(
                "\t".join([f"（{inputguidlst.index(son) + 1}/{len(inputguidlst)}）", son[0], son[1], "\t成功删除^_^"])
            )
        else:
            log.critical(
                "\t".join(
                    [f"（{inputguidlst.index(son) + 1}/{len(inputguidlst)}）", son[0], son[1], "\t未能删除！！！"]
                )
            )


# %% [markdown]
# ### expungetrash()


# %%
def expungetrash(times=10):
    @trycounttimes2("evernote服务器，清空垃圾篓", maxtimes=times)
    def innerexpungetrash():
        token = gettoken()
        evernoteapijiayi()
        nost.expungeInactiveNotes(token)
        return True

    log.info("开始清空垃圾篓……")
    if done := innerexpungetrash():
        log.info("垃圾篓成功清空^_^")
    else:
        log.critical("垃圾篓清空失败！！！")


# %% [markdown]
# ### expungenotescontainkey(qukw="区$", titlekw="图表")


# %%
def expungenotescontainkey(qukw="区$", titlekw="图表"):
    """
    删除指定关键词笔记本中标题包含指定关键词的笔记
    """
    # 避免循环导入，在函数体内import
    from func.wrapfuncs import timethis

    @timethis
    def sonexpungenotescontainkey(qukw, titlekw):
        ntdf = findnotebookfromevernote()
        tgds = ntdf[ntdf["名称"].str.contains(qukw)]["名称"]
        ntlst = [[v, k] for (k, v) in dict(tgds).items()]

        for nt in ntlst[::-1]:
            # 真元销售，分区，图表
            findnoteguidlst = findnotefromnotebook(nt[1], titlefind=titlekw, notecount=30)
            findnoteguidlst = [x for x in findnoteguidlst if len(x[1]) != (len(nt[0]) + 4)]
            if len(findnoteguidlst) == 0:
                log.info(f"笔记本《{nt[0]}》中没有找到符合规则的笔记，跳过！！！")
                continue
            log.info(
                f"开始删除【{ntlst.index(nt) + 1}/{len(ntlst)}】笔记本《{nt[0]}》中的笔记，共有{len(findnoteguidlst)}条………………………………"
            )
            expungenotes(findnoteguidlst)
            expungetrash(times=11)
            log.info(
                f"【{ntlst.index(nt) + 1}/{len(ntlst)}】笔记本《{nt[0]}》中符合规则的笔记共有{len(findnoteguidlst)}条，处理完毕！"
            )

    sonexpungenotescontainkey(qukw, titlekw)


# %% [markdown]
# token = getcfpoptionvalue('everwork', 'evernote', 'token')
# print(token)
# ENtimes, ENAPIlasttime = enapistartlog()
# evernoteapiclearatzero()


# %% [markdown]
# # 主函数

# %%
if __name__ == "__main__":
    if not_IPython():
        log.info(f"开始运行文件\t{__file__}……")
    print("I'm here now")
    if not_IPython():
        log.info(f"完成文件{__file__}\t的运行")
