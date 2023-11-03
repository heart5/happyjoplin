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
# # Joplin 工具库

# %% [markdown]
# ## 引入库

# %%
import os
import re
# import requests
# import subprocess
import arrow
# import joppy
# import datetime
# from pathlib import Path
from joppy.api import Api
from joppy import tools
# from tzlocal import get_localzone
# from dateutil import tz

# %%
import pathmagic
with pathmagic.context():
    from func.first import getdirmain
    from func.configpr import getcfpoptionvalue, setcfpoptionvalue
    from func.logme import log
    # from func.wrapfuncs import timethis, ift2phone
    # from func.termuxtools import termux_location, termux_telephony_deviceinfo
    # from func.nettools import ifttt_notify
    from etc.getid import getdevicename
    from func.sysfunc import not_IPython, set_timeout, after_timeout, execcmd


# %% [markdown]
# ## 功能函数集

# %% [markdown]
# ### joplincmd(cmd)

# %%
def joplincmd(cmd):
    """
    运行joplin命令行并返回输出结果
    """
    # result = subprocess.run(cmd, stdout=subprocess.PIPE, text=True, shell=True)
    return execcmd("cmd")


# %% [markdown]
# ### getapi()

# %%
def getapi():
    """
    获取api方便调用，自适应不同的joplin server端口；通过命令行joplin获取相关配置参数值
    """
    tokenstr = execcmd("joplin config api.token")
    portstr = execcmd("joplin config api.port")
    if (tokenstr.find("=") == -1) | (portstr.find("=") == -1):
        logstr = f"主机【{getdevicename()}】账户（{execcmd('whoami')}）貌似尚未运行joplin server！\n退出运行！！！"
        log.critical(f"{logstr}")
        exit(1)
    token = tokenstr.split("=")[1].strip()
    portraw = portstr.split("=")[1].strip()
    port = 41184 if portraw == "null" else portraw

    url = f"http://localhost:{port}"
    api = Api(token=token, url=url)

    return api, token, port


# %% [markdown]
# ### searchnotebook(query)

# %%
def searchnotebook(title):
    """
    查找指定title（全名）的笔记本并返回id，如果不存在，则新建一个返回id
    """
    api, token, port = getapi()
    result = api.search(query=title, type="folder")
    if len(result.items) == 0:
        nbid = api.add_notebook(title=title)
        log.critical(f"新建笔记本《{title}》，id为：\t{nbid}")
    else:
        nbid = result.items[0].id

    return nbid


# %% [markdown]
# ### def getallnotes()

# %%
def getallnotes():
    """
    获取所有笔记；默认仅输出id、parent_id和title三项有效信息
    """
    api = getapi()[0]

    return api.get_all_notes()


# %% [markdown]
# ### getnoteswithfields(fields, limit=10)

# %%
def getnoteswithfields(fields, limit=10):
    api = getapi()[0]
    fields_ls = fields.split(",")
    allnotes = [note for note in api.get_all_notes(fields=fields)[:limit]]
    geonotes = [note for note in allnotes if note.altitude != 0 and note.longitude != 0]
    print(len(allnotes), len(geonotes))
    for note in geonotes:
        # print(getattr(note, "id"))
        neededfls = [getattr(note, key) for key in fields_ls]
        print(neededfls)
        # if any(getattr(note, location_key) != 0 for location_key in location_keys):
        #     # api.modify_note(
        #     #     id_=note.id, **{location_key: 0 for location_key in location_keys}
        #     # )
        #     print(note)
    # geonotes = [[note.fl for fl in fields_ls] for note in geonotes]
    # print(geonotes)


# %% [markdown]
# ### getnote(id)

# %%
def getnote(id):
    """
    通过id获取笔记的所有可能内容，NoteData
    """
    api = getapi()[0]
    # 所有可能的属性名称：
    # fields="id, parent_id, title, body, created_time, updated_time, is_conflict, latitude, longitude, altitude, author, source_url, is_todo, todo_due, todo_completed, source, source_application, application_data, order, user_created_time, user_updated_time, encryption_cipher_text, encryption_applied, markup_language, is_shared, share_id, conflict_original_id, master_key_id, body_html, base_url, image_data_url, crop_rect")
    # 经过测试，fields中不能携带的属性值有：
    # latitude, longitude, altitude, master_key_id, body_html,  image_data_url, crop_rect
    fields="id, parent_id, title, body, created_time, updated_time, is_conflict, author, source_url, is_todo, todo_due, todo_completed, source, source_application, application_data, order, user_created_time, user_updated_time, encryption_cipher_text, encryption_applied, markup_language, is_shared, share_id, conflict_original_id"
    note = api.get_note(id, fields=fields)

    return note


# %% [markdown]
# ### noteid_used(target_id)

# %%
def noteid_used(targetid):
    api, token, port = getapi()
    try:
        getnote(targetid)
        return True
    except Exception as e:
        log.info(f"id为{targetid}的笔记不存在，id号可用。{e}")
        return False


# %% [markdown]
# ### resid_used(target_id)

# %%
def resid_used(targetid):
    api, token, port = getapi()
    try:
        res = api.get_resource(id_=targetid)
        return True
    except Exception as e:
        log.info(f"id为{targetid}的资源文件不存在，id号可用。")
        return False


# %% [markdown]
# ### createnote(title="Superman", body="Keep focus, man!", noteid_spec=None, parent_id=None, imgdata64=None)

# %%
def createnote(title="Superman", body="Keep focus, man!", parent_id=None, imgdata64=None):
    """
    按照传入的参数值构建笔记并返回id
    """

    api, token, port = getapi()
    if imgdata64:
        noteid = api.add_note(title=title, image_data_url=f"data:image/png;base64,{imgdata64}")
        api.modify_note(noteid, body=f"{body}\n{getnote(noteid).body}")
    else:
        noteid = api.add_note(title=title, body=body)
    if parent_id:
        api.modify_note(noteid, parent_id=parent_id)
    note = getnote(noteid)
    matches = re.findall(r"\[.*\]\(:.*\/([A-Za-z0-9]{32})\)", note.body)
    if len(matches) > 0:
        log.info(f"笔记《{note.title}》（id：{noteid}）构建成功，包含了资源文件{matches}。")
    else:
        log.info(f"笔记《{note.title}》（id：{noteid}）构建成功。")

    return noteid


# %% [markdown]
# ### createnote(title="Superman", body="Keep focus, man!", noteid_spec=None, parent_id=None, imgdata64=None)

# %%
def createnotewithfile(title="Superman", body="Keep focus, man!", parent_id=None, filepath=None):
    """
    按照传入的参数值构建笔记并返回id
    """

    api, token, port = getapi()
    if filepath:
        note_id = api.add_note(title=title)
        resource_id = api.add_resource(filename=filepath, title=filepath.split("/")[-1])
        api.add_resource_to_note(resource_id=resource_id, note_id=note_id)
        api.modify_note(note_id, body=f"{api.get_note(note_id).body}\n{body}")
    else:
        note_id = api.add_note(title=title, body=body)
    if parent_id:
        api.modify_note(noteid, parent_id=parent_id)
    note = getnote(note_id)
    matches = re.findall(r"\[.*\]\(:.*\/([A-Za-z0-9]{32})\)", note.body)
    if len(matches) > 0:
        log.info(f"笔记《{note.title}》（id：{noteid}）构建成功，包含了资源文件{matches}。")
    else:
        log.info(f"笔记《{note.title}》（id：{noteid}）构建成功。")

    return noteid


# %% [markdown]
# ### updatenote_title(noteid, titlestr)

# %%
def updatenote_title(noteid, titlestr):
    api = getapi()[0]
    note = getnote(noteid)
    titleold = note.title
    if titlestr == titleold:
        return
    api.modify_note(noteid, title=titlestr)
    log.info(f"笔记《{titleold}》的标题被更新为《{titlestr}》。")


# %% [markdown]
# ### updatenote_body(noteid, bodystr)

# %%
def updatenote_body(noteid, bodystr):
    api = getapi()[0]
    note = getnote(noteid)
    api.modify_note(noteid, body=bodystr)
    log.info(f"笔记《{note.title}》（id：{noteid}）的body内容被更新了。")


# %% [markdown]
# ### updatenote_imgdata(noteid, imgdata64, imgtitle=None)

# %%
def updatenote_imgdata(noteid, imgdata64=None, imgtitle=None):
    """
    用构新去旧的方式更新包含资源的笔记，返回新建笔记的id和资源id列表
    """
    api = getapi()[0]
    note = getnote(noteid)
    origin_body = note.body
    if (origin_body is None) or (len(origin_body) == 0):
        log.critical(f"笔记《{note.title}》（id：{noteid}）的内容为空，没有包含待更新的资源文件信息。")
        return
    print(f"笔记《{note.title}》（id：{noteid}）的内容为：\t{origin_body}")

    matches = re.findall(r"\[.*\]\(:.*\/([A-Za-z0-9]{32})\)", origin_body)
    for resid in matches:
        if resid_used(resid):
            api.delete_resource(resid)
            log.critical(f"资源文件（id：{resid}）成功删除。")
        else:
            log.critical(f"资源文件（id：{resid}）不存在，无法删除，跳过。")
    api.delete_note(noteid)
    log.info(f"笔记《{note.title}》（id：{noteid}）中的资源文件{matches}和该笔记都已从笔记系统中删除！")

    # notenew_id = api.add_note(title=note.title, image_data_url=f"data:image/png;base64,{imgdata64}")
    notenew_id = createnote(title=note.title, imgdata64=imgdata64)
    notenew = getnote(notenew_id)
    matchesnew = re.findall(r"\[.*\]\(:.*\/([A-Za-z0-9]{32})\)", notenew.body)
    res_id_lst = matchesnew
    if not imgtitle:
        imgtitle = f"happyjoplin {arrow.now()}"
    api.modify_resource(id_=res_id_lst[0], title=f"{imgtitle}")
    log.info(f"构建新的笔记《{note.title}》（id：{notenew_id}）成功，并且构建了新的资源文件{matchesnew}进入笔记系统。")
    print(f"笔记《{notenew.title}》（id：{notenew_id}）的内容为：\t{notenew.body}")

    return notenew_id, res_id_lst


# %% [markdown]
# ### test_updatenote_imgdata()

# %%
def test_updatenote_imgdata():
    note_health_lst = searchnotes("title:健康动态日日升")
    noteid = note_health_lst[0].id
    print(noteid)
    newfilename = os.path.abspath(f"{getdirmain() / 'data'}/QR.png")
    print(newfilename)
    image_data = tools.encode_base64(newfilename)
    print(image_data)
    notenew_id, res_id_lst = updatenote_imgdata(noteid=noteid, imgdata64=image_data, imgtitle="QR.png")
    print(f"包含新资源文件的新笔记的id为：{notenew_id}")
    resfile = api.get_resource_file(id_=res_id_lst[0])
    print(f"资源文件大小（二进制）为：{len(resfile)}字节。")


# %% [markdown]
# ### explore_resource(res_id)

# %%
def explore_resource(res_id):
    api = getapi()[0]
    # fields = ['encryption_blob_encrypted', 'share_id', 'mime', 'updated_time', 'master_key_id', 'is_shared', 'user_updated_time', 'encryption_applied', 'user_created_time', 'size', 'filename', 'file_extension', 'encryption_cipher_text', 'id', 'title', 'created_time']
    # res4test = api.get_resource(res_id, fileds=fields)
    resnew = api.get_resource(res_id)
    log.info(f"id为{res_id}的资源标题为《{resnew.title}》")
    res_file = api.get_resource_file(res_id)
    log.info(f"id为{res_id}的资源文件大小为{len(res_file)}")


# %% [markdown]
# ### modify_resource(res_id, imgdata64=None)

# %%
def modify_resource(res_id, imgdata64=None):
    """
    试图更新data但是无法成功，暂存之
    """
    api, token, port = getapi()
    res = api.get_resource(res_id)
    log.info(f"id为{res_id}的资源标题为《{res.title}》")
    res_file = api.get_resource_file(res_id)
    log.info(f"id为{res_id}的资源文件大小为{len(res_file)}")
    if not imgdata64:
        oldtitle = res.title
        newtitle = "This time is modify time for me."
        api.modify_resource(id_=res_id, title=newtitle)
        log.info(f"id为{res_id}的资源标题从《{oldtitle}》更改为《{newtitle}》")
    else:
        datastr = f"data:image/png;base64,{imgdata64}"
        begin_str = f"curl -X PUT -F 'data=\"{datastr}\"'"
        props_str = " -F 'props={\"title\":\"my modified title\"}'"
        url_str = f" http://localhost:{port}/resources/{res_id}?token={token}"
        update_curl_str = begin_str + props_str + url_str
        print(update_curl_str)
        outstr = execcmd(update_curl_str)
        log.info(f"{outstr}")
        # api.modify_resource(id_=res_id, data=f"data:image/png;base64,{imgdata64}")
    resnew = api.get_resource(res_id)
    log.info(f"id为{res_id}的资源标题为《{resnew.title}》")
    res_file = api.get_resource_file(res_id)
    log.info(f"id为{res_id}的资源文件大小为{len(res_file)}")

    return res_file


# %% [markdown]
# ### getreslst(noteid)

# %%
def getreslst(noteid):
    """
    以字典列表的形式返回输入id笔记包含的资源文件，包含id、title和contentb
    """
    api, token, port = getapi()
    dLst = api.get_resources(note_id=noteid)
    # print(type(dLst), dLst)
    reslst = []
    for res in dLst.items:
        sond = dict()
        sond["id"] = res.id
        sond["title"] = res.title
        sond["contentb"] = api.get_resource_file(res.id)
        reslst.append(sond)
    # print(reslst[0])
    return reslst


# %% [markdown]
# ### searchnotes(key, parent_id=None)

# %%
def searchnotes(key, parent_id=None):
    """
    传入关键字搜索并返回笔记列表，每个笔记中包含了所有可能提取field值
    """
    api = getapi()[0]
    # 经过测试，fields中不能携带的属性值有：latitude, longitude, altitude, master_key_id, body_html,  image_data_url, crop_rect
    # note = api.get_note(id, fields="id, parent_id, title, body, created_time, updated_time, is_conflict, author, source_url, is_todo, todo_due, todo_completed, source, source_application, application_data, order, user_created_time, user_updated_time, encryption_cipher_text, encryption_applied, markup_language, is_shared, share_id, conflict_original_id")
    # note = api.get_note(id, fields="id, latitude, longitude, altitude")
    fields="id, parent_id, title, body, created_time, updated_time, is_conflict, author, source_url, is_todo, todo_due, todo_completed, source, source_application, application_data, order, user_created_time, user_updated_time, encryption_cipher_text, encryption_applied, markup_language, is_shared, share_id, conflict_original_id"
    results = api.search(query=key, fields=fields).items
    log.info(f"搜索“{key}”，找到{len(results)}条笔记")
    if parent_id:
        nb= api.get_notebook(parent_id)
        results = [note for note in results if note.parent_id == parent_id]
        log.info(f"限定笔记本《{nb.title}》后，搜索结果有{len(results)}条笔记")

    return results


# %% [markdown]
# ### readinifromcloud()

# %%
@set_timeout(180, after_timeout)
def readinifromcloud():
    """
    通过对比更新时间（timestamp）来判断云端配置笔记是否有更新，有更新则更新至本地ini文件，确保数据新鲜
    """
    # 在happyjpsys配置文件中查找ini_cloud_updatetimestamp，找不到则表示首次运行，置零
    if not (ini_cloud_updatetimestamp := getcfpoptionvalue('happyjpsys', 'joplin', 'ini_cloud_updatetimestamp')):
        ini_cloud_updatetimestamp = 0

    # 在happyjp配置文件中查找ini_cloud_id，找不到则在云端搜索，搜不到就新建一个，无论是找到了还是新建一个，在happyjp中相应赋值
    if (noteid_inifromcloud := getcfpoptionvalue('happyjp', 'joplin', 'ini_cloud_id')) is None:
        if (resultitems := searchnotes("title:happyjoplin云端配置")) and (len(resultitems) > 0):
            noteid_inifromcloud = resultitems[0].id
        else:
            noteid_inifromcloud = createnote("happyjoplin云端配置", "")
        setcfpoptionvalue('happyjp', 'joplin', 'ini_cloud_id', str(noteid_inifromcloud))
    # print(noteid_inifromcloud)

    note = getnote(noteid_inifromcloud)
    noteupdatetimewithzone = arrow.get(note.updated_time, tzinfo="local")
    # print(arrow.get(ini_cloud_updatetimestamp, tzinfo=get_localzone()), note.updated_time, noteupdatetimewithzone)
    if noteupdatetimewithzone.timestamp() == ini_cloud_updatetimestamp:
        # print(f'配置笔记无更新【最新更新时间为：{noteupdatetimewithzone}】，不对本地化的ini配置文件做更新。')
        return

    items = note.body.split("\n")
    # print(items)
    fileobj = open(str(getdirmain() / 'data' / 'happyjpinifromcloud.ini'), 'w', encoding='utf-8')
    for item in items:
        fileobj.write(item + '\n')
    fileobj.close()

    setcfpoptionvalue('happyjpsys', 'joplin', 'ini_cloud_updatetimestamp', str(noteupdatetimewithzone.timestamp()))
    log.info(f'云端配置笔记有更新【（{noteupdatetimewithzone}）->（{arrow.get(ini_cloud_updatetimestamp, tzinfo="local")}）】，更新本地化的ini配置文件。')


# %% [markdown]
# ### getinivaluefromcloud(section, option)

# %%
def getinivaluefromcloud(section, option):
    readinifromcloud()

    return getcfpoptionvalue('happyjpinifromcloud', section, option)


# %% [markdown]
# ## 主函数main（）

# %%
if __name__ == '__main__':
    if not_IPython():
        log.info(f'开始运行文件\t{__file__}')
    # joplinport()

    api, token, port = getapi()
    # createnote(title="重生的笔记", body="some things happen", noteid_spec="3ffccc7c48fc4b25bcd7cf3841421ce5")
    test_updatenote_imgdata()
    # test_modify_res()
    # log.info(f"ping服务器返回结果：\t{api.ping()}")
    # allnotes = getallnotes()[:6]
    # # print(allnotes)
    # myid = allnotes[-3].id
    # print(getnote(myid))

    # location_keys = ["longitude", "latitude", "altitude"]
    # fields=",".join(["id,title,body,parent_id"] + location_keys)
    # getnoteswithfields(fields)

    # print(getinivaluefromcloud("happyjplog", "loglimit"))
    # findnotes = searchnotes("title:健康*")
    # findnotes = searchnotes("title:文峰*")

    # cmd = "joplin ls notebook"
    # cmd = "joplin status"
    # cmd = "joplin ls -l"
    # result = joplincmd(cmd)
    # print(result.stdout)

    if not_IPython():
        log.info(f'Done.结束执行文件\t{__file__}')

