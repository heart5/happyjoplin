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
import pathmagic
import subprocess
import arrow
import joppy
import datetime
from joppy.api import Api
from tzlocal import get_localzone
from dateutil import tz

# %%
with pathmagic.context():
    from func.first import getdirmain
    from func.configpr import getcfpoptionvalue, setcfpoptionvalue
    # from func.evernttest import get_notestore, imglist2note, readinifromnote, evernoteapijiayi, makenote, getinivaluefromnote
    from func.logme import log
    # from func.wrapfuncs import timethis, ift2phone
    # from func.termuxtools import termux_location, termux_telephony_deviceinfo
    # from func.nettools import ifttt_notify
    from etc.getid import getdevicename
    from func.sysfunc import not_IPython, set_timeout, after_timeout, execcmd


# %% [markdown]
# ## 功能函数集

# %% [markdown]
# ### def joplincmd(cmd)

# %%
def joplincmd(cmd):
    """
    运行joplin命令行并返回输出结果
    """
    # result = subprocess.run(cmd, stdout=subprocess.PIPE, text=True, shell=True)
    return execcmd("cmd")


# %% [markdown]
# ### joplintokenport()

# %%
def joplintokenport():
    tokenstr = execcmd("joplin config api.token")
    portstr = execcmd("joplin config api.port")
    if (tokenstr.find("=") == -1) | (portstr.find("=") == -1):
        log.critical(f"主机【{getdevicename()}】登陆账户（{execcmd('whoami')}）貌似尚未运行joplin server！\n退出运行！！！")
        exit(1)
    token = tokenstr.split("=")[1].strip()
    portraw = portstr.split("=")[1].strip()
    if portraw == "null":
        port = 41184
    else:
        port = portraw

    return token, port


# %% [markdown]
# ### getapi()

# %%
def getapi():
    token, port = joplintokenport()
    url = f"http://localhost:{port}"
    api = Api(token = token, url=url)

    return api


# %% [markdown]
# ### def getallnotes()

# %%
def getallnotes():
    """
    获取所有笔记；默认仅输出id、parent_id和title三项有效信息
    """
    api = getapi()

    return api.get_all_notes()


# %% [markdown]
# ### getnoteswithfields(fields, limit=10)

# %%
def getnoteswithfields(fields, limit=10):
    api = getapi()
    fields_ls = fields.split(",")
    allnotes = [note for note in api.get_all_notes(fields = fields)[:limit]]
    geonotes = [note for note in allnotes if note.altitude != 0 and note.longitude != 0]
    print(len(allnotes),len(geonotes))
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
# ### def getnote(id)

# %%
def getnote(id):
    """
    通过id获取笔记的所有可能内容，NoteData
    """
    api = getapi()
    # note = api.get_note(id, fields="id, parent_id, title, body, created_time, updated_time, is_conflict, latitude, longitude, altitude, author, source_url, is_todo, todo_due, todo_completed, source, source_application, application_data, order, user_created_time, user_updated_time, encryption_cipher_text, encryption_applied, markup_language, is_shared, share_id, conflict_original_id, master_key_id, body_html, base_url, image_data_url, crop_rect")
    # 经过测试，fields中不能携带的属性值有：latitude, longitude, altitude, master_key_id, body_html,  image_data_url, crop_rect
    note = api.get_note(id, fields="id, parent_id, title, body, created_time, updated_time, is_conflict, author, source_url, is_todo, todo_due, todo_completed, source, source_application, application_data, order, user_created_time, user_updated_time, encryption_cipher_text, encryption_applied, markup_language, is_shared, share_id, conflict_original_id")
    # note = api.get_note(id, fields="id, latitude, longitude, altitude")
    
    return note


# %% [markdown]
# ### def createnote(title="Superman", body="Keep focus, man!", parent_id=None)

# %%
def createnote(title="Superman", body="Keep focus, man!", parent_id=None):
    api = getapi()
    if parent_id:
        noteid = api.add_note(title=title, body=body, parent_id=parent_id)
    else:
        noteid = api.add_note(title=title, body=body)
    
    return noteid


# %% [markdown]
# ### def updatenote_title(noteid, titlestr)

# %%
def updatenote_title(noteid, titlestr):
    api = getapi()
    api.modify_note(noteid, title=titlestr)
    log.info(f"id为{noteid}的笔记的title被更新了。")



# %% [markdown]
# ### updatenote_body(noteid, bodystr)

# %%
def updatenote_body(noteid, bodystr):
    api = getapi()
    api.modify_note(noteid, body=bodystr)
    log.info(f"id为{noteid}的笔记的body被更新了。")


# %% [markdown]
# ### def searchnote(key)

# %%
def searchnotes(key):
    """
    传入关键字搜索并返回笔记列表，每个笔记中包含了所有可能提取field值
    """
    api = getapi()
    # note = api.get_note(id, fields="id, parent_id, title, body, created_time, updated_time, is_conflict, latitude, longitude, altitude, author, source_url, is_todo, todo_due, todo_completed, source, source_application, application_data, order, user_created_time, user_updated_time, encryption_cipher_text, encryption_applied, markup_language, is_shared, share_id, conflict_original_id, master_key_id, body_html, base_url, image_data_url, crop_rect")
    # 经过测试，fields中不能携带的属性值有：latitude, longitude, altitude, master_key_id, body_html,  image_data_url, crop_rect
    # note = api.get_note(id, fields="id, parent_id, title, body, created_time, updated_time, is_conflict, author, source_url, is_todo, todo_due, todo_completed, source, source_application, application_data, order, user_created_time, user_updated_time, encryption_cipher_text, encryption_applied, markup_language, is_shared, share_id, conflict_original_id")
    # note = api.get_note(id, fields="id, latitude, longitude, altitude")
    fields="id, parent_id, title, body, created_time, updated_time, is_conflict, author, source_url, is_todo, todo_due, todo_completed, source, source_application, application_data, order, user_created_time, user_updated_time, encryption_cipher_text, encryption_applied, markup_language, is_shared, share_id, conflict_original_id"
    results = api.search(query=key,fields=fields)
    log.info(f"搜索“{key}”，找到{len(results.items)}条笔记")
    for notedata in results.items:
        print()
        print(notedata.title, notedata.id, arrow.get(notedata.updated_time, tzinfo=get_localzone()), "\n" + notedata.body[:30])
    
    return results.items


# %% [markdown]
# ### def readinifromcloud()

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
    noteupdatetimewithzone = arrow.get(note.updated_time, tzinfo=get_localzone())
    # print(arrow.get(ini_cloud_updatetimestamp, tzinfo=get_localzone()), note.updated_time, noteupdatetimewithzone)
    if noteupdatetimewithzone.timestamp() == ini_cloud_updatetimestamp:
        # print(f'配置笔记无更新【最新更新时间为：{noteupdatetimewithzone}】，不对本地化的ini配置文件做更新。')
        return

    items = note.body.split("\n")
    # print(items)
    fileobj = open(str(getdirmain() / 'data' / 'happyjpinifromcloud.ini'), 'w',
                   encoding='utf-8')
    for item in items:
        fileobj.write(item + '\n')
    fileobj.close()

    setcfpoptionvalue('happyjpsys', 'joplin', 'ini_cloud_updatetimestamp', str(noteupdatetimewithzone.timestamp()))
    log.info(f'云端配置笔记有更新【（{noteupdatetimewithzone}）->（{arrow.get(ini_cloud_updatetimestamp, tzinfo=get_localzone())}）】，更新本地化的ini配置文件。')


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

    allnotes = getallnotes()[:6]
    # print(allnotes)
    myid = allnotes[-3].id
    # print(getnote(myid))
    
    location_keys = ["longitude", "latitude", "altitude"]
    fields=",".join(["id,title,body,parent_id"] + location_keys)
    # getnoteswithfields(fields)

    print(getinivaluefromcloud("happyjplog", "loglimit"))

    findnotes = searchnotes("title:健康*")
    # findnotes = searchnotes("title:文峰*")

    cmd = "joplin ls notebook"
    cmd = "joplin status"
    cmd = "joplin ls -l"
    # result = joplincmd(cmd)
    # print(result.stdout)
    

    if not_IPython():
        log.info(f'Done.结束执行文件\t{__file__}')

