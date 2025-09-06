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
import hashlib
import os
import re

# import requests
# import subprocess
import tempfile
from io import BytesIO

import arrow

# import joppy
# import datetime
# from pathlib import Path
from joppy.client_api import ClientApi
from tzlocal import get_localzone
# from joppy.api import Api
# from joppy import tools
# from tzlocal import get_localzone
# from dateutil import tz

# %%
import pathmagic

with pathmagic.context():
    # from func.termuxtools import termux_location, termux_telephony_deviceinfo
    # from func.nettools import ifttt_notify
    from etc.getid import getdevicename, gethostuser
    from func.configpr import getcfpoptionvalue, setcfpoptionvalue
    from func.first import getdirmain
    from func.logme import log
    from func.sysfunc import after_timeout, execcmd, not_IPython, set_timeout
    from func.wrapfuncs import timethis


# %% [markdown]
# ## 功能函数集

# %% [markdown]
# ### getapi()


# %%
@timethis
def getapi():
    """
    获取api方便调用，自适应不同的joplin server端口；通过命令行joplin获取相关配置参数值
    """
    # 一次运行两个命令，减少一次命令行调用
    jpcmdstr = execcmd("joplin config api.token&joplin config api.port")
    if jpcmdstr.find("=") == -1:
        logstr = f"主机【{gethostuser()}】貌似尚未运行joplin server！\n退出运行！！！"
        log.critical(f"{logstr}")
        exit(1)
    splitlst = [line.split("=") for line in re.findall(".+=.*", jpcmdstr)]
    # 简化api.token为token，port类似，同时把默认的port替换为41184
    kvdict = dict(
        [
            [
                x.split(".")[-1].strip()
                if x.split(".")[-1].strip() != "null"
                else 41184
                for x in sonlst
            ]
            for sonlst in splitlst
        ]
    )

    url = f"http://localhost:{kvdict.get('port')}"
    # print(kvdict.get("token"), url)
    api = ClientApi(token=kvdict.get("token"), url=url)

    return api


# %% [markdown]
# ### searchnotebook(query)


# %%
@timethis
def searchnotebook(title):
    """
    查找指定title（全名）的笔记本并返回id，如果不存在，则新建一个返回id
    """
    global jpapi
    result = jpapi.search(query=title, type="folder")
    if len(result.items) == 0:
        nbid = jpapi.add_notebook(title=title)
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
    global jpapi

    return jpapi.get_all_notes()


# %% [markdown]
# ### getnoteswithfields(fields, limit=10)


# %%
def getnoteswithfields(fields, limit=10):
    global jpapi
    fields_ls = fields.split(",")
    allnotes = [note for note in jpapi.get_all_notes(fields=fields)[:limit]]
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
# ### getnote(id, full_analysis=False)


# %%
def getnote(noteid, full_analysis=False):
    """
    通过id获取笔记内容，默认只获取基础字段，可选全字段分析
    :param noteid: 笔记ID
    :param full_analysis: 是否进行全字段分析（默认False）
    :return: NoteData对象
    """
    # 基础字段（parent_id到source_url）
    base_fields = "parent_id, title, body, created_time, updated_time, is_conflict, latitude, longitude, altitude, author, source_url"

    if full_analysis:
        # 全字段分析模式
        extended_fields = "is_todo, todo_due, todo_completed, source, source_application, application_data, order, user_created_time, user_updated_time, encryption_cipher_text, encryption_applied, markup_language, is_shared, share_id, conflict_original_id, master_key_id, body_html, base_url, image_data_url, crop_rect"
        allowed_fields = _validate_fields(noteid, base_fields + ", " + extended_fields)
        return jpapi.get_note(
            noteid,
            fields=",".join([f.strip() for f in ("id, " + allowed_fields).split(",")]),
        )
    else:
        # 常规模式：直接获取基础字段
        return jpapi.get_note(
            noteid,
            fields=",".join([f.strip() for f in ("id, " + base_fields).split(",")]),
        )


# %% [markdown]
# ### _validate_fields(noteid, fields_str)


# %%
def _validate_fields(noteid, fields_str):
    """检查字段可用性并返回有效字段列表"""
    flst = [f.strip() for f in fields_str.split(",")]
    allowed_fields = []

    for field in flst:
        if field == "id":  # 始终包含id
            allowed_fields.append(field)
            continue

        try:
            # 测试单个字段是否可获取
            jpapi.get_note(noteid, fields=f"id,{field}")
            allowed_fields.append(field)
        except Exception:
            continue  # 跳过无效字段

    # 共享笔记检测逻辑
    if "share_id" in allowed_fields:
        logging.debug(f"笔记（id：{noteid}）是共享笔记")
    return ",".join(allowed_fields)


# %% [markdown]
# ### noteid_used(target_id)


# %%
def noteid_used(targetid):
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
    global jpapi
    try:
        res = jpapi.get_resource(id_=targetid)
        return True
    except Exception as e:
        log.info(f"id为{targetid}的资源文件不存在，id号可用。")
        return False


# %% [markdown]
# ### createnote(title="Superman", body="Keep focus, man!", noteid_spec=None, parent_id=None, imgdata64=None)


# %%
@timethis
def createnote(
    title="Superman", body="Keep focus, man!", parent_id=None, imgdata64=None
):
    """
    按照传入的参数值构建笔记并返回id
    """

    global jpapi
    if imgdata64:
        noteid = jpapi.add_note(
            title=title, image_data_url=f"data:image/png;base64,{imgdata64}"
        )
        jpapi.modify_note(noteid, body=f"{getnote(noteid).body}\n{body}")
    else:
        noteid = jpapi.add_note(title=title, body=body)
    if parent_id:
        jpapi.modify_note(noteid, parent_id=parent_id)
    note = getnote(noteid)
    matches = re.findall(r"\[.*\]\(:.*\/([A-Za-z0-9]{32})\)", note.body)
    if len(matches) > 0:
        log.info(
            f"笔记《{note.title}》（id：{noteid}）构建成功，包含了资源文件{matches}。"
        )
    else:
        log.info(f"笔记《{note.title}》（id：{noteid}）构建成功。")

    return noteid


# %% [markdown]
# ### createresource(filename, title=None)


# %%
def createresource(filename, title=None):
    global jpapi
    if not title:
        res_title = title
    else:
        res_title = filename.split("/")[-1]
    res_id = jpapi.add_resource(filename=filename, title=res_title)
    log.info(f"资源文件《{res_title}》创建成功，纳入笔记资源系统管理，可以正常被调用！")

    return res_id


# %% [markdown]
# ### createresourcefromobj(file_obj, title=None)


# %%
def createresourcefromobj(file_obj, title=None):
    # print(file_obj)
    # 改用用户空间临时目录（绕过沙箱限制）
    tmp_dir = os.path.expanduser("~/.joplin_temp")
    os.makedirs(tmp_dir, exist_ok=True)
    # 创建一个临时文件
    with tempfile.NamedTemporaryFile(
        mode="wb",
        delete=False,
        prefix="joplin_res_",  # 便于识别
        dir=tmp_dir,  # 指定目录
    ) as tmpfile:
        # 将 BytesIO 的内容写入临时文件
        tmpfile.write(file_obj.getvalue())
        tmpfile_path = tmpfile.name
        os.chmod(tmpfile_path, 0o644)  # 显式设置权限
        tmpfile.flush()  # 强制写入磁盘
        # print(f"临时文件实际路径: {tmpfile_path}")
        # print(f"文件存在状态: {os.path.exists(tmpfile_path)}")
        # print(f"文件权限: {oct(os.stat(tmpfile_path).st_mode)}")

        try:
            # # 添加文件句柄释放保障
            # os.close(tmpfile.file.fileno())
            # os.sync()
            # 使用临时文件的路径调用 add_resource
            # 使用文件描述符重定向
            res_id = jpapi.add_resource(filename=tmpfile_path, title=title)
            log.info(
                f"资源文件《{title}》从file_obj创建成功，纳入笔记资源系统管理，可以正常被调用！"
            )
        except Exception as e:
            log.error(f"资源上传失败: {str(e)}")
            raise
        finally:
            try:
                # 延迟删除保障
                if os.path.exists(tmpfile_path):
                    os.unlink(tmpfile_path)
            except Exception as e:
                log.warning(f"文件清理失败: {str(e)}")
        return res_id


# %% [markdown]
# ### add_resource_from_bytes(data_bytes, title, mime_type="image/png")

# %%
def add_resource_from_bytes(data_bytes, title, mime_type="image/png"):
    """从字节数据创建资源"""
    file_obj = BytesIO(data_bytes)
    return createresourcefromobj(file_obj, title)

# %% [markdown]
# ### deleteresourcesfromnote(noteid)


# %%
def deleteresourcesfromnote(noteid):
    """
    遍历笔记中包含的资源文件并删除之！
    """
    global jpapi
    note = getnote(noteid)
    ptn = re.compile(r"\(:/(\w+)\)")
    residlst = re.findall(ptn, note.body)
    for i in range(len(residlst)):
        try:
            jpapi.delete_resource(residlst[i])
            log.info(
                f"【{i + 1}/{len(residlst)}】资源文件（{residlst[i]}）从笔记（{note.title}）中删除成功（也被从笔记资源系统中彻底删除）！"
            )
        except Exception as e:
            log.info(f"{e}")


# %% [markdown]
# ### createnotewithfile(title="Superman", body="Keep focus, man!", parent_id=None, filepath=None)


# %%
@timethis
def createnotewithfile(
    title="Superman", body="Keep focus, man!", parent_id=None, filepath=None
):
    """
    按照传入的参数值构建笔记并返回id
    """

    global jpapi
    if filepath:
        note_id = jpapi.add_note(title=title)
        resource_id = jpapi.add_resource(
            filename=filepath, title=filepath.split("/")[-1]
        )
        jpapi.add_resource_to_note(resource_id=resource_id, note_id=note_id)
        jpapi.modify_note(note_id, body=f"{jpapi.get_note(note_id).body}\n{body}")
    else:
        note_id = jpapi.add_note(title=title, body=body)
    if parent_id:
        jpapi.modify_note(noteid, parent_id=parent_id)
    note = getnote(note_id)
    matches = re.findall(r"\[.*\]\(:.*\/([A-Za-z0-9]{32})\)", note.body)
    if len(matches) > 0:
        log.info(
            f"笔记《{note.title}》（id：{noteid}）构建成功，包含了资源文件{matches}。"
        )
    else:
        log.info(f"笔记《{note.title}》（id：{noteid}）构建成功。")

    return noteid


# %% [markdown]
# ### updatenote_title(noteid, titlestr, parent_id=None)


# %%
def updatenote_title(noteid, titlestr, parent_id=None):
    global jpapi
    note = getnote(noteid)
    titleold = note.title
    if (parent_id is not None) & (note.parent_id != parent_id):
        print(f"传入的笔记父目录id为{note.parent_id}，将被调整为{parent_id}")
        jpapi.modify_note(noteid, parent_id=parent_id)
        log.critical(
            f"笔记《{titleold}》所在笔记本从《{jpapi.get_notebook(note.parent_id).title}》调整为《{jpapi.get_notebook(parent_id).title}》。"
        )
    if titlestr == titleold:
        return
    jpapi.modify_note(noteid, title=titlestr)
    log.info(f"笔记《{titleold}》的标题被更新为《{titlestr}》。")


# %% [markdown]
# ### updatenote_body(noteid, bodystr, parent_id=None)


# %%
def updatenote_body(noteid, bodystr, parent_id=None):
    global jpapi
    note = getnote(noteid)
    if (parent_id is not None) & (note.parent_id != parent_id):
        print(f"传入的笔记父目录id为{note.parent_id}，将被调整为{parent_id}")
        jpapi.modify_note(noteid, parent_id=parent_id)
        log.critical(
            f"笔记《{note.title}》所在笔记本从《{jpapi.get_notebook(note.parent_id).title}》调整为《{jpapi.get_notebook(parent_id).title}》。"
        )
    jpapi.modify_note(noteid, body=bodystr)
    log.info(f"笔记《{note.title}》（id：{noteid}）的body内容被更新了。")


# %% [markdown]
# ### updatenote_imgdata(noteid, imgdata64, parent_id=None, imgtitle=None)


# %%
@timethis
def updatenote_imgdata(noteid, parent_id=None, imgdata64=None, imgtitle=None):
    """
    用构新去旧的方式更新包含资源的笔记，返回新建笔记的id和资源id列表
    """
    global jpapi
    note = getnote(noteid)
    origin_body = note.body
    if (origin_body is None) or (len(origin_body) == 0):
        log.critical(
            f"笔记《{note.title}》（id：{noteid}）的内容为空，没有包含待更新的资源文件信息。"
        )
        return
    print(f"笔记《{note.title}》（id：{noteid}）的内容为：\t{origin_body}")

    matches = re.findall(r"\[.*\]\(:.*\/([A-Za-z0-9]{32})\)", origin_body)
    for resid in matches:
        if resid_used(resid):
            jpapi.delete_resource(resid)
            log.critical(f"资源文件（id：{resid}）成功删除。")
        else:
            log.critical(f"资源文件（id：{resid}）不存在，无法删除，跳过。")
    jpapi.delete_note(noteid)
    log.info(
        f"笔记《{note.title}》（id：{noteid}）中的资源文件{matches}和该笔记都已从笔记系统中删除！"
    )

    # notenew_id = api.add_note(title=note.title, image_data_url=f"data:image/png;base64,{imgdata64}")
    if parent_id:
        notenew_id = createnote(
            title=note.title, imgdata64=imgdata64, parent_id=parent_id
        )
    else:
        notenew_id = createnote(title=note.title, imgdata64=imgdata64)
    if parent_id != note.parent_id:
        jpapi.modify_note(notenew_id, parent_id=parent_id)
        nb_title = jpapi.get_notebook(parent_id).title
        nb_old_title = jpapi.get_notebook(note.parent_id).title
        log.critical(
            f"笔记《{note.title}》从笔记本《{nb_old_title}》调整到《{nb_title}》中！"
        )
    notenew = getnote(notenew_id)
    matchesnew = re.findall(r"\[.*\]\(:.*\/([A-Za-z0-9]{32})\)", notenew.body)
    res_id_lst = matchesnew
    if not imgtitle:
        imgtitle = f"happyjoplin {arrow.now()}"
    jpapi.modify_resource(id_=res_id_lst[0], title=f"{imgtitle}")
    log.info(
        f"构建新的笔记《{note.title}》（id：{notenew_id}）成功，并且构建了新的资源文件{matchesnew}进入笔记系统。"
    )
    print(f"笔记《{notenew.title}》（id：{notenew_id}）的内容为：\t{notenew.body}")

    return notenew_id, res_id_lst


# %% [markdown]
# ### test_updatenote_imgdata()


# %%
def test_updatenote_imgdata():
    global jpapi
    note_health_lst = searchnotes("健康动态日日升")
    noteid = note_health_lst[0].id
    print(noteid)
    newfilename = os.path.abspath(f"{getdirmain() / 'img' / 'fengye.jpg'}")
    print(newfilename)
    image_data = tools.encode_base64(newfilename)
    # print(image_data)
    notenew_id, res_id_lst = updatenote_imgdata(
        noteid=noteid, imgdata64=image_data, imgtitle="QR.png"
    )
    print(f"包含新资源文件的新笔记的id为：{notenew_id}")
    resfile = jpapi.get_resource_file(id_=res_id_lst[0])
    print(f"资源文件大小（二进制）为：{len(resfile)}字节。")


# %% [markdown]
# ### explore_resource(res_id)


# %%
def explore_resource(res_id):
    global jpapi
    # fields = ['encryption_blob_encrypted', 'share_id', 'mime', 'updated_time', 'master_key_id', 'is_shared', 'user_updated_time', 'encryption_applied', 'user_created_time', 'size', 'filename', 'file_extension', 'encryption_cipher_text', 'id', 'title', 'created_time']
    # res4test = api.get_resource(res_id, fileds=fields)
    resnew = jpapi.get_resource(res_id)
    log.info(f"id为{res_id}的资源标题为《{resnew.title}》")
    res_file = jpapi.get_resource_file(res_id)
    log.info(f"id为{res_id}的资源文件大小为{len(res_file)}")


# %% [markdown]
# ### modify_resource(res_id, imgdata64=None)


# %%
@timethis
def modify_resource(res_id, imgdata64=None):
    """
    试图更新data但是无法成功，暂存之
    """
    global jpapi
    res = jpapi.get_resource(res_id)
    log.info(f"id为{res_id}的资源标题为《{res.title}》")
    res_file = jpapi.get_resource_file(res_id)
    log.info(f"id为{res_id}的资源文件大小为{len(res_file)}")
    if not imgdata64:
        oldtitle = res.title
        newtitle = "This time is modify time for me."
        jpapi.modify_resource(id_=res_id, title=newtitle)
        log.info(f"id为{res_id}的资源标题从《{oldtitle}》更改为《{newtitle}》")
    else:
        datastr = f"data:image/png;base64,{imgdata64}"
        begin_str = f"curl -X PUT -F 'data=\"{datastr}\"'"
        props_str = ' -F \'props={"title":"my modified title"}\''
        url_str = f" {jpapi.url}/resources/{res_id}?token={jpapi.token}"
        update_curl_str = begin_str + props_str + url_str
        print(update_curl_str)
        outstr = execcmd(update_curl_str)
        log.info(f"{outstr}")
        # api.modify_resource(id_=res_id, data=f"data:image/png;base64,{imgdata64}")
    resnew = jpapi.get_resource(res_id)
    log.info(f"id为{res_id}的资源标题为《{resnew.title}》")
    res_file = jpapi.get_resource_file(res_id)
    log.info(f"id为{res_id}的资源文件大小为{len(res_file)}")

    return res_file


# %% [markdown]
# ### getreslst(noteid)


# %%
def getreslst(noteid):
    """
    以字典列表的形式返回输入id笔记包含的资源文件，包含id、title和contentb
    """
    global jpapi
    dLst = jpapi.get_resources(note_id=noteid)
    # print(type(dLst), dLst)
    reslst = []
    for res in dLst.items:
        sond = dict()
        sond["id"] = res.id
        sond["title"] = res.title
        sond["contentb"] = jpapi.get_resource_file(res.id)
        reslst.append(sond)
    # print(reslst[0])
    return reslst


# %% [markdown]
# ### searchnotes(key: str, filter: str= "title", parent_id=None)


# %%
@timethis
def searchnotes(key: str, filter: str = "title", parent_id: str = None):
    """
    传入关键字搜索并返回笔记列表，每个笔记中包含了所有可能提取field值
    """
    global jpapi
    # 经过测试，fields中不能携带的属性值有：latitude, longitude, altitude, master_key_id, body_html,  image_data_url, crop_rect，另外shared_id对于共享笔记本下的笔记无法查询，出错
    fields = "id, parent_id, title, body, created_time, updated_time, is_conflict, author, source_url, is_todo, todo_due, todo_completed, source, source_application, application_data, order, user_created_time, user_updated_time, encryption_cipher_text, encryption_applied, markup_language, is_shared, conflict_original_id"
    query = f"{filter}:{key}"
    results = jpapi.search(query=query, fields=fields).items
    log.info(f"搜索“{query}”，找到{len(results)}条笔记")
    if parent_id:
        nb = jpapi.get_notebook(parent_id)
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
    if not (
        ini_cloud_updatetimestamp := getcfpoptionvalue(
            "happyjpsys", "joplin", "ini_cloud_updatetimestamp"
        )
    ):
        ini_cloud_updatetimestamp = 0

    # 在happyjp配置文件中查找ini_cloud_id，找不到则在云端搜索，搜不到就新建一个，无论是找到了还是新建一个，在happyjp中相应赋值
    if (
        noteid_inifromcloud := getcfpoptionvalue("happyjp", "joplin", "ini_cloud_id")
    ) is None:
        if (resultitems := searchnotes("happyjoplin云端配置")) and (
            len(resultitems) > 0
        ):
            noteid_inifromcloud = resultitems[0].id
        else:
            noteid_inifromcloud = createnote("happyjoplin云端配置", "")
        setcfpoptionvalue("happyjp", "joplin", "ini_cloud_id", str(noteid_inifromcloud))
    # print(noteid_inifromcloud)

    note = getnote(noteid_inifromcloud)
    noteupdatetimewithzone = arrow.get(note.updated_time).to(get_localzone())
    # print(arrow.get(ini_cloud_updatetimestamp, tzinfo=get_localzone()), note.updated_time, noteupdatetimewithzone)
    if noteupdatetimewithzone.timestamp() == ini_cloud_updatetimestamp:
        # print(f'配置笔记无更新【最新更新时间为：{noteupdatetimewithzone}】，不对本地化的ini配置文件做更新。')
        return

    items = note.body.split("\n")
    # print(items)
    fileobj = open(
        str(getdirmain() / "data" / "happyjpinifromcloud.ini"), "w", encoding="utf-8"
    )
    for item in items:
        fileobj.write(item + "\n")
    fileobj.close()

    setcfpoptionvalue(
        "happyjpsys",
        "joplin",
        "ini_cloud_updatetimestamp",
        str(noteupdatetimewithzone.timestamp()),
    )
    log.info(
        f"云端配置笔记有更新【（{noteupdatetimewithzone}）->（{arrow.get(ini_cloud_updatetimestamp).to(get_localzone())}）】，更新本地化的ini配置文件。"
    )


# %% [markdown]
# ### getinivaluefromcloud(section, option)


# %%
def getinivaluefromcloud(section, option):
    readinifromcloud()

    return getcfpoptionvalue("happyjpinifromcloud", section, option)


# %% [markdown]
# ### content_hash(note_id)


# %%
def content_hash(note_id):
    note = getnote(note_id)
    return hashlib.md5(note.body.encode()).hexdigest()


# %% [markdown]
# ### 获取jpapi，全局共用

# %%
jpapi = getapi()

# %% [markdown]
# ## 主函数main（）

# %%
if __name__ == "__main__":
    if not_IPython():
        log.info(f"开始运行文件\t{__file__}")
    # joplinport()

    note_ids_to_monitor = [
        "ed8523d3812143e0943acd9c6cdd3ffe",
        "9025c19f884c40609bef2133d1a224a1",
    ]  # 需要监控的笔记ID列表，替换为实际的GUID
    for note_id in note_ids_to_monitor:
        updated_time = getnote(note_id).updated_time
        print(updated_time)
        utc_arrow = arrow.get(updated_time)
        local_tz = get_localzone()
        print(local_tz)
        # local_arrow = utc_arrow.to('Asia/Shanghai')
        local_arrow = utc_arrow.to(local_tz)
        print(local_arrow)

    # createnote(title="重生的笔记", body="some things happen", noteid_spec="3ffccc7c48fc4b25bcd7cf3841421ce5")
    # test_updatenote_imgdata()
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
        log.info(f"Done.结束执行文件\t{__file__}")
