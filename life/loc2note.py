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
# # 位置信息智能远程存储

# %% [markdown]
# ## 库导入

# %%
import hashlib
import os
import re
import sqlite3 as lite
from datetime import datetime
from io import BytesIO
from pathlib import Path

import pandas as pd
import xlsxwriter

# %%
import pathmagic

with pathmagic.context():
    from etc.getid import getdeviceid, getdevicename
    from filedatafunc import getfilemtime as getfltime
    from func.configpr import getcfpoptionvalue, setcfpoptionvalue
    from func.first import getdirmain, touchfilepath2depth
    from func.jpfuncs import (
        createnote,
        getinivaluefromcloud,
        getnote,
        getreslst,
        jpapi,
        searchnotebook,
        searchnotes,
        updatenote_body,
        updatenote_title,
    )
    from func.litetools import ifnotcreate, showtablesindb
    from func.logme import log
    from func.sysfunc import execcmd, not_IPython
    from func.wrapfuncs import timethis

# %% [markdown]
# ## 功能函数集

# %% [markdown]
# ### get_all_device_ids()

# %%
def get_all_device_ids():
    """从配置文件获取所有设备ID"""
    if (device_ids_str := getcfpoptionvalue("hjdevices", "DEVICES", "ids")) is not None:
        device_ids = [did for did in device_ids_str.split(",") if len(did) != 0]
    else:
        # 默认包含当前设备
        device_ids = [getdeviceid()]
        setcfpoptionvalue("hjdevices", "DEVICES", "ids", ",".join(device_ids))

    return device_ids


# %% [markdown]
# ### register_new_device(new_id)

# %%
def register_new_device(new_id):
    """注册新设备"""
    if (
        current_ids_str := getcfpoptionvalue("hjdevices", "DEVICES", "ids")
    ) is not None:
        current_ids = [did for did in current_ids_str.split(",") if len(did) != 0]
    else:
        current_ids = []
    if new_id not in current_ids:
        current_ids.append(new_id)
        setcfpoptionvalue("hjdevices", "DEVICES", "ids", ",".join(current_ids))

        log.info(f"新设备 {new_id} 注册成功")


# %%
def register_new_device(new_id):
    """注册新设备"""
    getdeviceid()
    current_ids = [
        did
        for did in getcfpoptionvalue("hjdevices", "DEVICES", "ids").split(",")
        if len(did) != 0
    ] or []
    new_id_not_in_current_ids = not (new_id in current_ids)

    if new_id_not_in_current_ids:
        current_ids.append(new_id)
        setcfpoptionvalue("hjdevices", "DEVICES", "ids", ",".join(current_ids))
        log.info(f"新设备 {new_id} 注册成功")


# %% [markdown]
# ### parse_location_txt(fl)

# %%
def parse_location_txt(fl):
    """解析位置信息文本文件为DataFrame"""
    try:
        # 指定日期格式，添加错误处理
        df = pd.read_csv(
            fl,
            sep="\t",
            header=None,
            parse_dates=[0],
            date_format="%Y-%m-%d %H:%M:%S",
            on_bad_lines="skip",
            names=[
                "time",
                "latitude",
                "longitude",
                "altitude",
                "accuracy",
                "bearing",
                "speed",
                "unknown1",
                "unknown2",
                "provider",
            ],
        )
        return df.sort_values("time").drop_duplicates()
    except Exception as e:
        log.error(f"解析位置文件失败: {str(e)}")
        return pd.DataFrame()

# %% [markdown]
# ### locationfiles2dfdict(dpath)

# %%
@timethis
def locationfiles2dfdict(dpath):
    """
    读取位置数据目录生成设备数据字典
    支持多设备
    """
    device_ids = get_all_device_ids()  # 新增函数获取所有设备ID
    pattern = re.compile(r"location_(\w+).txt")  # 匹配所有设备

    dfdict = {}
    for f in os.listdir(dpath):
        match = pattern.match(f)
        if match:
            device_id = match.group(1)
            if device_id in device_ids:  # 只处理已知设备
                df = parse_location_txt(dpath / f)
                if not df.empty:
                    dfdict[device_id] = df
                    log.info(f"加载设备 {device_id} 数据 {len(df)} 条")
    return dfdict


# %%
pattern = re.compile(r"location_(\w{18})_?\w*.txt")  # 匹配所有设备
dpath = getdirmain() / "data" / "ifttt"
for f in os.listdir(dpath):
    match = pattern.match(f)
    if match:
        device_id = match.group(1)
        print(match.group(0), device_id, len(device_id))
        # if device_id in device_ids:  # 只处理已知设备
        #     df = parse_location_txt(dpath / f)
        #     if not df.empty:
        #         dfdict[device_id] = df
        #         log.info(f"加载设备 {device_id} 数据 {len(df)} 条")


# %% [markdown]
# ### update_note_metadata(note_id, df, resource_id, is_new_device=False)

# %%
def update_note_metadata(note_id, df, resource_id, is_new_device=False):
    """更新笔记中的设备id和统计信息"""
    note = getnote(note_id)
    content = note.body

    # 更新设备哈希值
    device_id = df["device_id"].iloc[0]
    thedf = df[df["device_id"] == device_id]
    print(thedf.tail())
    print(thedf.info())

    if is_new_device:
        # 添加新设备信息
        content += f"\n- 设备：{device_id} 记录数：{len(thedf)}"
    else:
        # 更新现有设备信息
        content = re.sub(
            rf"设备：{device_id} 记录数：\d+",
            f"设备：{device_id} 记录数：{len(thedf)}",
            content,
        )

    # 更新总记录数
    total_match = re.search(r"\*\*总记录数\*\*：(\d+)", content)
    if total_match:
        content = re.sub(r"\*\*总记录数\*\*：\d+", f"**总记录数**：{len(df)}", content)
    else:
        content += f"\n- **总记录数**：{len(thedf)}"

    # 更新起止时间
    start2end = re.compile(
        r"时间范围：\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d+ 至 \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d+(\.\d+)?"
    )
    content = re.sub(
        start2end, f"时间范围：{thedf['time'].min()} 至 {thedf['time'].max()}", content
    )

    # 更新附件链接
    content = re.sub(
        r"\[\S+\]\(:/\w+\)",
        f"[下载数据文件{jpapi.get_resource(resource_id).title}](:/{resource_id})",
        content,
    )

    # 更新笔记
    updatenote_body(note_id, content)


# %% [markdown]
# ### upload_to_joplin(file_path, device_id, period, save_dir)

# %%
def upload_to_joplin(file_path, device_id, period, save_dir):
    """带版本控制的文件上传，支持多设备数据合并与精确哈希校验"""
    # 读取当前设备的新数据
    new_df = pd.read_excel(file_path)
    new_df["device_id"] = device_id  # 添加设备标识列

    note_title = f"位置数据_{period.strftime('%Y%m')}"
    existing_notes = searchnotes(f"title:{note_title}")

    if existing_notes:
        note = existing_notes[0]
        resources = jpapi.get_resources(note.id).items

        if resources:
            device_resource = resources[0]
            save_dir.mkdir(parents=True, exist_ok=True)
            # 下载附件中的历史数据
            old_data = jpapi.get_resource_file(device_resource.id)
            old_df = pd.read_excel(BytesIO(old_data))

            # 合并新旧数据
            merged_df = pd.concat([old_df, new_df])
            merged_df = merged_df.sort_values("time").drop_duplicates(
                subset=["time", "device_id", "latitude", "longitude"],
                keep="last",  # 保留最新记录
            )

            # 计算合并后的哈希值
            merged_len = len(merged_df)

            # 从笔记正文提取该设备的历史哈希值
            note_content = getnote(note.id).body
            old_len_from_note = re.search(
                rf"设备：{device_id} 记录数：(\d+)", note_content
            )

            if old_len_from_note:
                old_len = int(old_len_from_note.group(1))
                log.info(f"旧记录数（笔记）: {old_len}, 新记录数: {merged_len}")

                # 仅当数据发生变化时才更新
                if old_len == merged_len:
                    log.info(f"设备 {device_id} 数据未变化，跳过更新")
                    return
            else:
                oldstr = re.search(rf"设备：\w+ 记录数：\d+", note_content).group(0)
                print(oldstr)
                newstr = oldstr + f"\n- 设备：{device_id} 记录数：{merged_len}"
                new_content = re.sub(oldstr, newstr, note_content)
                updatenote_body(note.id, new_content)

            # 生成新附件
            new_file_name = f"location_{device_id}_{period.strftime('%y%m')}.xlsx"
            new_file = save_dir / new_file_name
            merged_df.to_excel(new_file, index=False)

            # 更新附件
            jpapi.delete_resource(device_resource.id)
            resource_title = re.sub(f"_{device_id}", "", new_file_name)
            new_resource_id = jpapi.add_resource(str(new_file), title=resource_title)

            # 更新笔记元数据
            update_note_metadata(note.id, merged_df, new_resource_id)
        else:
            # 添加新设备数据
            resource_title = re.sub(f"_{device_id}", "", file_path.name)
            new_resource_id = jpapi.add_resource(str(file_path), title=resource_title)
            jpapi.add_resource_to_note(new_resource_id, note.id)
            update_note_metadata(note.id, new_df, new_resource_id, is_new_device=True)
    else:
        # 创建新笔记
        note_body = f"""
## 位置数据元信息
- 时间范围：{newdf["time"].min()} 至 {newdf["time"].max()}
- 包含设备: {device_id}
- 设备：{device_id} 记录数：{len(new_df)}
- **总记录数**：{len(new_df)}
        """
        parent_id = searchnotebook("位置信息数据仓")
        new_file_name = f"location_{device_id}_{period.strftime('%y%m')}.xlsx"
        new_file = save_dir / new_file_name
        new_df.to_excel(new_file, index=False)
        resource_title = re.sub(f"_{device_id}", "", new_file_name)
        resource_id = jpapi.add_resource(str(new_file), title=resource_title)
        newnote_id = createnote(title=note_title, body=note_body, parent_id=parent_id)
        jpapi.add_resource_to_note(resource_id, newnote_id)

    log.info(f"成功更新 {note_title} 笔记")


# %% [markdown]
# ### locationsplit2xlsx(dfdict, save_path)

# %%
def locationsplit2xlsx(dfdict, save_path):
    """按月份拆分位置数据到Excel文件"""
    # 合并所有设备数据
    all_data = pd.DataFrame()
    for device_id, df in dfdict.items():
        df["device_id"] = device_id  # 添加设备标识列
        all_data = pd.concat([all_data, df])

    all_data["month"] = all_data["time"].dt.to_period("M")

    for period, group in all_data.groupby("month"):
        filepath = save_path / f"location_{period.strftime('%y%m')}.xlsx"

        if filepath.exists():
            # 读取现有数据并合并
            existing_df = pd.read_excel(filepath)
            merged_df = pd.concat([existing_df, group])

            # 关键改进：按时间戳去重（解决冲突）
            merged_df = merged_df.sort_values("time", ascending=False)
            merged_df = merged_df.drop_duplicates(
                subset=["time", "device_id"],
                keep="first",  # 保留最新记录[1](@ref)
            )
        else:
            merged_df = group

        merged_df.to_excel(filepath, index=False)
        upload_to_joplin(filepath, ",".join(merged_df["device_id"].unique()), period)

# %% [markdown]
# ### update_location_note(device_id, notebook_guid)

# %%
def update_location_note(device_id, notebook_guid):
    """更新位置数据到Joplin笔记"""
    section_name = f"定位设备_{device_id}"
    note_title = f"位置数据汇总_{device_id}"

    # 获取或创建父笔记本（返回笔记本ID字符串）
    try:
        # 调用searchnotebook返回的是笔记本ID字符串
        parent_id = searchnotebook("位置信息数据仓")
    except Exception as e:
        # 如果不存在则创建新笔记本，返回新笔记本ID字符串
        parent_id = searchnotebook(
            "位置信息数据仓"
        )  # 自动创建新笔记本的逻辑已在searchnotebook中实现

    # 构建笔记内容
    content = ["## 设备位置数据统计\n"]
    file_list = []
    total_records = 0

    # 遍历数据文件
    data_path = getdirmain() / "data" / "ifttt"
    for f in os.listdir(data_path):
        if f.startswith(f"location_{device_id}"):
            df = parse_location_txt(data_path / f)
            if not df.empty:
                time_range = f"{df['time'].min()} 至 {df['time'].max()}"
                record_count = len(df)
                content.append(
                    f"- 文件: {f}\n  记录数: {record_count}\n  时间范围: {time_range}"
                )
                total_records += record_count
                file_list.append(f)

    # 添加统计信息
    content.insert(1, f"### 总记录数: {total_records}\n")

    # 创建/更新笔记
    note = searchnotes(f"title:{note_title}", parent_id=parent_id)
    if note:
        note = note[0]
        updatenote_body(note.id, "\n".join(content))
    else:
        createnote(title=note_title, body="\n".join(content), parent_id=parent_id)

# %% [markdown]
# ### sync_location_data()

# %%
@timethis
def sync_location_data():
    """主同步流程"""
    data_dir = getdirmain() / "data" / "ifttt"
    save_dir = getdirmain() / "data" / "processed_locations"
    save_dir.mkdir(exist_ok=True)
    dfdict = locationfiles2dfdict(data_dir)

    for device_id, df in dfdict.items():
        if not df.empty:
            # 添加月份分组
            df["month"] = df["time"].dt.to_period("M")

            for period, group in df.groupby("month"):
                # 保存临时文件
                temp_file = data_dir / f"location_{device_id}_{period}.xlsx"
                group.to_excel(temp_file, index=False)

                # 上传到Joplin
                upload_to_joplin(temp_file, device_id, period, save_dir)

                # 删除临时文件
                temp_file.unlink()

# %% [markdown]
# ## 主执行流程

# %%
if __name__ == "__main__":
    if not_IPython():
        log.info(f"开始同步位置数据...")

    sync_location_data()

    if not_IPython():
        log.info(f"位置数据同步完成")
