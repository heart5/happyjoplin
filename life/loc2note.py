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
from collections import defaultdict
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
    # Extract device IDs from string if available
    current_ids = [
        did
        for did in getcfpoptionvalue("hjdevices", "DEVICES", "ids").split(",")
        if len(did) != 0
    ] or []
    new_id_not_in_current_ids = not (new_id in current_ids)

    # Register new device if it's not already present
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
    device_ids = set(get_all_device_ids())  # 使用集合加速查找
    pattern = re.compile(r"location_(\w{18})(?:_\w+)?\.txt$")
    dfdict = defaultdict(pd.DataFrame)  # 自动处理空DataFrame

    for f in os.listdir(dpath):
        if not pattern.match(f):
            continue

        device_id = pattern.match(f).group(1)
        if device_id not in device_ids:
            register_new_device(device_id)
            device_ids.add(device_id)  # 更新本地缓存

        try:
            df = parse_location_txt(dpath / f)
            if df.empty:
                continue

            if not dfdict[device_id].empty:
                chunks = [dfdict[device_id], df]
                dfdict[device_id] = (
                    pd.concat(chunks, ignore_index=True)
                    .sort_values("time")
                    .drop_duplicates(subset=["time", "latitude"], keep="last")
                )
            else:
                dfdict[device_id] = df

            log.info(f"加载 {f} → 设备 {device_id} 数据: {len(df)}条")
        except Exception as e:
            log.error(f"处理文件 {f} 错误: {str(e)}")

    return dict(dfdict)  # 转回普通dict


# %% [markdown]
# ### update_note_metadata(note_id, df, resource_id, is_new_device=False)


# %%
def update_note_metadata(note_id, df, resource_id):
    """更新笔记中的设备id和统计信息"""
    note = getnote(note_id)
    # 0: 位置数据元信息， 2：位置设备列表， 4： 分设备位置记录数量， 6 ：位置记录总量， 8： 数据文件
    struct_body = [
        line.strip()
        for line in re.split(r"##\s+(\S+)\n", note.body)
        if len(line.strip()) != 0
    ]

    device_id = df["device_id"].iloc[0]
    thedf = df[df["device_id"] == device_id]
    # print(thedf.tail())
    # print(thedf.info())

    # 更新设备列表
    device_lst = re.findall(r"-\s+(?:包含设备[：:])?\s*(\w{18})", struct_body[3], re.M)
    if device_id not in device_lst:
        device_lst.append(device_id)
    struct_body[3] = "\n".join([f"- {item}" for item in device_lst])

    # 更新总记录数
    total_match = re.search(r"\*\*总记录数\*\*[：:](\d+)", struct_body[7])
    if total_match:
        struct_body[7] = re.sub(
            r"\*\*总记录数\*\*[：:]\d+", f"**总记录数**：{len(df)}", struct_body[7]
        )
    else:
        struct_body[7] = f"- **总记录数**：{len(df)}"

    # 更新起止时间
    struct_body[1] = f"时间范围：{thedf['time'].min()} 至 {thedf['time'].max()}"

    # 更新附件链接
    struct_body[9] = (
        f"[下载数据文件{jpapi.get_resource(resource_id).title}](:/{resource_id})"
    )

    new_content = "\n".join(
        [f"## {item}" if i % 2 == 0 else item for i, item in enumerate(struct_body)]
    )

    # 更新笔记
    updatenote_body(note_id, new_content)


# %% [markdown]
# ### upload_to_joplin(file_path, device_id, period, save_dir)


# %%
def upload_to_joplin(file_path, device_id, period, save_dir):
    """文件上传至笔记，支持多设备数据合并与数据表大小校验"""
    # 读取当前设备的新数据
    local_df = pd.read_excel(file_path)
    local_df["device_id"] = device_id  # 添加设备标识列

    note_title = f"位置数据_{period.strftime('%Y%m')}"
    existing_notes = searchnotes(f"title:{note_title}")

    if existing_notes:
        note = existing_notes[0]
        resources = jpapi.get_resources(note.id).items

        device_resource = resources[0]
        save_dir.mkdir(parents=True, exist_ok=True)
        # 下载附件中的云端笔记附件数据
        cloud_data = jpapi.get_resource_file(device_resource.id)
        cloud_df = pd.read_excel(BytesIO(cloud_data))

        # 合并云端和本地数据
        merged_df = pd.concat([cloud_df, local_df])
        merged_df = merged_df.sort_values("time").drop_duplicates(
            subset=["time", "device_id", "latitude", "longitude"],
            keep="last",  # 保留最新记录
        )

        # 计算合并后的大小
        merged_len = len(merged_df)

        # 从笔记正文提取该设备的历史大小
        note_content = getnote(note.id).body
        cloud_len_from_note = re.search(
            rf"-\s+设备[：:]{device_id}\s+记录数[：:](\d+)", note_content
        )

        if cloud_len_from_note:
            cloud_len = int(cloud_len_from_note.group(1))
            log.info(f"旧记录数（笔记）: {cloud_len}, 新记录数: {merged_len}")

            # 仅当数据发生变化时才更新
            if cloud_len == merged_len:
                log.info(f"设备 {device_id} 数据未变化，跳过更新")
                return
            else:
                ptn_special = rf"^-\s+设备[:：]{device_id}\s+记录数[:：](\d+)$"
                find_line = re.search(ptn_special, note_content, re.M).group()
                new_content = re.sub(
                    find_line,
                    re.sub(r"记录数[:：]\s*\d+", f"记录数：{merged_len}", find_line),
                    note_content,
                )
                updatenote_body(note.id, new_content)
        else:
            newstr = re.findall(
                r"^-\s+设备[：:]\w{18}\s+记录数[：:]\d+$", note_content, re.M
            )
            newstrdone = (
                "\n".join(newstr) + f"\n- 设备：{device_id} 记录数：{merged_len}"
            )
            new_content = re.sub("\n".join(newstr), newstrdone, note_content)
            updatenote_body(note.id, new_content)

        # 生成新附件
        local_file_name = f"location_{device_id}_{period.strftime('%y%m')}.xlsx"
        local_file = save_dir / local_file_name
        merged_df.to_excel(local_file, index=False)

        # 更新附件
        jpapi.delete_resource(device_resource.id)
        resource_title = re.sub(f"_{device_id}", "", local_file_name)
        new_resource_id = jpapi.add_resource(str(local_file), title=resource_title)

        # 更新笔记元数据
        update_note_metadata(note.id, merged_df, new_resource_id)
    else:
        # 创建新笔记
        note_body = f"""
## 位置数据元信息
- 时间范围：{local_df["time"].min()} 至 {local_df["time"].max()}
## 位置设备列表
- 包含设备: {device_id}
## 分设备位置记录数量
- 设备：{device_id} 记录数：{len(local_df)}
## 位置记录总数量
- **总记录数**：{len(local_df)}
## 数据文件
        """
        parent_id = searchnotebook("位置信息数据仓")
        local_file_name = f"location_{device_id}_{period.strftime('%y%m')}.xlsx"
        local_file = save_dir / local_file_name
        local_df.to_excel(local_file, index=False)
        resource_title = re.sub(f"_{device_id}", "", local_file_name)
        resource_id = jpapi.add_resource(str(local_file), title=resource_title)
        newnote_id = createnote(title=note_title, body=note_body, parent_id=parent_id)
        jpapi.add_resource_to_note(resource_id, newnote_id)

    log.info(f"成功更新 {note_title} 笔记")


# %% [markdown]
# ### fix_content_location_notes()

# %%
def fix_content_location_notes():
    find_notes = searchnotes("title:位置数据_")
    for note in find_notes:
        content = note.body

        section_devices_list = "## 位置设备列表\n"
        section_multi_devices = "## 分设备位置记录数量\n"
        section_total_records = "## 位置记录总数量\n"
        section_data_download = "## 数据文件\n"

        ptn_devices_list = rf"({section_devices_list})?(-\s+包含设备[：:]\s*\w{{18}})"
        devices_list_lines = re.search(ptn_devices_list, content).group()
        devices_list_content = re.sub(
            ptn_devices_list, section_devices_list + devices_list_lines, content
        )
        # print(devices_list_content)

        ptn_multi_devices = rf"({section_multi_devices})?(-\s+设备[：:]\s*\w{{18}}\s+记录数[：:]\s*\d+\n)+(?:##\s\w+)?"
        multi_devices_lines = re.search(ptn_multi_devices, devices_list_content).group()
        multi_devices_content = re.sub(
            ptn_multi_devices,
            section_multi_devices + multi_devices_lines + section_total_records,
            devices_list_content,
        )
        # print(multi_devices_content)

        ptn_data_download = rf"({section_data_download})?(\[\S+\])\(:/\S+\)"
        data_download_lines = re.search(
            ptn_data_download, multi_devices_content
        ).group()
        # print(f"下载链接字符串为：\t{data_download_lines}")
        data_download_content = re.sub(
            ptn_data_download,
            section_data_download + data_download_lines,
            multi_devices_content,
        )
        # print(data_download_content)
        updatenote_body(note.id, data_download_content)
        log.info(f"笔记《{note.title}》内容已规整为结构化文档。")

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
        print(device_id, df.shape)
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
