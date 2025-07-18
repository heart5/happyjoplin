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
    from etc.getid import getdeviceid
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
def get_all_device_ids(device_id=None):
    """从配置文件获取所有设备ID, 默认不带输入参数（需要处理的新id）"""
    if not device_id:
        the_device_id = getdeviceid()
    else:
        the_device_id = device_id
    device_ids_str = getcfpoptionvalue("hjloc2note", "devices", "ids")
    device_ids = (
        [did.strip() for did in device_ids_str.split(", ") if did]
        if device_ids_str
        else [the_device_id]
    )
    if the_device_id not in device_ids:
        device_ids.append(the_device_id)
    setcfpoptionvalue("hjloc2note", "devices", "ids", ", ".join(device_ids))
    for device_id in device_ids:
        setcfpoptionvalue(
            "hjloc2note",
            f"{device_id}",
            "device_name",
            getinivaluefromcloud("device", device_id),
        )
    return device_ids

# %%
# get_all_device_ids("0x14bfbac75658f5a7")

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
    pattern = re.compile(r"location_(\w{18})(?:_\S+?)?\.txt$")
    dfdict = defaultdict(pd.DataFrame)  # 自动处理空DataFrame

    for f in os.listdir(dpath):
        if not pattern.match(f):
            continue

        device_id = pattern.match(f).group(1)
        if device_id not in device_ids:
            device_ids = set(get_all_device_ids(device_id))

        print(f, device_id, device_ids)
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
# ### parse_location_note_content(note_content: str) -> dict

# %%
def parse_location_note_content(note_content: str) -> dict:
    """解析Joplin位置笔记为结构化字典"""
    time_range = re.search(r"时间范围[：:]\s*(.*?)\s*至\s*(.*?)(?:\n|$)", note_content)
    data_file = re.search(
        r"\[(?:下载数据文件)?(\S+?)\]\((?:\:\/)?(\S+?)\)", note_content
    )

    device_counts = {
        match.group(1): int(match.group(2))
        for match in re.finditer(
            r"-\s+设备[：:]\s*(?:【.*?】)?\((\w{18})\)\s+记录数[：:](\d+)",
            note_content,
        )
    }

    # 新增：解析笔记更新记录
    update_records = []
    update_matches = re.finditer(
        r"-\s+(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) 由设备\s*(?:【.*?】)?\((\w{18})\)\s*更新，新增记录 (\d+) 条",
        note_content,
    )
    for match in update_matches:
        update_records.append(
            {
                "time": match.group(1),
                "device_id": match.group(2),
                "new_records": int(match.group(3)),
            }
        )

    return {
        "metadata": {
            "time_range": (time_range.group(1), time_range.group(2)),
            "data_file": data_file.group(1).strip(),
            "resource_id": data_file.group(2),
        },
        "devices": list(device_counts.keys()),
        "record_counts": {
            **device_counts,
            "total": int(re.search(r"总记录数.*?(\d+)", note_content).group(1)),
        },
        # 新增更新记录字段
        "update_records": update_records,
    }


# %% [markdown]
# ### location_dict2note_content(data_dict: dict) -> str

# %%
def location_dict2note_content(data_dict: dict) -> str:
    """将修改后的字典转换回Joplin笔记内容"""
    # 确保获取 设备id 的名称并保存至ini文件
    for device_id in [
        dev for dev in data_dict["record_counts"].keys() if dev != "total"
    ]:
        get_all_device_ids(device_id)
    metadata = data_dict["metadata"]
    content = f"## 位置数据元信息\n时间范围：{metadata['time_range'][0]} 至 {metadata['time_range'][1]}\n\n"
    content += (
        "## 位置设备列表\n"
        + "\n".join(f"- {dev}" for dev in data_dict["devices"])
        + "\n\n"
    )
    content += (
        "## 分设备位置记录数量\n"
        + "\n".join(
            "- 设备：【"
            + getcfpoptionvalue("hjloc2note", dev, "device_name")
            + f"】({dev}) 记录数：{count}"
            for dev, count in data_dict["record_counts"].items()
            if dev != "total"
        )
        + "\n"
    )
    content += f"\n## 位置记录总数量\n- **总记录数**：{data_dict['record_counts']['total']}\n\n"
    content += f"## 数据文件\n[下载数据文件{metadata['data_file']}](:/{metadata['resource_id']})"
    # 新增：笔记更新记录模块
    content += "\n## 笔记更新记录\n"
    if data_dict.get("update_records"):
        for record in data_dict["update_records"]:
            content += (
                f"- {record['time']} 由设备 【"
                + getcfpoptionvalue("hjloc2note", record["device_id"], "device_name")
                + f"】({record['device_id']}) 更新，"
                f"新增记录 {record['new_records']} 条\n"
            )
    else:
        content += "- 暂无更新记录\n"

    return content

# %% [markdown]
# ### update_note_metadata(note_id, df, resource_id, is_new_device=False)


# %%
def update_note_metadata(df, resource_id, location_dict):
    """更新笔记中的设备id和统计信息"""
    device_id = df["device_id"].iloc[0]
    thedf = df[df["device_id"] == device_id]

    if device_id not in location_dict["devices"]:
        location_dict["devices"].append(device_id)

    location_dict["record_counts"][device_id] = len(thedf)
    location_dict["record_counts"]["total"] = sum(
        location_dict["record_counts"][dev]
        for dev in location_dict["record_counts"]
        if dev != "total"
    )
    # 时间格式定义（根据实际数据格式调整）
    TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

    # 将字符串时间转换为datetime对象
    current_min = datetime.strptime(thedf["time"].min(), TIME_FORMAT)
    current_max = datetime.strptime(thedf["time"].max(), TIME_FORMAT)
    stored_min = datetime.strptime(
        location_dict["metadata"]["time_range"][0], TIME_FORMAT
    )
    stored_max = datetime.strptime(
        location_dict["metadata"]["time_range"][1], TIME_FORMAT
    )

    # 正确比较时间
    new_min = min(stored_min, current_min)
    new_max = max(stored_max, current_max)

    # 转回字符串存储（可选）
    location_dict["metadata"]["time_range"] = (
        new_min.strftime(TIME_FORMAT),
        new_max.strftime(TIME_FORMAT),
    )
    location_dict["metadata"]["resource_id"] = resource_id
    location_dict["metadata"]["data_file"] = jpapi.get_resource(resource_id).title

    return location_dict


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
        location_dict = parse_location_note_content(note.body)
        # print(location_dict)
        resource_id = location_dict["metadata"]["resource_id"]
        # print(jpapi.get_resources(note.id).items)

        if resource_id in [res.id for res in jpapi.get_resources(note.id).items]:
            save_dir.mkdir(parents=True, exist_ok=True)
            # 下载附件中的云端笔记附件数据
            cloud_data = jpapi.get_resource_file(resource_id)
            cloud_df = pd.read_excel(BytesIO(cloud_data))

            # 合并云端和本地数据，需要指定特定的devieid
            merged_df = pd.concat(
                [cloud_df[cloud_df["device_id"] == device_id], local_df]
            )
            merged_df = merged_df.sort_values("time").drop_duplicates(
                subset=["time", "device_id", "latitude", "longitude"],
                keep="last",  # 保留最新记录
            )
        else:
            merged_df = local_df
            log.info(f"资源文件 {resource_id} 无效，采用本地位置数据")

        # 计算合并后的大小
        merged_len = len(merged_df)

        if device_id in location_dict["record_counts"]:
            cloud_len = int(location_dict["record_counts"][f"{device_id}"])
            log.info(f"旧记录数（笔记）: {cloud_len}, 新记录数: {merged_len}")

            # 仅当数据发生变化时才更新
            if cloud_len == merged_len:
                log.info(f"设备 {device_id} 数据未变化，跳过更新")
                return
            else:
                added_records = merged_len - cloud_len
                location_dict["record_counts"][f"{device_id}"] = merged_len
        else:
            location_dict["record_counts"][f"{device_id}"] = merged_len
            added_records = merged_len

        # 生成新附件
        local_file_name = f"location_{device_id}_{period.strftime('%y%m')}.xlsx"
        local_file = save_dir / local_file_name
        merged_df.to_excel(local_file, index=False)

        resource_title = re.sub(f"_{device_id}", "", local_file_name)
        new_resource_id = jpapi.add_resource(str(local_file), title=resource_title)

        # 新增：记录更新信息
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        new_record = {
            "time": current_time,
            "device_id": device_id,
            "new_records": added_records,
        }

        # 添加到更新记录列表
        if "update_records" not in location_dict:
            location_dict["update_records"] = []
        location_dict["update_records"].insert(0, new_record)

        # 更新笔记内容字典
        location_dict_done = update_note_metadata(
            merged_df, new_resource_id, location_dict
        )
        new_content = location_dict2note_content(location_dict_done)
        updatenote_body(note.id, new_content)
        # 操作成功后删除原有resource
        for resource_id in [
            res.id
            for res in jpapi.get_resources(note.id).items
            if res.id != new_resource_id
        ]:
            jpapi.delete_resource(resource_id)
            log.critical(f"资源文件 {resource_id} 被成功删除！")
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
## 笔记更新记录
- 首次由设备（{device_id}）数据创建于 {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
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
# ### sync_location_data()


# %%
@timethis
def sync_location_data():
    """主同步流程"""
    data_dir = getdirmain() / "data" / "ifttt"
    save_dir = getdirmain() / "data" / "processed_locations"
    save_dir.mkdir(exist_ok=True)

    allmonth = getinivaluefromcloud("loc2note", "allmonth")
    monthrange = getinivaluefromcloud("loc2note", "monthrange")
    dfdict = locationfiles2dfdict(data_dir)

    for device_id, df in dfdict.items():
        print(device_id, df.shape)
        if not df.empty:
            # 添加月份分组
            df["month"] = df["time"].dt.to_period("M")
            # 获取该设备数据中的最近月份 [8](@ref)
            latest_month = df["month"].max()  # 关键修改：使用数据中的最新时间
            # 确定要处理的月份范围
            if allmonth:
                months_to_process = df["month"].unique()
            else:
                # 以数据最新月份为基准计算范围 [3](@ref)
                months_to_process = [latest_month - i for i in range(monthrange)]
            print(f"根据云端配置，待处理的月份列表为：{months_to_process}")

            # 按月份分组处理
            grouped = df.groupby("month")
            for period, group in grouped:
                if period not in months_to_process:
                    continue  # 跳过不在处理范围内的月份
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
