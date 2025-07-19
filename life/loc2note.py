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
import os
import re
from collections import defaultdict
from datetime import datetime
from io import BytesIO

import numpy as np
import pandas as pd

# %%
import pathmagic

with pathmagic.context():
    from etc.getid import getdeviceid
    from filedatafunc import getfilemtime as getfltime
    from func.configpr import getcfpoptionvalue, setcfpoptionvalue
    from func.first import getdirmain
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
    device_ids = [did.strip() for did in device_ids_str.split(", ") if did] if device_ids_str else [the_device_id]
    if the_device_id not in device_ids:
        device_ids.append(the_device_id)
    setcfpoptionvalue("hjloc2note", "devices", "ids", ", ".join(device_ids))
    for device_id in device_ids:
        setcfpoptionvalue(
            "hjloc2note",
            f"{device_id}",
            "device_name",
            getinivaluefromcloud("device", str(device_id)),
        )
    return device_ids


# %% [markdown]
# ### clean_location_data(df)

# %%
def clean_location_data(df):
    """清洗位置数据：处理异常值、去重、排序"""
    # 1. 过滤无效时间
    df = df[df["time"].notna()]

    # 2. 处理坐标异常值（超出合理范围）
    valid_lat = df["latitude"].between(-90, 90)
    valid_lon = df["longitude"].between(-180, 180)
    df = df[valid_lat & valid_lon]

    # 3. 处理精度异常（负值或过大值）
    df.loc[df["accuracy"] < 0, "accuracy"] = np.nan
    df.loc[df["accuracy"] > 10000, "accuracy"] = np.nan

    # 4. 排序并去重（保留最新记录）
    df = df.sort_values("time", ascending=False)
    df = df.drop_duplicates(subset=["time", "latitude", "longitude"], keep="first")

    # 5. 重置索引（优化内存）
    return df.reset_index(drop=True)


# %% [markdown]
# ### 全局变量

# %%
VALID_COLS = ["time", "latitude", "longitude", "altitude", "accuracy"]

# %% [markdown]
# ### parse_location_txt(fl)


# %%
def parse_location_txt(fl):
    """解析位置数据文本文件，处理异常值并优化内存使用"""
    # 优化数据类型定义（减少内存占用）
    dtypes = {
        "time": "object",  # 先作为字符串读取，后续转换为datetime
        "latitude": "float32",
        "longitude": "float32",
        "altitude": "float32",
        "accuracy": "float32",
    }

    # 只读取必要的列（避免无效字段占用内存）
    usecols = [0, 1, 2, 3, 4]  # time, lat, lon, alt, accuracy

    try:
        # 分块读取大文件（>100MB）
        chunks = []
        for chunk in pd.read_csv(
            fl,
            sep="\t",
            header=None,
            dtype=dtypes,
            usecols=usecols,
            names=VALID_COLS,
            na_values=["False", "None", "N/A"],  # 标记异常值为NaN
            skip_blank_lines=True,  # 跳过空行
            skipinitialspace=True,  # 跳过字段前的空格
            chunksize=10000,  # 分块处理大文件
        ):
            chunks.append(chunk)

        df = pd.concat(chunks)

        # 时间转换（带错误处理）
        df["time"] = pd.to_datetime(
            df["time"],
            errors="coerce",  # 转换失败设为NaT
            format="%Y-%m-%d %H:%M:%S",
        )

        # 数据清洗
        df = clean_location_data(df)

        return df

    except Exception as e:
        log.error(f"解析位置文件失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

# %% [markdown]
# ### locationfiles2dfdict(dpath)


# %%
@timethis
def locationfiles2dfdict(dpath):
    device_ids = set(get_all_device_ids())  # 使用集合加速查找
    pattern = re.compile(r"location_(\w+?)(?:_\S+?)?\.txt$")
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
            df = df[VALID_COLS]

            if not dfdict[device_id].empty:
                chunks = [dfdict[device_id], df]
                dfdict[device_id] = (
                    pd.concat(chunks, ignore_index=True)
                    .sort_values("time")
                    .drop_duplicates(subset=["time", "latitude", "longitude"], keep="last")
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
    data_file = re.search(r"\[(?:下载数据文件)?(\S+?)\]\((?:\:\/)?(\S+?)\)", note_content)

    device_counts = {
        match.group(1): int(match.group(2))
        for match in re.finditer(
            r"-\s+设备[：:]\s*(?:【.*?】)?\((\w+?)\)\s+记录数[：:](\d+)",
            note_content,
        )
    }

    # 新增：解析笔记更新记录
    update_records = []
    update_matches = re.finditer(
        r"-\s+(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+由设备\s*(?:【.*?】)?\((\w+?)\)\s*更新，新增记录\s+ (\d+)\s+条",
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
    for device_id in [str(dev) for dev in data_dict["record_counts"].keys() if dev != "total"]:
        get_all_device_ids(device_id)
    metadata = data_dict["metadata"]
    content = f"## 位置数据元信息\n时间范围：{metadata['time_range'][0]} 至 {metadata['time_range'][1]}\n\n"
    content += "## 位置设备列表\n" + "\n".join(f"- {dev}" for dev in data_dict["devices"]) + "\n\n"
    content += (
        "## 分设备位置记录数量\n"
        + "\n".join(
            "- 设备：【" + getcfpoptionvalue("hjloc2note", dev, "device_name") + f"】({dev}) 记录数：{count}"
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
    device_id = str(df["device_id"].iloc[0])
    thedf = df[df["device_id"] == device_id]

    if device_id not in location_dict["devices"]:
        location_dict["devices"].append(device_id)

    location_dict["record_counts"][device_id] = len(thedf)

    # 时间格式定义（根据实际数据格式调整）
    TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

    # 将字符串时间转换为datetime对象
    current_min = thedf["time"].min().to_pydatetime()
    current_max = thedf["time"].max().to_pydatetime()
    stored_min = datetime.strptime(location_dict["metadata"]["time_range"][0], TIME_FORMAT)
    stored_max = datetime.strptime(location_dict["metadata"]["time_range"][1], TIME_FORMAT)

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
            cloud_df = cloud_df[VALID_COLS]  # 精简原有笔记附件中的冗余列

            # 合并云端和本地数据
            merged_df = pd.concat([cloud_df, local_df])
            merged_df = merged_df.sort_values("time").drop_duplicates(
                subset=["time", "device_id", "latitude", "longitude"],
                keep="last",  # 保留最新记录
            )
        else:
            merged_df = local_df
            log.info(f"资源文件 {resource_id} 无效，采用本地位置数据")

        # 计算合并后的大小（仅限当前设备id
        the_df = merged_df[merged_df["device_id"] == device_id]
        the_merged_len = len(the_df)

        if device_id in location_dict["record_counts"]:
            cloud_len = int(location_dict["record_counts"][f"{device_id}"])
        else:
            cloud_len = 0
        location_dict["record_counts"][f"{device_id}"] = the_merged_len
        added_records = the_merged_len - cloud_len

        total_cloud_len = int(location_dict["record_counts"]["total"])
        if (cloud_len == the_merged_len) and (total_cloud_len == len(merged_df)):
            # 判断云端配饰处理所有数据的调试开关是否无视比较结果
            if not getinivaluefromcloud("loc2note", "alldata"):
                device_name = getcfpoptionvalue("hjloc2note", device_id, "device_name")
                log.info(
                    f"设备【{device_name}】的笔记端记录数: {cloud_len}, 和本地合并后记录数无变化；云端记录总数: {total_cloud_len}, 也没有变化。跳过！"
                )
                return
        location_dict["record_counts"]["total"] = len(merged_df)
        # 生成新附件，是包含所有设备数据的综合
        local_file_name = f"location_{period.strftime('%y%m')}.xlsx"
        local_file = save_dir / local_file_name
        merged_df.to_excel(local_file, index=False)

        new_resource_id = jpapi.add_resource(str(local_file), title=local_file_name)

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
        location_dict_done = update_note_metadata(merged_df, new_resource_id, location_dict)
        new_content = location_dict2note_content(location_dict_done)
        updatenote_body(note.id, new_content)
        # 操作成功后删除原有resource
        for resource_id in [res.id for res in jpapi.get_resources(note.id).items if res.id != new_resource_id]:
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
                upload_to_joplin(temp_file, str(device_id), period, save_dir)

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
