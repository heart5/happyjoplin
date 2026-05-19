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
# # 空闲内存动态

# %% [markdown]
# ## 引入重要库

# %%
import io
import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

# %%
import pathmagic

with pathmagic.context():
    # from func.first import getdirmain
    from func.getid import getdevicename, gethostuser
    from func.configpr import getcfpoptionvalue, setcfpoptionvalue
    from func.jpfuncs import (
        add_resource_from_bytes,
        createnote,
        deleteresourcesfromnote,
        getinivaluefromcloud,
        noteid_used,
        searchnotebook,
        searchnotes,
        updatenote_body,
    )
    from func.logme import log

    # from func.termuxtools import termux_telephony_deviceinfo
    from func.sysfunc import execcmd, not_IPython
    from func.wrapfuncs import timethis

# %% [markdown]
# ## 功能函数

# %% [markdown]
# ### getmemdf()


# %%
@timethis
def getmemdf() -> (int, pd.DataFrame):
    """从指定路径获取内存情况并处理数据，生成DataFrame返回."""
    # 根据不同的系统复制家目录
    sysinfo = execcmd("uname -a")
    if re.search("Android", sysinfo) is None:
        homepath = execcmd("echo ~")
        log.info(f"It's Linux[{gethostuser()}]. Home is {homepath}")
    else:
        homepath = execcmd("echo $HOME")
        log.info(f"It's Android[{gethostuser()}]. Home is {homepath}")
    dpath = Path(homepath) / "sbase/zshscripts/data/freeinfo.txt"
    print(dpath)
    if not os.path.exists(dpath):
        log.critical(f"内存数据文件（{dpath}）不存在，退出运行！！！")
        exit(1)
    with open(dpath, "r") as f:
        content = f.read()
    # 分行获取总内存(文件首行)和时间点空闲内存记录列表
    lineslst = [x for x in content.split("\n") if len(x) != 0]
    totalmem = int(lineslst[0].split("=")[-1])
    memlst = [x.split("\t") for x in lineslst[1:]]
    # 时间精确到分，方便后面去重
    memlstdone = []
    for x_ix, x in enumerate(memlst):
        if len(x) < 4 or len(x[0]) == 0:
            log.critical(f"第{x_ix + 2}存在错误：{x}")
            continue

        try:
            # 验证时间戳是否在合理范围内（1970-2100年）
            timestamp = int(x[0])
            if timestamp < 0 or timestamp > 4102444800:  # 2100-01-01的时间戳
                log.critical(f"第{x_ix + 2}存在错误：{x}")
                continue

            time_str = datetime.fromtimestamp(timestamp).strftime("%F %H:%M")
            memlstdone.append(
                [
                    time_str,
                    int(x[1]),
                    int(x[2]),
                    int(x[3]),
                ]
            )
        except (ValueError, IndexError):
            log.critical(f"第{x_ix + 2}存在错误：{x}")
            # 跳过无效数据
            continue

    if not memlstdone:
        return totalmem, pd.DataFrame()

    memdf = pd.DataFrame(memlstdone, columns=["time", "freepercent", "swaptotal", "swapfree"])
    memdf["time"] = pd.to_datetime(memdf["time"])
    print(memdf.dtypes)
    num_all = memdf.shape[0]
    memdf.drop_duplicates(["time"], inplace=True)
    log.info(f"{gethostuser()}内存占用记录共有{num_all}条，去重后有效记录有{memdf.shape[0]}条")
    log.info(f"{gethostuser()}内存占用记录最新日期为{memdf['time'].max()}，最早日期为{memdf['time'].min()}")
    # 重置索引，使其为连续的整数，方便后面精准切片
    memdfdone = memdf.reset_index()

    return totalmem, memdfdone


# %% [markdown]
# ### gap2img()


# %%
@timethis
def gap2img(gap: int = 30) -> str:
    """把内存记录按照间隔（30分钟）拆离，并生成最近的动图和所有数据集的总图."""
    totalmem, memdfdone = getmemdf()
    tmemg = totalmem / (1024 * 1024)

    time_elasp = memdfdone["time"] - memdfdone["time"].shift(1)
    tm_gap = time_elasp[time_elasp > pd.Timedelta(f"{gap}m")]
    print(gap, tm_gap, pd.Timedelta(f"{gap}m"))

    gaplst = list()
    for ix in tm_gap.index:
        gaplst.append(f"{ix}\t{memdfdone['time'].loc[ix]}\t{tm_gap[ix]}")
    log.info(f"{gethostuser()}的内存({tmemg})记录数据不连续(共有{tm_gap.shape[0]}个断点)：{'|'.join(gaplst)}")

    # 处理无断点的情况
    if len(gaplst) == 0:
        last_gap = memdfdone.set_index(["time"])["freepercent"]
    else:
        last_gap = memdfdone.loc[list(tm_gap.index)[-1] :].set_index(["time"])["freepercent"]

    plt.figure(figsize=(16, 40), dpi=300)

    ax1 = plt.subplot2grid((2, 1), (0, 0), colspan=1, rowspan=1)
    plt.ylim(0, 100)
    ax1.plot(last_gap)
    plt.title(f"最新周期内存占用动态图[{gethostuser()}]")

    ax2 = plt.subplot2grid((2, 1), (1, 0), colspan=1, rowspan=1)
    plt.ylim(0, 100)
    # 处理无断点的情况
    if len(gaplst) == 0:
        ax2.plot(last_gap)
    else:
        gaplst = list(tm_gap.index)
        gaplst.insert(0, 0)
        gaplst.append(memdfdone.index.max() + 1)
        print(gaplst)
        for i in range(len(gaplst) - 1):
            tmpdf = memdfdone.loc[gaplst[i] : gaplst[i + 1] - 1].set_index(["time"])["freepercent"]
            log.info(
                f"切片数据集最新日期为{tmpdf.index.max()}，最早日期为{tmpdf.index.min()}，数据项目数量为{tmpdf.shape[0]}"
            )
            ax2.plot(tmpdf)
    plt.title(f"全部周期内存占用动态图[{gethostuser()}]")

    # 保存图片至字节池
    buffer = io.BytesIO()
    plt.savefig(buffer, format="png", dpi=120)
    plt.close()

    # 生成笔记资源并返回id
    return add_resource_from_bytes(buffer.getvalue(), "内存占用.png")


# %% [markdown]
# ### create_disk_config_file(script_dir=None)

# %%
def create_disk_config_file(script_dir=None):
    """创建配置文件"""
    if script_dir is None:
        script_dir = os.path.join(os.path.expanduser("~"), "sbase", "zshscripts")

    config_file = os.path.join(script_dir, "data", "monitor_config.json")

    config = {
        "monitors": [
            {
                "mountpoint": "/",
                "name": "root",
                "description": "根分区",
                "enabled": True,
            },
            {
                "mountpoint": "/data",
                "name": "data",
                "description": "数据分区",
                "enabled": True,
            },
        ],
        "retention_days": 30,
        "log_rotation_lines": 500,
        "alert_threshold": 90,
        "warning_threshold": 80,
    }

    os.makedirs(os.path.dirname(config_file), exist_ok=True)

    with open(config_file, "w") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

    print(f"配置文件已创建: {config_file}")
    return config_file


# %% [markdown]
# ### load_disk_monitor_config(script_dir=None)

# %%
def load_disk_monitor_config(script_dir=None):
    """加载监控配置"""
    if script_dir is None:
        script_dir = os.path.join(os.path.expanduser("~"), "sbase", "zshscripts")

    config_file = os.path.join(script_dir, "data", "monitor_config.json")

    # 默认配置
    default_config = {
        "monitors": [
            {"mountpoint": "/", "name": "root", "description": "根分区"},
            {"mountpoint": "/data", "name": "data", "description": "数据分区"},
        ],
        "retention_days": 30,
        "log_rotation_lines": 500,
    }

    if os.path.exists(config_file):
        try:
            with open(config_file, "r") as f:
                config = json.load(f)
                # 合并默认配置
                for key, value in default_config.items():
                    if key not in config:
                        config[key] = value
                return config
        except json.JSONDecodeError:
            print(f"配置文件格式错误，使用默认配置")

    return default_config


# %% [markdown]
# ### parse_disk_logs_with_config(script_dir=None)

# %%
def parse_disk_logs_with_config(script_dir=None):
    """根据配置解析磁盘日志"""
    config = load_disk_monitor_config(script_dir)

    if script_dir is None:
        sysinfo = execcmd("uname -a")
        if re.search("Android", sysinfo) is None:
            homepath = execcmd("echo ~")
            log.info(f"It's Linux[{gethostuser()}]. Home is {homepath}")
        else:
            homepath = execcmd("echo $HOME")
            log.info(f"It's Android[{gethostuser()}]. Home is {homepath}")
        script_dir = Path(homepath) / "sbase" / "zshscripts"

    data_dir = script_dir / "data"
    disk_data = []

    # 遍历配置中的监控项
    for monitor in config.get("monitors", []):
        mountpoint = monitor.get("mountpoint")
        name = monitor.get("name")
        description = monitor.get("description", mountpoint)

        if not mountpoint or not name:
            continue

        log_file = data_dir / f"disk_{name}.log"

        if not os.path.exists(log_file):
            continue

        # 解析日志文件
        print(log_file)
        with open(log_file, "r") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue

                parts = line.split()
                if len(parts) >= 7:
                    try:
                        # 格式: 时间戳 挂载点 使用率% 已用 总量 可用 文件系统
                        timestamp = " ".join(parts[0:2])
                        log_mountpoint = parts[2]
                        usage_percent = float(parts[3])
                        used = parts[4]
                        total = parts[5]
                        available = parts[6]
                        filesystem = parts[7] if len(parts) > 7 else "unknown"

                        entry = {
                            "timestamp": timestamp,
                            "mountpoint": log_mountpoint,
                            "config_name": name,
                            "description": description,
                            "usage_percent": usage_percent,
                            "used": used,
                            "total": total,
                            "available": available,
                            "filesystem": filesystem,
                            "log_file": log_file,
                            "line_number": line_num,
                        }
                        disk_data.append(entry)
                    except (ValueError, IndexError) as e:
                        continue

    return disk_data, config


# %% [markdown]
# ### calculate_usage_trend(monitor_data, hours=24)

# %%
def calculate_usage_trend(monitor_data, hours=24):
    """计算磁盘使用率趋势

    参数:
        monitor_data: 单个监控项的DataFrame数据
        hours: 分析的时间窗口（小时）

    返回:
        趋势描述字符串
    """
    if len(monitor_data) < 2:
        return "📊 等待更多数据"

    # 按时间排序
    monitor_data = monitor_data.sort_values("timestamp")

    # 筛选指定时间窗口内的数据
    time_threshold = datetime.now() - timedelta(hours=hours)
    recent_data = monitor_data[monitor_data["timestamp"] >= time_threshold]

    if len(recent_data) < 2:
        return f"⏳ 过去{hours}小时内数据不足"

    # 计算变化 - 修正索引方式
    oldest = recent_data.iloc[0]["usage_percent"]  # 使用整数位置索引获取行，再通过列名访问
    latest = recent_data.iloc[-1]["usage_percent"]
    change = latest - oldest

    # 计算变化率（百分比）
    if oldest > 0:
        change_rate = (change / oldest) * 100
    else:
        change_rate = 0

    # 生成趋势描述
    if change > 2.0 or change_rate > 5:
        icon = "🚀"
        level = "显著增长"
    elif change > 0.5 or change_rate > 1:
        icon = "📈"
        level = "温和增长"
    elif change < -2.0 or change_rate < -5:
        icon = "⚠️"
        level = "显著下降"
    elif change < -0.5 or change_rate < -1:
        icon = "📉"
        level = "温和下降"
    else:
        icon = "➡️"
        level = "基本稳定"

    # 添加统计信息
    time_range = recent_data.iloc[-1]["timestamp"] - recent_data.iloc[0]["timestamp"]
    hours_range = time_range.total_seconds() / 3600

    return f"{icon} {level} ({change:+.1f}%, {hours_range:.1f}小时)"


# %% [markdown]
# ### analyze_disk_usage_by_config(script_dir=None)

# %%
def analyze_disk_usage_by_config(script_dir=None):
    """根据配置分析磁盘使用情况，生成Markdown表格报告"""
    data, config = parse_disk_logs_with_config(script_dir)

    if not data:
        return "暂无磁盘监控数据", config

    enabledconfigmonitors = [monitor for monitor in config["monitors"] if monitor.get("enabled")]
    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    report_lines = [
        "# 磁盘空间监控报告\n",
        f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  \n",
        f"**监控配置**: {len(enabledconfigmonitors)} 个监控项  \n",
        "---\n",
    ]

    # 1. 详细监控表格
    detailed_rows = []
    for monitor in enabledconfigmonitors:
        name = monitor["name"]
        monitor_data = df[df["config_name"] == name]

        if monitor_data.empty:
            row = {
                "监控项": monitor["description"],
                "挂载点": monitor["mountpoint"],
                "使用率": "无数据",
                "已用/总计": "无数据",
                "可用空间": "无数据",
                "文件系统": "无数据",
                "24小时趋势": "无数据",
                "状态": "⚪",
            }
        else:
            latest = monitor_data.iloc[-1]

            # 趋势计算
            trend = ""
            if len(monitor_data) > 1:
                trend = calculate_usage_trend(monitor_data, hours=24)

            # 状态判定
            usage = latest["usage_percent"]
            if usage >= 90:
                status = "🔴 紧急"
            elif usage >= 80:
                status = "🟡 警告"
            elif usage >= 70:
                status = "🔵 注意"
            else:
                status = "✅ 正常"

            row = {
                "监控项": monitor["description"],
                "挂载点": monitor["mountpoint"],
                "使用率": f"{usage:.1f}%",
                "已用/总计": f"{latest['used']}/{latest['total']}",
                "可用空间": latest["available"],
                "文件系统": latest["filesystem"],
                "24小时趋势": trend,
                "状态": status,
            }

        detailed_rows.append(row)

    if detailed_rows:
        detailed_df = pd.DataFrame(detailed_rows)
        # 按使用率排序
        detailed_df["排序键"] = detailed_df["使用率"].apply(lambda x: float(x.replace("%", "")) if "%" in str(x) else 0)
        detailed_df = detailed_df.sort_values("排序键", ascending=False).drop("排序键", axis=1)

        report_lines.append("## 📊 详细监控情况\n")
        report_lines.append(detailed_df.to_markdown(index=False, tablefmt="github"))
        report_lines.append("\n---\n")

    # 2. 摘要表格（Top 5使用率最高）
    summary_data = []
    for monitor in enabledconfigmonitors:
        name = monitor["name"]
        monitor_data = df[df["config_name"] == name]

        if not monitor_data.empty:
            latest = monitor_data.iloc[-1]
            summary_data.append(
                {
                    "监控项": monitor["description"],
                    "挂载点": monitor["mountpoint"],
                    "使用率": latest["usage_percent"],
                    "可用空间": latest["available"],
                }
            )

    if summary_data:
        summary_df = pd.DataFrame(summary_data)
        summary_df = summary_df.sort_values("使用率", ascending=False).head(5)  # 仅显示前5个

        report_lines.append("## 🚨 重点关注（使用率TOP）\n")
        report_lines.append(summary_df.to_markdown(index=False, tablefmt="simple"))
        report_lines.append("\n> 注：建议对使用率>80%的磁盘进行清理或扩容。\n")

    return "\n".join(report_lines)

# %% [markdown]
# ### freemem2note()


# %%
@timethis
def freemem2note() -> None:
    """综合输出内存动态图并更新至笔记."""
    login_user = execcmd("whoami")
    namestr = "happyjp_life"
    section = f"health_{getdevicename()}_{login_user}"
    notestat_title = f"内存硬盘动态监测图【{gethostuser()}】"

    if not (gapinmin := getcfpoptionvalue(namestr, section, "gapinmin")):
        gapinmin = 60
        setcfpoptionvalue(namestr, section, "gapinmin", "60")
    res_id = gap2img(gap=gapinmin)
    content_mem = f"![内存动态图【{gethostuser()}】](:/{res_id})"

    # 如果没有磁盘监控配置文件，创建一个
    if not os.path.exists(
        os.path.join(
            os.path.expanduser("~"),
            "sbase",
            "zshscripts",
            "data",
            "monitor_config.json",
        )
    ):
        create_disk_config_file()
    content_disk = f"{analyze_disk_usage_by_config()}"
    print(content_disk)
    content = "\n".join([content_disk, content_mem])
    nbid = searchnotebook("ewmobile")
    if not (freestat_cloud_id := getcfpoptionvalue(namestr, section, "freestat_cloud_id")):
        freenotefindlist = searchnotes(f"{notestat_title}")
        if len(freenotefindlist) == 0:
            freestat_cloud_id = createnote(title=notestat_title, parent_id=nbid, body=content)
            log.info(f"新的内存硬盘动态监测图笔记“{freestat_cloud_id}”新建成功！")
        else:
            freestat_cloud_id = freenotefindlist[-1].id
        setcfpoptionvalue(namestr, section, "freestat_cloud_id", f"{freestat_cloud_id}")

    if not noteid_used(freestat_cloud_id):
        freestat_cloud_id = createnote(title=notestat_title, parent_id=nbid, body=content)
        setcfpoptionvalue(namestr, section, "freestat_cloud_id", f"{freestat_cloud_id}")
    else:
        deleteresourcesfromnote(freestat_cloud_id)
        updatenote_body(noteid=freestat_cloud_id, bodystr=content, parent_id=nbid)
        log.info(f"内存动态图笔记“{freestat_cloud_id}”更新成功！")


# %% [markdown]
# ## 主函数，main()

# %%
if __name__ == "__main__":
    # 显性获取云端配置中的相关参数
    is_log_details = getinivaluefromcloud("happyjoplin", "is_log_details")
    if not_IPython() and is_log_details:
        log.info(f"运行文件\t{__file__}")

    freemem2note()

    if not_IPython() and is_log_details:
        log.info(f"文件\t{__file__}\t运行完毕。")
