#!/usr/bin/python
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

# %%
"""
IP信息更新工具 (增强版)
功能：获取设备IP和WiFi信息，记录变化并更新至Jupyter笔记
优化点：修复数字类型处理、增强错误处理、添加Markdown函数声明
"""

# %% [markdown]
# ## 导入依赖库

# %%
import datetime
import logging
import os
import platform
import re
import sys
from typing import Any, List, Optional, Tuple

try:
    import pathmagic

    with pathmagic.context():
        from etc.getid import getdeviceid, gethostuser
        from func.configpr import getcfpoptionvalue, setcfpoptionvalue
        from func.datatools import readfromtxt, write2txt
        from func.first import dirmainpath
        from func.jpfuncs import (
            createnote,
            getinivaluefromcloud,
            noteid_used,
            searchnotebook,
            searchnotes,
            updatenote_body,
            updatenote_imgdata,
            updatenote_title,
        )
        from func.logme import log
        from func.nettools import get_ip4alleth
        from func.sysfunc import execcmd, is_tool_valid, not_IPython
        from func.termuxtools import termux_wifi_connectioninfo
except ImportError as e:
    log.error(f"导入模块失败: {e}")
    # 尝试添加路径（适用于JupyterLab环境）
    sys.path.append(
        os.path.expanduser("~/codebase/happyjoplin")
    )  # 请修改为你的实际项目路径
    log.info("已尝试添加路径到sys.path")

# %% [markdown]
# ## 配置常量

# %%
CONFIG_NAME = "happyjpip"

# %% [markdown]
# ## 核心功能函数

# %% [markdown]
# ### getipwifi() - 获取IP和WiFi信息
#
# 根据不同操作系统，调用命令行工具获取ip（本地、外部）和wifi信息
#
# **返回格式:** (ip_local, ip_public, wifi, wifiid)
#
# **异常处理:** 自动降级处理，确保单点故障不影响整体功能


# %%
def getipwifi() -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    根据不同操作系统，调用命令行工具获取ip（本地、外部）和wifi信息
    返回格式: (ip_local, ip_public, wifi, wifiid)
    """
    ip_local, ip_public, wifi, wifiid = "", "", "", ""
    sys_platform_str = execcmd("uname -a")

    if re.findall("Linux", sys_platform_str):
        # 获取本地IP - 使用更通用的ip命令，而非nmcli
        try:
            # 优先使用 `ip` 命令，它比 ifconfig 更现代且普遍
            ip_output = execcmd("ip addr show")
            # 一个简单的例子，解析 IP 地址，可能需要更复杂的逻辑处理多个接口
            ip_match = (
                re.search(r"inet (192\.168\.\d+\.\d+)/", ip_output)
                or re.search(r"inet (10\.\d+\.\d+\.\d+)/", ip_output)
                or re.search(r"inet (172\.1[6-9]\.\d+\.\d+)/", ip_output)
            )
            if ip_match:
                ip_local = ip_match.group(1)
            else:
                #  fallback to ifconfig
                ifinet_str = execcmd(
                    "ifconfig | grep 'inet ' | grep -v 127.0.0.1 | awk '{print $2}'"
                )
                if ifinet_str:
                    ip_local = ifinet_str.split()[-1]  # 可能需要调整获取逻辑
        except Exception as e:
            log.warning(f"获取本地IP失败: {e}")

        # 获取公网IP - 使用通用的curl命令
        try:
            ip_public = execcmd("curl -s ifconfig.me").strip()
            if not ip_public:
                ip_public = execcmd("curl -s ipinfo.io/ip").strip()
        except Exception as e:
            log.warning(f"获取公网IP失败: {e}")

        # 获取WiFi信息 - 首先检查nmcli是否存在，不存在则尝试其他方法
        if re.findall("Android", sys_platform_str):
            # Termux 环境
            try:
                wifi_info = termux_wifi_connectioninfo()
                wifi = wifi_info.get("ssid", "")
                wifiid = wifi_info.get("bssid", "") if wifi != "<unknown ssid>" else ""
            except Exception as e:
                log.warning(f"Android WiFi信息获取失败: {e}")
        else:
            # 非Android Linux环境
            # 首先检查nmcli是否存在
            if is_tool_valid("nmcli"):
                try:
                    nmcli_str = execcmd("nmcli dev wifi")
                    if nmcli_str:
                        connected_lines = [
                            line for line in nmcli_str.split("\n") if "*" in line
                        ]
                        if connected_lines:
                            wifi = connected_lines[0].split()
                            nmclilst_str = execcmd("nmcli device wifi list")
                            if nmclilst_str:
                                for line in nmclilst_str.split("\n"):
                                    if wifi in line:
                                        wifiid = line.split()[-1]
                                        break
                except Exception as e:
                    log.warning(f"nmcli WiFi信息获取失败: {e}")
            else:
                log.info("nmcli 不可用，尝试其他方法获取WiFi信息")
                # 备选方案1: 查看无线设备状态（需要sudo权限或特定配置）
                try:
                    # 检查是否有无线设备
                    wireless_dev = execcmd(
                        "iw dev | awk '$1==\"Interface\"{print $2}'"
                    ).strip()
                    if wireless_dev:
                        # 尝试获取连接信息，这可能需要root权限
                        iw_output = execcmd(f"iw {wireless_dev} link")
                        if iw_output and "Connected" in iw_output:
                            ssid_match = re.search(r"SSID: (.+)", iw_output)
                            if ssid_match:
                                wifi = ssid_match.group(1)
                            # BSSID 可能也在输出中
                            bssid_match = re.search(r"Connected to (.+) \(", iw_output)
                            if bssid_match:
                                wifiid = bssid_match.group(1)
                except Exception as e:
                    log.warning(f"iw command failed: {e}")

                # 备选方案2: 解析系统文件 (如 /proc/net/wireless)
                # 注意：此文件通常不直接提供SSID，主要提供信号质量
                try:
                    with open("/proc/net/wireless", "r") as f:
                        wireless_lines = f.readlines()
                    if len(wireless_lines) > 2:  # 有标题行和至少一个设备行
                        # 这里可能只能获取设备名和信号强度，难以直接获取SSID
                        dev_line = wireless_lines[2].split()
                        dev_name = dev_line[0].strip(":")
                        log.info(
                            f"无线设备 {dev_name} 活跃，但无法通过文件直接获取SSID"
                        )
                except (IOError, IndexError) as e:
                    log.info("无法读取 /proc/net/wireless 或无线设备未激活")

    # 对于Windows系统，可以使用netsh
    elif platform.system() == "Windows":
        try:
            ipconfig_str = execcmd("ipconfig")
            ip_matches = [
                line.split(":")[-1].strip()
                for line in re.findall("IPv4.*", ipconfig_str)
                if re.search(r"\.\d+$", line)
            ]
            ip_local = ip_matches[-1] if ip_matches else ""

            nslookup_str = execcmd("nslookup myip.opendns.com resolver1.opendns.com")
            ip_public_matches = [
                line.split(":")[-1].strip()
                for line in re.findall("Address.*", nslookup_str)
                if re.search(r"\.\d+$", line)
            ]
            ip_public = ip_public_matches[-1] if ip_public_matches else ""

            wifi_str = execcmd("netsh wlan show interfaces")
            resultlst = [
                line.split(":", 1) for line in re.findall(".*SSID.*", wifi_str)
            ]
            splitlist = [x.strip() for line in resultlst for x in line]
            wifidict = dict(zip(splitlist[::2], splitlist[1::2]))
            wifi = wifidict.get("SSID", "")
            wifiid = wifidict.get("BSSID", "")
        except Exception as e:
            log.error(f"Windows系统信息获取失败: {e}")

    else:
        log.warning(f"未知操作系统: {platform.system()}")
        ip_local, ip_public, wifi, wifiid = "", "", "", ""

    # 处理空值并确保字符串类型
    result = [
        str(ip_local) if ip_local else None,
        str(ip_public) if ip_public else None,
        str(wifi) if wifi else None,
        str(wifiid) if wifiid else None,
    ]
    log.info(f"获取到的信息: {result}")
    return tuple(result)


# %% [markdown]
# ### safe_getcfpoptionvalue() - 安全获取配置值
#
# 解决getoptionvalue自动转换数字类型的问题，确保所有值以字符串形式返回
#
# **参数:**
# - section: 配置节名称
# - option: 配置项名称
# - default: 默认值（可选）
#
# **返回:** 字符串类型的配置值


# %%
def safe_getcfpoptionvalue(
    section: str, option: str, default: str = None
) -> Optional[str]:
    """
    安全获取配置值，确保所有值以字符串形式返回，避免数字类型自动转换

    Args:
        section: 配置节名称
        option: 配置项名称
        default: 默认值（可选）

    Returns:
        Optional[str]: 字符串类型的配置值，不存在时返回default
    """
    try:
        value = getcfpoptionvalue(CONFIG_NAME, section, option)
        if value is None:
            return default

        # 确保返回字符串类型，避免数字自动转换
        return str(value)
    except Exception as e:
        log.warning(f"获取配置值失败 [{section}]/{option}: {e}")
        return default


# %% [markdown]
# ### evalnone() - 安全处理None值
#
# 转换从终端接收数据的数据类型，安全处理字符串'None'
#
# **参数:** input_val - 输入值
#
# **返回:** 处理后的值


# %%
def evalnone(input_val: Any) -> Any:
    """
    安全处理字符串'None'，同时处理数字类型的字符串转换问题

    Args:
        input_val: 输入值，可能是各种类型

    Returns:
        处理后的值，保持原始字符串类型
    """
    if input_val is None:
        return None

    # 如果是字符串"None"，转换为Python的None
    if isinstance(input_val, str) and input_val == "None":
        return None

    # 确保返回字符串类型，避免数字转换问题
    return str(input_val)


# %% [markdown]
# ### showiprecords() - 显示IP记录
#
# 综合输出ip记录，处理IP信息变化并更新笔记
#
# **流程:**
# 1. 获取当前IP信息
# 2. 查找或创建笔记
# 3. 比较信息变化
# 4. 保存变化记录
# 5. 更新云端笔记


# %%
def showiprecords() -> bool:
    """
    主函数：获取IP记录并更新笔记，处理数字类型转换问题

    Returns:
        bool: 执行是否成功
    """
    try:
        device_id = getdeviceid()
        section = f"{device_id}"
        noteip_title = f"ip动态_【{gethostuser()}】"

        # 获取当前IP信息
        ip_local, ip_public, wifi, wifiid = getipwifi()
        log.info(f"当前获取的信息: {ip_local}, {ip_public}, {wifi}, {wifiid}")

        if not ip_public:
            log.error("无效的公网IP，可能未联网")
            return False

        # 查找或创建笔记
        nbid = searchnotebook("ewmobile")
        ip_cloud_id = safe_getcfpoptionvalue(section, "ip_cloud_id")

        if not ip_cloud_id:
            ipnotefindlist = searchnotes(f"title:{noteip_title}")
            if ipnotefindlist:
                ip_cloud_id = ipnotefindlist[-1].id
            else:
                ip_cloud_id = createnote(title=noteip_title, parent_id=nbid)
                log.info(f"新建IP动态笔记: {ip_cloud_id}")
            setcfpoptionvalue(CONFIG_NAME, section, "ip_cloud_id", str(ip_cloud_id))

        # 使用安全函数获取记录信息
        nowstr = datetime.datetime.now().strftime("%F %T")
        ip_local_r = evalnone(safe_getcfpoptionvalue(section, "ip_local_r"))
        ip_public_r = evalnone(safe_getcfpoptionvalue(section, "ip_public_r"))
        wifi_r = evalnone(safe_getcfpoptionvalue(section, "wifi_r"))
        wifiid_r = evalnone(safe_getcfpoptionvalue(section, "wifiid_r"))
        start_r = safe_getcfpoptionvalue(section, "start_r")

        log.info(f"上次记录的信息: {ip_local_r}, {ip_public_r}, {wifi_r}, {wifiid_r}")

        # 检查信息是否有变化（全部作为字符串比较）
        has_changed = (
            (str(ip_local) != str(ip_local_r))
            or (str(ip_public) != str(ip_public_r))
            or (str(wifi) != str(wifi_r))
            or (str(wifiid) != str(wifiid_r))
        )

        if has_changed or not start_r:
            log.info("检测到信息变化或首次运行，更新记录")

            # 保存变化记录到文件
            txtfilename = os.path.join(
                dirmainpath, "data", "ifttt", f"ip_{section}.txt"
            )
            os.makedirs(os.path.dirname(txtfilename), exist_ok=True)

            # 读取现有记录
            itemread = readfromtxt(txtfilename) if os.path.exists(txtfilename) else []
            itemclean = [x for x in itemread if "unknown" not in x]
            itempolluted = [x for x in itemread if "unknown" in x]

            if itempolluted:
                log.info(f"发现不合法记录: {itempolluted}")

            # 添加新记录
            if start_r and ip_local_r and ip_public_r:  # 有之前记录
                new_record = f"{ip_local_r}\t{ip_public_r}\t{wifi_r}\t{wifiid_r}\t{start_r}\t{nowstr}"
                itemnewr = [new_record] + itemclean
                write2txt(txtfilename, itemnewr)

            # 更新当前记录（确保存储为字符串）
            setcfpoptionvalue(CONFIG_NAME, section, "ip_local_r", str(ip_local))
            setcfpoptionvalue(CONFIG_NAME, section, "ip_public_r", str(ip_public))
            setcfpoptionvalue(CONFIG_NAME, section, "wifi_r", str(wifi))
            setcfpoptionvalue(CONFIG_NAME, section, "wifiid_r", str(wifiid))
            setcfpoptionvalue(CONFIG_NAME, section, "start_r", nowstr)

            # 更新笔记
            current_records = [f"{ip_local}\t{ip_public}\t{wifi}\t{wifiid}\t{nowstr}"]
            if start_r and ip_local_r and ip_public_r:
                current_records.extend(itemnewr[:4])  # 保留最近几条记录

            updatenote_title(ip_cloud_id, noteip_title, parent_id=nbid)
            updatenote_body(ip_cloud_id, "\n".join(current_records), parent_id=nbid)
            log.info("IP信息已更新")
        else:
            log.info("IP信息无变化，无需更新")

        return True

    except Exception as e:
        log.error(f"执行showiprecords时发生错误: {e}")
        return False


# %% [markdown]
# ## 主执行函数
#
# 程序入口点，处理执行逻辑和日志记录

# %%
if __name__ == "__main__":
    is_log_details = getinivaluefromcloud("happyjoplin", "logdetails")
    if not_IPython() and is_log_details:
        log.info(f"开始运行文件 {__file__}")

    success = showiprecords()

    if not_IPython() and is_log_details:
        log.info(f"文件执行完毕，状态: {'成功' if success else '失败'}")
