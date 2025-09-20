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
"""IP信息更新工具 (增强版).

功能：获取设备IP和WiFi信息，记录变化并更新至Jupyter笔记
优化点：修复数字类型处理、增强错误处理、添加Markdown函数声明.
"""

# %% [markdown]
# ## 导入依赖库

# %%
import datetime
import ipaddress
import os
import platform
import re
import sys
from typing import Any, Optional, Tuple

# %%
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
            searchnotebook,
            searchnotes,
            updatenote_body,
            # updatenote_imgdata,
            updatenote_title,
        )
        from func.logme import log

        # from func.nettools import get_ip4alleth
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
# ### is_valid_ip(ip_str: Optional[str]) -> bool

# %%
def is_valid_ip(ip_str: Optional[str]) -> bool:
    """验证一个字符串是否是有效的IPv4或IPv6地址.

    使用标准库 ipaddress 进行验证，最为可靠。
    """
    if not ip_str:
        return False
    try:
        ipaddress.ip_address(ip_str.strip())
        return True
    except ValueError:
        return False


# %% [markdown]
# ### get_public_ip() -> Tuple[Optional[str], Optional[str]]

# %%
def get_public_ip() -> Tuple[Optional[str], Optional[str]]:
    """尝试从多个源获取公网IP地址.

    返回一个元组 (ip_address, error_message)。
    如果成功获取到有效IP，则error_message为None。
    如果获取失败或IP无效，则ip_address为None，并返回错误信息。.
    """
    # 定义多个可靠的公网IP查询服务
    ip_services = [
        " https://api.ipify.org ",  # 简单可靠
        " https://ident.me ",
        " https://ifconfig.me/ip ",
        " https://ipinfo.io/ip ",
    ]

    for service in ip_services:
        try:
            # 使用带超时的curl命令，避免长时间阻塞
            cmd = f"curl -s -m 8 {service}"
            ip_candidate = execcmd(cmd).strip()

            if is_valid_ip(ip_candidate):
                # 成功获取到有效IP
                return (ip_candidate, None)
            else:
                # 获取到了响应，但内容不是IP，记录日志
                log.warning(f"服务 {service} 返回了无效内容: {ip_candidate}")
                continue

        except Exception as e:
            # 命令执行失败（超时、网络错误等）
            error_msg = f"从 {service} 获取IP失败: {str(e)}"
            log.warning(error_msg)
            continue

    # 所有服务都尝试失败
    return (None, "所有公网IP查询服务均不可用或返回无效数据")

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
    """根据不同操作系统，调用命令行工具获取ip（本地、外部）和wifi信息.

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
        ip_public, ip_error = get_public_ip()
        if ip_public is None:
            log.error(f"获取公网IP失败，原因：{ip_error}")

        # 获取WiFi信息 - 首先检查nmcli是否存在，不存在则尝试其他方法
        if re.findall("Android", sys_platform_str):
            # Termux 环境
            try:
                wifi_info = termux_wifi_connectioninfo()
                wifi = wifi_info.get("ssid", "")
                wifiid = wifi_info.get("bssid", "") if wifi != "<unknown ssid>" else ""
                wifi = "" if wifi == "<unknown ssid>" else wifi
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
                    log.info(f"无法读取 /proc/net/wireless 或无线设备未激活{e}")

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
    """主函数：获取IP记录并更新笔记，处理数字类型转换问题.

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

        should_update = False
        # 1. 检查公网IP是否有效
        if not is_valid_ip(ip_public):
            # 本次获取失败，但可能有之前有效的记录
            log.error(
                f"本次获取失败，公网IP无效: {ip_public}。将尝试使用上次的有效记录进行对比。"
            )
            # 从配置中读取上一次成功的公网IP记录
            ip_public_r_prev = str(getcfpoptionvalue(CONFIG_NAME, section, "ip_public_r_prev"))
            # 如果上一次的记录是有效的，说明网络状态可能从“有IP”变成了“无IP”（如断网）
            if is_valid_ip(ip_public_r_prev):
                log.info("检测到公网IP丢失（从有到无），这是一种状态变化，需要记录。")
                # 此处可以特殊处理，例如记录一条“网络断开”的日志
                should_update = True
            else:
                log.info("本次和上次获取均失败，无有效状态变化，跳过更新。")
                return False  # 不更新记录

        # 2. 检查其他信息是否有效（例如本地IP）
        if not is_valid_ip(ip_local):
            log.warning(f"本地IP地址无效: {ip_local}，但仍可继续处理公网IP")

        # 3. 只有所有核心信息（至少公网IP）有效，才与历史记录比较并决定是否更新
        ip_public_r = str(getcfpoptionvalue(CONFIG_NAME, section, "ippublic_r"))
        if is_valid_ip(ip_public):
            if ip_public != ip_public_r:
                should_update = True

        # 查找或创建笔记
        nbid = searchnotebook("ewmobile")
        ip_cloud_id = str(getcfpoptionvalue(CONFIG_NAME, section, "ip_cloud_id"))
        if not ip_cloud_id:
            ipnotefindlist = searchnotes(f"{noteip_title}")
            if ipnotefindlist:
                ip_cloud_id = ipnotefindlist[-1].id
            else:
                ip_cloud_id = createnote(title=noteip_title, parent_id=nbid)
                log.info(f"新建IP动态笔记: {ip_cloud_id}")
            setcfpoptionvalue(CONFIG_NAME, section, "ip_cloud_id", str(ip_cloud_id))

        # 使用安全函数获取记录信息
        nowstr = datetime.datetime.now().strftime("%F %T")
        ip_local_r = str(getcfpoptionvalue(CONFIG_NAME, section, "ip_local_r"))
        wifi_r = str(getcfpoptionvalue(CONFIG_NAME, section, "wifi_r"))
        wifiid_r = str(getcfpoptionvalue(CONFIG_NAME, section, "wifiid_r"))
        start_r = str(getcfpoptionvalue(CONFIG_NAME, section, "start_r"))

        log.info(f"上次记录的信息: {ip_local_r}, {ip_public_r}, {wifi_r}, {wifiid_r}")

        # 检查信息是否有变化（全部作为字符串比较）
        has_changed = (
            (str(ip_local) != str(ip_local_r))
            or (str(ip_public) != str(ip_public_r))
            or (str(wifi) != str(wifi_r))
            or (str(wifiid) != str(wifiid_r))
        )

        if has_changed or not start_r or should_update:
            log.info("检测到信息变化或首次运行，更新记录")

            # 保存变化记录到文件
            txtfilename = os.path.join(
                dirmainpath, "data", "ifttt", f"ip_{section}.txt"
            )
            os.makedirs(os.path.dirname(txtfilename), exist_ok=True)

            # 读取现有记录
            itemread = readfromtxt(txtfilename) if os.path.exists(txtfilename) else []
            itemclean = [x for x in itemread if "timeout" not in x]
            itempolluted = [x for x in itemread if "timeout" in x]

            if itempolluted:
                log.info(f"发现不合法记录: {itempolluted}")

            # 添加新记录并写入文件
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
            # 构建当前记录行
            if is_valid_ip(ip_public) and is_valid_ip(ip_local):
                # 一切正常，记录数据行
                current_record = f"{ip_local}\t{ip_public}\t{wifi}\t{wifiid}\t{nowstr}"
            else:
                # 获取失败，记录一条错误日志行（以#或//开头以示区别）
                error_type = "NoPublicIP" if not is_valid_ip(ip_public) else "NoLocalIP"
                current_record = f"# ERROR::timeout::{error_type}::{nowstr}:: Local={ip_local}, Public={ip_public}"
            # current_records = [f"{ip_local}\t{ip_public}\t{wifi}\t{wifiid}\t{nowstr}"]
            current_records = [current_record]
            if start_r and ip_local_r and ip_public_r:
                current_records.extend(itemnewr[:30])  # 保留最近几条记录

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
