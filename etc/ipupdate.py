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

# %% [markdown]
# # 获取运行服务器IP并动态管理更新至相应笔记

# %% [markdown]
# ## 引入核心库

# %%
import datetime
import os
import platform
import re
import sys

import pathmagic

# %%
with pathmagic.context():
    from etc.getid import getdeviceid, gethostuser
    from func.configpr import getcfpoptionvalue, is_log_details, setcfpoptionvalue
    from func.datatools import readfromtxt, write2txt
    from func.first import dirmainpath

    # from func.evernttest import get_notestore, imglist2note, makenote
    # from func.evernttest import evernoteapijiayi, readinifromnote
    from func.jpfuncs import (
        createnote,
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


# %% [markdown]
# ## 功能函数

# %% [markdown]
# ### getipwifi()


# %%
def getipwifi():
    """
    根据不同操作系统，调用命令行工具获取ip（本地、外部）和wifi信息
    返回ip_local, ip_public, wifi, wifiid。如果为空，则替换为None
    """
    if not is_tool_valid("neofetch"):
        log.critical(
            "Please install neofetch tool for system. Maybe run: 'pkg install neofetch' in your terminal."
        )
        exit(1)

    sys_platform_str = execcmd("uname -a")
    if re.findall("Linux", sys_platform_str):
        # ip_local = execcmd("neofetch local_ip").split(":")[-1].strip()
        ifinet_str = execcmd(
            "ifconfig | grep 'inet ' | grep -v 127.0.0.1 | awk '{print $2}'"
        )
        log.info(f"ifconfig grep inet shows:\t{ifinet_str}")
        ip_local = ifinet_str.split()[-1]
        curlifstr = "curl ifconfig.me"
        if (ip_public := execcmd(curlifstr)) and (
            len(re.findall("\d{1,3}\.?", ip_public)) != 4
        ):
            log.critical(
                f"({curlifstr})未获取合适的ipv4_public地址，而是【{ip_public}】"
            )
            curlipifystr = "curl 'https://api.ipify.org?format=json'"
            ip_public = eval(execcmd(curlipifystr)).get("ip")
            log.info(f"({curlipifystr})获取的ipv4_public地址【{ip_public}】")
            if len(re.findall("\d{1,3}\.?", ip_public)) != 4:
                log.critical(
                    f"({curlipifystr})未获取合适的ipv4_public地址，而是【{ip_public}】"
                )
                ip_public = execcmd("neofetch public_ip").split(":")[-1].strip()
        if re.findall("Android", sys_platform_str):
            wifi = termux_wifi_connectioninfo().get("ssid")
            if wifi != "<unknown ssid>":
                wifiid = termux_wifi_connectioninfo().get("bssid")
            else:
                wifi = wifiid = ""
        else:
            nmcli_str = execcmd("nmcli dev wifi")
            if len(nmcli_str) != 0:
                wifi = re.findall("\*.+", nmcli_str)[0].split()[1]
                nmclilst_str = execcmd("nmcli device wifi list")
                wifiid = re.findall(f"{wifi}.+", nmclilst_str)[0].split()[-1]
            else:
                wifi = wifiid = ""
    elif platform.system == "Windows":
        ipconfig_str = execcmd("ipconfig")
        ip_local = [
            line.split(":")[-1]
            for line in re.findall("IPv4.*", ipconfig_str)
            if re.findall("\.\d+$", line)
        ][-1].strip()
        nslookup_str = execcmd("nslookup myip.opendns.com resolver1.opendns.com")
        ip_public = [
            line.split(":")[-1]
            for line in re.findall("Address.*", nslookup_str)
            if re.findall("\.\d+$", line)
        ][-1].strip()
        wifi_str = execcmd("netsh wlan show interfaces")
        resultlst = [line.split(":", 1) for line in re.findall(".*SSID.*", wifi_str)]
        splitlist = [x.strip() for line in resultlst for x in line]
        wifidict = dict()
        for i in range(int(len(splitlist) / 2)):
            wifidict[splitlist[i * 2]] = splitlist[i * 2 + 1]
        wifi = wifidict.get("SSID")
        wifiid = wifidict.get("BSSID")
    else:
        # 未知操作系统，变量全部赋值为None
        ip_local = ip_public = wifi = wifiid = ""
    lst = [ip_local, ip_public, wifi, wifiid]
    print(lst)
    resultlst = [None if len(x) == 0 else x for x in lst]
    return tuple(resultlst)


# %% [markdown]
# ### evalnone(input1)


# %%
def evalnone(input1):
    """
    转换从终端接收数据的数据类型
    """
    if input1 == "None":
        return eval(input1)
    return input1


# %% [markdown]
# ### showiprecords()


# %%
def showiprecords():
    """
    综合输出ip记录
    """
    device_id = getdeviceid()
    namestr = "happyjpip"
    section = f"{device_id}"
    noteip_title = f"ip动态_【{gethostuser()}】"

    ip_local, ip_public, wifi, wifiid = getipwifi()
    log.info(f"{ip_local}\t{ip_public}\t{wifi}\t{wifiid}")
    if ip_public is None:
        logstr = "无效ip_public，可能是没有处于联网状态"
        log.critical(logstr)
        sys.exit(1)
    nbid = searchnotebook("ewmobile")
    if not (ip_cloud_id := getcfpoptionvalue(namestr, section, "ip_cloud_id")):
        ipnotefindlist = searchnotes(f"title:{noteip_title}")
        if len(ipnotefindlist) == 0:
            ip_cloud_id = createnote(title=noteip_title, parent_id=nbid)
            log.info(f"新的ip动态图笔记“{ip_cloud_id}”新建成功！")
        else:
            ip_cloud_id = ipnotefindlist[-1].id
        setcfpoptionvalue(namestr, section, "ip_cloud_id", f"{ip_cloud_id}")

    nowstr = datetime.datetime.now().strftime("%F %T")
    if getcfpoptionvalue(namestr, section, "ip_local_r"):
        ip_local_r = evalnone(getcfpoptionvalue(namestr, section, "ip_local_r"))
        ip_public_r = evalnone(getcfpoptionvalue(namestr, section, "ip_public_r"))
        wifi_r = evalnone(getcfpoptionvalue(namestr, section, "wifi_r"))
        if type(wifi_r) == int:
            log.info(f"wifi_r的值为{wifi_r}，值的类型为{type(wifi_r)}")
            wifi_r = str(wifi_r)
        wifiid_r = evalnone(getcfpoptionvalue(namestr, section, "wifiid_r"))
        start_r = getcfpoptionvalue(namestr, section, "start_r")
    else:
        setcfpoptionvalue(namestr, section, "ip_local_r", str(ip_local))
        ip_local_r = ip_local
        setcfpoptionvalue(namestr, section, "ip_public_r", str(ip_public))
        ip_public_r = ip_public
        setcfpoptionvalue(namestr, section, "wifi_r", str(wifi))
        wifi_r = wifi
        setcfpoptionvalue(namestr, section, "wifiid_r", str(wifiid))
        wifiid_r = wifiid
        start_r = nowstr
        setcfpoptionvalue(namestr, section, "start_r", start_r)

    if (
        (ip_local != ip_local_r)
        or (wifi != wifi_r)
        or (ip_public != ip_public_r)
        or (wifiid != wifiid_r)
    ):
        txtfilename = str(dirmainpath / "data" / "ifttt" / f"ip_{section}.txt")
        print(os.path.abspath(txtfilename))
        itemread = readfromtxt(txtfilename)
        itemclean = [x for x in itemread if "unknown" not in x]
        itempolluted = [x for x in itemread if "unknown" in x]
        if len(itempolluted) > 0:
            logstr = f"不合法记录列表：\t{itempolluted}"
            log.info(logstr)
        itemnewr = [
            f"{ip_local_r}\t{ip_public_r}\t{wifi_r}\t{wifiid_r}\t{start_r}\t{nowstr}"
        ]
        itemnewr.extend(itemclean)
        log.info(itemnewr[:4])
        write2txt(txtfilename, itemnewr)
        itemnew = [f"{ip_local}\t{ip_public}\t{wifi}\t{wifiid}\t{nowstr}"]
        itemnew.extend(itemnewr)
        log.info(itemnew[:5])
        setcfpoptionvalue(namestr, section, "ip_local_r", str(ip_local))
        setcfpoptionvalue(namestr, section, "ip_public_r", str(ip_public))
        setcfpoptionvalue(namestr, section, "wifi_r", str(wifi))
        setcfpoptionvalue(namestr, section, "wifiid_r", str(wifiid))
        setcfpoptionvalue(namestr, section, "start_r", nowstr)
        # 把笔记输出放到最后，避免更新不成功退出影响数据逻辑
        updatenote_title(ip_cloud_id, noteip_title, parent_id=nbid)
        updatenote_body(ip_cloud_id, "\n".join(itemnew), parent_id=nbid)


# %% [markdown]
# ## 主函数，main()

# %%
if __name__ == "__main__":
    if not_IPython() and is_log_details:
        logstr2 = f"开始运行文件\t{__file__}\t{sys._getframe().f_code.co_name}\t{sys._getframe().f_code.co_filename}"
        log.info(logstr2)
    showiprecords()
    # print(f"{self.__class__.__name__}")
    if not_IPython() and is_log_details:
        logstr1 = f"文件\t{__file__}\t执行完毕"
        log.info(logstr1)
