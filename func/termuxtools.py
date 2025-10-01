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
# # termux工具集，API

# %% [markdown]
# ## 引入重要库

# %%
import json
import re
import subprocess
from typing import Dict, Union

# %%
import pathmagic

with pathmagic.context():
    from func.common import utils
    from func.logme import log
    from func.sysfunc import (
        after_timeout,
        execcmd,
        is_tool_valid,
        not_IPython,
        set_timeout,
    )


# %% [markdown]
# ## 功能函数集合

# %%
"""
    implementation of all the termux-api commands
    via subprocesses,
"""

# %% [markdown]
# ### evaloutput(output: Union[str, bool, None]) -> Union[Dict, bool, str]


# %%
def evaloutput(output: Union[str, bool, None]) -> Union[Dict, bool, str]:
    """对output进行解析，返回字典或布尔值或原字符串"""
    if output is None or output == "null" or len(output) == 0:
        return False
    # 转换成字典输出
    if isinstance(output, str) and output.startswith("{") and output.endswith("}"):
        try:
            return json.loads(output)
        except json.JSONDecodeError:
            return output
    # 转换成布尔值输出
    if isinstance(output, str) and output.lower() in ["true", "false"]:
        return output.lower() == "true"
    # 其他情况返回原值
    return output


# %% [markdown]
# ### info2dict(info: str) -> Dict


# %%
def info2dict(info: str) -> Dict:
    """Convert the output of termux-info to a dictionary.

    Args:
        info (str): The output of termux-info.

    Returns:
        Dict: A dictionary containing the information.
    """
    ptn = re.compile("\n?.+?:\n")

    vals = re.split(ptn, info)
    vals = vals[1:]

    keys = re.findall(ptn, info)
    keys = [x.strip() for x in keys]

    return dict(zip(keys, vals))


# %% [markdown]
# ### battery_status() -> Union[Dict, bool]


# %%
@set_timeout(60, after_timeout)
def battery_status() -> Union[Dict, bool]:
    """获取电池状态，返回格式：{"status": xx, "health": xx, "plugged": xx, "voltage": xx, "temperature": xx}"""
    out = subprocess.check_output("termux-battery-status", shell=True).decode("utf-8")
    if "error" in out:
        return False
    else:
        status = out.split("\n")[0].split(":")[1].strip()
        health = out.split("\n")[1].split(":")[1].strip()
        plugged = out.split("\n")[2].split(":")[1].strip()
        voltage = out.split("\n")[3].split(":")[1].strip()
        temperature = out.split("\n")[4].split(":")[1].strip()
        return {"status": status, "health": health, "plugged": plugged, "voltage": voltage, "temperature": temperature}


# %% [markdown]
# ### camera_info() -> Union[Dict, bool]


# %%
def camera_info() -> Union[Dict, bool]:
    """获取手机相机信息

    :return: 字典，包含相机信息；False，获取失败
    """
    out, rc, err = utils.execute("termux-camera-info")
    if rc:
        return False
    return evaloutput(out)


# %% [markdown]
# ### termux_camera_photo()


# %%
def termux_camera_photo():
    out, rc, err = utils.execute("termux-camera-photo")
    if rc:
        raise Exception(err)
    return out


# %% [markdown]
# ### termux_clipboard_get()


# %%
def termux_clipboard_get():
    out, rc, err = utils.execute("termux-clipboard-get")
    if rc:
        raise Exception(err)
    return out


# %% [markdown]
# ### termux_clipboard_set()


# %%
def termux_clipboard_set():
    out, rc, err = utils.execute("termux-clipboard-set")
    if rc:
        raise Exception(err)
    return out


# %% [markdown]
# ### termux_contact_list()


# %%
def termux_contact_list():
    out, rc, err = utils.execute("termux-contact-list")
    if rc:
        raise Exception(err)
    return out


# %% [markdown]
# ### termux_dialog()


# %%
def termux_dialog():
    out, rc, err = utils.execute("termux-dialog")
    if rc:
        raise Exception(err)
    return out


# %% [markdown]
# ### termux_download()


# %%
def termux_download():
    out, rc, err = utils.execute("termux-download")
    if rc:
        raise Exception(err)
    return out


# %% [markdown]
# ### termux_fix_shebang()


# %%
def termux_fix_shebang():
    out, rc, err = utils.execute("termux-fix-shebang")
    if rc:
        raise Exception(err)
    return out


# %% [markdown]
# ### termux_info()


# %%
def termux_info():
    out, rc, err = utils.execute("termux-info")
    if rc:
        raise Exception(err)
    return out


# %% [markdown]
# ### termux_infrared_frequencies()


# %%
def termux_infrared_frequencies():
    out, rc, err = utils.execute("termux-infrared-frequencies")
    if rc:
        raise Exception(err)
    return out


# %% [markdown]
# ### termux_infrared_transmit()


# %%
def termux_infrared_transmit():
    out, rc, err = utils.execute("termux-infrared-transmit")
    if rc:
        raise Exception(err)
    return out


# %% [markdown]
# ### termux_location()


# %%
@set_timeout(210, after_timeout)
def termux_location() -> object:
    """获取当前位置信息，返回格式：{"latitude": xx, "longitude": xx, ...}"""
    try:
        out = execcmd("termux-location")
        result = evaloutput(out)
        if "latitude" in result and "longitude" in result:
            return result
    except Exception as e:
        log.warning(f"实时定位失败: {str(e)}")

    # 第二步：尝试缓存定位（最后一次成功记录）
    try:
        out_last = execcmd("termux-location -r last")
        if out_last:
            result_last = evaloutput(out_last)
            # 验证定位数据有效性（必须包含经纬度）
            if "latitude" in result_last and "longitude" in result_last:
                # 替换provider为缓存标记
                result_last["provider"] = "cached"
                return result_last
    except Exception as e:
        log.error(f"缓存定位失败: {str(e)}")

    # 终极回退：网络定位（需Termux API权限）
    try:
        out_network = execcmd("termux-location -p network")
        if out_network:
            result_network = evaloutput(out_network)
            # 验证定位数据有效性（必须包含经纬度）
            if "latitude" in result_network and "longitude" in result_network:
                # 替换provider为网络标记
                result_network["provider"] = "network"
                return result_network
    except Exception as e:
        log.error(f"网络定位失败: {str(e)}")

    return False


# %% [markdown]
# ### termux_notification()


# %%
def termux_notification():
    out, rc, err = utils.execute("termux-notification")
    if rc:
        raise Exception(err)
    return out


# %% [markdown]
# ### termux_notification_remove()


# %%
def termux_notification_remove():
    out, rc, err = utils.execute("termux-notification-remote")
    if rc:
        raise Exception(err)
    return out


# %% [markdown]
# ### termux_open()


# %%
def termux_open():
    out, rc, err = utils.execute("termux-open")
    if rc:
        raise Exception(err)
    return out


# %% [markdown]
# ### termux_open_url()


# %%
def termux_open_url():
    out, rc, err = utils.execute("termux-open-url")
    if rc:
        raise Exception(err)
    return out


# %% [markdown]
# ### termux_preload_settings()


# %%
def termux_reload_settings():
    out, rc, err = utils.execute("termux-reload-settings")
    if rc:
        raise Exception(err)
    return out


# %% [markdown]
# ### termux_setup_storage()


# %%
def termux_setup_storage():
    out, rc, err = utils.execute("termux-setup-storage")
    if rc:
        raise Exception(err)
    return out


# %% [markdown]
# ### termux_share()


# %%
def termux_share():
    out, rc, err = utils.execute("termux-share")
    if rc:
        raise Exception(err)
    return out


# %% [markdown]
# ### termux_sms_list(timecreated:bool = True, num:int = 10, shownumber:bool = True, where:str = 'all')


# %%
@set_timeout(90, after_timeout)
def termux_sms_list(
    timecreated: bool = True, num: int = 10, shownumber: bool = True, where: str = "all"
):
    cmdlst = ["termux-sms-list"]
    if timecreated:
        cmdlst.append("-d")
    cmdlst.extend(["-l", str(num)])
    cmdlst.append("-n")
    cmdlst.extend(["-t", where])
    out, rc, err = utils.execute(cmdlst)
    if rc:
        raise Exception(err)
    return evaloutput(out)


# %% [markdown]
# ### termux_sms_send(msg='hi')


# %%
@set_timeout(90, after_timeout)
def termux_sms_send(msg="hi"):
    cmdtool = "termux-sms-send"
    if not is_tool_valid(cmdtool):
        log.critical(f"命令\t{cmdtool}\t在该系统不存在，跳过执行")
        return
    cmdlist = [cmdtool, "-n", "15387182166", f"{msg}"]
    out, rc, err = utils.execute(cmdlist)
    if rc:
        log.Warning(f"发送短信时出现错误：{msg}")
        # raise Exception(err)
    else:
        log.info(f"成功发送短信。")
    return out


# %% [markdown]
# ### termux_storage_get()


# %%
def termux_storage_get():
    out, rc, err = utils.execute("termux-storage-get")
    if rc:
        raise Exception(err)
    return out


# %% [markdown]
# ### termux_telephony_call()


# %%
def termux_telephony_call():
    out, rc, err = utils.execute("termux-telephony-call")
    if rc:
        raise Exception(err)
    return out


# %% [markdown]
# ### termux_telephony_cellinfo()


# %%
def termux_telephony_cellinfo():
    out, rc, err = utils.execute("termux-telephony-cellinfo")
    if rc:
        raise Exception(err)
    return evaloutput(out)


# %% [markdown]
# ### termux_telephony_deviceinfo()


# %%
@set_timeout(90, after_timeout)
def termux_telephony_deviceinfo():
    out, rc, err = utils.execute("termux-telephony-deviceinfo")
    if rc:
        raise Exception(err)
    return evaloutput(out)


# %% [markdown]
# ### termux_toast()


# %%
def termux_toast():
    out, rc, err = utils.execute("termux-toast")
    if rc:
        raise Exception(err)
    return out


# %% [markdown]
# ### termux_tts_engines()


# %%
def termux_tts_engines():
    out, rc, err = utils.execute("termux-tts-engines")
    if rc:
        raise Exception(err)
    return out


# %% [markdown]
# ### termux_tts_speak()


# %%
def termux_tts_speak():
    out, rc, err = utils.execute("termux-tts-speak")
    if rc:
        raise Exception(err)
    return out


# %% [markdown]
# ### termux_vibrate()


# %%
def termux_vibrate():
    out, rc, err = utils.execute("termux-vibrate")
    if rc:
        raise Exception(err)
    return out


# %% [markdown]
# ### termux_wake_lock()


# %%
def termux_wake_lock():
    out, rc, err = utils.execute("termux-wake-lock")
    if rc:
        raise Exception(err)
    return out


# %% [markdown]
# ### termux_wake_unlock()


# %%
def termux_wake_unlock():
    out, rc, err = utils.execute("termux-wake-unlock")
    if rc:
        raise Exception(err)
    return out


# %% [markdown]
# ### termux_wifi_connectioninfo()


# %%
def termux_wifi_connectioninfo():
    out, rc, err = utils.execute("termux-wifi-connectioninfo")
    if rc:
        raise Exception(err)
    return evaloutput(out)


# %% [markdown]
# ### termux_wifi_scaninfo()


# %%
def termux_wifi_scaninfo():
    out, rc, err = utils.execute("termux-wifi-scaninfo")
    if rc:
        raise Exception(err)
    return evaloutput(out)


# %% [markdown]
# ## main()，主函数

# %%
if __name__ == "__main__":
    if not_IPython():
        log.info(f"测试文件\t{__file__}……")
    print(termux_telephony_deviceinfo())
    print(termux_info())
    print(termux_location())
    if not_IPython():
        log.info(f"文件\t{__file__}\t测试完毕。")
