# encoding:utf-8
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
# # 获取主机id和名称

# %%
"""获取主机的唯一id."""

# %% [markdown]
# ## 库引入

# %%
# import os
# import sys
import platform
import uuid
# import wmi_client_wrapper as wmi

# %%
import pathmagic

with pathmagic.context():
    from func.configpr import getcfpoptionvalue, setcfpoptionvalue
    from func.logme import log
    from func.sysfunc import execcmd, not_IPython

    #     from func.wrapfuncs import timethis, ift2phone
    # from func.jpfuncs import getinivaluefromcloud
    from func.termuxtools import termux_telephony_deviceinfo

    try:
        import wmi
    except ImportError:
        # log.warning('wmi库未安装或者是在linux系统下无法成功import而已。')
        # print('wmi库未安装或者是在linux系统下无法成功import而已。')
        pass


# %% [markdown]
# ## 功能函数

# %% [markdown]
# ### def set_devicename2ini(id, sysstr)

# %%
def set_devicename2ini(id: str, sysstr: str) -> None:
    """设置设备名称到ini配置文件.

    Args:
        id (str): id
        sysstr (str): str

    Returns:
        None: 不返回值

    """
    from func.jpfuncs import getinivaluefromcloud

    if (device_name := getcfpoptionvalue("happyjphard", id, "device_name")) is None:
        log.info(f"设备名称{device_name}为None，可能是尚未设置或从云端获取。")
        if device_name_fromcloud := getinivaluefromcloud("device", id):
            setcfpoptionvalue("happyjphard", id, "device_name", device_name_fromcloud)
        else:
            log.critical(
                f"当前主机（id：{id}）尚未在网络端配置笔记中设定名称或者是还没完成本地化设定！！！"
            )
            if sysstr == "Linux":
                log.critical(f"主机信息：{execcmd('uname -a')}")


# %% [markdown]
# ### def get_devicenamefromini(id)


# %%
def get_devicenamefromini(id: str) -> str:
    """从ini配置文件中获取设备名称.

    Args:
        id: id

    Returns:
        str: 设备名称

    """
    return getcfpoptionvalue("happyjphard", id, "device_name")


# %% [markdown]
# ### def getdeviceid()


# %%
# @timethis
def getdeviceid() -> None:
    """获取设备id.

    Returns:
        None: 空值
    """
    # printCPU()
    # printMain_board()
    # printBIOS()
    # printDisk()
    # printMacAddress()
    # print(printBattery())
    if d_id_from_ini := getcfpoptionvalue("happyjphard", "happyjphard", "device_id"):
        return str(d_id_from_ini)
    id = None
    sysstr = platform.system()
    # print(sysstr)
    if sysstr == "Windows":
        c = wmi.WMI()
        bios_id = c.Win32_BIOS()
        # biosidc = bios_id.BiosCharacteristics  # BIOS特征码
        bioss = bios_id[0].SerialNumber.strip()
        # for bios in bios_id:
        #     print(bios)
        cpu_id = c.Win32_Processor()
        cpus = cpu_id[0].SerialNumber.strip()
        cpus = cpu_id[0].ProcessorId.strip()
        # for cpu in cpu_id:
        #     print(cpu)
        board_id = c.Win32_BaseBoard()
        boards = board_id[0].SerialNumber.strip()
        # boards = board_id[0].Product.strip()
        # for board in board_id:
        #     print(board)
        disk_id = c.Win32_DiskDrive()
        disks = disk_id[0].SerialNumber.strip()
        # for disk in disk_id:
        #     print(disk)
        idstr = f"{bioss}\t{cpus}\t{boards}\t{disks}"
        uid = uuid.uuid3(uuid.NAMESPACE_URL, idstr)
        # print(uid)
        print(hex(hash(uid)))
        id = hex(hash(uid))
    elif sysstr == "Linux":
        try:
            outputdict = termux_telephony_deviceinfo()
            id = hex(hash(uuid.uuid3(uuid.NAMESPACE_URL, str(outputdict))))
        except Exception as e:
            print(f"运行termux专用库出错{e}\n下面尝试用主机名代替")
            try:
                idstr = execcmd("uname -a")
                print(idstr)
                uid = uuid.uuid3(uuid.NAMESPACE_URL, idstr)
                # print(uid)
                print(hex(hash(uid)))
                id = hex(hash(uid))
            except Exception as e:
                print("天啊，命令行都不成！只好强行赋值了")
                id = 123456789
                type(e)
    #                 raise
    else:
        log.critical("既不是Windows也不是Linux，那是啥啊。只好强行赋值了！！！")
        id = 123456789
    #         exit(1)

    id = str(id)
    setcfpoptionvalue("happyjphard", "happyjphard", "device_id", id)
    set_devicename2ini(id, sysstr)

    return id


# %% [markdown]
# ### getdevicename()


# %%
def getdevicename() -> str:
    """获取设备名称.

    Returns:
        str: 设备名称
    """
    id = getdeviceid()
    set_devicename2ini(id, "Linux")

    return get_devicenamefromini(id)


# %% [markdown]
# ### gethostuser()


# %%
def gethostuser() -> str:
    hostuser = getdevicename() + "(" + execcmd("whoami") + ")"

    return hostuser


# %% [markdown]
# ## 主函数

# %%
if __name__ == "__main__":
    if not_IPython():
        log.info(f"运行文件\t{__file__}")
    deviceid = getdeviceid()
    print(deviceid)
    devicename = getdevicename()
    print(f"{devicename}")
    if not_IPython():
        log.info(f"文件\t{__file__}\t运行完毕。")
