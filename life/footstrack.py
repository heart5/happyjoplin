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
# # GPS位置信息记录

# %% [markdown]
# ## 引入重要库

# %%
# from pylab import *
import datetime
# import subprocess

import pathmagic

with pathmagic.context():
    from func.configpr import getcfpoptionvalue, setcfpoptionvalue, is_log_details
    from func.first import dirmainpath
    from func.datatools import readfromtxt, write2txt
    from func.logme import log
    from func.wrapfuncs import timethis
    from func.termuxtools import termux_location
    from etc.getid import getdeviceid, gethostuser
    from func.sysfunc import set_timeout, after_timeout, not_IPython


# %% [markdown]
# ## 函数库

# %% [markdown]
# ### foot2record()


# %%
@set_timeout(240, after_timeout)
@timethis
def foot2record():
    """
    记录位置数据（经纬度等）
    """
    namestr = "happyjp_life"
    section = "hjloc"

    if device_id := getcfpoptionvalue(namestr, section, "device_id"):
        device_id = str(device_id)
    else:
        device_id = getdeviceid()
        setcfpoptionvalue(namestr, section, "device_id", device_id)

    txtfilename = str(dirmainpath / "data" / "ifttt" / f"location_{device_id}.txt")
    print(txtfilename)
    itemread = readfromtxt(txtfilename)
    numlimit = 5  # 显示项目数
    print(itemread[:numlimit])
    locinfo = termux_location()
    print(locinfo)
    nowstr = datetime.datetime.now().strftime("%F %T")
    itemnewr = [nowstr]
    if locinfo == False:
        itemnewr.extend[f"{str(locinfo)}"]
    else:
        itemnewr.extend(locinfo.values())
    itemnewr = [str(x) for x in itemnewr]
    itemline = ["\t".join(itemnewr)]
    itemline.extend(itemread)
    print(itemline[:numlimit])
    write2txt(txtfilename, itemline)


# %%
if __name__ == "__main__":
    if not_IPython():
        log.info(f"运行文件\t{__file__}……")
    foot2record()
    if not_IPython():
        print(f"完成文件{__file__}\t的运行")
