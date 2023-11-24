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
import re
import os
import base64
import io
import pandas as pd
from datetime import datetime
from pathlib import Path
import matplotlib.pyplot as plt

# %%
import pathmagic
with pathmagic.context():
    # from func.first import getdirmain
    from func.logme import log
    from etc.getid import getdevicename, gethostuser
    from func.configpr import getcfpoptionvalue, setcfpoptionvalue, is_log_details
    from func.jpfuncs import searchnotes, createnote, updatenote_imgdata, \
        noteid_used, searchnotebook
    from func.wrapfuncs import timethis
    # from func.termuxtools import termux_telephony_deviceinfo
    from func.sysfunc import execcmd, not_IPython


# %% [markdown]
# ## 功能函数

# %% [markdown]
# ### getmemdf()

# %%
@timethis
def getmemdf():
    """
    从指定路径获取内存情况并处理数据，生成DataFrame返回
    """
    # 根据不同的系统复制家目录
    sysinfo = execcmd("uname -a")
    if re.search("Android", sysinfo) is None:
        homepath = execcmd("echo ~")
        log.info(f"It's Linux[{gethostuser()}]. Home is {homepath}")
    else:
        homepath = execcmd("echo $HOME")
        log.info(f"It's Android[{gethostuser()}]. Home is {homepath}")
    dpath = Path(homepath) / "sbase/zshscripts/data/freeinfo.txt"
    if not os.path.exists(dpath):
        log.critical(f"内存数据文件（{dpath}）不存在，退出运行！！！")
        exit(1)
    with open(dpath, "r") as f:
        content = f.read()
    # 分行获取总内存(文件首行)和时间点空闲内存记录列表
    lineslst = content.split("\n")
    totalmem = int(lineslst[0].split("=")[-1])
    memlst = [x.split("\t") for x in lineslst[1:]]
    # 时间精确到分，方便后面去重
    memlstdone = [[datetime.fromtimestamp(int(x[0])).strftime("%F %H:%M"),
                   int(x[1]), int(x[2]), int(x[3])]
                  for x in memlst if len(x[0]) > 0]
    memdf = pd.DataFrame(memlstdone, columns=['time', 'freepercent', 'swaptotal', 'swapfree'])
    memdf['time'] = pd.to_datetime(memdf['time'])
    print(memdf.dtypes)
    num_all = memdf.shape[0]
    memdf.drop_duplicates(['time'], inplace=True)
    log.info(f"{gethostuser()}内存占用记录共有{num_all}条，去重后有效记录有{memdf.shape[0]}条")
    log.info(f"{gethostuser()}内存占用记录最新日期为{memdf['time'].max()}，最早日期为{memdf['time'].min()}")
    # 重置索引，使其为连续的整数，方便后面精准切片
    memdfdone = memdf.reset_index()

    return totalmem, memdfdone


# %% [markdown]
# ### gap2img()

# %%
@timethis
def gap2img():
    """
    把内存记录按照间隔（30分钟）拆离，并生成最近的动图和所有数据集的总图
    """
    totalmem, memdfdone = getmemdf()
    tmemg = totalmem / (1024 * 1024)

    time_elasp = memdfdone['time'] - memdfdone['time'].shift(1)
    tm_gap = time_elasp[time_elasp > pd.Timedelta("30m")]

    gaplst = list()
    for ix in tm_gap.index:
        gaplst.append(f"{ix}\t{memdfdone['time'].loc[ix]}\t{tm_gap[ix]}")
    log.info(f"{gethostuser()}的内存记录数据不连续(共有{tm_gap.shape[0]}个断点)：{'|'.join(gaplst)}")

    # 处理无断点的情况
    if len(gaplst) == 0:
        last_gap = memdfdone.set_index(['time'])['freepercent']
    else:
        last_gap = memdfdone.loc[list(tm_gap.index)[-1]:].set_index(['time'])['freepercent']

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
            tmpdf = memdfdone.loc[gaplst[i]:gaplst[i + 1] - 1].set_index(['time'])['freepercent']
            log.info(f"切片数据集最新日期为{tmpdf.index.max()}，最早日期为{tmpdf.index.min()}，数据项目数量为{tmpdf.shape[0]}")
            ax2.plot(tmpdf)
    plt.title(f"全部周期内存占用动态图[{gethostuser()}]")

    # convert the plot to a base64 encoded image
    buffer = io.BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)
    image_base64 = base64.b64encode(buffer.read()).decode()
    # now, 'image_base64' contains the base64 encoded image
    # close the plot to free up resources
    plt.tight_layout()
    plt.show()
    plt.close()

    return image_base64


# %% [markdown]
# ### freemem2note()

# %%
@timethis
def freemem2note():
    """
    综合输出内存动态图并更新至笔记
    """

    login_user = execcmd("whoami")
    namestr = "happyjp_life"
    section = f"health_{login_user}"
    notestat_title = f"内存动态图【{gethostuser()}】"

    image_base64 = gap2img()
    nbid = searchnotebook("ewmobile")
    if not (freestat_cloud_id := getcfpoptionvalue(namestr, section, 'freestat_cloud_id')):
        freenotefindlist = searchnotes(f"title:{notestat_title}")
        if (len(freenotefindlist) == 0):
            freestat_cloud_id = createnote(title=notestat_title, parent_id=nbid,
                                             imgdata64=image_base64)
            log.info(f"新的内存动态图笔记“{freestat_cloud_id}”新建成功！")
        else:
            freestat_cloud_id = freenotefindlist[-1].id
        setcfpoptionvalue(namestr, section, 'freestat_cloud_id', f"{freestat_cloud_id}")

    if not noteid_used(freestat_cloud_id):
        freestat_cloud_id = createnote(title=notestat_title, parent_id=nbid,
                                         imgdata64=image_base64)
    else:
        freestat_cloud_id, res_lst = updatenote_imgdata(noteid=freestat_cloud_id,
                                                          parent_id=nbid, imgdata64=image_base64)
    setcfpoptionvalue(namestr, section, 'freestat_cloud_id', f"{freestat_cloud_id}")


# %% [markdown]
# ## 主函数，main()

# %%
if __name__ == '__main__':
    if not_IPython() and is_log_details:
        log.info(f'运行文件\t{__file__}')

    freemem2note()

    if not_IPython() and is_log_details:
        log.info(f'文件\t{__file__}\t运行完毕。')
