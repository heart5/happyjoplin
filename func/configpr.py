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
# # 配置处理器相关的功能函数

# %% [markdown]
# ## 引入库

# %%
import os
import re
from configparser import ConfigParser, DuplicateOptionError, DuplicateSectionError
from pathlib import Path
from typing import Any, Tuple

import pathmagic

# %%
with pathmagic.context():
    from func.first import getdirmain, touchfilepath2depth
    from func.logme import log
    from func.sysfunc import not_IPython


# %% [markdown]
# ## 功能函数

# %% [markdown]
# ### dropdup4option(opcontent)


# %%
def dropdup4option(opcontent: str) -> str:
    ptno = re.compile(r"(\w+)\s*=\s*(\w*)")
    opdict = dict()
    fdlst = re.findall(ptno, opcontent)
    for item in fdlst:
        if item[0] in opdict.keys():
            log.critical(f"出现option名称重复：\t{item[0]}，取用最新的数据")
        opdict.update(dict({item[0]: item[1]}))
    rstlst = [" = ".join(list(x)) for x in list(zip(opdict.keys(), opdict.values()))]
    return "\n" + "\n".join(rstlst) + "\n\n"


# %% [markdown]
# ### dropdup4section(fcontent)


# %%
def dropdup4section(fcontent: str) -> str:
    ftn = re.compile(r"\[\w+\]")
    sectionlst = re.findall(ftn, fcontent)
    optionlst = re.split(ftn, fcontent)
    resultdict = dict()
    for i in range(len(sectionlst)):
        sname = sectionlst[i]
        if sname in resultdict.keys():
            thislen = len(resultdict[sname])
            thatlen = len(optionlst[i + 1])
            log.critical(f"存在重复的section：\t{sname}\t{thislen}\t{thatlen}")
            if thislen > thatlen:
                continue
        cleanopcontent = dropdup4option(optionlst[i + 1])
        resultdict.update({sname: cleanopcontent})
    rstlst = [x for y in list(zip(resultdict.keys(), resultdict.values())) for x in y]
    correctcontent = "".join(rstlst)

    return correctcontent


# %% [markdown]
# ### fixinifile(inipath)


# %%
def fixinifile(inipath: Path) -> str:
    with open(inipath, "r") as f:
        fcontent = f.read()
        with open(str(inipath) + ".bak", "w") as writer:
            writer.write(fcontent)
    correctcontent = dropdup4section(fcontent)
    with open(str(inipath), "w") as writer:
        writer.write(correctcontent)
    return correctcontent


# %% [markdown]
# ### removesection(cfpfilename, sectionname)


# %%
def removesection(cfpfilename: str, sectionname: str) -> None:
    """删除指定section，默认清除其下面的所有option"""
    cfpin, cfpinpath = getcfp(cfpfilename)
    if cfpin.has_section(sectionname):
        cfpin.remove_section(sectionname)
        cfpin.write(open(cfpinpath, "w", encoding="utf-8"))
        log.critical(f"成功清除{sectionname}下的所有option！！！")


# %% [markdown]
# ### getcfp(cfpfilename)


# %%
def getcfp(cfpfilename: str) -> Tuple[ConfigParser, Path]:
    cfpson = ConfigParser()
    inipathson = Path(getdirmain()) / "data" / (cfpfilename + ".ini")
    touchfilepath2depth(inipathson)
    try:
        cfpson.read(inipathson, encoding="utf-8")
    except (DuplicateSectionError, DuplicateOptionError) as dse:
        log.critical(
            f"ini文件《{inipathson}》中存在重复的section或option名称，备份文件并试图修复文件……{dse}"
        )
        fixinifile(inipathson)
    except Exception as eee:
        log.critical(eee)
        try:
            cfpson.read(inipathson, encoding="utf-8")
            log.critical(f"ini文件《{inipathson}》修复成功！！！")
        except Exception as e:
            log.critical(f"读取配置文件《{inipathson}》时失败！！！{e}")
            log.critical(f"试图强制删除该配置文件《{inipathson}》")
            os.remove(inipathson)
            log.critical(f"配置文件《{inipathson}》被强制删除！！！")
            return

    return cfpson, inipathson


# %% [markdown]
# ### setcfpoptionvalue(cfpfilename, sectionname, optionname, optionvalue)


# %%
def setcfpoptionvalue(cfpfilename: str, sectionname: str, optionname: str, optionvalue: str) -> None:
    cfpin, cfpinpath = getcfp(cfpfilename)
    if not cfpin.has_section(sectionname):
        cfpin.add_section(sectionname)
        cfpin.write(open(cfpinpath, "w", encoding="utf-8"))
    cfpin.set(sectionname, optionname, optionvalue)
    cfpin.write(open(cfpinpath, "w", encoding="utf-8"))


# %% [markdown]
# ### getcfpoptionvalue(cfpfilename, sectionname, optionname)


# %%
def getcfpoptionvalue(cfpfilename: str, sectionname: str, optionname: str) -> Any:  # noqa: ANN401
    cfpin, cfpinpath = getcfp(cfpfilename)
    if not cfpin.has_section(sectionname):
        print(f"seticon {sectionname} is not exists. Then creating it now ...")
        cfpin.add_section(sectionname)
        cfpin.write(open(cfpinpath, "w", encoding="utf-8"))
        return
    if not cfpin.has_option(sectionname, optionname):
        print(f"option {optionname} is not exists.")
        return

    targetvalue = str(cfpin.get(sectionname, optionname))

    # 处理None
    if targetvalue.strip().lower() == "none":
        return None

    # 处理布尔值
    if targetvalue.strip().lower() == "true":
        return True

    if targetvalue.strip().lower() == "false":
        return False

    # 处理整数
    ptn = re.compile(r"^[+-]?[0-9]+$")
    result = ptn.match(targetvalue)
    if result:
        targetvalue = int(result.group())
        return targetvalue

    # 处理小数
    ptn = re.compile(r"^[+-]?[0-9]+\.[0-9]+$")
    result = ptn.match(targetvalue)
    if result:
        targetvalue = float(result.group())
        return targetvalue
    # if isinstance(targetvalue, int):
    # targetvalue = int(targetvalue)
    # elif isinstance(targetvalue, float):
    # targetvalue = float(targetvalue)

    return targetvalue


# %% [markdown]
# ## 主函数main()

# %%
if __name__ == "__main__":
    from jpfuncs import getinivaluefromcloud
    is_log_details = getinivaluefromcloud('happyjoplin', 'is_log_details')
    if not_IPython() and is_log_details:
        print(f"开始测试文件\t{__file__}")
    cfpapiname = "happyjp"
    inipathson = Path(getdirmain()) / "data" / (cfpapiname + ".ini")
    cp, cppath = getcfp(cfpapiname)
    print(cp)
    print(cppath)
    if not_IPython() and is_log_details:
        print("Done.")
