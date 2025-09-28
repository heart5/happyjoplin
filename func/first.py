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
# # 首函

# %% [markdown]
# 首函，用于定位相对目录，丰富工作目录路径，还有构建路径的基本函数，配置中文字体支持matplotlib

# %% [markdown]
# ## 重要库导入

# %%
import os
import sys
from pathlib import Path

import matplotlib.font_manager as fm
import matplotlib.pyplot as pltpp

# %%
import pathmagic

with pathmagic.context():
    import func.fordirmainonly as fdmo


# %% [markdown]
# ## 函数库

# %% [markdown]
# ### touchfilepath2depth(filepath: Path)

# %% [markdown]
# from func.logme import log


# %%
def touchfilepath2depth(filepath: Path) -> Path:
    filep = Path(filepath)
    filep.parent.mkdir(parents=True, exist_ok=True)

    return filep

# %% [markdown]
# ### getdirmain()


# %%
def getdirmain() -> None:
    fdmodir = fdmo.__file__
    dirmainin = os.path.split(fdmodir)[0]
    dirmaininoutput = os.path.split(dirmainin)[0]

    return Path(dirmaininoutput).resolve()


# %% [markdown]
# ## 显性指定中文字体并配置matplotlib

# %%
# 添加字体路径（容器内路径）
font_path = "/usr/share/fonts/simhei.ttf"  # 指定具体字体文件
# 判断路径为了兼容其它能正常识别中文字体路径的环境，比如手机上的termux
if Path(font_path).exists():
    fm.fontManager.addfont(font_path)
    print(f"中文字体路径{font_path}存在")

# 配置全局字体
pltpp.rcParams['font.family'] = 'sans-serif'
pltpp.rcParams['font.sans-serif'] = ['SimHei']  # 使用字体Family名
pltpp.rcParams['axes.unicode_minus'] = False    # 解决负号显示问题

# %% [markdown]
# ## 定义全局变量

# %%
dirmainpath = getdirmain()
dirmain = str(getdirmain())
dirlog = str(getdirmain() / "log" / "happyjoplin.log")
dbpathworkplan = str(getdirmain() / "data" / "workplan.db")
dbpathquandan = str(getdirmain() / "data" / "quandan.db")
dbpathdingdanmingxi = str(getdirmain() / "data" / "dingdanmingxi.db")
ywananchor = 50000  # 纵轴标识万化锚点


# %%
path2include = ["etc", "func", "work", "life", "study"]
for p2i in path2include:
    combinepath = str((dirmainpath / p2i).resolve())
    if combinepath not in sys.path:
        sys.path.append(combinepath)

# %% [markdown]
# ## 主函数，main()

# %%
if __name__ == "__main__":
    # print(f'开始测试文件\t{__file__}')
    print(getdirmain())
    for dr in sys.path:
        print(dr)
    print("Done.")
