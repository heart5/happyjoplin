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
# 首函，用于定位相对目录，丰富工作目录路径，还有构建路径的基本函数

# %% [markdown]
# ## 重要库导入

# %%
import os
import sys
from pathlib import Path

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
def touchfilepath2depth(filepath: Path):
    if not os.path.exists(os.path.split(str(filepath))[0]):
        os.makedirs(os.path.split(str(filepath))[0])
        print(f"目录《{os.path.split(str(filepath))[0]}》不存在，构建之。")

    return filepath


# %% [markdown]
# ### getdirmain()


# %%
def getdirmain():
    fdmodir = fdmo.__file__
    dirmainin = os.path.split(fdmodir)[0]
    dirmaininoutput = os.path.split(dirmainin)[0]

    return Path(dirmaininoutput).resolve()


# %% [markdown]
# ### 定义全局变量

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
    sys.path.append(str(dirmainpath / p2i))
# for dr in sys.path:
#     print(dr)

# %% [markdown]
# ## 主函数，main()

# %%
if __name__ == "__main__":
    # print(f'开始测试文件\t{__file__}')
    print(getdirmain())
    for dr in sys.path:
        print(dr)
    print("Done.")
