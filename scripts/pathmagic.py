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
# # 魔法路径

# %% [markdown]
# ## 引入库

# %%
import sys
from pathlib import Path

# %% [markdown]
# ## context类


# %%
class context:  # noqa: N801
    """这是一个上下文管理器类，用于临时修改sys.path。"""

    def __enter__(self) -> None:
        """进入上下文管理器时，将当前目录和上级目录添加到sys.path中。"""
        syspathlst = [Path(p).resolve() for p in sys.path]
        for inpath in ["..", "."]:
            if Path(inpath).resolve() not in syspathlst:
                sys.path.append(inpath)
        # self.printsyspath()

    @staticmethod
    def printsyspath() -> None:
        """打印sys.path中的所有路径。"""
        for pson in sys.path:
            print(Path(pson).resolve())
        print(10 * '*')

    def __exit__(self, *args: any) -> None:
        """退出上下文管理器时，不做任何操作。"""
        pass


# %% [markdown]
# ## 主函数main()

# %%
if __name__ == "__main__":
    # print(f"运行文件\t{__file__}")
    for pp in sys.path:
        pson = Path(pp).resolve()
        print(pson)
    print("Done.完毕。")
