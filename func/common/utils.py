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
import subprocess
from typing import List, Optional, Tuple


# %% [markdown]
# ### execute(cmd: List[str], encoding: str = "UTF-8", timeout: Optional[int] = None, shell: bool = False) -> Tuple[str, int, str]

# %%
def execute(cmd: List[str], encoding: str = "UTF-8", timeout: Optional[int] = None, shell: bool = False) -> Tuple[str, int, str]:
    """执行一个shell命令或二进制文件。

    如果你在以特权用户身份运行此脚本，请谨慎对待要执行的内容。

    参数:
    cmd:      List[str] -- 分割的命令（例如：['ls', '-la', '~']）
    encoding: str       -- 用于解码命令输出的编码格式（默认：'UTF-8'）
    timeout:  Optional[int] -- 以秒为单位，设置命令执行超时时间。如果超过该时间，将抛出TimeoutExpired异常（默认：None）
    shell:    bool      -- 如果为True，则通过shell执行命令（默认：False）

    返回:
    Tuple[str, int, str] -- 包含标准输出、返回码和标准错误的元组
    """
    try:
        proc = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=shell)
        output, error = proc.communicate(timeout=timeout)
        output = output.decode(encoding).rstrip()
        error = error.decode(encoding).rstrip()
        rc = proc.returncode
        return (output, rc, error)
    except subprocess.TimeoutExpired:
        return ("", -1, "Command execution timed out")
    except Exception as e:
        return ("", -1, str(e))

