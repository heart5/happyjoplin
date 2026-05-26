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
# # 聊天消息 TSV 序列化

# %%
"""webchat ↔ wc2note 共享的消息序列化/反序列化"""


# %% [markdown]
# ## format_tsv(fmmsg: dict) -> str

# %%
def format_tsv(fmmsg: dict) -> str:
    """将 formatmsg 输出 dict 序列化为 TSV 行。

    字段顺序：fmTime, fmSend, fmSender, fmType, fmText
    与 writefmmsg2txtandmaybeevernotetoo 原手工拼接逐字节一致。
    """
    return "\t".join(
        [
            fmmsg["fmTime"],
            str(fmmsg["fmSend"]),
            fmmsg["fmSender"],
            fmmsg["fmType"],
            fmmsg["fmText"],
        ]
    )


# %% [markdown]
# ## parse_tsv(line: str) -> dict | None

# %%
def parse_tsv(line: str) -> dict | None:
    """从 TSV 行反序列化为 dict，解析失败返回 None。

    maxsplit=4 保证第 5 字段（content）中的 tab 字符不被截断。
    """
    parts = line.strip().split("\t", maxsplit=4)
    if len(parts) != 5:
        return None
    return {
        "fmTime": parts[0],
        "fmSend": parts[1] == "True",
        "fmSender": parts[2],
        "fmType": parts[3],
        "fmText": parts[4],
    }
