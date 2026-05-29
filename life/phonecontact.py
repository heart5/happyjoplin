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
# # 手机通讯录

# %%
"""通过Termux API获取手机通讯录并渲染为图片，配合webchat_commands中「真元宝 联系人」命令使用"""

# %%
import pathmagic

with pathmagic.context():
    from func.logme import log
    from func.pdtools import db2img

import json

import pandas as pd

# %% [markdown]
# ## showphoneinfoimg() — 获取通讯录并渲染图片

# %%
def showphoneinfoimg():
    """通过Termux API获取手机通讯录，渲染为图片。

    Returns:
        Path: 图片文件路径，供 itchat.send_image 使用
    """
    from func.termuxtools import termux_contact_list

    raw = termux_contact_list()
    contacts = json.loads(raw)
    log.info(f"phonecontact: 获取到 {len(contacts)} 个联系人")

    if not contacts:
        return _make_text_img("手机通讯录为空")

    df = pd.DataFrame(contacts)
    # termux-contact-list 返回的常见字段：name, number
    keep_cols = [c for c in ["name", "number"] if c in df.columns]
    sdf = df[keep_cols].copy()
    sdf.rename(columns={"name": "姓名", "number": "电话"}, inplace=True)

    return db2img(sdf, title="手机通讯录", fontsize=14)


# %%
def _make_text_img(text):
    """纯文本渲染为图片，用于错误/空数据提示。"""
    return db2img(
        pd.DataFrame([text], columns=[""]),
        title="手机通讯录",
        fontsize=14,
    )


# %% [markdown]
# ## 主函数

# %%
if __name__ == "__main__":
    imgpath = showphoneinfoimg()
    print(f"图片路径: {imgpath}")
