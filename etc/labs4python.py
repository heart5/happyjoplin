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
# # python内置函数和各种功能测试

# %% [markdown]
# ## 引入库

# %%
import pathmagic

with pathmagic.context():
    from func.sysfunc import not_IPython


# %% [markdown]
# ## 函数功能集

# %% [markdown]
# ### testtpdict()


# %%
def testtpdict():
    """
    测试dict的用法，包括get、默认键值列表等
    """
    tpdict = {"Picture": "img", "Video": "vid", "Recording": "fil", "Attachment": "fil"}

    fmtype = "Picture"
    if (prefix := tpdict.get(fmtype)) is not None:
        print(f"It's maybe a {fmtype} file to send.")
    else:
        print("It's a Text message.")


# %% [markdown]
# ## 主函数main()

# %%
if __name__ == "__main__":
    if not_IPython():
        log.info(f"运行文件\t{__file__}")

    testtpdict()
    if not_IPython():
        log.info(f"{__file__}\t运行结束！")
