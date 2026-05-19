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
from pathlib import Path

# %%
import pathmagic

with pathmagic.context():
    from func.getid import getdeviceid, getdevicename, gethostuser
    from func.configpr import getcfp, getcfpoptionvalue, setcfpoptionvalue
    from func.datatools import getkeysfromcloud
    from func.first import dirmainpath, getdirmain
    from func.jpfuncs import (
        createnote,
        getinivaluefromcloud,
        getnote,
        jpapi,
        searchnotebook,
        searchnotes,
        updatenote_body,
        updatenote_title,
    )

# %%
getkeysfromcloud()

# %%
notes = searchnotes("四件套")
for note in notes:
    print(note.id, note.title)

# %%
starline = "**123123**"
# print(starline.trim("*", ""))
starline.strip("*")

# %%
cfpfromcloud, cpath = getcfp("happyjpinifromcloud")

# %%
cfpfromcloud.has_section("device")

# %%
cfpfromcloud.sections()


# %%
def findvaluebykeyinsection(cfpname, optionname, value):
    cfp, cfppath = getcfp(cfpname)
    for option in cfp.options(optionname):
        findvalue = cfp.get(optionname, option)
        if findvalue == value:
            print(f"在【{optionname}】中找到value为{value}的键值对，key为{option}")
            return option


# %%
findvaluebykeyinsection("happyjpinifromcloud", "device", "Pixel 6 Pro")

# %%
import os

os.environ["OLLAMA_HOST"] = " http://localhost:11434 "

# %%
import os

# %%
oh = os.getenv("OLLAMA_HOST")
print(oh)

# %%
from func.first import getdirmain

# %%
print(getdirmain())
