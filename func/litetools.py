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
# # sqlite3数据库相关函数

# %%
"""
sqlite数据库相关应用函数
"""

# %% [markdown]
# ## 引入重要库

# %%
import os
import re
import arrow
import sqlite3 as lite
import pandas as pd

# %%
import pathmagic

with pathmagic.context():
    from func.first import getdirmain, touchfilepath2depth
    from func.logme import log
    from etc.getid import getdevicename
    from func.first import dbpathquandan, dbpathworkplan, dbpathdingdanmingxi
    from func.configpr import getcfpoptionvalue, setcfpoptionvalue
    from func.sysfunc import not_IPython, execcmd
    from func.wrapfuncs import timethis


# %% [markdown]
# ## 功能函数集

# %% [markdown]
# ### def get_filesize(filepath)


# %%
def get_filesize(filepath):
    fsize = os.path.getsize(filepath)
    fsize = fsize / float(1024 * 1024)
    return round(fsize, 2)


# %% [markdown]
# ### def istableindb(tablein, dbname)


# %%
def istableindb(tablenin: str, dbname: str):
    result = False
    try:
        conn = lite.connect(dbname)
        cursor = conn.cursor()
        cursor.execute("select * from sqlite_master where type='table'")
        table = cursor.fetchall()
        # print(table)
        chali = [x for item in table for x in item[1:3]]
        # print(chali)
        result = tablenin in chali
    except Exception as eee:
        log.critical(f"查询数据表是否存在时出错。{eee}")
    finally:
        if "conn" in locals():
            conn.close()

    return result


# %% [markdown]
# ### def ifnotcreate(tablein, createsql, dbn)


# %%
def ifnotcreate(tablen: str, createsql: str, dbn: str):
    """
    如果没有相应数据表就创建一个
    :param tablen:
    :param dbn:
    :return:
    """

    if istableindb(tablen, dbn):
        return

    try:
        conn = lite.connect(dbn)
        cursor = conn.cursor()

        cursor.execute(createsql)
        conn.commit()
        log.info(f"数据表：\t{tablen} 被创建成功。\t{createsql}")

    except Exception as eee:
        log.critical(f"操作数据库时出现错误。{dbn}\t{eee}")
    finally:
        if "conn" in locals():
            conn.close()


# %% [markdown]
# ### def ifclexists(dbin, tb, cl)


# %%
def ifclexists(dbin, tb, cl):
    conn = lite.connect(dbin)
    cursor = conn.cursor()
    structsql = f"SELECT sql FROM sqlite_master WHERE type = 'table' AND tbl_name = '{tb}';"
    tablefd = cursor.execute(structsql).fetchall()
    # [('CREATE TABLE heart5 (id integer primary key autoincrement, name text, age int, imguuid text)',)]
    conn.commit()
    tcs = conn.total_changes
    print(tcs)
    conn.close()

    if len(tablefd) == 0:
        print(f"数据表{tb}不存在")
        return False

    createsql = [name for x in tablefd for name in x][0]
    print(createsql)
    ptn = re.compile(r"\((.+)\)")
    print(re.findall(ptn, createsql)[0])
    rstsplst = re.findall(ptn, createsql)[0].split(",")
    print([x.strip() for x in rstsplst])
    finallst = [x.strip().split()[:2] for x in rstsplst]
    print(finallst)
    targetdict = dict(finallst)
    if cl in targetdict:
        print(f"列{cl}已经在数据表{tb}中存在")
        return True
    else:
        print(f"列{cl}在数据表{tb}中尚未存在，可以新增")
        return False


# %% [markdown]
# ### def shwotableindb(dbname)


# %%
def showtablesindb(dbname: str):
    conn = lite.connect(dbname)
    cursor = conn.cursor()
    tbnsql = f"SELECT tbl_name FROM sqlite_master WHERE type = 'table';"
    tablefd = cursor.execute(tbnsql).fetchall()
    #     print(tablefd)
    tablesnamelst = [name for x in tablefd for name in x]
    print(tablesnamelst)
    for tname in tablesnamelst:
        structsql = f"SELECT sql FROM sqlite_master WHERE type = 'table' AND tbl_name = '{tname}';"
        tablefd = cursor.execute(structsql).fetchall()
        print(tname + ":\t", [name for x in tablefd for name in x][0])
    conn.commit()
    tcs = conn.total_changes
    print(tcs)
    conn.close()


# %% [markdown]
# ### def droptablefromdb(dbname, tablename, confirm=False)


# %%
def droptablefromdb(dbname: str, tablename: str, confirm=False):
    if not confirm:
        logstr = f"【警告】：数据表{tablename}将从{dbname}中删除，请确认！！！"
        log.critical(logstr)
    else:
        conn = lite.connect(dbname)
        cursor = conn.cursor()
        cursor.execute(f"drop table {tablename}")
        # cursor.execute(f'drop table cinfo')
        conn.commit()
        logstr = f"【警告】：数据表{tablename}已经从{dbname}中删除，谨以记！！！"
        log.critical(logstr)

        conn.close()


# %% [markdown]
# ### def checktableindb(ininame, dbpath, tablename, creattablesql, confirm=False)


# %%
def checktableindb(ininame: str, dbpath: str, tablename: str, creattablesql: str, confirm=False):
    """
    检查数据表（ini登记，物理存储）是否存在并根据情况创建
    """
    absdbpath = os.path.abspath(dbpath)  # 取得绝对路径，用于作为section名称
    if not (ifcreated := getcfpoptionvalue(ininame, absdbpath, tablename)):
        print(ifcreated)
        if istableindb(tablename, dbpath) and confirm:
            # 删表操作，危险，谨慎操作
            droptablefromdb(dbpath, tablename, confirm=confirm)
            logstr = f"数据表{tablename}于{dbpath}中被删除"
            log.critical(logstr)
        ifnotcreate(tablename, creattablesql, dbpath)

        setcfpoptionvalue(ininame, absdbpath, tablename, str(True))


# %% [markdown]
# ### def convert_intstr_datetime(value)


# %%
def convert_intstr_datetime(value):
    """
    将时间值转换为 datetime ，支持字符串和整数(timestamp)格式。
    """
    if pd.isna(value):
        return None  # 如果值是 NaN，返回 None
    if isinstance(value, str):
        try:
            # 将字符串转换为 datetime 对象
            return arrow.get(value).datetime
        except Exception:
            return None  # 如果转换失败，返回 None
    elif isinstance(value, (int, float)):
        return arrow.get(value).to("Asia/Shanghai").datetime
    else:
        return None  # 对于其他类型返回 None


# %% [markdown]
# ### clean4timecl(name, dbname, confirm)


# %%
@timethis
def clean4timecl(name, dbname, confirm):
    with lite.connect(dbname) as conn:
        sql = f"select * from wc_{name}"
        df = pd.read_sql(sql, conn)

    # 调用函数转换为datetime
    df["time"] = df["time"].apply(convert_intstr_datetime)
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df1 = df[~df.time.isnull()]
    df2 = df1.set_index("id")

    # 处理成相对路径，逻辑是准备把所有音频等文件集中到主运行环境
    ptn = re.compile(r"^/.+happyjoplin/")
    df2.loc[:, "content"] = df2["content"].apply(
        lambda x: re.sub(ptn, "", x) if isinstance(x, str) and ptn.match(x) else x
    )

    outdf = df2.drop_duplicates()
    outdf = outdf.sort_values("time")
    log.info(
        f"从数据库【{dbname}】的数据表《wc_{name}》中读出记录总数{df.shape[0]}条，去掉(time经过转换后为空的)后还有{df1.shape[0]}条，去重(文件路径已经转换为相对路径)后还有{outdf.shape[0]}条"
    )

    if confirm == "yes":
        log.critical(
            f"重大操作，向数据库【{dbname}】的数据表《wc_{name}》写回大量数据，原数据将被覆盖，但是表结构保持不变！！！"
        )
        with lite.connect(dbname) as conn:
            cursor = conn.cursor()
            # 创建一个新的临时表，结构与原数据表相同
            cursor.execute(f"CREATE TABLE wc_{name}_temp AS SELECT * FROM wc_{name} WHERE 1=0")
            # outdf覆盖导出到临时数据表
            outdf.to_sql(f"wc_{name}_temp", conn, if_exists="append", index=False)
            # 清空原数据表
            cursor.execute(f"DELETE FROM wc_{name}")
            # 将临时表的数据插入原数据表
            cursor.execute(
                f""" INSERT INTO wc_{name} (time, send, sender, type, content) SELECT time, send, sender, type, content FROM wc_{name}_temp """
            )
            # 删除临时表
            cursor.execute(f"DROP TABLE wc_{name}_temp")
            # 提交更改并关闭数据库连接
            conn.commit()
        log.critical(
            f"数据库【{dbname}】的数据表《wc_{name}》数据清洗完成后有{outdf.shape[0]}条记录，并已成功覆盖回去，表结构保持了不变！！！"
        )
        compact_sqlite3_db(dbname)

    return outdf


# %% [markdown]
# ### def compact_sqlite3_db(dbpath)


# %%
@timethis
def compact_sqlite3_db(dbpath):
    sizebefore = get_filesize(dbpath)
    conn = lite.connect(dbpath)
    conn.execute("VACUUM")
    conn.close()
    log.info(f"{dbpath}数据库压缩前大小为{sizebefore}MB，压缩之后为{get_filesize(dbpath)}MB。")


# %% [markdown]
# ## 主函数main

# %%
if __name__ == "__main__":
    if not_IPython():
        logstr = f"运行文件\t{__file__}\t……"
        log.info(logstr)
    # print(get_filesize(dbpathquandan))
    loginstr = "" if (whoami := execcmd("whoami")) and (len(whoami) == 0) else f"{whoami}"
    dbfilename = f"wcitemsall_({getdevicename()})_({loginstr}).db".replace(" ", "_")
    wcdatapath = getdirmain() / "data" / "webchat"
    dbname = os.path.abspath(wcdatapath / dbfilename)
    name = "heart5"
    outdf = clean4timecl(name, dbname, "no")

    if not_IPython():
        logstr = f"文件\t{__file__}\t运行完毕。"
        log.info(logstr)
