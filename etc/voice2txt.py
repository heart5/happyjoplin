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
# # 中文语音识别

# %% [markdown]
# ## 引入重要库

# %%
import json
import os
import sqlite3 as lite
import wave
from datetime import datetime as _datetime

import pandas as pd

# import vosk
# from pydub import AudioSegment
# %%
import pathmagic

with pathmagic.context():

    from func.first import getdirmain
    from func.getid import getdevicename
    from func.litetools import ifnotcreate
    from func.logme import log
    from func.sysfunc import execcmd, not_IPython
    from func.wrapfuncs import timethis


# %% [markdown]
# ## 核心函数

# %% [markdown]
# ### v2t_vosk(vfile)


# %%
@timethis
def v2t_vosk(vfile, quick=False):
    # 加载 Vosk 模型
    if quick:
        model = vosk.Model("/opt/vosk/vosk-model-small-cn-0.22")
    else:
        model = vosk.Model("/opt/vosk/vosk-model-cn-0.22")

    # 打开音频文件
    wav_file = vfile.replace(".mp3", ".wav")
    audio = AudioSegment.from_mp3(vfile)
    audio.export(wav_file, format="wav")
    wf = wave.open(wav_file, "rb")

    # 创建识别器实例
    rec = vosk.KaldiRecognizer(model, wf.getframerate())

    # 读取音频数据并进行识别
    while True:
        data = wf.readframes(4000)
        if len(data) == 0:
            break
        if rec.AcceptWaveform(data):
            result = json.loads(rec.Result())
            print("转换后的文本：", result["text"])

    # 获取最终结果
    final_result = json.loads(rec.FinalResult())
    print("最终转换后的文本：", final_result["text"])

    wf.close()
    os.remove(wav_file)

    return final_result


# %% [markdown]
# ### v2t_funasr(vfilelst)


# %%
@timethis
def v2t_funasr(vfilelst):
    from funasr import AutoModel
    from funasr.utils.postprocess_utils import rich_transcription_postprocess

    # 加载 SenseVoice 模型
    model_dir = "iic/SenseVoiceSmall"
    model = AutoModel(
        model=model_dir,
        trust_remote_code=True,
        remote_code="./model.py",
        vad_model="fsmn-vad",
        vad_kwargs={"max_single_segment_time": 30000},
        device="cuda:0",
        disable_update=True,  # 禁用更新检查
    )

    txtlst = list()
    for vfile in vfilelst:
        log.info(f"【{vfilelst.index(vfile)}/{len(vfilelst)}】\t{vfile}")
        try:
            # 转换音频文件为文本
            res = model.generate(
                input=vfile,
                cache={},
                # language="auto",
                language="cn",
                use_itn=True,
                batch_size_s=30,
                merge_vad=True,
                merge_length_s=10,
            )

            # 处理转换后的文本
            text = rich_transcription_postprocess(res[0]["text"])
        except Exception as E:
            text = f"语音转换失败：{E}"
        log.info(f"{text}")
        txtlst.append("【funasr】" + text)
    return txtlst


# %% [markdown]
# ### v2t_ollama(filepath, account, msg_time, sender, voice_url)


# %%
@timethis
def v2t_ollama(filepath, account, msg_time, sender, voice_url="https://ollama.strcoder.com/voice"):
    """单文件语音转文字，POST 到 hcx voice API。

    转录后服务端自动写入 v4txt_v2，客户端无需缓存。
    返回 text 字符串，失败时返回以"语音转换失败："开头的错误文本。
    """
    import requests

    log.info(f"转录: {filepath}")
    try:
        with open(filepath, "rb") as fh:
            resp = requests.post(
                f"{voice_url}/transcribe",
                files={"file": (os.path.basename(filepath), fh)},
                data={"account": account, "msg_time": msg_time, "sender": sender},
                timeout=120,
            )
        if resp.ok:
            data = resp.json()
            text = data.get("text", "")
            lang = data.get("language", "?")
            prob = data.get("probability", 0)
            log.info(f"  → {text[:50]}... (lang={lang}, p={prob:.2f})")
            return text
        else:
            log.error(f"  → HTTP {resp.status_code}")
            return f"语音转换失败：HTTP {resp.status_code}"
    except Exception as e:
        log.error(f"  → {e}")
        return f"语音转换失败：{e}"


# %% [markdown]
# ### apply_transcription(df, account)


# %%
def apply_transcription(df, account):
    """对 DataFrame 中的 Recording 行关联 v4txt_v2 转录结果。

    输入：wc_{账号} 表的 DataFrame（含 time, sender, type, content 列）+ 账号名
    输出：同一 DataFrame，但命中转录的行 type 改为 'VoiceText'，content 改为转录文字。
          未命中保持原样。

    hcx 本机直连 voice_transcriptions.db，非 hcx 通过 voice API 批量查询。
    """
    recording_mask = df["type"] == "Recording"
    if not recording_mask.any():
        return df

    # 收集需要查询的 (time, sender) 对，time 统一转为 unix 时间戳字符串
    recs = df.loc[recording_mask, ["time", "sender"]].drop_duplicates()
    records = [(_normalize_time(t), s) for t, s in zip(recs["time"], recs["sender"])]
    records = [(t, s) for t, s in records if t]

    if not records:
        return df

    # 尝试本地查询，失败则走 API
    hits = _query_v4txt_v2_local(account, records)

    # 改造命中行
    df_time_norm = df["time"].apply(_normalize_time)
    for (msg_time, sender), text in hits.items():
        mask = recording_mask & (df_time_norm == msg_time) & (df["sender"] == sender)
        df.loc[mask, "type"] = "VoiceText"
        df.loc[mask, "content"] = text

    log.info(f"apply_transcription: {len(records)} 条录音, 命中 {len(hits)} 条")
    return df


def _normalize_time(val):
    """将各种时间值统一转为 unix 时间戳字符串，用于匹配 v4txt_v2.msg_time。"""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    if isinstance(val, (int, float)):
        return str(int(val))
    # datetime / Timestamp
    try:
        return str(int(val.timestamp()))
    except Exception:
        pass
    # 字符串：优先当数字时间戳，再尝试常见日期格式
    if isinstance(val, str):
        try:
            return str(int(float(val)))
        except (ValueError, OverflowError):
            pass
        for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"]:
            try:
                return str(int(_datetime.strptime(val[:19], fmt).timestamp()))
            except (ValueError, OverflowError):
                continue
    return str(val)


def _query_v4txt_v2_local(account, records):
    """本地查询 v4txt_v2（hcx 上直连数据库）。

    records 中 time 已由 _normalize_time 转为 unix 时间戳字符串。
    """
    db_path = "/data/codebase/joplinai/data/voice_transcriptions.db"
    if not os.path.exists(db_path):
        log.warning("voice_transcriptions.db 不存在，跳过转录关联")
        return {}
    try:
        conn = lite.connect(db_path)
        hits = {}
        for msg_time, sender in records:
            row = conn.execute(
                "SELECT text FROM v4txt_v2 WHERE account=? AND msg_time=? AND sender=?",
                (account, msg_time, sender),
            ).fetchone()
            if row:
                hits[(msg_time, sender)] = row[0]
        conn.close()
        return hits
    except Exception as e:
        log.error(f"查询 v4txt_v2 失败: {e}")
        return {}


# %% [markdown]
# ### clean_transcribed_mp3(db_path, dry_run)


# %%
def clean_transcribed_mp3(db_path, dry_run=True):
    """扫描已转录的 mp3 文件并清理。

    1. 扫描 wc_{账号} 表中 type=Recording 的记录
    2. 按 (time, sender) 查 v4txt_v2
    3. 命中 → mp3 文件存在 → 删除
    4. dry_run=True 时只报告，不删除

    返回: {"scanned": N, "hit": N, "deleted": N, "missing": N, "freed_mb": N}
    """
    v4_db = "/data/codebase/joplinai/data/voice_transcriptions.db"
    if not os.path.exists(v4_db):
        return {"error": "voice_transcriptions.db 不存在"}

    conn = lite.connect(db_path)
    v4conn = lite.connect(v4_db)
    tables = [
        t[0]
        for t in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'wc_%'"
        ).fetchall()
    ]

    proot = str(getdirmain())
    scanned, hit, deleted, missing, freed = 0, 0, 0, 0, 0

    for table in tables:
        account = table.replace("wc_", "")
        rows = conn.execute(
            f"SELECT time, sender, content FROM [{table}] WHERE type='Recording'"
        ).fetchall()
        for msg_time, sender, content in rows:
            if not content or not content.endswith(".mp3"):
                continue
            scanned += 1
            # 查 v4txt_v2
            row = v4conn.execute(
                "SELECT id, cleaned FROM v4txt_v2 WHERE account=? AND msg_time=? AND sender=?",
                (account, str(msg_time), sender),
            ).fetchone()
            if not row:
                continue
            hit += 1
            v4id, cleaned = row
            if cleaned:
                continue
            # 构建文件路径
            fpath = os.path.join(proot, content) if not content.startswith("/") else content
            if os.path.exists(fpath):
                fsize_mb = os.path.getsize(fpath) / (1024 * 1024)
                if not dry_run:
                    os.remove(fpath)
                    v4conn.execute("UPDATE v4txt_v2 SET cleaned=1 WHERE id=?", (v4id,))
                    v4conn.commit()
                deleted += 1
                freed += fsize_mb
            else:
                missing += 1
                # 文件已不存在，标记已清理
                if not dry_run:
                    v4conn.execute("UPDATE v4txt_v2 SET cleaned=1 WHERE id=?", (v4id,))
                    v4conn.commit()

    conn.close()
    v4conn.close()

    result = {"scanned": scanned, "hit": hit, "deleted": deleted, "missing": missing, "freed_mb": round(freed, 1)}
    if dry_run:
        log.info(f"[DRY-RUN] 扫描{scanned}条, 命中{hit}, 可删{deleted}个文件({result['freed_mb']}MB), 已缺失{missing}")
    else:
        log.info(f"清理完成: 删{deleted}个文件({result['freed_mb']}MB), 已缺失{missing}")
    return result


# %% [markdown]
# ### v4txt(vfile, dbn)


# %%
def v4txt(vfile, dbn):
    """根据传入的文件路径在数据表中查询结果，如果不存在则执行语音转换并存入数据表
    """
    # 检查v4txt数据表是否已经存在，不存在则构建之
    createsql = "CREATE TABLE v4txt ( id INTEGER PRIMARY KEY AUTOINCREMENT, filepath TEXT NOT NULL UNIQUE, text TEXT NOT NULL );"
    ifnotcreate("v4txt", createsql, dbn)
    # 连接到 sqlite3 数据库
    conn = lite.connect(dbn)
    cursor = conn.cursor()

    # 查找文件路径对应的文本
    cursor.execute("SELECT text FROM v4txt WHERE filepath = ?", (vfile,))
    result = cursor.fetchone()

    if result:
        # 如果找到相应的文本，则返回该文本
        text = result[0]
    else:
        # 如果找不到，则调用 v2t_funasr 函数执行转换
        text = v2t_funasr([vfile])[0]

        # 将转换后的文本存入 v4txt 数据表
        cursor.execute(
            "INSERT INTO v4txt (filepath, text) VALUES (?, ?)", (vfile, text)
        )
        conn.commit()

    # 关闭数据库连接
    conn.close()

    return text


# %% [markdown]
# ### batch_v4txt(vfilelst, dbn)


# %%
@timethis
def batch_v4txt(vfilelst, dbn, batch_size=100):
    """批量转换文件路径列表并存入数据库
    """
    # 检查v4txt数据表是否已经存在，不存在则构建之
    createsql = "CREATE TABLE IF NOT EXISTS v4txt (id INTEGER PRIMARY KEY AUTOINCREMENT, filepath TEXT NOT NULL UNIQUE, text TEXT NOT NULL);"
    ifnotcreate("v4txt", createsql, dbn)

    # 连接到 sqlite3 数据库
    conn = lite.connect(dbn)
    cursor = conn.cursor()

    # 查找文件路径对应的文本
    cursor.execute("SELECT filepath FROM v4txt")
    existing_files = {row[0] for row in cursor.fetchall()}

    # 过滤出需要转换的文件
    files_to_convert = [vfile for vfile in vfilelst if vfile not in existing_files]

    # 分批处理
    for i in range(0, len(files_to_convert), batch_size):
        log.info(f"【{i}/{len(files_to_convert)}】\t…………………………")
        batch = files_to_convert[i : i + batch_size]
        if batch:
            # 调用 v2t_funasr 函数执行转换
            texts = v2t_funasr(batch)

            # 将转换后的文本存入 v4txt 数据表
            for vfile, text in zip(batch, texts):
                cursor.execute(
                    "INSERT  OR REPLACE INTO v4txt (filepath, text) VALUES (?, ?)",
                    (vfile, text),
                )
            conn.commit()

    # 关闭数据库连接
    conn.close()


# %% [markdown]
# ### query_v4txt(vfile, dbn)


# %%
def query_v4txt(vfile, dbn):
    """根据传入的文件路径在数据表中查询结果"""
    # 连接到 sqlite3 数据库
    conn = lite.connect(dbn)
    cursor = conn.cursor()

    # 查找文件路径对应的文本
    cursor.execute("SELECT text FROM v4txt WHERE filepath = ?", (vfile,))
    result = cursor.fetchone()

    # 关闭数据库连接
    conn.close()
    if result:
        # 如果找到相应的文本，则返回该文本
        return result[0]
    else:
        # 如果找不到，则返回 vfile
        return vfile


# %% [markdown]
# ## main，主函数

# %%
if __name__ == "__main__":
    if not_IPython():
        log.info(f"运行文件\t{__file__}")
    loginstr = (
        "" if (whoami := execcmd("whoami")) and (len(whoami) == 0) else f"{whoami}"
    )
    dbfilename = f"wcitemsall_({getdevicename()})_({loginstr}).db".replace(" ", "_")
    wcdatapath = getdirmain() / "data" / "webchat"
    dbname = os.path.abspath(wcdatapath / dbfilename)
    # vfile = str(getdirmain()) + "/img/webchat/20241108/蒲苇_241108-213337.mp3"
    # outtxt = v2t_vosk(vfile)
    # outtxt = v2t_vosk(vfile, quick=True)
    vfilelst = [
        "/home/baiyefeng/codebase/happyjoplin/img/webchat/20241108/蒲苇_241108-092025.mp3",
        "/home/baiyefeng/codebase/happyjoplin/img/webchat/20241108/蒲苇_241109-011903.mp3",
        "/home/baiyefeng/codebase/happyjoplin/img/webchat/20241108/蒲苇_241108-213337.mp3",
        "/home/baiyefeng/codebase/happyjoplin/img/webchat/20241108/蒲苇_241109-011903.mp3",
        "/home/baiyefeng/codebase/happyjoplin/img/webchat/20241108/蒲苇_241108-213452.mp3",
    ]
    vfilelst_filter = [vfile for vfile in vfilelst if os.path.exists(vfile)]
    print(vfilelst_filter)
    # batch_v4txt(vfilelst_filter, dbname)
    for file in vfilelst_filter:
        print(f"{file}\t{query_v4txt(file, dbname)}")

    if not_IPython():
        log.info(f"文件\t{__file__}\t运行结束。")
