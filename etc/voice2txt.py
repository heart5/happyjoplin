# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
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
import sys
import os
import json
import wave
import sqlite3 as lite
# import vosk
# from pydub import AudioSegment

# %%
import pathmagic
with pathmagic.context():
    from func.first import getdirmain, touchfilepath2depth
    from func.logme import log
    from etc.getid import getdevicename
    from func.wrapfuncs import timethis
    from func.sysfunc import not_IPython, execcmd
    from func.configpr import setcfpoptionvalue, getcfpoptionvalue
    from func.litetools import ifnotcreate, showtablesindb, convert_intstr_datetime
    from func.jpfuncs import getapi, getinivaluefromcloud, searchnotes, \
        searchnotebook, createnote, getreslst, updatenote_body, updatenote_title, \
        getnote
    from filedatafunc import getfilemtime as getfltime
    from life.wc2note import items2df


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
    wav_file = vfile.replace('.mp3', '.wav')
    audio = AudioSegment.from_mp3(vfile)
    audio.export(wav_file, format='wav')
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
        disable_update=True # 禁用更新检查
    )

    txtlst = list()
    for vfile in vfilelst:
        log.info(f"【{vfilelst.index(vfile)}/{len(vfilelst)}】\t{vfile}")
        try:
            # 转换音频文件为文本
            res = model.generate(
                input = vfile,
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
        txtlst.append('【funasr】' + text)
    return txtlst


# %% [markdown]
# ### v4txt(vfile, dbn)

# %%
def v4txt(vfile, dbn): 
    """
    根据传入的文件路径在数据表中查询结果，如果不存在则执行语音转换并存入数据表
    """
    # 检查v4txt数据表是否已经存在，不存在则构建之
    createsql = "CREATE TABLE v4txt ( id INTEGER PRIMARY KEY AUTOINCREMENT, filepath TEXT NOT NULL UNIQUE, text TEXT NOT NULL );"
    ifnotcreate('v4txt', createsql, dbn)
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
        cursor.execute("INSERT INTO v4txt (filepath, text) VALUES (?, ?)", (vfile, text)) 
        conn.commit() 
        
    # 关闭数据库连接 
    conn.close() 
    
    return text


# %% [markdown]
# ### batch_v4txt(vfilelst, dbn)

# %%
@timethis
def batch_v4txt(vfilelst, dbn, batch_size=100):
    """
    批量转换文件路径列表并存入数据库
    """
    # 检查v4txt数据表是否已经存在，不存在则构建之
    createsql = "CREATE TABLE IF NOT EXISTS v4txt (id INTEGER PRIMARY KEY AUTOINCREMENT, filepath TEXT NOT NULL UNIQUE, text TEXT NOT NULL);"
    ifnotcreate('v4txt', createsql, dbn)

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
        batch = files_to_convert[i:i + batch_size]
        if batch:
            # 调用 v2t_funasr 函数执行转换
            texts = v2t_funasr(batch)

            # 将转换后的文本存入 v4txt 数据表
            for vfile, text in zip(batch, texts):
                cursor.execute("INSERT  OR REPLACE INTO v4txt (filepath, text) VALUES (?, ?)", (vfile, text))
            conn.commit()

    # 关闭数据库连接
    conn.close()


# %% [markdown]
# ### query_v4txt(vfile, dbn)

# %%
def query_v4txt(vfile, dbn):
    """ 根据传入的文件路径在数据表中查询结果 """
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
if __name__ == '__main__':
    if not_IPython():
        log.info(f'运行文件\t{__file__}')
    loginstr = "" if (whoami := execcmd("whoami")) and (len(whoami) == 0) else f"{whoami}"
    dbfilename = f"wcitemsall_({getdevicename()})_({loginstr}).db".replace(" ", "_")
    wcdatapath = getdirmain() / "data" / "webchat"
    dbname = os.path.abspath(wcdatapath / dbfilename)
    # vfile = str(getdirmain()) + "/img/webchat/20241108/蒲苇_241108-213337.mp3"
    # outtxt = v2t_vosk(vfile)
    # outtxt = v2t_vosk(vfile, quick=True)
    vfilelst = ['/home/baiyefeng/codebase/happyjoplin/img/webchat/20241108/蒲苇_241108-092025.mp3', '/home/baiyefeng/codebase/happyjoplin/img/webchat/20241108/蒲苇_241109-011903.mp3', '/home/baiyefeng/codebase/happyjoplin/img/webchat/20241108/蒲苇_241108-213337.mp3', '/home/baiyefeng/codebase/happyjoplin/img/webchat/20241108/蒲苇_241109-011903.mp3', '/home/baiyefeng/codebase/happyjoplin/img/webchat/20241108/蒲苇_241108-213452.mp3']
    vfilelst_filter = [vfile for vfile in vfilelst if os.path.exists(vfile)]
    print(vfilelst_filter)
    # batch_v4txt(vfilelst_filter, dbname)
    for file in vfilelst_filter:
        print(f"{file}\t{query_v4txt(file, dbname)}")

    if not_IPython():
        log.info(f"文件\t{__file__}\t运行结束。")
