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
# # 微信聊天记录分析工具

# %% [markdown]
# ## 库导入

# %%
# wechat_analysis.py
import os
import re
import arrow
import sqlite3 as lite
import pandas as pd
from pathlib import Path
from collections import Counter
from datetime import datetime

# %%
import pathmagic
with pathmagic.context():
    from func.first import getdirmain, touchfilepath2depth
    from func.logme import log
    from etc.getid import getdevicename
    from func.wrapfuncs import timethis
    from func.sysfunc import not_IPython, execcmd
    from func.configpr import setcfpoptionvalue, getcfpoptionvalue
    from func.litetools import ifnotcreate, showtablesindb, compact_sqlite3_db, convert_intstr_datetime, clean4timecl
    from func.jpfuncs import getapi, getinivaluefromcloud, searchnotes, \
        searchnotebook, createnote, getreslst, updatenote_body, updatenote_title, \
        getnote
    from filedatafunc import getfilemtime as getfltime
    from life.wc2note import items2df
    from etc.voice2txt import query_v4txt, batch_v4txt


# %% [markdown]
# ## 功能函数集

# %% [markdown]
# ### WeChatAnalysis(class)

# %%
class WeChatAnalysis:

    def __init__(self, db_path, name, chunk_size=800000):
        self.db_path = db_path
        self.name = name
        self.conn = lite.connect(self.db_path)
        self.chunk_size = chunk_size

    def process_chunk(self, indf):
        # 丢掉那些time字段为空的记录
        df = indf.dropna(subset=['time'])
        # 处理content的时间差前缀
        df.loc[:, 'content'] = df['content'].apply(lambda x:re.sub(r"(\[\w+前\]|\[刚才\])?", "", x) if x is not None else x)
        
        # 处理多运行平台重复记录的图片或文件路径
        main_path = str(getdirmain().resolve())
        # 处理成相对路径，逻辑是准备把所有音频文件集中到主运行环境
        ptn = re.compile(r"^/.+happyjoplin/")
        df.loc[:, 'content'] = df['content'].apply(lambda x: re.sub(ptn, '', x) if isinstance(x, str) and ptn.match(x) else x)
        # 处理文件的相对路径，处理成绝对路径方便判断文件是否在本环境存在
        # df.loc[:, 'content'] = df['content'].apply(lambda x: str(getdirmain().resolve() / x) if ((x is not None) and x.startswith('img/webchat')) else x)
        
        # 过滤掉包含路径但不是以 main_path 开头的记录 
        # mydf = df[(df['content'].str.startswith(main_path) & df['content'].str.contains(r'^/', regex=True)) | ~df['content'].str.contains(r'^/', regex=True)]

        outdf = df.drop_duplicates(subset=['time', 'send', 'sender', 'type', 'content'])
        if outdf.shape[0] == 0:
            return None
        # 处理time字段存在int和str两种数据类型的可能
        outdf.loc[:, 'time'] = outdf['time'].apply(convert_intstr_datetime)
        # 处理录音的音频文件，转换为文字输出
        outdf.loc[:, 'content'] = outdf['content'].apply(lambda x: query_v4txt(x, self.db_path) if isinstance(x, str) and x.endswith('.mp3') and os.path.exists(x) else x)
        # 重新设置index，用读取的id列
        outdf.set_index('id', inplace=True)
        log.info(f"传入的DF数据有【{indf.shape[0]}】条，去除time为空后数据为【{df.shape[0]}】条，多重字段去重后还有【{outdf.shape[0]}】条")

        return outdf
        
    def load_data_raw(self):
        """从数据库加载RAW数据"""
        query = f"SELECT * FROM wc_{self.name}; "  # 假设表名为 wc
        # self.data = pd.read_sql(query, self.conn, parse_dates=['time'])
        alldf = pd.read_sql(query, self.conn)
        self.data_raw = alldf

        
    def load_data_mp3(self):
        """从数据库加载mp3数据"""
        query = f"SELECT * FROM wc_{self.name} WHERE content LIKE '%.mp3'; "  # 假设表名为 wc
        # self.data = pd.read_sql(query, self.conn, parse_dates=['time'])
        df = pd.read_sql(query, self.conn)
        # 处理多运行平台重复记录的图片或文件路径
        main_path = str(getdirmain().resolve())
        # 处理成相对路径，逻辑是准备把所有音频文件集中到主运行环境
        ptn = re.compile(r"^/.+happyjoplin/")
        df.loc[:, 'content'] = df['content'].apply(lambda x: re.sub(ptn, '', x) if isinstance(x, str) and ptn.match(x) else x)
        # 处理文件的相对路径，处理成绝对路径方便判断文件是否在本环境存在
        df.loc[:, 'content'] = df['content'].apply(lambda x: str(getdirmain().resolve() / x) if isinstance(x, str) and x.startswith('img/webchat') else x)
        outdf = df[df['content'].apply(lambda x: os.path.exists(x))]
        mp3s = outdf['content'].unique().tolist()
        log.info(f"读取的MP3数据条目有【{df.shape[0]}】条，本运行环境实际存在的数据条目为【{outdf.shape[0]}】条，有效的文件数量为{len(mp3s)}个。")
        batch_v4txt(mp3s, self.db_path)
        self.data_mp3 = outdf

    @timethis
    def load_data(self, keyword='all'):
        """从数据库加载数据"""
        offset = 0
        chunk_list = []
        while True:
            query = f"SELECT * FROM wc_{self.name} LIMIT {self.chunk_size} OFFSET {offset}"
            chunk_df = pd.read_sql(query, self.conn)
            if chunk_df.empty:
                break

            # 处理每个分段的数据判断关键词并相应进行过滤
            if (keyword != 'all'):
                # 按 sender 列进行筛选
                # 转义关键词中的特殊字符 
                escaped_keyword = re.escape(keyword) 
                # 按 sender 列进行筛选 
                chunk_df = chunk_df[chunk_df['sender'].str.contains(escaped_keyword, regex=True, na=False)]

            processed_chunk = self.process_chunk(chunk_df)
            # 将处理后的 DataFrame 存储如列表中
            chunk_list.append(processed_chunk)
            # 更新偏移量
            offset += self.chunk_size
        # 将所有分段的 DataFrame 合并成一个完整的 DataFrame 
        full_df = pd.concat(chunk_list)
        cdf = full_df.drop_duplicates(subset=['time', 'send', 'sender', 'type', 'content'])
        cdf = cdf.sort_values(['time'], ascending=True)
        log.info(f"合并所有分段DF后的数据有{full_df.shape[0]}条，整理去重后有{cdf.shape[0]}条")
        self.data = cdf

    def export_data(self):
        """导出数据表"""
        return self.data

    def export_data_raw(self):
        """导出数据表"""
        return self.data_raw

    def analyze_spec(self):
        num4all = self.data.shape[0]
        print(f"加载的数据条数: {num4all}")
        sport_df = self.data[self.data.sender.str.contains('微信运动')]
        sport_df.loc[:, 'time'] = sport_df['time'].apply(lambda x: datetime.fromtimestamp(x))
        spdf = sport_df.loc[:, ['time', 'content']]
        spdf.loc[:, 'content'] = spdf['content'].apply(lambda x:re.sub(r"(\[\w+前\]|\[刚才\])?", "", x) )
        num4all = spdf.shape[0]
        print(spdf[spdf.duplicated()])
        spdf.drop_duplicates(inplace=True)
        print(f"数据有{num4all}条，去重后有{spdf.shape[0]}条")

    def analyze_messages(self):
        """进行消息统计分析"""
        if self.data.empty:
            print("没有消息可分析，请先加载聊天记录！")
            return

        # 总消息数量
        total_messages = self.data.shape[0]
        print(f"总消息数量: {total_messages}")

        # 发送者统计
        sender_counter = Counter(self.data['sender'])
        print("发送者统计:")
        for sender, count in sender_counter.most_common():
            print(f"{sender}: {count} 条消息")

        # 消息类型统计
        type_counter = Counter(self.data['type'])
        print("消息类型统计:")
        for msg_type, count in type_counter.items():
            print(f"{msg_type}: {count} 条消息")

        # 按月统计消息数量
        self.data['month'] = self.data['time'].dt.to_period('M')
        monthly_count = self.data.groupby('month').size()
        print("按月统计消息数量:")
        for month, count in monthly_count.items():
            print(f"{month}: {count} 条消息")

    def save_report(self, report_file='wechat_report.txt'):
        """将分析结果保存到文件中"""
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(f"总消息数量: {self.data.shape[0]}\n")

            # 发送者统计
            sender_counter = Counter(self.data['sender'])
            f.write("发送者统计:\n")
            for sender, count in sender_counter.most_common():
                f.write(f"{sender}: {count} 条消息\n")

            # 消息类型统计
            type_counter = Counter(self.data['type'])
            f.write("消息类型统计:\n")
            for msg_type, count in type_counter.items():
                f.write(f"{msg_type}: {count} 条消息\n")

            # 按月统计消息数量
            self.data['month'] = self.data['time'].dt.to_period('M')
            monthly_count = self.data.groupby('month').size()
            f.write("按月统计消息数量:\n")
            for month, count in monthly_count.items():
                f.write(f"{month}: {count} 条消息\n")

        print(f"分析结果已保存到 {report_file}")

    def close(self):
        """关闭数据库连接"""
        self.conn.close()

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
    name = "白晔峰"
    analysis = WeChatAnalysis(dbname, name, chunk_size=800000)

    # analysis.load_data("梅富忠")
    # sdf = analysis.export_data()

    # 仅处理mp3文件
    analysis.load_data_mp3()
    mp3df = analysis.data_mp3
    
    # analysis.analyze_messages()
    # analysis.save_report('wechat_report.txt')
    analysis.close()
    # compact_sqlite3_db(dbname)
    if not_IPython():
        log.info(f"文件\t{__file__}\t运行结束。")
