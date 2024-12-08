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
# # 监测joplin四件套

# %% [markdown]
# ## 引入库

# %%
import re
import json
from pathlib import Path
from datetime import datetime, timedelta, date

# %%
import pathmagic
with pathmagic.context():
    from func.first import getdirmain
    from func.configpr import getcfpoptionvalue, setcfpoptionvalue
    from func.logme import log
    # from func.wrapfuncs import timethis
    # from func.termuxtools import termux_location, termux_telephony_deviceinfo
    # from func.nettools import ifttt_notify
    # from etc.getid import getdevicename, gethostuser
    from func.sysfunc import not_IPython, set_timeout, after_timeout, execcmd
    from func.jpfuncs import getapi, getnote, searchnotes, createnote, updatenote_body


# %% [markdown]
# ## 核心函数

# %% [markdown]
# ### NoteMonitor（类）

# %%
class NoteMonitor:
    def __init__(self, state_file=getdirmain() / 'data' / 'monitor_state_notes.json'):
        self.state_file = state_file
        self.monitored_notes = {}  # 存储笔记ID和监控信息的字典
        self.load_state()  # 从文件加载状态
        # 转换 last_fetch_time
        for note_id, note_info in self.monitored_notes.items():
            if isinstance(note_info['last_fetch_time'], str):
                note_info['last_fetch_time'] = datetime.strptime(note_info['last_fetch_time'], '%Y-%m-%d %H:%M:%S.%f')

            # 初始化 content_by_date 为字典
            if 'content_by_date' not in note_info:
                note_info['content_by_date'] = {}

    def add_note(self, note_id):
        if note_id not in self.monitored_notes:
            self.monitored_notes[note_id] = {
                'title': '',
                'update_count': 0,
                'last_fetch_time': None,
                'word_count_history': [],
                'previous_word_count': 0,
                'content_by_date': {}  # 初始化 content_by_date
            }

    def update_monitor(self, note_id, current_time, word_count):
        note_info = self.monitored_notes[note_id]
        if note_info['last_fetch_time'] is not None:
            if current_time.date() == note_info['last_fetch_time'].date():
                note_info['update_count'] += 1
            else:
                note_info['update_count'] = 1
        else:
            note_info['update_count'] = 1

        note_info['last_fetch_time'] = current_time
        note_info['title'] = getattr(getnote(note_id), 'title')
        note_info['word_count_history'].append(word_count)
        # print(note_id, type(note_info['content_by_date']))

        # 更新 content_by_date
        self.update_content_by_date(note_id, current_time, word_count)

    def update_content_by_date(self, note_id, current_time, word_count):
        note_content = getattr(getnote(note_id), 'body')  # 假设我们可以从 getnote 中获取笔记内容
        entries = note_content.split('###')  # 按照日期标题分割内容
        
        for entry in entries:
            entry = entry.strip()
            if entry:  # 确保条目不为空
                # 提取日期和正文
                lines = entry.split('\n', 1)
                date_str = lines[0].strip()
                content = lines[1].strip() if len(lines) > 1 else ''

                # 解析日期并计算字数
                try:
                    date1 = datetime.strptime(date_str, '%Y年%m月%d日').date()
                    # print(type(date1))
                    # date_str_2 = date.strftime('%Y年%m月%d日')
                    word_count = len(content.split())  # 计算字数

                    note_info = self.monitored_notes[note_id]
                    self.update_word_count_by_date(note_info, date1, current_time, word_count)
                except ValueError:
                    continue  # 如果日期格式不正确，跳过

    def update_word_count_by_date(self, note_info, date, current_time, word_count):
        # print(date, (note_info['content_by_date']))
        if date not in note_info['content_by_date']:
            # 如果该日期没有记录，直接添加
            note_info['content_by_date'][date] = [(current_time, word_count)]
        else:
            # 获取该日期的更新时间字数列表
            update_list = note_info['content_by_date'][date]
            last_fetch_time, last_word_count = update_list[-1]

            # 确保 last_fetch_time 是 datetime 对象
            if isinstance(last_fetch_time, str):
                last_fetch_time = datetime.fromisoformat(last_fetch_time)

            if word_count != last_word_count:
                # 字数有变化，进行进一步处理
                if last_fetch_time.date() == current_time.date():
                    # 同一天的最新更新时间
                    update_list[-1] = (last_fetch_time, word_count)  # 更新字数
                else:
                    # 不同天的情况
                    if current_time.time() >= datetime.strptime('00:00', '%H:%M').time() and \
                       current_time.time() <= datetime.strptime('08:00', '%H:%M').time():
                        # 在零时到次日八点之间，更新最新的时间和字数
                        update_list[-1] = (current_time, word_count)
                    else:
                        # 次日八点之后，新增数据对
                        update_list.append((current_time, word_count))
        # 对字典进行排序输出
        # note_info['content_by_date'] = dict(sorted(note_info['content_by_date'].items(), key=lambda x: x[0]), reverse=True)

    def save_state(self):
        # 创建一个新的字典以存储可序列化的状态
        serializable_notes = {}
        
        for note_id, note_info in self.monitored_notes.items():
            # 创建一个新的字典来存储每个笔记的信息
            serializable_info = {}
            for key, value in note_info.items():
                if key == 'last_fetch_time' and isinstance(value, (datetime, date)):
                    # 将日期转换为字符串
                    serializable_info[key] = value.isoformat()
                elif isinstance(value, dict):
                    # 对于包含日期的字典，转换日期键为字符串
                    serializable_value = {}
                    for inner_key, inner_value in value.items():
                        if isinstance(inner_key, (date, datetime)):
                            serializable_value[inner_key.isoformat()] = inner_value
                        else:
                            serializable_value[inner_key] = inner_value
                    serializable_info[key] = serializable_value
                else:
                    serializable_info[key] = value

            serializable_notes[note_id] = serializable_info

        # 将可序列化的字典保存到文件
        with open(self.state_file, 'w') as f:
            json.dump(serializable_notes, f, default=str)

    def load_state(self):
        if Path(self.state_file).exists():
            with open(self.state_file, 'r') as f:
                loaded_data = json.load(f)
                # 将日期字符串转换为 datetime.date 对象
                for note_id, note_info in loaded_data.items():
                    for key, value in note_info.items():
                        if key == 'last_fetch_time' and isinstance(value, str):
                            note_info[key] = datetime.fromisoformat(value)
                        elif key == 'content_by_date' and isinstance(value, dict):
                            content_by_date = {}
                            for date_str, updates in value.items():
                                date_key = datetime.fromisoformat(date_str).date()
                                content_by_date[date_key] = updates
                            note_info[key] = content_by_date
                self.monitored_notes = loaded_data

    def clear_state(self):
        self.monitored_notes = {}
        self.save_state()


# %% [markdown]
# ### monitor_notes(note_ids)

# %%
def monitor_notes(note_ids):
    log.info(f'监控笔记 ID 列表: {note_ids_to_monitor}')

    global jpapi
    note_monitor = NoteMonitor()

    for note_id in note_ids:
        note_monitor.add_note(note_id)
        print(note_id)
        note = getnote(note_id)
        current_time = datetime.now()
        # 不是英文需要统计所有字数而不是英语单词
        # current_word_count = len(note.body.split())
        current_word_count = len(note.body)

        # 更新监控信息
        if current_word_count != note_monitor.monitored_notes[note_id]['previous_word_count']:
            note_monitor.update_monitor(note_id, current_time, current_word_count)
            note_monitor.monitored_notes[note_id]['previous_word_count'] = current_word_count
    # 保存监控状态
    note_monitor.save_state()

    # 返回监控笔记用于后续更新
    return note_monitor


# %% [markdown]
# ### ensure_monitor_note_exists(title='监控笔记')

# %%
def ensure_monitor_note_exists(title="监控笔记"):
    global jpapi
    # 查找监控笔记
    results = searchnotes(f"title:{title}")
    if results:
        monitor_note_id = results[0].id
    else:
        monitor_note_id = createnote(title=title, body="监控笔记已创建。")

    print(f"监控笔记id为：\t{monitor_note_id}")
    return monitor_note_id



# %% [markdown]
# ### log_monitor_info(monior_note_id, note_monitor)

# %%
def log_monitor_info(monitor_note_id, note_monitor):
    global jpapi
    body_content = "监控结果:\n\n"
    
    for note_id, info in note_monitor.monitored_notes.items():
        body_content += f"笔记ID: {note_id}\n"
        body_content += f"笔记标题: {info['title']}\n"
        body_content += f"更新次数: {info['update_count']}\n"
        body_content += f"最后抓取时间: {info['last_fetch_time']}\n"
        body_content += f"字数历史变化: {info['word_count_history']}\n"
        body_content += f"笔记最新有效日期: {max(info['content_by_date'])}\n"
        body_content += f"笔记有效日期数量: {len(info['content_by_date'])}\n\n"

    # 更新监控笔记的内容
    updatenote_body(monitor_note_id, body_content)



# %% [markdown]
# ## 主函数，__main__

# %%
if __name__ == '__main__':
    if not_IPython():
        log.info(f'开始运行文件\t{__file__}')

    # 从指定待监控笔记列表笔记获取内容
    note = getnote('2ec45f5b1a10470db1eb3e52462edd18')
    note_links = getattr(note, 'body').split()
    # 提取笔记 ID
    note_ids_to_monitor = [re.search(r'\(:/(.+)\)', link).group(1) for link in note_links]
    monitor_note_id = ensure_monitor_note_exists()  # 确保监控笔记存在
    note_monitor = monitor_notes(note_ids_to_monitor)  # 监控笔记
    log_monitor_info(monitor_note_id, note_monitor)  # 输出结果到监控笔记

    if not_IPython():
        log.info(f'Done.结束执行文件\t{__file__}')

