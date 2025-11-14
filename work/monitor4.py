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
# # 监测joplin四件套

# %% [markdown]
# ## 引入库

# %%
import json
import re
from datetime import date, datetime, timedelta
from pathlib import Path

import arrow
import pandas as pd
from tzlocal import get_localzone

# %%
import pathmagic

with pathmagic.context():
    from func.configpr import getcfpoptionvalue, setcfpoptionvalue
    from func.first import getdirmain
    from func.jpfuncs import createnote, getnote, searchnotes, updatenote_body
    from func.logme import log

    # from func.wrapfuncs import timethis
    # from func.termuxtools import termux_location, termux_telephony_deviceinfo
    # from func.nettools import ifttt_notify
    # from etc.getid import getdevicename, gethostuser
    from func.sysfunc import not_IPython


# %% [markdown]
# ## 核心函数

# %% [markdown]
# ### NoteMonitor（类）


# %%
class NoteMonitor:
    """笔记监控类"""

    def __init__(self, state_file: Path=getdirmain() / "data" / "monitor_state_notes.json") -> None:
        """初始化"""
        self.state_file = state_file
        self.monitored_notes = {}  # 存储笔记ID和监控信息的字典
        self.load_state()  # 从文件加载状态
        # 转换 last_fetch_time等字符串time为datetime类型
        for note_id, note_info in self.monitored_notes.items():
            for option_time in [
                "last_fetch_time",
                "first_fetch_time",
                "note_update_time",
            ]:
                if isinstance(note_info[option_time], str):
                    note_info[option_time] = datetime.strptime(
                        note_info[option_time], "%Y-%m-%d %H:%M:%S.%f"
                    )
            # 初始化 content_by_date 为字典
            if "content_by_date" not in note_info:
                note_info["content_by_date"] = {}

            # 初始化 person 为str
            if "person" not in note_info:
                note_info["person"] = ""

            # 初始化 section 为str
            if "section" not in note_info:
                note_info["section"] = ""

    def add_note(self, note_id: str) -> None:
        """添加笔记"""
        if note_id not in self.monitored_notes:
            self.monitored_notes[note_id] = {
                "title": "",
                "person": "",
                "section": "",
                "fetch_count": 0,
                "note_update_time": None,
                "first_fetch_time": None,
                "last_fetch_time": None,
                "word_count_history": [],
                "previous_word_count": 0,
                "content_by_date": {},  # 初始化 content_by_date
            }

    def update_monitor(self, note_id: str, current_time: datetime, word_count: int) -> None :
        """更新笔记监控信息"""
        note_info = self.monitored_notes[note_id]
        if note_info["last_fetch_time"] is not None:
            note_info["fetch_count"] += 1
        else:
            note_info["fetch_count"] = 1

        # first_fetch_time正常赋值，如果之前就有则找到所有日期条目中的时间次数对，采用最小那个时间充当
        if note_info["first_fetch_time"] is None:
            if len(note_info["content_by_date"]) == 0:
                note_info["first_fetch_time"] = current_time
            else:
                timelst = [
                    x
                    for sonlst in note_info["content_by_date"].values()
                    for (x, y) in sonlst
                ]
                note_info["first_fetch_time"] = min(timelst)
        note_info["last_fetch_time"] = current_time
        note = getnote(note_id)
        note_info["title"] = getattr(note, "title")
        if len(note_info["person"]) == 0:
            ptn = re.compile(r"[(（](\w+)[)）]")
            if grp := re.findall(ptn, note_info["title"]):
                note_info["person"] = grp[0]
        note_info["note_update_time"] = arrow.get(getattr(note, "updated_time")).to(
            get_localzone()
        )
        note_info["word_count_history"].append(word_count)
        log.info(f"笔记《{note_info['title']}》进入监测并更新相应数据……")
        # 更新 content_by_date
        self.update_content_by_date(note_id, current_time, word_count)

    def update_section_for_note_id(self, note_id: str, section: str) -> None:
        """更新笔记的section"""
        note_info = self.monitored_notes[note_id]
        if len(note_info["section"]) == 0:
            note_info["section"] = section

    def update_content_by_date(self, note_id: str, current_time: datetime, word_count: int) -> None:
        """更新笔记内容按日期统计"""
        note = getnote(note_id)
        note_content = getattr(note, "body")  # 假设我们可以从 getnote 中获取笔记内容
        # 按照三级标题加日期分割文本，datetime.strptime时处理日期字符串中的空格
        ptn = re.compile(r"^###\s+(\d{4}\s*年\s*\d{1,2}\s*月\s*\d{1,2}\s*日)\s*$", re.M)
        date_lst_raw = re.split(ptn, note_content.strip())
        entries_dict_raw = dict(
            zip(
                [
                    datetime.strptime(
                        re.sub(r"\s+", "", datestr), "%Y年%m月%d日"
                    ).date()
                    for datestr in date_lst_raw[1::2]
                ],
                date_lst_raw[2::2],
            )
        )
        # 过滤日期超过当天一天之内的日期数据对
        one_days_later = current_time.date() + timedelta(days=1)
        entries_dict = {
            date: count
            for date, count in entries_dict_raw.items()
            if date <= one_days_later
        }
        if len(entries_dict_raw) != len(entries_dict):
            log.critical(
                f"笔记《{getattr(note, 'title')}》存在超纲日期：{[date for date in entries_dict_raw.keys() if date > one_days_later]}，过滤之"
            )
        # print(f"before(from raw):{entries_dict}")
        # 无有效日期文本数据对则返回
        if len(entries_dict) == 0:
            return

        for entry in entries_dict:
            try:
                word_count = len(entries_dict[entry].strip())
                note_info = self.monitored_notes[note_id]
                self.update_word_count_by_date(
                    note_info, entry, current_time, word_count
                )
            except ValueError:
                print(entry)
                continue
        # print(f"after(update word count done):{entries_dict}")
        # 处理非笔记有效日期的初始化填空问题
        # 最大日期取自笔记最大日期和昨天，避免迟交无效留空
        entry_date_min = min(entries_dict)
        entry_date_max = max(
            max(entries_dict), current_time.date() + timedelta(days=-1)
        )
        daterange = pd.date_range(entry_date_min, entry_date_max)
        datezero = [
            date.date() for date in daterange if date.date() not in entries_dict
        ]
        log.info(
            f"笔记《{getattr(note, 'title')}》的有效数据最早日期为{min(entries_dict)}，最新日期为{max(entries_dict)}，其中内容为空的条目数量为：{len(datezero)}"
        )
        note_info = self.monitored_notes[note_id]
        # 读取笔记的title，来确保是最新的
        note_info["title"] = getattr(note, "title")
        # 从最新的title提取person确保是最新的
        ptn = re.compile(r"[(（](\w+)[)）]")
        if grp := re.findall(ptn, note_info["title"]):
            note_info["person"] = grp[0]
        timelst = [
            x for sonlst in note_info["content_by_date"].values() for (x, y) in sonlst
        ]
        def str2time(x: str) -> datetime:
            return (datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f")
                    if isinstance(x, str)
                    else x)
        timelst = [str2time(x) for x in timelst]
        oldesttime = min(timelst)
        datezero_not_init = [
            date for date in datezero if date not in note_info["content_by_date"]
        ]
        log.info(
            f"笔记《{getattr(note, 'title')}》内容为空且还未初始化的日期列表为：{datezero_not_init}"
        )
        for date1 in datezero_not_init:
            if note_info["fetch_count"] != 1:
                note_info["content_by_date"][date1] = [(oldesttime, 0)]
            else:
                note_info["content_by_date"][date1] = [(current_time, 0)]

    def update_word_count_by_date(self, note_info: str, date: date, current_time: datetime, word_count: int) -> None:
        """更新笔记内容按日期统计"""
        # print(date, (note_info['content_by_date']))
        if date not in note_info["content_by_date"]:
            # 如果该日期没有记录，直接添加
            note_info["content_by_date"][date] = [(current_time, word_count)]
        else:
            # 获取该日期的更新时间字数列表
            update_list = note_info["content_by_date"][date]
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
                    if (
                        current_time.time()
                        >= datetime.strptime("00:00", "%H:%M").time()
                        and current_time.time()
                        < datetime.strptime("08:00", "%H:%M").time()
                    ):
                        # 在零时到次日八点之间，更新最新的时间和字数
                        update_list[-1] = (current_time, word_count)
                    else:
                        # 次日八点之后，新增数据对
                        update_list.append((current_time, word_count))
        # 对字典进行排序输出
        # note_info['content_by_date'] = dict(sorted(note_info['content_by_date'].items(), key=lambda x: x[0]), reverse=True)

    def save_state(self) -> None:
        """保存状态到文件"""
        serializable_notes = {}

        for note_id, note_info in self.monitored_notes.items():
            # 创建一个新的字典来存储每个笔记的信息
            serializable_info = {}
            for key, value in note_info.items():
                if key in [
                    "last_fetch_time",
                    "first_fetch_time",
                    "note_update_time",
                ] and isinstance(value, (datetime, date)):
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
        with open(self.state_file, "w") as f:
            json.dump(serializable_notes, f, default=str)

    def load_state(self) -> None:
        """从文件加载状态"""
        if Path(self.state_file).exists():
            with open(self.state_file, "r") as f:
                loaded_data = json.load(f)
                # 将日期字符串转换为 datetime.date 对象
                for note_id, note_info in loaded_data.items():
                    for key, value in note_info.items():
                        if key in [
                            "last_fetch_time",
                            "first_fetch_time",
                            "note_update_time",
                        ] and isinstance(value, str):
                            note_info[key] = datetime.fromisoformat(value)
                        elif key == "content_by_date" and isinstance(value, dict):
                            content_by_date = {}
                            for date_str, updates in value.items():
                                date_key = datetime.fromisoformat(date_str).date()
                                content_by_date[date_key] = updates
                            note_info[key] = content_by_date
                self.monitored_notes = loaded_data

    def clear_state(self) -> None:
        """清除状态"""
        self.monitored_notes = {}
        self.save_state()


# %% [markdown]
# ### monitor_notes(note_ids, note_monitor)


# %%
def monitor_notes(note_ids: list, note_monitor: NoteMonitor) -> None:
    for note_id in note_ids:
        # 确保该id的笔记存在，否则跳过
        try:
            note = getnote(note_id)
        except Exception as e:
            log.critical(
                f"获取id为“{note_id}”的笔记时出错如下，或许是不存在或笔记冲突导致的：\n{e}"
            )
            continue
        note_monitor.add_note(note_id)
        current_time = datetime.now()
        # 不是英文需要统计所有字数而不是英语单词
        # current_word_count = len(note.body.split())
        current_word_count = len(note.body.strip())
        last_update_time_note = arrow.get(getattr(note, "updated_time")).to(
            get_localzone()
        )

        # 计算当天的标识
        current_day_identity = arrow.now(get_localzone()).replace(
            hour=8, minute=0, second=0, microsecond=0
        )
        if arrow.now().hour < 8:
            current_day_identity = current_day_identity.shift(days=-1)

        # 计算上次更新时间的标识
        last_update_day_identity = last_update_time_note.replace(
            hour=8, minute=0, second=0, microsecond=0
        )
        if last_update_time_note.hour < 8:
            last_update_day_identity = last_update_day_identity.shift(days=-1)
        # 判断是否需要监控, 监控条件：当天的笔记更新时间大于上次更新时间，或当天的笔记更新时间大于8点，且上次更新时间小于8点
        should_monitor = False
        if monitor_current_date := getcfpoptionvalue("happyjpmonitor", "monitor_current_date", "note_id"):
            if monitor_current_date != current_day_identity.date().strftime("%Y-%m-%d"):
                should_monitor = True
                setcfpoptionvalue("happyjpmonitor", "monitor_current_date", "note_id", current_day_identity.date().strftime("%Y-%m-%d"))
        else:
            should_monitor = True
            setcfpoptionvalue("happyjpmonitor", "monitor_current_date", "note_id", current_day_identity.date().strftime("%Y-%m-%d"))
        # 更新监控信息，用笔记更新时间和日志标识做判断依据
        if (
            last_update_time_note
           != note_monitor.monitored_notes[note_id]["note_update_time"]
        ):
            should_monitor = True
        if should_monitor:
            note_monitor.update_monitor(note_id, current_time, current_word_count)
            note_monitor.monitored_notes[note_id]["previous_word_count"] = (
                current_word_count
            )
    # 保存监控状态
    note_monitor.save_state()


# %% [markdown]
# ### ensure_monitor_note_exists(title='监控笔记')


# %%
def ensure_monitor_note_exists(title: str="监控笔记") -> str:
    """查找监控笔记"""
    if (
        monitor_note_id := getcfpoptionvalue("happyjpmonitor", "monitor", "monitor_id")
    ) is None:
        results = searchnotes(f"{title}")
        if results:
            monitor_note_id = results[0].id
        else:
            monitor_note_id = createnote(title=title, body="监控笔记已创建。")
        setcfpoptionvalue("happyjpmonitor", "monitor", "monitor_id", monitor_note_id)

    return monitor_note_id


# %% [markdown]
# ### log_monitor_info(monior_note_id, note_monitor)


# %%
def monitor_log_info(title: str, note_ids_to_monitor: list, note_monitor: NoteMonitor) -> str:
    """综合输出指定title和ids的监测情况"""
    # 只处理输入的note_id列表，因为note_monitor包含了所有
    targetdict = {
        note_id: note_info
        for note_id, note_info in note_monitor.monitored_notes.items()
        if note_id in note_ids_to_monitor
    }
    # 排序，按照note_info的last_fetch_time倒序排列
    targetdict = dict(
        sorted(
            targetdict.items(),
            key=lambda item: item[1]["last_fetch_time"],
            reverse=True,
        )
    )
    # print(len(targetdict))
    body_content = f"## {title}\n"
    for note_id, info in targetdict.items():
        if len(info["content_by_date"]) == 0:
            log.info(f"笔记《{info['title']}》的有效日期内容为空，跳过")
            continue
        body_content += f"笔记ID: {note_id}\n"
        body_content += f"### 笔记标题: {info['title']}\n"
        body_content += (
            f"抓取时间起止: {info['first_fetch_time'].strftime('%Y-%m-%d %H:%M:%S')}，"
        )
        body_content += f"{info['last_fetch_time'].strftime('%Y-%m-%d %H:%M:%S')}\n"
        body_content += f"有效抓取次数: {info['fetch_count']}\n"
        body_content += f"笔记最近更新时间: {info['note_update_time'].strftime('%Y-%m-%d %H:%M:%S')}\n"
        body_content += f"字数历史变化: {info['word_count_history']}\n"
        body_content += f"笔记有效内容起止日期: {min(info['content_by_date'])}，"
        body_content += f"{max(info['content_by_date'])}\n"
        valid_date = [
            son
            for son in info["content_by_date"]
            if info["content_by_date"][son][0][1] != 0
        ]
        # print(valid_date)
        body_content += f"笔记内容有效日期数量: {len(valid_date)}({len(info['content_by_date'])})\n\n"

    return body_content


# %% [markdown]
# ### split_ref()


# %%
def split_ref() -> None:
    """从指定待监控笔记列表笔记获取内容，分区块处理，并生成监控笔记的输出内容。"""
    title = "四件套笔记列表"
    results = searchnotes(f"{title}")
    if results:
        note_list_id = results[0].id
    else:
        log.critical(f"标题为：《{title}》的笔记不存在")
        return
    note = getnote(note_list_id)
    bodystr = getattr(note, "body")
    ptn = re.compile(r"^###\s+(\w+)\s*$", re.M)
    section_lst_raw = re.split(ptn, bodystr.strip())
    section_dict = dict(zip(section_lst_raw[1::2], section_lst_raw[2::2]))

    # 监控笔记
    note_monitor = NoteMonitor()

    outputstr = ""
    person_updated = False  # person更新开关
    section_updated = False  # section更新开关
    person_ptn = re.compile(r"[(（](\w+)[)）]")  # 识别title中person的正则表达式
    for section in section_dict:
        outputstr += "---\n"
        # 提取笔记 ID并监测指定section的笔记列表
        note_ids_to_monitor = [
            re.search(r"\(:/(.+)\)", link).group(1)
            for link in section_dict[section].split()
            if re.search(r"\(:/(.+)\)", link)
        ]
        print(f"section is 《{section}》")
        monitor_notes(note_ids_to_monitor, note_monitor)

        # 检查person和section是否已经设置，没有设置则设置之
        for note_id in note_ids_to_monitor:
            info = note_monitor.monitored_notes[note_id]
            if len(info["person"]) == 0:
                if grp := re.findall(person_ptn, info["title"]):
                    info["person"] = grp[0]
                    person_updated = True
            if len(info["section"]) == 0:
                info["section"] = section
                section_updated = True

        # 累加生成输出结果
        outputstr += monitor_log_info(section, note_ids_to_monitor, note_monitor)

    # 如果section设置开关为真则保存状态文件
    if section_updated or person_updated:
        note_monitor.save_state()

    monitor_note_id = ensure_monitor_note_exists()  # 确保监控笔记存在
    # 更新监控笔记的内容
    updatenote_body(monitor_note_id, outputstr)


# %% [markdown]
# ## 主函数，__main__

# %%
if __name__ == "__main__":
    if not_IPython():
        log.info(f"开始运行文件\t{__file__}")

    split_ref()

    if not_IPython():
        log.info(f"Done.结束执行文件\t{__file__}")
