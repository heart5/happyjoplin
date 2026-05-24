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
# # 笔记监测 —— 采集层

# %% [markdown]
# ## 引入库

# %%
import re
from datetime import datetime, timedelta

# %%
import pathmagic

with pathmagic.context():
    from func.first import getdirmain
    from func.jpfuncs import content_hash, getnote, searchnotes
    from func.logme import log
    from func.sysfunc import not_IPython
    from func.nettools import ifttt_notify
    from work.monitor_store import (
        MAX_PENDING_WAIT_HOURS,
        STABILITY_COOLDOWN_MINUTES,
        delete_pending_change,
        get_last_snapshot_hash,
        get_latest_snapshot,
        get_note_info,
        get_pending_change,
        get_previous_snapshot,
        init_db,
        insert_alert,
        insert_snapshot,
        mark_note_inactive,
        mark_report_dirty,
        upsert_daily_stat,
        upsert_note,
        upsert_pending_change,
    )


# %% [markdown]
# ## 配置笔记解析


# %%
def fetch_note_list_sections() -> dict[str, list[str]]:
    """从「四件套笔记列表」解析 section→[note_id] 映射。

    Returns:
        {'核心客户': ['id1', 'id2'], '工作笔记': [...], ...}
    """
    title = "四件套笔记列表"
    results = searchnotes(f"{title}")
    if not results:
        log.critical(f"标题为：《{title}》的笔记不存在")
        return {}

    note = getnote(results[0].id)
    body = getattr(note, "body")

    ptn = re.compile(r"^###\s+(\w+)\s*$", re.M)
    section_lst = re.split(ptn, body.strip())

    section_dict = {}
    for i in range(1, len(section_lst), 2):
        section_name = section_lst[i]
        section_body = section_lst[i + 1]
        note_ids = [
            re.search(r"\(:/(.+)\)", link).group(1) for link in section_body.split() if re.search(r"\(:/(.+)\)", link)
        ]
        section_dict[section_name] = note_ids

    return section_dict


# %% [markdown]
# ## 每日内容解析


# %%
def parse_daily_entries(body: str, current_time: datetime) -> dict:
    """解析笔记body中的 ### YYYY年MM月DD日 三级标题段落，返回 {date: word_count}。

    过滤掉超过current_time后一天的日期（防异常日期）。
    """
    ptn = re.compile(r"^###\s+(\d{4}\s*年\s*\d{1,2}\s*月\s*\d{1,2}\s*[日号])\s*$", re.M)

    parts = re.split(ptn, body.strip())
    if len(parts) < 3:
        return {}

    entries = {}
    for i in range(1, len(parts), 2):
        date_str_raw = parts[i]
        content = parts[i + 1]
        date_str = re.sub(r"\s+", "", date_str_raw).replace("号", "日")

        try:
            entry_date = datetime.strptime(date_str, "%Y年%m月%d日").date()
        except ValueError:
            continue

        cutoff = current_time.date() + timedelta(days=1)
        if entry_date > cutoff:
            log.critical(f"日期 {entry_date} 超过截止日 {cutoff}，已过滤")
            continue

        entries[str(entry_date)] = len(content.strip())

    return entries


# %% [markdown]
# ## 单篇笔记采集


# %%
def collect_one(note_id: str, section: str, current_time: datetime | None = None) -> bool:
    """采集单篇笔记。返回 True 表示触发了稳定快照（需要更新报告）。

    逻辑：
    1. 计算 content_hash，与上次快照对比
    2. 相同 → 清理pending，返回False
    3. 不同 → 管理pending_changes
       - 首次不同：创建pending
       - 继续不同：更新pending
       - 相同(hash稳定)：检查冷却期
         - 冷却达标或超时 → 正式快照 → 标记dirty → 返回True
         - 冷却不足 → 返回False
    """
    if current_time is None:
        current_time = datetime.now()

    try:
        note = getnote(note_id)
    except Exception as e:
        log.critical(f"获取笔记 {note_id} 失败: {e}")
        # 检测笔记是否被删除：notes表中有记录且之前活跃则告警
        info = get_note_info(note_id)
        if info and info.get("is_active"):
            # 从最近快照获取字数作为prev_word_count
            latest = get_latest_snapshot(note_id)
            prev_wc = latest["word_count"] if latest else 0
            insert_alert(
                note_id=note_id,
                person=info.get("person", ""),
                prev_word_count=prev_wc,
                new_word_count=0,
                alert_type="note_missing",
            )
            mark_note_inactive(note_id)
            log.warning(f"笔记《{info.get('title', note_id)}》无法访问，已标记为不活跃")
            ifttt_notify(
                f"笔记消失告警:《{info.get('title', note_id)}》无法访问，已标记不活跃",
                "monitor_collect",
            )
        return False

    title = getattr(note, "title", "")
    body = getattr(note, "body", "")
    current_hash = content_hash(note_id)
    last_hash = get_last_snapshot_hash(note_id)

    # 提取 person（从标题中正则匹配）
    person = ""
    if ptn_grp := re.findall(re.compile(r"[(（](\w+)[)）]"), title):
        person = ptn_grp[0]

    # 确保notes表有记录
    upsert_note(note_id, title=title, person=person, section=section)

    pending = get_pending_change(note_id)

    # 情况A: hash与上次快照相同，且没有pending → 无变化
    if current_hash == last_hash and pending is None:
        return False

    # 情况B: hash与上次快照相同，但有pending → 用户可能回滚了，清理pending
    if current_hash == last_hash and pending is not None:
        log.info(f"笔记《{title}》hash回退到已快照版本，清理pending")
        delete_pending_change(note_id)
        return False

    # hash不同 → 有变化
    log.info(f"笔记《{title}》检测到变化: {last_hash[:8] if last_hash else 'None'}... → {current_hash[:8]}...")

    # 情况C: 首次变化 → 新建pending
    if pending is None:
        upsert_pending_change(note_id, current_hash, current_time, current_time)
        log.info(f"笔记《{title}》进入待确认状态，等待冷却...")
        return False

    # 情况D: hash与pending不同 → 还在编辑中
    if current_hash != pending["content_hash"]:
        upsert_pending_change(note_id, current_hash, pending["first_seen"], current_time)
        log.info(f"笔记《{title}》仍在编辑中，更新pending...")
        return False

    # 情况E: hash与pending相同 → 检查冷却期
    last_seen = (
        datetime.fromisoformat(pending["last_seen"]) if isinstance(pending["last_seen"], str) else pending["last_seen"]
    )
    first_seen = (
        datetime.fromisoformat(pending["first_seen"])
        if isinstance(pending["first_seen"], str)
        else pending["first_seen"]
    )

    stable_minutes = (current_time - last_seen).total_seconds() / 60
    total_wait_hours = (current_time - first_seen).total_seconds() / 3600

    if stable_minutes >= STABILITY_COOLDOWN_MINUTES:
        reason = "冷却达标"
        is_forced = 0
    elif total_wait_hours >= MAX_PENDING_WAIT_HOURS:
        reason = "超时强制"
        is_forced = 1
    else:
        log.info(
            f"笔记《{title}》冷却中: 已稳定{stable_minutes:.0f}分钟, 还需{STABILITY_COOLDOWN_MINUTES - stable_minutes:.0f}分钟"
        )
        return False

    # 正式快照
    word_count = len(body.strip())
    snapshot_id = insert_snapshot(
        note_id=note_id,
        captured_at=current_time,
        content_hash=current_hash,
        word_count=word_count,
        body_fulltext=body,
        is_forced=is_forced,
    )

    delete_pending_change(note_id)

    # 解析每日条目
    daily = parse_daily_entries(body, current_time)
    for entry_date, wc in daily.items():
        is_backfill = _check_backfill(entry_date, current_time) if snapshot_id else 0
        upsert_daily_stat(
            note_id=note_id,
            entry_date=entry_date,
            word_count=wc,
            is_backfill=is_backfill,
            snapshot_id=snapshot_id,
        )

    # 处理未写入的日期（填0值）
    _fill_empty_dates(note_id, body, current_time)

    # 内容突变检测：对比上一次快照字数
    if person and word_count > 0:
        prev_snap = get_previous_snapshot(note_id)
        if prev_snap and prev_snap["word_count"] > 0:
            drop = prev_snap["word_count"] - word_count
            drop_pct = drop / prev_snap["word_count"]
            if drop_pct > 0.3 and drop > 500:
                insert_alert(
                    note_id=note_id,
                    person=person,
                    prev_word_count=prev_snap["word_count"],
                    new_word_count=word_count,
                    alert_type="content_drop",
                    prev_snapshot_id=prev_snap["id"],
                )
                log.warning(
                    f"笔记《{title}》字数骤降: {prev_snap['word_count']}→{word_count} ({drop_pct:.0%})"
                )
                ifttt_notify(
                    f"内容骤降告警:《{title}》{prev_snap['word_count']}→{word_count}字({drop_pct:.0%})",
                    "monitor_collect",
                )

    # 标记脏
    if person:
        mark_report_dirty(person)
        log.info(
            f"笔记《{title}》快照完成({reason}): snapshot_id={snapshot_id}, word_count={word_count}, person={person}"
        )
    else:
        log.info(f"笔记《{title}》快照完成({reason}): snapshot_id={snapshot_id}, word_count={word_count}")

    return True


# %% [markdown]
# ## 辅助函数


# %%
def _check_backfill(entry_date_str: str, current_time: datetime) -> int:
    """检查指定日期条目是否是补填（快照时间超过该日期的次日08:00截止线）。"""
    entry_date = datetime.strptime(entry_date_str, "%Y-%m-%d").date()
    deadline = datetime(entry_date.year, entry_date.month, entry_date.day, 8, 0, 0) + timedelta(days=1)
    return 1 if current_time > deadline else 0


# %%
def _fill_empty_dates(note_id: str, body: str, current_time: datetime) -> None:
    """对于笔记中存在但未写入daily_stats的日期，填0值条目。"""
    ptn = re.compile(r"^###\s+(\d{4}\s*年\s*\d{1,2}\s*月\s*\d{1,2}\s*[日号])\s*$", re.M)
    parts = re.split(ptn, body.strip())
    if len(parts) < 3:
        return

    all_dates = set()
    for i in range(1, len(parts), 2):
        date_str = re.sub(r"\s+", "", parts[i]).replace("号", "日")
        try:
            all_dates.add(datetime.strptime(date_str, "%Y年%m月%d日").date())
        except ValueError:
            continue

    existing_dates = set()
    with __import__("sqlite3").connect(str(getdirmain() / "data" / "monitor.db")) as c:
        c.row_factory = __import__("sqlite3").Row
        rows = c.execute(
            "SELECT DISTINCT entry_date FROM daily_stats WHERE note_id=?",
            (note_id,),
        ).fetchall()
        existing_dates = {r["entry_date"] for r in rows}

    cutoff = current_time.date() + timedelta(days=1)
    missing = {d for d in all_dates if str(d) not in existing_dates and d <= cutoff}

    for d in missing:
        upsert_daily_stat(
            note_id=note_id,
            entry_date=str(d),
            word_count=0,
            is_backfill=0,
            snapshot_id=None,
        )


# %% [markdown]
# ## 全量采集入口


# %%
def collect_all(current_time: datetime | None = None) -> dict:
    """全量采集所有被监测笔记。返回采集汇总。

    Returns:
        {'total': N, 'changed': N, 'persons_dirty': [...], 'pending': N}
    """
    init_db()

    if current_time is None:
        current_time = datetime.now()

    sections = fetch_note_list_sections()
    if not sections:
        log.critical("未找到「四件套笔记列表」中的任何section，跳过采集")
        return {"total": 0, "changed": 0, "persons_dirty": [], "pending": 0}

    total = 0
    changed = 0
    all_note_ids = set()

    for section, note_ids in sections.items():
        for note_id in note_ids:
            all_note_ids.add(note_id)
            total += 1
            try:
                if collect_one(note_id, section, current_time):
                    changed += 1
            except Exception as e:
                log.critical(f"采集笔记 {note_id} 异常: {e}")

    # 将不在当前配置中的笔记标记为不活跃
    from work.monitor_store import deactivate_notes_except

    deactivated = deactivate_notes_except(all_note_ids)
    if deactivated > 0:
        log.info(f"已停用 {deactivated} 篇已移出配置的笔记")

    from work.monitor_store import get_dirty_persons, get_pending_changes_summary

    dirty = get_dirty_persons()
    pending_count = len(get_pending_changes_summary())

    log.info(f"采集完成: 共{total}篇笔记, {changed}篇生成快照, {pending_count}篇待确认, dirty_persons={dirty}")

    # 汇总未解决告警并推送通知
    from work.monitor_store import get_all_unresolved_alerts
    alerts = get_all_unresolved_alerts()
    if alerts:
        alert_summary = "; ".join(
            f"[{a['alert_type']}]{a['person']}:{a['prev_word_count']}→{a['new_word_count']}"
            for a in alerts[:5]
        )
        if len(alerts) > 5:
            alert_summary += f" ...等{len(alerts)}条"
        log.critical(f"本次采集产生{len(alerts)}条未解决告警: {alert_summary}")

    return {
        "total": total,
        "changed": changed,
        "persons_dirty": dirty,
        "pending": pending_count,
    }


# %% [markdown]
# ## 主函数，__main__

# %%
if __name__ == "__main__":
    if not_IPython():
        log.info(f"开始运行文件\t{__file__}")

    result = collect_all()
    print(f"采集结果: {result}")

    if not_IPython():
        log.info(f"Done.结束执行文件\t{__file__}")
