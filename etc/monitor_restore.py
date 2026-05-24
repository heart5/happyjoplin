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
# # monitor_restore — 快照恢复工具

# %% [markdown]
# ## 引入库

# %%
import argparse
import sys
from datetime import datetime

import pathmagic

# %%
with pathmagic.context():
    from func.jpfuncs import updatenote_body
    from func.logme import log
    from work.monitor_collect import _fill_empty_dates, collect_one, parse_daily_entries
    from work.monitor_report import generate_all_reports
    from work.monitor_store import (
        get_alerts_by_person,
        get_all_unresolved_alerts,
        get_notes_by_person,
        get_snapshot_by_date,
        mark_report_dirty,
        resolve_alert,
        upsert_daily_stat,
    )

# %% [markdown]
# ## 核心函数


# %%
def _find_restore_snapshot(person: str, target_date: str) -> tuple | None:
    """找到某人目标日期对应的快照。返回 (note_id, note_title, snapshot_dict) 或 None。"""
    notes = get_notes_by_person(person)
    if not notes:
        print(f"未找到 {person} 的活跃笔记")
        return None

    candidates = []
    for n in notes:
        snap = get_snapshot_by_date(n["note_id"], target_date)
        if snap:
            candidates.append((n, snap))

    if not candidates:
        print(f"未找到 {person} 在 {target_date} 之后的快照")
        return None

    if len(candidates) == 1:
        n, snap = candidates[0]
        return (n["note_id"], n["title"], snap)

    # 多人多笔记：选该日期字数最高的那篇
    best = max(candidates, key=lambda x: x[1]["word_count"])
    n, snap = best
    print(f"{person} 有 {len(candidates)} 篇笔记命中，选字数最高的: 《{n['title']}》")
    return (n["note_id"], n["title"], snap)


def do_restore(person: str, target_date: str, dry_run: bool = False) -> bool:
    """执行恢复。返回 True 表示成功。"""
    result = _find_restore_snapshot(person, target_date)
    if not result:
        return False

    note_id, title, snap = result

    print(f"\n{'[DRY-RUN] ' if dry_run else ''}恢复《{title}》")
    print(f"  快照时间: {snap['captured_at']}")
    print(f"  快照字数: {snap['word_count']:,}")
    print("  恢复内容预览（前200字）:")
    body = snap["body_fulltext"]
    preview = body[:200].replace("\n", "\\n")
    print(f"  {preview}...")

    if dry_run:
        return True

    # 执行恢复
    updatenote_body(note_id, body)

    # 用旧快照的snapshot_id重建daily_stats，避免NULL问题
    daily = parse_daily_entries(body, datetime.now())
    for entry_date, wc in daily.items():
        upsert_daily_stat(
            note_id=note_id,
            entry_date=entry_date,
            word_count=wc,
            is_backfill=0,
            snapshot_id=snap["id"],
        )
    _fill_empty_dates(note_id, body, datetime.now())

    mark_report_dirty(person)
    log.info(f"恢复笔记《{title}》至 {target_date} 的快照 (snapshot_id={snap['id']})")
    print(f"已恢复《{title}》→ {target_date} 快照")

    # 自动运行采集+报告，跳过冷却等待
    print("\n触发全量采集（跳过冷却）...")
    # 对该笔记立即执行collect_one以更新状态
    collect_one(note_id, "", datetime.now())
    print("生成报告...")
    generate_all_reports(dirty_only=True)
    print("恢复流程完成。")

    return True


def show_alerts(person: str = None):
    """显示告警列表。"""
    if person:
        alerts = get_alerts_by_person(person, resolved=None)
        print(f"{person} 的告警记录 ({len(alerts)}条):\n")
    else:
        alerts = get_all_unresolved_alerts()
        print(f"所有未处理告警 ({len(alerts)}条):\n")

    if not alerts:
        print("  无记录")
        return

    for a in alerts:
        status = "已处理" if a["resolved"] else "待处理"
        atype = "内容骤降" if a["alert_type"] == "content_drop" else "笔记消失"
        drop_pct = (
            f"{(a['prev_word_count'] - a['new_word_count']) / a['prev_word_count'] * 100:.0f}%"
            if a["prev_word_count"] > 0
            else "N/A"
        )
        print(
            f"  [{a['id']}] {status} | {atype} | {a['person']} | "
            f"{a['prev_word_count']}→{a['new_word_count']}字 ({drop_pct}) | "
            f"{a['detected_at']}"
        )


# %% [markdown]
# ## 主函数


# %%
def main():
    parser = argparse.ArgumentParser(description="快照恢复工具")
    parser.add_argument("--person", "-p", help="目标人名")
    parser.add_argument("--date", "-d", help="恢复到指定日期 (YYYY-MM-DD)")
    parser.add_argument("--dry-run", "-n", action="store_true", help="预览模式，不执行实际操作")
    parser.add_argument("--alerts", "-a", action="store_true", help="查看告警列表")
    parser.add_argument("--resolve", "-r", type=int, help="将指定告警ID标记为已处理")
    args = parser.parse_args()

    if args.resolve:
        resolve_alert(args.resolve)
        print(f"告警 #{args.resolve} 已标记为已处理")
        return

    if args.alerts:
        show_alerts(args.person)
        return

    if not args.person or not args.date:
        parser.error("恢复需要 --person 和 --date")

    ok = do_restore(args.person, args.date, dry_run=args.dry_run)
    if not ok:
        sys.exit(1)


# %%
if __name__ == "__main__":
    from func.sysfunc import not_IPython

    if not_IPython():
        log.info(f"开始运行文件\t{__file__}")

    main()

    if not_IPython():
        log.info(f"Done. 结束执行文件\t{__file__}")
