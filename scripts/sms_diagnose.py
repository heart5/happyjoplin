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
#     jupytext_version: 1.14.4
# ---

# %% [markdown]
# # SMS 采集诊断工具
#
# 在 Termux 手机端运行，排查短信采集的问题：
# 1. 手机短信总量 & _id 范围，判断采集上限是否足够
# 2. 对比已上传的记录，算出"漏采"的财务短信
# 3. 检查采集脚本定时任务状态
# 4. 给出修复建议

# %%
"""
手机端 SMS 采集诊断。

用法：
    python scripts/sms_diagnose.py                # 默认：只读本地缓存+手机 SMS 快照
    python scripts/sms_diagnose.py --deep          # 逐个比对手机短信，找出漏采（慢，遍历全量）
    python scripts/sms_diagnose.py --quick         # 只看统计摘要，不比对

输出：打印诊断报告，可选保存到文件。

注意事项：
    - 需要 Termux:API 权限（termux-sms-list）
    - 全量模式（--deep）遍历手机全部短信，5000 条约 3-5 秒，多了会慢
    - 建议在 WiFi 下运行
"""

import json
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    import pathmagic  # noqa: F401

    with pathmagic.context():
        from func.termuxtools import termux_sms_list
        from func.logme import log

    log = log
except ImportError:
    # 降级：直接调用 termux-sms-list
    log = None
    print("⚠ 未找到 pathmagic/func 模块，降级为直接调用 termux-sms-list", file=sys.stderr)

    def termux_sms_list(num=10):
        raw = subprocess.check_output(
            ["termux-sms-list", "-d", "-l", str(num), "-n"],
            timeout=30,
        )
        return json.loads(raw.decode())


# ── 路径 ──
HERE = Path(__file__).resolve().parent
PROJ_ROOT = HERE.parent
SMS_RECEIVED_DB = PROJ_ROOT / "data" / "sms_received.db"       # 服务端已入库的
SMS_CACHE_DB = PROJ_ROOT / "data" / "sms_cache.db"             # 手机端缓存的


# ── 过滤关键词（与 sms_collector.py 保持一致）──

_BANK_SHORT_CODES = [
    "95555", "95533", "95508", "95559", "95595", "95558",
    "95588", "95599", "95566", "95577", "95561",
    "95568", "95528", "95526", "95580",
    "95511", "95519", "95522",
]

_FINANCE_KEYWORDS = [
    "消费", "支出", "收入", "转账", "余额",
    "￥", "¥", "人民币", "元",
    "交易", "支付", "退款", "退税",
    "信用卡", "储蓄卡", "银行卡", "借记卡",
    "工资", "报销", "理财", "收益",
    "分期", "账单", "额度", "还款",
    "存入", "支取", "汇款", "到账",
]


# 补充关键词（当前脚本漏掉的，用于对比验证）
_EXTRA_KEYWORDS = ["扣款", "代扣", "放款", "转出", "转入", "还清",
                   "结清", "逾期", "拖欠", "快捷支付", "网上支付",
                   "保费", "年金", "保险"]


def is_finance_msg(msg: dict) -> bool:
    """与 sms_collector.py 完全一致的过滤逻辑。"""
    number = str(msg.get("number", ""))
    body = str(msg.get("body", ""))
    if not body:
        return False
    if any(code in number for code in _BANK_SHORT_CODES):
        return True
    if any(kw in body for kw in _FINANCE_KEYWORDS):
        return True
    return False


def is_finance_msg_deep(msg: dict) -> bool:
    """宽松过滤：包含补充关键词也算。"""
    number = str(msg.get("number", ""))
    body = str(msg.get("body", ""))
    if not body:
        return False
    if any(code in number for code in _BANK_SHORT_CODES):
        return True
    all_kw = _FINANCE_KEYWORDS + _EXTRA_KEYWORDS
    if any(kw in body for kw in all_kw):
        return True
    return False


# ── 诊断报告 ──


def section(title):
    print()
    print("=" * 60)
    print(f"  {title}")
    print("=" * 60)


def diagnose(deep: bool, quick: bool):
    start = time.time()

    # ──── 1) 手机端 SMS 存量 ────
    section("1. 手机短信存量")

    print("正在读取手机短信（最多 5000 条）...")
    phone_sms = termux_sms_list(num=5000)
    if not phone_sms:
        print("❌ termux-sms-list 返回空，检查 Termux:API 权限")
        return

    phone_count = len(phone_sms)
    phone_ids = [int(m["_id"]) for m in phone_sms]
    phone_min_id = min(phone_ids)
    phone_max_id = max(phone_ids)
    phone_span = phone_max_id - phone_min_id

    print(f"手机端短信总量（最近 5000 条）:")
    print(f"  _id 范围: {phone_min_id} ~ {phone_max_id}")
    print(f"  id 跨度: {phone_span}")
    print(f"  最小 id = {phone_min_id}")

    if phone_min_id > 10:
        print(f"  ⚠ 最小 _id 为 {phone_min_id}，说明手机上有更多更老的短信未被拉取")
        print(f"  → 建议加大 full_fetch_limit 或分多次 --full 覆盖")
        estimated_total = phone_span // (len(phone_ids) // 5000) * 5000 if len(
            phone_ids) >= 5000 else phone_max_id
        if len(phone_ids) >= 5000:
            print(f"  → 预估手机短信总量约 {estimated_total}+ 条")
    else:
        print(f"  ✓ 5000 条已覆盖手机全部历史短信")

    # 按年月分布
    yr_counts = {}
    ym_counts = {}
    for m in phone_sms:
        r = str(m.get("received", ""))[:7]
        if r:
            ym_counts[r] = ym_counts.get(r, 0) + 1
            yr_counts[r[:4]] = yr_counts.get(r[:4], 0) + 1
    if yr_counts:
        print(f"\n手机端现有短信按年分布:")
        for yr in sorted(yr_counts):
            print(f"  {yr}: {yr_counts[yr]} 条")
        print(f"\n最近 6 个月:")
        sorted_yms = sorted(ym_counts.keys(), reverse=True)[:6]
        for ym in sorted_yms:
            print(f"  {ym}: {ym_counts[ym]} 条")

    if quick:
        return

    # ──── 2) 过滤命中率 ────
    section("2. 财务短信过滤命中率")

    standard_hits = 0
    deep_hits = 0
    missed_samples = []

    for m in phone_sms:
        if is_finance_msg(m):
            standard_hits += 1
        elif is_finance_msg_deep(m):
            deep_hits += 1
            if len(missed_samples) < 10:
                missed_samples.append(m)

    total_finance = standard_hits + deep_hits
    print(f"手机端最近 {phone_count} 条短信：")
    print(f"  当前脚本会捕获: {standard_hits} 条 ({standard_hits/phone_count*100:.1f}%)")
    print(f"  补充关键词额外捕获: {deep_hits} 条 ({deep_hits/phone_count*100:.1f}%)")
    print(f"  合计财务短信: {total_finance} 条 ({total_finance/phone_count*100:.1f}%)")

    if missed_samples:
        print(f"\n⚠ 补充关键词能捕获但当前脚本漏掉的（样本）:")
        for m in missed_samples:
            kw_hit = [kw for kw in _EXTRA_KEYWORDS if kw in str(m.get("body", ""))]
            print(f"  number={m.get('number','')} | 命中={kw_hit}")
            print(f"  body: {str(m.get('body',''))[:100]}")
            print()

    # ──── 3) 与已入库数据对比 ────
    if SMS_RECEIVED_DB.exists():
        section("3. 已上传 vs 手机端对比")

        conn = sqlite3.connect(str(SMS_RECEIVED_DB))
        server_count = conn.execute("SELECT COUNT(*) FROM sms_messages").fetchone()[0]
        server_ids = set(
            r[0] for r in conn.execute("SELECT sms_id FROM sms_messages").fetchall()
        )
        server_max_id = conn.execute("SELECT MAX(sms_id) FROM sms_messages").fetchone()[0]
        conn.close()

        print(f"服务端已入库: {server_count} 条")
        print(f"服务端最大 sms_id: {server_max_id}")
        print(f"手机端最大 sms_id: {phone_max_id}")

        if server_max_id < phone_max_id:
            gap = phone_max_id - server_max_id
            print(f"\n⚠ 手机端比服务端多出约 {gap} 个 id 的短信未上传")
            print(f"  → 可能是脚本 cron 未运行，建议检查定时任务")

        # 手机端财务短信中，哪些还没上传
        not_uploaded = [m for m in phone_sms
                        if is_finance_msg(m)
                        and int(m["_id"]) not in server_ids]

        not_uploaded_deep = [m for m in phone_sms
                             if is_finance_msg_deep(m)
                             and not is_finance_msg(m)
                             and int(m["_id"]) not in server_ids]

        print(f"\n手机端已过滤 > 未上传的财务短信:")
        print(f"  当前脚本范围: {len(not_uploaded)} 条")
        print(f"  补充关键词范围: {len(not_uploaded_deep)} 条")

        unfin_not_uploaded = [m for m in phone_sms
                              if int(m["_id"]) not in server_ids
                              and not is_finance_msg_deep(m)]
        print(f"  非财务（不上传是正常的）: {len(unfin_not_uploaded)} 条")

        if not_uploaded:
            print(f"\n⚠ 未上传的财务短信示例:")
            for m in not_uploaded[:5]:
                r = m.get("received", "")
                print(f"  [{r}] id={m['_id']} {m.get('number','')}: {str(m.get('body',''))[:100]}")

    # ──── 4) 定时任务状态 ────
    section("4. 定时任务状态")

    if SMS_CACHE_DB.exists():
        conn = sqlite3.connect(str(SMS_CACHE_DB))
        cur = conn.execute(
            "SELECT value FROM sms_sync_state WHERE key='last_run'"
        )
        last_run = cur.fetchone()
        cur = conn.execute(
            "SELECT value FROM sms_sync_state WHERE key='total_uploaded'"
        )
        total_up = cur.fetchone()
        cur = conn.execute("SELECT value FROM sms_sync_state WHERE key='last_sms_id'")
        last_id = cur.fetchone()
        conn.close()

        print(f"  sms_cache.db 状态:")
        print(f"    上次运行: {last_run[0] if last_run else '从未运行'}")
        print(f"    累计上传: {total_up[0] if total_up else '0'} 条")
        print(f"    最后 id:  {last_id[0] if last_id else '0'}")
    else:
        print(f"  ⚠ sms_cache.db 不存在 — 采集脚本从未运行过")
        print(f"  → 首次运行: python scripts/sms_collector.py --full")

    # 检查 crontab / termux-job-scheduler
    try:
        proc_status = subprocess.run(
            ["termux-job-scheduler", "-s"],
            capture_output=True, text=True, timeout=10,
        )
        if proc_status.stdout:
            print(f"\n  termux-job-scheduler 输出: {proc_status.stdout.strip()[:200]}")
        else:
            print(f"\n  ⚠ termux-job-scheduler 未返回任务列表")
            print(f"  → 可能需要手动添加定时任务")
    except FileNotFoundError:
        print(f"\n  ⚠ termux-job-scheduler 不可用（未安装 Termux:API 插件？）")
        print(f"  → 推荐用 crond 或 termux-job-scheduler 做定时调度")
    except Exception as e:
        print(f"\n  ⚠ 检查定时任务失败: {e}")

    # ──── 5) 汇总建议 ────
    section("5. 诊断结论与建议")

    issues = []
    if phone_min_id > 10:
        issues.append(
            f"🔴 手机短信 _id 起始 {phone_min_id}，"
            f"可能还有更老的短信未被拉取"
        )
    if server_max_id < phone_max_id and server_max_id > 0:
        issues.append(f"🟡 服务端最大 id ({server_max_id}) < 手机端 ({phone_max_id})，"
                      f"还有短信未上传")
    if not SMS_CACHE_DB.exists():
        issues.append("🔴 采集脚本从未运行过，需执行 --full 首次全量采集")
    if deep_hits > 0:
        issues.append(f"🟡 当前脚本漏掉了 {deep_hits} 条财务短信"
                      f"（补充关键词可挽回）")

    if not issues:
        print("  ✅ 一切正常，无显著问题")
    else:
        for issue in issues:
            print(f"  {issue}")
        print()
        if "采集脚本从未运行过" in str(issues) or not SMS_CACHE_DB.exists():
            print("  👉 推荐操作：")
            print(f"     python {PROJ_ROOT / 'scripts' / 'sms_collector.py'} --full")
        if deep_hits > 0:
            print(f"  👉 推荐在 keywords 里补充: {_EXTRA_KEYWORDS}")

    elapsed = time.time() - start
    print(f"\n诊断耗时: {elapsed:.1f} 秒")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="SMS 采集诊断工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--deep", action="store_true",
                        help="全量比对（遍历手机所有短信，较慢）")
    parser.add_argument("--quick", action="store_true",
                        help="只看统计摘要，不做逐条比对")
    args = parser.parse_args()

    diagnose(deep=args.deep, quick=args.quick)


if __name__ == "__main__":
    main()
