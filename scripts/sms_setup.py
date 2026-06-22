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
# # SMS 采集一键设置
#
# 手机 Termux 上跑一次，完成：
# 1. 探测手机短信总量，自动决定拉取上限
# 2. 全量重跑采集（更新后的关键词）
# 3. 打印 crontab 配置指导

# %%
"""
SMS 采集一键设置（手机 Termux 端）。

用法：
    python scripts/sms_setup.py
    python scripts/sms_setup.py --limit N
    python scripts/sms_setup.py --dry-run
"""

import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROJ_ROOT = HERE.parent


def step(title):
    print()
    print("=" * 60)
    print(f"  [{title}]")
    print("=" * 60)


def probe_volume(max_limit=50000):
    for limit in [5000, 10000, 20000, 50000]:
        if limit > max_limit:
            break
        print(f"  拉取 {limit} 条...", end=" ", flush=True)
        try:
            r = subprocess.run(
                ["termux-sms-list", "-d", "-l", str(limit), "-n"],
                capture_output=True, text=True, timeout=120,
            )
            data = json.loads(r.stdout)
            ids = [int(m["_id"]) for m in data]
            print(f"{len(data)} 条, _id {min(ids)} ~ {max(ids)}")
            if len(data) < limit:
                return len(data), min(ids)
        except Exception as e:
            print(f"失败: {e}")
            break
    return None, None


def main():
    import argparse
    parser = argparse.ArgumentParser(description="SMS 采集一键设置")
    parser.add_argument("--limit", type=int, help="强制拉取上限")
    parser.add_argument("--dry-run", action="store_true", help="只探测不上传")
    parser.add_argument("--no-scan", action="store_true", help="跳过探测直接重采")
    args = parser.parse_args()

    # 1) 探测
    step("1/3: 短信总量探测")
    if not args.no_scan and not args.limit:
        total, min_id = probe_volume()
        if total:
            fetch_limit = max(total + 1000, 10000)
            if min_id and min_id > 10:
                print(f"  ⚠ 最老 _id={min_id}，更早的短信已不在手机上")
        else:
            fetch_limit = 5000
            print("  ⚠ 探测失败，使用默认 5000")
    else:
        fetch_limit = args.limit or 5000
    print(f"  → 拉取上限: {fetch_limit} 条")

    # 2) 全量采集
    step(f"2/3: 全量采集（上限 {fetch_limit} 条）")
    cmd = [
        sys.executable,
        str(PROJ_ROOT / "scripts" / "sms_collector.py"),
        "--full", "--limit", str(fetch_limit),
    ]
    if args.dry_run:
        cmd.append("--dry-run")
    r = subprocess.run(cmd)
    if r.returncode != 0:
        print(f"❌ 采集失败")
        sys.exit(1)

    # 3) Crontab
    step("3/3: 定时任务")
    r = subprocess.run(["crontab", "-l"], capture_output=True, text=True, timeout=10)
    if "sms_collector" in r.stdout:
        print("  ✓ crontab 已有 sms_collector 任务：")
        for line in r.stdout.splitlines():
            if "sms_collector" in line:
                print(f"    {line}")
    else:
        print("  ⚠ 未找到定时任务，请添加：")
        print()
        print(f"  crontab -e")
        print(f"  */30 * * * * cd {PROJ_ROOT} && python scripts/sms_collector.py >> data/sms_collector.log 2>&1")


if __name__ == "__main__":
    main()
