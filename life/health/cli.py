# 健康数据报告命令入口

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

from . import db, memos, publish


def _out_dir(prefix: str) -> Path:
    path = Path.home() / "tmp" / "health_reports" / f"{prefix}_{datetime.now():%Y%m%d_%H%M%S}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _publish(pub, title, body, charts, out_dir, dry_run, force, alert=False):
    status = pub.publish(title, body, charts, dry_run=dry_run, force=force, out_dir=out_dir)
    print(f"{status:<8} {title}")
    return status


def _parse_month(value: str) -> tuple[int, int]:
    year, month = value.split("-", 1)
    return int(year), int(month)


def _common(parser):
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="健康数据报告（华为健康 → Joplin）")
    sub = parser.add_subparsers(dest="command", required=True)

    p_memos = sub.add_parser("memos", help="重扫旧笔记随记并发布汇编")
    p_memos.add_argument("--refresh", action="store_true")
    _common(p_memos)

    p_daily = sub.add_parser("daily", help="滚动日报")
    p_daily.add_argument("--date")
    _common(p_daily)

    p_weekly = sub.add_parser("weekly", help="周报")
    p_weekly.add_argument("--week")
    _common(p_weekly)

    p_monthly = sub.add_parser("monthly", help="月报")
    p_monthly.add_argument("--month")
    _common(p_monthly)

    p_overview = sub.add_parser("overview", help="全历史总览")
    _common(p_overview)

    p_status = sub.add_parser("status", help="系统状态")
    p_status.add_argument("--alert", action="store_true")
    _common(p_status)

    p_all = sub.add_parser("all", help="日报 + 周报 + 月报 + 总览 + 状态")
    _common(p_all)

    args = parser.parse_args(argv)
    out_dir = _out_dir(args.command)
    pub = publish.HealthPublisher()
    conn = db.connect()

    try:
        if args.command == "memos":
            if args.refresh:
                stats = memos.refresh_memos()
                print(f"memos refreshed: {stats}")
            body = memos.build_compilation()
            _publish(pub, publish.MEMOS_TITLE, body, {}, out_dir,
                     args.dry_run, args.force)
            return 0

        if args.command in ("daily", "all"):
            target = date.fromisoformat(args.date) if getattr(args, "date", None) else None
            body, charts = publish.build_daily(conn, out_dir, target)
            if body is None:
                print("skip 日报：最新数据超过 2 天或尚无数据")
            else:
                _publish(pub, publish.DAILY_TITLE, body, charts, out_dir,
                         args.dry_run, args.force)
            if args.command == "daily":
                return 0

        if args.command in ("weekly", "all"):
            year = week = None
            if getattr(args, "week", None):
                year, week = publish._parse_week(args.week)
            body, charts = publish.build_weekly(conn, out_dir, year, week)
            if body is None:
                print("skip 周报：该周无睡眠数据")
            else:
                iso = (year, week) if year else publish.default_week(date.today())
                title = f"健康数据周报｜{iso[0]}年第{iso[1]}周"
                _publish(pub, title, body, charts, out_dir, args.dry_run, args.force)
            if args.command == "weekly":
                return 0

        if args.command in ("monthly", "all"):
            year = month = None
            if getattr(args, "month", None):
                year, month = _parse_month(args.month)
            body, charts = publish.build_monthly(conn, out_dir, year, month)
            if body is None:
                print("skip 月报：该月无睡眠数据")
            else:
                if year is None:
                    year, month = publish.default_month(date.today())
                title = f"健康数据月报｜{year}年{month:02d}月"
                _publish(pub, title, body, charts, out_dir, args.dry_run, args.force)
            if args.command == "monthly":
                return 0

        if args.command in ("overview", "all"):
            body, charts = publish.build_overview(conn, out_dir)
            if body is None:
                print("skip 总览：尚无睡眠数据")
            else:
                _publish(pub, publish.OVERVIEW_TITLE, body, charts, out_dir,
                         args.dry_run, args.force)

        if args.command in ("status", "all"):
            alert = bool(getattr(args, "alert", False))
            body, _ = publish.build_status(conn, alert)
            _publish(pub, publish.STATUS_TITLE, body, {}, out_dir,
                     args.dry_run, args.force)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
