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
# # SMS 财务消息采集脚本（Termux 端）
#
# 定时扫描手机短信，过滤财务相关消息，增量上传 HCX 服务端。
# 复用 func 子模块的 termux_sms_list、configpr、logme 等工具。

# %%
"""
手机 Termux 端 SMS 财务消息采集与上传。

数据流：
    termux-sms-list → 关键词过滤 → 增量去重 → HTTP POST → HCX 服务端

用法：
    python scripts/sms_collector.py               # 增量（默认）
    python scripts/sms_collector.py --full         # 全量扫描（首次运行）
    python scripts/sms_collector.py --dry-run      # 试跑，只看不传
    python scripts/sms_collector.py --stats        # 统计短信总量

首次运行（--full）：
    termux-sms-list 拉取最近 5000 条短信 → 全部遍历 → 逐批上传 → 记录 last_sms_id。
    手机短信约 2-5 元/MB 流量的按量计费场景，建议首次在 WiFi 下执行。

增量运行（默认）：
    拉取最近 500 条 → 按 last_sms_id 跳过已处理 → 新消息上传。
    每 30 分钟 cron 触发，单次流量 < 100KB。
"""

import json
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

# 项目路径引导
import pathmagic

with pathmagic.context():
    from func.configpr import getcfpoptionvalue, setcfpoptionvalue
    from func.logme import log
    from func.sysfunc import is_tool_valid
    from func.termuxtools import termux_sms_list

log = log  # 让静态检查器闭嘴

__all__ = ["SMSCollector", "main"]

# ── 默认配置（由 INI 文件覆盖）──

DEFAULT_CONFIG = {
    "hcx_url": "https://long9.org/sms/upload",
    "api_key": "",
    "batch_size": "200",       # 单批上传条数
    "full_fetch_limit": "5000",  # 全量拉取上限
    "incr_fetch_limit": "500",    # 增量拉取上限
}

INI_FILE = "happyjphard"
INI_SECTION = "sms_collector"

# ── 财务消息过滤 ──

# 银行/金融发件人短号
_BANK_SHORT_CODES = [
    "95555", "95533", "95508", "95559", "95595", "95558",  # 招/建/广/交/光/中信
    "95588", "95599", "95566", "95577", "95561",            # 工/农/中/华/兴业
    "95568", "95528", "95526", "95580",                     # 民生/浦发/光大…/邮储
    "95511", "95519", "95522",                              # 平安/人寿/泰康（保险）
]

# 财务关键词（匹配 body）
_FINANCE_KEYWORDS = [
    "消费", "支出", "收入", "转账", "余额",
    "￥", "¥", "人民币", "元",
    "交易", "支付", "退款", "退税",
    "信用卡", "储蓄卡", "银行卡", "借记卡",
    "工资", "报销", "理财", "收益",
    "分期", "账单", "额度", "还款",
    "存入", "支取", "汇款", "到账",
    "扣款", "放款", "代扣", "逾期", "拖欠",
]


def _is_finance_msg(msg: dict) -> bool:
    """判断单条短信是否为财务相关。

    匹配规则：
    1. 发件人号码为银行/金融短号
    2. 正文含财务关键词
    """
    number = str(msg.get("number", ""))
    body = str(msg.get("body", ""))

    if not body:
        return False

    # 发件人是银行短号
    if any(code in number for code in _BANK_SHORT_CODES):
        return True

    # 正文含财务关键词
    if any(kw in body for kw in _FINANCE_KEYWORDS):
        return True

    return False


# ── 持久化（sqlite3 本地缓存）──


class SMSCache:
    """跟踪已上传的 SMS id，避免重复上传。

    库文件：data/sms_cache.db
    表：
      sms_sync_state — 单行状态（last_sms_id, last_run 等）
      sms_archive    — 已上传消息的简要归档（可选，仅保留 id + 摘要）
    """

    def __init__(self, db_path: str = None):
        if db_path is None:
            db_path = str(Path(__file__).resolve().parent.parent / "data" / "sms_cache.db")
        self.db_path = db_path
        self._init_db()

    def _conn(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        conn = self._conn()
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sms_sync_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sms_archive (
                id INTEGER PRIMARY KEY,
                address TEXT,
                body_preview TEXT,
                received TEXT,
                uploaded_at TEXT
            )
        """)
        conn.commit()
        conn.close()

    def get_last_id(self) -> int:
        """获取上次已上传的最大 SMS _id。"""
        conn = self._conn()
        row = conn.execute(
            "SELECT value FROM sms_sync_state WHERE key='last_sms_id'"
        ).fetchone()
        conn.close()
        return int(row[0]) if row else 0

    def set_last_id(self, id_val: int):
        """更新已上传的最大 SMS _id。"""
        conn = self._conn()
        conn.execute(
            "INSERT OR REPLACE INTO sms_sync_state (key, value) VALUES ('last_sms_id', ?)",
            (str(id_val),)
        )
        conn.commit()
        conn.close()

    def get_stat(self, key: str, default: str = "") -> str:
        conn = self._conn()
        row = conn.execute(
            "SELECT value FROM sms_sync_state WHERE key=?", (key,)
        ).fetchone()
        conn.close()
        return row[0] if row else default

    def set_stat(self, key: str, value: str):
        conn = self._conn()
        conn.execute(
            "INSERT OR REPLACE INTO sms_sync_state (key, value) VALUES (?, ?)",
            (key, value)
        )
        conn.commit()
        conn.close()

    def archive_sms(self, sms_list: list):
        """归档已上传的短信（轻量记录）。"""
        conn = self._conn()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for m in sms_list:
            body = str(m.get("body", ""))[:80]
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO sms_archive (id, address, body_preview, received, uploaded_at) VALUES (?,?,?,?,?)",
                    (int(m["_id"]), str(m.get("number", "")), body, str(m.get("received", "")), now)
                )
            except (ValueError, KeyError):
                continue
        conn.commit()
        conn.close()

    @property
    def archive_count(self) -> int:
        conn = self._conn()
        cnt = conn.execute("SELECT COUNT(*) FROM sms_archive").fetchone()[0]
        conn.close()
        return cnt


# ── 采集器 ──


class SMSCollector:
    """SMS 采集器。

    职责：
    - 调用 termux_sms_list 读取短信
    - 过滤财务消息
    - 按 last_sms_id 增量去重
    - 上传 HCX
    - 记录状态
    """

    def __init__(self, config: dict = None):
        self.config = dict(DEFAULT_CONFIG)
        self._load_config()
        if config:
            self.config.update(config)

        self.cache = SMSCache()
        self.session_start = time.time()

    def _load_config(self):
        """从 INI 文件加载配置，覆盖默认值。"""
        for key in DEFAULT_CONFIG:
            val = getcfpoptionvalue(INI_FILE, INI_SECTION, key)
            if val:
                self.config[key] = val

    # ── 短信获取 ──

    def fetch_messages(self, full_scan: bool = False) -> list:
        """获取短信并按财务关键词过滤。

        参数：
            full_scan: True=首次全量（拉取上限 full_fetch_limit 条）
                       False=增量（拉取 incr_fetch_limit 条）

        返回：过滤后的财务消息列表（已按 last_sms_id 去重）
        """
        if not is_tool_valid("termux-sms-list"):
            log.error("termux-sms-list 不可用，请安装 Termux:API")
            return []

        last_id = self.cache.get_last_id()

        if full_scan or last_id == 0:
            # 首次 / 全量模式：拉取大量数据
            fetch_limit = int(self.config["full_fetch_limit"])
            log.info(f"全量扫描模式：拉取最近 {fetch_limit} 条短信")
        else:
            fetch_limit = int(self.config["incr_fetch_limit"])
            log.info(f"增量扫描：拉取最近 {fetch_limit} 条，上次已处理至 id={last_id}")

        try:
            raw_sms = termux_sms_list(num=fetch_limit)
        except Exception as e:
            log.error(f"termux-sms-list 调用失败: {e}")
            return []

        if not raw_sms:
            log.info("无短信返回")
            return []

        # termux_sms_list 的 evaloutput 不处理 JSON 数组，
        # 返回字符串时手动解析
        if isinstance(raw_sms, str):
            try:
                raw_sms = json.loads(raw_sms)
            except (json.JSONDecodeError, TypeError) as e:
                log.warning(f"JSON 解析失败: {e}")
                return []

        if not isinstance(raw_sms, list):
            log.warning(f"返回格式异常: {type(raw_sms)}")
            return []

        # 按 id 去重（跳过已处理的消息）
        new_sms = [m for m in raw_sms if int(m["_id"]) > last_id]

        if not new_sms:
            log.info("无新短信")
            return []

        # 过滤财务消息
        finance_sms = [m for m in new_sms if _is_finance_msg(m)]

        skipped = len(new_sms) - len(finance_sms)
        log.info(
            f"短信: 总计{len(raw_sms)}条, 新{len(new_sms)}条, "
            f"财务{len(finance_sms)}条, 过滤跳过{skipped}条"
        )

        return finance_sms

    # ── 上传 ──

    def upload_batch(self, batch: list) -> bool:
        """上传一批短信到 HCX。"""
        hcx_url = self.config["hcx_url"]
        api_key = self.config.get("api_key", "")

        if not hcx_url:
            log.warning("HCX_URL 未配置，跳过上传")
            return False

        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["X-API-Key"] = api_key

        payload = {
            "messages": batch,
            "source": "termux",
            "client_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        try:
            import requests
            resp = requests.post(hcx_url, json=payload, headers=headers, timeout=60)
            resp.raise_for_status()
            result = resp.json()
            log.info(f"上传成功: {result.get('imported', '?')} 条入账")
            return True
        except ImportError:
            log.error("requests 库未安装，无法上传")
            return False
        except Exception as e:
            log.error(f"上传失败: {e}")
            return False

    def run(self, full_scan: bool = False, dry_run: bool = False) -> dict:
        """主执行流程。

        返回统计信息 dict。
        """
        stats = {
            "fetched": 0, "uploaded": 0, "batches": 0,
            "errors": 0, "duration_seconds": 0,
        }

        messages = self.fetch_messages(full_scan=full_scan)
        stats["fetched"] = len(messages)

        if not messages:
            stats["duration_seconds"] = round(time.time() - self.session_start, 1)
            return stats

        # 按 id 排序（旧→新），确保 last_sms_id 单调递增
        messages.sort(key=lambda m: int(m["_id"]))

        # 分批上传
        batch_size = int(self.config["batch_size"])
        max_id = 0

        for start in range(0, len(messages), batch_size):
            batch = messages[start:start + batch_size]
            stats["batches"] += 1

            # 更新本批次 max_id
            batch_max = max(int(m["_id"]) for m in batch)
            if batch_max > max_id:
                max_id = batch_max

            if dry_run:
                log.info(f"[试跑] 批次{stats['batches']}: {len(batch)} 条, max_id={batch_max}")
                stats["uploaded"] += len(batch)
                continue

            ok = self.upload_batch(batch)
            if ok:
                stats["uploaded"] += len(batch)
                # 每批成功就推进 last_sms_id（断点续传保障）
                self.cache.set_last_id(max_id)
                self.cache.archive_sms(batch)
            else:
                stats["errors"] += len(batch)
                log.warning(f"批次上传失败，后续批次取消（断点保护，下次重试）")
                break

        # 更新运行统计
        self.cache.set_stat("last_run", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        self.cache.set_stat("total_uploaded", str(self.cache.archive_count))

        stats["duration_seconds"] = round(time.time() - self.session_start, 1)
        log.info(
            f"采集完成: 获取{stats['fetched']}条, "
            f"上传{stats['uploaded']}条, "
            f"错误{stats['errors']}条, "
            f"耗时{stats['duration_seconds']}秒"
        )
        return stats


# ── CLI ──


def _show_stats():
    """显示短信统计。"""
    cache = SMSCache()
    last_id = cache.get_last_id()
    last_run = cache.get_stat("last_run", "从未运行")
    total = cache.archive_count

    print(f"=== SMS 采集统计 ===")
    print(f"  上次运行:  {last_run}")
    print(f"  最后 id:   {last_id}")
    print(f"  累计上传:  {total} 条")
    print(f"  缓存库:    {cache.db_path}")

    # 尝试获取手机端总量
    if is_tool_valid("termux-sms-list"):
        try:
            all_sms = termux_sms_list(num=1)
            print(f"  手机端最新 id: {all_sms[0]['_id'] if all_sms else 'N/A'}")
        except Exception:
            pass


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="SMS 财务消息采集（Termux 端）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--full", action="store_true", help="全量扫描（首次运行）")
    parser.add_argument("--dry-run", action="store_true", help="试跑，不上传")
    parser.add_argument("--stats", action="store_true", help="显示统计")
    parser.add_argument("--batch-size", type=int, help="单批上传条数（覆盖 INI 配置）")
    parser.add_argument("--config", help="从指定 INI 文件读取配置（不含 .ini 后缀）")
    args = parser.parse_args()

    if args.stats:
        _show_stats()
        return

    config = {}
    if args.batch_size:
        config["batch_size"] = str(args.batch_size)

    collector = SMSCollector(config=config)
    stats = collector.run(full_scan=args.full, dry_run=args.dry_run)

    if args.dry_run:
        print(f"\n[试跑] 可上传 {stats['fetched']} 条，共 {stats['batches']} 批")
    else:
        print(f"\n采集完成: {stats['uploaded']}/{stats['fetched']} 条已上传，耗时{stats['duration_seconds']}秒")


if __name__ == "__main__":
    main()
