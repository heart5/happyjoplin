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
# # 微信聊天记录财务解析与月报生成

# %%
"""从微信聊天记录中提取消费信息，生成月度财务报告。

数据源：
- 微信支付交易通知（sender 含 "微信支付"）
- 银行卡交易提醒（sender 含银行名或 content 含消费/支出）
- 转账/红包记录

用法：
    python life/wechat_finance.py --account 白晔峰 --month 2026-06
    python life/wechat_finance.py --account 白晔峰 --month prev
"""

import argparse
import json
import logging
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import pathmagic

with pathmagic.context():
    from func.logme import log

# 添加 joplinai 路径以便导入 WeChatClient
JOPLINAI = Path(__file__).resolve().parent.parent.parent / "joplinai"
if str(JOPLINAI) not in sys.path:
    sys.path.insert(0, str(JOPLINAI))

log = logging.getLogger("wechat_finance")

__all__ = [
    "parse_finance_messages",
    "classify_merchant",
    "load_category_map",
    "save_category_map",
    "generate_finance_report",
    "process_month",
]

# ── 正则模式（基于实际数据格式）──

# 微信支付 ¥XX.XX paid 消息 — 最干净的消费记录
RE_PAID = re.compile(r"[￥¥](\d+\.?\d*)\s*paid", re.IGNORECASE)

# 微信支付凭证 — 含商户名的消费记录（付款金额和商品名称之间换行分隔）
RE_WXPAY_RECEIPT = re.compile(
    r"付款金额[￥¥](\d+\.?\d*).*?商品名称(.+?)(?:\n|$)", re.DOTALL
)

# 银行交易提醒中的金额和商户
RE_BANK_AMOUNT = re.compile(r"交易金额[：:]\s*([\d]+\.?\d*)")  # 交易金额：210.00
RE_BANK_MERCHANT = re.compile(r"交易商户[：:]\s*(.+?)(?:\n|$)")

# 收到转账消息
RE_RECEIVED_TRANSFER = re.compile(r"收到转账([\d]+\.?\d*)元")

# 银行卡消费/收入关键词
_RE_BANK_SENDER = re.compile(
    r"(银行|信用卡|信用卡还款|微众银行|网商银行|银盛支付|微信转账助手)"
)
_RE_BANK_CONSUME_KW = ["消费", "支出", "pos", "POS", "快捷"]
_RE_BANK_INCOME_KW = ["收入", "工资", "报销", "转入", "入账", "收益"]
_RE_BANK_SKIP_KW = ["还款", "最后还款日", "账单", "分期", "失败", "退款", "退税"]

# 微信支付排除关键词（非实际消费）
_RE_WXPAY_SKIP = ["零钱提现", "记账月报", "记账日报", "收款月报", "收款周报", "收款汇总"]

# 微信转账助手排除关键词（汇总/月报/周报）
_RE_TRANSFER_SKIP = ["收款月报", "收款周报", "收款日报", "转账收款汇总", "转账即将过期"]

# 标准金额提取
RE_AMOUNT = re.compile(r"[￥¥](\d+\.?\d*)")

# ── 默认商户分类映射 ──

_DEFAULT_CATEGORY_MAP = {
    # 餐饮-外卖
    "美团外卖": "餐饮-外卖", "饿了么": "餐饮-外卖", "百度外卖": "餐饮-外卖",
    # 餐饮-饮品
    "瑞幸": "餐饮-饮品", "星巴克": "餐饮-饮品", "蜜雪冰城": "餐饮-饮品",
    "库迪": "餐饮-饮品", "喜茶": "餐饮-饮品", "霸王茶姬": "餐饮-饮品",
    # 餐饮-正餐
    "海底捞": "餐饮-正餐", "西贝": "餐饮-正餐", "肯德基": "餐饮-正餐",
    "麦当劳": "餐饮-正餐", "必胜客": "餐饮-正餐",
    "冯校长老火锅": "餐饮-正餐", "大宅门夜宵": "餐饮-正餐", "美团收银": "餐饮-外卖",
    "逸富广场": "购物-其他",
    # 购物-便利店
    "美宜佳": "购物-便利店",
    # 交通-网约车
    "滴滴": "交通-网约车", "高德打车": "交通-网约车", "曹操出行": "交通-网约车",
    "T3出行": "交通-网约车", "花小猪": "交通-网约车",
    # 交通-公共交通
    "北京公交": "交通-公共交通", "地铁": "交通-公共交通",
    "铁路": "交通-公共交通", "12306": "交通-公共交通",
    # 交通-加油充电
    "中石化": "交通-加油", "中石油": "交通-加油",
    "特来电": "交通-充电", "星星充电": "交通-充电",
    # 购物-电商
    "京东": "购物-电商", "淘宝": "购物-电商", "天猫": "购物-电商",
    "拼多多": "购物-电商", "得物": "购物-电商", "闲鱼": "购物-电商",
    "唯品会": "购物-电商",
    # 购物-超市
    "永辉": "购物-超市", "盒马": "购物-超市", "物美": "购物-超市",
    "沃尔玛": "购物-超市", "山姆": "购物-超市", "Costco": "购物-超市",
    # 购物-其他
    "名创优品": "购物-其他", "无印良品": "购物-其他", "屈臣氏": "购物-其他",
    "MINISO": "购物-其他",
    # 居住
    "房租": "居住-房租", "物业": "居住-物业",
    "电费": "居住-水电", "水费": "居住-水电", "燃气": "居住-燃气",
    # 医疗
    "叮当快药": "医疗-药品", "药店": "医疗-药品",
    "医院": "医疗-就诊", "诊所": "医疗-就诊",
    # 社交
    "微信红包": "社交-红包", "转账": "社交-转账",
    # 收入
    "工资": "收入-工资", "报销": "收入-报销", "退款": "收入-退款",
}


def load_category_map(path: str = None) -> dict:
    """加载商户分类映射表。"""
    if path is None:
        path = str(Path(__file__).resolve().parent.parent / "data" / "finance_category_map.json")
    p = Path(path)
    if p.exists():
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    return dict(_DEFAULT_CATEGORY_MAP)


def save_category_map(mapping: dict, path: str = None):
    if path is None:
        path = str(Path(__file__).resolve().parent.parent / "data" / "finance_category_map.json")
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)


def classify_merchant(merchant: str, category_map: dict = None) -> str:
    """商户名 → 分类。完全匹配优先，否则按匹配键长度降序模糊匹配（长键优先）。"""
    if not merchant:
        return "未分类-其他"
    if category_map is None:
        category_map = _DEFAULT_CATEGORY_MAP
    merchant = merchant.strip()
    # 完全匹配
    if merchant in category_map:
        return category_map[merchant]
    # 模糊匹配：商户名包含分类键，按键长度降序（避免"交通银行"误夺"交通银行信用卡"）
    for key, cat in sorted(category_map.items(), key=lambda x: -len(x[0])):
        if key in merchant or merchant in key:
            return cat
    return "未分类-其他"


def _parse_amount(text: str) -> float:
    """从文本中提取金额。"""
    nums = RE_AMOUNT.findall(text)
    for n in nums:
        try:
            return float(n.replace(",", ""))
        except ValueError:
            continue
    return 0.0


def _extract_merchant(content: str) -> str:
    """从微信支付凭证中提取商户名。"""
    m = RE_WXPAY_RECEIPT.search(content)
    if m:
        return m.group(2).strip()
    return ""


def _detect_payment_method(content: str) -> str:
    """从 paid 消息中检测支付方式。"""
    if "GDB" in content or "广发" in content:
        return "广发信用卡"
    if "CMB" in content:
        return "招商银行信用卡"
    if "CCB" in content or "建设银行" in content:
        return "建设银行信用卡"
    if "Balance" in content:
        return "零钱"
    return "微信支付"


def _extract_bank_merchant(content: str) -> str:
    """从银行交易提醒中提取商户名。"""
    m = RE_BANK_MERCHANT.search(content)
    if m:
        return m.group(1).strip()
    return ""


def _parse_time_seconds(time_str: str) -> float:
    """将时间字符串转为秒数（用于时间差计算）。"""
    if not time_str:
        return 0.0
    try:
        dt = datetime.strptime(time_str[:19], "%Y-%m-%d %H:%M:%S")
        return dt.timestamp()
    except (ValueError, OSError):
        return 0.0


def _is_bank_sender(sender: str) -> bool:
    """判断 sender 是否为银行/信用卡/金融类。"""
    if not sender:
        return False
    if sender in ("信用卡还款", "微信转账助手"):
        return True
    return bool(_RE_BANK_SENDER.search(sender))


def _collect_finance_events(messages: list) -> list:
    """Phase 1: 遍历所有消息，收集原始财务事件。

    每条事件含 time, amount, source(paid/voucher/bank_expense/bank_income/transfer/redpacket),
    merchant, direction, payment_method 等字段。
    """
    events = []

    for msg in messages:
        sender = msg.get("sender", "")
        content = msg.get("content") or ""
        msg_time = msg.get("time", "")
        msg_type = msg.get("type", "")

        evt = {
            "time": msg_time,
            "amount": 0.0,
            "source": "",
            "merchant": "",
            "direction": "支出",
            "category": "未分类-其他",
            "payment_method": "",
            "source_text": content[:100],
            "raw": msg,
        }

        # ── 零钱提现/充值（微信余额↔银行卡互转）──
        if "零钱提现" in content or "零钱充值" in content:
            m = RE_AMOUNT.search(content)
            if m:
                amt = float(m.group(1))
                if amt > 0:
                    kw = "零钱提现" if "零钱提现" in content else "零钱充值"
                    evt["amount"] = amt
                    evt["source"] = "paid"
                    evt["merchant"] = kw
                    evt["category"] = "内部-转账"
                    evt["payment_method"] = "零钱"
                    events.append(evt)
                    continue

        # ── 微信支付消息 ──
        if sender == "微信支付":
            if any(kw in content for kw in _RE_WXPAY_SKIP):
                continue

            # paid 消息
            m = RE_PAID.search(content)
            if m:
                evt["amount"] = float(m.group(1))
                evt["source"] = "paid"
                evt["payment_method"] = _detect_payment_method(content)
                events.append(evt)
                continue

            # 微信支付凭证
            if "微信支付凭证" in content:
                amt = _parse_amount(content)
                if amt > 0:
                    evt["amount"] = amt
                    evt["source"] = "voucher"
                    evt["merchant"] = _extract_merchant(content)
                    evt["payment_method"] = _detect_payment_method(content)
                    events.append(evt)
                    continue
            continue

        # ── 收到转账消息（个人对个人）──
        m = RE_RECEIVED_TRANSFER.search(content)
        if m:
            amt = float(m.group(1))
            if amt > 0:
                evt["amount"] = amt
                evt["source"] = "transfer"
                # send=1 表示我发起的转账（我转出），send=0 表示我接收的转账
                is_send = msg.get("send", False)
                evt["direction"] = "支出" if is_send else "收入"
                evt["category"] = "社交-转账"
                events.append(evt)
                continue

        # ── 微信转账助手汇总消息（排除）──
        if sender == "微信转账助手":
            if any(kw in content for kw in _RE_TRANSFER_SKIP):
                continue

        # ── 京东白条消息（格式不同：消费金额/服务商户而非交易金额/交易商户）──
        if sender == "京东白条":
            # 失败通知/额度通知/非交易提醒 → 跳过
            if any(kw in content for kw in ("失败通知", "交易到账通知", "账单", "还款", "分期", "红包")):
                continue
            # 交易提醒 → 提取金额和商户
            if "交易提醒" in content:
                m = re.search(r"消费金额[：:](\d+\.?\d*)元", content)
                if m:
                    amt = float(m.group(1))
                else:
                    amt = _parse_amount(content)
                if amt > 0:
                    m2 = re.search(r"服务商户[：:]\s*(.+?)(?:\n|\)|$)", content)
                    merchant = m2.group(1).strip() if m2 else "京东白条"
                    evt["amount"] = amt
                    evt["source"] = "bank_expense"
                    evt["merchant"] = merchant
                    evt["payment_method"] = "京东白条"
                    events.append(evt)
                    continue
            continue

        # ── 银行/信用卡消息 ──
        if _is_bank_sender(sender):
            # 先走跳过关键词（还款/账单/分期/失败/退款/退税等），过滤非消费
            is_skip = any(kw in content for kw in _RE_BANK_SKIP_KW)
            if is_skip:
                continue

            is_consume = any(kw in content for kw in _RE_BANK_CONSUME_KW)
            is_bank_income = any(kw in content for kw in _RE_BANK_INCOME_KW)

            # 消费类（用 RE_BANK_AMOUNT 提取交易金额，避免误取"可用额度"）
            if is_consume:
                m = RE_BANK_AMOUNT.search(content)
                if m:
                    amt = float(m.group(1))
                else:
                    amt = _parse_amount(content)
                if amt > 0:
                    evt["amount"] = amt
                    evt["source"] = "bank_expense"
                    evt["merchant"] = _extract_bank_merchant(content) or sender
                    events.append(evt)
                    continue

            # 收入类（银行主动推送的入账通知，含理财收益）
            if is_bank_income:
                amt = _parse_amount(content)
                if amt > 0:
                    evt["amount"] = amt
                    evt["source"] = "bank_income"
                    evt["direction"] = "收入"
                    events.append(evt)
                    continue
            continue

        # ── 红包消息 ──
        if "红包" in content:
            amt = _parse_amount(content)
            if amt > 0:
                evt["amount"] = amt
                evt["source"] = "redpacket"
                evt["direction"] = "收入" if "收款" in content or "收到" in content else "支出"
                evt["category"] = "社交-红包"
                events.append(evt)
                continue

    # 按时间排序（空时间放最后）
    events.sort(key=lambda e: e["time"] or "9999")
    return events


_MERGE_WINDOW = 30  # 归并时间窗口（秒）


def _find_companions(events: list, start_idx: int, used: list) -> list:
    """在已排序 events 中，查找与 start_idx 事件配对的事件。

    条件：时间差 < _MERGE_WINDOW 秒 + 金额完全匹配。
    返回匹配事件的索引列表。"""
    evt = events[start_idx]
    base_ts = _parse_time_seconds(evt["time"])
    if base_ts <= 0:
        return []

    companions = []
    n = len(events)

    for j in range(start_idx + 1, n):
        if used[j]:
            continue
        other = events[j]
        other_ts = _parse_time_seconds(other["time"])
        if other_ts <= 0:
            continue
        diff = other_ts - base_ts
        if diff > _MERGE_WINDOW:
            break
        if abs(other["amount"] - evt["amount"]) >= 0.01:
            continue

        # paid/voucher 找 bank_expense 补充商户名
        if evt["source"] in ("paid", "voucher"):
            if other["source"] in ("bank_expense", "voucher"):
                companions.append(j)

    return companions


def _build_record(primary: dict, companions: list) -> dict:
    """将主事件和它的 companion 事件合并为一条消费记录。

    优先级：
    - 金额/方向/时间从 primary 取
    - 商户名：bank_expense > voucher > primary 的 merchant
    """
    record = {
        "time": primary["time"],
        "amount": primary["amount"],
        "merchant": primary["merchant"] or "",
        "direction": primary["direction"],
        "category": primary["category"],
        "payment_method": primary.get("payment_method", ""),
        "source_text": primary.get("source_text", ""),
        "raw": primary.get("raw", {}),
    }

    for c in companions:
        # 优先用银行通知的商户名（通常比微信凭证更规范）
        if c["source"] == "bank_expense" and c["merchant"]:
            record["merchant"] = c["merchant"]
        # 其次用凭证的商户名
        elif c["source"] == "voucher" and c["merchant"] and not record["merchant"]:
            record["merchant"] = c["merchant"]
        # 补充支付方式
        if c.get("payment_method"):
            record["payment_method"] = c["payment_method"]

    # 如果仍然没有商户名，用支付方式代替
    if not record["merchant"]:
        pm = record.get("payment_method") or "微信支付"
        if pm not in ("零钱", "微信支付"):
            record["merchant"] = pm

    return record


def parse_finance_messages(messages: list) -> list:
    """从消息列表中提取去重后的财务记录。

    去重策略（Phase 2）：
    - bank_expense 不独立产生记录，只作为已存在记录的商户名补充
    - 同一笔交易的多个通知（paid + voucher + bank）归并为一条记录
    - 归并条件：时间差 < 30秒 + 金额精确匹配
    - 转账按 (sender, amount, 日期) 分组去重，避免合并库双侧数据重复
    """
    events = _collect_finance_events(messages)

    n = len(events)
    used = [False] * n
    records = []

    for i in range(n):
        if used[i]:
            continue
        evt = events[i]

        # bank_expense 不独立产生记录（由对应的 paid/voucher 承载）
        if evt["source"] == "bank_expense":
            continue

        # transfer 由 pass 2 按 (sender, amount, 日期) 分组去重
        if evt["source"] == "transfer":
            continue

        # bank_income 直接产生收入记录（无对应微信支付通知）
        if evt["source"] == "bank_income":
            records.append({
                "time": evt["time"], "sender": evt["raw"].get("sender", ""),
                "amount": evt["amount"], "merchant": evt["merchant"] or evt["raw"].get("sender", ""),
                "direction": "收入", "category": evt["category"],
                "source_text": evt.get("source_text", ""), "raw": evt["raw"],
            })
            used[i] = True
            continue

        # 查找 companion 事件
        companions_idx = _find_companions(events, i, used)
        companion_events = [events[j] for j in companions_idx]
        record = _build_record(evt, companion_events)

        records.append(record)
        used[i] = True
        for j in companions_idx:
            used[j] = True

    # ── Pass 1.5: 未被 paid/voucher 消耗的 bank_expense → 独立支出记录 ──
    # 未绑定微信的银行卡只有银行短信通知，无微信支付消息，
    # 这些 bank_expense 没有 companion，需要独立产生记录
    for i in range(n):
        if not used[i] and events[i]["source"] == "bank_expense":
            evt = events[i]
            records.append({
                "time": evt["time"],
                "amount": evt["amount"],
                "merchant": evt["merchant"] or evt["raw"].get("sender", ""),
                "direction": "支出",
                "category": evt["category"],
                "payment_method": evt["raw"].get("sender", ""),
                "source_text": evt.get("source_text", ""),
                "raw": evt["raw"],
            })
            used[i] = True

    # ── Pass 2: 转账去重 ──
    # 同一笔转账在合并库中可能出现两次（双侧手机导出），
    # 按 (sender, amount, 日期) 分组，只保留一条记录
    transfer_groups = defaultdict(list)
    for i, evt in enumerate(events):
        if not used[i] and evt["source"] == "transfer":
            key = (evt["raw"]["sender"], round(evt["amount"], 2), evt["time"][:10])
            transfer_groups[key].append(i)

    for key, indices in transfer_groups.items():
        for i in indices:
            used[i] = True
        sender, amount, _ = key

        # 确定方向：取最早事件的 send 标志
        # 收入场景：先收到通知 (send=0) → 后再确认 (send=1)
        # 支出场景：我先发起 (send=1) → 对方确认 (send=0)
        earliest_idx = min(indices, key=lambda i: events[i]["time"])
        is_income = not events[earliest_idx]["raw"].get("send")  # send=0 为收入
        primary = events[earliest_idx]

        direction = "收入" if is_income else "支出"

        records.append({
            "time": primary["time"],
            "amount": amount,
            "merchant": sender,
            "direction": direction,
            "category": "社交-转账",
            "source_text": primary.get("source_text", ""),
            "raw": primary["raw"],
        })

    # 排序：按时间
    records.sort(key=lambda r: r.get("time") or "")

    return records


def _trend_icon(current: float, previous: float) -> str:
    if previous <= 0:
        return "—"
    pct = (current - previous) / previous * 100
    if pct > 30:
        return "↑↑"
    if pct > 10:
        return "↑"
    if pct < -30:
        return "↓↓"
    if pct < -10:
        return "↓"
    return "→"


def generate_finance_report(records: list, year: int, month: int, prev_records: list = None) -> str:
    """生成月度财务报告 Markdown。"""
    cat_totals = defaultdict(lambda: {"amount": 0.0, "count": 0})
    merchant_totals = defaultdict(lambda: {"amount": 0.0, "count": 0})
    daily_expense = defaultdict(float)
    daily_income = defaultdict(float)
    total_expense = 0.0
    total_income = 0.0

    for r in records:
        amt = r["amount"]
        cat = r["category"]
        mch = r["merchant"]
        day = r["time"][:10] if r["time"] else "unknown"
        if r["direction"] == "收入":
            total_income += amt
            daily_income[day] += amt
        else:
            total_expense += amt
            cat_totals[cat]["amount"] += amt
            cat_totals[cat]["count"] += 1
            merchant_totals[mch]["amount"] += amt
            merchant_totals[mch]["count"] += 1
            daily_expense[day] += amt

    prev_total = sum(r["amount"] for r in prev_records if r["direction"] != "收入") if prev_records else 0

    lines = []
    lines.append(f"# 微信消费月报 — {year}年{month}月")
    lines.append("")

    # 概要
    lines.append("## 概要")
    lines.append("")
    lines.append("| 指标 | 数值 | 环比 |")
    lines.append("|------|------|------|")
    lines.append(f"| 总支出 | ¥{total_expense:,.2f} | {_trend_icon(total_expense, prev_total)} |")
    lines.append(f"| 总收入 | ¥{total_income:,.2f} | — |")
    lines.append(f"| 交易笔数 | {len(records)} 笔 | — |")
    expense_days = len(daily_expense)
    lines.append(f"| 日均消费 | ¥{total_expense / max(1, expense_days):,.2f} | — |")
    if total_expense > 0:
        max_amt = max(r["amount"] for r in records if r["direction"] != "收入")
        max_mch = next((r["merchant"] for r in records if r["amount"] == max_amt and r["direction"] != "收入"), "")
        lines.append(f"| 最大单笔 | ¥{max_amt:,.2f} ({max_mch}) | — |")
    lines.append("")

    # 分类排行
    lines.append("## 分类支出排行")
    lines.append("")
    sorted_cats = sorted(cat_totals.items(), key=lambda x: x[1]["amount"], reverse=True)
    lines.append("| 分类 | 金额 | 占比 | 笔数 |")
    lines.append("|------|------|------|------|")
    for cat, vals in sorted_cats:
        pct = vals["amount"] / total_expense * 100 if total_expense > 0 else 0
        lines.append(f"| {cat} | ¥{vals['amount']:,.2f} | {pct:.1f}% | {vals['count']} |")
    lines.append("")

    # 商户排行（仅支出）
    lines.append("## 商户消费排行 Top 10")
    lines.append("")
    sorted_mchs = sorted(merchant_totals.items(), key=lambda x: x[1]["amount"], reverse=True)[:10]
    lines.append("| 排名 | 商户 | 金额 | 笔数 | 分类 |")
    lines.append("|------|------|------|------|------|")
    for i, (mch, vals) in enumerate(sorted_mchs, 1):
        cat = "未分类"
        for r in records:
            if r["merchant"] == mch:
                cat = r["category"]
                break
        lines.append(f"| {i} | {mch} | ¥{vals['amount']:,.2f} | {vals['count']} | {cat} |")
    lines.append("")

    # 日支出分布
    lines.append("## 日支出分布")
    lines.append("")
    sorted_days = sorted(daily_expense.items())
    if sorted_days:
        max_daily = max(v for _, v in sorted_days)
        lines.append("```")
        for day, amt in sorted_days:
            bar_len = max(1, int(amt / max(max_daily, 1) * 30))
            bar = "█" * bar_len
            lines.append(f"  {day[-5:]}  {bar}  ¥{amt:,.2f}")
        lines.append("```")
    lines.append("")

    # 未分类交易
    uncategorized = [r for r in records if r["category"].startswith("未分类")]
    if uncategorized:
        lines.append("## 未分类交易（待确认）")
        lines.append("")
        lines.append("| 时间 | 商户 | 金额 | 原文 |")
        lines.append("|------|------|------|------|")
        for r in uncategorized[:20]:
            lines.append(f"| {r['time'][:10]} | {r['merchant']} | ¥{r['amount']:,.2f} | {r['source_text'][:40]} |")
        if len(uncategorized) > 20:
            lines.append(f"| ... | 还有 {len(uncategorized) - 20} 条 | ... | ... |")
        lines.append("")

    # 分析建议
    lines.append("## 分析与建议")
    lines.append("")
    if sorted_cats:
        top_cat = sorted_cats[0]
        lines.append(f"- 本月 **{top_cat[0]}** 支出最高，达 ¥{top_cat[1]['amount']:,.2f}，"
                     f"占总支出的 {top_cat[1]['amount']/total_expense*100:.1f}%")
    if total_expense > 0 and prev_total > 0:
        change = (total_expense - prev_total) / prev_total * 100
        if abs(change) > 10:
            direction = "增长" if change > 0 else "下降"
            lines.append(f"- 总支出环比{direction} {abs(change):.1f}%")
    if uncategorized:
        lines.append(f"- 发现 {len(uncategorized)} 笔未分类交易，建议补充商户分类映射")
    lines.append("")

    lines.append("---")
    lines.append("")
    lines.append(f"*报告生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}*")
    lines.append("*数据来源：微信聊天记录合并库*")
    lines.append("")

    return "\n".join(lines)


def process_month(account: str, year: int, month: int, client=None) -> str:
    """处理指定月份，返回 Markdown 报告。"""
    if client is None:
        from aimod.wechat_client import WeChatClient
        client = WeChatClient(mode="local", db_path=str(JOPLINAI / "data" / "wcitemsall_merged.db"))

    date_from = f"{year}-{month:02d}-01"
    if month == 12:
        date_to = f"{year + 1}-01-01"
    else:
        date_to = f"{year}-{month + 1:02d}-01"

    all_msgs = client.query(account, date_from=date_from, date_to=date_to, limit=50000)
    log.info(f"{year}-{month:02d}: 拉取 {len(all_msgs)} 条消息")

    records = parse_finance_messages(all_msgs)
    log.info(f"{year}-{month:02d}: 识别 {len(records)} 条财务记录")

    cat_map = load_category_map()
    for r in records:
        if r["category"] == "未分类-其他":
            r["category"] = classify_merchant(r["merchant"], cat_map)

    prev_records = []
    prev_month = month - 1 if month > 1 else 12
    prev_year = year if month > 1 else year - 1
    try:
        prev_from = f"{prev_year}-{prev_month:02d}-01"
        prev_to = f"{prev_year}-{prev_month + 1:02d}-01" if prev_month < 12 else f"{prev_year + 1}-01-01"
        prev_msgs = client.query(account, date_from=prev_from, date_to=prev_to, limit=50000)
        prev_records = parse_finance_messages(prev_msgs)
        for r in prev_records:
            if r["category"] == "未分类-其他":
                r["category"] = classify_merchant(r["merchant"], cat_map)
    except Exception as e:
        log.warning(f"加载上月数据失败: {e}")

    report = generate_finance_report(records, year, month, prev_records)
    return report


def main():
    parser = argparse.ArgumentParser(description="微信消费月报生成")
    parser.add_argument("--account", default="白晔峰")
    parser.add_argument("--month", help="月份 YYYY-MM 或 prev")
    parser.add_argument("--output", "-o", help="输出到文件（默认 stdout）")
    args = parser.parse_args()

    now = datetime.now()
    if args.month == "prev":
        first = now.replace(day=1) - timedelta(days=1)
        year, month = first.year, first.month
    elif args.month:
        parts = args.month.split("-")
        year, month = int(parts[0]), int(parts[1])
    else:
        year, month = now.year, now.month

    log.info(f"开始处理 {year}-{month:02d} 财务月报")
    report = process_month(args.account, year, month)

    if args.output:
        Path(args.output).write_text(report, encoding="utf-8")
        print(f"报告已保存: {args.output}")
    else:
        print(report)

    log.info(f"财务月报 {year}-{month:02d} 完成")


if __name__ == "__main__":
    main()
