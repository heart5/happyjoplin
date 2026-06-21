"""财务系统 — 云端配置读取（回落硬编码默认值）。

用法：
    from .cloudcfg import get_sms_api_url, get_bank_short_codes, ...

设计：
    - 进程级一次性加载，首次 get_*() 触发 _load_from_cloud()
    - Joplin API 不可达时静默失败，返回硬编码默认值
    - 复杂结构（映射/列表）用 JSON 值存储
"""

import json
from configparser import ConfigParser

import pathmagic

with pathmagic.context():
    from func.first import getdirmain
    from func.jpfuncs import readinifromcloud
    from func.logme import log


# ── 硬编码默认值（云端不可用时的回退）──

_DEFAULT_SMS_API_URL = "https://ollama.qingxd.com/sms/query"
_DEFAULT_WECHAT_API_URL = "https://ollama.qingxd.com/wechat/query"
_DEFAULT_MERGE_WINDOW = 30

_DEFAULT_BANK_SHORT_CODES = {
    "95555": "招商银行", "95588": "工商银行", "95599": "农业银行",
    "95566": "中国银行", "95559": "交通银行", "95533": "建设银行",
    "95508": "广发银行", "95595": "光大银行", "95568": "民生银行",
    "95528": "浦发银行", "95558": "中信银行", "95577": "华夏银行",
    "95561": "兴业银行", "95580": "邮储银行", "95511": "平安银行",
}

_DEFAULT_BANK_NAMES = [
    "招商银行", "工商银行", "农业银行", "中国银行", "交通银行",
    "建设银行", "广发银行", "光大银行", "民生银行", "浦发银行",
    "中信银行", "华夏银行", "兴业银行", "邮储银行", "平安银行",
    "微众银行", "网商银行",
]

_DEFAULT_LOAN_PLATFORMS = [
    "洋钱罐", "小赢卡贷", "宜享花", "招联金融", "中邮消金",
    "京东金融", "花呗", "借呗", "微粒贷", "木吉网络",
    "美团月付", "美团借钱", "分期乐", "安逸花", "马上消费",
    "京东白条", "网银在线京东白条",
]

_DEFAULT_LOAN_DISBURSEMENT_KEYWORDS = ["放款", "借款到账", "贷款发放", "借款成功", "借款已到账"]
_DEFAULT_LOAN_REPAYMENT_KEYWORDS = ["还款", "自动扣款", "扣款"]

_DEFAULT_PAYMENT_METHOD_MAP = {
    "广发信用卡": ("bank_credit", "广发银行"),
    "招商银行信用卡": ("bank_credit", "招商银行"),
    "建设银行信用卡": ("bank_credit", "建设银行"),
    "工商银行信用卡": ("bank_credit", "工商银行"),
    "农业银行信用卡": ("bank_credit", "农业银行"),
    "中国银行信用卡": ("bank_credit", "中国银行"),
    "交通银行信用卡": ("bank_credit", "交通银行"),
    "零钱": ("wechat_wallet", None),
    "微信支付": ("wechat_wallet", None),
    "中国银行": ("bank_debit", "中国银行"),
    "交通银行": ("bank_debit", "交通银行"),
    "农业银行": ("bank_debit", "农业银行"),
    "广发银行": ("bank_debit", "广发银行"),
    "京东白条": ("loan", "京东白条"),
    "南京银行(白条分分卡)": ("loan", "南京银行"),
}

# ── 进程级缓存 ──

_cache = {}
_cache_loaded = False


def _load_from_cloud():
    """从云端 INI [finance] 节加载配置到 _cache。"""
    global _cache
    try:
        readinifromcloud()
        ini_path = getdirmain() / "data" / "happyjpinifromcloud.ini"
        if not ini_path.exists():
            return
        cp = ConfigParser()
        cp.read(str(ini_path))
        if not cp.has_section("finance"):
            return
        for key, raw_value in cp.items("finance"):
            value = raw_value.strip()
            if not value:
                continue
            try:
                _cache[key] = json.loads(value)
            except (json.JSONDecodeError, TypeError):
                _cache[key] = value
    except Exception:
        log.warning("云端配置 [finance] 加载失败，使用默认值", exc_info=True)


def _ensure_loaded():
    global _cache_loaded
    if not _cache_loaded:
        _load_from_cloud()
        _cache_loaded = True


# ── 类型化访问器 ──

def get_sms_api_url() -> str:
    _ensure_loaded()
    return str(_cache.get("sms_api_url", _DEFAULT_SMS_API_URL))


def get_wechat_api_url() -> str:
    _ensure_loaded()
    return str(_cache.get("wechat_api_url", _DEFAULT_WECHAT_API_URL))


def get_merge_window() -> int:
    _ensure_loaded()
    return int(_cache.get("merge_window", _DEFAULT_MERGE_WINDOW))


def get_bank_short_codes() -> dict:
    _ensure_loaded()
    return _cache.get("bank_short_codes", dict(_DEFAULT_BANK_SHORT_CODES))


def get_bank_names() -> list:
    _ensure_loaded()
    return _cache.get("bank_names", list(_DEFAULT_BANK_NAMES))


def get_loan_platforms() -> list:
    _ensure_loaded()
    return _cache.get("loan_platforms", list(_DEFAULT_LOAN_PLATFORMS))


def get_loan_disbursement_keywords() -> list:
    _ensure_loaded()
    return _cache.get("loan_disbursement_keywords", list(_DEFAULT_LOAN_DISBURSEMENT_KEYWORDS))


def get_loan_repayment_keywords() -> list:
    _ensure_loaded()
    return _cache.get("loan_repayment_keywords", list(_DEFAULT_LOAN_REPAYMENT_KEYWORDS))


def get_payment_method_map() -> dict:
    _ensure_loaded()
    return _cache.get("payment_method_map", dict(_DEFAULT_PAYMENT_METHOD_MAP))
