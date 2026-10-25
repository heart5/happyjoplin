#!/usr/bin/python
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
# # IP信息更新工具
#
# 功能：读取设备IP/WiFi变化日志，分析并生成Markdown报告，更新至Joplin笔记。
#
# 结构：配置装载（load_config）→ 解析（parse_ip_log_file）→ 分析（analyze_ip_data）
# → 归属地反查（resolve_ip_geos）→ 实时信息采集（collect_live_network_details / collect_live_location）
# → 图表（render_chart）→ 报告（render_report）
# → 变化检测（detect_ip_changes）→ 笔记定位（resolve_note）→ 资源替换与发布（sync_note_resources）。

# %% [markdown]
# ## 导入依赖库

# %%
import io
import json
import math
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union

import matplotlib.pyplot as plt
import pandas as pd
import requests
from matplotlib.ticker import MaxNLocator

plt.switch_backend("Agg")

# %%
try:
    import pathmagic

    with pathmagic.context():
        from func.configpr import getcfpoptionvalue, setcfpoptionvalue
        from func.getid import getdeviceid, gethostuser
        from func.jpfuncs import (
            add_resource_from_bytes,
            createnote,
            extract_resource_ids_from_note,
            getinivaluefromcloud,
            getnote,
            jpapi,
            searchnotebook,
            searchnotes,
            updatenote_body,
            updatenote_title,
        )
        from func.logme import log
        from func.sysfunc import not_IPython
        from func.wrapfuncs import timethis
except ImportError as e:
    print(f"导入模块失败: {e}", file=sys.stderr)
    raise SystemExit(2) from e

# %% [markdown]
# ## 配置常量与数据结构

# %%
CONFIG_NAME = "happyjpip"
IP_UPDATE_CONFIG_SECTION = "ip_update_status"
DEFAULT_NOTEBOOK = "ewmobile"
DEFAULT_REPORT_DAYS = 7
RECENT_LOG_LIMIT = 15
WIFI_HOTSPOT_LIMIT = 7
CHART_PLACEHOLDER = "*(图表已更新至笔记附件)*"
# 报告标题的「（更新于…）」后缀（须完整闭合到结尾；新格式全角/旧格式半角括号均认）
TITLE_UPDATE_SUFFIX_RE = re.compile(r"^\s*[（(]更新于[^）)]*[）)]\s*$")
GEO_API_URL = "http://ip-api.com/json/{ip}"
GEO_API_FIELDS = "status,country,regionName,city,isp"
GEO_CACHE_SECTION = "geo_cache"
GEO_CACHE_KEY = "ip_geos"
GEO_CACHE_LIMIT = 200
PLACES_URL = "https://loc.xilong9.com/places"
OSM_LINK_TEMPLATE = "https://www.openstreetmap.org/?mlat={lat}&mlon={lng}&zoom=15"
GPSLOGGER_PACKAGE = "com.mendhak.gpslogger"
GPSLOGGER_MAX_AGE_SEC = 1800  # GPSLogger 通知定位的最大可接受陈旧度
LOCATION_RETRY_WARN_AT = 10  # 连续定位失败告警阈值
LOCATION_RETRY_BACKOFF_MAX_SEC = 3600
# VPN接口/IP 等字段在日志契约中以这些字符串表示无值
BLANK_VALUES = {"", "n/a", "na", "none", "unknown"}
# WiFi 名「未知」语义集合：None/NaN 判空后，剩余归一为未命名的文本（Unknown_WiFi 已在解析层转 None）
UNKNOWN_WIFI_TEXT = set(BLANK_VALUES) | {"unknown_wifi"}
# 这些字段的「无值」代表探测失败：不计变化、不覆盖已知值；VPN 的 N/A 是未连接状态，必须照常参与变化比较
UNKNOWN_SETS = {
    "public_ip": BLANK_VALUES,
    "wifi_name": UNKNOWN_WIFI_TEXT,
    "public_ip_cn": BLANK_VALUES,
    "public_ip_source": BLANK_VALUES,
}
# 日志行字段 → DataFrame 列；未知键忽略（采集端新增字段不破坏旧报告端）
LOG_FIELD_MAP = {
    "Network": "network",
    "WiFi_Name": "wifi_name",
    "Public_IP": "public_ip",
    "Local_IP": "local_ip",
    "VPN_Interface": "vpn_interface",
    "VPN_IP": "vpn_ip",
    "Public_IP_Source": "public_ip_source",
    "Public_IP_CN": "public_ip_cn",
    "WiFi_BSSID": "wifi_bssid",
}
LOG_TIMESTAMP_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \| ")
# 缺少任一核心字段的畸形行跳过（旧 7 字段行含这两个字段，仍可解析）
CORE_LOG_FIELDS = {"network", "local_ip"}
# 端点来源 token → 报告展示名
SOURCE_LABELS = {
    "ipsb": "ip.sb",
    "icanhazip": "icanhazip",
    "aws-checkip": "AWS checkip",
    "ipinfo": "ipinfo",
    "ifconfig": "ifconfig.me",
    "hcx-echo": "HCX 自建",
}
SOURCE_UNRECORDED = "未记录"
BSSID_RE = re.compile(r"^[0-9a-f]{2}(?::[0-9a-f]{2}){5}$")
# 仅随状态记录/报告展示、不参与变化比较的字段（端点切换、AP 漫游不应触发报告刷写）
STATE_EXTRA_FIELDS = ("public_ip_source", "wifi_bssid")


def _sanitize_field_value(value: Any) -> Optional[str]:
    """字段值转字符串；None/NaN 归一为 None，避免 NaN 进入比较与状态文件."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    return text or None


def _normalize_unknown(value: Any, unknown_texts: set) -> Optional[str]:
    """sanitize 后再把哨兵文本（Unknown/N/A 等）视为无值."""
    text = _sanitize_field_value(value)
    if text is None or text.lower() in unknown_texts:
        return None
    return text


def _valid_bssid(value: Any) -> Optional[str]:
    """返回归一化的小写 BSSID；N/A、Unknown、非 MAC 文本均视为无值."""
    text = _sanitize_field_value(value)
    if text is None:
        return None
    text = text.lower()
    return text if BSSID_RE.match(text) else None


def _source_label(value: Any) -> str:
    """端点来源 token 转展示名；缺失展示为「未记录」."""
    token = _sanitize_field_value(value)
    if token is None:
        return SOURCE_UNRECORDED
    return SOURCE_LABELS.get(token.lower(), token)


RADIO_TYPE_MAP = {
    "nr": "5G NR",
    "lte": "LTE",
    "wcdma": "WCDMA",
    "hspa": "HSPA",
    "gsm": "GSM",
    "cdma": "CDMA",
    "tdscdma": "TD-SCDMA",
}
CARRIER_MAP = {
    "46000": "中国移动",
    "46002": "中国移动",
    "46004": "中国移动",
    "46007": "中国移动",
    "46008": "中国移动",
    "46001": "中国联通",
    "46006": "中国联通",
    "46009": "中国联通",
    "46003": "中国电信",
    "46005": "中国电信",
    "46011": "中国电信",
    "46015": "中国广电",
}
PROVIDER_TEXT_MAP = {
    "network": "网络",
    "gps": "GPS",
    "fused": "融合",
    "passive": "被动",
    "cached": "缓存",
    "gpslogger": "GPSLogger 通知",
}
CJK_FONT_CANDIDATES = (
    "/system/fonts/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
)

# 变化检测字段表：(记录键, 展示名, 比较前转换)
CHANGE_FIELDS: Tuple[Tuple[str, str, Optional[type]], ...] = (
    ("public_ip", "公网IP", None),
    ("public_ip_cn", "国内公网IP", None),
    ("network", "网络类型", None),
    ("wifi_name", "WiFi", str),
    ("local_ip", "本地IP", None),
    ("vpn_interface", "VPN接口", None),
    ("vpn_ip", "VPN IP", None),
)


# %%
@dataclass
class IpConfig:
    """运行期配置（来自云端配置笔记与本地设备信息）."""

    device_id: str
    host_user: str
    log_file: Path
    report_days: int


def load_config() -> IpConfig:
    """装载运行配置；缺失关键配置时抛出 RuntimeError 明确退出."""
    device_id = getdeviceid()
    pathtext = getinivaluefromcloud(CONFIG_NAME, f"{device_id}_ip_log")
    if not pathtext or str(pathtext).strip().lower() == "none":
        raise RuntimeError(f"云端配置 [happyjpip] 缺少 {device_id}_ip_log，无法定位IP日志文件")
    report_days = getinivaluefromcloud(CONFIG_NAME, "REPORT_DAYS") or DEFAULT_REPORT_DAYS
    return IpConfig(
        device_id=device_id,
        host_user=gethostuser(),
        log_file=Path(str(pathtext)).expanduser(),
        report_days=int(report_days),
    )


# %% [markdown]
# ## 核心功能函数

# %% [markdown]
# ### parse_ip_log_file(log_path: Path) -> pd.DataFrame

# %%
def _parse_ip_log_line(line: str) -> Optional[Dict[str, Any]]:
    """解析单行日志为记录字典；非日志行或缺少核心字段返回None.

    宽松键值解析：按 " | " 切段、每段 "键: 值"；未知键忽略，新增字段（Source/CN/BSSID）
    缺失时为 None，因此旧 7 字段行与畸形行可共存。
    """
    match = LOG_TIMESTAMP_RE.match(line)
    if not match:
        return None
    record: Dict[str, Any] = {"timestamp": pd.to_datetime(match.group(1))}
    for segment in line[match.end():].split(" | "):
        key, sep, value = segment.partition(": ")
        if not sep:
            continue
        column = LOG_FIELD_MAP.get(key.strip())
        if column is not None:
            record[column] = value.strip()
    if not CORE_LOG_FIELDS.issubset(record):
        return None
    # 记录日志是否带 Source 字段：源 token（含 unknown）随后会被归一为 None，借此区分旧格式与探测失败
    record["source_recorded"] = "public_ip_source" in record
    # 探测失败哨兵归一为 None，避免进入分析与统计
    record["public_ip"] = _normalize_unknown(record.get("public_ip"), BLANK_VALUES)
    record["wifi_name"] = _normalize_unknown(record.get("wifi_name"), UNKNOWN_WIFI_TEXT)
    record["public_ip_cn"] = _normalize_unknown(record.get("public_ip_cn"), BLANK_VALUES)
    record["public_ip_source"] = _normalize_unknown(record.get("public_ip_source"), BLANK_VALUES)
    record["wifi_bssid"] = _sanitize_field_value(record.get("wifi_bssid"))
    return record


def parse_ip_log_file(log_path: Path) -> pd.DataFrame:
    """解析结构化的IP日志文件，返回DataFrame。

    格式：2026-03-09 00:35:02 | Network: WiFi | WiFi_Name: yj8510 | Public_IP: 219.76.131.102 | ...
    旧 7 字段行与新增 9 字段行（Public_IP_Source / Public_IP_CN / WiFi_BSSID）均可解析。
    """
    data = []
    try:
        with open(log_path, "r", encoding="utf-8") as f:
            for line in f:
                record = _parse_ip_log_line(line.strip())
                if record is not None:
                    data.append(record)
        df = pd.DataFrame(data)
        if not df.empty:
            df = df.sort_values("timestamp").reset_index(drop=True)
        return df
    except FileNotFoundError:
        log.error(f"日志文件未找到: {log_path}")
        return pd.DataFrame()


# %% [markdown]
# ### analyze_ip_data(df: pd.DataFrame, days: int = DEFAULT_REPORT_DAYS) -> Dict

# %%
def analyze_ip_data(df: pd.DataFrame, days: int = DEFAULT_REPORT_DAYS) -> Dict:
    """分析IP数据，生成统计摘要和用于可视化的数据。

    返回一个包含各类分析结果的字典。
    """
    if df.empty:
        return {}

    # 筛选指定时间范围
    cutoff_time = datetime.now() - timedelta(days=days)
    df_recent = df[df["timestamp"] >= cutoff_time].copy()

    analysis = {
        "time_range": (df["timestamp"].min(), df["timestamp"].max()),
        "total_records": len(df),
        "recent_records": len(df_recent),
        "summary": {},
        "detail": {},
        "latest_record": {},
    }

    # 1. 获取最新一行IP数据记录
    latest_row = df.iloc[-1]
    analysis["latest_record"] = {
        "timestamp": latest_row["timestamp"],
        "network": latest_row["network"],
        "wifi_name": None if pd.isna(latest_row["wifi_name"]) else latest_row["wifi_name"],
        "public_ip": None if pd.isna(latest_row["public_ip"]) else latest_row["public_ip"],
        "local_ip": latest_row["local_ip"],
        "vpn_interface": latest_row["vpn_interface"],
        "vpn_ip": latest_row["vpn_ip"],
        "public_ip_source": None if pd.isna(latest_row.get("public_ip_source")) else latest_row.get("public_ip_source"),
        "public_ip_cn": None if pd.isna(latest_row.get("public_ip_cn")) else latest_row.get("public_ip_cn"),
        "wifi_bssid": None if pd.isna(latest_row.get("wifi_bssid")) else latest_row.get("wifi_bssid"),
    }

    # 旧格式日志（升级前）没有新字段列，补齐为空列以便统一统计
    for column in ("public_ip_cn", "public_ip_source", "wifi_bssid", "source_recorded"):
        if column not in df_recent.columns:
            df_recent[column] = False if column == "source_recorded" else None

    # 2. 网络连接类型统计
    analysis["summary"]["network_stats"] = df_recent["network"].value_counts().to_dict()

    # 3. WiFi热点统计：按最近连接时间倒序（渲染端限额），出口取该热点下公网IP众数
    wifi_stats = {}
    for wifi_name in df_recent["wifi_name"].dropna().unique():
        wifi_data = df_recent[df_recent["wifi_name"] == wifi_name]
        exit_ips = wifi_data["public_ip"].dropna()
        # 仅统计合法 MAC（Unknown/N/A 不计为 AP）；行序已按时间升序，最后一个即最近
        bssids = [b for b in (_valid_bssid(v) for v in wifi_data["wifi_bssid"]) if b]
        wifi_stats[wifi_name] = {
            "count": len(wifi_data),
            "latest_time": wifi_data["timestamp"].max(),
            "exit_ip": exit_ips.mode().iloc[0] if not exit_ips.empty else None,
            "bssid_count": len(set(bssids)),
            "latest_bssid": bssids[-1] if bssids else None,
        }
    sorted_wifi = sorted(wifi_stats.items(), key=lambda kv: kv[1]["latest_time"], reverse=True)
    analysis["summary"]["wifi_stats"] = dict(sorted_wifi)

    # 4. 公网IP变化分析（首行无前序记录、或任一端探测失败，均不计入变化）
    def _change_mask(series: pd.Series) -> pd.Series:
        prev = series.shift(1)
        mask = series.notna() & prev.notna() & series.ne(prev)
        if not mask.empty:
            mask.iloc[0] = False
        return mask

    change_mask = _change_mask(df_recent["public_ip"])
    cn_change_mask = _change_mask(df_recent["public_ip_cn"])
    analysis["summary"]["public_ip_changes"] = int(change_mask.sum())
    analysis["summary"]["public_ip_cn_changes"] = int(cn_change_mask.sum())

    # 端点来源分布（探测失败/旧日志归「未记录」）
    analysis["summary"]["source_stats"] = df_recent["public_ip_source"].fillna(SOURCE_UNRECORDED).value_counts().to_dict()

    window_log = df_recent[
        ["timestamp", "public_ip", "public_ip_cn", "public_ip_source", "source_recorded", "network"]
    ].copy()
    window_log["ip_changed"] = change_mask
    window_log["cn_changed"] = cn_change_mask
    analysis["detail"]["recent_log"] = [
        {
            "timestamp": row.timestamp,
            "public_ip": None if pd.isna(row.public_ip) else row.public_ip,
            "public_ip_cn": None if pd.isna(row.public_ip_cn) else row.public_ip_cn,
            "public_ip_source": None if pd.isna(row.public_ip_source) else row.public_ip_source,
            "source_recorded": bool(row.source_recorded) if not pd.isna(row.source_recorded) else False,
            "network": row.network,
            "ip_changed": bool(row.ip_changed),
            "cn_changed": bool(row.cn_changed),
        }
        for row in window_log.itertuples(index=False)
    ]

    # 5. 本地IP段统计
    def extract_ip_segment(ip):
        if ip and "." in ip:
            parts = ip.split(".")
            return f"{parts[0]}.{parts[1]}.{parts[2]}.x"
        return "Unknown"

    df_recent["ip_segment"] = df_recent["local_ip"].apply(extract_ip_segment)
    analysis["summary"]["local_ip_segments"] = df_recent["ip_segment"].value_counts().to_dict()

    # 6. VPN连接稳定性
    analysis["summary"]["vpn_stats"] = df_recent["vpn_interface"].value_counts().to_dict()

    # 为图表准备数据
    analysis["chart_data"] = {
        "timeline": df_recent[["timestamp", "public_ip", "wifi_name"]],
        "network_dist": analysis["summary"]["network_stats"],
    }

    return analysis


# %% [markdown]
# ### render_chart(chart_data: Dict, geo: Optional[Dict] = None) -> Optional[bytes]

# %%
def ensure_cjk_font() -> bool:
    """为matplotlib注册可用中文字体；找不到时静默返回False."""
    from matplotlib import font_manager

    for font_path in CJK_FONT_CANDIDATES:
        if Path(font_path).exists():
            try:
                font_manager.fontManager.addfont(font_path)
                font_name = font_manager.FontProperties(fname=font_path).get_name()
                plt.rcParams["font.sans-serif"] = [font_name]
                plt.rcParams["axes.unicode_minus"] = False
                return True
            except Exception as e:
                log.warning(f"加载中文字体失败（{font_path}）：{e}")
    return False


def render_chart(chart_data: Dict, geo: Optional[Dict[str, Dict[str, str]]] = None) -> Optional[bytes]:
    """生成公网IP出现频率条形图（x轴两行标签：IP+归属地名），返回PNG字节；无有效数据时返回None."""
    try:
        timeline = chart_data.get("timeline")
        if timeline is None or timeline.empty:
            return None
        ip_series = timeline["public_ip"].dropna()
        if ip_series.empty:
            return None

        geo = geo or {}
        ensure_cjk_font()
        fig, ax = plt.subplots(figsize=(10, 6))
        try:
            counts = ip_series.value_counts().head(8)
            counts.plot(kind="bar", color="skyblue", ax=ax)
            ax.set_title("近期公网IP出现频率 (Top 8)")
            ax.set_xlabel("公网IP地址")
            ax.set_ylabel("出现次数")
            ax.yaxis.set_major_locator(MaxNLocator(integer=True))
            labels = []
            for ip in counts.index:
                place = (geo.get(ip) or {}).get("place", "")
                labels.append(f"{ip}\n（{place}）" if place else ip)
            ax.set_xticks(range(len(labels)))
            ax.set_xticklabels(labels)
            ax.tick_params(axis="x", labelrotation=45)
            fig.tight_layout()

            buf = io.BytesIO()
            fig.savefig(buf, format="png", dpi=150)
            return buf.getvalue()
        finally:
            plt.close(fig)
    except Exception as e:
        log.error(f"生成图表时出错: {e}")
        return None


# %% [markdown]
# ### lookup_ip_geo(ip) / resolve_ip_geos(ips)

# %%
def lookup_ip_geo(ip: str) -> Optional[Dict[str, str]]:
    """反查公网IP归属地（中文地名 + 运营商）；失败返回None."""
    try:
        resp = requests.get(
            GEO_API_URL.format(ip=ip),
            params={"fields": GEO_API_FIELDS, "lang": "zh-CN"},
            timeout=8,
        )
        data = resp.json()
    except Exception as e:
        log.warning(f"反查公网IP归属地失败（{ip}）：{e}")
        return None
    if data.get("status") != "success":
        log.warning(f"反查公网IP归属地未命中（{ip}）：{data}")
        return None
    country = str(data.get("country") or "").strip()
    region = str(data.get("regionName") or "").strip()
    city = str(data.get("city") or "").strip()
    isp = str(data.get("isp") or "").strip()
    if country and country != "中国":
        place = " ".join(p for p in (country, city or region) if p)
    else:
        place = " ".join(p for p in (region, city) if p)
    if not place:
        return None
    return {"place": place, "isp": isp}


def resolve_ip_geos(ips) -> Dict[str, Dict[str, str]]:
    """批量反查公网IP归属地（本地缓存，仅新IP发请求），返回 ip → {place, isp}."""
    cache_str = getcfpoptionvalue(CONFIG_NAME, GEO_CACHE_SECTION, GEO_CACHE_KEY)
    cache = json.loads(cache_str) if cache_str else {}
    updated = False
    for ip in dict.fromkeys(str(i) for i in ips if i):
        if ip in cache:
            continue
        geo = lookup_ip_geo(ip)
        if geo:
            cache[ip] = geo
            updated = True
    if updated:
        if len(cache) > GEO_CACHE_LIMIT:
            cache = dict(list(cache.items())[-GEO_CACHE_LIMIT:])
        setcfpoptionvalue(CONFIG_NAME, GEO_CACHE_SECTION, GEO_CACHE_KEY, json.dumps(cache, ensure_ascii=False))
    return cache


# %% [markdown]
# ### 实时网络与定位（termux-api / geotrace 注册地点）

# %%
def _run_termux_command(args: list, timeout: int) -> Optional[str]:
    """运行 termux-api 命令返回 stdout；命令缺失或执行失败返回 None."""
    if shutil.which(args[0]) is None:
        return None
    try:
        proc = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    except (subprocess.SubprocessError, OSError) as e:
        log.warning(f"执行 {' '.join(args)} 失败：{e}")
        return None
    if proc.returncode != 0 or not proc.stdout.strip():
        return None
    return proc.stdout.strip()


def _run_termux_json(args: list, timeout: int) -> Optional[Union[dict, list]]:
    """运行 termux-api 命令并解析其 JSON 输出；失败返回 None."""
    out = _run_termux_command(args, timeout)
    if out is None:
        return None
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        log.warning(f"解析 {' '.join(args)} 输出失败：{out[:120]}")
        return None


def collect_live_network_details() -> Dict[str, str]:
    """实时采集网络链路细节：WiFi 连接返回链路质量，否则返回基站信号；不可用返回 {}."""
    wifi_info = _run_termux_json(["termux-wifi-connectioninfo"], 8)
    if isinstance(wifi_info, dict) and str(wifi_info.get("supplicant_state", "")).upper() in {"COMPLETED", "CONNECTED"}:
        ssid = str(wifi_info.get("ssid") or "").strip()
        if ssid.lower() in {"<unknown ssid>", "null"}:
            ssid = ""
        details = {"ssid": ssid}
        text_parts = []
        rssi = wifi_info.get("rssi")
        if isinstance(rssi, (int, float)) and rssi > -127:
            text_parts.append(f"{rssi} dBm")
        speed = wifi_info.get("link_speed_mbps")
        if isinstance(speed, (int, float)) and speed > 0:
            text_parts.append(f"{speed:g} Mbps")
        freq = wifi_info.get("frequency_mhz")
        if isinstance(freq, (int, float)) and freq > 0:
            band = "2.4 GHz" if freq < 2500 else ("5 GHz" if freq < 5900 else "6 GHz")
            text_parts.append(band)
        if text_parts:
            details["signal_label"] = "WiFi 链路"
            details["signal_text"] = " · ".join(text_parts)
        return details

    cells = _run_termux_json(["termux-telephony-cellinfo"], 8)
    registered = next((c for c in (cells or []) if isinstance(c, dict) and c.get("registered")), None)
    if not registered:
        return {}
    device_info = _run_termux_json(["termux-telephony-deviceinfo"], 8) or {}

    text_parts = []
    radio = RADIO_TYPE_MAP.get(str(registered.get("type") or "").lower())
    if radio:
        text_parts.append(radio)
    dbm = registered.get("dbm")
    if isinstance(dbm, (int, float)) and dbm < 0:
        text_parts.append(f"{dbm} dBm")
    try:
        mcc_mnc = f"{int(registered.get('mcc'))}{int(registered.get('mnc')):02d}"
    except (TypeError, ValueError):
        mcc_mnc = ""
    carrier = CARRIER_MAP.get(mcc_mnc) or str(device_info.get("sim_operator_name") or "").strip()
    if carrier:
        text_parts.append(carrier)
    if not text_parts:
        return {}
    return {"signal_label": "基站信号", "signal_text": " · ".join(text_parts)}


_GPSLOGGER_DMS_RE = re.compile(
    r"(\d{1,3})\s*°\s*(\d{1,2}(?:\.\d+)?)\s*['′]\s*(?:(\d{1,2}(?:\.\d+)?)\s*[\"″]\s*)?([NSEW])",
    re.IGNORECASE,
)
_GPSLOGGER_DECIMAL_HEMI_RE = re.compile(
    r"(-?\d{1,3}(?:\.\d+)?)\s*°?\s*([NS])\s*[,，]\s*(-?\d{1,3}(?:\.\d+)?)\s*°?\s*([EW])",
    re.IGNORECASE,
)
_GPSLOGGER_DECIMAL_PAIR_RE = re.compile(r"(-?\d{1,3}(?:\.\d+)?)\s*[,，]\s*(-?\d{1,3}(?:\.\d+)?)")


def _validated_latlng(lat: float, lng: float) -> Optional[Tuple[float, float]]:
    """校验经纬度范围；拒绝 NaN/Inf 与 (0,0)."""
    if not (math.isfinite(lat) and math.isfinite(lng)):
        return None
    if not (-90 <= lat <= 90 and -180 <= lng <= 180):
        return None
    if abs(lat) < 1e-9 and abs(lng) < 1e-9:
        return None
    return float(lat), float(lng)


def _dms_to_decimal(degrees: float, minutes: float, seconds: float, hemi: str) -> float:
    """度分秒转十进制度；南纬/西经取负."""
    value = float(degrees) + float(minutes) / 60.0 + float(seconds or 0) / 3600.0
    return -value if hemi.upper() in ("S", "W") else value


def _parse_gpslogger_coords(title: str) -> Optional[Tuple[float, float]]:
    """从 GPSLogger 通知标题解析坐标（DMS 优先，兼容十进制）."""
    text = str(title or "")
    matches = _GPSLOGGER_DMS_RE.findall(text)
    if len(matches) >= 2:
        first, second = matches[0], matches[1]
        try:
            if (
                float(first[1]) >= 60
                or float(second[1]) >= 60
                or float(first[2] or 0) >= 60
                or float(second[2] or 0) >= 60
            ):
                raise ValueError
            lat = _dms_to_decimal(float(first[0]), float(first[1]), float(first[2] or 0), first[3])
            lng = _dms_to_decimal(float(second[0]), float(second[1]), float(second[2] or 0), second[3])
        except ValueError:
            return None  # 畸形的 DMS 文本不退回十进制，避免跨字段误拆
        if first[3].upper() in ("N", "S") and second[3].upper() in ("E", "W"):
            return _validated_latlng(lat, lng)
        return None
    match = _GPSLOGGER_DECIMAL_HEMI_RE.search(text)
    if match:
        lat, lng = float(match.group(1)), float(match.group(3))
        if match.group(2).upper() == "S":
            lat = -abs(lat)
        if match.group(4).upper() == "W":
            lng = -abs(lng)
        return _validated_latlng(lat, lng)
    match = _GPSLOGGER_DECIMAL_PAIR_RE.search(text)
    if match:
        return _validated_latlng(float(match.group(1)), float(match.group(2)))
    return None


def _parse_gpslogger_time(text: object) -> Optional[datetime]:
    """解析 GPSLogger 通知的 when 字段（本地时间）."""
    try:
        return datetime.strptime(str(text or "").strip(), "%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return None


def collect_gpslogger_location(now: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
    """读取 GPSLogger 常驻通知中的定位（termux-location 失败时的第一兜底）."""
    items = _run_termux_json(["termux-notification-list"], 20)
    if not isinstance(items, list):
        return None
    current = now or datetime.now()
    best: Optional[Tuple[datetime, Tuple[float, float], Dict[str, Any]]] = None
    for item in items:
        if not isinstance(item, dict) or item.get("packageName") != GPSLOGGER_PACKAGE:
            continue
        coords = _parse_gpslogger_coords(str(item.get("title") or ""))
        fix_time = _parse_gpslogger_time(item.get("when"))
        if coords is None or fix_time is None:
            continue
        age = (current - fix_time).total_seconds()
        if age < -300 or age > GPSLOGGER_MAX_AGE_SEC:
            continue
        if best is None or fix_time > best[0]:
            best = (fix_time, coords, item)
    if best is None:
        return None
    fix_time, (lat, lng), item = best
    location: Dict[str, Any] = {
        "latitude": lat,
        "longitude": lng,
        "provider": "gpslogger",
        "fix_time": fix_time,
    }
    match = re.search(r"Accuracy:\s*(\d+(?:\.\d+)?)\s*meters", str(item.get("content") or ""), re.IGNORECASE)
    if match:
        accuracy = float(match.group(1))
        if accuracy > 0:
            location["accuracy"] = accuracy
    log.info(f"采用 GPSLogger 通知定位：{lat:.5f}, {lng:.5f}（{_relative_time(fix_time)}）")
    return location


def _location_from_fix(fix: object, provider: str) -> Optional[Dict[str, Any]]:
    """termux-location JSON 转定位字典；坐标缺失返回 None."""
    if not isinstance(fix, dict):
        return None
    lat, lng = fix.get("latitude"), fix.get("longitude")
    if not isinstance(lat, (int, float)) or not isinstance(lng, (int, float)):
        log.warning(f"定位输出缺少经纬度：{str(fix)[:120]}")
        return None
    location = {"latitude": float(lat), "longitude": float(lng), "provider": provider}
    accuracy = fix.get("accuracy")
    if isinstance(accuracy, (int, float)) and accuracy > 0:
        location["accuracy"] = float(accuracy)
    fix_time = fix.get("time")
    if isinstance(fix_time, (int, float)) and fix_time > 0:
        stamp = fix_time / 1000.0 if fix_time > 1e11 else float(fix_time)
        try:
            fix_dt = datetime.fromtimestamp(stamp)
        except (OverflowError, OSError, ValueError):
            fix_dt = None
        if fix_dt is not None and datetime(2020, 1, 1) <= fix_dt <= datetime.now() + timedelta(days=1):
            location["fix_time"] = fix_dt
    return location


def collect_live_location(retry: bool = False, now: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
    """获取即时定位：网络定位优先，GPSLogger 通知兜底，last 缓存收尾；全失败返回 None.

    retry=True 用于「上次报告缺定位」的下一轮重试：跳过 60s 的 once 尝试，控制每 tick 成本。
    """
    if not retry:
        fix = _run_termux_json(["termux-location", "-p", "network", "-r", "once"], 60)
        if isinstance(fix, dict):
            provider = str(fix.get("provider") or "network")
            location = _location_from_fix(fix, provider)
            if location is not None:
                return location
    fallback = collect_gpslogger_location(now=now)
    if fallback is not None:
        return fallback
    fix = _run_termux_json(["termux-location", "-p", "network", "-r", "last"], 15)
    return _location_from_fix(fix, "cached")


def _haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """两坐标间大圆距离（米）."""
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lng2 - lng1)
    a = math.sin(d_phi / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(d_lambda / 2) ** 2
    return 2 * 6371000.0 * math.asin(math.sqrt(a))


def fetch_registered_places() -> list:
    """拉取 geotrace 已注册地点（name/lat/lng/radius）；失败返回 []（定位退化为仅坐标）."""
    token = getinivaluefromcloud("sms_collector", "api_key")
    if not token or str(token).strip().lower() == "none":
        log.warning("云端配置缺少 [sms_collector] api_key，跳过注册地点匹配。")
        return []
    for attempt in range(3):
        try:
            resp = requests.get(f"{PLACES_URL}?token={token}", timeout=15)
            data = resp.json()
        except Exception as e:
            log.warning(f"拉取注册地点失败（第 {attempt + 1}/3 次）：{e}")
        else:
            if isinstance(data, list):
                return data
            log.warning(f"注册地点接口返回异常：{str(data)[:120]}")
        if attempt < 2:
            time.sleep(2 * (attempt + 1))
    return []


def match_registered_place(lat: float, lng: float, places: list) -> Optional[str]:
    """在已注册地点中取最近命中（bbox 预筛 + haversine ≤ radius）；未命中返回 None."""
    best_name, best_dist = None, float("inf")
    for place in places:
        if not isinstance(place, dict):
            continue
        try:
            p_lat = float(place.get("lat", 0))
            p_lng = float(place.get("lng", 0))
            radius_m = max(float(place.get("radius", 50) or 50), 50.0)
        except (TypeError, ValueError):
            continue
        if (p_lat == 0 and p_lng == 0) or abs(lat - p_lat) > radius_m / 111000.0 + 1e-6:
            continue
        if abs(lng - p_lng) > radius_m / (111000.0 * max(math.cos(math.radians(p_lat)), 0.01)) + 1e-6:
            continue
        dist = _haversine_m(lat, lng, p_lat, p_lng)
        if dist <= radius_m and dist < best_dist:
            name = str(place.get("name") or "").strip()
            if name:
                best_name, best_dist = name, dist
    return best_name


# %% [markdown]
# ### _relative_time(ts: datetime) -> str

# %%
def _relative_time(ts: datetime) -> str:
    """把时间戳转换为相对当下的粗略描述（用于仪表盘状态行）."""
    if not isinstance(ts, datetime):
        return ""
    secs = int((datetime.now() - ts).total_seconds())
    if secs < 60:
        return "刚刚"
    if secs < 3600:
        return f"约 {secs // 60} 分钟前"
    if secs < 86400:
        return f"约 {secs // 3600} 小时前"
    return f"约 {secs // 86400} 天前"


# %% [markdown]
# ### _render_location_line(location: Dict) -> str

# %%
def _render_location_line(location: Dict[str, Any]) -> str:
    """渲染「📍 当前定位」状态行；无有效坐标返回空串."""
    lat, lng = location.get("latitude"), location.get("longitude")
    if lat is None or lng is None:
        return ""
    detail_parts = []
    provider = str(location.get("provider") or "").strip()
    if provider:
        detail_parts.append(PROVIDER_TEXT_MAP.get(provider.lower(), provider))
    accuracy = location.get("accuracy")
    if isinstance(accuracy, (int, float)) and accuracy > 0:
        detail_parts.append(f"±{accuracy:.0f}m")
    fix_time = location.get("fix_time")
    if isinstance(fix_time, datetime):
        age = _relative_time(fix_time)
        if age and age != "刚刚":
            detail_parts.append(age)
    place = str(location.get("place") or "").strip()
    place_text = f"{place} · " if place else ""
    detail_text = f"（{' · '.join(detail_parts)}）" if detail_parts else ""
    map_link = f"[地图]({OSM_LINK_TEMPLATE.format(lat=lat, lng=lng)})"
    return f"📍 **当前定位** ｜ {place_text}{lat:.3f}, {lng:.3f}{detail_text} · {map_link}"


# %% [markdown]
# ### _format_geo(geo_info: Optional[Dict]) -> str

# %%
def _format_geo(geo_info: Optional[Dict[str, str]]) -> str:
    """归属地渲染为（地名 · 运营商）；无地名返回空串."""
    if not geo_info or not geo_info.get("place"):
        return ""
    text = geo_info["place"]
    if geo_info.get("isp"):
        text += f" · {geo_info['isp']}"
    return f"（{text}）"


# %% [markdown]
# ### render_report(analysis: Dict, cfg: IpConfig, chart_image: Optional[bytes], geo/live/location) -> str

# %%
def render_report(
    analysis: Dict,
    cfg: IpConfig,
    chart_image: Optional[bytes],
    geo: Optional[Dict[str, Dict[str, str]]] = None,
    live: Optional[Dict[str, str]] = None,
    location: Optional[Dict[str, Any]] = None,
) -> str:
    """生成状态仪表盘式Markdown报告（图表以占位符替代，发布时替换为资源引用）."""
    if not analysis:
        return "# 🌐 IP 分析报告\n\n暂无有效数据。\n"

    geo = geo or {}
    live = live or {}
    location = location or {}
    summary = analysis["summary"]
    latest = analysis.get("latest_record", {})

    md_lines = [f"# 🌐 IP 分析报告 · {cfg.host_user}", ""]

    # 状态仪表盘：当前网络 / 公网出口 / 最近上报
    latest_time = latest.get("timestamp")
    if latest_time is not None:
        time_text = latest_time.strftime("%m-%d %H:%M") if hasattr(latest_time, "strftime") else str(latest_time)
        rel = _relative_time(latest_time)
        rel_text = f"（{rel}）" if rel else ""
        public_ip = latest.get("public_ip")
        geo_text = _format_geo(geo.get(public_ip))
        network = str(latest.get("network", "未知"))
        ssid = str(live.get("ssid") or "").strip() or str(latest.get("wifi_name") or "").strip()
        ssid_text = f" · {ssid}" if network == "WiFi" and ssid else ""
        bssid = _valid_bssid(latest.get("wifi_bssid")) if network == "WiFi" else None
        bssid_text = f" · AP {bssid}" if bssid else ""
        md_lines.append(f"📡 **当前网络** ｜ {network}{ssid_text}{bssid_text} · 本地 `{latest.get('local_ip', '未知')}`")
        if live.get("signal_label") and live.get("signal_text"):
            md_lines.append(f"📶 **{live['signal_label']}** ｜ {live['signal_text']}")
        location_line = _render_location_line(location)
        if location_line:
            md_lines.append(location_line)
        vpn_intf = str(latest.get("vpn_interface") or "").strip()
        vpn_ip = str(latest.get("vpn_ip") or "").strip()
        if vpn_intf.lower() not in BLANK_VALUES and vpn_ip.lower() not in BLANK_VALUES:
            md_lines.append(f"🔒 **VPN** ｜ {vpn_intf} · `{vpn_ip}`")
        source_text = f" · 来源 {_source_label(latest.get('public_ip_source'))}"
        md_lines.append(f"🌍 **公网出口** ｜ `{public_ip or '未知'}`{geo_text}{source_text}")
        public_cn = latest.get("public_ip_cn")
        if public_cn:
            if public_ip and public_cn == public_ip:
                md_lines[-1] += " · 国内同出口"
            else:
                md_lines.append(f"🇨🇳 **国内出口** ｜ `{public_cn}`{_format_geo(geo.get(public_cn))}")
        md_lines.append(f"🕐 **最近上报** ｜ {time_text}{rel_text}")
        md_lines.append("")

    # 近 N 天摘要：记录数 / 国际与国内出口切换 / 端点来源 / WiFi 次数
    wifi_count = summary.get("network_stats", {}).get("WiFi", 0)
    md_lines.append(f"## 🔄 近 {cfg.report_days} 天")
    md_lines.append(
        f"{analysis['recent_records']} 条记录 ｜ **{summary.get('public_ip_changes', 0)} 次** 国际IP切换 ｜ "
        f"**{summary.get('public_ip_cn_changes', 0)} 次** 国内出口切换 ｜ WiFi **{wifi_count}** 次"
    )
    source_stats = summary.get("source_stats", {})
    if source_stats:
        top_sources = sorted(source_stats.items(), key=lambda kv: kv[1], reverse=True)[:4]
        source_parts = " · ".join(f"{_source_label(token)} {count}" for token, count in top_sources)
        md_lines.append(f"🔌 **端点来源** ｜ {source_parts}")
    md_lines.append("")

    # 记录表：列出统计窗口内全部记录（新→旧），🔄加粗行为对应出口切换点；
    # 国内出口与公网出口不同时动态增加「国内IP」列，否则保持三列
    recent_log = analysis.get("detail", {}).get("recent_log", [])
    if recent_log:
        show_cn_col = any(
            entry.get("public_ip_cn") and entry.get("public_ip_cn") != entry.get("public_ip")
            for entry in recent_log
        )
        if show_cn_col:
            md_lines.append("| 时间 | 公网IP | 国内IP | 网络 |")
            md_lines.append("|:---|:---|:---|:---|")
        else:
            md_lines.append("| 时间 | 公网IP | 网络 |")
            md_lines.append("|:---|:---|:---|")
        shown_log = recent_log[-RECENT_LOG_LIMIT:]
        for entry in reversed(shown_log):
            timestamp = entry.get("timestamp")
            if isinstance(timestamp, pd.Timestamp):
                timestamp = timestamp.strftime("%m-%d %H:%M")
            entry_ip = entry.get("public_ip")
            entry_place = (geo.get(entry_ip) or {}).get("place", "")
            if entry_ip:
                ip_cell = f"`{entry_ip}`" + (f"（{entry_place}）" if entry_place else "")
            elif entry.get("source_recorded"):
                # 新格式行有来源字段却无 IP = 本轮探测全部失败；旧格式行则是当时未记录
                ip_cell = "⚠️ 探测失败"
            else:
                ip_cell = "未记录"
            if entry.get("ip_changed"):
                ip_cell = f"🔄 **{ip_cell}**"
            network_text = entry.get("network") or "未知"
            if show_cn_col:
                entry_cn = entry.get("public_ip_cn")
                cn_place = (geo.get(entry_cn) or {}).get("place", "") if entry_cn else ""
                if entry_cn:
                    cn_cell = f"`{entry_cn}`" + (f"（{cn_place}）" if cn_place else "")
                elif entry.get("source_recorded"):
                    cn_cell = "探测失败"
                else:
                    cn_cell = "未记录"
                if entry_cn and entry.get("cn_changed"):
                    cn_cell = f"🔄 **{cn_cell}**"
                md_lines.append(f"| {timestamp} | {ip_cell} | {cn_cell} | {network_text} |")
            else:
                md_lines.append(f"| {timestamp} | {ip_cell} | {network_text} |")
        if len(recent_log) > len(shown_log):
            # 空行分隔，否则脚注会被 Markdown 归并为表格行（首格有字、其余空）
            md_lines.append("")
            md_lines.append(f"*（共 {len(recent_log)} 条，仅列最近 {len(shown_log)} 条）*")
    else:
        md_lines.append("近期无记录。")
    md_lines.append("")

    # 近期热点（按最近连接倒序，最多 WIFI_HOTSPOT_LIMIT 个）
    wifi_stats = summary.get("wifi_stats", {})
    if wifi_stats:
        md_lines.append("🏨 **近期热点**（按最近连接倒序）")
        for wifi_name, wifi_data in list(wifi_stats.items())[:WIFI_HOTSPOT_LIMIT]:
            latest_time = wifi_data.get("latest_time")
            if isinstance(latest_time, pd.Timestamp):
                latest_time = latest_time.strftime("%m-%d %H:%M")
            exit_ip = wifi_data.get("exit_ip")
            exit_place = (geo.get(exit_ip) or {}).get("place", "") if exit_ip else ""
            exit_text = ""
            if exit_ip:
                exit_text = f" ｜ 出口 `{exit_ip}`" + (f"（{exit_place}）" if exit_place else "")
            bssid_text = ""
            if wifi_data.get("bssid_count"):
                bssid_text = f" ｜ {wifi_data['bssid_count']} 个 AP"
                if wifi_data.get("latest_bssid"):
                    bssid_text += f"（最近 {wifi_data['latest_bssid']}）"
            md_lines.append(f"- **{wifi_name}** · {latest_time} · {wifi_data.get('count', 0)} 次{exit_text}{bssid_text}")
        if len(wifi_stats) > WIFI_HOTSPOT_LIMIT:
            md_lines.append("")
            md_lines.append(f"*（共 {len(wifi_stats)} 个热点，仅列最近 {WIFI_HOTSPOT_LIMIT} 个）*")
        md_lines.append("")

    # 技术信息+图表折叠区（图表默认收起，发布时替换为资源引用）
    md_lines.append("<details>")
    md_lines.append("<summary>📁 技术信息与图表</summary>")
    md_lines.append("")
    md_lines.append(f"设备 `{cfg.device_id}` ｜ 统计窗口 近 {cfg.report_days} 天")
    md_lines.append(f"日志 `{cfg.log_file}` ｜ 总记录 {analysis['total_records']:,} 条")
    md_lines.append(f"生成 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    md_lines.append("")
    md_lines.append(CHART_PLACEHOLDER if chart_image else "*(图表生成跳过)*")
    md_lines.append("")
    md_lines.append("</details>")

    return "\n".join(md_lines) + "\n"


# %% [markdown]
# ### detect_ip_changes(current_record: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]

# %%
def detect_ip_changes(current_record: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """检测IP记录是否有变化，返回是否有变化和变化详情（字段表驱动）."""
    last_record_str = getcfpoptionvalue(CONFIG_NAME, IP_UPDATE_CONFIG_SECTION, "last_record")
    last_record = json.loads(last_record_str) if last_record_str else {}

    # 统一清洗：NaN/None（pandas 会把探测失败变成 NaN）不进入比较与状态文件；N/A 等真实哨兵值按字段语义处理
    current_record = dict(current_record)
    for key, _display, _conv in CHANGE_FIELDS:
        current_record[key] = _sanitize_field_value(current_record.get(key))
    # 探测失败类字段先归一无值，合并阶段据此保留最近已知值（Source 随公网IP粘滞）
    for key, unknown_set in UNKNOWN_SETS.items():
        current_record[key] = _normalize_unknown(current_record.get(key), unknown_set)
    # 展示用字段只净化、不参与变化比较
    for key in STATE_EXTRA_FIELDS:
        current_record[key] = _sanitize_field_value(current_record.get(key))

    if not last_record:
        # 首次运行，记录当前状态
        setcfpoptionvalue(
            CONFIG_NAME, IP_UPDATE_CONFIG_SECTION, "last_record", json.dumps(current_record, ensure_ascii=False)
        )
        setcfpoptionvalue(
            CONFIG_NAME, IP_UPDATE_CONFIG_SECTION, "last_update_time", datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        return True, {"initial": "首次记录IP状态"}

    changes = {}
    for key, _display, conv in CHANGE_FIELDS:
        old_val = last_record.get(key, "")
        new_val = current_record.get(key, "")
        unknown_set = UNKNOWN_SETS.get(key)
        if unknown_set is not None:
            # 「未知不计变化」：探测失败不视为变化、不覆盖已知值；未知→已知则记变化（首次学习）
            old_val = _normalize_unknown(old_val, unknown_set)
            new_val = _normalize_unknown(new_val, unknown_set)
            if new_val is None:
                continue
        else:
            old_val = _sanitize_field_value(old_val)
            new_val = _sanitize_field_value(new_val)
        if conv is not None:
            old_val = conv(old_val) if old_val is not None else None
            new_val = conv(new_val) if new_val is not None else None
        if old_val != new_val:
            changes[key] = {"old": old_val, "new": new_val}

    if changes:
        # 「未知不覆盖已知」：公网IP/WiFi 探测失败时保留最近已知值；VPN 的 N/A 是真实状态，照常写入
        merged = dict(current_record)
        for key, unknown_set in UNKNOWN_SETS.items():
            if merged.get(key) is None:
                merged[key] = _normalize_unknown(last_record.get(key), unknown_set)
        setcfpoptionvalue(
            CONFIG_NAME, IP_UPDATE_CONFIG_SECTION, "last_record", json.dumps(merged, ensure_ascii=False)
        )
        setcfpoptionvalue(
            CONFIG_NAME, IP_UPDATE_CONFIG_SECTION, "last_update_time", datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        return True, changes
    return False, {}


# %% [markdown]
# ### generate_change_summary(changes: Dict[str, Any]) -> str

# %%
def generate_change_summary(changes: Dict[str, Any]) -> str:
    """安全生成变化摘要，处理各种数据类型."""
    if not changes:
        return "无变化"

    field_display_names = {key: display for key, display, _conv in CHANGE_FIELDS}
    field_display_names["initial"] = "状态"

    summary_parts = []
    for key, value in changes.items():
        display_name = field_display_names.get(key, key)
        if isinstance(value, dict):
            if "old" in value and "new" in value:
                old_val = value["old"] if value["old"] is not None else "空"
                new_val = value["new"] if value["new"] is not None else "空"
                summary_parts.append(f"{display_name}: {old_val} → {new_val}")
            else:
                summary_parts.append(f"{display_name}: {str(value)}")
        elif key == "initial":
            summary_parts.append(f"首次记录: {value}")
        else:
            summary_parts.append(f"{display_name}: {str(value)}")

    return "; ".join(summary_parts)


# %% [markdown]
# ### resolve_note(cfg: IpConfig) -> str

# %%
def _matches_report_title(title: str, base: str) -> bool:
    """严格判定标题属本设备报告：等于基础标题，或仅追加「（更新于…）」后缀；防搜索子串误命中他笔记."""
    if title == base:
        return True
    if not title.startswith(base):
        return False
    return bool(TITLE_UPDATE_SUFFIX_RE.match(title[len(base):]))


# %%
def resolve_note(cfg: IpConfig) -> str:
    """定位本设备的报告笔记：ini登记的note_id为准，仅登记缺失/失效时按标题严格匹配找回，兜底新建."""
    notebook_id = searchnotebook(DEFAULT_NOTEBOOK)
    note_id = getcfpoptionvalue(CONFIG_NAME, IP_UPDATE_CONFIG_SECTION, "note_id")

    if note_id:
        try:
            note = getnote(note_id)
        except Exception as e:
            log.warning(f"ini登记的笔记（{note_id}）读取失败（{e}），改为标题严格匹配找回。")
            note_id = None
        else:
            if notebook_id and note.parent_id != notebook_id:
                log.warning(f"ini登记的笔记（{note_id}）不在《{DEFAULT_NOTEBOOK}》，改为标题严格匹配找回。")
                note_id = None

    if not note_id:
        note_title = f"IP分析报告_{cfg.host_user}"
        candidates = searchnotes(note_title, parent_id=notebook_id)
        matched = [n for n in candidates if _matches_report_title(n.title, note_title)]
        if matched:
            if len(matched) > 1:
                log.warning(f"标题严格匹配到多条笔记：{[(n.id, n.title) for n in matched]}，取最近更新者。")
            note_id = max(matched, key=lambda n: n.updated_time).id
            log.info(f"标题严格匹配找回笔记并重新登记：{note_id}")
        else:
            note_id = createnote(title=note_title, parent_id=notebook_id)
            log.info(f"创建新笔记《{note_title}》（{note_id}）")
        setcfpoptionvalue(CONFIG_NAME, IP_UPDATE_CONFIG_SECTION, "note_id", str(note_id))

    return note_id


# %% [markdown]
# ### sync_note_resources(note_id: str, note_title: str, report_content: str, chart_image: Optional[bytes]) -> None

# %%
def sync_note_resources(note_id: str, note_title: str, report_content: str, chart_image: Optional[bytes]) -> None:
    """安全替换笔记图表资源：先加新图-嵌引用-更新正文-再删旧资源（含上轮遗留）."""
    old_resource_ids = set(extract_resource_ids_from_note(note_id))
    last_resource_id = getcfpoptionvalue(CONFIG_NAME, IP_UPDATE_CONFIG_SECTION, "last_resource_id")
    if last_resource_id:
        old_resource_ids.add(str(last_resource_id))

    new_resource_id = None
    if chart_image:
        new_resource_id = add_resource_from_bytes(chart_image, f"ip_chart_{datetime.now().strftime('%Y%m%d_%H%M')}")
        report_content = report_content.replace(CHART_PLACEHOLDER, f"![IP连接分析图表](:/{new_resource_id})")

    updatenote_title(note_id, note_title)
    updatenote_body(note_id, report_content)

    for resource_id in old_resource_ids:
        if resource_id == new_resource_id:
            continue
        try:
            jpapi.delete_resource(resource_id)
            log.info(f"旧图表资源（{resource_id}）已删除。")
        except Exception as e:
            log.warning(f"删除旧资源（{resource_id}）失败，留待下轮清理：{e}")

    setcfpoptionvalue(CONFIG_NAME, IP_UPDATE_CONFIG_SECTION, "last_resource_id", str(new_resource_id or ""))


# %% [markdown]
# ### 定位兜底状态（缺定位重试与退避）

# %%
def _location_capable() -> bool:
    """本机是否具备定位采集能力；Linux 主机不做缺定位重试."""
    return shutil.which("termux-location") is not None or shutil.which("termux-notification-list") is not None


def _ini_truthy(value: object) -> bool:
    """兼容 getcfpoptionvalue 的 int/str 返回值."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _get_int_option(name: str, default: int = 0) -> int:
    value = getcfpoptionvalue(CONFIG_NAME, IP_UPDATE_CONFIG_SECTION, name)
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _get_time_option(name: str) -> Optional[datetime]:
    value = getcfpoptionvalue(CONFIG_NAME, IP_UPDATE_CONFIG_SECTION, name)
    try:
        return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return None


def _location_retry_delay(failures: int) -> int:
    """连续失败达阈值后指数退避，避免定位长期不可用时每 tick 空跑."""
    if failures < LOCATION_RETRY_WARN_AT:
        return 0
    return min(LOCATION_RETRY_BACKOFF_MAX_SEC, 360 * 2 ** min(failures - LOCATION_RETRY_WARN_AT, 4))


def _warn_location_failures(failures: int) -> None:
    if failures == LOCATION_RETRY_WARN_AT:
        log.warning(
            f"连续 {failures} 次未取到定位；请检查省电模式、Termux/Termux:API 后台活动与位置权限（P50Pro 同类问题）。"
        )


def _write_location_state(missing: bool, failures: int) -> None:
    setcfpoptionvalue(CONFIG_NAME, IP_UPDATE_CONFIG_SECTION, "location_missing", "1" if missing else "0")
    setcfpoptionvalue(CONFIG_NAME, IP_UPDATE_CONFIG_SECTION, "location_retry_failures", str(failures))


# %% [markdown]
# ### update_ip_report_note(cfg: Optional[IpConfig] = None) -> Tuple[bool, str]

# %%
@timethis
def update_ip_report_note(cfg: Optional[IpConfig] = None) -> Tuple[bool, str]:
    """主函数：读取日志、分析数据、生成报告并更新Joplin笔记."""
    try:
        cfg = cfg or load_config()

        host_user = cfg.host_user

        # 1. 读取数据
        df = parse_ip_log_file(cfg.log_file)
        if df.empty:
            log.warning("IP日志文件为空或解析失败，跳过报告更新。")
            return False, "无数据"

        # 2. 获取最新记录用于变化检测
        latest_row = df.iloc[-1]
        current_record = {
            "timestamp": latest_row["timestamp"].isoformat()
            if hasattr(latest_row["timestamp"], "isoformat")
            else str(latest_row["timestamp"]),
            "network": _sanitize_field_value(latest_row["network"]),
            "wifi_name": _sanitize_field_value(latest_row["wifi_name"]),
            "public_ip": _sanitize_field_value(latest_row["public_ip"]),
            "local_ip": _sanitize_field_value(latest_row["local_ip"]),
            "vpn_interface": _sanitize_field_value(latest_row["vpn_interface"]),
            "vpn_ip": _sanitize_field_value(latest_row["vpn_ip"]),
            "public_ip_source": _sanitize_field_value(latest_row.get("public_ip_source")),
            "public_ip_cn": _sanitize_field_value(latest_row.get("public_ip_cn")),
            "wifi_bssid": _sanitize_field_value(latest_row.get("wifi_bssid")),
        }

        # 3. 检测IP变化
        has_changes, changes = detect_ip_changes(current_record)

        # 4. 无变化时：24h 内通常跳过；上次发布缺定位则在后续 tick 重试补定位
        location = None
        location_retry_used = False
        if not has_changes:
            last_update_time_str = getcfpoptionvalue(CONFIG_NAME, IP_UPDATE_CONFIG_SECTION, "last_full_update")
            within_24h = False
            if last_update_time_str:
                last_update_time = datetime.strptime(last_update_time_str, "%Y-%m-%d %H:%M:%S")
                within_24h = datetime.now() - last_update_time < timedelta(hours=24)
            if within_24h:
                missing_raw = getcfpoptionvalue(CONFIG_NAME, IP_UPDATE_CONFIG_SECTION, "location_missing")
                retry_due = _location_capable() and (missing_raw is None or _ini_truthy(missing_raw))
                if not retry_due:
                    log.info("IP记录无变化，且上次完整更新在24小时内，跳过报告更新。")
                    return True, "IP无变化，跳过更新"
                failures = _get_int_option("location_retry_failures")
                last_retry = _get_time_option("location_retry_last")
                delay = _location_retry_delay(failures)
                if last_retry is not None and delay > 0 and (datetime.now() - last_retry).total_seconds() < delay:
                    log.info(f"缺定位重试处于退避期（连续失败 {failures} 次），本轮跳过。")
                    return True, "定位重试退避中"
                location = collect_live_location(retry=True)
                setcfpoptionvalue(
                    CONFIG_NAME,
                    IP_UPDATE_CONFIG_SECTION,
                    "location_retry_last",
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                )
                if location is None:
                    failures += 1
                    _write_location_state(True, failures)
                    _warn_location_failures(failures)
                    log.info("上次报告缺少定位，本轮重试仍未取到，下轮继续。")
                    return True, "报告缺定位，等待下轮重试"
                location_retry_used = True
                log.info("上次报告缺少定位，本轮已取到，执行补充更新。")
            else:
                log.info("IP记录无变化，但超过24小时未更新报告，执行强制更新。")

        # 5. 分析数据
        analysis = analyze_ip_data(df, days=cfg.report_days)

        # 5.5 反查公网IP归属地（本地缓存，仅新IP发请求；国际与国内出口都查）
        geo_ips = [
            analysis["latest_record"].get("public_ip"),
            analysis["latest_record"].get("public_ip_cn"),
        ]
        for entry in analysis.get("detail", {}).get("recent_log", []):
            geo_ips.append(entry.get("public_ip"))
            geo_ips.append(entry.get("public_ip_cn"))
        geo_map = resolve_ip_geos(geo_ips)

        # 5.6 实时网络与定位（termux-api；失败优雅省略）
        live = collect_live_network_details()
        if location is None:
            location = collect_live_location()
        if location:
            places = fetch_registered_places()
            place = match_registered_place(location["latitude"], location["longitude"], places)
            if place:
                location["place"] = place

        # 6. 生成图表与报告文本
        chart_image = render_chart(analysis.get("chart_data", {}), geo_map)
        report_content = render_report(analysis, cfg, chart_image, geo_map, live, location)

        # 7. 定位笔记并发布（含图表资源的安全替换；标题带快照更新时间，列表即可见新旧）
        note_id = resolve_note(cfg)
        note_title = f"IP分析报告_{host_user}（更新于 {datetime.now().strftime('%m-%d %H:%M')}）"
        sync_note_resources(note_id, note_title, report_content, chart_image)

        # 8. 记录完整更新与定位兜底状态（发布成功后才落盘，避免发布失败误清重试标记）
        setcfpoptionvalue(
            CONFIG_NAME, IP_UPDATE_CONFIG_SECTION, "last_full_update", datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        if location is not None:
            _write_location_state(False, 0)
        elif _location_capable():
            failures = _get_int_option("location_retry_failures") + 1
            _write_location_state(True, failures)
            _warn_location_failures(failures)
        else:
            _write_location_state(False, 0)

        # 9. 记录更新摘要
        if has_changes:
            change_summary = generate_change_summary(changes)
            log.info(f"IP分析报告已更新至笔记: {note_title}, 变化: {change_summary}")
        elif location_retry_used:
            log.info(f"IP分析报告已更新至笔记: {note_title} (补充定位，无IP变化)")
        else:
            log.info(f"IP分析报告已更新至笔记: {note_title} (强制更新，无IP变化)")

        return True, "报告更新成功"

    except Exception as e:
        import traceback

        error_detail = traceback.format_exc()
        log.error(f"更新IP报告笔记失败: {e}\n详细错误信息:\n{error_detail}")
        return False, f"更新失败: {str(e)} - 详细错误请查看日志"


# %% [markdown]
# ## 主函数main()

# %%
if __name__ == "__main__":
    if not_IPython():
        log.info(f"开始运行文件\t{__file__}")
    success, message = update_ip_report_note()
    if not_IPython():
        status = "成功" if success else "失败"
        log.info(f"文件执行{status}{message}\t{__file__}")
