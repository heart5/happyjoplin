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
# → 图表（render_chart）→ 报告（render_report）→ 变化检测（detect_ip_changes）
# → 笔记定位（resolve_note）→ 资源替换与发布（sync_note_resources）。

# %% [markdown]
# ## 导入依赖库

# %%
import io
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import matplotlib.pyplot as plt
import pandas as pd
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
CHART_PLACEHOLDER = "*(图表已更新至笔记附件)*"
CJK_FONT_CANDIDATES = (
    "/system/fonts/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
)

# 变化检测字段表：(记录键, 展示名, 比较前转换)
CHANGE_FIELDS: Tuple[Tuple[str, str, Optional[type]], ...] = (
    ("public_ip", "公网IP", None),
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
def parse_ip_log_file(log_path: Path) -> pd.DataFrame:
    """解析结构化的IP日志文件，返回DataFrame。

    格式：2026-03-09 00:35:02 | Network: WiFi | WiFi_Name: yj8510 | Public_IP: 219.76.131.102 | ...
    """
    data = []
    pattern = re.compile(
        r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \| "
        r"Network: (\w+) \| "
        r"WiFi_Name: ([^|]+) \| "
        r"Public_IP: ([^|]+) \| "
        r"Local_IP: ([^|]+) \| "
        r"VPN_Interface: ([^|]+) \| "
        r"VPN_IP: ([^|\n]+)"
    )
    try:
        with open(log_path, "r", encoding="utf-8") as f:
            for line in f:
                match = pattern.match(line.strip())
                if match:
                    (
                        timestamp,
                        network,
                        wifi_name,
                        public_ip,
                        local_ip,
                        vpn_intf,
                        vpn_ip,
                    ) = match.groups()
                    # 清洗数据，将"Unknown"替换为None或空字符串以便分析
                    public_ip = None if public_ip.strip() == "Unknown" else public_ip.strip()
                    wifi_name = None if wifi_name.strip() == "Unknown_WiFi" else wifi_name.strip()
                    data.append(
                        {
                            "timestamp": pd.to_datetime(timestamp),
                            "network": network.strip(),
                            "wifi_name": wifi_name,
                            "public_ip": public_ip,
                            "local_ip": local_ip.strip(),
                            "vpn_interface": vpn_intf.strip(),
                            "vpn_ip": vpn_ip.strip(),
                        }
                    )
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
        "wifi_name": latest_row["wifi_name"],
        "public_ip": latest_row["public_ip"],
        "local_ip": latest_row["local_ip"],
        "vpn_interface": latest_row["vpn_interface"],
        "vpn_ip": latest_row["vpn_ip"],
    }

    # 2. 网络连接类型统计
    analysis["summary"]["network_stats"] = df_recent["network"].value_counts().to_dict()

    # 3. WiFi热点统计 (Top 5) 并添加最近连接时间
    wifi_stats = {}
    for wifi_name in df_recent["wifi_name"].dropna().unique():
        wifi_data = df_recent[df_recent["wifi_name"] == wifi_name]
        wifi_stats[wifi_name] = {"count": len(wifi_data), "latest_time": wifi_data["timestamp"].max()}
    sorted_wifi = sorted(wifi_stats.items(), key=lambda x: x[1]["count"], reverse=True)[:5]
    analysis["summary"]["wifi_stats"] = dict(sorted_wifi)

    # 4. 公网IP变化分析（首行无前序记录，不计入变化）
    change_mask = df_recent["public_ip"].ne(df_recent["public_ip"].shift(1))
    if not change_mask.empty:
        change_mask.iloc[0] = False
    ip_change_points = df_recent[change_mask]
    analysis["summary"]["public_ip_changes"] = len(ip_change_points)
    analysis["detail"]["ip_change_log"] = ip_change_points[["timestamp", "public_ip", "network"]].to_dict("records")

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
# ### render_chart(chart_data: Dict) -> Optional[bytes]

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


def render_chart(chart_data: Dict) -> Optional[bytes]:
    """生成公网IP出现频率条形图，返回PNG字节；无有效数据时返回None."""
    try:
        timeline = chart_data.get("timeline")
        if timeline is None or timeline.empty:
            return None
        ip_series = timeline["public_ip"].dropna()
        if ip_series.empty:
            return None

        ensure_cjk_font()
        fig, ax = plt.subplots(figsize=(10, 6))
        try:
            ip_series.value_counts().head(8).plot(kind="bar", color="skyblue", ax=ax)
            ax.set_title("近期公网IP出现频率 (Top 8)")
            ax.set_xlabel("公网IP地址")
            ax.set_ylabel("出现次数")
            ax.yaxis.set_major_locator(MaxNLocator(integer=True))
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
# ### render_report(analysis: Dict, cfg: IpConfig, chart_image: Optional[bytes]) -> str

# %%
def render_report(analysis: Dict, cfg: IpConfig, chart_image: Optional[bytes]) -> str:
    """生成状态仪表盘式Markdown报告（图表以占位符替代，发布时替换为资源引用）."""
    if not analysis:
        return "# 🌐 IP 分析报告\n\n暂无有效数据。\n"

    summary = analysis["summary"]
    latest = analysis.get("latest_record", {})

    md_lines = [f"# 🌐 IP 分析报告 · {cfg.host_user}", ""]

    # 状态仪表盘：当前网络 / 公网出口 / 最近上报
    latest_time = latest.get("timestamp")
    if latest_time is not None:
        time_text = latest_time.strftime("%m-%d %H:%M") if hasattr(latest_time, "strftime") else str(latest_time)
        rel = _relative_time(latest_time)
        rel_text = f"（{rel}）" if rel else ""
        md_lines.append(f"📡 **当前网络** ｜ {latest.get('network', '未知')} · 本地 `{latest.get('local_ip', '未知')}`")
        md_lines.append(f"🌍 **公网出口** ｜ `{latest.get('public_ip') or '未知'}`")
        md_lines.append(f"🕐 **最近上报** ｜ {time_text}{rel_text}")
        md_lines.append("")

    # 图表前置（发布时替换为资源引用）
    if chart_image:
        md_lines.append(f"## 📈 图表\n\n{CHART_PLACEHOLDER}\n")
    else:
        md_lines.append("## 📈 图表\n\n*(图表生成跳过)*\n")

    # 近 N 天摘要：记录数 / 公网IP切换 / WiFi 次数
    wifi_count = summary.get("network_stats", {}).get("WiFi", 0)
    md_lines.append(f"## 🔄 近 {cfg.report_days} 天")
    md_lines.append(
        f"{analysis['recent_records']} 条记录 ｜ **{summary.get('public_ip_changes', 0)} 次** 公网IP切换 ｜ "
        f"WiFi **{wifi_count}** 次"
    )
    md_lines.append("")

    change_log = analysis.get("detail", {}).get("ip_change_log", [])
    if change_log:
        md_lines.append("| 时间 | 公网IP | 网络 |")
        md_lines.append("|:---|:---|:---|")
        for entry in reversed(change_log[-10:]):  # 最近10次变化，倒序（新→旧）
            timestamp = entry.get("timestamp")
            if isinstance(timestamp, pd.Timestamp):
                timestamp = timestamp.strftime("%m-%d %H:%M")
            md_lines.append(f"| {timestamp} | `{entry.get('public_ip') or '未知'}` | {entry.get('network', '')} |")
    else:
        md_lines.append("近期无公网IP变化。")
    md_lines.append("")

    # 常连热点（Top 1）
    wifi_stats = summary.get("wifi_stats", {})
    if wifi_stats:
        wifi_name, wifi_data = next(iter(wifi_stats.items()))
        latest_time = wifi_data.get("latest_time")
        if isinstance(latest_time, pd.Timestamp):
            latest_time = latest_time.strftime("%m-%d %H:%M")
        md_lines.append(f"🏨 常连热点：{wifi_name}（{wifi_data.get('count', 0)} 次 · 最近 {latest_time}）\n")

    # 技术信息折叠区
    md_lines.append("<details>")
    md_lines.append("<summary>📁 技术信息</summary>")
    md_lines.append("")
    md_lines.append(f"设备 `{cfg.device_id}` ｜ 统计窗口 近 {cfg.report_days} 天")
    md_lines.append(f"日志 `{cfg.log_file}` ｜ 总记录 {analysis['total_records']:,} 条")
    md_lines.append(f"生成 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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
        if conv is not None:
            old_val, new_val = conv(old_val), conv(new_val)
        if old_val != new_val:
            changes[key] = {"old": old_val, "new": new_val}

    if changes:
        setcfpoptionvalue(
            CONFIG_NAME, IP_UPDATE_CONFIG_SECTION, "last_record", json.dumps(current_record, ensure_ascii=False)
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
def resolve_note(cfg: IpConfig) -> str:
    """定位本设备的报告笔记：ini记忆的note_id优先，失效则按标题搜索取最新，兜底新建."""
    notebook_id = searchnotebook(DEFAULT_NOTEBOOK)
    note_id = getcfpoptionvalue(CONFIG_NAME, IP_UPDATE_CONFIG_SECTION, "note_id")

    if note_id:
        try:
            note = getnote(note_id)
        except Exception as e:
            log.warning(f"ini记忆的笔记（{note_id}）读取失败（{e}），回退标题搜索。")
            note_id = None
        else:
            if notebook_id and note.parent_id != notebook_id:
                log.warning(f"ini记忆的笔记（{note_id}）不在《{DEFAULT_NOTEBOOK}》，回退标题搜索。")
                note_id = None

    if not note_id:
        note_title = f"IP分析报告_{cfg.host_user}"
        existing_notes = searchnotes(note_title, parent_id=notebook_id)
        if existing_notes:
            note_id = max(existing_notes, key=lambda n: n.updated_time).id
            log.info(f"按标题《{note_title}》找到现有笔记：{note_id}")
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
            "network": latest_row["network"],
            "wifi_name": latest_row["wifi_name"],
            "public_ip": latest_row["public_ip"],
            "local_ip": latest_row["local_ip"],
            "vpn_interface": latest_row["vpn_interface"],
            "vpn_ip": latest_row["vpn_ip"],
        }

        # 3. 检测IP变化
        has_changes, changes = detect_ip_changes(current_record)

        # 4. 如果没有变化，检查是否需要强制更新（基于时间）
        if not has_changes:
            last_update_time_str = getcfpoptionvalue(CONFIG_NAME, IP_UPDATE_CONFIG_SECTION, "last_full_update")
            if last_update_time_str:
                last_update_time = datetime.strptime(last_update_time_str, "%Y-%m-%d %H:%M:%S")
                if datetime.now() - last_update_time < timedelta(hours=24):
                    log.info("IP记录无变化，且上次完整更新在24小时内，跳过报告更新。")
                    return True, "IP无变化，跳过更新"
            log.info("IP记录无变化，但超过24小时未更新报告，执行强制更新。")

        # 5. 分析数据
        analysis = analyze_ip_data(df, days=cfg.report_days)

        # 6. 生成图表与报告文本
        chart_image = render_chart(analysis.get("chart_data", {}))
        report_content = render_report(analysis, cfg, chart_image)

        # 7. 定位笔记并发布（含图表资源的安全替换）
        note_id = resolve_note(cfg)
        note_title = f"IP分析报告_{host_user}"
        sync_note_resources(note_id, note_title, report_content, chart_image)

        # 8. 记录完整更新时间
        setcfpoptionvalue(
            CONFIG_NAME, IP_UPDATE_CONFIG_SECTION, "last_full_update", datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )

        # 9. 记录更新摘要
        if has_changes:
            change_summary = generate_change_summary(changes)
            log.info(f"IP分析报告已更新至笔记: {note_title}, 变化: {change_summary}")
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
