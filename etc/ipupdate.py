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
# # IP信息更新工具 (增强版)

# %% [markdown]
# 功能：获取设备IP和WiFi信息，记录变化并更新至Jupyter笔记
# 优化点：修复数字类型处理、增强错误处理、添加Markdown函数声明.

# %% [markdown]
# ## 导入依赖库

# %%
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import pandas as pd

# %%
try:
    import pathmagic

    with pathmagic.context():
        from etc.getid import getdeviceid, gethostuser
        from func.configpr import getcfpoptionvalue, setcfpoptionvalue
        from func.datatools import readfromtxt, write2txt
        from func.first import dirmainpath
        from func.jpfuncs import (
            createnote,
            getinivaluefromcloud,
            jpapi,
            searchnotebook,
            searchnotes,
            updatenote_body,
            # updatenote_imgdata,
            updatenote_title,
        )
        from func.logme import log

        # from func.nettools import get_ip4alleth
        from func.sysfunc import execcmd, is_tool_valid, not_IPython
        from func.termuxtools import termux_wifi_connectioninfo
        from func.wrapfuncs import timethis
except ImportError as e:
    log.error(f"导入模块失败: {e}")
    # 尝试添加路径（适用于JupyterLab环境）
    sys.path.append(os.path.expanduser("~/codebase/happyjoplin"))  # 请修改为你的实际项目路径
    log.info("已尝试添加路径到sys.path")

# %% [markdown]
# ## 配置常量
# %%
CONFIG_NAME = "happyjpip"
pathtext = getinivaluefromcloud(CONFIG_NAME, f"{getdeviceid()}_ip_log")
LOG_FILE_PATH = Path(f"{pathtext}").expanduser()  # 示例路径，需根据实际调整
REPORT_DAYS = getinivaluefromcloud(CONFIG_NAME, "REPORT_DAYS")


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
# ### analyze_ip_data(df: pd.DataFrame, days: int = REPORT_DAYS) -> Dict

# %%
def analyze_ip_data(df: pd.DataFrame, days: int = REPORT_DAYS) -> Dict:
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
        "latest_record": {},  # 新增：最新记录
    }

    # 1. 获取最新一行IP数据记录
    if not df.empty:
        latest_row = df.iloc[-1]  # 获取最新一行（已按时间排序）
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
        count = len(wifi_data)
        latest_time = wifi_data["timestamp"].max() if not wifi_data.empty else None
        wifi_stats[wifi_name] = {"count": count, "latest_time": latest_time}

    # 按连接次数排序，取Top 5
    sorted_wifi = sorted(wifi_stats.items(), key=lambda x: x["count"], reverse=True)[:5]
    analysis["summary"]["wifi_stats"] = dict(sorted_wifi)

    # 4. 公网IP变化分析
    df_recent["public_ip_change"] = df_recent["public_ip"] != df_recent["public_ip"].shift(1)
    ip_change_points = df_recent[df_recent["public_ip_change"]]
    analysis["summary"]["public_ip_changes"] = len(ip_change_points)
    analysis["detail"]["ip_change_log"] = ip_change_points[["timestamp", "public_ip"]].to_dict("records")

    # 5. 本地IP段统计
    def extract_ip_segment(ip):
        if ip and "." in ip:
            parts = ip.split(".")
            return f"{parts[0]}.{parts[1]}.{parts[2]}.x"
        return "Unknown"

    df_recent["ip_segment"] = df_recent["local_ip"].apply(extract_ip_segment)
    analysis["summary"]["local_ip_segments"] = df_recent["ip_segment"].value_counts().to_dict()

    # 6. VPN连接稳定性
    vpn_stats = df_recent["vpn_interface"].value_counts()
    analysis["summary"]["vpn_stats"] = vpn_stats.to_dict()

    # 为图表准备数据
    analysis["chart_data"] = {
        "timeline": df_recent[["timestamp", "public_ip", "wifi_name"]],
        "network_dist": analysis["summary"]["network_stats"],
        "wifi_dist": {wifi: data["count"] for wifi, data in analysis["summary"]["wifi_stats"].items()},
    }

    return analysis


# %% [markdown]
# ### generate_ip_report(analysis: Dict, device_id: str, host_user: str) -> Tuple[str, Optional[bytes]]

# %%
def generate_ip_report(analysis: Dict, device_id: str, host_user: str) -> Tuple[str, Optional[bytes]]:
    """生成图文并茂的Markdown报告内容，并返回报告文本和图表图片的二进制数据（可选）。"""
    if not analysis:
        return "# IP网络分析报告\n\n暂无有效数据。\n", None

    md_lines = []

    # 标题与概览
    md_lines.append(f"# 🌐 IP网络连接分析报告 ({host_user})")
    md_lines.append(f"**设备ID**: `{device_id}`  |  **生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    md_lines.append(f"**分析时间范围**: {analysis['time_range'][0]} 至 {analysis['time_range'][1]}")
    md_lines.append(
        f"**总记录数**: {analysis['total_records']} | **近期记录数(最近{REPORT_DAYS}天)**: {analysis['recent_records']}\n"
    )

    # 1. 核心摘要 (用表格展示)
    md_lines.append("## 📊 核心摘要")
    summary_table = []
    summary = analysis["summary"]

    # 新增：显示最新IP记录
    latest_record = analysis.get("latest_record", {})
    if latest_record:
        latest_time = latest_record.get("timestamp", "")
        if isinstance(latest_time, pd.Timestamp):
            latest_time = latest_time.strftime("%Y-%m-%d %H:%M:%S")
        summary_table.append(["**最新记录时间**", f"{latest_time}"])
        summary_table.append(["**最新网络类型**", f"{latest_record.get('network', 'N/A')}"])
        summary_table.append(["**最新公网IP**", f"`{latest_record.get('public_ip', 'N/A')}`"])
        summary_table.append(["**最新本地IP**", f"`{latest_record.get('local_ip', 'N/A')}`"])

    summary_table.append(["**公网IP变化次数**", f"{summary.get('public_ip_changes', 0)} 次"])

    # 主要网络类型
    main_network = max(
        summary.get("network_stats", {}),
        key=summary.get("network_stats", {}).get,
        default="N/A",
    )
    summary_table.append(["**主要连接方式**", main_network])

    # 主要本地IP段
    main_segment = max(
        summary.get("local_ip_segments", {}),
        key=summary.get("local_ip_segments", {}).get,
        default="N/A",
    )
    summary_table.append(["**主要本地IP段**", main_segment])

    # 使用Markdown表格语法
    md_lines.append("| 指标 | 值 |")
    md_lines.append("|:---|:---|")
    for row in summary_table:
        md_lines.append(f"| {row[0]} | {row[1]} |")
    md_lines.append("")

    # 2. 详细统计表格
    md_lines.append("## 📈 详细统计")

    # 网络类型分布
    md_lines.append("### 网络连接类型分布")
    for net_type, count in summary.get("network_stats", {}).items():
        md_lines.append(f"- **{net_type}**: {count} 次")
    md_lines.append("")

    # WiFi热点分布 (新增最近连接时间)
    md_lines.append("### 常连WiFi热点 (Top 5)")
    wifi_stats = summary.get("wifi_stats", {})
    for wifi, data in wifi_stats.items():
        count = data.get("count", 0)
        latest_time = data.get("latest_time")

        # 格式化时间
        if latest_time:
            if isinstance(latest_time, pd.Timestamp):
                time_str = latest_time.strftime("%Y-%m-%d %H:%M")
            else:
                time_str = str(latest_time)
            time_display = f" (最近连接: {time_str})"
        else:
            time_display = ""

        display_name = wifi if wifi else "<未知>"
        md_lines.append(f"- **{display_name}**: {count} 次{time_display}")
    md_lines.append("")

    # 3. 公网IP变化历史
    md_lines.append("## 🔄 公网IP变化历史")
    change_log = analysis.get("detail", {}).get("ip_change_log", [])
    if change_log:
        md_lines.append("| 时间 | 公网IP |")
        md_lines.append("|:---|:---|")
        for entry in change_log[-10:]:  # 显示最近10次变化
            timestamp = entry.get("timestamp", "")
            if isinstance(timestamp, pd.Timestamp):
                timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")
            md_lines.append(f"| {timestamp} | `{entry['public_ip']}` |")
    else:
        md_lines.append("近期无公网IP变化。")
    md_lines.append("")

    # 4. 生成图表
    chart_image_data = generate_charts(analysis["chart_data"])

    # 在报告中插入图表引用
    if chart_image_data:
        md_lines.append("## 📸 可视化图表\n\n*(图表已更新至笔记附件)*")
    else:
        md_lines.append("## 📸 可视化图表\n\n*(图表生成跳过)*")

    # 5. 原始数据摘要
    md_lines.append("## 📁 数据来源")
    md_lines.append(f"- 日志文件: `{LOG_FILE_PATH}`")
    md_lines.append(f"- 最后更新: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    md_lines.append("- 注：本报告基于自动采集的日志生成。\n")

    report_content = "\n".join(md_lines)
    return report_content, chart_image_data


# %% [markdown]
# ### generate_charts(chart_data: Dict) -> Optional[bytes]

# %%
def generate_charts(chart_data: Dict) -> Optional[bytes]:
    """生成可视化图表，返回图片二进制数据（如PNG格式）。
    可以生成：
        1. 公网IP随时间的变化序列（折线图，不同IP用不同颜色）。
        2. 网络类型与WiFi热点分布（条形图或饼图）。
    """
    try:
        # 示例：生成一个简单的公网IP出现次数的条形图
        plt.figure(figsize=(10, 6))
        ip_series = chart_data["timeline"]["public_ip"].dropna()
        if not ip_series.empty:
            ip_counts = ip_series.value_counts().head(8)  # Top 8个IP
            ip_counts.plot(kind="bar", color="skyblue")
            plt.title("近期公网IP出现频率 (Top 8)")
            plt.xlabel("公网IP地址")
            plt.ylabel("出现次数")
            plt.xticks(rotation=45, ha="right")
            plt.tight_layout()

            # 将图表保存到字节流
            import io

            buf = io.BytesIO()
            plt.savefig(buf, format="png", dpi=150)
            plt.close()
            buf.seek(0)
            return buf.getvalue()
    except Exception as e:
        log.error(f"生成图表时出错: {e}")
    return None


# %% [markdown]
# ### update_ip_report_note()

# %%
@timethis
def update_ip_report_note():
    """主函数：读取日志、分析数据、生成报告并更新Joplin笔记。"""
    try:
        device_id = getdeviceid()
        host_user = gethostuser()

        # 1. 读取数据
        df = parse_ip_log_file(LOG_FILE_PATH)
        if df.empty:
            log.warning("IP日志文件为空或解析失败，跳过报告更新。")
            return False, "无数据"

        # 2. 分析数据
        analysis = analyze_ip_data(df, days=REPORT_DAYS)

        # 3. 生成报告文本和图表
        report_content, chart_image = generate_ip_report(analysis, device_id, host_user)

        # 4. 查找或创建笔记
        notebook_id = searchnotebook("ewmobile")  # 假设仍在ewmobile笔记本
        note_title = f"IP分析报告_{host_user}"
        existing_notes = searchnotes(note_title, parent_id=notebook_id)

        if existing_notes:
            note = existing_notes[0]
            note_id = note.id
            log.info(f"找到现有笔记: {note_title}")
        else:
            note_id = createnote(title=note_title, parent_id=notebook_id)
            log.info(f"创建新笔记: {note_title}")

        # 5. 如果有图表，先上传为资源文件并获取链接
        if chart_image:
            # 此处需要调用Joplin API上传资源，并获取资源ID
            # resource_id = jpapi.add_resource_from_bytes(chart_image, title="ip_chart.png", mime="image/png")
            # 在报告内容中插入图片链接
            # report_content += f"\n\n![IP分析图表](:/{resource_id})"
            # 简化处理：先保存图表到临时文件，然后上传
            temp_chart_path = Path(dirmainpath) / "img" / "ip_chart.png"
            with open(temp_chart_path, "wb") as f:
                f.write(chart_image)
            resource_id = jpapi.add_resource(str(temp_chart_path))
            # 在报告开头或合适位置插入图片引用
            report_content = report_content.replace("*(图表已更新至笔记附件)*", f"![IP连接分析图表](:/{resource_id})")

        # 6. 更新笔记内容和标题（附带更新时间）
        new_title = f"{note_title} (更新于{datetime.now().strftime('%Y-%m-%d %H:%M')})"
        updatenote_title(note_id, new_title)
        updatenote_body(note_id, report_content)

        log.info(f"IP分析报告已更新至笔记: {new_title}")
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
