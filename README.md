---
jupyter:
  jupytext:
    cell_metadata_filter: -all
    formats: ipynb,md
    main_language: bash
    notebook_metadata_filter: jupytext,-kernelspec,-jupytext.text_representation.jupytext_version
    split_at_heading: true
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
---

# happyjoplin

美好的Joplin — 连接时代，简化生活

## 项目简介

HappyJoplin 是一个以 Joplin 笔记应用为中心枢纽的个人自动化系统。它将 Joplin 既用作数据库，也用作事件源——脚本从 Joplin 笔记读取数据，进行处理，然后将结果写回 Joplin。

## 主要功能

| 子系统 | 位置 | 说明 |
|--------|------|------|
| Joplin API 层 | `func/jpfuncs.py` | 笔记 CRUD、资源管理、标签操作、云配置同步 |
| 微信桥接 | `life/webchat.py` | 微信消息归档、撤回检测、AI 问答、自定义命令 |
| 工作监控 | `work/monitor4.py` | 四件套笔记每日更新追踪、字数统计 |
| 健康仪表盘 | `life/health.py` | 步数/睡眠/啤酒追踪、matplotlib 图表、分析报告 |
| 位置追踪 | `life/footstrack.py` | GPS 采集、轨迹图、停留点分析 |
| 主机配置 | `etc/hostconfig.py` | 多设备配置收集、对比、Joplin 同步 |
| IP 监控 | `etc/ipupdate.py` | 设备 IP/WiFi 变化检测与记录 |
| 空闲内存 | `etc/freemem.py` | 内存占用趋势监测 |
| 语音转文字 | `etc/voice2txt.py` | 微信语音消息转文本 |
| 日志上云 | `etc/log2note.py` | 本地日志推送到 Joplin 笔记 |

## 快速开始

```bash
# 克隆（含子模块）
git clone --recurse-submodules <repo-url>

# 更新子模块
git submodule update --remote func

# 代码检查
pip install ruff
ruff check .
ruff format .

# 测试入口
python fortest.py
```

## 项目结构

```
happyjoplin/
├── func/          ← git 子模块，核心工具库（Joplin API、配置、日志等）
├── etc/           ← 系统级运维脚本
├── work/          ← 工作相关
├── life/          ← 生活相关
├── data/          ← 运行时数据（配置文件、数据库、状态文件）
├── log/           ← 日志
├── img/           ← 生成的图片
├── docs/          ← 文档（变更记录、技术手册）
├── rootfile       ← 项目根目录定位哨兵文件
└── pyproject.toml ← Ruff + Jupytext 配置
```

## 运行环境

- Python 3.10+
- 主要运行平台：Termux (Android)
- 核心依赖：joppy, pandas, matplotlib, itchat, arrow
- 开发环境：Jupyter Lab + Jupytext

## 文档

- [变更记录](docs/CHANGELOG.md)
- [技术手册](docs/TECHNICAL_MANUAL.md)

## 许可证

MIT
