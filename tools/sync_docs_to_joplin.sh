#!/bin/bash
# post-commit hook: 提交后触发 TC joplin sync
#
# docs/*.md 与根目录 *.md 的 Joplin 同步归 ops 的 sync_docs_to_joplin.py，
# 不再由本脚本用 md2note 重复写一遍——md2note --find-files 只扫 docs/ 与根目录，
# 而 docs/ 被 gitignore，靠 commit 触发结构上就追不上，且它会另建一份标题带
# 路径后缀的笔记，与 ops 的副本重复。log/、data/ 生成物由
# scripts/run_wechat_reports.sh 用 md2note 显式文件路径同步，也不经此处。
#
# 依赖：SSH 免密登录 tc（~/.ssh/config 配置主机别名为 tc）

PROJ_ROOT="/data/codebase/happyjoplin"
LOGGER_TAG="sync-docs-joplin"

echo "[$LOGGER_TAG] 通知 TC 触发 joplin sync..."
cd "$PROJ_ROOT" || exit 1
# 勿用 conda run/activate newlsp 包裹：该环境 node v18 跑 /usr/bin/joplin 会 ERR_REQUIRE_ESM
ssh tc "joplin sync" 2>/dev/null && \
  echo "[$LOGGER_TAG] TC joplin sync 完成" || \
  echo "[$LOGGER_TAG] TC joplin sync 跳过（非阻塞）"
echo "[$LOGGER_TAG] 完成"
