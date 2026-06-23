#!/bin/bash
# post-commit hook: .md 文件变更 → md2note 同步到 Joplin → TC joplin sync
#
# 依赖：
#   - func 子模块（func/tools/md2note.py）
#   - SSH 免密登录 tc（~/.ssh/config 配置主机别名为 tc）

PROJ_ROOT="/data/codebase/happyjoplin"
LOGGER_TAG="sync-docs-joplin"

echo "[$LOGGER_TAG] 同步 .md 文件到 Joplin..."
cd "$PROJ_ROOT" || exit 1
python -m func.tools.md2note --find-files --notebook happyjoplin --quiet
echo "[$LOGGER_TAG] 同步完成，通知 TC 触发 joplin sync..."
ssh tc "/usr/miniconda3/bin/conda run -n newlsp joplin sync" 2>/dev/null && \
  echo "[$LOGGER_TAG] TC joplin sync 完成" || \
  echo "[$LOGGER_TAG] TC joplin sync 跳过（非阻塞）"
echo "[$LOGGER_TAG] 完成"
