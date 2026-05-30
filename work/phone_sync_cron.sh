#!/data/data/com.termux/files/usr/bin/bash
# 手机端定时同步聊天记录到 hcx（Pixel 6 Pro / P50 Pro 通用）
# crontab 示例: */30 * * * * $HOME/codebase/happyjoplin/work/phone_sync_cron.sh >> $HOME/tmp/phone_sync.log 2>&1

HAPPYJOPLIN_DIR="$HOME/codebase/happyjoplin"
[ -d "$HAPPYJOPLIN_DIR" ] || HAPPYJOPLIN_DIR="$HOME/storage/shared/codebase/happyjoplin"
[ -d "$HAPPYJOPLIN_DIR" ] || { echo "找不到 happyjoplin 目录"; exit 1; }

cd "$HAPPYJOPLIN_DIR" || exit 1

python work/phone_sync.py --limit 2000
