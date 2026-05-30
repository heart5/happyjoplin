#!/bin/bash
# tc 定时推送聊天记录到 hcx
# crontab 示例: */30 * * * * $HOME/codebase/happyjoplin/work/tc_sync_cron.sh >> $HOME/tmp/tc_sync.log 2>&1

export https_proxy=http://127.0.0.1:7890
export http_proxy=http://127.0.0.1:7890

cd "$HOME/codebase/happyjoplin" || exit 1

NOW=$(date "+%Y-%m-%d %H:%M:%S")
echo "========================================"
echo "tc_sync @ $NOW  host=$(hostname)"

START=$(date +%s)
/usr/miniconda3/envs/newlsp/bin/python3 work/wc_sync.py --account 白晔峰 --limit 5000
RET=$?
ELAPSED=$(($(date +%s) - START))

echo "结束: exit=$RET 耗时=${ELAPSED}s"
echo "========================================"
echo ""
