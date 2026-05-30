#!/bin/bash
# tc 定时上传 mp3 至 hcx 语音转文字
# crontab 示例: 7 */2 * * * $HOME/codebase/happyjoplin/work/tc_transcribe_cron.sh >> $HOME/tmp/tc_transcribe.log 2>&1

export https_proxy=http://127.0.0.1:7890
export http_proxy=http://127.0.0.1:7890

cd "$HOME/codebase/happyjoplin" || exit 1

NOW=$(date "+%Y-%m-%d %H:%M:%S")
echo "========================================"
echo "tc_transcribe @ $NOW  host=$(hostname)"

START=$(date +%s)
/usr/miniconda3/envs/newlsp/bin/python3 work/phone_sync.py --transcribe --limit 50
RET=$?
ELAPSED=$(($(date +%s) - START))

echo "结束: exit=$RET 耗时=${ELAPSED}s"
echo "========================================"
echo ""
