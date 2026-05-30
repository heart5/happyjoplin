#!/data/data/com.termux/files/usr/bin/bash
# 手机端定时上传 mp3 至 hcx 语音转文字
# crontab: 57 */2 * * * $HOME/codebase/happyjoplin/work/wc_transcribe_cron.sh

HAPPYJOPLIN_DIR="$HOME/codebase/happyjoplin"
[ -d "$HAPPYJOPLIN_DIR" ] || HAPPYJOPLIN_DIR="$HOME/storage/shared/codebase/happyjoplin"
[ -d "$HAPPYJOPLIN_DIR" ] || { echo "找不到 happyjoplin 目录"; exit 1; }

LOGDIR="${TMPDIR:-$HOME/tmp}"
mkdir -p "$LOGDIR"
exec >> "$LOGDIR/wc_transcribe.log" 2>&1

cd "$HAPPYJOPLIN_DIR" || exit 1

NOW=$(date "+%Y-%m-%d %H:%M:%S")
echo "========================================"
echo "phone_transcribe @ $NOW  host=$(hostname)"

START=$(date +%s)
python work/wc_sync.py --transcribe --limit 100
RET=$?
ELAPSED=$(($(date +%s) - START))

echo "结束: exit=$RET 耗时=${ELAPSED}s"
echo "========================================"
echo ""
