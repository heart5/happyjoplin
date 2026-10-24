#!/bin/bash
# 健康周报（上一完整 ISO 周）+ 状态刷新
set -uo pipefail
cd /data/codebase/happyjoplin || exit 1
PY=/etc/miniconda3/envs/lsp/bin/python
LOG=$HOME/tmp/health_weekly.log
LOCK=$HOME/tmp/health_report.lock
mkdir -p "$HOME/tmp"
exec 9>"$LOCK"
flock -n 9 || { echo "$(date "+%F %T") skip: report busy" >>"$LOG"; exit 0; }
exec >>"$LOG" 2>&1
sync_tc() { timeout 60 ssh -o BatchMode=yes -o ConnectTimeout=10 tc "joplin sync" || echo "tc sync rc=$?"; }
echo "=== $(date "+%F %T") health_weekly ==="
sync_tc
"$PY" -m life.health.cli memos --refresh || echo "memos rc=$?"
"$PY" -m life.health.cli weekly || echo "weekly rc=$?"
"$PY" -m life.health.cli status --alert || echo "status rc=$?"
sync_tc
echo "=== done ==="
