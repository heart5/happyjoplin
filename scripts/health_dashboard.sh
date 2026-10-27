#!/bin/bash
# 健康动态日日升（HCX 健康库）每日 07:40 / 21:40 更新
set -uo pipefail
HAPPY=/data/codebase/happyjoplin
PY=/etc/miniconda3/envs/lsp/bin/python
LOG=$HOME/tmp/health_dashboard.log
mkdir -p "$HOME/tmp"
cd "$HAPPY" || exit 1
exec 9>"$HOME/tmp/health_collect.lock"
flock -w 900 9 || { echo "$(date "+%F %T") skip: collector busy 15min" >>"$LOG"; exit 0; }
exec >>"$LOG" 2>&1
sync_tc() { timeout 60 ssh -o BatchMode=yes -o ConnectTimeout=10 tc "joplin sync" || echo "tc sync rc=$?"; }
echo "=== $(date "+%F %T") health_dashboard ==="
sync_tc
"$PY" life/health.py --source db
rc=$?
sync_tc
echo "=== done rc=$rc ==="
exit $rc
