#!/bin/bash
# 财务账本每日备份（HCX）— 2026-11-12 新增
# 本地 30 份 + 每周日异机 TC 一份（保留 14 份）；每次 integrity_check。
set -uo pipefail
PY=/etc/miniconda3/envs/lsp/bin/python
SRC=/data/codebase/happyjoplin/data/finance.db
DIR=/data/backups/finance
KEEP_LOCAL=30
KEEP_REMOTE=14
OFFSITE="tc:backups/finance/"
STAMP=$(date +%Y%m%d)
DST="$DIR/finance-$STAMP.db"
LOG=/home/baiyefeng/tmp/finance_backup.log
mkdir -p "$DIR" /home/baiyefeng/tmp

log() { printf '%s %s\n' "$(date '+%F %T')" "$*" >>"$LOG"; }

"$PY" - "$SRC" "$DST" <<'PY'
import sqlite3, sys
src, dst = sys.argv[1], sys.argv[2]
s = sqlite3.connect("file:" + src + "?mode=ro", uri=True, timeout=30)
d = sqlite3.connect(dst)
try:
    s.backup(d)
finally:
    d.close()
    s.close()
c = sqlite3.connect("file:" + dst + "?mode=ro", uri=True)
ok = c.execute("pragma integrity_check").fetchone()[0]
n = c.execute("select count(*) from account_flows").fetchone()[0]
c.close()
print(f"backup={dst} integrity={ok} flows={n}")
if ok != "ok":
    sys.exit(1)
PY
if [ $? -ne 0 ]; then
  log "BACKUP FAILED: $DST"
  rm -f "$DST"
  exit 1
fi
chmod 600 "$DST"
gzip -9 -f "$DST"
log "backup ok: $DST.gz $(stat -c%s "$DST.gz") bytes"

# 本地保留 30 份
find "$DIR" -maxdepth 1 -name 'finance-20??????.db.gz' -mtime +"$KEEP_LOCAL" -print -delete >>"$LOG" 2>&1

# 每周日异机一份到 TC，远端保留 14 份（失败只记日志，不影响本地备份成功）
if [ "$(date +%u)" = "7" ]; then
  ssh -o BatchMode=yes -o ConnectTimeout=15 tc "mkdir -p ~/backups/finance" >>"$LOG" 2>&1
  if scp -q -o BatchMode=yes -o ConnectTimeout=20 "$DST.gz" "$OFFSITE" >>"$LOG" 2>&1; then
    ssh -o BatchMode=yes -o ConnectTimeout=15 tc \
      "ls -1t ~/backups/finance/finance-*.db.gz 2>/dev/null | tail -n +$((KEEP_REMOTE + 1)) | xargs -r rm -f" >>"$LOG" 2>&1
    log "offsite ok"
  else
    log "offsite FAILED (本地备份不受影响)"
  fi
fi
exit 0
