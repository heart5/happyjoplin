#!/data/data/com.termux/files/usr/bin/bash
# Pixel 6 Pro 定时同步聊天记录到 hcx
# crontab 示例: */30 * * * * /path/to/phone_sync_cron.sh >> ~/tmp/phone_sync.log 2>&1
# 免打扰时段(23:00-07:00)自动跳过

cd ~/storage/shared/happyjoplin || cd ~/codebase/happyjoplin || exit 1

python work/phone_sync.py --limit 2000
