#!/bin/sh
# webchat保活脚本（配合 --renew 续期机制）
# 适用于 Pixel 6 Pro Termux 和 腾讯云
# cron: */5 * * * * ~/codebase/happyjoplin/startwebchatprocess.sh

# Termux 用 TMPDIR 或 ~/tmp，标准 Linux 用 /tmp
TMP="${TMPDIR:-/tmp}"
if [ ! -w "$TMP" ]; then
    TMP="$HOME/tmp"
    mkdir -p "$TMP"
fi

# --renew 续期进行中，不干预
if [ -f "$TMP/webchat_renewing" ]; then
    exit 0
fi

# 自动检测 Python 路径：腾讯云用 newlsp conda 环境，Termux 用系统 python
if [ -x /usr/miniconda3/envs/newlsp/bin/python ]; then
    PYTHON=/usr/miniconda3/envs/newlsp/bin/python
else
    PYTHON=python
fi

ps -fe|grep 'python life/webchat.py' |grep -v grep
if [ $? -ne 0 ]
then
    echo "start life/webchat process....."
    cd ~/codebase/happyjoplin
    nohup $PYTHON life/webchat.py >> "$TMP/lifewebchat.out" 2>&1 &
else
    echo "python life/webchat.py is already running....."
fi
