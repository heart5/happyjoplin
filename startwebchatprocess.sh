#!/bin/sh
# 腾讯云: webchat保活脚本（配合 --renew 续期机制）
# cron: */5 * * * * ~/codebase/happyjoplin/startwebchatprocess.sh >> ~/downloads/lifewebchat_tc.out 2>&1

if [ -f /tmp/webchat_renewing ]; then
    exit 0
fi

ps -fe|grep "python life/webchat.py" |grep -v grep
if [ $? -ne 0 ]
then
    echo "start life/webchat process....."
    cd ~/codebase/happyjoplin
    source /usr/miniconda3/etc/profile.d/conda.sh
    conda activate newlsp
    nohup python life/webchat.py >> ~/downloads/lifewebchat_tc.out 2>&1 &
else
    echo "python life/webchat.py is already running....."
fi
