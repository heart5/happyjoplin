#!/bin/sh
# webchat保活脚本（配合 --renew 续期机制）
# 双平台适配：腾讯云 (conda newlsp) / Pixel 6 Pro Termux (系统 python)
# cron: */5 * * * * ~/codebase/happyjoplin/startwebchatprocess.sh >> ~/downloads/lifewebchat_tc.out 2>&1

if [ -f /tmp/webchat_renewing ]; then
    exit 0
fi

ps -fe|grep "python life/webchat.py" |grep -v grep
if [ $? -ne 0 ]
then
    echo "start life/webchat process....."
    cd ~/codebase/happyjoplin
    # 双平台自适配：有 conda 用 conda，无 conda 用系统 python (Termux)
    if [ -f /usr/miniconda3/etc/profile.d/conda.sh ]; then
        source /usr/miniconda3/etc/profile.d/conda.sh
        conda activate newlsp
    fi
    nohup python life/webchat.py >> ~/downloads/lifewebchat_tc.out 2>&1 &
else
    echo "python life/webchat.py is already running....."
fi
