# scripts/sms_fields_check.py
"""查看 termux-sms-list 返回的字段名，了解手机端短信的数据结构。"""
import json
import subprocess

r = subprocess.run(
    ["termux-sms-list", "-d", "-l", "3"],
    capture_output=True, text=True, timeout=30,
)
data = json.loads(r.stdout)
for i, m in enumerate(data):
    print(f"\n=== 短信 {i+1} ===")
    print(json.dumps(m, indent=2, ensure_ascii=False))
