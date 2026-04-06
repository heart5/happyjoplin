# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     notebook_metadata_filter: jupytext,-kernelspec,-jupytext.text_representation.jupytext_version
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
# ---

# %% [markdown]
# # joplin笔记知识库客户端

# %% [markdown]
# # 导入库

# %%
from typing import Any, Dict, List, Optional

import requests

# %%
import pathmagic

with pathmagic.context():
    from etc.getid import getdeviceid, getdevicename
    from func.configpr import getcfpoptionvalue, setcfpoptionvalue
    from func.first import dirmainpath, getdirmain, touchfilepath2depth
    from func.logme import log
    from func.nettools import trycounttimes2
    from func.sysfunc import execcmd, listallloghander, not_IPython, uuid3hexstr


# %% [markdown]
# # JoplinQAClient类

# %%
class JoplinQAClient:
    def __init__(self, base_url="http://127.0.0.1:5000"):
        self.base_url = base_url
        # 关键修改：允许传入 session_id，否则生成一个随机的
        if session_id is None:
            import uuid

            session_id = f"session_{uuid.uuid4().hex[:8]}"  # 生成随机会话ID
        self.session_id = session_id
        log.info(f"JoplinQAClient 初始化，会话ID: {self.session_id}")

    def ask(self, question, use_history=True):
        resp = requests.post(
            f"{self.base_url}/ask",
            json={
                "question": question,
                "session_id": self.session_id,
                "use_history": use_history,
            },
        )
        return resp.json()

    def get_history(self, limit=10):
        resp = requests.get(
            f"{self.base_url}/history",
            params={"session_id": self.session_id, "limit": limit},
        )
        return resp.json()

    # +++ 新增功能1：清空指定会话的历史记录 +++
    def clear_history(self, session_id: Optional[str] = None) -> Dict[str, Any]:
        """清空指定会话的对话历史。

        Args:
            session_id: 要清空的会话ID。如果为None，则使用客户端实例的默认session_id。

        Returns:
            API响应字典。
        """
        url = f"{self.base_url}/clear_history"

        try:
            response = requests.post(
                url, json={"session_id": self.session_id}, timeout=10
            )
            response.raise_for_status()  # 如果状态码不是200，抛出异常
            return response.json()
        except requests.exceptions.RequestException as e:
            return {
                "success": False,
                "error": f"请求API失败: {e}",
                "session_id": self.session_id,
            }

    # +++ 新增功能2：获取统计信息 +++
    def get_statistics(self, session_id: Optional[str] = None) -> Dict[str, Any]:
        """获取指定会话问答系统的统计信息。

        Args:
            session_id: 要查询的会话ID。如果为None，则使用客户端实例的默认session_id。

        Returns:
            包含统计信息的字典。
        """
        url = f"{self.base_url}/stats"

        try:
            # 这里使用GET请求，并通过查询参数传递session_id
            params = {"session_id": self.session_id}
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return {
                "success": False,
                "error": f"请求API失败: {e}",
                "session_id": self.session_id,
            }


# %% [markdown]
# # 全局变量

# %%
client = JoplinQAClient()


# %% [markdown]
# # 函数库

# %% [markdown]
# ## qa4joplin(question: str) -> str

# %%
def qa4joplin(question: str) -> str:
    global client
    result = client.ask(question)
    answer = result.get("answer")
    # print("答案:", result.get("answer"))
    log.debug(f"问题为：{question}，答案长度为：{len(answer)}")
    return answer


# %% [markdown]
# # 主函数main

# %%
if __name__ == "__main__":
    result = qa4joplin("介绍以下我（白晔峰）")
    print(result)
