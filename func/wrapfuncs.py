# encoding:utf-8
# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     formats: ipynb,py:percent
#     notebook_metadata_filter: jupytext,-kernelspec,-jupytext.text_representation.jupytext_version
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
# ---

# %% [markdown]
# # 装饰器功能函数集

# %%
"""
装饰器函数集，ift2phone、timethis, logit
"""

# %% [markdown]
# ## 引入重要库

# %%
import time
from functools import wraps
from inspect import signature

# %%
import pathmagic

with pathmagic.context():
    from func.logme import log
    from func.nettools import ifttt_notify

    # from func.jpfuncs import getinivaluefromcloud
    from func.sysfunc import not_IPython


# %% [markdown]
# ## 功能函数集合

# %% [markdown]
# ### def logit(func)


# %%
def logit(func):
    """
    函数具体调用信息写入日志或print至控制台
    :param func
    :return
    """

    @wraps(func)
    def with_logging(*args, **kwargs):
        def truncate(arg):
            if isinstance(arg, str) and len(arg) > 50:
                return arg[:50] + "...(参数超长，显示截断)"
            elif isinstance(arg, list) and len(arg) > 10:
                return arg[:10] + ["...(列表超长，显示截断)"]
            elif isinstance(arg, dict) and len(arg) > 10:
                truncated_dict = {k: arg[k] for i, k in enumerate(arg) if i < 10}
                truncated_dict["...(字典超长，显示截断)"] = "..."
                return truncated_dict
            return arg

        args4show = [truncate(x) for x in args]
        kwargs4show = {k: truncate(v) for k, v in kwargs.items()}
        if not_IPython():
            log.info(f"{func.__name__}函数被调用，参数列表：{args4show}, 关键字参数：{kwargs4show}")
        else:
            print(f"{func.__name__}函数被调用，参数列表：{args4show}, 关键字参数：{kwargs4show}")

        return func(*args, **kwargs)

    return with_logging


# %% [markdown]
# ### def ift2phone(msg=None)


# %%
def ift2phone(msg=None):
    """
    目标函数运行时将信息通过ifttt发送至手机
    :param msg:
    :return:
    """

    def decorate(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            if msg is None:
                msginner = func.__doc__
            else:
                msginner = msg
            ifttt_notify(f"{msginner}_{args}", f"{func.__name__}")
            return result

        return wrapper

    return decorate


# %% [markdown]
# ### def timethis(func)


# %%
def timethis(func):
    """
    装饰执行时间（tida）
    :param func:
    :return:
    """

    @logit
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        timelen = end - start
        if timelen >= (60 * 60):
            timelenstr = (
                f"{int(timelen / (60 * 60))}小时{int((timelen % (60 * 60)) / 60)}分钟{timelen % (60 * 60) % 60:.2f}秒"
            )
        elif timelen >= 60:
            timelenstr = f"{int(timelen / 60)}分钟{timelen % 60:.2f}秒"
        else:
            timelenstr = f"{timelen % 60:.2f}秒"
        if not_IPython():
            log.info(f"{func.__name__}\t{timelenstr}")
        else:
            print(f"{func.__name__}\t{timelenstr}")

        return result

    return wrapper


# %% [markdown]
# ### def countdown(n: int) # 用于测试各种装饰器


# %%
@timethis
@ift2phone("倒数计时器")
@ift2phone()
# @lpt_wrapper()
def countdown(n: int):
    """
    倒计时
    :param n:
    :return: NULL
    """
    print(n)
    while n > 0:
        n -= 1
        if (n % 5000) == 0:
            print(n)


# %% [markdown]
# ## 主函数main

# %%
if __name__ == "__main__":
    if not_IPython():
        log.info(f"运行文件\t{__file__}")
    countdown(10088)
    print(f"函数名\t{countdown.__name__}")
    print(f"函数文档\t{countdown.__doc__}")
    print(f"函数参数注释\t{countdown.__annotations__}")
    # countdown(12234353)
    countdown(500)
    countdown.__wrapped__(500)
    print(f"函数参数签名\t{signature(countdown)}")
    print(f"函数类名\t{countdown.__class__}")
    print(f"函数模块\t{countdown.__module__}")
    print(f"函数包裹函数\t{countdown.__wrapped__}")
    print(f"函数语句\t{countdown.__closure__}")
    print(f"函数代码\t{countdown.__code__}")
    print(f"函数默认值\t{countdown.__defaults__}")
    print(f"函数字典\t{countdown.__dict__}")
    print(f"函数内涵全集\t{countdown.__dir__()}")
    if not_IPython():
        log.info(f"文件\t{__file__}\t结束运行")
