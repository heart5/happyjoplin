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
# # 主机配置收集与对比工具（增强版）

# %%
"""主机配置收集与对比工具（增强版）
功能：收集各主机配置信息，存储到本地配置文件，并同步到Joplin笔记中
支持：云端配置库列表、变化检测、更新记录
"""

# %% [markdown]
# ## 导入重要库

# %%
import json
import os
import platform
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

# 导入项目相关模块
import pathmagic

with pathmagic.context():
    from etc.getid import getdeviceid, getdevicename, gethostuser
    from func.configpr import findvaluebykeyinsection, getcfpoptionvalue, setcfpoptionvalue
    from func.first import dirmainpath, getdirmain
    from func.jpfuncs import (
        createnote,
        getinivaluefromcloud,
        getnote,
        jpapi,
        searchnotebook,
        searchnotes,
        updatenote_body,
        updatenote_title,
    )
    from func.logme import log
    from func.sysfunc import execcmd, not_IPython
    from func.wrapfuncs import timethis


# %% [markdown]
# ## 类和函数

# %% [markdown]
# ### get_libs_from_cloud(config_key: str) -> List[str]

# %%
def get_libs_from_cloud(config_key: str) -> List[str]:
    """从云端配置获取库列表"""
    try:
        libs_str = getinivaluefromcloud("hostconfig", config_key)
        if libs_str:
            # 支持逗号、分号、空格分隔
            libs = []
            for sep in [",", ";", "\n"]:
                if sep in libs_str:
                    libs = [lib.strip() for lib in libs_str.split(sep) if lib.strip()]
                    break
            if not libs:  # 如果没有分隔符，尝试空格分隔
                libs = [lib.strip() for lib in libs_str.split() if lib.strip()]

            # 过滤空字符串
            libs = [lib for lib in libs if lib]

            if libs:
                log.info(f"从云端配置获取 {config_key}: {len(libs)} 个库")
                return libs
    except Exception as e:
        log.warning(f"获取云端配置 {config_key} 失败: {e}")

    # 默认库列表（如果云端配置不存在）
    default_libs = {
        "required_libs": [
            "pandas",
            "numpy",
            "matplotlib",
            "jupyter",
            "jupyterlab",
            "notebook",
            "seaborn",
            "scipy",
            "scikit-learn",
            "geopandas",
            "plotly",
            "dash",
            "joplin",
            "pathmagic",
            "arrow",
        ],
        "optional_libs": [
            "torch",
            "tensorflow",
            "keras",
            "pytorch",
            "transformers",
            "langchain",
            "openai",
            "anthropic",
            "cohere",
        ],
        "ai_libs": [
            "torch",
            "tensorflow",
            "keras",
            "pytorch",
            "transformers",
            "langchain",
            "openai",
            "anthropic",
            "cohere",
            "llama_index",
        ],
    }

    return default_libs.get(config_key, [])


# %% [markdown]
# ### class HostConfigCollector

# %%
class HostConfigCollector:
    """主机配置收集器（增强版）"""


# %% [markdown]
# #### def __init__(self)

    # %%
    def __init__(self):
        self.config_name = "hostconfig"
        self.device_id = getdeviceid()
        self.host_user = gethostuser()
        self.device_name = getdevicename()
        self.config_dir = getdirmain() / "data" / "hostconfig"
        self.config_dir.mkdir(parents=True, exist_ok=True)
        
        # 从云端配置获取库列表
        self.required_libs = get_libs_from_cloud("required_libs")
        self.optional_libs = get_libs_from_cloud("optional_libs")
        self.ai_libs = get_libs_from_cloud("ai_libs")
        
        # 本地配置文件路径
        self.local_config_file = self.config_dir / f"{self.device_id}.json"
        
        # 更新记录文件
        self.update_record_file = self.config_dir / f"{self.device_id}_updates.json"
        
        # 存储配置数据
        self.config_data = None

# %% [markdown]
# #### get_config_data(self) -> Dict[str, Any]

    # %%
    def get_config_data(self) -> Dict[str, Any]:
        """获取配置数据"""
        if self.config_data is None:
            # 如果没有配置数据，收集当前主机的配置
            self.config_data = self.collect_all_info()
        return self.config_data

# %% [markdown]
# #### save_config_to_local(self) -> bool

    # %%
    @timethis
    def save_config_to_local(self) -> bool:
        """保存配置到本地文件"""
        if self.config_data is None:
            log.warning(f"设备 {self.device_name} 没有配置数据，无法保存")
            return False
        
        try:
            with open(self.local_config_file, "w", encoding="utf-8") as f:
                json.dump(self.config_data, f, indent=2, ensure_ascii=False)
            log.info(f"配置信息已保存到: {self.local_config_file}")
            return True
        except Exception as e:
            log.error(f"保存配置失败: {e}")
            return False

# %% [markdown]
# #### _cleanup_old_configs(self, current_configs: Dict[str, Any]) -> None

    # %%
    def _cleanup_old_configs(self, current_configs: Dict[str, Any]) -> None:
        """清理过时的本地配置文件"""
        try:
            # 获取当前有效的设备ID集合
            current_device_ids = set(current_configs.keys())
            current_device_ids.add(self.device_id)  # 包括当前主机

            # 遍历所有配置文件
            for config_file in self.config_dir.glob("*.json"):
                if "_updates.json" in str(config_file):
                    continue

                # 提取设备ID（从文件名）
                device_id = config_file.stem

                # 如果设备ID不在当前有效集合中，且不是当前主机
                if device_id not in current_device_ids and device_id != self.device_id:
                    # 检查文件创建时间
                    file_age_days = (datetime.now() - datetime.fromtimestamp(config_file.stat().st_mtime)).days

                    if file_age_days > 30:  # 超过30天的旧文件
                        try:
                            config_file.unlink()
                            log.info(f"清理过时配置文件: {device_id} ({file_age_days}天)")
                        except Exception as e:
                            log.error(f"清理配置文件失败: {device_id}, {e}")
        except Exception as e:
            log.error(f"清理过时配置失败: {e}")

    # %%
    def save_config_to_local(self) -> bool:
        """保存配置到本地文件"""
        if self.config_data is None:
            log.warning(f"设备 {self.device_name} 没有配置数据，无法保存")
            return False
        
        try:
            with open(self.local_config_file, "w", encoding="utf-8") as f:
                json.dump(self.config_data, f, indent=2, ensure_ascii=False)
            log.info(f"配置信息已保存到: {self.local_config_file}")
            return True
        except Exception as e:
            log.error(f"保存配置失败: {e}")
            return False

# %% [markdown]
# #### _load_previous_config(self) -> Dict[str, Any]

    # %%
    @timethis
    def _load_previous_config(self) -> Dict[str, Any]:
        """加载上一次的配置"""
        if not self.local_config_file.exists():
            return {}

        try:
            with open(self.local_config_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            log.error(f"加载上一次配置失败: {e}")
            return {}

# %% [markdown]
# #### _cleanup_old_configs(self, current_configs: Dict[str, Any]) -> None

    # %%
    def _cleanup_old_configs(self, current_configs: Dict[str, Any]) -> None:
        """清理过时的本地配置文件"""
        try:
            # 获取当前有效的设备ID集合
            current_device_ids = set(current_configs.keys())
            current_device_ids.add(self.device_id)  # 包括当前主机

            # 遍历所有配置文件
            for config_file in self.config_dir.glob("*.json"):
                if "_updates.json" in str(config_file):
                    continue

                # 提取设备ID（从文件名）
                device_id = config_file.stem

                # 如果设备ID不在当前有效集合中，且不是当前主机
                if device_id not in current_device_ids and device_id != self.device_id:
                    # 检查文件创建时间
                    file_age_days = (datetime.now() - datetime.fromtimestamp(config_file.stat().st_mtime)).days

                    if file_age_days > 30:  # 超过30天的旧文件
                        try:
                            config_file.unlink()
                            log.info(f"清理过时配置文件: {device_id} ({file_age_days}天)")
                        except Exception as e:
                            log.error(f"清理配置文件失败: {device_id}, {e}")
        except Exception as e:
            log.error(f"清理过时配置失败: {e}")

# %% [markdown]
# #### _is_config_complete(self, config: Dict[str, Any]) -> bool

    # %%
    def _is_config_complete(self, config: Dict[str, Any]) -> bool:
        """检查配置是否完整"""
        try:
            # 检查必需字段
            required_fields = ["system", "python", "libraries", "collection_time"]

            for field in required_fields:
                if field not in config:
                    log.debug(f"配置缺少必需字段: {field}")
                    return False

            # 检查system字段的必需子字段
            if "device_name" not in config["system"]:
                log.debug(f"配置缺少device_name")
                return False

            if "device_id" not in config["system"]:
                log.debug(f"配置缺少device_id")
                return False

            return True

        except Exception as e:
            log.debug(f"检查配置完整性失败: {e}")
            return False

# %% [markdown]
# #### _load_update_records(self) -> List[Dict[str, Any]]

    # %%
    @timethis
    def _load_update_records(self) -> List[Dict[str, Any]]:
        """加载更新记录"""
        if not self.update_record_file.exists():
            return []

        try:
            with open(self.update_record_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            log.error(f"加载更新记录失败: {e}")
            return []

# %% [markdown]
# #### _save_update_record(self, old_config: Dict[str, Any], new_config: Dict[str, Any]) -> Dict[str, Any]

    # %%
    @timethis
    def _save_update_record(self, old_config: Dict[str, Any], new_config: Dict[str, Any]) -> Dict[str, Any]:
        """保存更新记录"""
        update_time = datetime.now().isoformat()

        # 检测变化
        changes = self._detect_changes(old_config, new_config)

        # 如果没有变化，直接返回空记录，不保存到文件
        if not changes:
            return {
                "timestamp": update_time,
                "device_id": self.device_id,
                "device_name": self.device_name,
                "has_changes": False,
                "changes": {},
                "summary": "无变化"
            }

        update_record = {
            "timestamp": update_time,
            "device_id": self.device_id,
            "device_name": self.device_name,
            "has_changes": len(changes) > 0,
            "changes": changes,
            "summary": self._generate_change_summary(changes),
        }

        # 加载现有记录
        records = self._load_update_records()

        # 只保留最近100条记录
        records.insert(0, update_record)
        if len(records) > 100:
            records = records[:100]

        # 保存记录
        try:
            with open(self.update_record_file, "w", encoding="utf-8") as f:
                json.dump(records, f, indent=2, ensure_ascii=False)
            log.info(f"更新记录已保存: {self.update_record_file}")
        except Exception as e:
            log.error(f"保存更新记录失败: {e}")

        return update_record

# %% [markdown]
# #### _detect_changes(self, old_config: Dict[str, Any], new_config: Dict[str, Any]) -> Dict[str, Any]

    # %%
    @timethis
    def _detect_changes(self, old_config: Dict[str, Any], new_config: Dict[str, Any]) -> Dict[str, Any]:
        """检测配置变化"""
        if not old_config:
            return {"initial": "首次收集配置"}

        changes = {}

        # 1. 检测Python版本变化
        old_python = old_config.get("python", {}).get("python_version", "")
        new_python = new_config.get("python", {}).get("python_version", "")
        if old_python != new_python:
            changes["python_version"] = {"old": old_python, "new": new_python}

        # 2. 检测Conda版本变化
        old_conda = old_config.get("python", {}).get("conda_version", "")
        new_conda = new_config.get("python", {}).get("conda_version", "")
        if old_conda != new_conda:
            changes["conda_version"] = {"old": old_conda, "new": new_conda}

        # 3. 检测Conda环境变化
        old_env = old_config.get("python", {}).get("conda_env", "")
        new_env = new_config.get("python", {}).get("conda_env", "")
        if old_env != new_env:
            changes["conda_env"] = {"old": old_env, "new": new_env}

        # 4. 检测核心库版本变化
        old_libs = old_config.get("libraries", {})
        new_libs = new_config.get("libraries", {})

        # 监控的关键库
        key_libs = ["pandas", "numpy", "matplotlib", "jupyter", "jupyterlab"]
        for lib in key_libs:
            old_ver = old_libs.get(lib, "")
            new_ver = new_libs.get(lib, "")
            if old_ver != new_ver:
                changes[f"lib_{lib}"] = {"old": old_ver, "new": new_ver}

        # 5. 检测库安装状态变化
        all_libs = set(list(old_libs.keys()) + list(new_libs.keys()))
        for lib in all_libs:
            old_status = old_libs.get(lib, "Not installed")
            new_status = new_libs.get(lib, "Not installed")

            # 检查状态变化
            if old_status != new_status:
                # 如果是版本号变化，已经在上面的key_libs中处理了
                if lib not in key_libs:
                    # 检查是否是从未安装到安装，或从安装到未安装
                    if (old_status == "Not installed" and new_status != "Not installed") or (
                        old_status != "Not installed" and new_status == "Not installed"
                    ):
                        changes[f"lib_status_{lib}"] = {"old": old_status, "new": new_status}

        return changes

# %% [markdown]
# #### _generate_change_summary(self, changes: Dict[str, Any]) -> str

    # %%
    @timethis
    def _generate_change_summary(self, changes: Dict[str, Any]) -> str:
        """生成变化摘要"""
        if not changes:
            return "无变化"

        summary_parts = []

        if "python_version" in changes:
            old = changes["python_version"]["old"]
            new = changes["python_version"]["new"]
            summary_parts.append(f"Python: {old} → {new}")

        if "conda_version" in changes:
            old = changes["conda_version"]["old"]
            new = changes["conda_version"]["new"]
            summary_parts.append(f"Conda: {old} → {new}")

        if "conda_env" in changes:
            old = changes["conda_env"]["old"]
            new = changes["conda_env"]["new"]
            summary_parts.append(f"环境: {old} → {new}")

        # 库版本变化
        lib_changes = [k for k in changes.keys() if k.startswith("lib_") and not k.startswith("lib_status_")]
        for lib_key in lib_changes:
            lib_name = lib_key.replace("lib_", "")
            old = changes[lib_key]["old"]
            new = changes[lib_key]["new"]
            summary_parts.append(f"{lib_name}: {old} → {new}")

        # 库状态变化
        status_changes = [k for k in changes.keys() if k.startswith("lib_status_")]
        for status_key in status_changes:
            lib_name = status_key.replace("lib_status_", "")
            old = changes[status_key]["old"]
            new = changes[status_key]["new"]
            if old == "Not installed":
                summary_parts.append(f"安装 {lib_name}: {new}")
            elif new == "Not installed":
                summary_parts.append(f"卸载 {lib_name}")
            else:
                summary_parts.append(f"{lib_name}: {old} → {new}")

        return "; ".join(summary_parts)

# %% [markdown]
# #### _format_changes_summary(self, changes: Dict[str, Any]) -> str

    # %%
    def _format_changes_summary(self, changes: Dict[str, Any]) -> str:
        """格式化变化摘要"""
        if not changes:
            return "无变化"
        
        summary_parts = []
        
        for key, change_info in changes.items():
            if key == "initial":
                summary_parts.append("首次收集配置")
            elif isinstance(change_info, dict) and "old" in change_info and "new" in change_info:
                summary_parts.append(f"{key}: {change_info['old']} -> {change_info['new']}")
            else:
                summary_parts.append(f"{key}: {change_info}")
        
        return "; ".join(summary_parts)


# %% [markdown]
# #### get_system_info(self)

    # %%
    @timethis
    def get_system_info(self):
        """获取系统信息"""
        system_info = {
            "device_id": self.device_id,
            "device_name": self.device_name,
            "host_user": self.host_user,
            "timestamp": datetime.now().isoformat(),
            "system": {
                "platform": platform.platform(),
                "system": platform.system(),
                "release": platform.release(),
                "version": platform.version(),
                "machine": platform.machine(),
                "processor": platform.processor(),
                "architecture": platform.architecture(),
            },
        }

        # 获取更多系统信息
        try:
            if platform.system() == "Linux":
                # 获取发行版信息
                try:
                    with open("/etc/os-release", "r") as f:
                        for line in f:
                            if line.startswith("PRETTY_NAME="):
                                system_info["system"]["distro"] = line.split("=")[1].strip().strip('"')
                                break
                except:
                    pass

                # 获取内核版本
                try:
                    system_info["system"]["kernel"] = execcmd("uname -r").strip()
                except:
                    pass
            elif platform.system() == "Windows":
                # Windows 特定信息
                try:
                    system_info["system"]["windows_edition"] = platform.win32_edition()
                except:
                    pass
        except Exception as e:
            log.warning(f"获取系统额外信息失败: {e}")

        return system_info

# %% [markdown]
# #### get_python_info(self)

    # %%
    @timethis
    def get_python_info(self):
        """获取Python环境信息"""
        python_info = {
            "python_version": platform.python_version(),
            "python_implementation": platform.python_implementation(),
            "python_compiler": platform.python_compiler(),
            "python_build": platform.python_build(),
        }

        # 获取conda信息
        try:
            conda_result = subprocess.run(["conda", "--version"], capture_output=True, text=True, timeout=5)
            if conda_result.returncode == 0:
                python_info["conda_version"] = conda_result.stdout.strip()
            else:
                python_info["conda_version"] = "Not installed or not in PATH"
        except (subprocess.TimeoutExpired, FileNotFoundError):
            python_info["conda_version"] = "Not installed or not in PATH"

        # 获取pip信息
        try:
            pip_result = subprocess.run(["pip", "--version"], capture_output=True, text=True, timeout=5)
            if pip_result.returncode == 0:
                pip_output = pip_result.stdout.strip()
                python_info["pip_version"] = pip_output.split()
            else:
                python_info["pip_version"] = "Unknown"
        except (subprocess.TimeoutExpired, FileNotFoundError):
            python_info["pip_version"] = "Unknown"

        # 获取虚拟环境信息
        python_info["virtual_env"] = os.environ.get("VIRTUAL_ENV", "Not in virtual environment")
        python_info["conda_env"] = os.environ.get("CONDA_DEFAULT_ENV", "base")

        return python_info

# %% [markdown]
# #### get_library_versions(self)

    # %%
    @timethis
    def get_library_versions(self):
        """获取库版本信息"""
        lib_versions = {}

        # 合并所有要检查的库
        all_libs = list(set(self.required_libs + self.optional_libs + self.ai_libs))

        for lib_name in all_libs:
            try:
                module = __import__(lib_name)
                version = getattr(module, "__version__", "Unknown")
                lib_versions[lib_name] = version
            except ImportError:
                lib_versions[lib_name] = "Not installed"
            except Exception as e:
                lib_versions[lib_name] = f"Error: {str(e)[:50]}"

        return lib_versions

# %% [markdown]
# #### get_project_info(self)

    # %%
    @timethis
    def get_project_info(self):
        """获取项目相关信息"""
        project_info = {"project_path": str(getdirmain()), "codebase_path": str(dirmainpath), "config_files": {}}

        # 检查重要配置文件
        config_files = ["pyproject.toml", "requirements.txt", "environment.yml", "setup.py", "README.md"]

        for config_file in config_files:
            file_path = getdirmain() / config_file
            if file_path.exists():
                project_info["config_files"][config_file] = "Exists"
                # 获取文件修改时间
                try:
                    mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                    project_info["config_files"][f"{config_file}_mtime"] = mtime.isoformat()
                except:
                    pass
            else:
                project_info["config_files"][config_file] = "Not found"

        return project_info

# %% [markdown]
# #### collect_all_info(self)

    # %%
    @timethis
    def collect_all_info(self):
        """收集所有配置信息"""
        log.info(f"开始收集主机配置信息: {self.device_name} ({self.host_user})")

        all_info = {
            "system": self.get_system_info(),
            "python": self.get_python_info(),
            "libraries": self.get_library_versions(),
            "project": self.get_project_info(),
            "collection_time": datetime.now().isoformat(),
        }

        return all_info

# %% [markdown]
# #### save_config_with_change_detection(self, new_config: Dict[str, Any]) -> Dict[str, Any]

    # %%
    @timethis
    def save_config_with_change_detection(self, current_config: Dict[str, Any]) -> Dict[str, Any]:
        """保存配置并检测变化，返回更新记录"""
        try:
            # 加载旧配置
            old_config = self._load_previous_config()
            
            # 检测变化
            changes = self._detect_changes(old_config, current_config)
            
            if changes:
                # 有变化，保存新配置
                self.config_data = current_config
                self.save_config_to_local()
                
                # 创建更新记录
                update_record = {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "device_id": current_config["system"]["device_id"],
                    "device_name": current_config["system"]["device_name"],
                    "has_changes": True,
                    "summary": self._format_changes_summary(changes)
                }
                
                log.info(f"配置有变化，已保存: {update_record['summary']}")
                return update_record
            else:
                # 无变化
                log.info("配置无变化，跳过保存配置文件")
                
                # 返回一个无变化的更新记录
                update_record = {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "device_id": current_config["system"]["device_id"],
                    "device_name": current_config["system"]["device_name"],
                    "has_changes": False,
                    "summary": "无变化"
                }
                
                return update_record
                
        except Exception as e:
            log.error(f"保存配置失败: {e}")
            
            # 返回一个错误更新记录
            update_record = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "device_id": current_config["system"]["device_id"],
                "device_name": current_config["system"]["device_name"],
                "has_changes": False,
                "summary": f"保存配置失败: {str(e)}"
            }
            
            return update_record


# %% [markdown]
# #### show_config_summary(self)

    # %%
    def show_config_summary(self):
        """显示配置摘要"""
        pass


# %% [markdown]
# ### show_config_summary(self)

# %%
def show_config_summary(self):
    """显示配置摘要"""
    config = self.collect_all_info()

    print("=" * 60)
    print(f"主机配置摘要: {self.device_name}")
    print("=" * 60)

    print(f"\n1. 系统信息:")
    print(f"   设备ID: {config['system']['device_id']}")
    print(f"   设备名: {config['system']['device_name']}")
    print(f"   用户: {config['system']['host_user']}")
    print(f"   系统: {config['system']['system'].get('distro', config['system']['system']['platform'])}")
    print(f"   架构: {config['system']['system']['machine']}")

    print(f"\n2. Python环境:")
    print(f"   Python版本: {config['python']['python_version']}")
    print(f"   Conda版本: {config['python'].get('conda_version', 'N/A')}")
    print(f"   Conda环境: {config['python']['conda_env']}")

    print(f"\n3. 核心库版本:")
    core_libs = self.required_libs[:5]  # 显示前5个核心库
    for lib in core_libs:
        version = config["libraries"].get(lib, "Not installed")
        print(f"   {lib:15} : {version}")

    print(f"\n4. 配置文件:")
    print(f"   本地存储: {self.local_config_file}")
    print(f"   文件大小: {self.local_config_file.stat().st_size if self.local_config_file.exists() else 0} bytes")

    print(f"\n5. 收集时间: {config['collection_time']}")
    print("=" * 60)


HostConfigCollector.show_config_summary = show_config_summary


# %% [markdown]
# ### JoplinConfigManager

# %%
class JoplinConfigManager:
    """Joplin配置管理器 - 处理多主机配置对比和笔记同步"""


# %% [markdown]
# #### __init__(self, config_dir: Path = None)

    # %%
    def __init__(self, config_dir: Path = None):
        """初始化"""
        self.device_id = getdeviceid()
        if config_dir is None:
            self.config_dir = getdirmain() / "data" / "hostconfig"
        else:
            self.config_dir = config_dir

        self.config_dir.mkdir(parents=True, exist_ok=True)

        # 从云端配置获取库列表
        self.required_libs = get_libs_from_cloud("required_libs")
        self.optional_libs = get_libs_from_cloud("optional_libs")
        self.ai_libs = get_libs_from_cloud("ai_libs")

# %% [markdown]
# #### _is_config_complete(self, config: Dict[str, Any]) -> bool

    # %%
    def _is_config_complete(self, config: Dict[str, Any]) -> bool:
        """检查配置是否完整"""
        try:
            # 检查必需字段
            required_fields = ["system", "python", "libraries", "collection_time"]

            for field in required_fields:
                if field not in config:
                    log.debug(f"配置缺少必需字段: {field}")
                    return False

            # 检查system字段的必需子字段
            if "device_name" not in config["system"]:
                log.debug(f"配置缺少device_name")
                return False

            if "device_id" not in config["system"]:
                log.debug(f"配置缺少device_id")
                return False

            return True

        except Exception as e:
            log.debug(f"检查配置完整性失败: {e}")
            return False

# %% [markdown]
# #### _cleanup_old_configs(self, current_configs: Dict[str, Any]) -> None

    # %%
    def _cleanup_old_configs(self, current_configs: Dict[str, Any]) -> None:
        """清理过时的本地配置文件"""
        try:
            # 获取当前有效的设备ID集合
            current_device_ids = set(current_configs.keys())
            current_device_ids.add(self.device_id)  # 包括当前主机

            # 遍历所有配置文件
            for config_file in self.config_dir.glob("*.json"):
                if "_updates.json" in str(config_file):
                    continue

                # 提取设备ID（从文件名）
                device_id = config_file.stem

                # 如果设备ID不在当前有效集合中，且不是当前主机
                if device_id not in current_device_ids and device_id != self.device_id:
                    # 检查文件创建时间
                    file_age_days = (datetime.now() - datetime.fromtimestamp(config_file.stat().st_mtime)).days

                    if file_age_days > 30:  # 超过30天的旧文件
                        try:
                            config_file.unlink()
                            log.info(f"清理过时配置文件: {device_id} ({file_age_days}天)")
                        except Exception as e:
                            log.error(f"清理配置文件失败: {device_id}, {e}")
        except Exception as e:
            log.error(f"清理过时配置失败: {e}")

# %% [markdown]
# #### load_all_configs(self)

    # %%
    @timethis
    def load_all_configs(self):
        """加载所有主机的配置信息"""
        configs = {}

        for config_file in self.config_dir.glob("*.json"):
            # 跳过更新记录文件
            if "_updates.json" in str(config_file):
                continue

            try:
                with open(config_file, "r", encoding="utf-8") as f:
                    config_data = json.load(f)
                    device_id = config_data["system"]["device_id"]
                    configs[device_id] = config_data
            except Exception as e:
                log.error(f"加载配置文件 {config_file} 失败: {e}")

        return configs

# %% [markdown]
# #### load_all_update_records(self)

    # %%
    @timethis
    def load_all_update_records(self):
        """加载所有主机的更新记录"""
        all_records = {}

        for record_file in self.config_dir.glob("*_updates.json"):
            try:
                device_id = record_file.stem.replace("_updates", "")
                with open(record_file, "r", encoding="utf-8") as f:
                    records = json.load(f)
                    all_records[device_id] = records
            except Exception as e:
                log.error(f"加载更新记录文件 {record_file} 失败: {e}")

        return all_records

# %% [markdown]
# #### save_update_records_to_local(self, all_update_records: Dict[str, Any]) -> None

    # %%
    @timethis
    def save_update_records_to_local(self, all_update_records: Dict[str, Any]) -> None:
        """保存所有主机的更新记录"""

        # 保存到本地
        for device_id, records in all_update_records.items():
            if device_id != self.device_id:  # 不覆盖当前主机的记录
                update_file = self.config_dir / f"{device_id}_updates.json"
    
                # 检查是否需要更新
                should_save = False
                if not update_file.exists():
                    should_save = True
                else:
                    try:
                        with open(update_file, "r", encoding="utf-8") as f:
                            existing_records = json.load(f)
    
                        # 如果记录数量不同，则更新
                        if len(existing_records) != len(records):
                            should_save = True
                    except:
                        should_save = True
    
                if should_save:
                    try:
                        with open(update_file, "w", encoding="utf-8") as f:
                            json.dump(records, f, indent=2, ensure_ascii=False)
                        log.info(f"同步更新记录: {device_id}")
                    except Exception as e:
                        log.error(f"保存更新记录失败: {device_id}, {e}")

# %% [markdown]
# #### parse_config_from_markdown_table(self, markdown_content: str) -> Tuple[Dict[str, Any], Dict[str, List[Dict[str, Any]]]]

    # %%
    @timethis
    def parse_config_from_markdown_table(self, markdown_content: str) -> Tuple[Dict[str, Any], Dict[str, List[Dict[str, Any]]]]:
        """从Markdown表格中解析配置信息和更新记录
        
        Args:
            markdown_content: Markdown笔记内容
            
        Returns:
            Tuple[configs, update_records]:
                configs: 设备ID -> 配置信息的字典
                update_records: 设备ID -> 更新记录列表的字典
        """
        configs = {}
        update_records = {}
        
        try:
            lines = markdown_content.split('\n')
            current_section = None
            device_names = []
            device_id_map = {}
            
            # 第一步：解析设备名称（从第一个表格的标题行）
            for i, line in enumerate(lines):
                line = line.strip()
                if line.startswith('| 配置项 |') and '|' in line:
                    # 分割单元格，移除空值
                    parts = [p.strip() for p in line.split('|') if p.strip()]
                    if len(parts) > 1:
                        # 提取设备名称（跳过第一个"配置项"）
                        device_names = parts[1:]
                        log.info(f"从表格中解析到设备名称: {device_names}")
                        break
            
            if not device_names:
                log.warning("无法从表格中解析设备名称")
                return {}, {}
            
            # 第二步：为每个设备查找对应的device_id
            for device_name in device_names:
                device_id = findvaluebykeyinsection("happyjpinifromcloud", "device", device_name)
                if not device_id or device_id == "None":
                    # 如果找不到，使用哈希作为后备
                    import hashlib
                    device_id = f"joplin_{hashlib.md5(device_name.encode()).hexdigest()[:8]}"
                    log.warning(f"无法找到设备 {device_name} 的ID，使用哈希ID: {device_id}")
                
                device_id_map[device_name] = device_id
                
                # 创建初始配置结构
                configs[device_id] = {
                    "system": {
                        "device_name": device_name,
                        "device_id": device_id,
                        "host_user": "N/A",
                        "system": {
                            "platform": "N/A",
                            "system": "N/A",
                            "release": "N/A",
                            "version": "N/A",
                            "machine": "N/A",
                            "processor": "N/A",
                        }
                    },
                    "python": {
                        "python_version": "N/A",
                        "conda_version": "N/A",
                        "pip_version": "N/A",
                        "virtual_env": "N/A",
                        "conda_env": "N/A",
                    },
                    "libraries": {},
                    "project": {
                        "project_path": "N/A",
                        "config_files": {}
                    },
                    "collection_time": "N/A"
                }
                
                # 初始化更新记录列表
                update_records[device_id] = []

            # 第三步：解析各个部分的配置
            for line in lines:
                line = line.strip()
                
                # 检测章节标题
                if line.startswith('## '):
                    if '系统信息' in line:
                        current_section = "system"
                    elif 'Python环境' in line:
                        current_section = "python"
                    elif '核心库版本' in line:
                        current_section = "core_libs"
                    elif 'AI/ML相关库' in line:
                        current_section = "ai_libs"
                    elif '项目信息' in line:
                        current_section = "project"
                    elif '信息收集时间' in line:
                        current_section = "collection_time"
                    elif '更新历史' in line:
                        current_section = "update_history"
                    continue
                
                # 跳过空行和非表格行
                if not line.startswith('|'):
                    continue
                
                # 跳过表头行和分隔行
                if line.startswith('|:---') or line.startswith('|---'):
                    continue
                
                # 解析表格行
                cells = [cell.strip() for cell in line.strip('|').split('|')]
                if len(cells) < 2:
                    continue
                
                # 第一列是配置项名称
                config_item = cells[0] if cells[0] else ""
                
                # 跳过空配置项和表头
                if not config_item or config_item == "配置项" or config_item == "主机" or config_item == "时间":
                    continue
                
                # 根据当前章节处理数据
                if current_section == "system":
                    # 系统信息表格
                    for j, device_name in enumerate(device_names):
                        if j + 1 < len(cells):
                            value = cells[j + 1]
                            device_id = device_id_map.get(device_name)
                            
                            if not device_id or device_id not in configs:
                                continue
                            
                            # 跳过无效值
                            if value in ["N/A", "Not found", "Unknown", "Not installed", ""]:
                                continue
                            
                            # 根据配置项填充数据
                            if config_item == "操作系统":
                                configs[device_id]["system"]["system"]["platform"] = value
                            elif config_item == "内核版本":
                                configs[device_id]["system"]["system"]["release"] = value
                            elif config_item == "架构":
                                # 解析架构信息，如 "x86_64 (64bit)"
                                if "(" in value:
                                    machine = value.split("(")[0].strip()
                                    configs[device_id]["system"]["system"]["machine"] = machine
                                else:
                                    configs[device_id]["system"]["system"]["machine"] = value
                            elif config_item == "主机用户":
                                configs[device_id]["system"]["host_user"] = value
                
                elif current_section == "python":
                    # Python环境表格
                    for j, device_name in enumerate(device_names):
                        if j + 1 < len(cells):
                            value = cells[j + 1]
                            device_id = device_id_map.get(device_name)
                            
                            if not device_id or device_id not in configs:
                                continue
                            
                            if value in ["N/A", "Not found", "Unknown", "Not installed", ""]:
                                continue
                            
                            if config_item == "Python版本":
                                configs[device_id]["python"]["python_version"] = value
                            elif config_item == "Conda版本":
                                configs[device_id]["python"]["conda_version"] = value
                            elif config_item == "Pip版本":
                                configs[device_id]["python"]["pip_version"] = value
                            elif config_item == "虚拟环境":
                                configs[device_id]["python"]["virtual_env"] = value
                            elif config_item == "Conda环境":
                                configs[device_id]["python"]["conda_env"] = value
                
                elif current_section == "core_libs" or current_section == "ai_libs":
                    # 库版本表格
                    for j, device_name in enumerate(device_names):
                        if j + 1 < len(cells):
                            value = cells[j + 1]
                            device_id = device_id_map.get(device_name)
                            
                            if not device_id or device_id not in configs:
                                continue
                            
                            if value in ["N/A", "Not found", "Unknown", "Not installed", ""]:
                                continue
                            
                            # 库名称就是config_item
                            configs[device_id]["libraries"][config_item] = value
                
                elif current_section == "project":
                    # 项目信息表格
                    for j, device_name in enumerate(device_names):
                        if j + 1 < len(cells):
                            value = cells[j + 1]
                            device_id = device_id_map.get(device_name)
                            
                            if not device_id or device_id not in configs:
                                continue
                            
                            if value in ["N/A", "Not found", "Unknown", "Not installed", ""]:
                                continue
                            
                            if config_item == "项目路径":
                                configs[device_id]["project"]["project_path"] = value
                            elif config_item == "requirements.txt":
                                configs[device_id]["project"]["config_files"]["requirements.txt"] = {
                                    "exists": value != "Not found",
                                    "status": value
                                }
                
                elif current_section == "collection_time":
                    # 信息收集时间表格
                    # 表格格式：| 主机 | 收集时间 |
                    if config_item in device_names:
                        device_id = device_id_map.get(config_item)
                        if device_id and device_id in configs and len(cells) >= 2:
                            collection_time = cells[1]
                            if collection_time not in ["N/A", ""]:
                                configs[device_id]["collection_time"] = collection_time
                
                elif current_section == "update_history":
                    # 更新历史表格
                    # 表格格式：| 时间 | 主机 | 变化摘要 |
                    if len(cells) >= 3:
                        time_str, host_name, summary = tuple(cells)
                        
                        # 查找对应的设备ID
                        device_id = device_id_map.get(host_name)
                        if device_id:
                            # 创建更新记录
                            update_record = {
                                "timestamp": time_str,
                                "device_id": device_id,
                                "device_name": host_name,
                                "has_changes": summary != "无变化" and summary != "****",
                                "summary": summary.replace("**", "") if "**" in summary else summary
                            }
                            
                            # 添加到更新记录列表
                            update_records[device_id].append(update_record)
            
            # 第四步：检查并清理配置
            for device_id, config in configs.items():
                device_name = config["system"]["device_name"]
                
                # 清理库信息：移除值为N/A的库
                libraries = config["libraries"]
                libraries_to_remove = []
                for lib_name, lib_value in libraries.items():
                    if lib_value in ["N/A", "Not found", "Unknown", "Not installed", ""]:
                        libraries_to_remove.append(lib_name)
                
                for lib_name in libraries_to_remove:
                    del libraries[lib_name]
                
                # 检查配置是否为空
                if not libraries and config["python"]["python_version"] == "N/A":
                    log.warning(f"设备 {device_name} 的配置信息为空")
            
            log.info(f"从Markdown表格解析了 {len(configs)} 个主机的配置")
            
            # 打印解析结果摘要
            for device_id, config in configs.items():
                device_name = config["system"]["device_name"]
                python_version = config["python"].get("python_version", "N/A")
                lib_count = len(config["libraries"])
                collection_time = config.get("collection_time", "N/A")
                update_count = len(update_records.get(device_id, []))
                
                log.info(f"设备 {device_name}: Python={python_version}, 库数量={lib_count}, 收集时间={collection_time}, 更新记录数={update_count}")
            
            return configs, update_records
            
        except Exception as e:
            log.error(f"解析Markdown表格失败: {e}")
            import traceback
            log.error(traceback.format_exc())
            return {}, {}

# %% [markdown]
# #### load_configs_from_joplin_note(self) -> Tuple[Dict[str, HostConfigCollector], Dict[str, List[Dict[str, Any]]]]

    # %%
    @timethis
    def load_configs_from_joplin_note(self) -> Tuple[Dict[str, HostConfigCollector], Dict[str, List[Dict[str, Any]]]]:
        """从Joplin笔记中读取所有主机的配置和更新记录
    
        Returns:
            Tuple[config_collectors, update_records]:
                config_collectors: 设备ID -> HostConfigCollector 对象的字典
                update_records: 设备ID -> 更新记录列表的字典
        """
        try:
            # 查找配置笔记
            note_title = "主机配置对比表"
            existing_notes = searchnotes(note_title)
    
            if not existing_notes or len(existing_notes) == 0:
                log.info("未找到主机配置对比笔记")
                return {}, {}
    
            note = existing_notes[0]
            note_content = note.body
    
            # 使用解析方法获取配置字典
            configs_dict, update_records = self.parse_config_from_markdown_table(note_content)
            print("调试线…………调试线")
            print(configs_dict)
            print("调试线…………调试线")
    
            # 将配置字典转换为 HostConfigCollector 对象
            config_collectors = {}
            for device_id, config_data in configs_dict.items():
                # 创建 HostConfigCollector 对象
                collector = HostConfigCollector()
    
                # 设置设备ID（从配置数据中获取）
                if "device_id" in config_data.get("system", {}):
                    collector.device_id = config_data["system"]["device_id"]
    
                # 设置设备名称
                if "device_name" in config_data.get("system", {}):
                    collector.device_name = config_data["system"]["device_name"]
    
                # 设置主机用户
                if "host_user" in config_data.get("system", {}):
                    collector.host_user = config_data["system"]["host_user"]
    
                # 保存配置数据
                collector.config_data = config_data
    
                # 设置本地配置文件路径
                collector.local_config_file = self.config_dir / f"{collector.device_id}.json"
    
                config_collectors[device_id] = collector
    
            log.info(f"从Joplin笔记中成功解析 {len(config_collectors)} 个主机的配置")
            return config_collectors, update_records
    
        except Exception as e:
            log.error(f"从Joplin笔记读取配置失败: {e}")
            import traceback
    
            log.error(traceback.format_exc())
            return {}, {}

# %% [markdown]
# #### save_configs_to_local_smart(self, configs: Dict[str, Any]) -> None

    # %%
    def save_configs_to_local_smart(self, configs: Dict[str, Any]) -> None:
        """智能保存从笔记加载的配置到本地（增强版）"""
        saved_count = 0
        updated_count = 0
        skipped_count = 0
    
        for device_id, config in configs.items():
            # 跳过当前主机的配置（已经保存过了）
            if device_id == self.device_id:
                skipped_count += 1
                continue
    
            # 构建本地配置文件路径
            config_file = self.config_dir / f"{device_id}.json"
    
            # 检查配置的完整性
            if not self._is_config_complete(config):
                log.warning(f"配置不完整，跳过保存: {device_id}")
                skipped_count += 1
                continue
    
            # 决定是否保存
            should_save = False
            save_reason = ""
    
            if not config_file.exists():
                should_save = True
                save_reason = "文件不存在"
            else:
                try:
                    # 读取现有配置进行比较
                    with open(config_file, "r", encoding="utf-8") as f:
                        existing_config = json.load(f)
    
                    # 比较关键字段
                    existing_time = existing_config.get("collection_time", "")
                    new_time = config.get("collection_time", "")
    
                    # 如果收集时间不同，或者配置内容有显著差异
                    if existing_time != new_time:
                        should_save = True
                        save_reason = f"时间不同 ({existing_time} -> {new_time})"
                    else:
                        # 比较其他关键字段
                        existing_name = existing_config.get("system", {}).get("device_name", "")
                        new_name = config.get("system", {}).get("device_name", "")
    
                        if existing_name != new_name:
                            should_save = True
                            save_reason = f"设备名称不同 ({existing_name} -> {new_name})"
                except Exception as e:
                    should_save = True
                    save_reason = f"读取失败: {e}"
    
            # 保存配置
            if should_save:
                try:
                    # 确保配置目录存在
                    self.config_dir.mkdir(parents=True, exist_ok=True)
    
                    # 保存配置
                    with open(config_file, "w", encoding="utf-8") as f:
                        json.dump(config, f, indent=2, ensure_ascii=False)
    
                    if config_file.exists():
                        saved_count += 1
                        log.info(f"保存配置: {device_id} ({save_reason}) -> {config_file}")
                    else:
                        log.error(f"保存失败: {device_id}")
                except Exception as e:
                    log.error(f"保存配置失败: {device_id}, {e}")
            else:
                updated_count += 1
                log.debug(f"跳过保存（无变化）: {device_id}")
    
        # 统计报告
        log.info(f"配置保存统计: 保存{saved_count}个, 跳过{skipped_count}个, 无变化{updated_count}个")
    
        # 清理过时的配置文件
        self._cleanup_old_configs(configs)

# %% [markdown]
# #### fix_config_with_local_data(self, parsed_configs: Dict[str, Any]) -> Dict[str, Any]

    # %%
    def fix_config_with_local_data(self, parsed_configs: Dict[str, Any]) -> Dict[str, Any]:
        """用本地配置修复解析的配置"""
        fixed_configs = parsed_configs.copy()

        # 加载所有本地配置
        local_configs = self.load_all_configs()

        for device_id, config in fixed_configs.items():
            device_name = config["system"]["device_name"]

            # 检查是否需要修复
            needs_fix = False

            # 检查system字段
            if config["system"].get("host_user") == "N/A":
                needs_fix = True
            if config["system"]["system"].get("platform") == "N/A":
                needs_fix = True

            # 检查python字段
            if config["python"].get("python_version") == "N/A":
                needs_fix = True

            # 检查libraries是否为空
            if not config["libraries"]:
                needs_fix = True

            # 如果需要修复且有本地配置
            if needs_fix and device_id in local_configs:
                local_config = local_configs[device_id]
                log.info(f"修复设备 {device_name} 的配置")

                # 修复system字段
                if config["system"].get("host_user") == "N/A" and "host_user" in local_config["system"]:
                    config["system"]["host_user"] = local_config["system"]["host_user"]

                # 修复system.system字段
                for key in ["platform", "system", "release", "version", "machine", "processor"]:
                    if config["system"]["system"].get(key) == "N/A" and key in local_config["system"]["system"]:
                        config["system"]["system"][key] = local_config["system"]["system"][key]

                # 修复python字段
                for key in ["python_version", "conda_version", "pip_version", "virtual_env", "conda_env"]:
                    if config["python"].get(key) == "N/A" and key in local_config["python"]:
                        config["python"][key] = local_config["python"][key]

                # 修复libraries
                if not config["libraries"] and local_config["libraries"]:
                    config["libraries"] = local_config["libraries"].copy()

                # 修复project
                if config["project"].get("project_path") == "N/A" and "project_path" in local_config["project"]:
                    config["project"]["project_path"] = local_config["project"]["project_path"]

        return fixed_configs

# %% [markdown]
# #### merge_all_configs(self) -> Dict[str, Any]

    # %%
    @timethis
    def merge_all_configs(self) -> Dict[str, Any]:
        """合并本地配置和Joplin笔记中的配置"""
        # 加载本地所有配置
        local_configs = self.load_all_configs()
        
        # 从Joplin笔记中读取配置
        joplin_configs = self.load_configs_from_joplin_note()
        
        # 关键新增：将从笔记加载的配置保存到本地
        if joplin_configs:
            self.save_configs_to_local_smart(joplin_configs)
        
        # 重新加载本地配置（可能已经更新）
        local_configs = self.load_all_configs()
        
        # 合并配置（本地配置优先）
        merged_configs = {}
        
        # 首先添加所有本地配置
        for device_id, config in local_configs.items():
            merged_configs[device_id] = config

        # 然后添加本地配置（如果不存在于Joplin配置中）
        for device_id, config in local_configs.items():
            if device_id not in merged_configs:
                merged_configs[device_id] = config
            else:
                # 如果已经存在，检查收集时间，保留最新的
                joplin_time = merged_configs[device_id].get("collection_time", "")
                local_time = config.get("collection_time", "")
                
                # 如果本地配置的收集时间更新，则更新合并后的配置
                if local_time > joplin_time:
                    merged_configs[device_id] = config
        
        # 保存其他主机的配置到本地（用于下次比较）
        self.save_configs_to_local_smart(merged_configs)
        
        log.info(f"合并后共有 {len(merged_configs)} 个主机的配置")
        return merged_configs

# %% [markdown]
# #### generate_markdown_table(self, configs: Dict[str, Any]) -> str

    # %%
    @timethis
    def generate_markdown_table(self, config_collectors: Dict[str, HostConfigCollector]) -> str:
        """生成Markdown对比表格"""
        if not config_collectors:
            return "# 主机配置对比表\n\n暂无配置信息\n"
        
        # 获取所有设备ID并按设备名称排序
        device_ids = sorted(
            config_collectors.keys(),
            key=lambda x: config_collectors[x].device_name
        )
        
        md_lines = ["# 主机配置对比表\n"]
        
        # 1. 系统信息
        md_lines.append("\n## 1. 系统信息\n")
        md_lines.append("| 配置项 | " + " | ".join([config_collectors[did].device_name for did in device_ids]) + " |")
        md_lines.append("|:---|" + "|".join([":---:" for _ in device_ids]) + "|")
        
        system_items = ["系统", "发行版", "内核版本", "架构", "主机用户"]
        for item in system_items:
            row = [f"**{item}**"]
            for did in device_ids:
                collector = config_collectors[did]
                config_data = collector.get_config_data()
                system_info = config_data.get("system", {}).get("system", {})
                
                if item == "系统":
                    value = system_info.get("system", "N/A")
                elif item == "发行版":
                    value = system_info.get("distro", "N/A")
                elif item == "内核版本":
                    value = system_info.get("kernel", "N/A")
                elif item == "架构":
                    value = system_info.get("architecture", "N/A")
                elif item == "主机用户":
                    value = config_data.get("system", {}).get("host_user", "N/A")
                else:
                    value = "N/A"
                
                row.append(str(value))
            
            md_lines.append("| " + " | ".join(row) + " |")
        
        # 2. Python环境信息
        md_lines.append("\n## 2. Python环境\n")
        md_lines.append("| 配置项 | " + " | ".join([config_collectors[did].device_name for did in device_ids]) + " |")
        md_lines.append("|:---|" + "|".join([":---:" for _ in device_ids]) + "|")
        
        python_items = ["Python版本", "Conda版本", "Pip版本", "虚拟环境", "Conda环境"]
        for item in python_items:
            row = [f"**{item}**"]
            for did in device_ids:
                collector = config_collectors[did]
                config_data = collector.get_config_data()
                python_info = config_data.get("python", {})
                
                if item == "Python版本":
                    value = python_info.get("python_version", "N/A")
                elif item == "Conda版本":
                    value = python_info.get("conda_version", "N/A")
                elif item == "Pip版本":
                    value = python_info.get("pip_version", "N/A")
                    if isinstance(value, list):
                        value = value[1] if len(value) > 1 else "N/A"
                elif item == "虚拟环境":
                    value = python_info.get("virtual_env", "N/A")
                elif item == "Conda环境":
                    value = python_info.get("conda_env", "N/A")
                else:
                    value = "N/A"
                
                row.append(str(value))
            
            md_lines.append("| " + " | ".join(row) + " |")
        
        # 3. 库信息（按类别分组）
        md_lines.append("\n## 3. 主要库版本\n")
        
        # 定义库类别
        lib_categories = {
            "基础库": ["pandas", "numpy", "matplotlib", "seaborn", "scipy"],
            "Jupyter": ["jupyter", "jupyterlab", "notebook"],
            "AI/ML": ["scikit-learn", "torch", "tensorflow", "keras", "pytorch"],
            "NLP": ["transformers", "langchain", "nltk", "spacy"],
            "其他": ["geopandas", "plotly", "dash", "joplin", "pathmagic", "arrow"]
        }
        
        for category, libs in lib_categories.items():
            md_lines.append(f"\n### {category}\n")
            md_lines.append("| 库名 | " + " | ".join([config_collectors[did].device_name for did in device_ids]) + " |")
            md_lines.append("|:---|" + "|".join([":---:" for _ in device_ids]) + "|")
            
            for lib in libs:
                row = [f"**{lib}**"]
                for did in device_ids:
                    collector = config_collectors[did]
                    config_data = collector.get_config_data()
                    libraries = config_data.get("libraries", {})
                    value = libraries.get(lib, "Not installed")
                    row.append(str(value))
                
                md_lines.append("| " + " | ".join(row) + " |")
        
        # 4. 项目信息
        md_lines.append("\n## 4. 项目信息\n")
        md_lines.append("| 配置项 | " + " | ".join([config_collectors[did].device_name for did in device_ids]) + " |")
        md_lines.append("|:---|" + "|".join([":---:" for _ in device_ids]) + "|")
        
        project_items = ["项目路径", "配置文件数量"]
        for item in project_items:
            row = [f"**{item}**"]
            for did in device_ids:
                collector = config_collectors[did]
                config_data = collector.get_config_data()
                project_info = config_data.get("project", {})
                
                if item == "项目路径":
                    value = project_info.get("project_path", "N/A")
                elif item == "配置文件数量":
                    config_files = project_info.get("config_files", {})
                    value = len([k for k, v in config_files.items() if v != "Not found"])
                else:
                    value = "N/A"
                
                row.append(str(value))
            
            md_lines.append("| " + " | ".join(row) + " |")
        
        # 5. 信息收集时间
        md_lines.append("\n## 5. 信息收集时间\n")
        md_lines.append("| 主机 | 收集时间 |")
        md_lines.append("|:---|:---|")
        
        for did in device_ids:
            collector = config_collectors[did]
            config_data = collector.get_config_data()
            device_name = collector.device_name
            collection_time = config_data.get("collection_time", "N/A")
            
            # 格式化时间
            if collection_time != "N/A":
                try:
                    if "T" in collection_time:
                        dt = datetime.fromisoformat(collection_time.replace("Z", "+00:00"))
                        formatted_time = dt.strftime("%Y-%m-%d %H:%M:%S")
                    else:
                        formatted_time = collection_time
                except:
                    formatted_time = collection_time
            else:
                formatted_time = "N/A"
            
            md_lines.append(f"| {device_name} | {formatted_time} |")
        
        return "\n".join(md_lines) + "\n\n"


# %% [markdown]
# #### generate_update_history(self, all_records: Dict[str, Any]) -> str

    # %%
    def generate_update_history(self, all_records: Dict[str, Any]) -> str:
        """生成更新历史记录"""
        pass

# %% [markdown]
# #### update_joplin_note(self)

    # %%
    def update_joplin_note(self, current_config: Dict[str, Any], update_record: Dict[str, Any]) -> Tuple[bool, str]:
        """更新Joplin笔记"""
        pass


# %% [markdown]
# ### update_joplin_note(self, current_config: Dict[str, Any], update_record: Dict[str, Any]) -> Tuple[bool, str]

# %%
@timethis
def update_joplin_note(self, current_config: Dict[str, Any], update_record: Dict[str, Any]) -> Tuple[bool, str]:
    """更新Joplin笔记"""
    try:
        # 检查配置是否有变化
        if not update_record.get("has_changes", False):
            log.info(f"主机《{current_config['system']['device_name']}》的配置无变化")
            return True, "配置无变化，无需更新笔记"

        # 查找或创建笔记本
        notebook_title = "ewmobile"
        notebook_id = searchnotebook(notebook_title)
        if not notebook_id:
            notebook_id = jpapi.add_notebook(title=notebook_title)
            log.info(f"创建新笔记本: {notebook_title}")

        # 查找或创建笔记
        note_title = "主机配置对比表"
        existing_notes = searchnotes(note_title, parent_id=notebook_id)

        # 创建当前主机的 HostConfigCollector
        current_collector = HostConfigCollector()
        current_collector.config_data = current_config

        # 从Joplin笔记加载其他主机配置
        other_collectors, joplin_update_records = self.load_configs_from_joplin_note()
        print(other_collectors)

        # 合并所有配置收集器
        all_collectors = {}

        # 添加当前主机
        all_collectors[current_collector.device_id] = current_collector

        # 添加其他主机（不覆盖当前主机）
        for device_id, collector in other_collectors.items():
            if device_id != current_collector.device_id:
                all_collectors[device_id] = collector

        # 保存所有配置到本地
        saved_count = 0
        for device_id, collector in all_collectors.items():
            if collector.save_config_to_local():
                saved_count += 1

        log.info(f"共保存了 {saved_count} 个主机的配置到本地")

        # 合并更新记录
        all_update_records = self.load_all_update_records() or {}

        # 添加当前主机更新记录
        device_id = current_config["system"]["device_id"]
        if device_id not in all_update_records:
            all_update_records[device_id] = []

        # 检查是否已存在相同时间戳的记录
        current_timestamp = update_record.get("timestamp")
        if current_timestamp:
            existing_timestamps = [x.get("timestamp") for x in all_update_records[device_id]]
            if current_timestamp not in existing_timestamps:
                all_update_records[device_id].insert(0, update_record)
        else:
            all_update_records[device_id].insert(0, update_record)

        # 限制每个主机最多100条记录
        if len(all_update_records[device_id]) > 100:
            all_update_records[device_id] = all_update_records[device_id][:100]

        # 保存更新记录到本地
        self.save_update_records_to_local(all_update_records)

        # 生成markdown对比表格
        markdown_content = self.generate_markdown_table(all_collectors)

        # 添加更新历史
        update_history = self.generate_update_history(all_update_records)
        markdown_content += update_history

        # 更新笔记
        if existing_notes and len(existing_notes) > 0:
            note = existing_notes[0]

            # 更新笔记内容
            updatenote_body(note.id, markdown_content)

            # 更新标题显示更新时间
            new_title = f"{note_title} (更新于{datetime.now().strftime('%Y-%m-%d %H:%M')})"
            updatenote_title(note.id, new_title)

            log.info(f"更新笔记: {new_title}")
            return True, "笔记更新成功"
        else:
            # 创建新笔记
            note_id = createnote(note_title, markdown_content, parent_id=notebook_id)
            log.info(f"创建新笔记: {note_title}")
            return True, "笔记创建成功"

    except Exception as e:
        log.error(f"更新Joplin笔记失败: {e}")
        import traceback

        log.error(traceback.format_exc())
        return False, f"更新失败: {str(e)}"


JoplinConfigManager.update_joplin_note = update_joplin_note


# %% [markdown]
# ### generate_update_history(self, all_records: Dict[str, Any]) -> str

# %%
@timethis
def generate_update_history(self, all_records: Dict[str, Any]) -> str:
    """生成更新历史记录"""
    if not all_records:
        return "\n## 更新历史\n\n暂无更新记录"

    md_lines = ["\n## 更新历史\n"]
    md_lines.append("*按时间倒序排列，最近更新在前*\n")

    # 收集所有主机的更新记录
    all_updates = []
    for device_id, records in all_records.items():
        if isinstance(records, list):
            for record in records:
                if isinstance(record, dict):
                    # 确保每条记录都有设备ID
                    record_copy = record.copy()
                    record_copy["device_id"] = device_id
                    all_updates.append(record_copy)

    # 按时间排序（倒序）
    all_updates.sort(key=lambda x: x.get("timestamp", ""), reverse=True)

    # 只显示最近20条记录
    recent_updates = all_updates[:20]

    md_lines.append("| 时间 | 主机 | 变化摘要 |")
    md_lines.append("|:---|:---|:---|")

    for update in recent_updates:
        timestamp = update.get("timestamp", "")
        device_name = update.get("device_name", update.get("device_id", "Unknown"))
        summary = update.get("summary", "无变化")

        # 格式化时间
        try:
            if "T" in timestamp:
                dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                formatted_time = dt.strftime("%Y-%m-%d %H:%M")
            else:
                formatted_time = timestamp
        except:
            formatted_time = timestamp

        # 如果有变化，加粗显示
        if update.get("has_changes", False) and summary != "无变化":
            summary = f"**{summary}**"

        md_lines.append(f"| {formatted_time} | {device_name} | {summary} |")

    # 添加统计信息
    total_updates = len(all_updates)
    changes_count = sum(
        1 for u in all_updates if u.get("has_changes", False) and u.get("summary", "无变化") != "无变化"
    )

    md_lines.append(f"\n*统计：共 {total_updates} 次更新，其中 {changes_count} 次有配置变化*")

    return "\n".join(md_lines)


JoplinConfigManager.generate_update_history = generate_update_history


# %% [markdown]
# ### hostconfig2note()

# %%
@timethis
def hostconfig2note():
    """主函数：收集主机配置并更新到笔记"""
    try:
        # 1. 收集当前主机配置
        collector = HostConfigCollector()
        collector.show_config_summary()

        current_config = collector.collect_all_info()
        update_record = collector.save_config_with_change_detection(current_config)

        # 2. 更新Joplin笔记
        joplin_manager = JoplinConfigManager()
        result = joplin_manager.update_joplin_note(current_config, update_record)

        # 检查返回值是否为元组
        if isinstance(result, tuple) and len(result) == 2:
            success, message = result
        else:
            # 如果返回的不是元组，记录错误
            log.error(f"update_joplin_note 返回了无效的值: {result}")
            success, message = False, "更新函数返回无效值"

        if success:
            if update_record.get("has_changes", False):
                log.info(f"主机配置已更新到Joplin笔记，变化: {update_record.get('summary', '无变化')}")
            else:
                log.info("主机配置已更新到Joplin笔记（无变化）")
        else:
            log.error(f"主机配置更新到Joplin笔记失败: {message}")

        return success, update_record

    except Exception as e:
        log.error(f"hostconfig2note 执行失败: {e}")
        import traceback

        log.error(traceback.format_exc())
        return False, {"error": str(e)}


# %% [markdown]
# ## 主函数

# %%
if __name__ == "__main__":
    if not_IPython():
        log.info(f"开始运行文件\t{__file__}")

    success, update_record = hostconfig2note()

    if not_IPython():
        status = "成功" if success else "失败"
        changes = (
            f"，变化: {update_record.get('summary', '无变化')}"
            if update_record.get("has_changes", False)
            else "（无变化）"
        )
        log.info(f"文件执行{status}{changes}\t{__file__}")
