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
        self.required_libs = self._get_libs_from_cloud("required_libs")
        self.optional_libs = self._get_libs_from_cloud("optional_libs")
        self.ai_libs = self._get_libs_from_cloud("ai_libs")
        
        # 打印调试信息
        log.info(f"从云端配置获取的库列表:")
        log.info(f"  required_libs: {len(self.required_libs)} 个库")
        log.info(f"  optional_libs: {len(self.optional_libs)} 个库")
        log.info(f"  ai_libs: {len(self.ai_libs)} 个库")
        
        # 本地配置文件路径
        self.local_config_file = self.config_dir / f"{self.device_id}.json"
        
        # 更新记录文件
        self.update_record_file = self.config_dir / f"{self.device_id}_updates.json"

    # %%
    def _get_libs_from_cloud(self, config_key: str) -> List[str]:
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
# #### _load_previous_config(self) -> Dict[str, Any]

    # %%
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
    def save_config_with_change_detection(self, new_config: Dict[str, Any]) -> Dict[str, Any]:
        """保存配置并检测变化"""
        # 加载上一次配置
        old_config = self._load_previous_config()

        # 保存更新记录
        update_record = self._save_update_record(old_config, new_config)

        # 只有有变化时才保存新配置
        if update_record.get("has_changes", False):
            try:
                with open(self.local_config_file, "w", encoding="utf-8") as f:
                    json.dump(new_config, f, indent=2, ensure_ascii=False)
                log.info(f"配置信息已保存到: {self.local_config_file}")
            except Exception as e:
                log.error(f"保存配置失败: {e}")
        else:
            log.info("配置无变化，跳过保存配置文件")

        return update_record

# %% [markdown]
# #### save_configs_to_local(self, configs: Dict[str, Any]) -> None

    # %%
    def save_configs_to_local(self, configs: Dict[str, Any]) -> None:
        """将从笔记加载的配置保存到本地"""
        saved_count = 0
        
        for device_id, config in configs.items():
            # 跳过当前主机的配置（已经保存过了）
            if device_id == self.device_id:
                continue
            
            # 构建本地配置文件路径
            config_file = self.config_dir / f"{device_id}.json"
            
            # 检查是否需要保存（文件不存在或内容不同）
            should_save = False
            
            if not config_file.exists():
                should_save = True
                log.info(f"本地配置文件不存在，将保存: {device_id}")
            else:
                try:
                    # 读取现有配置进行比较
                    with open(config_file, "r", encoding="utf-8") as f:
                        existing_config = json.load(f)
                    
                    # 简单比较：检查收集时间是否不同
                    existing_time = existing_config.get("collection_time", "")
                    new_time = config.get("collection_time", "")
                    
                    if existing_time != new_time:
                        should_save = True
                        log.info(f"配置时间不同，将更新: {device_id} ({existing_time} -> {new_time})")
                except Exception as e:
                    should_save = True
                    log.info(f"读取现有配置失败，将覆盖: {device_id}, {e}")
            
            # 保存配置
            if should_save:
                try:
                    with open(config_file, "w", encoding="utf-8") as f:
                        json.dump(config, f, indent=2, ensure_ascii=False)
                    saved_count += 1
                    log.info(f"保存配置到本地: {device_id} -> {config_file}")
                except Exception as e:
                    log.error(f"保存配置失败: {device_id}, {e}")
        
        if saved_count > 0:
            log.info(f"共保存了 {saved_count} 个其他主机的配置到本地")
        else:
            log.info("没有需要保存的其他主机配置")

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
# #### parse_config_from_markdown_table(self, markdown_content: str) -> Dict[str, Any]

    # %%
    @timethis
    def parse_config_from_markdown_table(self, markdown_content: str) -> Dict[str, Any]:
        """从Markdown表格中解析配置信息"""
        configs = {}
        
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
                return {}
            
            # 第二步：为每个设备查找对应的device_id
            for device_name in device_names:
                device_id = findvaluebykeyinsection("happyjpinifromcloud", "device", device_name)
                if not device_id or device_id == "None":
                    # 如果找不到，使用哈希作为后备
                    import hashlib
                    device_id = f"joplin_{hashlib.md5(device_name.encode()).hexdigest()[:8]}"
                    log.warning(f"无法找到设备 {device_name} 的ID，使用哈希ID: {device_id}")
                
                device_id_map[device_name] = device_id
                log.info(f"设备映射: {device_name} -> {device_id}")
                
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
                            "architecture": ["N/A"]
                        }
                    },
                    "python": {
                        "python_version": "N/A",
                        "conda_version": "N/A",
                        "pip_version": "N/A",
                        "virtual_env": "N/A",
                        "conda_env": "N/A"
                    },
                    "libraries": {},
                    "project": {
                        "project_path": "N/A",
                        "config_files": {}
                    },
                    "collection_time": datetime.now().isoformat()
                }
            
            # 第三步：解析各个部分的配置
            section_data = {}
            current_section = None
            table_started = False
            
            for i, line in enumerate(lines):
                line = line.strip()
                
                # 检测章节标题
                if line.startswith('## '):
                    current_section = line[3:].strip()
                    table_started = False
                    log.debug(f"检测到章节: {current_section}")
                    continue
                
                # 检测表格开始（表头行）
                if current_section and line.startswith('|') and '|' in line:
                    if not table_started:
                        # 检查是否是表头行（包含设备名称）
                        if any(device_name in line for device_name in device_names):
                            table_started = True
                            log.debug(f"开始解析 {current_section} 表格")
                        continue
                    
                    # 跳过表头分隔行
                    if line.startswith('|:---') or line.startswith('|---'):
                        continue
                    
                    # 解析数据行
                    # 关键修复：正确处理单元格分割
                    cells = [cell.strip() for cell in line.strip('|').split('|')]
                    if len(cells) < 2:
                        continue
                    
                    # 第一列是配置项名称 - 确保是字符串
                    config_item = cells[0] if cells[0] else ""
                    
                    # 跳过空配置项
                    if not config_item or config_item == "配置项":
                        continue
                    
                    # 根据当前章节处理数据
                    for j, device_name in enumerate(device_names):
                        if j + 1 < len(cells):
                            value = cells[j + 1]
                            device_id = device_id_map.get(device_name)
                            
                            if not device_id or device_id not in configs:
                                continue
                            
                            # 跳过无效值
                            if value in ["N/A", "Not found", "Unknown", "Not installed", ""]:
                                continue
                            
                            # 根据章节填充数据
                            if current_section == "1. 系统信息":
                                if config_item == "操作系统":
                                    configs[device_id]["system"]["system"]["platform"] = value
                                elif config_item == "内核版本":
                                    configs[device_id]["system"]["system"]["release"] = value
                                elif config_item == "架构":
                                    # 解析架构信息，如 "x86_64 (64bit)"
                                    if "(" in value:
                                        machine = value.split("(")[0].strip()
                                        arch = value.split("(")[1].replace(")", "").strip()
                                        configs[device_id]["system"]["system"]["machine"] = machine
                                        configs[device_id]["system"]["system"]["architecture"] = [arch]
                                    else:
                                        configs[device_id]["system"]["system"]["machine"] = value
                                elif config_item == "主机用户":
                                    configs[device_id]["system"]["host_user"] = value
                            
                            elif current_section == "2. Python环境":
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
                            
                            elif current_section == "3. 核心库版本":
                                # 只处理有效的库版本
                                if value and value != "N/A":
                                    # 关键修复：确保 config_item 是字符串
                                    if isinstance(config_item, str):
                                        configs[device_id]["libraries"][config_item] = value
                                    else:
                                        log.warning(f"配置项不是字符串: {type(config_item)} - {config_item}")
                            
                            elif current_section == "4. AI/ML相关库":
                                # 只处理有效的库版本
                                if value and value != "N/A":
                                    # 关键修复：确保 config_item 是字符串
                                    if isinstance(config_item, str):
                                        configs[device_id]["libraries"][config_item] = value
                                    else:
                                        log.warning(f"配置项不是字符串: {type(config_item)} - {config_item}")
                            
                            elif current_section == "5. 项目信息":
                                if config_item == "项目路径":
                                    configs[device_id]["project"]["project_path"] = value
                                elif config_item == "requirements.txt" and value != "Not found":
                                    configs[device_id]["project"]["config_files"]["requirements.txt"] = {
                                        "exists": True,
                                        "status": value
                                    }
            
            # 第四步：检查并清理配置
            for device_id, config in configs.items():
                device_name = config["system"]["device_name"]
                
                # 清理库信息：移除值为N/A的库
                libraries = config["libraries"]
                libraries_to_remove = []
                for lib_name, lib_value in libraries.items():
                    if lib_value in ["N/A", "Not installed", "Unknown"]:
                        libraries_to_remove.append(lib_name)
                
                for lib_name in libraries_to_remove:
                    del libraries[lib_name]
                
                # 检查是否有有效数据
                has_system_info = any(
                    value != "N/A" for value in config["system"]["system"].values()
                )
                has_python_info = any(
                    value != "N/A" for value in config["python"].values()
                )
                
                if not has_system_info and not has_python_info and not libraries:
                    log.warning(f"设备 {device_name} 的配置信息为空")
            
            log.info(f"从Markdown表格解析了 {len(configs)} 个主机的配置")
            
            # 打印解析结果摘要
            for device_id, config in configs.items():
                device_name = config["system"]["device_name"]
                python_version = config["python"].get("python_version", "N/A")
                lib_count = len(config["libraries"])
                log.info(f"设备 {device_name}: Python={python_version}, 库数量={lib_count}")
            
            return configs
            
        except Exception as e:
            log.error(f"解析Markdown表格失败: {e}")
            import traceback
            log.error(traceback.format_exc())
            return {}

# %% [markdown]
# #### load_configs_from_joplin_note(self) -> Dict[str, Any]

    # %%
    @timethis
    def load_configs_from_joplin_note(self) -> Dict[str, Any]:
        """从Joplin笔记中读取所有主机的配置"""
        try:
            # 查找配置笔记
            note_title = "主机配置对比表"
            existing_notes = searchnotes(note_title)
            
            if not existing_notes or len(existing_notes) == 0:
                log.info("未找到主机配置对比笔记")
                return {}
            
            note = existing_notes[0]
            note_content = note.body
            
            # 使用解析方法
            configs = self.parse_config_from_markdown_table(note_content)
            
            if configs:
                # 检查是否有N/A值需要修复
                needs_fix = False
                for device_id, config in configs.items():
                    if config["system"].get("host_user") == "N/A" or config["python"].get("python_version") == "N/A":
                        needs_fix = True
                        break
                
                if needs_fix:
                    log.info("检测到N/A值，使用本地配置进行修复")
                    configs = self.fix_config_with_local_data(configs)
                
                log.info(f"从Joplin笔记中成功解析 {len(configs)} 个主机的配置")
            else:
                log.warning("无法从Joplin笔记中解析配置信息")
            
            return configs
            
        except Exception as e:
            log.error(f"从Joplin笔记读取配置失败: {e}")
            return {}

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
        self.save_configs_to_local(merged_configs)
        
        log.info(f"合并后共有 {len(merged_configs)} 个主机的配置")
        return merged_configs

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
# #### sync_update_records(self) -> None

    # %%
    def sync_update_records(self) -> None:
        """同步所有主机的更新记录"""
        # 从Joplin笔记加载所有配置
        joplin_configs = self.load_configs_from_joplin_note()
    
        # 为每个主机加载更新记录
        all_update_records = {}
    
        for device_id in joplin_configs.keys():
            # 构建更新记录文件路径
            update_file = self.config_dir / f"{device_id}_updates.json"
    
            if update_file.exists():
                try:
                    with open(update_file, "r", encoding="utf-8") as f:
                        records = json.load(f)
                        all_update_records[device_id] = records
                except Exception as e:
                    log.error(f"加载更新记录文件 {update_file} 失败: {e}")
    
        # 保存合并后的更新记录到本地
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
# #### generate_markdown_table(self, configs: Dict[str, Any]) -> str

    # %%
    def generate_markdown_table(self, configs: Dict[str, Any]) -> str:
        """生成Markdown对比表格"""
        if not configs:
            return "# 主机配置对比\n\n暂无配置数据"
        
        # 按设备名称排序
        sorted_configs = sorted(
            configs.items(),
            key=lambda x: x[1].get("system", {}).get("device_name", "").lower()
        )
        
        device_ids = [item[0] for item in sorted_configs]
        # device_names = [getinivaluefromcloud("device", device_id) for device_id in device_ids]
        device_names = [configs[device_id]["system"]["device_name"] for device_id in device_ids]
        
        md_lines = ["# 主机配置对比\n"]
        md_lines.append(f"*更新时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n")
        
        # 1. 系统信息对比表
        md_lines.append("## 1. 系统信息\n")
        md_lines.append("| 配置项 | " + " | ".join(device_names) + " |")
        md_lines.append("|:---|" + "|".join([":---:" for _ in device_names]) + "|")
        
        system_items = [
            ("操作系统", lambda c: c["system"]["system"].get("distro", c["system"]["system"]["platform"])),
            ("内核版本", lambda c: c["system"]["system"].get("kernel", c["system"]["system"]["release"])),
            ("架构", lambda c: f"{c['system']['system']['machine']} ({c['system']['system']['architecture'][0]})"),
            ("主机用户", lambda c: c["system"]["host_user"]),
        ]
        
        for item_name, extractor in system_items:
            row = [item_name]
            for did in device_ids:
                try:
                    value = extractor(configs[did])
                    row.append(str(value))
                except:
                    row.append("N/A")
            md_lines.append("| " + " | ".join(row) + " |")
        
        # 2. Python环境对比表
        md_lines.append("\n## 2. Python环境\n")
        md_lines.append("| 配置项 | " + " | ".join(device_names) + " |")
        md_lines.append("|:---|" + "|".join([":---:" for _ in device_names]) + "|")
        
        python_items = [
            ("Python版本", lambda c: c["python"]["python_version"]),
            ("Conda版本", lambda c: c["python"].get("conda_version", "N/A")),
            ("Pip版本", lambda c: c["python"].get("pip_version", "N/A")),
            ("虚拟环境", lambda c: c["python"]["virtual_env"]),
            ("Conda环境", lambda c: c["python"]["conda_env"]),
        ]
        
        for item_name, extractor in python_items:
            row = [item_name]
            for did in device_ids:
                try:
                    value = extractor(configs[did])
                    row.append(str(value))
                except:
                    row.append("N/A")
            md_lines.append("| " + " | ".join(row) + " |")
        
        # 3. 核心库版本对比表（使用云端配置的核心库列表）
        md_lines.append("\n## 3. 核心库版本\n")
        
        # 使用云端配置的核心库列表（前12个）
        core_libs = self.required_libs[:12] if len(self.required_libs) >= 12 else self.required_libs
        
        md_lines.append("| 库名称 | " + " | ".join(device_names) + " |")
        md_lines.append("|:---|" + "|".join([":---:" for _ in device_names]) + "|")
        
        for lib_name in core_libs:
            row = [lib_name]
            for did in device_ids:
                try:
                    # 修复这里：正确获取库版本
                    version = configs[did]["libraries"].get(lib_name, "N/A")
                    row.append(str(version))
                except Exception as e:
                    row.append("N/A")
            md_lines.append("| " + " | ".join(row) + " |")
        
        # 4. AI/ML相关库对比表
        md_lines.append("\n## 4. AI/ML相关库\n")
        
        # 使用云端配置的AI/ML库列表
        ai_libs = self.ai_libs if hasattr(self, 'ai_libs') else []
        
        if ai_libs:
            md_lines.append("| 库名称 | " + " | ".join(device_names) + " |")
            md_lines.append("|:---|" + "|".join([":---:" for _ in device_names]) + "|")
            
            for lib_name in ai_libs:
                row = [lib_name]
                for did in device_ids:
                    try:
                        version = configs[did]["libraries"].get(lib_name, "N/A")
                        row.append(str(version))
                    except:
                        row.append("N/A")
                md_lines.append("| " + " | ".join(row) + " |")
        
        # 5. 项目信息对比表
        md_lines.append("\n## 5. 项目信息\n")
        md_lines.append("| 配置项 | " + " | ".join(device_names) + " |")
        md_lines.append("|:---|" + "|".join([":---:" for _ in device_names]) + "|")
        
        project_items = [
            ("项目路径", lambda c: c["project"].get("project_path", "N/A")),
            ("requirements.txt", lambda c: c["project"].get("config_files", {}).get("requirements.txt", {}).get("status", "Not found")),
        ]
        
        for item_name, extractor in project_items:
            row = [item_name]
            for did in device_ids:
                try:
                    value = extractor(configs[did])
                    row.append(str(value))
                except:
                    row.append("N/A")
            md_lines.append("| " + " | ".join(row) + " |")

        # 6. 信息收集时间 - 修复：使用每个主机自己的收集时间
        md_lines.append("\n## 6. 信息收集时间\n")
        md_lines.append("| 主机 | 收集时间 |")
        md_lines.append("|:---|:---|")
        
        for did in device_ids:
            device_name = configs[did]["system"]["device_name"]
            # 关键修复：从配置中获取实际的收集时间
            collection_time = configs[did].get("collection_time", "N/A")
            
            # 格式化时间：将ISO格式转换为更易读的格式
            if collection_time != "N/A":
                try:
                    # 如果是ISO格式的时间字符串，转换为更友好的格式
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

        return "\n".join(md_lines)

# %% [markdown]
# #### generate_update_history(self, all_records: Dict[str, Any]) -> str

    # %%
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

# %% [markdown]
# #### update_joplin_note(self)

    # %%
    @timethis
    def update_joplin_note(self) -> Tuple[bool, Dict[str, Any]]:
        """更新Joplin笔记"""
        try:
            # 查找或创建笔记本
            notebook_title = "ewmobile"
            notebook_id = searchnotebook(notebook_title)
            if not notebook_id:
                notebook_id = jpapi.add_notebook(title=notebook_title)
                log.info(f"创建新笔记本: {notebook_title}")
    
            # 查找或创建笔记
            note_title = "主机配置对比表"
            existing_notes = searchnotes(note_title, parent_id=notebook_id)
    
            # 收集当前主机信息
            current_config = self.collect_all_info()
    
            # 保存配置并检测变化
            update_record = self.save_config_with_change_detection(current_config)

            # 只有有变化时才更新笔记
            if update_record.get("has_changes", False):
                # 关键修改：合并本地配置和joplin笔记中的配置
                # 这个函数现在会自动将从笔记加载的配置保存到本地
                all_configs = self.merge_all_configs()
            
                # 同步更新记录（新增）
                self.sync_update_records()
                
                # 加载所有更新记录
                all_update_records = self.load_all_update_records()
                
                # 生成markdown对比表格（包含所有主机）
                markdown_content = self.generate_markdown_table(all_configs)
                
                # 添加更新历史
                update_history = self.generate_update_history(all_update_records)
                markdown_content += update_history
                print("调试线…… ————……")
                
                # 更新笔记
                if existing_notes and len(existing_notes) > 0:
                    # 更新现有笔记（取第一个匹配的笔记）
                    note = existing_notes[0]
    
                    # 更新笔记内容
                    updatenote_body(note.id, markdown_content)
    
                    # 更新标题显示更新时间
                    new_title = f"{note_title} (更新于{datetime.now().strftime('%Y-%m-%d %H:%M')})"
                    updatenote_title(note.id, new_title)
    
                    log.info(f"更新笔记: {new_title}")
                    log.info(f"配置变化: {update_record.get('summary', '无变化')}")
                else:
                    # 创建新笔记
                    note_id = createnote(note_title, markdown_content, parent_id=notebook_id)
                    log.info(f"创建新笔记: {note_title}")
    
                return True, update_record
            else:
                # 无变化时，不更新笔记
                log.info("配置无变化，跳过笔记更新")
                return True, update_record
    
        except Exception as e:
            log.error(f"更新Joplin笔记失败: {e}")
            return False, {}

# %% [markdown]
# #### show_config_summary(self)

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


# %% [markdown]
# ### hostconfig2note()

# %%
@timethis
def hostconfig2note():
    """主函数：收集主机配置并更新到笔记"""
    collector = HostConfigCollector()

    # 显示当前主机配置摘要
    collector.show_config_summary()

    # 更新到Joplin笔记
    success, update_record = collector.update_joplin_note()

    if success:
        if update_record.get("has_changes", False):
            log.info(f"主机配置已更新到Joplin笔记，变化: {update_record.get('summary', '无变化')}")
        else:
            log.info("主机配置已更新到Joplin笔记（无变化）")
    else:
        log.error("主机配置更新到Joplin笔记失败")

    return success, update_record


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
