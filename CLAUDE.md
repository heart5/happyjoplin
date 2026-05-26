---
jupyter:
  jupytext:
    cell_metadata_filter: -all
    formats: ipynb,md
    main_language: python
    notebook_metadata_filter: jupytext,-kernelspec,-jupytext.text_representation.jupytext_version
    split_at_heading: true
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
---

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

HappyJoplin is a personal automation system using Joplin as its central hub. Scripts read from Joplin notes, process data, and write results back — using Joplin as both database and event source.

## Build / lint / format

```bash
ruff check .                    # lint all (.py and .ipynb)
ruff format .                   # format all
```

No test suite exists. Quick validation: `python fortest.py` or open `fortest.ipynb` in Jupyter Lab.

## Architecture

### Jupytext pairing

All `.py` files are paired with `.ipynb` via Jupytext percent format. `# %%` marks cell boundaries, `# %% [markdown]` marks markdown cells. The `.py` files are the git-tracked source of truth; `.ipynb` is generated. Config is in `pyproject.toml` `[tool.jupytext]` section.

### Path bootstrap pattern

Every entry-point script starts with:

```python
import pathmagic
with pathmagic.context():
    import func.first
```

`pathmagic.context()` injects `.` and `..` into `sys.path`. Each subdirectory (func, etc, work, life, muse) has its own `pathmagic.py` — this is intentional: at import time `func/` is not yet on `sys.path`, so the local copy bootstraps the path. Keep all copies in sync with `func/pathmagic.py` (the canonical version).

`func.first.getdirmain()` locates the project root by walking upward until it finds the `rootfile` sentinel (empty file in project root). `first` then adds `etc/`, `func/`, `work/`, `life/` to `sys.path` and sets up matplotlib Chinese font support.

The root-level `pathmagic.py` is an older version kept for reference; it is not used by any script.

### Core modules (func/ — git submodule from github.com/heart5/func)

| Module | Role |
|---|---|
| `jpfuncs.py` | Joplin API via `joppy`. `getapi()` tries remote server config first (`data/joplinai.ini`), falls back to local `joplin config` CLI. Note CRUD, resources, tags, and `readinifromcloud()` for syncing config from Joplin notes. Uses lazy loading: `jpapi` is a `_LazyJoplinAPI` proxy, and `log` is a `_LazyLogger` proxy. |
| `configpr.py` | `.ini` file wrapper. `getcfp(name)` → `ConfigParser` for `data/<name>.ini`, auto-creates if missing. Type-aware get/set. |
| `logme.py` | `_LazyLogger` proxy — defers `RotatingFileHandler` creation until first log call. Logger name "hjer", 1 MB × 23 files. |
| `getid.py` | Device identity: `getdeviceid()`, `getdevicename()`, `gethostuser()`. Uses UUID fallback (not magic numbers). All scripts import from `func.getid`. |
| `datatools.py` | File I/O, hashing, SQLite vacuum, cloud key retrieval (`getkeysfromcloud()`). |
| `litetools.py` | SQLite schema inspection and webchat data cleaning. |
| `pdtools.py` | Pandas visualization: DataFrames to PNG, business charts (monthly/year-over-year comparisons). |
| `nettools.py` | Retry decorator with exponential backoff and SMS notification via Termux, IFTTT webhook integration, itchat management. |
| `wrapfuncs.py` | Decorators: `@logit`, `@timethis`, `@ift2phone`. |
| `sysfunc.py` | Signal-based timeout decorator, shell execution, traceback extraction. |
| `filedatafunc.py` | Alipay XLS parsing, incremental file processing, Google Sheets integration. |
| `termuxtools.py` | Termux Android API: SMS (`termux_sms_send` — reads phone number from `happyjphard.ini`), GPS, device info. |

### Key subsystems

- **Monitor (`work/monitor4.py`)**: Tracks 4 daily-update notes. Checks word count changes, content freshness, writes summary back to Joplin.
- **Monitor heatmap (`work/monitor2map.py`)**: Generates heatmap visualizations from monitor data, uploads to Joplin. Uses `retry_jp()` with exponential backoff for Joplin API resilience.
- **WeChat bridge (`life/webchat.py`)**: `itchat`-based WeChat client. Archives messages to txt, syncs periodic summaries to Joplin, responds to custom commands.
- **Health dashboard (`life/health.py`)**: Parses structured Joplin notes for steps/sleep/beer/notes, generates multi-panel matplotlib charts, writes analysis back.
- **Location tracking (`life/footstrack.py`, `life/footsshow.py`)**: GPS data collection (Termux) and multi-level report generation (weekly through yearly). footsshow generates 6 chart types per report at 8×8"/150DPI with `_safe_add_resource()` retry uploads. Large binary downloads use direct HTTP fallback to avoid charset_normalizer bugs in joppy.
- **Cloud config sync**: `jpfuncs.readinifromcloud()` pulls config from a Joplin note named "happyjoplin云端配置" into local `.ini` files. All devices stay in sync this way.
- **QA client (`joplin_qa_client.py`)**: GenAI-powered Q&A against a Joplin knowledge base server.

### Data flow convention

Scripts store their state in `data/` (`.ini`, `.json`, `.db`), logs go to `log/`, images to `img/`. This project is designed to run on Termux (Android), so `func/termuxtools.py` provides SMS and GPS access.

## Documentation

- `README.md` — project overview and quick start
- `docs/CHANGELOG.md` — full change history, update after each work session
- `docs/TECHNICAL_MANUAL.md` — architecture diagrams, data flows, config files, maintenance operations
- `CLAUDE.md` — this file, AI assistant guidance

When making significant changes, update `docs/CHANGELOG.md` with the date and changes. When architecture/config changes, update `docs/TECHNICAL_MANUAL.md` accordingly.

### Documentation standards

Docs/analysis reports stored in `docs/` should be:

- **Jupytext-paired**: Use `ipynb,md` format so `.md` is the tracked source and `.ipynb` is generated for JupyterLab viewing. Include standard jupytext frontmatter.
- **Rich with diagrams**: Prefer mermaid syntax (flowcharts, gitGraph, sequence diagrams) for charts. Use ASCII art as fallback when mermaid can't express the content. Avoid emojis, `Note` reserved word, and `=` in mermaid labels.
- **Sync before commit**: Run `jupytext --sync <file>.md` to generate/update the `.ipynb` before committing.
- **Update log dates unique**: In technical docs (e.g. MONITOR_SYSTEM.md), the "更新维护记录" section at the bottom must not repeat dates — group all same-day entries under one date heading. This does NOT apply to the project-level CHANGELOG.md.

## Commits

Commits are in Chinese. Follow the existing style: short, descriptive, feature-focused.
