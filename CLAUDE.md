# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

probe-_doe is an OSINT (Open Source Intelligence) agentic framework that orchestrates data collection, processing, and analysis workflows. It implements a 3-layer architecture separating directives (instructions), orchestration (AI decision-making), and execution (deterministic Python scripts).

## Commands

```bash
# Run the news fetcher
python execution/fetch_news.py --output-dir .tmp

# Check today's execution logs
tail -f logs/exec_$(date +%Y%m%d).log

# Validate JSON output
python -m json.tool .tmp/news_vulkangruppe*.json
```

## Architecture

**3-Layer System:**
1. **Directives** (`directives/`) - Markdown SOPs defining goals, inputs, tools, outputs, edge cases
2. **Orchestration** (AI) - Read directives, call execution tools, handle errors, update directives with learnings
3. **Execution** (`execution/`) - Deterministic Python scripts for API calls, data processing, file operations

**Why:** LLMs are probabilistic; business logic needs consistency. 90% accuracy per step = 59% success over 5 steps. Push complexity into deterministic code.

## Directory Structure

- `directives/` - Operating procedures (SOPs) in Markdown
- `execution/` - Python scripts (deterministic tools)
- `logs/` - Daily execution logs (`exec_YYYYMMDD.log`)
- `reports/` - OSINT investigation outputs (`YYYYMMDD_HHMMSS_<target>/`)
- `.tmp/` - Intermediate files (git-ignored, always regenerated)
- `.env` - API keys and credentials

## Key Operating Principles

1. **Check `execution/` for existing tools before creating new scripts**
2. **Self-anneal on errors:** Fix script → Test → Update directive with learnings
3. **Log all tool executions** to `logs/exec_YYYYMMDD.log` with: timestamp, tool name, params, status, errors
4. **Deliverables in cloud** (Google Sheets/Slides), local files are temporary
5. **Directives are living documents** - update with API constraints, better approaches, edge cases

## Environment Variables

Required in `.env` (see `.env.example`):
- `NEWS_API_KEY` - EventRegistry API key for fetch_news.py
- `TELEGRAM_*` - Telegram API credentials
- `STRAVA_*` - Strava API credentials

## Dependencies

Python with `requests` package. No formal requirements.txt - install manually as needed.
