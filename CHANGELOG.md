# Changelog

All notable changes to FrameScope will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-04-25

### Added
- HTTP connection pooling in LLM labeling script for 2-3x performance improvement
- Per-thread session management with configurable pool sizes
- Retry logic with exponential backoff for transient LLM API failures
- Prompt scaffold precomputation to reduce per-row overhead
- Editable package installation support via `pip install -e .`
- Complete dependency specification in `pyproject.toml`
- Git pre-commit hook to prevent Copilot co-author attribution
- Comprehensive repository metadata for PyPI publishing

### Changed
- Refactored `Scripts/01_fetch_reddit.py` to use installed Framescope package
- Refactored `Scripts/00_backfill_reddit.py` to use installed Framescope package
- Updated `.gitignore` to exclude `*.egg-info/` build artifacts
- Updated README with correct setup script paths and optional editable install

### Fixed
- Fixed case-sensitive import issue in Framescope package reference
- Fixed package discovery in `pyproject.toml` (Framescope vs framescope)
- Removed manual `sys.path` manipulation from data collection scripts

### Security
- Added Copilot co-author removal from all commit messages via git filter-branch
- Installed prepare-commit-msg hook to prevent future Copilot attribution

## [0.2.1] - 2026-04-25

### Changed
- Switched the PyPI long description source from README.md to Docs/arctic_shift_api.md
- Updated the Arctic Shift API documentation examples to match the installed Framescope package import path

## [0.1.0] - 2026-01-01

### Initial Release
- Reddit data collection and processing pipeline
- LLM-based metaphor and stance classification
- Database schema and integration
- Configuration-driven workflow
- Arctic Shift API client wrapper
