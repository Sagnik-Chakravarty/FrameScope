# PyPI Release Guide

This document describes how to release FrameScope to PyPI.

## Prerequisites

1. **PyPI Account**: Create an account at https://pypi.org
2. **GitHub Token**: Generate a PyPI API token:
   - Go to https://pypi.org/manage/account/tokens/
   - Create a new token with "Entire repository" scope
   - Copy the token

3. **GitHub Secrets**: Add the token to your repository:
   - Go to GitHub repo → Settings → Secrets and variables → Actions
   - Create a new repository secret: `PYPI_API_TOKEN`
   - Paste the PyPI token value

## Release Process

### Option A: Automatic (Recommended)

1. **Tag a release**:
   ```bash
   git tag v0.2.0
   git push origin v0.2.0
   ```
   OR create a release on GitHub at: https://github.com/sagnik-chakravarty/FrameScope/releases/new

2. **GitHub Actions automatically**:
   - Builds the package
   - Runs verification (twine check)
   - Publishes to PyPI

### Option B: Manual

1. **Update version** in `pyproject.toml`:
   ```toml
   version = "0.2.0"
   ```

2. **Update CHANGELOG.md** with release notes

3. **Commit and tag**:
   ```bash
   git add pyproject.toml CHANGELOG.md
   git commit -m "Release v0.2.0"
   git tag v0.2.0
   git push origin main
   git push origin v0.2.0
   ```

4. **Build locally**:
   ```bash
   pip install build twine
   python -m build
   twine check dist/*
   ```

5. **Upload to PyPI**:
   ```bash
   twine upload dist/*
   ```
   (Enter your PyPI credentials or use token)

## Verification

After release, verify on PyPI:
- https://pypi.org/project/arcshiftwrap/

Install and test:
```bash
pip install --upgrade arcshiftwrap
python -c "from arcshiftwrap import ArcticShiftClient; print('Success!')"
```

## Troubleshooting

**"Invalid distribution"**: Run `twine check dist/*` to see errors

**"Already exists"**: Version already released. Update version in `pyproject.toml` and rebuild.

**"Unauthorized"**: Check PyPI token or credentials are correct.
