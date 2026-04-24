# FrameScope

FrameScope is a pipeline for studying how people talk about AI in real-world data.

It collects data from Reddit and news sources, processes the text, and uses LLMs to label things like metaphors and stance. The goal isn’t just to run models, but to understand how well they actually capture the way people describe AI, especially compared to human annotations.

The project focuses on building a clean, reproducible workflow—from data collection to evaluation—and on comparing different models in a more systematic way.

## Table of Contents

- [Setup](#setup)

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/sagnik-chakravarty/FrameScope.git
cd FrameScope
```

### 2. Create a Python environment

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Install and prepare Ollama models

This script will:
- install Ollama if it is missing
- start the Ollama server if needed
- check which models are already installed
- download only the missing models

```bash
python Framescope/download_ollama_models.py
```

### 4. Verify Ollama is running

```bash
curl http://localhost:11434/api/tags
```

If Ollama is working, you should see a JSON response.

## Notes

- If you see `connection refused`, start Ollama with `ollama serve`.
- The downloader skips models that are already installed.
- Large models such as `llama3.1:70b` may require significant RAM and disk space.
