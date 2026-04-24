# FrameScope

End-to-end pipeline for collecting, labeling, and analyzing metaphor framing and stance in Reddit and news discourse using LLMs.

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

### 5. Run the pipeline

```bash
python Scripts/01_fetch_reddit.py
python Scripts/02_clean_store.py
python Scripts/03_sentence_preprocess.py
python Scripts/04_update_database.py
```

Or open the notebook:

```bash
jupyter notebook Notebooks/01_LLM_comparison_metrics.ipynb
```

## Notes

- If you see `connection refused`, start Ollama with `ollama serve`.
- The downloader skips models that are already installed.
- Large models such as `llama3.1:70b` may require significant RAM and disk space.
