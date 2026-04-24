# FrameScope
End-to-end pipeline for collecting, labeling, and analyzing metaphor framing and stance in Reddit and news discourse using LLMs.
SETUP

1) Clone and enter repo
git clone https://github.com/<sagnik-chakravarty>/FrameScope.git
cd FrameScope

2) Python environment
python3 -m venv .venv
source .venv/bin/activate  # Mac/Linux
```{python}
pip install -r requirements.txt
```

3) Local LLMs (Ollama)
```{bash}
# one command (installs Ollama if needed, starts server, pulls models)
python setup/download_ollama_models.py

# optional: heavy models (may require high RAM)
python setup/download_ollama_models.py --include-heavy
```

4) Verify
curl http://localhost:11434   # should return: Ollama is running

5) Run
```{bash}
python scripts/01_fetch_reddit.py
jupyter notebook notebooks/01_LLM_comparison_metrics.ipynb
```

Notes:
- If you see “connection refused”, run: ollama serve
- Default safe models: llama3.1:8b, llama3.2, mistral
