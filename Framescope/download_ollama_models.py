from __future__ import annotations

import json
import shutil
import subprocess
import sys
import time
from urllib.request import urlopen


OLLAMA_BASE_URL = "http://localhost:11434"

OLLAMA_MODELS = [
    "llama3.1:8b",
    "llama3.1:70b",
    "llama3.2",
    "mistral",
    "mixtral",
    "gemma:7b",
    "phi3",
    "qwen2",
]


def run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, text=True, capture_output=True, check=check)


def ensure_ollama_installed() -> None:
    if shutil.which("ollama"):
        return

    print("ollama not found on PATH.")

    if shutil.which("brew"):
        print("Installing ollama with Homebrew...")
        run(["brew", "install", "ollama"], check=True)
        return

    raise SystemExit(
        "ollama is not installed and Homebrew is not available. Install ollama first, then rerun this script."
    )


def ensure_server_running() -> None:
    try:
        with urlopen(f"{OLLAMA_BASE_URL}/api/tags", timeout=5) as response:
            if response.status == 200:
                return
    except Exception:
        pass

    print("Starting Ollama server...")
    subprocess.Popen(
        ["ollama", "serve"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    for _ in range(20):
        time.sleep(1)
        try:
            with urlopen(f"{OLLAMA_BASE_URL}/api/tags", timeout=5) as response:
                if response.status == 200:
                    return
        except Exception:
            continue

    raise SystemExit("Ollama server did not become ready at http://localhost:11434")


def get_installed_models() -> set[str]:
    with urlopen(f"{OLLAMA_BASE_URL}/api/tags", timeout=10) as response:
        payload = json.loads(response.read().decode("utf-8"))

    models = payload.get("models", [])
    installed: set[str] = set()

    for item in models:
        name = item.get("name")
        if name:
            installed.add(name)

    return installed


def pull_model(model: str) -> None:
    print(f"Pulling {model}...")
    result = run(["ollama", "pull", model], check=False)
    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr, file=sys.stderr)
        raise SystemExit(f"Failed to pull {model}")

    if result.stdout:
        print(result.stdout)


def main() -> None:
    ensure_ollama_installed()
    ensure_server_running()

    installed_models = get_installed_models()

    print("Installed models:")
    if installed_models:
        for model in sorted(installed_models):
            print(f"  - {model}")
    else:
        print("  (none)")

    for model in OLLAMA_MODELS:
        if model in installed_models:
            print(f"Skipping {model} (already installed)")
            continue

        pull_model(model)

    print("\nDone.")


if __name__ == "__main__":
    main()