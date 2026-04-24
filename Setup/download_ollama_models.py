from __future__ import annotations

import argparse
import json
import platform
import re
import shutil
import subprocess
import sys
import time
from urllib.error import URLError
from urllib.request import Request, urlopen


OLLAMA_BASE_URL = "http://localhost:11434"

DEFAULT_MODELS = [
    "llama3.1:8b",
    "llama3.2",
    "mistral",
    "gemma:7b",
    "phi3",
    "qwen2",
]

HEAVY_MODELS = [
    "llama3.1:70b",
    "mixtral",
]

ALL_MODELS = DEFAULT_MODELS + HEAVY_MODELS


def run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        text=True,
        capture_output=True,
        check=check,
    )


def request_json(url: str, timeout: int = 10) -> dict:
    req = Request(url, headers={"Accept": "application/json"})

    with urlopen(req, timeout=timeout) as response:
        raw = response.read().decode("utf-8")
        return json.loads(raw)


def render_progress(model: str, percent: int, status: str) -> None:
    width = 30
    percent = max(0, min(percent, 100))
    filled = int(width * percent / 100)
    bar = "#" * filled + "-" * (width - filled)

    sys.stdout.write(f"\r[{bar}] {percent:>3}% | {model} | {status[:70]}")
    sys.stdout.flush()


def ensure_ollama_installed(auto_install: bool = True) -> None:
    if shutil.which("ollama"):
        return

    print("ollama not found on PATH.")

    if not auto_install:
        raise SystemExit("Install Ollama manually: https://ollama.com/download")

    if platform.system() == "Darwin" and shutil.which("brew"):
        print("Installing Ollama with Homebrew...")
        try:
            run(["brew", "install", "ollama"], check=True)
            return
        except subprocess.CalledProcessError as exc:
            print(exc.stdout)
            print(exc.stderr, file=sys.stderr)
            raise SystemExit("Homebrew install failed. Install Ollama manually.") from exc

    raise SystemExit(
        "Automatic Ollama installation is only supported on macOS with Homebrew. "
        "Install manually: https://ollama.com/download"
    )


def is_server_ready() -> bool:
    try:
        payload = request_json(f"{OLLAMA_BASE_URL}/api/tags", timeout=5)
        return isinstance(payload, dict)
    except Exception:
        return False


def ensure_server_running(start_server: bool = True) -> subprocess.Popen | None:
    if is_server_ready():
        print("Ollama server is already running.")
        return None

    if not start_server:
        raise SystemExit(
            "Ollama server is not running. Start it manually with: ollama serve"
        )

    print("Starting Ollama server...")

    process = subprocess.Popen(
        ["ollama", "serve"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    for _ in range(30):
        time.sleep(1)
        if is_server_ready():
            print("Ollama server is ready.")
            return process

    raise SystemExit("Ollama server did not become ready at http://localhost:11434")


def get_installed_models() -> set[str]:
    payload = request_json(f"{OLLAMA_BASE_URL}/api/tags", timeout=10)
    models = payload.get("models", [])

    installed: set[str] = set()

    for item in models:
        name = item.get("name")
        if name:
            installed.add(name)

    return installed


def normalize_model_name(model: str) -> str:
    return model.strip()


def model_is_installed(model: str, installed: set[str]) -> bool:
    model = normalize_model_name(model)

    if model in installed:
        return True

    # Ollama may return names with default tags in some environments.
    if ":" not in model:
        return f"{model}:latest" in installed

    return False


def parse_progress_percent(line: str) -> int | None:
    match = re.search(r"(\d{1,3})%", line)
    if match:
        return max(0, min(int(match.group(1)), 100))
    return None


def pull_model(model: str, retries: int = 2) -> None:
    for attempt in range(1, retries + 1):
        print(f"Pulling {model} (attempt {attempt}/{retries})...")

        process = subprocess.Popen(
            ["ollama", "pull", model],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        if process.stdout is None:
            raise SystemExit(f"Failed to read output while pulling {model}")

        last_percent = 0
        output_lines: list[str] = []

        for raw_line in process.stdout:
            line = raw_line.strip()
            if not line:
                continue

            output_lines.append(line)

            percent = parse_progress_percent(line)
            if percent is not None:
                last_percent = percent
                render_progress(model, last_percent, line)
            elif any(
                keyword in line.lower()
                for keyword in [
                    "pulling",
                    "downloading",
                    "verifying",
                    "writing",
                    "success",
                    "manifest",
                ]
            ):
                render_progress(model, last_percent, line)

        return_code = process.wait()

        if return_code == 0:
            render_progress(model, 100, "completed")
            print(f"\nFinished {model}")
            return

        print()
        print("\n".join(output_lines[-25:]))

        if attempt < retries:
            wait_seconds = 5 * attempt
            print(f"Retrying {model} in {wait_seconds} seconds...")
            time.sleep(wait_seconds)
        else:
            raise SystemExit(f"Failed to pull {model}")


def print_installed_models(installed: set[str]) -> None:
    print("Installed models:")

    if installed:
        for model in sorted(installed):
            print(f"  - {model}")
    else:
        print("  (none)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Install Ollama if needed, start the server, and download FrameScope local LLM models."
    )

    parser.add_argument(
        "--include-heavy",
        action="store_true",
        help="Also download large models such as llama3.1:70b and mixtral.",
    )

    parser.add_argument(
        "--models",
        nargs="*",
        default=None,
        help="Specific Ollama model names to download. Overrides default model list.",
    )

    parser.add_argument(
        "--no-auto-install",
        action="store_true",
        help="Do not attempt to install Ollama automatically.",
    )

    parser.add_argument(
        "--no-start-server",
        action="store_true",
        help="Do not attempt to start the Ollama server automatically.",
    )

    parser.add_argument(
        "--retries",
        type=int,
        default=2,
        help="Number of pull attempts per model.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    ensure_ollama_installed(auto_install=not args.no_auto_install)
    ensure_server_running(start_server=not args.no_start_server)

    if args.models:
        models = [normalize_model_name(m) for m in args.models]
    elif args.include_heavy:
        models = ALL_MODELS
    else:
        models = DEFAULT_MODELS

    installed_models = get_installed_models()
    print_installed_models(installed_models)

    print("\nModels requested:")
    for model in models:
        print(f"  - {model}")

    for index, model in enumerate(models, start=1):
        print(f"\n[{index}/{len(models)}] {model}")

        installed_models = get_installed_models()

        if model_is_installed(model, installed_models):
            print(f"Skipping {model} (already installed)")
            continue

        pull_model(model, retries=args.retries)

    print("\nDone.")


if __name__ == "__main__":
    main()