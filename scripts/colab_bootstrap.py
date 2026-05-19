"""
Colab environment bootstrap (reference implementation).

Notebooks embed the same logic inline so setup works before the package is installed.
This module is used after `pip install -e .` for tests or programmatic setup.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

DEFAULT_REPO_URL = "https://github.com/dk546/openf1-big-data-pipeline.git"
DEFAULT_REPO_NAME = "openf1-big-data-pipeline"
DEFAULT_DRIVE_OUTPUT = "/content/drive/MyDrive/openf1_big_data_pipeline"


def run_colab_setup(
    *,
    use_google_drive: bool = True,
    repo_url: str = DEFAULT_REPO_URL,
    repo_name: str = DEFAULT_REPO_NAME,
    project_root: Path | None = None,
    drive_output_root: Path | None = None,
) -> dict[str, Path]:
    """Clone/update repo, install deps, configure paths. Returns ensure_project_directories() map."""
    project_root = project_root or Path(f"/content/{repo_name}")
    drive_output_root = drive_output_root or Path(DEFAULT_DRIVE_OUTPUT)

    if use_google_drive:
        from google.colab import drive

        drive.mount("/content/drive")
        drive_output_root.mkdir(parents=True, exist_ok=True)
        os.environ["OPENF1_DATA_ROOT"] = str(drive_output_root)
    else:
        os.environ.pop("OPENF1_DATA_ROOT", None)

    if not project_root.exists():
        subprocess.run(["git", "clone", repo_url, str(project_root)], check=True)
    else:
        subprocess.run(["git", "-C", str(project_root), "pull"], check=False)

    os.chdir(project_root)

    required = [
        project_root / "README.md",
        project_root / "pyproject.toml",
        project_root / "src" / "openf1_pipeline" / "__init__.py",
        project_root / "src" / "openf1_pipeline" / "config.py",
    ]
    for path in required:
        if not path.exists():
            raise FileNotFoundError(f"Missing required project file: {path}")

    subprocess.run(
        [sys.executable, "-m", "pip", "install", "-q", "-r", "requirements.txt"],
        check=True,
    )
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "-e", "."], check=True)

    src = project_root / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))

    import openf1_pipeline  # noqa: F401

    from openf1_pipeline.config import ensure_project_directories

    return ensure_project_directories()
