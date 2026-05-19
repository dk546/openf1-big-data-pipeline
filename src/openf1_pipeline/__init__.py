"""
OpenF1 Medallion pipeline package.

Bronze → Silver → Gold → modeling and report artifacts.
"""

from openf1_pipeline.config import PROJECT_NAME, RANDOM_SEED, get_project_root

__all__ = ["PROJECT_NAME", "RANDOM_SEED", "get_project_root"]
