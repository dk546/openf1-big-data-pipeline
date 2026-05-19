"""
Generate and validate artifacts/feature_definitions/feature_dictionary.csv.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


FEATURE_DICTIONARY_COLUMNS = [
    "feature_name",
    "definition",
    "source_tables",
    "transformation",
    "decision_time_available",
]


def build_feature_dictionary(entries: list[dict]) -> pd.DataFrame:
    """Assemble feature dictionary from documented feature specs."""
    # TODO: implement
    raise NotImplementedError("TODO: build_feature_dictionary")


def write_feature_dictionary(df: pd.DataFrame, output_path: Path) -> None:
    """Write artifacts/feature_definitions/feature_dictionary.csv."""
    # TODO: implement
    raise NotImplementedError("TODO: write_feature_dictionary")


def validate_no_leakage(df: pd.DataFrame, dictionary: pd.DataFrame) -> None:
    """Raise if model feature set includes post-decision-time columns."""
    # TODO: implement
    raise NotImplementedError("TODO: validate_no_leakage")
