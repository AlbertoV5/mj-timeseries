from scipy.stats import spearmanr, pearsonr, kendalltau
from typing import Callable
import pandas as pd


def _make_correlation_table(
    df: pd.DataFrame, func: Callable, index: int
) -> pd.DataFrame:
    """Use scipy correlation methods"""
    table = []
    cols = set(df.columns)
    for c1 in cols:
        cols = cols - {c1}
        for c2 in cols:
            result = func(df[c1], df[c2])[index]
            table.append((c1, c2, result))
    return (
        pd.DataFrame(table, columns=["A", "B", "score"])
        .sort_values("score", ascending=False)
        .reset_index()
        .drop(columns=["index"])
    )


def make_spearman_table(df: pd.DataFrame) -> pd.DataFrame:
    """Create a table of spearman correlation for all columns and sort descending"""
    return _make_correlation_table(df, spearmanr, 0)


def make_pearson_table(df: pd.DataFrame) -> pd.DataFrame:
    """Create a table of pearson correlation for all columns and sort descending"""
    return _make_correlation_table(df, pearsonr, 0)


def make_kendalltau_table(df: pd.DataFrame) -> pd.DataFrame:
    """Create a table of kendalltau correlation for all columns and sort descending"""
    return _make_correlation_table(df, kendalltau, 0)
