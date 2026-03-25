"""
Indicadores técnicos: Stochastic RSI (14, 3, 3) e média móvel simples de 7 dias.

- Stoch RSI: pandas-ta com high=low=close (só temos preço único da CoinGecko).
- MA7: SMA de 7 fechos diários (ta.sma), alinhada a cada barra horária (merge_asof).
"""
from __future__ import annotations

import logging
from typing import Final

import pandas as pd
import pandas_ta as ta

logger = logging.getLogger(__name__)

# Parâmetros Stochastic RSI (especificação)
STOCH_LENGTH: Final[int] = 14
STOCH_RSI_LENGTH: Final[int] = 14
STOCH_K: Final[int] = 3
STOCH_D: Final[int] = 3

# Nomes das colunas devolvidas pelo pandas-ta (fixos para estes parâmetros)
_STOCH_K_COL: Final[str] = "STOCHRSIk_14_14_3_3"
_STOCH_D_COL: Final[str] = "STOCHRSId_14_14_3_3"

# MA: 7 dias de fechos diários
MA7_LENGTH: Final[int] = 7


def compute_stochastic_rsi(
    df: pd.DataFrame,
    *,
    close_col: str = "close",
) -> pd.DataFrame:
    """
    Calcula Stochastic RSI 14,3,3 sobre a coluna de fecho.
    Adiciona colunas `stoch_k` e `stoch_d` (%K e %D).
    """
    if close_col not in df.columns:
        raise ValueError(f"Coluna em falta: {close_col}")

    work = df.copy()
    work = work.sort_values(by=_time_column(work)).reset_index(drop=True)
    work["high"] = work[close_col]
    work["low"] = work[close_col]

    stoch = work.ta.stochrsi(
        high="high",
        low="low",
        close=close_col,
        length=STOCH_LENGTH,
        rsi_length=STOCH_RSI_LENGTH,
        k=STOCH_K,
        d=STOCH_D,
    )
    if stoch is None or stoch.empty:
        work["stoch_k"] = pd.NA
        work["stoch_d"] = pd.NA
        logger.warning("stochrsi não produziu dados (série demasiado curta?).")
        return work

    work["stoch_k"] = stoch[_STOCH_K_COL].values
    work["stoch_d"] = stoch[_STOCH_D_COL].values
    return work


def _time_column(df: pd.DataFrame) -> str:
    if "timestamp" in df.columns:
        return "timestamp"
    if isinstance(df.index, pd.DatetimeIndex):
        return "__index__"
    raise ValueError("É necessária coluna 'timestamp' ou índice DatetimeIndex.")


def compute_ma7_daily(
    df: pd.DataFrame,
    *,
    close_col: str = "close",
) -> pd.Series:
    """
    Média móvel simples dos últimos 7 **dias** de preço de fecho (agregação diária),
    usando `ta.sma` sobre fechos diários. Devolve uma série alinhada à ordem das
    linhas de `df` (após ordenar por tempo).
    """
    work = df.copy()
    tcol = _time_column(work)
    if tcol == "__index__":
        work = work.reset_index().rename(columns={"index": "timestamp"})
        tcol = "timestamp"

    work = work.sort_values(by=tcol).reset_index(drop=True)
    work[tcol] = pd.to_datetime(work[tcol], utc=True)
    s = work.set_index(tcol)[close_col].sort_index()

    # Um fecho por dia civil (UTC): último preço de cada dia
    daily_close = s.resample("1D").last().dropna()
    if daily_close.empty:
        return pd.Series([pd.NA] * len(work), dtype="Float64")

    ma7_daily = ta.sma(daily_close, length=MA7_LENGTH)
    if ma7_daily is None:
        return pd.Series([pd.NA] * len(work), dtype="Float64")

    curve = ma7_daily.rename("ma7").reset_index()
    curve.columns = [tcol, "ma7"]
    base = work[[tcol]].copy()

    merged = pd.merge_asof(
        base.sort_values(tcol),
        curve.sort_values(tcol),
        on=tcol,
        direction="backward",
    )
    # merge_asof pode deixar NA no início até existirem 7 dias de histórico
    return merged["ma7"].reset_index(drop=True)


def compute_all_indicators(
    df: pd.DataFrame,
    *,
    close_col: str = "close",
) -> pd.DataFrame:
    """
    Junta Stoch RSI e MA7 num único DataFrame.
    Mantém `timestamp` e `close` e adiciona `stoch_k`, `stoch_d`, `ma7`.
    """
    if df.empty:
        out = df.copy()
        for c in ("stoch_k", "stoch_d", "ma7"):
            out[c] = pd.Series(dtype="float64")
        return out

    with_stoch = compute_stochastic_rsi(df, close_col=close_col)
    ma7 = compute_ma7_daily(df, close_col=close_col)
    with_stoch["ma7"] = ma7.values
    # Colunas só para o pandas-ta
    for aux in ("high", "low"):
        if aux in with_stoch.columns:
            with_stoch = with_stoch.drop(columns=[aux])
    return with_stoch


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    from data_feed import fetch_market_chart_history

    import config

    for sym, cid in config.ASSETS.items():
        hist = fetch_market_chart_history(cid, force_refresh=False)
        ind = compute_all_indicators(hist)
        print(f"\n=== {sym} ({cid}) — últimas 5 linhas com indicadores ===")
        cols = ["timestamp", "close", "stoch_k", "stoch_d", "ma7"]
        cols = [c for c in cols if c in ind.columns]
        print(ind[cols].tail(5).to_string())
