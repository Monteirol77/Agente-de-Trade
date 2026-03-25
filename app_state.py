"""
Estado global partilhado entre o ciclo do agente (thread) e o dashboard Dash.
"""
from __future__ import annotations

import threading
from typing import Any

import pandas as pd

from portfolio import Portfolio


class AppState:
    """Locks em torno de dados atualizados a cada ciclo."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.agent_running: bool = True
        self.portfolio = Portfolio()
        # id CoinGecko -> DataFrame com indicadores
        self.indicator_dfs: dict[str, pd.DataFrame] = {}
        self.last_prices: dict[str, float] = {}
        self.last_cycle_ts: float | None = None
        self.last_error: str | None = None

    def snapshot_ui(self) -> dict[str, Any]:
        """Cópia segura para callbacks Dash (cópias leves)."""
        with self.lock:
            return {
                "agent_running": self.agent_running,
                "last_prices": dict(self.last_prices),
                "last_cycle_ts": self.last_cycle_ts,
                "last_error": self.last_error,
                "saldo_disponivel": self.portfolio.saldo_disponivel,
                "agente_parado": self.portfolio.agente_parado,
                "indicator_dfs": {k: v.copy() for k, v in self.indicator_dfs.items()},
            }


state = AppState()
