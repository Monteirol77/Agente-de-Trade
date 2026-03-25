"""
Persistência SQLite: preços, trades e sinais.

Os nomes de colunas `preco_eur`, `valor_eur`, `pl_eur` são históricos (legado);
os valores gravados correspondem à moeda de cotação actual (`config.VS_CURRENCY`, USD por defeito).
"""
from __future__ import annotations

import logging
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

import numpy as np
import pandas as pd

import config

logger = logging.getLogger(__name__)


def _to_epoch_ms_series(ts: pd.Series) -> pd.Series:
    """
    Converte para int64 de milissegundos desde epoch (UTC).
    Trata datetime64 ns/us/ms (pandas 2.x pode usar microsegundos).
    """
    dt = pd.to_datetime(ts, utc=True)
    if hasattr(dt.dt, "as_unit"):
        return dt.dt.as_unit("ms").astype("int64")
    unit = np.datetime_data(dt.dtype)[0]
    val = dt.astype("int64")
    if unit == "ns":
        return (val // 1_000_000).astype(int)
    if unit == "us":
        return (val // 1_000).astype(int)
    if unit == "ms":
        return val.astype(int)
    if unit == "s":
        return (val * 1_000).astype(int)
    return (val // 1_000_000).astype(int)


def _ensure_data_dir() -> None:
    config.DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)


def backup_database_file() -> Path:
    """
    Cópia de segurança de `trading.db` no mesmo diretório (timestamp no nome).
    Não apaga dados; útil antes de reinstalar ou migrar manualmente.
    """
    import shutil
    from datetime import datetime

    init_db()
    src = config.DATABASE_PATH
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = src.with_name(f"trading_backup_{ts}.db")
    shutil.copy2(src, dst)
    logger.info("Backup da base de dados: %s", dst)
    return dst


def init_db() -> None:
    """Cria tabelas se não existirem."""
    _ensure_data_dir()
    with get_connection() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS prices (
                timestamp INTEGER NOT NULL,
                ativo TEXT NOT NULL,
                preco_eur REAL NOT NULL,
                PRIMARY KEY (timestamp, ativo)
            );

            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ativo TEXT NOT NULL,
                preco_entrada REAL NOT NULL,
                preco_saida REAL,
                quantidade REAL NOT NULL,
                valor_eur REAL NOT NULL,
                pl_eur REAL,
                pl_pct REAL,
                motivo_saida TEXT,
                ts_entrada INTEGER NOT NULL,
                ts_saida INTEGER
            );

            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER NOT NULL,
                ativo TEXT NOT NULL,
                k REAL,
                d REAL,
                ma7 REAL,
                preco REAL NOT NULL,
                sinal TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_signals_ts ON signals(timestamp);

            CREATE TABLE IF NOT EXISTS chart_cache_meta (
                ativo TEXT PRIMARY KEY,
                last_fetch_epoch REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS portfolio_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                saldo_disponivel REAL NOT NULL,
                agente_parado INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS equity_history (
                timestamp INTEGER NOT NULL PRIMARY KEY,
                saldo_disponivel REAL NOT NULL,
                valor_posicoes REAL NOT NULL,
                patrimonio_total REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_equity_ts ON equity_history(timestamp);
            """
        )
        conn.commit()
        _ensure_portfolio_state_row(conn)
        _migrate_trades_schema(conn)
    logger.info("Base de dados inicializada em %s", config.DATABASE_PATH)


def _migrate_trades_schema(conn: sqlite3.Connection) -> None:
    """Adiciona colunas novas à tabela `trades` (SQLite não tem IF NOT EXISTS em ALTER)."""
    cur = conn.execute("PRAGMA table_info(trades)")
    cols = {row[1] for row in cur.fetchall()}
    if "duracao_segundos" not in cols:
        conn.execute(
            "ALTER TABLE trades ADD COLUMN duracao_segundos INTEGER DEFAULT 0"
        )
        conn.commit()
        logger.info("Coluna trades.duracao_segundos adicionada (DEFAULT 0).")
    conn.execute(
        """
        UPDATE trades
        SET duracao_segundos = CAST((ts_saida - ts_entrada) AS INTEGER) / 1000
        WHERE ts_saida IS NOT NULL
          AND ts_entrada IS NOT NULL
          AND (duracao_segundos IS NULL OR duracao_segundos < 0)
        """
    )
    conn.commit()


def _ensure_portfolio_state_row(conn: sqlite3.Connection) -> None:
    cur = conn.execute("SELECT COUNT(*) FROM portfolio_state WHERE id = 1")
    if cur.fetchone()[0] == 0:
        conn.execute(
            "INSERT INTO portfolio_state (id, saldo_disponivel, agente_parado) VALUES (1, ?, 0)",
            (config.SALDO_INICIAL,),
        )
        conn.commit()


@contextmanager
def get_connection() -> Generator[sqlite3.Connection, None, None]:
    conn = sqlite3.connect(config.DATABASE_PATH, timeout=30.0)
    try:
        conn.row_factory = sqlite3.Row
        yield conn
    finally:
        conn.close()


def replace_prices_for_asset(df: pd.DataFrame, ativo: str) -> None:
    """
    Substitui o histórico em cache para um ativo.
    Espera colunas: timestamp (datetime ou int ms), close.
    """
    if df.empty:
        return
    init_db()
    work = df.copy()
    ts = work["timestamp"]
    if pd.api.types.is_datetime64_any_dtype(ts):
        work["timestamp"] = _to_epoch_ms_series(ts)
    elif pd.api.types.is_integer_dtype(ts):
        work["timestamp"] = ts.astype(int)
    else:
        work["timestamp"] = _to_epoch_ms_series(pd.to_datetime(ts, utc=True))

    # A API pode devolver o mesmo timestamp em mais do que uma linha
    work = work.drop_duplicates(subset=["timestamp"], keep="last")
    work = work.sort_values("timestamp").reset_index(drop=True)

    with get_connection() as conn:
        conn.execute("DELETE FROM prices WHERE ativo = ?", (ativo,))
        rows = [
            (int(r["timestamp"]), ativo, float(r["close"]))
            for _, r in work.iterrows()
        ]
        conn.executemany(
            "INSERT INTO prices (timestamp, ativo, preco_eur) VALUES (?, ?, ?)",
            rows,
        )
        conn.commit()
    logger.info("Cache prices: %s pontos para %s", len(rows), ativo)


def load_prices_from_db(ativo: str) -> pd.DataFrame:
    """Carrega preços do SQLite para [timestamp, close]."""
    init_db()
    with get_connection() as conn:
        cur = conn.execute(
            "SELECT timestamp, preco_eur FROM prices WHERE ativo = ? ORDER BY timestamp",
            (ativo,),
        )
        rows = cur.fetchall()
    if not rows:
        return pd.DataFrame(columns=["timestamp", "close"])
    df = pd.DataFrame(rows, columns=["timestamp", "preco_eur"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df = df.rename(columns={"preco_eur": "close"})
    return df


def get_latest_chart_fetch_time(ativo: str) -> int | None:
    """Último timestamp (ms) do último candle guardado, ou None."""
    init_db()
    with get_connection() as conn:
        cur = conn.execute(
            "SELECT MAX(timestamp) FROM prices WHERE ativo = ?", (ativo,)
        )
        row = cur.fetchone()
    if row is None or row[0] is None:
        return None
    return int(row[0])


def get_last_chart_api_fetch_epoch(ativo: str) -> float | None:
    """Epoch (segundos) da última vez que o histórico foi atualizado via API."""
    init_db()
    with get_connection() as conn:
        cur = conn.execute(
            "SELECT last_fetch_epoch FROM chart_cache_meta WHERE ativo = ?",
            (ativo,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return float(row[0])


def set_last_chart_api_fetch_epoch(ativo: str, epoch: float) -> None:
    init_db()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO chart_cache_meta (ativo, last_fetch_epoch)
            VALUES (?, ?)
            ON CONFLICT(ativo) DO UPDATE SET last_fetch_epoch = excluded.last_fetch_epoch
            """,
            (ativo, epoch),
        )
        conn.commit()


def get_portfolio_state() -> tuple[float, bool]:
    """(saldo_disponivel, agente_parado)."""
    init_db()
    with get_connection() as conn:
        row = conn.execute(
            "SELECT saldo_disponivel, agente_parado FROM portfolio_state WHERE id = 1"
        ).fetchone()
    if row is None:
        return config.SALDO_INICIAL, False
    return float(row[0]), bool(row[1])


def set_portfolio_state(
    saldo_disponivel: float,
    *,
    agente_parado: bool | None = None,
) -> None:
    init_db()
    with get_connection() as conn:
        if agente_parado is None:
            conn.execute(
                "UPDATE portfolio_state SET saldo_disponivel = ? WHERE id = 1",
                (saldo_disponivel,),
            )
        else:
            conn.execute(
                "UPDATE portfolio_state SET saldo_disponivel = ?, agente_parado = ? WHERE id = 1",
                (saldo_disponivel, int(agente_parado)),
            )
        conn.commit()


def insert_trade_open(
    ativo: str,
    preco_entrada: float,
    quantidade: float,
    valor_eur: float,
    ts_entrada_ms: int,
) -> int:
    """Insere trade aberto (preco_saida NULL). Devolve id."""
    init_db()
    with get_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO trades (
                ativo, preco_entrada, preco_saida, quantidade, valor_eur,
                pl_eur, pl_pct, motivo_saida, ts_entrada, ts_saida
            ) VALUES (?, ?, NULL, ?, ?, NULL, NULL, NULL, ?, NULL)
            """,
            (ativo, preco_entrada, quantidade, valor_eur, ts_entrada_ms),
        )
        conn.commit()
        return int(cur.lastrowid)


def close_trade(
    trade_id: int,
    preco_saida: float,
    pl_eur: float,
    pl_pct: float,
    motivo_saida: str,
    ts_saida_ms: int,
) -> None:
    init_db()
    with get_connection() as conn:
        row = conn.execute(
            "SELECT ts_entrada FROM trades WHERE id = ?",
            (trade_id,),
        ).fetchone()
        dur_sec: int | None = None
        if row and row[0] is not None:
            dur_sec = max(0, int((ts_saida_ms - int(row[0])) // 1000))
        conn.execute(
            """
            UPDATE trades SET
                preco_saida = ?,
                pl_eur = ?,
                pl_pct = ?,
                motivo_saida = ?,
                ts_saida = ?,
                duracao_segundos = ?
            WHERE id = ?
            """,
            (preco_saida, pl_eur, pl_pct, motivo_saida, ts_saida_ms, dur_sec, trade_id),
        )
        conn.commit()


def fetch_open_trades() -> list[dict]:
    init_db()
    with get_connection() as conn:
        cur = conn.execute(
            """
            SELECT id, ativo, preco_entrada, quantidade, valor_eur, ts_entrada
            FROM trades
            WHERE preco_saida IS NULL
            ORDER BY ts_entrada
            """
        )
        rows = cur.fetchall()
    return [
        {
            "id": r[0],
            "ativo": r[1],
            "preco_entrada": float(r[2]),
            "quantidade": float(r[3]),
            "valor_eur": float(r[4]),
            "ts_entrada": int(r[5]),
        }
        for r in rows
    ]


def fetch_closed_trades(limit: int = 10_000) -> list[dict]:
    init_db()
    with get_connection() as conn:
        cur = conn.execute(
            """
            SELECT id, ativo, preco_entrada, preco_saida, quantidade, valor_eur,
                   pl_eur, pl_pct, motivo_saida, ts_entrada, ts_saida, duracao_segundos
            FROM trades
            WHERE preco_saida IS NOT NULL
            ORDER BY ts_saida DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = cur.fetchall()
    out = []
    for r in rows:
        out.append(
            {
                "id": r[0],
                "ativo": r[1],
                "preco_entrada": float(r[2]),
                "preco_saida": float(r[3]),
                "quantidade": float(r[4]),
                "valor_eur": float(r[5]),
                "pl_eur": float(r[6]) if r[6] is not None else None,
                "pl_pct": float(r[7]) if r[7] is not None else None,
                "motivo_saida": r[8],
                "ts_entrada": int(r[9]),
                "ts_saida": int(r[10]),
                "duracao_segundos": int(r[11]) if r[11] is not None else None,
            }
        )
    return out


def fetch_trade_by_id(trade_id: int) -> dict | None:
    """Um trade fechado ou aberto por id."""
    init_db()
    with get_connection() as conn:
        cur = conn.execute(
            """
            SELECT id, ativo, preco_entrada, preco_saida, quantidade, valor_eur,
                   pl_eur, pl_pct, motivo_saida, ts_entrada, ts_saida, duracao_segundos
            FROM trades WHERE id = ?
            """,
            (trade_id,),
        )
        r = cur.fetchone()
    if r is None:
        return None
    return {
        "id": r[0],
        "ativo": r[1],
        "preco_entrada": float(r[2]),
        "preco_saida": float(r[3]) if r[3] is not None else None,
        "quantidade": float(r[4]),
        "valor_eur": float(r[5]),
        "pl_eur": float(r[6]) if r[6] is not None else None,
        "pl_pct": float(r[7]) if r[7] is not None else None,
        "motivo_saida": r[8],
        "ts_entrada": int(r[9]),
        "ts_saida": int(r[10]) if r[10] is not None else None,
        "duracao_segundos": int(r[11]) if r[11] is not None else None,
    }


def load_prices_range_ms(ativo: str, ts_ms_from: int, ts_ms_to: int) -> pd.DataFrame:
    """Preços em cache entre dois instantes (ms), [timestamp, close]."""
    init_db()
    with get_connection() as conn:
        cur = conn.execute(
            """
            SELECT timestamp, preco_eur FROM prices
            WHERE ativo = ? AND timestamp >= ? AND timestamp <= ?
            ORDER BY timestamp
            """,
            (ativo, ts_ms_from, ts_ms_to),
        )
        rows = cur.fetchall()
    if not rows:
        return pd.DataFrame(columns=["timestamp", "close"])
    df = pd.DataFrame(rows, columns=["timestamp", "preco_eur"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df = df.rename(columns={"preco_eur": "close"})
    return df


def insert_equity_snapshot(
    ts_ms: int,
    saldo_disponivel: float,
    valor_posicoes: float,
    patrimonio_total: float,
) -> None:
    """Um ponto da curva de equity por ciclo do agente (~30s)."""
    init_db()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO equity_history (
                timestamp, saldo_disponivel, valor_posicoes, patrimonio_total
            ) VALUES (?, ?, ?, ?)
            """,
            (ts_ms, saldo_disponivel, valor_posicoes, patrimonio_total),
        )
        conn.commit()


def fetch_equity_history_df(limit: int = 10_000) -> pd.DataFrame:
    init_db()
    with get_connection() as conn:
        cur = conn.execute(
            """
            SELECT timestamp, saldo_disponivel, valor_posicoes, patrimonio_total
            FROM equity_history
            ORDER BY timestamp ASC
            LIMIT ?
            """,
            (limit,),
        )
        rows = cur.fetchall()
    if not rows:
        return pd.DataFrame(
            columns=["timestamp", "saldo_disponivel", "valor_posicoes", "patrimonio_total"]
        )
    df = pd.DataFrame(rows, columns=["timestamp", "saldo_disponivel", "valor_posicoes", "patrimonio_total"])
    return df


def fetch_chart_trade_markers(coin_id: str) -> dict[str, list[dict[str, Any]]]:
    """Marcadores de compra/venda para gráficos por id CoinGecko."""
    init_db()
    with get_connection() as conn:
        cur = conn.execute(
            """
            SELECT ts_entrada, preco_entrada, ts_saida, preco_saida
            FROM trades
            WHERE ativo = ?
            ORDER BY ts_entrada
            """,
            (coin_id,),
        )
        rows = cur.fetchall()
    buys: list[dict[str, Any]] = []
    sells: list[dict[str, Any]] = []
    for r in rows:
        buys.append({"ts": int(r[0]), "price": float(r[1])})
        if r[2] is not None and r[3] is not None:
            sells.append({"ts": int(r[2]), "price": float(r[3])})
    return {"buy": buys, "sell": sells}


def insert_signal_row(
    ts_ms: int,
    ativo: str,
    k: float | None,
    d: float | None,
    ma7: float | None,
    preco: float,
    sinal: str,
) -> None:
    init_db()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO signals (timestamp, ativo, k, d, ma7, preco, sinal)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (ts_ms, ativo, k, d, ma7, preco, sinal),
        )
        conn.commit()
