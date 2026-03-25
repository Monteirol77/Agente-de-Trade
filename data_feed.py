"""
Fonte de dados CoinGecko: preço atual (leve) vs histórico market_chart (pesado).

- simple/price: a cada ciclo (~30s) — poucas chamadas.
- market_chart: cache SQLite; nova chamada à API no máximo 1× / 10 min por ativo
  (e no arranque se não houver dados).
- Espaço mínimo de 2s entre pedidos HTTP consecutivos.
- Em 429: espera 60s e registo de aviso antes de repetir.
"""
from __future__ import annotations

import logging
import time
from collections import deque
from typing import Any

import pandas as pd
import requests

import config
import database as db
from database import (
    get_last_chart_api_fetch_epoch,
    load_prices_from_db,
    replace_prices_for_asset,
    set_last_chart_api_fetch_epoch,
)

logger = logging.getLogger(__name__)

_RATE_WINDOW_SEC = 60.0
_last_request_end_mono: float = 0.0


class _RateLimiter:
    """Janela 60s — limite de chamadas/minuto (reforço além do gap de 2s)."""

    def __init__(self, max_per_minute: int) -> None:
        self._max = max(1, max_per_minute)
        self._times: deque[float] = deque()

    def acquire(self) -> None:
        now = time.monotonic()
        while self._times and self._times[0] < now - _RATE_WINDOW_SEC:
            self._times.popleft()
        if len(self._times) >= self._max:
            wait = _RATE_WINDOW_SEC - (now - self._times[0]) + 0.05
            if wait > 0:
                logger.warning(
                    "Rate limit interno: a aguardar %.1fs (%d chamadas/60s)",
                    wait,
                    len(self._times),
                )
                time.sleep(wait)
            now = time.monotonic()
            while self._times and self._times[0] < now - _RATE_WINDOW_SEC:
                self._times.popleft()
        self._times.append(time.monotonic())


_rate_limiter = _RateLimiter(config.COINGECKO_MAX_CALLS_PER_MINUTE)
_session = requests.Session()


def _sleep_min_gap_between_calls() -> None:
    """Pelo menos `COINGECKO_MIN_API_INTERVAL_SEC` s entre o fim de um pedido e o início do próximo."""
    global _last_request_end_mono
    gap = config.COINGECKO_MIN_API_INTERVAL_SEC
    if gap <= 0:
        return
    now = time.monotonic()
    if _last_request_end_mono > 0:
        elapsed = now - _last_request_end_mono
        if elapsed < gap:
            time.sleep(gap - elapsed)


def _mark_request_completed() -> None:
    global _last_request_end_mono
    _last_request_end_mono = time.monotonic()


def _headers() -> dict[str, str]:
    h: dict[str, str] = {}
    if config.COINGECKO_API_KEY:
        h["x-cg-pro-api-key"] = config.COINGECKO_API_KEY
    return h


def _request_with_retry(
    url: str,
    *,
    params: dict[str, Any] | None = None,
    max_retries: int = 5,
    timeout: float = 30.0,
) -> requests.Response:
    """
    GET com espaçamento mínimo entre chamadas, limite por minuto e retry.
    HTTP 429: espera fixa `COINGECKO_429_WAIT_SEC` (60s) + log de aviso.
    5xx / rede: backoff exponencial (até 60s).
    """
    backoff_net = 1.0
    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 1):
        _sleep_min_gap_between_calls()
        _rate_limiter.acquire()
        try:
            r = _session.get(url, params=params, headers=_headers(), timeout=timeout)
            _mark_request_completed()
            if r.status_code == 429:
                logger.warning(
                    "CoinGecko 429 (rate limit) — %s | tentativa %d/%d. "
                    "A aguardar %.0fs antes de repetir.",
                    url.split("/api/")[-1][:80],
                    attempt,
                    max_retries,
                    config.COINGECKO_429_WAIT_SEC,
                )
                time.sleep(config.COINGECKO_429_WAIT_SEC)
                continue
            if 500 <= r.status_code < 600:
                logger.warning(
                    "CoinGecko %s — tentativa %d/%d. Backoff %.1fs",
                    r.status_code,
                    attempt,
                    max_retries,
                    backoff_net,
                )
                time.sleep(backoff_net)
                backoff_net = min(backoff_net * 2, 60.0)
                continue
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            last_exc = e
            logger.exception(
                "Erro de rede CoinGecko (tentativa %d/%d): %s",
                attempt,
                max_retries,
                e,
            )
            time.sleep(backoff_net)
            backoff_net = min(backoff_net * 2, 60.0)
    if last_exc:
        raise last_exc
    raise RuntimeError("Falha desconhecida em _request_with_retry")


def fetch_simple_prices_eur(coin_ids: list[str] | None = None) -> dict[str, float]:
    """
    Preços atuais em EUR (endpoint simple/price) — chamada leve, adequada a cada ~30s.
    """
    ids = coin_ids or list(config.ASSETS.values())
    url = f"{config.COINGECKO_BASE_URL}/simple/price"
    params = {
        "ids": ",".join(ids),
        "vs_currencies": config.VS_CURRENCY,
    }
    try:
        r = _request_with_retry(url, params=params)
        data = r.json()
        out: dict[str, float] = {}
        for cid in ids:
            if cid in data and config.VS_CURRENCY in data[cid]:
                out[cid] = float(data[cid][config.VS_CURRENCY])
            else:
                logger.error("Resposta sem preço para id=%s: %s", cid, data.get(cid))
        return out
    except Exception:
        logger.exception("Falha ao obter preços simples")
        return {}


def fetch_coins_markets_eur(coin_ids: list[str] | None = None) -> dict[str, dict[str, Any]]:
    """
    Uma chamada a `/coins/markets` com preço, variação 24h, market cap e volume (EUR).
    Útil para cartões do dashboard (evita N× simple/price).
    """
    ids = coin_ids or list(config.ASSETS.values())
    url = f"{config.COINGECKO_BASE_URL}/coins/markets"
    params: dict[str, Any] = {
        "vs_currency": config.VS_CURRENCY,
        "ids": ",".join(ids),
        "order": "market_cap_desc",
        "per_page": max(10, len(ids)),
        "page": 1,
        "sparkline": "false",
    }
    try:
        r = _request_with_retry(url, params=params)
        rows = r.json()
        out: dict[str, dict[str, Any]] = {}
        for row in rows:
            cid = row.get("id")
            if not cid:
                continue
            out[str(cid)] = {
                "symbol": row.get("symbol"),
                "name": row.get("name"),
                "current_price": row.get("current_price"),
                "price_change_percentage_24h": row.get("price_change_percentage_24h"),
                "market_cap": row.get("market_cap"),
                "total_volume": row.get("total_volume"),
            }
        return out
    except Exception:
        logger.exception("Falha em coins/markets")
        return {}


def _market_chart_to_dataframe(payload: dict[str, Any]) -> pd.DataFrame:
    prices = payload.get("prices") or []
    if not prices:
        return pd.DataFrame(columns=["timestamp", "close"])
    rows = []
    for ts_ms, price in prices:
        rows.append({"timestamp": int(ts_ms), "close": float(price)})
    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df = df.sort_values("timestamp").drop_duplicates(
        subset=["timestamp"], keep="last"
    ).reset_index(drop=True)
    return df


def _should_refresh_chart_from_api(coin_id: str, *, force_refresh: bool) -> bool:
    """True se for necessário pedir `market_chart` à API."""
    if force_refresh:
        return True
    now = time.time()
    last = get_last_chart_api_fetch_epoch(coin_id)
    cached = load_prices_from_db(coin_id)
    if cached.empty:
        logger.info("Sem cache local para %s — a pedir market_chart à API.", coin_id)
        return True
    if last is None:
        return True
    age = now - last
    if age >= config.COINGECKO_CHART_REFRESH_SEC:
        logger.info(
            "Cache histórico %s com %.0f min — a atualizar market_chart (limite %d s).",
            coin_id,
            age / 60.0,
            config.COINGECKO_CHART_REFRESH_SEC,
        )
        return True
    logger.debug(
        "Cache hit histórico %s (idade %.0f s < refresh %d s) — sem API.",
        coin_id,
        age,
        config.COINGECKO_CHART_REFRESH_SEC,
    )
    return False


def fetch_market_chart_history(
    coin_id: str,
    *,
    days: int = 30,
    interval: str = "hourly",
    force_refresh: bool = False,
) -> pd.DataFrame:
    """
    Histórico para indicadores. Servido a partir do SQLite se o último `market_chart`
    tiver menos de `COINGECKO_CHART_REFRESH_SEC` segundos (predef.: 10 min).

    Não repetir `market_chart` a cada ciclo de 30s — só `fetch_simple_prices_eur`.
    """
    if not _should_refresh_chart_from_api(coin_id, force_refresh=force_refresh):
        return load_prices_from_db(coin_id)

    url = f"{config.COINGECKO_BASE_URL}/coins/{coin_id}/market_chart"
    params = {
        "vs_currency": config.VS_CURRENCY,
        "days": days,
        "interval": interval,
    }
    now = time.time()
    try:
        r = _request_with_retry(url, params=params)
        payload = r.json()
        df = _market_chart_to_dataframe(payload)
        if df.empty:
            logger.error("market_chart vazio para %s", coin_id)
            return load_prices_from_db(coin_id)
        replace_prices_for_asset(df, coin_id)
        set_last_chart_api_fetch_epoch(coin_id, now)
        return df
    except Exception:
        logger.exception("Falha market_chart para %s — a usar cache SQLite se existir", coin_id)
        return load_prices_from_db(coin_id)


def fetch_all_tracked_histories(
    force_refresh: bool = False,
) -> dict[str, pd.DataFrame]:
    out: dict[str, pd.DataFrame] = {}
    for _sym, cid in config.ASSETS.items():
        out[cid] = fetch_market_chart_history(cid, force_refresh=force_refresh)
    return out


def fetch_market_chart_range_sec(
    coin_id: str,
    ts_from_sec: int,
    ts_to_sec: int,
) -> pd.DataFrame:
    """
    Histórico de preços num intervalo [from, to] em segundos UNIX (UTC).
    Não altera o cache SQLite global (apenas leitura para gráficos de trade).
    """
    url = f"{config.COINGECKO_BASE_URL}/coins/{coin_id}/market_chart/range"
    params = {
        "vs_currency": config.VS_CURRENCY,
        "from": int(ts_from_sec),
        "to": int(ts_to_sec),
    }
    r = _request_with_retry(url, params=params)
    payload = r.json()
    return _market_chart_to_dataframe(payload)


def fetch_ohlc_eur(coin_id: str, days: int) -> pd.DataFrame:
    """
    Velas OHLC da CoinGecko. `days` ∈ {1,7,14,30,90,180,365}; aproxima se necessário.
    Colunas: timestamp, open, high, low, close (EUR).
    """
    allowed = (1, 7, 14, 30, 90, 180, 365)
    d = days if days in allowed else min(allowed, key=lambda a: abs(a - days))
    url = f"{config.COINGECKO_BASE_URL}/coins/{coin_id}/ohlc"
    params = {"vs_currency": config.VS_CURRENCY, "days": d}
    r = _request_with_retry(url, params=params)
    raw = r.json()
    if not raw:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close"])
    rows = []
    for candle in raw:
        if len(candle) < 5:
            continue
        ts_ms, o, h, low, c = candle[0], candle[1], candle[2], candle[3], candle[4]
        rows.append(
            {
                "timestamp": pd.to_datetime(int(ts_ms), unit="ms", utc=True),
                "open": float(o),
                "high": float(h),
                "low": float(low),
                "close": float(c),
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return df.sort_values("timestamp").reset_index(drop=True)


def merge_trade_window_prices(
    coin_id: str,
    ts_ms_from: int,
    ts_ms_to: int,
) -> pd.DataFrame:
    """
    Junta preços do SQLite (cache do agente) com `market_chart/range` da CoinGecko
    para cobrir a janela do trade sem substituir a tabela `prices`.
    """
    df_db = db.load_prices_range_ms(coin_id, ts_ms_from, ts_ms_to)
    ts_from_sec = int(ts_ms_from // 1000)
    ts_to_sec = int((ts_ms_to + 999) // 1000)
    if ts_to_sec <= ts_from_sec:
        ts_to_sec = ts_from_sec + 60
    df_api = pd.DataFrame(columns=["timestamp", "close"])
    try:
        df_api = fetch_market_chart_range_sec(coin_id, ts_from_sec, ts_to_sec)
    except Exception:
        logger.exception("market_chart/range falhou para %s — só cache local", coin_id)
    if df_db.empty:
        return df_api.copy() if not df_api.empty else df_db
    if df_api.empty:
        return df_db.copy()
    merged = pd.concat([df_db, df_api], ignore_index=True)
    merged = merged.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last")
    return merged.reset_index(drop=True)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    print("--- Preços atuais (simple/price) ---")
    simple = fetch_simple_prices_eur()
    print(simple)
    print("\n--- Indicadores a partir do histórico (cache ou API) ---")
    from indicators import compute_all_indicators

    for sym, cid in config.ASSETS.items():
        hist = fetch_market_chart_history(cid, force_refresh=False)
        ind = compute_all_indicators(hist)
        print(f"\n>> {sym} ({cid}) — linhas histórico: {len(hist)} | indicadores: {len(ind)}")
        if not ind.empty:
            cols = [c for c in ("timestamp", "close", "stoch_k", "stoch_d", "ma7") if c in ind.columns]
            print(ind[cols].tail(3).to_string())
        else:
            print("(sem dados — corre antes: python data_feed.py com rede)")
