"""
Gráfico detalhado por trade (modal do dashboard): velas, níveis e Stochastic RSI.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import config
from data_feed import merge_trade_window_prices
from indicators import compute_stochastic_rsi

logger = logging.getLogger(__name__)

TPL_DARK = "plotly_dark"
MIN_POINTS = 6
PAD_MS = 2 * 3600 * 1000


def format_duration_human(seconds: int | None) -> str:
    if seconds is None or seconds < 0:
        return "—"
    if seconds < 3600:
        m = max(1, seconds // 60)
        return f"{m} min"
    if seconds < 86400:
        h = seconds // 3600
        m = (seconds % 3600) // 60
        return f"{h}h {m}min"
    days = seconds // 86400
    rem = seconds % 86400
    h = rem // 3600
    return f"{days} dias {h}h"


def format_duration_title(seconds: int | None) -> str:
    """Ex.: 2h 15min (título do modal)."""
    if seconds is None or seconds < 0:
        return "—"
    if seconds < 3600:
        return f"{max(1, seconds // 60)} min"
    if seconds < 86400:
        h = seconds // 3600
        m = (seconds % 3600) // 60
        return f"{h}h {m}min"
    days = seconds // 86400
    rem = seconds % 86400
    h = rem // 3600
    m = (rem % 3600) // 60
    return f"{days}d {h}h {m}min"


def effective_duration_sec(t: dict[str, Any]) -> int | None:
    if t.get("duracao_segundos") is not None:
        return int(t["duracao_segundos"])
    if t.get("ts_saida") is not None and t.get("ts_entrada") is not None:
        return max(0, int((int(t["ts_saida"]) - int(t["ts_entrada"])) // 1000))
    return None


def initial_modal_figure() -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        template=TPL_DARK,
        height=300,
        paper_bgcolor="#161b22",
        plot_bgcolor="#0d1117",
    )
    return fig


def empty_insufficient_figure() -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        template=TPL_DARK,
        height=440,
        paper_bgcolor="#161b22",
        plot_bgcolor="#0d1117",
        annotations=[
            dict(
                text="Dados históricos insuficientes para este trade",
                showarrow=False,
                x=0.5,
                y=0.5,
                xref="paper",
                yref="paper",
                font=dict(size=14, color="#8b949e"),
            )
        ],
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
    )
    return fig


def _resample_rule(span_sec: float, n: int) -> str:
    if span_sec <= 6 * 3600:
        return "15min"
    if span_sec <= 48 * 3600:
        return "1h"
    if span_sec <= 30 * 86400:
        return "4h"
    return "1d"


def _closes_to_ohlc(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    if df.empty or len(df) < 2:
        return pd.DataFrame()
    d = df.sort_values("timestamp").set_index("timestamp")
    o = d["close"].resample(rule, label="right", closed="right").first()
    h = d["close"].resample(rule, label="right", closed="right").max()
    low = d["close"].resample(rule, label="right", closed="right").min()
    c = d["close"].resample(rule, label="right", closed="right").last()
    out = pd.DataFrame({"open": o, "high": h, "low": low, "close": c})
    out = out.dropna(how="all").dropna(subset=["close"])
    out = out.reset_index()
    out = out.rename(columns={out.columns[0]: "timestamp"})
    return out


def build_modal_title(sym: str, trade_id: int, pl_pct: float | None, dur_sec: int | None) -> str:
    pl = f"{pl_pct:+.1f}%" if pl_pct is not None else "—"
    dur = format_duration_title(dur_sec)
    return f"{sym}/EUR — Trade #{trade_id} | {pl} | {dur}"


def build_trade_detail_figure(trade: dict[str, Any], sym: str) -> go.Figure:
    """
    trade: linha de `fetch_trade_by_id` (fechado).
    sym: ticker ex. UNI
    """
    if trade.get("ts_saida") is None or trade.get("ts_entrada") is None:
        return empty_insufficient_figure()

    cid = str(trade["ativo"])
    ts_e = int(trade["ts_entrada"])
    ts_x = int(trade["ts_saida"])
    win_from = ts_e - PAD_MS
    win_to = ts_x + PAD_MS

    try:
        merged = merge_trade_window_prices(cid, win_from, win_to)
    except Exception:
        logger.exception("merge_trade_window_prices")
        return empty_insufficient_figure()

    if merged.empty or len(merged) < MIN_POINTS:
        return empty_insufficient_figure()

    t0 = merged["timestamp"].iloc[0]
    t1 = merged["timestamp"].iloc[-1]
    span_sec = float((t1 - t0).total_seconds())
    if span_sec <= 0:
        span_sec = float(max(1, len(merged)) * 60.0)
    rule = _resample_rule(span_sec, len(merged))
    ohlc = _closes_to_ohlc(merged, rule)
    if ohlc.empty or len(ohlc) < 2:
        return empty_insufficient_figure()

    preco_entrada = float(trade["preco_entrada"])
    sl = preco_entrada * (1.0 - config.STOP_LOSS_PCT / 100.0)
    tp = preco_entrada * (1.0 + config.TAKE_PROFIT_PCT / 100.0)
    pl_eur = trade.get("pl_eur")
    is_profit = pl_eur is not None and float(pl_eur) >= 0.0
    fill_rgba = "rgba(63, 185, 80, 0.12)" if is_profit else "rgba(248, 81, 73, 0.12)"

    # Plotly add_vline / vrect com pandas Timestamp pode falhar — usar datetime nativo
    x_ent = datetime.fromtimestamp(ts_e / 1000.0, tz=timezone.utc)
    x_ex = datetime.fromtimestamp(ts_x / 1000.0, tz=timezone.utc)

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.06,
        row_heights=[0.62, 0.38],
        subplot_titles=("Preço (EUR)", "Stochastic RSI (14, 3, 3)"),
    )

    ts_c = pd.to_datetime(ohlc["timestamp"], utc=True)
    cx_plot = [t.to_pydatetime() for t in ts_c]

    fig.add_trace(
        go.Candlestick(
            x=cx_plot,
            open=ohlc["open"],
            high=ohlc["high"],
            low=ohlc["low"],
            close=ohlc["close"],
            name="OHLC",
            increasing_line_color="#3fb950",
            decreasing_line_color="#f85149",
        ),
        row=1,
        col=1,
    )

    fig.add_hline(
        y=preco_entrada,
        line_dash="solid",
        line_color="#d29922",
        annotation_text="Entrada",
        annotation_position="right",
        row=1,
        col=1,
    )
    fig.add_hline(
        y=sl,
        line_dash="dot",
        line_color="#f85149",
        annotation_text=f"SL −{config.STOP_LOSS_PCT:.0f}%",
        annotation_position="right",
        row=1,
        col=1,
    )
    fig.add_hline(
        y=tp,
        line_dash="dot",
        line_color="#3fb950",
        annotation_text=f"TP +{config.TAKE_PROFIT_PCT:.0f}%",
        annotation_position="right",
        row=1,
        col=1,
    )

    fig.add_shape(
        type="rect",
        x0=x_ent,
        x1=x_ex,
        y0=0,
        y1=1,
        yref="y domain",
        xref="x domain",
        fillcolor=fill_rgba,
        layer="below",
        line_width=0,
        row=1,
        col=1,
    )
    fig.add_shape(
        type="line",
        x0=x_ent,
        x1=x_ent,
        y0=0,
        y1=1,
        yref="y domain",
        xref="x domain",
        line=dict(color="#3fb950", width=2, dash="dash"),
        layer="above",
        row=1,
        col=1,
    )
    fig.add_shape(
        type="line",
        x0=x_ex,
        x1=x_ex,
        y0=0,
        y1=1,
        yref="y domain",
        xref="x domain",
        line=dict(color="#f85149", width=2, dash="dash"),
        layer="above",
        row=1,
        col=1,
    )

    fig.update_yaxes(title_text="€", row=1, col=1, gridcolor="#21262d")
    fig.update_xaxes(gridcolor="#21262d", row=1, col=1)

    # Stochastic RSI
    try:
        ind = compute_stochastic_rsi(merged.copy())
        stoch_ok = (
            not ind.empty
            and "stoch_k" in ind.columns
            and ind["stoch_k"].notna().sum() >= 3
        )
    except Exception:
        stoch_ok = False
        ind = merged

    if stoch_ok:
        ts_k = pd.to_datetime(ind["timestamp"], utc=True)
        sx = [t.to_pydatetime() for t in ts_k]
        fig.add_trace(
            go.Scatter(
                x=sx,
                y=ind["stoch_k"],
                name="%K",
                line=dict(color="#3fb950", width=1.5),
            ),
            row=2,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=sx,
                y=ind["stoch_d"],
                name="%D",
                line=dict(color="#f85149", width=1.5),
            ),
            row=2,
            col=1,
        )
        fig.add_hline(y=20, line_dash="dot", line_color="#484f58", row=2, col=1)
        fig.add_hline(y=80, line_dash="dot", line_color="#484f58", row=2, col=1)
        fig.update_yaxes(range=[0, 100], title_text="%", row=2, col=1, gridcolor="#21262d")
    else:
        xm = merged["timestamp"].median()
        fig.add_trace(
            go.Scatter(
                x=[xm],
                y=[50],
                mode="text",
                text=["Stochastic RSI indisponível (série curta)"],
                textposition="middle center",
                showlegend=False,
                textfont=dict(color="#8b949e", size=12),
            ),
            row=2,
            col=1,
        )
        fig.update_yaxes(range=[0, 100], title_text="%", row=2, col=1, gridcolor="#21262d")

    fig.update_xaxes(gridcolor="#21262d", row=2, col=1)
    fig.update_layout(
        template=TPL_DARK,
        height=560,
        margin=dict(l=50, r=30, t=40, b=30),
        paper_bgcolor="#161b22",
        plot_bgcolor="#0d1117",
        hovermode="x unified",
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
        xaxis_rangeslider_visible=False,
    )
    return fig
