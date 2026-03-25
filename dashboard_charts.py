"""
Figuras Plotly para o dashboard: ativos (linha/velas + Stoch) e curva de equity.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import config
import database as db
from data_feed import fetch_ohlc_eur
from indicators import compute_stochastic_rsi

logger = logging.getLogger(__name__)

TPL = "plotly_dark"
COLOR_MA = "#d29922"
COLOR_K = "#ff9500"
COLOR_D = "#ffffff"


def _ts_ms_to_dt(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)


def _clip_days(df: pd.DataFrame, days: int) -> pd.DataFrame:
    if df.empty or "timestamp" not in df.columns:
        return df
    cut = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=days)
    return df[df["timestamp"] >= cut].copy()


def _markers_from_db(coin_id: str) -> tuple[list[datetime], list[float], list[datetime], list[float]]:
    m = db.fetch_chart_trade_markers(coin_id)
    bx = [_ts_ms_to_dt(x["ts"]) for x in m["buy"]]
    by = [x["price"] for x in m["buy"]]
    sx = [_ts_ms_to_dt(x["ts"]) for x in m["sell"]]
    sy = [x["price"] for x in m["sell"]]
    return bx, by, sx, sy


def _stoch_subplot(fig: go.Figure, df: pd.DataFrame, row: int, col: int = 1) -> None:
    if df.empty or "stoch_k" not in df.columns:
        return
    tail = df.dropna(subset=["stoch_k"]).tail(500)
    if tail.empty:
        return
    xplot = [t.to_pydatetime() for t in pd.to_datetime(tail["timestamp"], utc=True)]
    fig.add_hrect(y0=0, y1=20, fillcolor="rgba(63, 185, 80, 0.15)", line_width=0, row=row, col=col)
    fig.add_hrect(y0=80, y1=100, fillcolor="rgba(248, 81, 73, 0.15)", line_width=0, row=row, col=col)
    fig.add_trace(
        go.Scatter(x=xplot, y=tail["stoch_k"], name="%K", line=dict(color=COLOR_K, width=1.5)),
        row=row,
        col=col,
    )
    fig.add_trace(
        go.Scatter(x=xplot, y=tail["stoch_d"], name="%D", line=dict(color=COLOR_D, width=1.5)),
        row=row,
        col=col,
    )
    fig.add_hline(y=20, line_dash="dash", line_color="#8b949e", opacity=0.8, row=row, col=col)
    fig.add_hline(y=80, line_dash="dash", line_color="#8b949e", opacity=0.8, row=row, col=col)
    fig.update_yaxes(range=[0, 100], title_text="RSI", row=row, col=col)


def build_asset_combined_figure(
    df_ind: pd.DataFrame,
    coin_id: str,
    sym: str,
    mode: str,
    days_key: str,
) -> go.Figure:
    """
    mode: 'line' | 'candle'
    days_key: '7d' | '30d'
    Um único Figure com painel principal (70%) + Stoch (30%).
    """
    days = 7 if days_key == "7d" else 30
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.06,
        row_heights=[0.7, 0.3],
        subplot_titles=(f"{sym}/EUR — preço", "Stochastic RSI (14, 3, 3)"),
    )

    fig.update_layout(
        template=TPL,
        height=480,
        margin=dict(l=50, r=24, t=48, b=36),
        paper_bgcolor="#161b22",
        plot_bgcolor="#0d1117",
        hovermode="x unified",
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.05, x=0),
    )

    bx, by, sx, sy = _markers_from_db(coin_id)

    if mode == "line":
        d = _clip_days(df_ind, days) if not df_ind.empty else df_ind
        if d.empty or "close" not in d.columns:
            fig.add_annotation(
                text="A aguardar dados...",
                xref="paper",
                yref="paper",
                x=0.5,
                y=0.65,
                showarrow=False,
                font=dict(size=14, color="#8b949e"),
            )
            _stoch_subplot(fig, df_ind, row=2)
            fig.update_xaxes(gridcolor="#21262d", row=1, col=1)
            fig.update_yaxes(gridcolor="#21262d", title="€", row=1, col=1)
            return fig

        xline = [t.to_pydatetime() for t in pd.to_datetime(d["timestamp"], utc=True)]
        fig.add_trace(
            go.Scatter(
                x=xline,
                y=d["close"],
                name="Preço",
                mode="lines",
                line=dict(color="#3fb950", width=2),
                fill="tozeroy",
                fillcolor="rgba(63, 185, 80, 0.08)",
            ),
            row=1,
            col=1,
        )
        if "ma7" in d.columns:
            fig.add_trace(
                go.Scatter(
                    x=xline,
                    y=d["ma7"],
                    name="MA 7d",
                    mode="lines",
                    line=dict(color=COLOR_MA, width=2, dash="dash"),
                ),
                row=1,
                col=1,
            )
        if bx and by:
            fig.add_trace(
                go.Scatter(
                    x=bx,
                    y=by,
                    name="Compra",
                    mode="markers",
                    marker=dict(symbol="triangle-up", size=12, color="#3fb950", line=dict(width=0)),
                    text=["▲"] * len(bx),
                ),
                row=1,
                col=1,
            )
        if sx and sy:
            fig.add_trace(
                go.Scatter(
                    x=sx,
                    y=sy,
                    name="Venda",
                    mode="markers",
                    marker=dict(symbol="triangle-down", size=12, color="#f85149", line=dict(width=0)),
                    text=["▼"] * len(sx),
                ),
                row=1,
                col=1,
            )
        _stoch_subplot(fig, d if "stoch_k" in d.columns else df_ind, row=2)

    else:
        ohlc_days = 7 if days_key == "7d" else 30
        try:
            ohlc = fetch_ohlc_eur(coin_id, days=ohlc_days)
        except Exception:
            logger.exception("ohlc %s", coin_id)
            ohlc = pd.DataFrame()

        if ohlc.empty or len(ohlc) < 2:
            fig.add_annotation(
                text="A aguardar dados...",
                xref="paper",
                yref="paper",
                x=0.5,
                y=0.65,
                showarrow=False,
                font=dict(size=14, color="#8b949e"),
            )
            ind2 = compute_stochastic_rsi(df_ind.copy()) if not df_ind.empty else df_ind
            _stoch_subplot(fig, ind2, row=2)
            fig.update_yaxes(title="€", row=1, col=1)
            return fig

        ohlc = ohlc.sort_values("timestamp").reset_index(drop=True)
        ohlc["ma7"] = ohlc["close"].rolling(7, min_periods=1).mean()
        cx = [t.to_pydatetime() for t in pd.to_datetime(ohlc["timestamp"], utc=True)]

        fig.add_trace(
            go.Candlestick(
                x=cx,
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
        fig.add_trace(
            go.Scatter(
                x=cx,
                y=ohlc["ma7"],
                name="MA 7d",
                mode="lines",
                line=dict(color=COLOR_MA, width=2, dash="dash"),
            ),
            row=1,
            col=1,
        )
        if bx and by:
            fig.add_trace(
                go.Scatter(
                    x=bx,
                    y=by,
                    name="Compra",
                    mode="markers",
                    marker=dict(symbol="triangle-up", size=11, color="#3fb950"),
                ),
                row=1,
                col=1,
            )
        if sx and sy:
            fig.add_trace(
                go.Scatter(
                    x=sx,
                    y=sy,
                    name="Venda",
                    mode="markers",
                    marker=dict(symbol="triangle-down", size=11, color="#f85149"),
                ),
                row=1,
                col=1,
            )

        stoch_df = compute_stochastic_rsi(
            pd.DataFrame(
                {
                    "timestamp": ohlc["timestamp"],
                    "close": ohlc["close"],
                }
            )
        )
        _stoch_subplot(fig, stoch_df, row=2)

    fig.update_xaxes(gridcolor="#21262d", row=1, col=1)
    fig.update_yaxes(gridcolor="#21262d", title="€", row=1, col=1)
    fig.update_xaxes(gridcolor="#21262d", row=2, col=1)
    fig.update_layout(xaxis_rangeslider_visible=False, xaxis2_rangeslider_visible=False)
    return fig


def build_equity_figure_and_stats() -> tuple[go.Figure, Any]:
    """Retorna figura + div de estatísticas."""
    from dash import html

    df = db.fetch_equity_history_df(20_000)
    saldo_ini = config.SALDO_INICIAL
    lim = config.LIMITE_SALDO_MINIMO

    if df.empty:
        fig = go.Figure()
        fig.update_layout(
            template=TPL,
            height=320,
            paper_bgcolor="#161b22",
            plot_bgcolor="#0d1117",
            annotations=[
                dict(
                    text="Sem dados ainda — o agente regista o património a cada ciclo.",
                    showarrow=False,
                    x=0.5,
                    y=0.5,
                    xref="paper",
                    yref="paper",
                    font=dict(color="#8b949e", size=14),
                )
            ],
        )
        stats = html.Div(
            "Retorno: — · Máximo: — · Drawdown máx.: —",
            className="small text-muted mb-2",
        )
        return fig, stats

    df = df.sort_values("timestamp").reset_index(drop=True)
    x = [datetime.fromtimestamp(r["timestamp"] / 1000.0, tz=timezone.utc) for _, r in df.iterrows()]
    y = df["patrimonio_total"].astype(float)

    last = float(y.iloc[-1])
    ret_pct = (last / saldo_ini - 1.0) * 100.0 if saldo_ini else 0.0
    peak = float(y.max())
    # drawdown máximo (histórico): min (valor - peak até esse ponto) / peak
    cummax = y.cummax()
    dd_series = (y - cummax) / cummax * 100.0
    max_dd = float(dd_series.min()) if len(dd_series) else 0.0

    line_color = "#3fb950" if last >= saldo_ini else "#f85149"
    fill_col = "rgba(63, 185, 80, 0.18)" if last >= saldo_ini else "rgba(248, 81, 73, 0.15)"

    fig = go.Figure()
    base_y = [saldo_ini] * len(x)
    fig.add_trace(
        go.Scatter(
            x=x,
            y=base_y,
            mode="lines",
            line=dict(width=0),
            showlegend=False,
            hoverinfo="skip",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=x,
            y=y,
            name="Património",
            mode="lines",
            line=dict(color=line_color, width=2),
            fill="tonexty",
            fillcolor=fill_col,
        )
    )
    fig.add_hline(y=saldo_ini, line_dash="dash", line_color="#8b949e", annotation_text=f"Inicial {saldo_ini:,.0f} €")
    fig.add_hline(y=lim, line_dash="dash", line_color="#f85149", annotation_text=f"Mínimo {lim:,.0f} €")

    fig.update_layout(
        template=TPL,
        height=320,
        margin=dict(l=50, r=30, t=40, b=40),
        paper_bgcolor="#161b22",
        plot_bgcolor="#0d1117",
        yaxis=dict(title="€", gridcolor="#21262d"),
        xaxis=dict(gridcolor="#21262d"),
        hovermode="x unified",
        title=dict(text="Evolução do património total (EUR)", font=dict(size=14)),
    )

    stats = html.Div(
        [
            html.Span(f"Retorno total: {ret_pct:+.2f}%", className="me-3"),
            html.Span(f"Máximo património: {_fmt_eur_static(peak)}", className="me-3"),
            html.Span(f"Drawdown máximo: {max_dd:.2f}%", className="text-danger" if max_dd < -0.01 else ""),
        ],
        className="small text-light mb-2 d-flex flex-wrap gap-2",
    )
    return fig, stats


def _fmt_eur_static(x: float) -> str:
    return f"{x:,.2f} €"
