"""
Dashboard Dash — tema escuro, estilo operacional (paper trading UNI / PENDLE).
"""
from __future__ import annotations

import errno
import os
import traceback
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Pasta deste ficheiro — obrigatório para o Dash encontrar `assets/` em qualquer cwd
_DASH_DIR = Path(__file__).resolve().parent

import dash_bootstrap_components as dbc
import database as db
import pandas as pd
import plotly.graph_objects as go
import dash
from dash import ALL, Dash, Input, Output, State, dash_table, dcc, html, no_update
from dash.exceptions import PreventUpdate
import app_state
import config
import trade_chart
import dashboard_charts
from data_feed import fetch_simple_prices_eur
from indicators import compute_all_indicators

logger = logging.getLogger(__name__)

AVISO_SALDO_EUR: float = 7500.0
CID_TO_SYM: dict[str, str] = {v: k for k, v in config.ASSETS.items()}
TPL_DARK = "plotly_dark"
COLOR_UNI = "#ff6b35"
COLOR_PENDLE = "#6366f1"
NOME_ATIVO: dict[str, str] = {"UNI": "Uniswap", "PENDLE": "Pendle"}


def _fmt_eur(x: float | None) -> str:
    if x is None:
        return "—"
    return f"{x:,.2f} €"


def _fmt_pct(x: float | None) -> str:
    if x is None:
        return "—"
    return f"{x:+.2f} %"


def _fmt_compact_eur(n: float | None) -> str:
    if n is None:
        return "—"
    if abs(n) >= 1e12:
        return f"{n / 1e12:.2f}×10¹² €"
    if abs(n) >= 1e9:
        return f"{n / 1e9:.2f} mil M€"
    if abs(n) >= 1e6:
        return f"{n / 1e6:.2f} M€"
    if abs(n) >= 1e3:
        return f"{n / 1e3:.2f} k€"
    return f"{n:.2f} €"


def _merge_indicator_frames(cached: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    out: dict[str, pd.DataFrame] = {}
    for _sym, cid in config.ASSETS.items():
        df = cached.get(cid)
        if df is not None and not df.empty and "stoch_k" in df.columns:
            out[cid] = df
            continue
        hist = db.load_prices_from_db(cid)
        if hist.empty:
            out[cid] = pd.DataFrame()
            continue
        try:
            out[cid] = compute_all_indicators(hist)
        except Exception:
            logger.exception("Indicadores %s", cid)
            out[cid] = pd.DataFrame()
    return out


def _resolve_precos(snap_prices: dict[str, float]) -> dict[str, float]:
    if snap_prices and len(snap_prices) >= len(config.ASSETS):
        return {k: float(v) for k, v in snap_prices.items() if k in config.ASSETS.values()}
    try:
        api = fetch_simple_prices_eur()
        return {cid: float(api[cid]) for cid in config.ASSETS.values() if cid in api}
    except Exception:
        return {k: float(v) for k, v in snap_prices.items()}


def _dashboard_precos_only(snap_prices: dict[str, float]) -> dict[str, float]:
    """
    Preços só a partir do snapshot do agente — evita chamadas CoinGecko no callback
    (esperas longas por 429 deixam o painel em branco / timeout).
    """
    return {k: float(v) for k, v in snap_prices.items() if k in config.ASSETS.values()}


def _asset_header_meta(sym: str, cid: str, px: float | None, color: str) -> Any:
    letter = sym[0].upper()
    return html.Div(
        className="asset-card-head pb-2",
        children=[
            dbc.Row(
                [
                    dbc.Col(
                        html.Div(
                            letter,
                            className="avatar-coin",
                            style={"backgroundColor": color, "color": "#0d1117"},
                        ),
                        width="auto",
                    ),
                    dbc.Col(
                        [
                            html.Div(f"{NOME_ATIVO.get(sym, sym)} ({sym})", className="fw-bold"),
                            html.Div(cid, className="small text-muted"),
                        ],
                        width=True,
                    ),
                    dbc.Col(
                        html.Div(_fmt_eur(px) if px is not None else "—", className="price-big text-end"),
                        width="auto",
                    ),
                ],
                align="center",
            ),
        ],
    )


def _build_dynamic_main() -> tuple[Any, Any]:
    st = app_state.state
    snap = st.snapshot_ui()
    p = st.portfolio

    precos = _dashboard_precos_only(snap["last_prices"])
    # Não chamar coins/markets aqui: em 429 o cliente espera minutos e o callback pode falhar (UI vazia).
    markets: dict[str, dict[str, Any]] = {}

    st_total = p.saldo_total(precos)
    pl_total = p.pl_total(precos)
    var_pct = ((st_total - config.SALDO_INICIAL) / config.SALDO_INICIAL * 100.0) if config.SALDO_INICIAL else 0.0

    fechados = db.fetch_closed_trades(500)
    n_ops = len(fechados)
    wins = sum(1 for t in fechados if t.get("pl_eur") is not None and float(t["pl_eur"]) > 0)
    win_rate = (wins / n_ops * 100.0) if n_ops else 0.0
    pls = [float(t["pl_eur"]) for t in fechados if t.get("pl_eur") is not None]
    max_loss = min(pls) if pls else None

    badge = dbc.Badge(
        "● Activo",
        color="success",
        className="fs-6 me-2",
        pill=True,
    )
    if not snap["agent_running"] or p.agente_parado:
        badge = dbc.Badge("● Parado", color="secondary", className="fs-6 me-2", pill=True)

    alerta: Any = html.Div()
    if st_total < AVISO_SALDO_EUR:
        alerta = dbc.Alert(
            f"Património abaixo do aviso ({AVISO_SALDO_EUR:,.0f} €): {st_total:,.2f} €",
            color="warning",
            className="py-2",
        )

    report = dbc.Row(
        className="g-2 mb-4",
        children=[
            dbc.Col(
                html.Div(
                    [html.Div("Total de operações", className="label"), html.Div(str(n_ops), className="value")],
                    className="metric-tile",
                ),
                md=3,
            ),
            dbc.Col(
                html.Div(
                    [
                        html.Div("Win rate (fechadas)", className="label"),
                        html.Div(f"{win_rate:.1f} %" if n_ops else "—", className="value"),
                    ],
                    className="metric-tile",
                ),
                md=3,
            ),
            dbc.Col(
                html.Div(
                    [
                        html.Div("Lucro / Perda total", className="label"),
                        html.Div(_fmt_eur(pl_total), className="value text-success" if pl_total >= 0 else "value text-danger"),
                    ],
                    className="metric-tile",
                ),
                md=3,
            ),
            dbc.Col(
                html.Div(
                    [
                        html.Div("Maior perda (trade)", className="label"),
                        html.Div(_fmt_eur(max_loss) if max_loss is not None else "—", className="value text-danger"),
                    ],
                    className="metric-tile",
                ),
                md=3,
            ),
        ],
    )

    paper = html.Div(
        [
            html.H5("Paper trading (EUR)", className="mt-2 mb-3"),
            html.P(
                f"Saldo inicial simulado: {config.SALDO_INICIAL:,.2f} € · Preços CoinGecko em tempo quase real.",
                className="small text-muted mb-3",
            ),
            dbc.Row(
                className="g-2 mb-4",
                children=[
                    dbc.Col(
                        html.Div(
                            [html.Div("Saldo EUR", className="label"), html.Div(_fmt_eur(p.saldo_disponivel), className="value")],
                            className="metric-tile",
                        ),
                        md=3,
                    ),
                    dbc.Col(
                        html.Div(
                            [
                                html.Div("Valor total carteira", className="label"),
                                html.Div(_fmt_eur(st_total), className="value"),
                            ],
                            className="metric-tile",
                        ),
                        md=3,
                    ),
                    dbc.Col(
                        html.Div(
                            [
                                html.Div("Lucro / Perda total", className="label"),
                                html.Div(
                                    _fmt_eur(pl_total),
                                    className="value text-success" if pl_total >= 0 else "value text-danger",
                                ),
                            ],
                            className="metric-tile",
                        ),
                        md=3,
                    ),
                    dbc.Col(
                        html.Div(
                            [
                                html.Div("Variação %", className="label"),
                                html.Div(_fmt_pct(var_pct), className="value"),
                            ],
                            className="metric-tile",
                        ),
                        md=3,
                    ),
                ],
            ),
        ]
    )

    rows_pos: list[dict[str, Any]] = []
    for ativo, pos in p.posicoes.items():
        px = precos.get(ativo)
        if px is None:
            continue
        val = pos.quantidade * px
        pl_pct_pos = (px - pos.preco_entrada) / pos.preco_entrada * 100.0 if pos.preco_entrada else 0.0
        rows_pos.append(
            {
                "Ativo": CID_TO_SYM.get(ativo, ativo),
                "Quantidade": round(pos.quantidade, 8),
                "Preço médio": round(pos.preco_entrada, 6),
                "Valor atual": round(val, 2),
                "P&L %": round(pl_pct_pos, 2),
            }
        )

    tab_pos: Any
    if rows_pos:
        tab_pos = dash_table.DataTable(
            columns=[{"name": c, "id": c} for c in rows_pos[0].keys()],
            data=rows_pos,
            style_table={"overflowX": "auto"},
            style_cell={"padding": "10px", "backgroundColor": "#161b22", "color": "#e6edf3", "border": "1px solid #30363d"},
            style_header={"backgroundColor": "#21262d", "fontWeight": "bold"},
            style_data_conditional=[
                {"if": {"column_id": "P&L %", "filter_query": "{P&L %} < 0"}, "color": "#f85149"},
                {"if": {"column_id": "P&L %", "filter_query": "{P&L %} >= 0"}, "color": "#3fb950"},
            ],
        )
    else:
        tab_pos = html.P("Sem posições abertas.", className="text-muted")

    rows_hist: list[Any] = []
    durations_known: list[int] = []
    for t in fechados:
        dur_sec = trade_chart.effective_duration_sec(t)
        if dur_sec is not None:
            durations_known.append(dur_sec)
        dur_txt = trade_chart.format_duration_human(dur_sec)
        sym_h = CID_TO_SYM.get(t["ativo"], t["ativo"])
        rows_hist.append(
            html.Tr(
                [
                    html.Td(
                        datetime.fromtimestamp(t["ts_saida"] / 1000.0, tz=timezone.utc).strftime(
                            "%Y-%m-%d %H:%M"
                        )
                    ),
                    html.Td("Venda"),
                    html.Td(sym_h),
                    html.Td(f"{float(t['quantidade']):.8f}"),
                    html.Td(f"{float(t['preco_saida']):.6f}"),
                    html.Td(f"{float(t['quantidade']) * float(t['preco_saida']):.2f}"),
                    html.Td(t.get("motivo_saida") or "—"),
                    html.Td(dur_txt),
                    html.Td(
                        dbc.Button(
                            "Ver Gráfico 📊",
                            id={"type": "trade-chart-btn", "tid": int(t["id"])},
                            size="sm",
                            color="success",
                            outline=True,
                            className="text-nowrap",
                        )
                    ),
                ]
            )
        )

    footer_hist: Any = html.Div()
    if durations_known:
        avg_sec = int(round(sum(durations_known) / len(durations_known)))
        footer_hist = html.Div(
            f"Duração média: {trade_chart.format_duration_human(avg_sec)}",
            className="small text-muted mt-2",
        )

    if rows_hist:
        tab_hist = html.Div(
            [
                html.Div(
                    className="table-responsive",
                    children=[
                        html.Table(
                            [
                                html.Thead(
                                    html.Tr(
                                        [
                                            html.Th("Data"),
                                            html.Th("Tipo"),
                                            html.Th("Ativo"),
                                            html.Th("Qtd"),
                                            html.Th("Preço"),
                                            html.Th("Total €"),
                                            html.Th("Motivo"),
                                            html.Th("Duração"),
                                            html.Th(""),
                                        ]
                                    )
                                ),
                                html.Tbody(rows_hist),
                            ],
                            className="table table-dark table-sm table-bordered align-middle mb-0",
                            style={"borderColor": "#30363d", "width": "100%"},
                        )
                    ],
                ),
                footer_hist,
            ]
        )
    else:
        tab_hist = html.P("Nenhuma operação fechada ainda.", className="text-muted")

    body = html.Div(
        [
            html.H5("Relatório de operações", className="mb-3"),
            report,
            paper,
            alerta,
            html.H5("Posições", className="mt-4 mb-2"),
            tab_pos,
            html.H5("Histórico de operações", className="mt-4 mb-2"),
            tab_hist,
        ]
    )

    return body, badge


def create_app() -> Dash:
    app = Dash(
        __name__,
        external_stylesheets=[dbc.themes.CYBORG],
        assets_folder=str(_DASH_DIR / "assets"),
        suppress_callback_exceptions=True,
    )
    app.title = "Agente trading — Paper (EUR)"

    app.layout = html.Div(
        className="dark-page",
        children=[
            dcc.Interval(id="interval-atualiza", interval=max(5, config.CICLO_SEGUNDOS) * 1000, n_intervals=0),
            dcc.Store(id="store-chart-mode-uni", data="line", storage_type="local"),
            dcc.Store(id="store-chart-mode-pendle", data="line", storage_type="local"),
            dcc.Store(id="store-modal-open", data=False),
            dbc.Container(
                fluid=True,
                style={"maxWidth": "1280px"},
                children=[
                    dbc.Row(
                        className="align-items-center py-3 mb-2",
                        children=[
                            dbc.Col(
                                html.Span(
                                    f"Atualização automática a cada {config.CICLO_SEGUNDOS} segundos",
                                    className="top-bar-muted",
                                ),
                                width=True,
                            ),
                            dbc.Col(html.Span(id="txt-relogio", className="top-bar-muted text-end"), width="auto"),
                        ],
                    ),
                    dbc.Row(
                        className="align-items-center mb-3",
                        children=[
                            dbc.Col(
                                [
                                    html.Div(
                                        "Painel UNI / PENDLE · tema escuro (v2)",
                                        className="text-success small mb-1 fw-semibold",
                                    ),
                                    html.H2("Agente de trading automático", className="mb-2 fw-bold"),
                                    html.P(
                                        "Modo paper trading em EUR (CoinGecko). O agente avalia sinais a cada "
                                        f"{config.CICLO_SEGUNDOS}s — sem ligação a exchanges.",
                                        className="text-muted small mb-0",
                                    ),
                                ],
                                md=8,
                            ),
                            dbc.Col(
                                dbc.ButtonGroup(
                                    [
                                        dbc.Button("Desactivar", id="btn-parar", color="danger", outline=False, size="sm"),
                                        dbc.Button("Activar", id="btn-iniciar", color="success", outline=False, size="sm"),
                                    ]
                                ),
                                className="text-md-end",
                                md=4,
                            ),
                        ],
                    ),
                    html.Div(id="badge-estado", className="mb-3"),
                    dbc.Row(
                        className="g-3 mb-4",
                        children=[
                            dbc.Col(
                                dbc.Card(
                                    className="card-rule h-100",
                                    children=[
                                        dbc.CardHeader(html.H5("Regras de gestão de risco", className="mb-0 fs-6")),
                                        dbc.CardBody(
                                            html.Ul(
                                                [
                                                    html.Li(f"Máximo {config.MAX_PCT_POR_OPERACAO:.0%} do saldo por operação."),
                                                    html.Li(f"No máximo {config.MAX_POSICOES} posições abertas em simultâneo."),
                                                    html.Li(
                                                        f"Paragem automática se o património total descer abaixo de {config.LIMITE_SALDO_MINIMO:,.0f} €."
                                                    ),
                                                ],
                                                className="small mb-0 ps-3",
                                            )
                                        ),
                                    ],
                                ),
                                md=6,
                            ),
                            dbc.Col(
                                dbc.Card(
                                    className="card-rule h-100",
                                    children=[
                                        dbc.CardHeader(html.H5("Regras de entrada / saída (Stochastic RSI 14,3,3)", className="mb-0 fs-6")),
                                        dbc.CardBody(
                                            html.Div(
                                                [
                                                    html.P(
                                                        [
                                                            html.Strong("Compra: "),
                                                            "%K cruza acima de %D, ambos < 20 e preço > MA 7 dias.",
                                                        ],
                                                        className="small mb-2",
                                                    ),
                                                    html.P(
                                                        [
                                                            html.Strong("Venda: "),
                                                            "cruzamento em sobrecomprado, stop-loss ",
                                                            f"{config.STOP_LOSS_PCT:.0f}%",
                                                            " ou take-profit ",
                                                            f"{config.TAKE_PROFIT_PCT:.0f}%",
                                                            ".",
                                                        ],
                                                        className="small mb-2",
                                                    ),
                                                    html.P(
                                                        "Ativos: Uniswap (UNI) e Pendle (PENDLE).",
                                                        className="small text-muted mb-0",
                                                    ),
                                                ]
                                            )
                                        ),
                                    ],
                                ),
                                md=6,
                            ),
                        ],
                    ),
                    html.Div(
                        className="mb-2 d-flex align-items-center gap-3 flex-wrap",
                        children=[
                            html.Span("Intervalo gráfico:", className="small text-muted"),
                            dcc.RadioItems(
                                id="range-charts",
                                options=[
                                    {"label": " 7 dias ", "value": "7d"},
                                    {"label": " 30 dias ", "value": "30d"},
                                ],
                                value="30d",
                                inline=True,
                                className="text-light",
                                inputStyle={"marginRight": "6px", "marginLeft": "12px"},
                                labelStyle={"display": "inline-block"},
                            ),
                        ],
                    ),
                    html.H5("Evolução do Portfólio", className="mt-4 mb-2"),
                    html.Div(id="equity-stats-bar", className="mb-2"),
                    dcc.Graph(
                        id="equity-graph",
                        figure=go.Figure(layout=dict(template=TPL_DARK, height=320)),
                        config={"displayModeBar": True, "scrollZoom": True, "responsive": True},
                        style={"height": "340px", "width": "100%", "maxWidth": "100%"},
                    ),
                    html.H5("Mercado — UNI & PENDLE", className="mt-4 mb-3"),
                    dbc.Row(
                        className="g-3 mb-4",
                        children=[
                            dbc.Col(
                                dbc.Card(
                                    className="asset-card-dash mb-3",
                                    children=[
                                        html.Div(id="asset-meta-uni"),
                                        html.Div(
                                            className="mb-2 chart-mode-selector",
                                            children=[
                                                dbc.ButtonGroup(
                                                    [
                                                        dbc.Button(
                                                            "📈 Linha",
                                                            id="btn-linha-uni",
                                                            n_clicks=0,
                                                            color="warning",
                                                            outline=False,
                                                            size="sm",
                                                            className="px-3",
                                                        ),
                                                        dbc.Button(
                                                            "🕯️ Candlestick",
                                                            id="btn-candle-uni",
                                                            n_clicks=0,
                                                            color="secondary",
                                                            outline=True,
                                                            size="sm",
                                                            className="px-3",
                                                        ),
                                                    ],
                                                ),
                                            ],
                                        ),
                                        dcc.Graph(
                                            id="graph-asset-uni",
                                            figure=go.Figure(layout=dict(template=TPL_DARK, height=480)),
                                            config={"displayModeBar": True, "scrollZoom": True, "responsive": True},
                                            style={"height": "520px", "width": "100%"},
                                        ),
                                    ],
                                ),
                                lg=6,
                                md=12,
                            ),
                            dbc.Col(
                                dbc.Card(
                                    className="asset-card-dash mb-3",
                                    children=[
                                        html.Div(id="asset-meta-pendle"),
                                        html.Div(
                                            className="mb-2 chart-mode-selector",
                                            children=[
                                                dbc.ButtonGroup(
                                                    [
                                                        dbc.Button(
                                                            "📈 Linha",
                                                            id="btn-linha-pendle",
                                                            n_clicks=0,
                                                            color="warning",
                                                            outline=False,
                                                            size="sm",
                                                            className="px-3",
                                                        ),
                                                        dbc.Button(
                                                            "🕯️ Candlestick",
                                                            id="btn-candle-pendle",
                                                            n_clicks=0,
                                                            color="secondary",
                                                            outline=True,
                                                            size="sm",
                                                            className="px-3",
                                                        ),
                                                    ],
                                                ),
                                            ],
                                        ),
                                        dcc.Graph(
                                            id="graph-asset-pendle",
                                            figure=go.Figure(layout=dict(template=TPL_DARK, height=480)),
                                            config={"displayModeBar": True, "scrollZoom": True, "responsive": True},
                                            style={"height": "520px", "width": "100%"},
                                        ),
                                    ],
                                ),
                                lg=6,
                                md=12,
                            ),
                        ],
                    ),
                    html.Div(id="dynamic-main"),
                    html.Div(
                        className="footer-note",
                        children=[
                            "Dashboard (EUR) · Paper trading · Dados CoinGecko. ",
                            "Se não vês o fundo escuro nem a linha verde acima, faz hard refresh (Cmd+Shift+R) e confirma que corres ",
                            html.Code("python main.py", className="text-light"),
                            " dentro da pasta ",
                            html.Code(str(_DASH_DIR.name), className="text-light"),
                            ".",
                        ],
                    ),
                ],
            ),
            dbc.Modal(
                id="trade-modal",
                is_open=False,
                size="lg",
                centered=True,
                className="trade-modal-dark",
                children=[
                    dbc.ModalHeader(
                        html.Div(
                            className="d-flex w-100 justify-content-between align-items-center gap-2",
                            children=[
                                dbc.ModalTitle(id="trade-modal-title", children="Detalhe do trade", className="mb-0 fs-6"),
                                dbc.Button(
                                    "✕",
                                    id="trade-modal-close",
                                    color="secondary",
                                    size="sm",
                                    outline=True,
                                    className="border-secondary text-light",
                                ),
                            ],
                        ),
                        close_button=False,
                        className="border-secondary bg-dark",
                    ),
                    dbc.ModalBody(
                        dcc.Graph(
                            id="trade-modal-graph",
                            figure=trade_chart.initial_modal_figure(),
                            config={"displayModeBar": True, "scrollZoom": True},
                            style={"height": "580px"},
                        ),
                        className="pt-0",
                    ),
                ],
            ),
            html.Div(id="ctrl-dummy", style={"display": "none"}),
        ],
    )

    @app.callback(
        Output("ctrl-dummy", "children"),
        Input("btn-parar", "n_clicks"),
        Input("btn-iniciar", "n_clicks"),
        prevent_initial_call=True,
    )
    def _botões(_p: int, _i: int) -> str:
        ctx = dash.callback_context
        print("[dash] _botões triggered", ctx.triggered, flush=True)
        if not ctx.triggered:
            return ""
        tid = ctx.triggered[0]["prop_id"].split(".")[0]
        st = app_state.state
        if tid == "btn-parar":
            st.agent_running = False
        elif tid == "btn-iniciar":
            st.agent_running = True
        return ""

    @app.callback(
        Output("store-chart-mode-uni", "data"),
        Output("store-chart-mode-pendle", "data"),
        Input("btn-linha-uni", "n_clicks"),
        Input("btn-candle-uni", "n_clicks"),
        Input("btn-linha-pendle", "n_clicks"),
        Input("btn-candle-pendle", "n_clicks"),
        State("store-chart-mode-uni", "data"),
        State("store-chart-mode-pendle", "data"),
        prevent_initial_call=True,
    )
    def _chart_mode_from_buttons(
        _nlu: int | None,
        _ncu: int | None,
        _nlp: int | None,
        _ncp: int | None,
        su: str | None,
        sp: str | None,
    ) -> tuple[str, str]:
        ctx = dash.callback_context
        if not ctx.triggered:
            raise PreventUpdate
        su = su if su in ("line", "candle") else "line"
        sp = sp if sp in ("line", "candle") else "line"
        tid = ctx.triggered[0]["prop_id"].split(".")[0]
        if tid == "btn-linha-uni":
            su = "line"
        elif tid == "btn-candle-uni":
            su = "candle"
        elif tid == "btn-linha-pendle":
            sp = "line"
        elif tid == "btn-candle-pendle":
            sp = "candle"
        else:
            raise PreventUpdate
        return su, sp

    @app.callback(
        Output("btn-linha-uni", "color"),
        Output("btn-linha-uni", "outline"),
        Output("btn-candle-uni", "color"),
        Output("btn-candle-uni", "outline"),
        Input("store-chart-mode-uni", "data"),
    )
    def _style_btns_uni(mode: str | None) -> tuple[str, bool, str, bool]:
        mode = mode or "line"
        if mode == "line":
            return "warning", False, "secondary", True
        return "secondary", True, "warning", False

    @app.callback(
        Output("btn-linha-pendle", "color"),
        Output("btn-linha-pendle", "outline"),
        Output("btn-candle-pendle", "color"),
        Output("btn-candle-pendle", "outline"),
        Input("store-chart-mode-pendle", "data"),
    )
    def _style_btns_pendle(mode: str | None) -> tuple[str, bool, str, bool]:
        mode = mode or "line"
        if mode == "line":
            return "warning", False, "secondary", True
        return "secondary", True, "warning", False

    @app.callback(
        Output("dynamic-main", "children"),
        Output("txt-relogio", "children"),
        Output("badge-estado", "children"),
        Input("interval-atualiza", "n_intervals"),
        Input("range-charts", "value"),
    )
    def _render(_n: int, days_key: str | None) -> tuple[Any, str, Any]:
        dk = days_key or "30d"
        now_s = datetime.now().strftime("%H:%M:%S")
        relogio = f"Última atualização: {now_s}"
        print(f"[dash] _render start n={_n} dk={dk}", flush=True)
        try:
            body, badge = _build_dynamic_main()
        except Exception as e:
            logger.exception("dashboard _render")
            print(f"[dash] _render ERRO: {e}", flush=True)
            traceback.print_exc()
            err = html.Div(
                [
                    dbc.Alert(
                        [
                            html.Strong("Erro ao renderizar o painel. "),
                            "Consulta o terminal para o traceback completo.",
                            html.Br(),
                            html.Code(str(e)),
                        ],
                        color="danger",
                    ),
                    html.Pre(
                        traceback.format_exc()[:8000],
                        className="small text-muted",
                        style={"whiteSpace": "pre-wrap", "maxHeight": "360px", "overflow": "auto"},
                    ),
                ]
            )
            return err, relogio, dbc.Badge("● Erro", color="danger", className="fs-6 me-2", pill=True)
        print("[dash] _render OK", flush=True)
        return body, relogio, badge

    @app.callback(
        Output("equity-graph", "figure"),
        Output("equity-stats-bar", "children"),
        Output("graph-asset-uni", "figure"),
        Output("graph-asset-pendle", "figure"),
        Output("asset-meta-uni", "children"),
        Output("asset-meta-pendle", "children"),
        Input("interval-atualiza", "n_intervals"),
        Input("range-charts", "value"),
        Input("store-chart-mode-uni", "data"),
        Input("store-chart-mode-pendle", "data"),
    )
    def _update_charts_equity(
        _n: int,
        rk: str | None,
        ru: str | None,
        rp: str | None,
    ) -> tuple[Any, Any, Any, Any, Any, Any]:
        rk = rk or "30d"
        if ru not in ("line", "candle"):
            ru = "line"
        if rp not in ("line", "candle"):
            rp = "line"
        print(f"[dash] charts rk={rk} uni={ru} pendle={rp}", flush=True)
        try:
            st = app_state.state
            snap = st.snapshot_ui()
            dfs = _merge_indicator_frames(snap["indicator_dfs"])
            precos = _dashboard_precos_only(snap["last_prices"])
            fig_eq, stats = dashboard_charts.build_equity_figure_and_stats()
            df_u = dfs.get("uniswap", pd.DataFrame())
            df_p = dfs.get("pendle", pd.DataFrame())
            g_u = dashboard_charts.build_asset_combined_figure(df_u, "uniswap", "UNI", ru, rk)
            g_p = dashboard_charts.build_asset_combined_figure(df_p, "pendle", "PENDLE", rp, rk)
            m_u = _asset_header_meta("UNI", "uniswap", precos.get("uniswap"), COLOR_UNI)
            m_p = _asset_header_meta("PENDLE", "pendle", precos.get("pendle"), COLOR_PENDLE)
            return fig_eq, stats, g_u, g_p, m_u, m_p
        except Exception as e:
            logger.exception("_update_charts_equity")
            err = go.Figure()
            err.update_layout(
                template=TPL_DARK,
                height=200,
                annotations=[
                    dict(text=f"Erro: {e}", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
                ],
            )
            z = html.Div("—", className="small text-danger")
            return err, z, err, err, html.Div(), html.Div()

    # Modal: fechar (X) ou abrir ao clicar em "Ver Gráfico 📊".
    # store-modal-open espelha o estado aberto/fechado sem depender de botão fantasma.
    # A tabela de histórico recria os botões a cada _render — disparos com n_clicks=0
    # ou triggered_id None não alteram o modal nem o store.
    @app.callback(
        Output("trade-modal", "is_open"),
        Output("trade-modal-graph", "figure"),
        Output("trade-modal-title", "children"),
        Output("store-modal-open", "data"),
        Input("trade-modal-close", "n_clicks"),
        Input({"type": "trade-chart-btn", "tid": ALL}, "n_clicks"),
        State("store-modal-open", "data"),
        prevent_initial_call=True,
    )
    def _trade_modal(
        _close_n: int | None,
        _btn_ns: list[int | None],
        store_modal_open: bool | None,
    ) -> tuple[bool, Any, Any, bool]:
        ctx = dash.callback_context
        if not ctx.triggered:
            raise PreventUpdate
        tid = getattr(ctx, "triggered_id", None)
        if tid is None:
            raise PreventUpdate
        trig_prop = ctx.triggered[0]["prop_id"]
        trig_val = ctx.triggered[0].get("value")

        if "trade-modal-close" in trig_prop:
            if trig_val is None or int(trig_val) < 1:
                raise PreventUpdate
            return False, no_update, no_update, False

        if isinstance(tid, dict) and tid.get("type") == "trade-chart-btn":
            tid_int = int(tid["tid"])
            if trig_val is None or int(trig_val) < 1:
                raise PreventUpdate
            # store_modal_open (State): espelha o modal independentemente dos botões; o output
            # actualiza para True só ao abrir com n_clicks>=1 (ponto 3–4).
            _ = store_modal_open
            trade = db.fetch_trade_by_id(tid_int)
            if not trade or trade.get("ts_saida") is None:
                return True, trade_chart.empty_insufficient_figure(), "Trade não encontrado", True
            sym = CID_TO_SYM.get(trade["ativo"], trade["ativo"])
            fig = trade_chart.build_trade_detail_figure(trade, sym)
            dur = trade_chart.effective_duration_sec(trade)
            plv = trade.get("pl_pct")
            title = trade_chart.build_modal_title(
                sym,
                tid_int,
                float(plv) if plv is not None else None,
                dur,
            )
            return True, fig, title, True
        raise PreventUpdate

    return app


def _port_em_uso(err: OSError) -> bool:
    if err.errno in (errno.EADDRINUSE, getattr(errno, "WSAEADDRINUSE", -1)):
        return True
    return "address already in use" in str(err).lower()


def run_dashboard() -> None:
    app = create_app()
    logger.info("Dashboard em %s | assets=%s", _DASH_DIR, _DASH_DIR / "assets")
    base = config.DASH_PORT
    # No Railway (e similares) PORT é fixo; não tentar portas alternativas.
    port_env_fixed = bool(os.getenv("PORT"))
    for offset in range(20):
        port = base + offset
        try:
            logger.info("Servidor Dash em http://%s:%s/", config.DASH_HOST, port)
            app.run(host=config.DASH_HOST, port=port, debug=False)
            return
        except OSError as e:
            if _port_em_uso(e) and offset < 19 and not port_env_fixed:
                logger.warning(
                    "Porta %s ocupada — a tentar %s (ou termina o outro Python: lsof -i :%s)",
                    port,
                    port + 1,
                    port,
                )
                continue
            raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_dashboard()
