"""
Lógica de entrada e saída com base no Stochastic RSI (14,3,3) e MA7.

Entrada (compra): cruzamento %K acima de %D na zona de sobrevendido + preço > MA7.
Saída (venda): qualquer entre sinal técnico (sobrecomprado), stop-loss ou take-profit.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Final

import numpy as np
import pandas as pd

import config

logger = logging.getLogger(__name__)

# Zonas Stochastic RSI (especificação)
ZONA_SOBREVENDIDO: Final[float] = 20.0
ZONA_SOBRECOMPRADO: Final[float] = 80.0


class Sinal(str, Enum):
    """Valor compatível com a coluna `signals.sinal` na base de dados."""

    COMPRA = "COMPRA"
    VENDA = "VENDA"
    NENHUM = "NENHUM"


@dataclass(frozen=True)
class ResultadoEntrada:
    """Resultado da avaliação de compra num par de barras (anterior, atual)."""

    sinal: bool
    ativo: str | None
    stoch_k: float | None
    stoch_d: float | None
    preco: float | None
    ma7: float | None
    motivo: str


@dataclass(frozen=True)
class ResultadoSaida:
    """Motivo de saída ou None se não fechar."""

    motivo: str | None  # STOP_LOSS | TAKE_PROFIT | SINAL_TECNICO


def _num(x: float | None) -> float | None:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    return float(x)


def cruzamento_k_acima_d(
    prev_k: float, prev_d: float, k: float, d: float
) -> bool:
    """%K cruza acima de %D: antes %K < %D, agora %K > %D."""
    return prev_k < prev_d and k > d


def cruzamento_k_abaixo_d(
    prev_k: float, prev_d: float, k: float, d: float
) -> bool:
    """%K cruza abaixo de %D: antes %K > %D, agora %K < %D."""
    return prev_k > prev_d and k < d


def condicoes_entrada_compra(
    prev_k: float,
    prev_d: float,
    k: float,
    d: float,
    preco: float,
    ma7: float,
) -> bool:
    """
    Compra só se TODAS forem verdadeiras:
    1. %K cruza acima de %D
    2. %K < 20 e %D < 20 (sobrevendido)
    3. Preço atual > MA7
    """
    if not cruzamento_k_acima_d(prev_k, prev_d, k, d):
        return False
    if not (k < ZONA_SOBREVENDIDO and d < ZONA_SOBREVENDIDO):
        return False
    if not (preco > ma7):
        return False
    return True


def condicao_saida_sobrecomprado(
    prev_k: float, prev_d: float, k: float, d: float
) -> bool:
    """
    Venda por sinal técnico: %K cruza abaixo de %D E ambos %K > 80 e %D > 80.
    """
    if not cruzamento_k_abaixo_d(prev_k, prev_d, k, d):
        return False
    if not (k > ZONA_SOBRECOMPRADO and d > ZONA_SOBRECOMPRADO):
        return False
    return True


def verificar_stop_loss(preco_entrada: float, preco_atual: float) -> bool:
    """Perda ≥ STOP_LOSS_PCT % (posição long)."""
    if preco_entrada <= 0:
        return False
    perda_pct = (preco_entrada - preco_atual) / preco_entrada * 100.0
    return perda_pct >= config.STOP_LOSS_PCT


def verificar_take_profit(preco_entrada: float, preco_atual: float) -> bool:
    """Lucro ≥ TAKE_PROFIT_PCT % (posição long)."""
    if preco_entrada <= 0:
        return False
    lucro_pct = (preco_atual - preco_entrada) / preco_entrada * 100.0
    return lucro_pct >= config.TAKE_PROFIT_PCT


def avaliar_saida_posicao(
    *,
    preco_entrada: float,
    preco_atual: float,
    prev_k: float,
    prev_d: float,
    k: float,
    d: float,
) -> ResultadoSaida:
    """
    Qualquer condição verdadeira gera saída (ordem: stop-loss, take-profit, sinal).
    """
    if verificar_stop_loss(preco_entrada, preco_atual):
        return ResultadoSaida(motivo="STOP_LOSS")
    if verificar_take_profit(preco_entrada, preco_atual):
        return ResultadoSaida(motivo="TAKE_PROFIT")
    if condicao_saida_sobrecomprado(prev_k, prev_d, k, d):
        return ResultadoSaida(motivo="SINAL_TECNICO")
    return ResultadoSaida(motivo=None)


def _linhas_validas(df: pd.DataFrame) -> bool:
    need = {"stoch_k", "stoch_d", "close", "ma7"}
    if len(df) < 2 or not need.issubset(df.columns):
        return False
    return True


def avaliar_entrada_dataframe(
    df: pd.DataFrame,
    *,
    ativo: str | None = None,
) -> ResultadoEntrada:
    """
    Usa as duas últimas linhas (penúltima = ciclo anterior, última = agora).
    """
    if not _linhas_validas(df):
        return ResultadoEntrada(
            sinal=False,
            ativo=ativo,
            stoch_k=None,
            stoch_d=None,
            preco=None,
            ma7=None,
            motivo="Dados insuficientes ou colunas em falta (mínimo 2 linhas, stoch_k/d, close, ma7).",
        )
    prev = df.iloc[-2]
    cur = df.iloc[-1]
    pk, pd_ = _num(prev["stoch_k"]), _num(prev["stoch_d"])
    k, d = _num(cur["stoch_k"]), _num(cur["stoch_d"])
    preco = _num(cur["close"])
    ma7 = _num(cur["ma7"])
    if any(v is None for v in (pk, pd_, k, d, preco, ma7)):
        return ResultadoEntrada(
            sinal=False,
            ativo=ativo,
            stoch_k=k,
            stoch_d=d,
            preco=preco,
            ma7=ma7,
            motivo="Indicadores ou preço inválidos (NaN).",
        )
    assert pk is not None and pd_ is not None and k is not None and d is not None
    assert preco is not None and ma7 is not None

    ok = condicoes_entrada_compra(pk, pd_, k, d, preco, ma7)
    motivo = "Condições de compra satisfeitas." if ok else "Critérios de entrada não reunidos."
    return ResultadoEntrada(
        sinal=ok,
        ativo=ativo,
        stoch_k=k,
        stoch_d=d,
        preco=preco,
        ma7=ma7,
        motivo=motivo,
    )


def avaliar_saida_dataframe(
    df: pd.DataFrame,
    *,
    preco_entrada: float,
) -> ResultadoSaida:
    """Última barra vs penúltima para cruzamentos; preço atual = último close."""
    if not _linhas_validas(df):
        return ResultadoSaida(motivo=None)
    prev = df.iloc[-2]
    cur = df.iloc[-1]
    pk, pd_ = _num(prev["stoch_k"]), _num(prev["stoch_d"])
    k, d = _num(cur["stoch_k"]), _num(cur["stoch_d"])
    preco = _num(cur["close"])
    if any(v is None for v in (pk, pd_, k, d, preco)):
        return ResultadoSaida(motivo=None)
    assert pk is not None and pd_ is not None and k is not None and d is not None and preco is not None

    return avaliar_saida_posicao(
        preco_entrada=preco_entrada,
        preco_atual=preco,
        prev_k=pk,
        prev_d=pd_,
        k=k,
        d=d,
    )


def avaliar_saida_com_mercado(
    df: pd.DataFrame,
    *,
    preco_entrada: float,
    preco_mercado: float,
) -> ResultadoSaida:
    """
    Como `avaliar_saida_dataframe`, mas usa `preco_mercado` (ex.: API simple/price)
    para stop-loss e take-profit; cruces de %K/%D continuam nas últimas barras do gráfico.
    """
    if not _linhas_validas(df):
        return ResultadoSaida(motivo=None)
    prev = df.iloc[-2]
    cur = df.iloc[-1]
    pk, pd_ = _num(prev["stoch_k"]), _num(prev["stoch_d"])
    k, d = _num(cur["stoch_k"]), _num(cur["stoch_d"])
    if any(v is None for v in (pk, pd_, k, d)):
        return ResultadoSaida(motivo=None)
    assert pk is not None and pd_ is not None and k is not None and d is not None

    return avaliar_saida_posicao(
        preco_entrada=preco_entrada,
        preco_atual=preco_mercado,
        prev_k=pk,
        prev_d=pd_,
        k=k,
        d=d,
    )


def escolher_compra_prioridade_menor_k(
    candidatos: list[tuple[str, float]],
) -> str | None:
    """
    Entre vários ativos com sinal de compra, escolhe o de menor %K atual.
    `candidatos`: lista de (id CoinGecko ou símbolo, stoch_k).
    """
    if not candidatos:
        return None
    return min(candidatos, key=lambda x: x[1])[0]


def selecionar_unica_compra_por_ciclo(
    resultados: dict[str, ResultadoEntrada],
) -> tuple[str | None, Sinal]:
    """
    No máximo uma COMPRA por ciclo: entre os que têm `sinal=True`,
    prioriza o menor `stoch_k`.
    """
    compras: list[tuple[str, float]] = [
        (aid, r.stoch_k)
        for aid, r in resultados.items()
        if r.sinal and r.stoch_k is not None
    ]
    if not compras:
        return None, Sinal.NENHUM
    escolhido = escolher_compra_prioridade_menor_k(compras)
    return escolhido, Sinal.COMPRA


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    from indicators import compute_all_indicators
    from data_feed import fetch_market_chart_history

    import config as cfg

    resultados: dict[str, ResultadoEntrada] = {}
    for sym, cid in cfg.ASSETS.items():
        hist = fetch_market_chart_history(cid, force_refresh=False)
        ind = compute_all_indicators(hist)
        r = avaliar_entrada_dataframe(ind, ativo=cid)
        resultados[cid] = r
        print(f"\n--- {sym} ({cid}) ---")
        print(f"  Entrada: sinal={r.sinal}  k={r.stoch_k}  d={r.stoch_d}  preco={r.preco}  ma7={r.ma7}")
        print(f"  {r.motivo}")

    escolhido, sinal = selecionar_unica_compra_por_ciclo(resultados)
    print("\n--- Um sinal por ciclo ---")
    print(f"  Escolhido: {escolhido}  sinal_agregado={sinal.value}")

    # Exemplo de saída com preço de entrada fictício
    print("\n--- Teste saída (preco_entrada = último close * 0.97) ---")
    for sym, cid in cfg.ASSETS.items():
        hist = fetch_market_chart_history(cid, force_refresh=False)
        ind = compute_all_indicators(hist)
        pe = float(ind["close"].iloc[-1]) * 0.97
        sa = avaliar_saida_dataframe(ind, preco_entrada=pe)
        print(f"  {sym}: motivo_saida={sa.motivo} (entrada simulada {pe:.4f})")
