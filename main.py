"""
Ponto de entrada: ciclo do agente (schedule, 30s) + arranque do dashboard Dash.
"""
from __future__ import annotations

import logging
import threading
import time

import schedule

import app_state
import config
import database as db
import risk
from data_feed import fetch_simple_prices_eur, fetch_market_chart_history
from indicators import compute_all_indicators
from strategy import (
    Sinal,
    avaliar_entrada_dataframe,
    avaliar_saida_com_mercado,
    selecionar_unica_compra_por_ciclo,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def _ts_ms() -> int:
    return int(time.time() * 1000)


def _precos_tracked(precos: dict[str, float]) -> dict[str, float]:
    out: dict[str, float] = {}
    for _sym, cid in config.ASSETS.items():
        if cid in precos:
            out[cid] = precos[cid]
    return out


def run_cycle() -> None:
    """Um ciclo: preços, indicadores, risco, saídas, no máximo uma entrada, logs."""
    st = app_state.state
    p = st.portfolio

    if not st.agent_running:
        return
    if p.agente_parado:
        return

    try:
        precos_raw = fetch_simple_prices_eur()
        precos = _precos_tracked(precos_raw)
        if len(precos) < len(config.ASSETS):
            logger.warning("Preços incompletos: %s", precos_raw)

        indicator_dfs: dict[str, object] = {}
        for _sym, cid in config.ASSETS.items():
            hist = fetch_market_chart_history(cid, force_refresh=False)
            indicator_dfs[cid] = compute_all_indicators(hist)

        if p.verificar_paragem_por_saldo(precos):
            try:
                vp = p.valor_mercado_posicoes(precos)
                pt = p.saldo_total(precos)
                db.insert_equity_snapshot(_ts_ms(), p.saldo_disponivel, vp, pt)
            except Exception:
                logger.exception("equity snapshot (paragem)")
            with st.lock:
                st.indicator_dfs = {k: v.copy() for k, v in indicator_dfs.items()}  # type: ignore[assignment]
                st.last_prices = dict(precos)
                st.last_cycle_ts = time.time()
                st.last_error = "Paragem por saldo mínimo."
            return

        fechados: set[str] = set()

        for ativo in list(p.posicoes.keys()):
            if ativo not in indicator_dfs or ativo not in precos:
                continue
            df = indicator_dfs[ativo]
            pos = p.posicoes[ativo]
            rs = avaliar_saida_com_mercado(
                df,  # type: ignore[arg-type]
                preco_entrada=pos.preco_entrada,
                preco_mercado=precos[ativo],
            )
            if rs.motivo:
                if p.fechar_posicao(ativo, precos[ativo], motivo=rs.motivo, ts_ms=_ts_ms()):
                    fechados.add(ativo)

        resultados = {
            cid: avaliar_entrada_dataframe(indicator_dfs[cid], ativo=cid)  # type: ignore[arg-type]
            for cid in config.ASSETS.values()
        }
        escolhido, sinal_ag = selecionar_unica_compra_por_ciclo(resultados)

        if (
            sinal_ag == Sinal.COMPRA
            and escolhido is not None
            and escolhido in precos
        ):
            vmax = risk.valor_maximo_por_operacao(p.saldo_disponivel)
            ok, _msg = p.tentar_comprar(
                escolhido,
                precos[escolhido],
                vmax,
                ts_ms=_ts_ms(),
            )
            if not ok:
                logger.info("Compra não executada: %s", _msg)

        for cid in config.ASSETS.values():
            r = resultados[cid]
            preco = precos.get(cid)
            if preco is None:
                continue
            if cid in fechados:
                sig = "VENDA"
            elif r.sinal:
                sig = "COMPRA"
            else:
                sig = "NENHUM"
            try:
                db.insert_signal_row(
                    _ts_ms(),
                    cid,
                    r.stoch_k,
                    r.stoch_d,
                    r.ma7,
                    float(preco),
                    sig,
                )
            except Exception:
                logger.exception("Falha ao gravar sinal para %s", cid)

        try:
            vp = p.valor_mercado_posicoes(precos)
            pt = p.saldo_total(precos)
            db.insert_equity_snapshot(_ts_ms(), p.saldo_disponivel, vp, pt)
        except Exception:
            logger.exception("equity snapshot")

        with st.lock:
            st.indicator_dfs = {k: v.copy() for k, v in indicator_dfs.items()}  # type: ignore[assignment]
            st.last_prices = dict(precos)
            st.last_cycle_ts = time.time()
            st.last_error = None

    except Exception as e:
        logger.exception("Erro no ciclo do agente: %s", e)
        with st.lock:
            st.last_error = str(e)


def _loop_schedule() -> None:
    schedule.every(config.CICLO_SEGUNDOS).seconds.do(run_cycle)
    logger.info(
        "Scheduler ativo: ciclo a cada %s s.",
        config.CICLO_SEGUNDOS,
    )
    while True:
        schedule.run_pending()
        time.sleep(0.5)


def iniciar_thread_agente() -> threading.Thread:
    t = threading.Thread(target=_loop_schedule, daemon=True, name="agent-scheduler")
    t.start()
    return t


def main() -> None:
    """Arranca o agente e o servidor Dash (bloqueante)."""
    run_cycle()
    iniciar_thread_agente()
    from dashboard import run_dashboard

    run_dashboard()


if __name__ == "__main__":
    main()
