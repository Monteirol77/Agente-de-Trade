"""
Portefólio em paper trading: saldo, posições abertas, P&L e persistência SQLite.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Final

import config
import database as db
import risk

logger = logging.getLogger(__name__)

# Motivos de saída alinhados com strategy / especificação
MOTIVO_STOP: Final[str] = "STOP_LOSS"
MOTIVO_TP: Final[str] = "TAKE_PROFIT"
MOTIVO_SINAL: Final[str] = "SINAL_TECNICO"
MOTIVO_LIQUIDACAO: Final[str] = "LIQUIDACAO_EMERGENCIA"


@dataclass
class Posicao:
    """Posição long simulada (um contrato por ativo CoinGecko)."""

    trade_id: int
    ativo: str
    preco_entrada: float
    quantidade: float
    valor_eur: float
    ts_entrada_ms: int


class Portfolio:
    """
    Estado do portefólio: saldo em memória + tabela `trades` para histórico.
    No máximo uma posição aberta por `ativo` (id CoinGecko).
    """

    def __init__(self) -> None:
        db.init_db()
        self.saldo_disponivel: float
        self.agente_parado: bool
        self.posicoes: dict[str, Posicao] = {}
        self._recarregar_do_db()

    def _recarregar_do_db(self) -> None:
        self.saldo_disponivel, self.agente_parado = db.get_portfolio_state()
        self.posicoes.clear()
        for row in db.fetch_open_trades():
            aid = row["ativo"]
            self.posicoes[aid] = Posicao(
                trade_id=int(row["id"]),
                ativo=aid,
                preco_entrada=float(row["preco_entrada"]),
                quantidade=float(row["quantidade"]),
                valor_eur=float(row["valor_eur"]),
                ts_entrada_ms=int(row["ts_entrada"]),
            )

    def _persistir_estado(self) -> None:
        db.set_portfolio_state(self.saldo_disponivel, agente_parado=self.agente_parado)

    def num_posicoes_abertas(self) -> int:
        return len(self.posicoes)

    def tem_posicao(self, ativo: str) -> bool:
        return ativo in self.posicoes

    def valor_mercado_posicoes(self, precos_por_ativo: dict[str, float]) -> float:
        """Valor de mercado (€) das posições abertas com os preços dados."""
        total = 0.0
        for ativo, pos in self.posicoes.items():
            px = precos_por_ativo.get(ativo)
            if px is None:
                logger.warning("Preço em falta para marcar posição %s", ativo)
                continue
            total += pos.quantidade * px
        return total

    def saldo_total(self, precos_por_ativo: dict[str, float]) -> float:
        return risk.saldo_total_portfolio(self.saldo_disponivel, self.valor_mercado_posicoes(precos_por_ativo))

    def pl_realizado_acumulado(self) -> float:
        """Soma do P&L dos trades já fechados (€)."""
        s = 0.0
        for t in db.fetch_closed_trades():
            if t.get("pl_eur") is not None:
                s += float(t["pl_eur"])
        return s

    def pl_nao_realizado(self, precos_por_ativo: dict[str, float]) -> float:
        u = 0.0
        for ativo, pos in self.posicoes.items():
            px = precos_por_ativo.get(ativo)
            if px is None:
                continue
            u += (px - pos.preco_entrada) * pos.quantidade
        return u

    def pl_total(self, precos_por_ativo: dict[str, float]) -> float:
        return self.pl_realizado_acumulado() + self.pl_nao_realizado(precos_por_ativo)

    def verificar_paragem_por_saldo(self, precos_por_ativo: dict[str, float]) -> bool:
        """
        Se saldo total < limite mínimo: alerta, liquidação total, agente parado.
        Devolve True se entrou em paragem nesta chamada.
        """
        st = self.saldo_total(precos_por_ativo)
        if not risk.abaixo_do_limite_saldo_minimo(st):
            return False

        risk.alerta_saldo_critico(st)
        self.liquidar_todas_posicoes(precos_por_ativo, motivo=MOTIVO_LIQUIDACAO)
        self.agente_parado = True
        self._persistir_estado()
        logger.critical(
            "Agente PARADO: património total %.2f € inferior a %.2f €.",
            st,
            config.LIMITE_SALDO_MINIMO,
        )
        return True

    def liquidar_todas_posicoes(
        self,
        precos_por_ativo: dict[str, float],
        *,
        motivo: str,
    ) -> None:
        """Fecha todas as posições ao preço indicado (emergência ou fim de sessão)."""
        for ativo in list(self.posicoes.keys()):
            px = precos_por_ativo.get(ativo)
            if px is None:
                logger.error("Sem preço para liquidar %s — a ignorar.", ativo)
                continue
            self.fechar_posicao(ativo, px, motivo=motivo, ts_ms=_agora_ms())

    def fechar_posicao(
        self,
        ativo: str,
        preco_saida: float,
        *,
        motivo: str,
        ts_ms: int | None = None,
    ) -> bool:
        """Fecha uma posição aberta; atualiza saldo e registo na BD."""
        if ativo not in self.posicoes:
            return False
        pos = self.posicoes[ativo]
        ts = ts_ms if ts_ms is not None else _agora_ms()
        q = pos.quantidade
        proceeds = q * preco_saida
        custo = pos.valor_eur
        pl_eur = proceeds - custo
        pl_pct = (preco_saida - pos.preco_entrada) / pos.preco_entrada * 100.0 if pos.preco_entrada else 0.0

        db.close_trade(
            pos.trade_id,
            preco_saida,
            pl_eur,
            pl_pct,
            motivo,
            ts,
        )
        self.saldo_disponivel += proceeds
        del self.posicoes[ativo]
        self._persistir_estado()
        logger.info(
            "Fechado %s @ %.6f  P&L %.2f € (%.2f%%)  motivo=%s",
            ativo,
            preco_saida,
            pl_eur,
            pl_pct,
            motivo,
        )
        return True

    def tentar_comprar(
        self,
        ativo: str,
        preco: float,
        valor_ordem_eur: float,
        ts_ms: int | None = None,
    ) -> tuple[bool, str]:
        """
        Executa compra simulada até ao mínimo entre o valor pedido e o teto de risco.
        """
        ts = ts_ms if ts_ms is not None else _agora_ms()
        av = risk.avaliar_permitir_compra(
            saldo_disponivel=self.saldo_disponivel,
            num_posicoes_abertas=self.num_posicoes_abertas(),
            ja_tem_posicao_no_ativo=self.tem_posicao(ativo),
            agente_parado=self.agente_parado,
            valor_ordem_eur=valor_ordem_eur,
        )
        if not av.permitido:
            return False, av.motivo

        # Não ultrapassar o teto nem o saldo
        valor = min(valor_ordem_eur, av.valor_maximo_ordem_eur, self.saldo_disponivel)
        if valor <= 0 or preco <= 0:
            return False, "Valor ou preço inválido."

        quantidade = valor / preco
        custo = quantidade * preco
        if custo > self.saldo_disponivel + 1e-9:
            return False, "Saldo insuficiente após arredondamento."

        tid = db.insert_trade_open(ativo, preco, quantidade, custo, ts)
        self.saldo_disponivel -= custo
        self.posicoes[ativo] = Posicao(
            trade_id=tid,
            ativo=ativo,
            preco_entrada=preco,
            quantidade=quantidade,
            valor_eur=custo,
            ts_entrada_ms=ts,
        )
        self._persistir_estado()
        logger.info(
            "Compra %s  q=%.8f  preço=%.6f  custo=%.2f €  trade_id=%s",
            ativo,
            quantidade,
            preco,
            custo,
            tid,
        )
        return True, "OK."

    def reiniciar_paper(self) -> None:
        """Apaga estado de simulação (útil em testes): fecha BD lógica — apenas estado."""
        # Não apaga trades automaticamente; exposto para testes manuais
        self.saldo_disponivel = config.SALDO_INICIAL
        self.agente_parado = False
        db.set_portfolio_state(self.saldo_disponivel, agente_parado=False)
        self._recarregar_do_db()


def _agora_ms() -> int:
    return int(time.time() * 1000)


def novo_portfolio() -> Portfolio:
    """Fábrica com init explícito."""
    return Portfolio()
