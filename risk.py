"""
Regras de gestão de risco: tamanho máximo por ordem, número de posições e saldo mínimo.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

import config

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AvaliacaoCompra:
    """Resultado da verificação de risco antes de abrir posição."""

    permitido: bool
    motivo: str
    valor_maximo_ordem_eur: float


def valor_maximo_por_operacao(saldo_disponivel: float) -> float:
    """Nunca mais que MAX_PCT_POR_OPERACAO do saldo disponível numa única ordem."""
    if saldo_disponivel <= 0:
        return 0.0
    return saldo_disponivel * config.MAX_PCT_POR_OPERACAO


def saldo_total_portfolio(saldo_disponivel: float, valor_mercado_posicoes: float) -> float:
    """Saldo em caixa + valor de mercado das posições abertas (€)."""
    return float(saldo_disponivel) + float(valor_mercado_posicoes)


def abaixo_do_limite_saldo_minimo(saldo_total: float) -> bool:
    """True se o património total cair abaixo do limite (disparar paragem e liquidação)."""
    return saldo_total < config.LIMITE_SALDO_MINIMO


def pode_abrir_nova_posicao(num_posicoes_abertas: int) -> bool:
    """Máximo MAX_POSICOES posições em simultâneo."""
    return num_posicoes_abertas < config.MAX_POSICOES


def ordem_respeita_limite_percentual(valor_ordem_eur: float, saldo_disponivel: float) -> bool:
    """Ordem não pode exceder o teto percentual (com tolerância numérica)."""
    limite = valor_maximo_por_operacao(saldo_disponivel)
    return valor_ordem_eur <= limite + 1e-9


def avaliar_permitir_compra(
    *,
    saldo_disponivel: float,
    num_posicoes_abertas: int,
    ja_tem_posicao_no_ativo: bool,
    agente_parado: bool,
    valor_ordem_eur: float,
) -> AvaliacaoCompra:
    """
    Combina todas as regras obrigatórias para uma nova compra simulada.
    """
    vmax = valor_maximo_por_operacao(saldo_disponivel)

    if agente_parado:
        return AvaliacaoCompra(False, "Agente em estado parado.", vmax)

    if ja_tem_posicao_no_ativo:
        return AvaliacaoCompra(False, "Já existe posição aberta neste ativo.", vmax)

    if not pode_abrir_nova_posicao(num_posicoes_abertas):
        return AvaliacaoCompra(
            False,
            f"Limite de {config.MAX_POSICOES} posições abertas atingido.",
            vmax,
        )

    if valor_ordem_eur <= 0:
        return AvaliacaoCompra(False, "Valor da ordem inválido.", vmax)

    if valor_ordem_eur > saldo_disponivel + 1e-9:
        return AvaliacaoCompra(False, "Saldo disponível insuficiente.", vmax)

    if not ordem_respeita_limite_percentual(valor_ordem_eur, saldo_disponivel):
        return AvaliacaoCompra(
            False,
            f"Ordem acima de {config.MAX_PCT_POR_OPERACAO:.0%} do saldo (máx. {vmax:.2f} €).",
            vmax,
        )

    return AvaliacaoCompra(True, "OK.", vmax)


def alerta_saldo_critico(saldo_total: float) -> None:
    """Registo de alerta antes do limite duro (opcionalmente usado pelo dashboard)."""
    if saldo_total < config.LIMITE_SALDO_MINIMO:
        logger.critical(
            "ALERTA RISCO: saldo total %.2f € abaixo do limite mínimo %.2f € — paragem obrigatória.",
            saldo_total,
            config.LIMITE_SALDO_MINIMO,
        )
