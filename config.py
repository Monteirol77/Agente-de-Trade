"""
Configurações centralizadas carregadas de variáveis de ambiente (.env).
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# Raiz do projeto (pasta onde está este ficheiro)
BASE_DIR = Path(__file__).resolve().parent

load_dotenv(BASE_DIR / ".env")

# --- Portfólio e risco ---
SALDO_INICIAL: float = float(os.getenv("SALDO_INICIAL", "10000"))
LIMITE_SALDO_MINIMO: float = float(os.getenv("LIMITE_SALDO_MINIMO", "7000"))
MAX_POSICOES: int = int(os.getenv("MAX_POSICOES", "2"))
MAX_PCT_POR_OPERACAO: float = float(os.getenv("MAX_PCT_POR_OPERACAO", "0.20"))
STOP_LOSS_PCT: float = float(os.getenv("STOP_LOSS_PCT", "3"))
TAKE_PROFIT_PCT: float = float(os.getenv("TAKE_PROFIT_PCT", "8"))

# --- Ciclo do agente ---
CICLO_SEGUNDOS: int = int(os.getenv("CICLO_SEGUNDOS", "30"))

# --- CoinGecko ---
COINGECKO_API_KEY: str = os.getenv("COINGECKO_API_KEY", "").strip()
COINGECKO_BASE_URL: str = "https://api.coingecko.com/api/v3"
# Limite conservador da API gratuita (chamadas por minuto)
COINGECKO_MAX_CALLS_PER_MINUTE: int = int(os.getenv("COINGECKO_MAX_CALLS_PER_MINUTE", "30"))
# Espaço mínimo entre **qualquer** pedido HTTP à CoinGecko (segundos)
COINGECKO_MIN_API_INTERVAL_SEC: float = float(os.getenv("COINGECKO_MIN_API_INTERVAL_SEC", "2"))
# Novo pedido `market_chart` só se o último sucesso tiver há mais de N segundos (10 min por defeito)
COINGECKO_CHART_REFRESH_SEC: int = int(
    os.getenv("COINGECKO_CHART_REFRESH_SEC", os.getenv("COINGECKO_CHART_CACHE_TTL", "600"))
)
# Espera após HTTP 429 antes de repetir (segundos)
COINGECKO_429_WAIT_SEC: float = float(os.getenv("COINGECKO_429_WAIT_SEC", "60"))

# Ativos (ids CoinGecko)
ASSETS: dict[str, str] = {
    "UNI": "uniswap",
    "PENDLE": "pendle",
}

VS_CURRENCY: str = "eur"

# --- Base de dados ---
DATABASE_PATH: Path = BASE_DIR / "data" / "trading.db"

# --- Dashboard ---
# Railway/Heroku/Render definem PORT; em local usa-se normalmente só DASH_PORT (8050).
DASH_PORT: int = int(os.getenv("PORT", os.getenv("DASH_PORT", "8050")))
# Com PORT definido pelo PaaS o processo tem de escutar em 0.0.0.0 (não em 127.0.0.1).
DASH_HOST: str = os.getenv("DASH_HOST", "0.0.0.0" if os.getenv("PORT") else "127.0.0.1")
