# Agente de trading (paper) — CoinGecko

Simulação de compra/venda de **UNI** e **PENDLE** com dados em tempo real da API pública [CoinGecko](https://www.coingecko.com/en/api), sem ligação a exchanges.

## Requisitos

- Python **3.11+**
- Conta opcional na CoinGecko Pro (campo `COINGECKO_API_KEY` no `.env`; deixe vazio para API gratuita)

## Instalação

```bash
cd trading-agent
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Copie `.env` e ajuste valores se necessário (já existe um `.env` de exemplo no repositório).

## Executar agente + dashboard

```bash
python main.py
```

- Corre **um ciclo** ao arranque e depois a cada `CICLO_SEGUNDOS` (30 s por defeito) via `schedule` (thread em segundo plano).
- Abre o **Dash** em `http://127.0.0.1:8050` (ou `DASH_HOST` / `DASH_PORT` no `.env`).
- Botões **PARAR AGENTE** / **INICIAR AGENTE** pausam ou retomam o ciclo (não reinicia o servidor).

Apenas o dashboard (sem thread do agente):

```bash
python dashboard.py
```

## Testar o feed de dados

```bash
python data_feed.py
```

## Estrutura

| Ficheiro        | Função                                      |
|-----------------|---------------------------------------------|
| `main.py`       | Ciclo do agente + arranque do Dash          |
| `app_state.py`  | Estado partilhado (thread + UI)             |
| `data_feed.py`  | CoinGecko, rate limit, cache, retry         |
| `indicators.py` | Stoch RSI, MA7                              |
| `strategy.py`   | Sinais compra/venda                         |
| `risk.py`       | Limites de risco                            |
| `portfolio.py`  | Saldo e posições simuladas                  |
| `dashboard.py`  | Dash / Plotly                               |
| `database.py`   | SQLite: preços, trades, sinais, estado      |
| `config.py`     | Configuração e constantes                   |

## Notas

- Limite prudencial: **30 pedidos/minuto** (configurável em `COINGECKO_MAX_CALLS_PER_MINUTE`).
- Histórico `market_chart` fica em SQLite; nova chamada à API no máximo a cada `COINGECKO_CHART_REFRESH_SEC` (predef.: 600 s = 10 min). Entre pedidos HTTP há `COINGECKO_MIN_API_INTERVAL_SEC` (2 s). Em 429 aguarda-se `COINGECKO_429_WAIT_SEC` (60 s).
- Erros de rede ou HTTP são registados em log; o feed tenta devolver dados em cache quando a API falha.
