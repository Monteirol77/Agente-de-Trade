"""
Ponto de entrada WSGI para Gunicorn (Railway, Render, etc.).

O servidor de desenvolvimento do Flask (app.run) costuma dar 502 atrás do proxy;
Gunicorn + workers=1 evita estado duplicado do Dash.
"""
from __future__ import annotations

import logging
import threading

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

from werkzeug.middleware.proxy_fix import ProxyFix

from dashboard import create_app
from main import iniciar_thread_agente, run_cycle

iniciar_thread_agente()
threading.Thread(target=run_cycle, daemon=True, name="first-cycle").start()

_dash = create_app()
flask_app = _dash.server
flask_app.wsgi_app = ProxyFix(
    flask_app.wsgi_app,
    x_for=1,
    x_proto=1,
    x_host=1,
    x_prefix=1,
)
server = flask_app
