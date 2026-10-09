from __future__ import annotations
import asyncio
import atexit
import concurrent.futures
import csv
import hashlib
import io
import hmac
import json
import os
import sys
import tempfile
import threading
import time
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timezone
from math import floor
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode
import urllib.error
import urllib.request

from flask import Flask, jsonify, make_response, render_template_string, request

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from WS import SymbolWebSocketPriceCache, _ALL_MARKET_ENABLED  # noqa: E402
from kline_ws import KlineWebSocketStream                     # noqa: E402
from state_store import StateStore, backend_from_env, make_owner_id  # noqa: E402

# ── ExecutorBridge (señales al Executor externo) ─────────────────────────────

@dataclass
class _ExecutorSignalConfig:
    executor_url:   str = ""
    signal_secret:   str = "clave-secreta-aleatoria"
    poll_secs:       int = 5
    timeout_signal:  int = 8
    timeout_state:   int = 8


class ExecutorBridge:
    """Envía señales de apertura/cierre al Executor y consulta su estado."""

    def __init__(
        self,
        executor_url: str = "",
        signal_secret: str = "clave-secreta-aleatoria",
        poll_secs: int = 5,
        logger=None,
    ) -> None:
        self.config = _ExecutorSignalConfig(
            executor_url=executor_url.strip().rstrip("/"),
            signal_secret=signal_secret,
            poll_secs=int(poll_secs),
        )
        self.logger = logger or print

    def _log(self, message: str) -> None:
        try:
            self.logger(message)
        except Exception:
            pass

    def _build_signal_request(self, payload: dict) -> urllib.request.Request:
        body = json.dumps(payload).encode("utf-8")
        return urllib.request.Request(
            f"{self.config.executor_url}/signal",
            data=body,
            headers={
                "Content-Type": "application/json",
                "X-Signal-Secret": self.config.signal_secret,
            },
            method="POST",
        )

    def send_signal_sync(self, payload: dict) -> None:
        """Envía una señal al Executor. No lanza excepción: solo registra el error."""
        if not self.config.executor_url:
            return
        try:
            req = self._build_signal_request(payload)
            with urllib.request.urlopen(req, timeout=self.config.timeout_signal) as resp:
                resp.read()
                self._log(
                    f"[executor] ✓ señal enviada: {payload.get('action')} {payload.get('symbol')}"
                )
        except Exception as exc:
            self._log(
                f"[executor] error enviando {payload.get('action')} "
                f"{payload.get('symbol')}: {exc}"
            )

    async def send_signal_async(self, payload: dict) -> None:
        """Versión no bloqueante para usar desde el event loop."""
        await asyncio.to_thread(self.send_signal_sync, payload)

    def fetch_state_sync(self) -> Optional[dict]:
        """Lee /api/state del Executor."""
        if not self.config.executor_url:
            return None
        try:
            req = urllib.request.Request(
                f"{self.config.executor_url}/api/state", method="GET"
            )
            with urllib.request.urlopen(req, timeout=self.config.timeout_state) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception:
            return None

    def notify_open(
        self,
        trade_id: int,
        symbol: str,
        direction: str,
        price: float,
        quantity: float,
        notional: float = 0.0,
        level: float = 0.0,
    ) -> None:
        """Notifica apertura de posición al Executor sin bloquear el loop."""
        payload = {
            "action":    "open",
            "trade_id":  trade_id,
            "symbol":    symbol,
            "direction": direction,
            "price":     price,
            "quantity":  quantity,
            "notional":  notional,
            "level":     level,
        }
        self.notify_async(payload)

    def notify_close(
        self,
        trade_id: int,
        symbol: str,
        direction: str,
        reason: str,
        close_price: float,
        pnl: float = 0.0,
    ) -> None:
        """Notifica cierre de posición al Executor sin bloquear el loop."""
        payload = {
            "action":      "close",
            "trade_id":    trade_id,
            "symbol":      symbol,
            "direction":   direction,
            "reason":      reason,
            "close_price": close_price,
            "pnl":         pnl,
        }
        self.notify_async(payload)

    def notify_async(self, payload: dict) -> None:
        """Dispara el envío sin bloquear el event loop."""
        if not self.config.executor_url:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            threading.Thread(
                target=self.send_signal_sync,
                args=(payload,),
                daemon=True,
            ).start()
            return
        loop.create_task(self.send_signal_async(payload))



# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────

BASE_URL      = os.getenv("BASE_URL",    "https://fapi.binance.com")
QUOTE_ASSET   = os.getenv("QUOTE_ASSET", "USDT")
PAPER_MODE    = os.getenv("PAPER_MODE",   "true").lower() == "true"
LIVE_TRADING  = os.getenv("LIVE_TRADING", "false").lower() == "true"
API_KEY       = os.getenv("BINANCE_API_KEY",    "")
API_SECRET    = os.getenv("BINANCE_API_SECRET", "")
# Proxy HTTP opcional SOLO para la única consulta REST del arranque
# (exchangeInfo). Ej.: http://usuario:clave@host:80 — configúralo como secreto,
# nunca en el código. Si falla, se reintenta una vez por la IP directa.
REST_PROXY_URL = os.getenv("REST_PROXY_URL", "http://fixie:6U5qGtczYcZJRHN@ventoux.usefixie.com:80").strip()
STATE_FILE    = os.getenv("STATE_FILE", os.path.join(tempfile.gettempdir(), "botshort_state.json"))
# ── Gestión de símbolos ───────────────────────────────────────────────────────
INITIAL_SYMBOLS = [ s.strip() for s in os.getenv("INITIAL_SYMBOLS", "").split(",") if s.strip() ]
# Lista de perpetuos operables + filtros (stepSize/minQty/minNotional).
# Se obtiene con UNA consulta REST (exchangeInfo) al arrancar y se guarda en
# disco; si el caché tiene menos de SYMBOL_REFRESH_HOURS, el arranque no hace
# ninguna petición REST. Durante la ejecución NO se vuelve a consultar.
SYMBOLS_CACHE_FILE   = os.getenv(
    "SYMBOLS_CACHE_FILE",
    os.path.join(tempfile.gettempdir(), "futures_symbols_cache.json")
)
SYMBOL_REFRESH_HOURS = int(os.getenv("SYMBOL_REFRESH_HOURS", "12"))

# ── Vigilancia continua (sin ciclos de filtrado ni sleeps de detección) ───────
# UNA sola conexión WebSocket permanente (WS.py):
#   • !ticker@arr    → cambio 24h + open 24h de TODOS los símbolos (~1 s)
#   • !markPrice@arr → precio de TODOS los símbolos (1 s)
#   • <sym>@bookTicker (ms) → solo para los símbolos del RADAR
# Radar = símbolos con cambio >= MIN_GAIN_FILTER (o con posición abierta).
# Cada tick de un símbolo del radar dispara su evaluación (entradas/TP/SL).
MIN_GAIN_FILTER        = float(os.getenv("MIN_GAIN_FILTER",        "20"))   # % 24h para entrar al radar
WATCH_PRUNE_SECS       = float(os.getenv("WATCH_PRUNE_SECS",       "20"))   # limpieza del radar (housekeeping)
WATCH_PRUNE_HYSTERESIS = float(os.getenv("WATCH_PRUNE_HYSTERESIS", "5"))    # sale al bajar de MIN_GAIN_FILTER-5
# Red de seguridad: re-evalúa posiciones/radar aunque no lleguen ticks (libro
# quieto, reintentos tras error, fin de cooldown). NO es la vía principal.
# 0 = desactivada.
SAFETY_TICK_SECS       = float(os.getenv("SAFETY_TICK_SECS",       "1.0"))
PRICE_MAX_AGE_S        = float(os.getenv("PRICE_MAX_AGE_S",        "5"))    # un precio más viejo no se usa para operar
# true = el % de cambio se recalcula en cada tick con (precio / open24h - 1),
# en vez de esperar al ticker 24h (que llega cada ~1 s).
USE_LIVE_CHANGE        = os.getenv("USE_LIVE_CHANGE", "true").lower() == "true"
# Velas SOLO por WebSocket (kline_ws.py) para TODOS los símbolos, sin REST:
# una conexión por intervalo. "1m" siempre se incluye (confirmación de entrada).
KLINE_INTERVALS        = list(dict.fromkeys(
    ["1m"] + [x.strip() for x in os.getenv("KLINE_INTERVALS", "1m").split(",") if x.strip()]
))
# Velas cerradas guardadas por símbolo e intervalo (2 = última y penúltima).
KLINE_HISTORY          = int(os.getenv("KLINE_HISTORY", "2"))
# float64 = precisión exacta (48 bytes/vela); float32 = mitad de RAM (28 bytes/vela).
KLINE_DTYPE            = os.getenv("KLINE_DTYPE", "float32")
# false = sin vela cerrada reciente (primer minuto tras arrancar o reconexión)
# NO se abre la entrada; true = se permite (comportamiento anterior).
KLINE_ALLOW_NO_DATA    = os.getenv("KLINE_ALLOW_NO_DATA", "false").lower() == "true"
ENTRY_ERROR_BACKOFF_S  = float(os.getenv("ENTRY_ERROR_BACKOFF_S",  "5"))    # pausa tras fallo al abrir
CLOSE_ERROR_BACKOFF_S  = float(os.getenv("CLOSE_ERROR_BACKOFF_S",  "3"))    # pausa tras fallo al cerrar
STATE_PERSIST_SECS     = float(os.getenv("STATE_PERSIST_SECS",     "10"))   # guardado periódico del estado

# ── Dashboard ─────────────────────────────────────────────────────────────────
LIVE_POLL_MS   = int(os.getenv("LIVE_POLL_MS",   "250"))    # el navegador pide precios cada 250 ms (payload mínimo)
STATUS_POLL_MS = int(os.getenv("STATUS_POLL_MS", "1500"))   # estructura completa de las tablas

# ── Resto de parámetros operativos ────────────────────────────────────────────
COOLDOWN_SECONDS     = int(os.getenv("COOLDOWN_SECONDS",     "86400"))

# Tiempo de gracia al detener un cache WS (segundos)
WS_STOP_GRACE        = float(os.getenv("WS_STOP_GRACE", "0.4"))

# Precio máximo permitido para abrir nuevas entradas (bloqueo permanente si supera)
MAX_PRICE_BLOCK = float(os.getenv("MAX_PRICE_BLOCK", "1.5"))

ENTRY_LEVELS    = [float(x) for x in os.getenv("ENTRY_LEVELS",    "50,75,100,150,200,250,300").split(",")]
ENTRY_NOTIONALS = [float(x) for x in os.getenv("ENTRY_NOTIONALS", "5,5,10,20,40,80,160").split(",")]
TAKE_PROFIT_FRACTION = float(os.getenv("TAKE_PROFIT_FRACTION", "0.14284"))

# Stop loss por defecto en USD (pérdida absoluta, valor negativo). Es el SL
# "estándar": se usa en cuanto la posición tiene 2 o más tramos.
# Puede sobreescribirse por posición desde el dashboard (POST /api/set-sl/<symbol>).
DEFAULT_STOP_LOSS_USD = float(os.getenv("DEFAULT_STOP_LOSS_USD", "-8.0"))

# Stop loss del PRIMER tramo: pérdida máxima = notional del primer fill × 0.251.
# Ej.: primer tramo de 5 USDT → SL = -1.255 USD. Al abrirse el 2.º tramo la
# posición vuelve al SL estándar (DEFAULT_STOP_LOSS_USD). Un SL fijado a mano
# desde el dashboard SIEMPRE se respeta.
FIRST_TRANCHE_SL_FRACTION = float(os.getenv("FIRST_TRANCHE_SL_FRACTION", "0.251"))

# ── Executor externo ──────────────────────────────────────────────────────────
EXECUTOR_URL    = os.getenv("EXECUTOR_URL",    "https://executor-5lu0.onrender.com")
EXECUTOR_SECRET = os.getenv("EXECUTOR_SECRET", "clave-secreta-aleatoria")

# ── Persistencia de estadísticas (MFE/MAE) y ajustes editables desde la web ───
# IMPORTANTE: en hosting con disco efímero (Render free, etc.) apunta estas rutas
# a un disco persistente (STATS_FILE=/data/trade_stats.jsonl) o se perderán al redeploy.
STATS_FILE    = os.getenv("STATS_FILE",    os.path.join(_HERE, "trade_stats.jsonl"))
SETTINGS_FILE = os.getenv("SETTINGS_FILE", os.path.join(_HERE, "bot_settings.json"))

# ── Recuperación tras reinicio (state_store.py) ───────────────────────────────
# Posiciones abiertas, cooldowns, trade_id, SL global e historial de cierres se
# guardan en Upstash Redis (UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN)
# y en una copia local. Sin esas variables solo queda la copia local, que en
# Render free se pierde en cada reinicio.
RECOVERY_FILE     = os.getenv("RECOVERY_FILE", os.path.join(tempfile.gettempdir(), "botshort_recovery.json"))
STATE_KEY_PREFIX  = os.getenv("STATE_KEY_PREFIX", "botshort")
STATE_TRADES_MAX  = int(os.getenv("STATE_TRADES_MAX", "5000"))
STATE_BOOT_WAIT_S = float(os.getenv("STATE_BOOT_WAIT_S", "20"))   # espera inicial al estado antes de abrir los WS
SHUTDOWN_DRAIN_S  = 5.0     # al apagar: espera a que terminen las órdenes en vuelo antes del guardado final

# True si el SL global se cambió desde la web: solo entonces viaja en el estado
# guardado (si no, manda DEFAULT_STOP_LOSS_USD del entorno).
DEFAULT_SL_FROM_UI = False


def _load_settings() -> None:
    """Restaura el SL global guardado desde la web (sobrescribe el valor de entorno)."""
    global DEFAULT_STOP_LOSS_USD, DEFAULT_SL_FROM_UI
    try:
        with open(SETTINGS_FILE, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        val = float(data.get("default_stop_loss_usd"))
        if val < 0:
            DEFAULT_STOP_LOSS_USD = val
            DEFAULT_SL_FROM_UI = True
    except Exception:
        pass


_load_settings()


# ─────────────────────────────────────────────────────────────────────────────
# MODELOS DE DATOS
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Fill:
    level:       float
    notional:    float
    entry_price: float
    qty:         float
    opened_at:   float = field(default_factory=time.time)


@dataclass
class BotPosition:
    symbol:       str
    fills:        List[Fill] = field(default_factory=list)
    realized_pnl: float = 0.0
    status:       str   = "OPEN"
    trade_id:     int   = 0
    # Stop loss vigente en USD (pérdida absoluta, valor negativo).
    sl_usd:       float = DEFAULT_STOP_LOSS_USD
    # True si el SL lo fijó el usuario desde el dashboard: el bot NO lo toca más.
    sl_manual:    bool  = False
    # ── Excursiones de la operación (PnL no realizado, USD y % del notional) ──
    opened_ts:    float = field(default_factory=time.time)
    mfe_usd:      float = 0.0    # máximo a favor  (>= 0)
    mae_usd:      float = 0.0    # máximo en contra (<= 0)
    mfe_pct:      float = 0.0
    mae_pct:      float = 0.0
    mfe_ts:       float = 0.0
    mae_ts:       float = 0.0
    low_price:    float = 0.0    # precio mínimo visto (favorable en un short)
    high_price:   float = 0.0    # precio máximo visto (adverso en un short)

    def update_excursions(self, price: float) -> None:
        """Actualiza MFE/MAE con el precio actual. Llamar con self.lock tomado."""
        if price <= 0 or not self.fills:
            return
        pnl      = self.unrealized_pnl(price)
        notional = self.notional
        pct      = (pnl / notional * 100.0) if notional > 0 else 0.0
        now      = time.time()
        if pnl > self.mfe_usd:
            self.mfe_usd, self.mfe_ts = pnl, now
        if pnl < self.mae_usd:
            self.mae_usd, self.mae_ts = pnl, now
        if pct > self.mfe_pct:
            self.mfe_pct = pct
        if pct < self.mae_pct:
            self.mae_pct = pct
        if self.low_price <= 0 or price < self.low_price:
            self.low_price = price
        if price > self.high_price:
            self.high_price = price

    def reset_excursions(self) -> None:
        self.opened_ts = time.time()
        self.mfe_usd = self.mae_usd = self.mfe_pct = self.mae_pct = 0.0
        self.mfe_ts = self.mae_ts = 0.0
        self.low_price = self.high_price = 0.0

    @property
    def qty(self) -> float:
        return sum(f.qty for f in self.fills)

    @property
    def notional(self) -> float:
        return sum(f.notional for f in self.fills)

    @property
    def avg_entry(self) -> float:
        if self.qty <= 0:
            return 0.0
        return sum(f.entry_price * f.qty for f in self.fills) / self.qty

    def unrealized_pnl(self, mark_price: float) -> float:
        if mark_price <= 0:
            return 0.0
        return sum((f.entry_price - mark_price) * f.qty for f in self.fills)

    def opened_levels(self) -> set:
        return {f.level for f in self.fills}

    # ── Stop loss automático por tramos ──────────────────────────────────────
    def auto_sl_usd(self) -> float:
        """1 solo tramo → -(notional del primer fill × 0.251).
        2 o más tramos → SL estándar (DEFAULT_STOP_LOSS_USD)."""
        if len(self.fills) == 1:
            return -abs(self.fills[0].notional) * FIRST_TRANCHE_SL_FRACTION
        return DEFAULT_STOP_LOSS_USD

    def refresh_auto_sl(self) -> None:
        """Recalcula el SL según los tramos abiertos, salvo SL manual."""
        if not self.sl_manual:
            self.sl_usd = self.auto_sl_usd()

    @property
    def sl_mode(self) -> str:
        if self.sl_manual:
            return "manual"
        return "1er tramo" if len(self.fills) == 1 else "estándar"


# ─────────────────────────────────────────────────────────────────────────────
# CLIENTE BINANCE FUTURES
# ─────────────────────────────────────────────────────────────────────────────

class BinanceFuturesClient:
    def __init__(self) -> None:
        self.exchange_filters: Dict[str, Dict[str, float]] = {}
        self.route_used = ""

    async def request(self, method: str, path: str,
                      params: Optional[dict] = None, signed: bool = False,
                      timeout: int = 15, proxy: str = "") -> Any:
        return await asyncio.to_thread(
            self._sync_request, BASE_URL, method, path, params, signed, timeout, proxy
        )

    def _sync_request(self, base_url: str, method: str, path: str,
                      params: Optional[dict] = None, signed: bool = False,
                      timeout: int = 15, proxy: str = "") -> Any:
        params  = dict(params or {})
        headers = {"User-Agent": "BOTSHORT/2.0"}
        if signed:
            if not API_KEY or not API_SECRET:
                raise RuntimeError("Faltan BINANCE_API_KEY / BINANCE_API_SECRET")
            params["timestamp"]  = int(time.time() * 1000)
            params["recvWindow"] = 5000
            query     = urlencode(params, doseq=True)
            signature = hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
            params["signature"] = signature
            headers["X-MBX-APIKEY"] = API_KEY
        elif API_KEY:
            headers["X-MBX-APIKEY"] = API_KEY

        query = urlencode(params, doseq=True)
        url   = f"{base_url}{path}" + (f"?{query}" if query else "")
        req   = urllib.request.Request(url, headers=headers, method=method.upper())
        handlers = [urllib.request.ProxyHandler({"http": proxy, "https": proxy})] if proxy else []
        opener   = urllib.request.build_opener(*handlers)
        try:
            with opener.open(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Binance HTTP {exc.code}: {body[:300]}") from exc

    async def fetch_exchange_info(self) -> Dict[str, Dict[str, float]]:
        """ÚNICA consulta REST del bot (solo al arrancar). Va por REST_PROXY_URL si
        está configurado y, si el proxy falla, se intenta una vez por la IP directa.
        Rellena y devuelve exchange_filters (símbolo → filtros)."""
        routes = [("proxy", REST_PROXY_URL), ("IP directa", "")] if REST_PROXY_URL else [("IP directa", "")]
        last_exc: Optional[Exception] = None
        data = None
        for label, proxy in routes:
            try:
                data = await self.request("GET", "/fapi/v1/exchangeInfo", proxy=proxy)
                self.route_used = label
                break
            except Exception as exc:
                last_exc = exc
                print(f"[REST] exchangeInfo por {label} falló: {exc}", flush=True)
        if data is None:
            raise RuntimeError(f"exchangeInfo falló: {last_exc}")
        filters: Dict[str, Dict[str, float]] = {}
        for sym in data.get("symbols", []):
            if sym.get("quoteAsset")    != QUOTE_ASSET:  continue
            if sym.get("contractType")  != "PERPETUAL":  continue
            if sym.get("status")        != "TRADING":    continue
            row = {"stepSize": 0.001, "minQty": 0.0, "minNotional": 5.0}
            for f in sym.get("filters", []):
                if f.get("filterType") == "LOT_SIZE":
                    row["stepSize"] = float(f.get("stepSize", row["stepSize"]))
                    row["minQty"]   = float(f.get("minQty",   row["minQty"]))
                if f.get("filterType") == "MIN_NOTIONAL":
                    row["minNotional"] = float(f.get("notional", row["minNotional"]))
            filters[sym["symbol"]] = row
        self.exchange_filters = filters
        return filters

    def normalize_qty(self, symbol: str, qty: float) -> float:
        info = self.exchange_filters.get(symbol, {"stepSize": 0.001, "minQty": 0.0})
        step = info["stepSize"]
        norm = floor(qty / step) * step
        decs = max(0, len(f"{step:.12f}".rstrip("0").split(".")[-1]))
        norm = round(norm, decs)
        return norm if norm >= info.get("minQty", 0.0) else 0.0

    async def market_short(self, symbol: str, notional: float, price: float) -> float:
        min_notional = self.exchange_filters.get(symbol, {}).get("minNotional", 5.0)
        effective    = max(notional, min_notional)
        qty          = self.normalize_qty(symbol, effective / price)
        if qty <= 0:
            raise RuntimeError(f"Qty inválida {symbol}: notional={effective} price={price}")
        if PAPER_MODE or not LIVE_TRADING:
            return qty
        # Sin consulta de leverage: se usa el apalancamiento ya configurado en la cuenta.
        await self.request("POST", "/fapi/v1/order",
            {"symbol": symbol, "side": "SELL", "type": "MARKET", "quantity": qty},
            signed=True)
        return qty

    async def close_short(self, symbol: str, qty: float) -> None:
        qty = self.normalize_qty(symbol, qty)
        if qty <= 0 or PAPER_MODE or not LIVE_TRADING:
            return
        # Timeout 10 s: debe completarse antes del timeout de Flask (30 s)
        await self.request("POST", "/fapi/v1/order",
            {"symbol": symbol, "side": "BUY", "type": "MARKET",
             "quantity": qty, "reduceOnly": "true"},
            signed=True, timeout=10)




# ─────────────────────────────────────────────────────────────────────────────
# BOT PRINCIPAL (motor por eventos)
# ─────────────────────────────────────────────────────────────────────────────

class TradingBot:
    def __init__(self) -> None:
        self.client   = BinanceFuturesClient()
        self.positions: Dict[str, BotPosition] = {}
        self.closed_trades: List[dict] = []
        self.events:        List[str]  = []
        self.lock = threading.Lock()
        self._trade_id_lock = threading.Lock()

        # Cooldown: symbol → timestamp hasta el que está bloqueado
        self.symbol_cooldown: Dict[str, float] = {}

        # Bloqueo permanente por precio
        self.price_blocked: set = set()

        # ── Lista de símbolos operables (REST + caché en disco, refresh c/12h) ──
        self.all_symbols:              List[str] = []
        self._tradable:                frozenset = frozenset()
        self._price_block_reset_at:    float     = time.time()

        # ── WS caches ─────────────────────────────────────────────────────
        self.price_cache:   Optional[SymbolWebSocketPriceCache] = None
        self.kline_cache:   Optional[KlineWebSocketStream]      = None

        # ── Radar (símbolos con bookTicker suscrito). Copy-on-write: SOLO el
        #    loop del bot lo reasigna; el resto (hilo WS, Flask) solo lee. ──
        self.watch: frozenset = frozenset()

        # ── Motor por eventos ─────────────────────────────────────────────
        self._tick_event:   Optional[asyncio.Event] = None   # se crea en el loop del bot
        self._pending:      Dict[str, float] = {}            # símbolo → instante del primer tick pendiente
        self._pending_lock  = threading.Lock()
        self._wake_scheduled = False
        self._bg_tasks:     set = set()
        self._entry_inflight: set = set()                    # símbolos con una tanda de entradas en curso
        self._entry_reserved: set = set()                    # (símbolo, nivel) reservados antes de enviar la orden
        self._entry_backoff:  Dict[str, float] = {}
        self._close_backoff:  Dict[str, float] = {}
        self._log_throttle:   Dict[str, float] = {}

        # ── Métricas ──────────────────────────────────────────────────────
        self.running              = False
        self.scan_count           = 0           # evaluaciones realizadas (1 por símbolo y tick)
        self.last_scan_at         = 0.0
        self.eval_rate            = 0.0         # evaluaciones/s (última ventana de 1 s)
        self.latency_avg_ms       = 0.0         # tick recibido → evaluado (media, ventana 1 s)
        self.latency_max_ms       = 0.0         # idem, máximo
        self._lat_sum = 0.0
        self._lat_n   = 0
        self._lat_max = 0.0
        self.last_error           = ""
        self.last_startup_err     = ""
        self.exchange_symbols     = 0
        self.started_at           = time.time()

        self.loop:   Optional[asyncio.AbstractEventLoop] = None
        self.thread: Optional[threading.Thread] = None

        # ── Persistencia asíncrona ────────────────────────────────────────
        self._persist_event: Optional[asyncio.Event] = None
        self._persist_debounce_secs: float = 0.35

        # ── Guardia anti doble cierre ─────────────────────────────────────
        self._closing_symbols: set[str] = set()

        # ── Executor bridge ───────────────────────────────────────────────
        self._trade_id_seq: int = 0
        self.executor = ExecutorBridge(
            executor_url=EXECUTOR_URL,
            signal_secret=EXECUTOR_SECRET,
        )
        # total PnL realizado acumulado (suma de todos los cierres)
        self.total_realized_pnl: float = 0.0

        # ── Estadísticas MFE/MAE por operación cerrada (se cargan del disco) ──
        self.trade_stats: List[dict] = []
        self._stats_lock = threading.Lock()
        self._load_trade_stats()

        # ── Persistencia externa para recuperar el estado tras un reinicio ──
        backend, self._store_desc = backend_from_env(self.log)
        self.store = StateStore(
            backend, RECOVERY_FILE, make_owner_id(),
            build_doc=self._recovery_doc, log=self.log, on_fenced=self._on_fenced,
            key_prefix=STATE_KEY_PREFIX, max_trades=STATE_TRADES_MAX,
        )

    # ── Logging ───────────────────────────────────────────────────────────────

    def log(self, msg: str) -> None:
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        line  = f"{stamp} | {msg}"
        print(line, flush=True)
        with self.lock:
            self.events = [line, *self.events[:99]]

    # ── Start / Stop ──────────────────────────────────────────────────────────

    def start(self) -> None:
        if self.running:
            return
        self.running = True
        self.thread  = threading.Thread(
            target=self._run_loop, daemon=True, name="BotLoop"
        )
        self.thread.start()

    def stop(self) -> None:
        self.log("Deteniendo bot...")
        self.running = False
        self._stop_price_cache()
        self._stop_kline_cache()

    def _run_loop(self) -> None:
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self._persist_event = asyncio.Event()
        self._tick_event    = asyncio.Event()
        try:
            self.loop.run_until_complete(self._main())
        except Exception as exc:
            self.running    = False
            self.last_error = str(exc)
            self.log(f"Bot detenido por error no controlado: {exc}")

    # ── Supervisor ────────────────────────────────────────────────────────────

    async def _supervised(self, coro_factory, name: str, restart_delay: float = 2.0):
        """Envuelve una corrutina con reinicio automático."""
        while self.running:
            try:
                await coro_factory()
            except asyncio.CancelledError:
                if not self.running:
                    break
                self.log(f"[supervisor] {name}: CancelledError — relanzando en {restart_delay}s...")
            except Exception as exc:
                if not self.running:
                    break
                self.last_error = str(exc)
                self.log(f"[supervisor] {name}: excepción '{exc}' — relanzando en {restart_delay}s...")
            else:
                if not self.running:
                    break
                self.log(f"[supervisor] {name}: retornó inesperadamente — relanzando en {restart_delay}s...")

            try:
                await asyncio.sleep(restart_delay)
            except asyncio.CancelledError:
                if not self.running:
                    break

    # ── Main ──────────────────────────────────────────────────────────────────

    def _next_trade_id(self) -> int:
        """Genera un trade_id único e incremental sin reentrar en self.lock."""
        with self._trade_id_lock:
            self._trade_id_seq += 1
            return self._trade_id_seq

    async def _main(self) -> None:
        # Conectar el logger del executor al sistema de log del bot
        self.executor.logger = self.log
        if EXECUTOR_URL:
            self.log(f"[executor] Bridge configurado → {EXECUTOR_URL}")
        else:
            self.log("[executor] EXECUTOR_URL no configurado — señales desactivadas")

        self.log("Bot iniciado — modo " + (
            "PAPER" if PAPER_MODE or not LIVE_TRADING else "REAL"
        ))
        # Recuperación: pide el control del estado en Upstash mientras carga lo demás
        self.log(f"[estado] Persistencia: {self._store_desc}")
        self.store.start()

        # Lista de perpetuos operables: caché en disco o UNA petición REST
        await self._init_all_symbols()
        self.exchange_symbols = len(self.client.exchange_filters)
        self._update_tradable()

        # Las posiciones se restauran ANTES de abrir los WebSockets y el motor:
        # así se suscriben desde el inicio y ningún tick abre un duplicado.
        if await asyncio.to_thread(self.store.acquired.wait, STATE_BOOT_WAIT_S):
            self._restore_from_store()
        else:
            # Otra instancia sigue siendo la dueña (deploy con solapamiento) o Upstash
            # no responde: el mantenimiento aplicará el estado en cuanto llegue.
            self.log("[estado] ⚠️ Aún no controlo el estado (otra instancia activa o Upstash no responde): "
                     "sin entradas ni cierres hasta recuperarlo")

        if not _ALL_MARKET_ENABLED:
            self.log("⚠️ WS_ALL_MARKET=false: sin !ticker@arr/!markPrice@arr el bot NO puede "
                     "descubrir símbolos. Pon WS_ALL_MARKET=true.")

        # UNA sola conexión WS permanente; el callback despierta al motor en cada tick
        self._start_price_cache()
        # Velas 1m de TODOS los símbolos por WebSocket (sin REST, sin reinicios)
        self._start_kline_cache(self.all_symbols)

        tasks = [
            asyncio.create_task(self._supervised(self._engine_loop,             "_engine_loop")),
            asyncio.create_task(self._supervised(self._maintenance_loop,        "_maintenance_loop")),
            asyncio.create_task(self._supervised(self._persist_state_loop,      "_persist_state_loop")),
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    # ── Gestión de WS caches ──────────────────────────────────────────────────

    def _stop_price_cache(self) -> None:
        if self.price_cache:
            try:
                self.price_cache.stop()
            except Exception:
                pass
            time.sleep(WS_STOP_GRACE)
            self.price_cache = None

    def _stop_kline_cache(self) -> None:
        if self.kline_cache:
            try:
                self.kline_cache.stop()
            except Exception:
                pass
            time.sleep(WS_STOP_GRACE)
            self.kline_cache = None

    def _open_position_symbols(self) -> List[str]:
        with self.lock:
            return [
                sym for sym, pos in self.positions.items()
                if pos.status == "OPEN" and pos.fills
            ]

    def _start_price_cache(self) -> None:
        """Arranca el ÚNICO caché WS (permanente). Nace con las posiciones
        abiertas (si las hay); el radar crece solo con los ticks 24h."""
        self._stop_price_cache()
        initial = self._open_position_symbols()
        self.price_cache = SymbolWebSocketPriceCache(initial)
        self.price_cache.set_update_callback(self._on_ws_event)
        self.watch = frozenset(initial)
        self.price_cache.start()
        self.log(
            f"PriceCache permanente iniciado: !ticker@arr + !markPrice@arr (todos los símbolos) "
            f"+ bookTicker ms para el radar (≥{MIN_GAIN_FILTER:.0f}% o posición abierta)"
        )

    def _start_kline_cache(self, symbols: List[str]) -> None:
        """Arranca UNA vez el stream de velas (una conexión por intervalo) para
        todos los símbolos. Los que entren después al radar se añaden con
        ensure_symbols()."""
        self._stop_kline_cache()
        self.kline_cache = KlineWebSocketStream(
            symbols, intervals=KLINE_INTERVALS, history=KLINE_HISTORY, dtype=KLINE_DTYPE,
        )
        self.kline_cache.start()
        self.log(f"Velas por WebSocket ({', '.join(KLINE_INTERVALS)}): {len(symbols)} símbolos, "
                 f"{KLINE_HISTORY} velas cerradas por símbolo, {KLINE_DTYPE} (sin REST)")

    # ── Símbolos operables (caché en disco + REST) ────────────────────────────

    def _update_tradable(self) -> None:
        self._tradable = frozenset(self.all_symbols)

    def _is_tradable(self, symbol: str) -> bool:
        tradable = self._tradable
        if tradable:
            return symbol in tradable
        # Sin lista (REST caído): se aceptan perpetuos USDT y se descartan trimestrales (BTCUSDT_250627)
        return symbol.endswith(QUOTE_ASSET) and "_" not in symbol

    def _load_symbols_cache(self) -> Optional[dict]:
        """Lee {symbols, filters, saved_at} del caché en disco (None si no hay)."""
        try:
            with open(SYMBOLS_CACHE_FILE, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data.get("symbols"), list) and data["symbols"]:
                return data
        except FileNotFoundError:
            pass
        except Exception as exc:
            self.log(f"No pude leer caché de símbolos: {exc}")
        return None

    def _save_symbols_cache(self, symbols: List[str], filters: Dict[str, Dict[str, float]]) -> None:
        tmp = f"{SYMBOLS_CACHE_FILE}.tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump({"symbols": symbols, "filters": filters, "saved_at": time.time()}, fh)
            os.replace(tmp, SYMBOLS_CACHE_FILE)
        except Exception as exc:
            self.log(f"No pude guardar caché de símbolos: {exc}")

    async def _init_all_symbols(self) -> None:
        """Lista de perpetuos y filtros, con como mucho UNA petición REST:
          1. Caché en disco con menos de SYMBOL_REFRESH_HOURS → 0 peticiones.
          2. exchangeInfo (proxy si hay REST_PROXY_URL, si no IP directa).
          3. Si falla: caché viejo, y como último recurso INITIAL_SYMBOLS.
        Después de esto el bot no vuelve a usar REST para datos de mercado."""
        cached = self._load_symbols_cache()
        age_h  = (time.time() - float(cached.get("saved_at", 0))) / 3600 if cached else None
        if cached and cached.get("filters") and age_h is not None and age_h < SYMBOL_REFRESH_HOURS:
            self.all_symbols = list(cached["symbols"])
            self.client.exchange_filters = dict(cached["filters"])
            self.log(f"Símbolos desde caché en disco: {len(self.all_symbols)} "
                     f"(hace {age_h:.1f} h) — sin peticiones REST")
            return

        try:
            filters = await self.client.fetch_exchange_info()
            self.all_symbols = sorted(filters)
            self._save_symbols_cache(self.all_symbols, filters)
            self.log(f"REST exchangeInfo ({self.client.route_used}): "
                     f"{len(self.all_symbols)} perpetuos {QUOTE_ASSET} — única petición REST de datos")
            return
        except Exception as exc:
            self.last_startup_err = str(exc)
            self.last_error = str(exc)
            self.log(f"exchangeInfo falló ({exc}).")

        if cached:
            self.all_symbols = list(cached["symbols"])
            self.client.exchange_filters = dict(cached.get("filters") or {})
            self.log(f"Usando caché viejo de símbolos: {len(self.all_symbols)} (hace {age_h:.1f} h)")
        elif INITIAL_SYMBOLS:
            self.all_symbols = INITIAL_SYMBOLS.copy()
            self.log(f"Usando INITIAL_SYMBOLS: {len(self.all_symbols)} símbolos (sin filtros de lote)")
        else:
            self.log("ADVERTENCIA: sin lista de símbolos; se aceptan perpetuos "
                     f"{QUOTE_ASSET} vistos por WebSocket.")

    # ── Motor por eventos ─────────────────────────────────────────────────────
    #
    # Camino de un tick:
    #   WS (hilo ws-price-cache) → _on_ws_event → _enqueue → loop.call_soon_threadsafe(set)
    #   → _engine_loop despierta → _evaluate_symbol (entradas / TP / SL)
    # Sin sleeps ni ciclos: el único retraso es el viaje del propio mensaje.

    def _on_ws_event(self, symbol: str, kind: str) -> None:
        """Callback del WS (hilo del WS, fuera de locks): debe ser instantáneo."""
        if symbol in self.watch:                    # radar o posición: cualquier tick evalúa
            self._enqueue(symbol)
            return
        if kind != "ticker":                        # fuera del radar solo interesa el ticker 24h
            return
        pc = self.price_cache
        t  = pc.ticker_cache.get(symbol) if pc is not None else None
        if t is None or t.change_pct < MIN_GAIN_FILTER:
            return
        if self._is_tradable(symbol):
            self._enqueue(symbol)                   # candidato nuevo: el motor lo mete al radar

    def _enqueue(self, symbol: str) -> None:
        now = time.perf_counter()
        with self._pending_lock:
            if symbol not in self._pending:
                self._pending[symbol] = now
            if self._wake_scheduled:
                return
            self._wake_scheduled = True
        loop, ev = self.loop, self._tick_event
        try:
            if loop is None or ev is None:
                raise RuntimeError("loop no listo")
            loop.call_soon_threadsafe(ev.set)
        except Exception:
            with self._pending_lock:
                self._wake_scheduled = False

    async def _engine_loop(self) -> None:
        self.log(
            f"Motor por eventos activo (sin ciclos de filtrado ni sleeps de detección | "
            f"radar ≥{MIN_GAIN_FILTER:.0f}% | red de seguridad {SAFETY_TICK_SECS:g}s)"
        )
        ev = self._tick_event
        while self.running:
            await ev.wait()
            ev.clear()
            with self._pending_lock:
                batch = self._pending
                self._pending = {}
                self._wake_scheduled = False
            for sym, t_in in batch.items():
                lat = (time.perf_counter() - t_in) * 1000.0
                self._lat_sum += lat
                self._lat_n   += 1
                if lat > self._lat_max:
                    self._lat_max = lat
                try:
                    self._evaluate_symbol(sym)
                except Exception as exc:
                    self.last_error = str(exc)
                    self._log_throttled("eval_err", f"Error evaluando {sym}: {exc!r}")
            # Cede el loop: sin esto, un flujo continuo de ticks dejaría sin turno
            # a las tareas de órdenes (entradas/cierres).
            await asyncio.sleep(0)

    def _log_throttled(self, key: str, msg: str, secs: float = 5.0) -> None:
        now = time.time()
        if now - self._log_throttle.get(key, 0.0) >= secs:
            self._log_throttle[key] = now
            self.log(msg)

    def _spawn(self, coro) -> None:
        """Lanza una tarea en segundo plano (órdenes) sin bloquear el motor."""
        task = asyncio.get_running_loop().create_task(coro)
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_done)

    def _bg_done(self, task) -> None:
        self._bg_tasks.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            self.last_error = str(exc)
            self._log_throttled("bg_err", f"Tarea en segundo plano falló: {exc!r}")

    # ── Precio / cambio en vivo ───────────────────────────────────────────────

    def _price_for(self, symbol: str) -> Optional[float]:
        """Precio para OPERAR: mid del bookTicker (o mark) con antigüedad máxima
        PRICE_MAX_AGE_S; si no hay, último precio del ticker 24h con la misma
        regla. None = no operar con un dato viejo."""
        pc = self.price_cache
        if pc is None:
            return None
        now = time.time()
        e = pc.price_cache.get(symbol)
        if e and now - e[1] <= PRICE_MAX_AGE_S:
            return e[0]
        t = pc.ticker_cache.get(symbol)
        if t and t.last_price > 0 and now - t.ts <= PRICE_MAX_AGE_S:
            return t.last_price
        return None

    def _display_price(self, symbol: str) -> float:
        """Precio para MOSTRAR: el fresco si lo hay, si no el último conocido."""
        pc = self.price_cache
        if pc is None:
            return 0.0
        p = self._price_for(symbol)
        if p:
            return p
        e = pc.price_cache.get(symbol)
        if e:
            return e[0]
        m = pc.mark_cache.get(symbol)
        if m:
            return m[0]
        t = pc.ticker_cache.get(symbol)
        return t.last_price if t else 0.0

    def _change_for(self, symbol: str, price: float) -> float:
        """% de cambio 24h en vivo: (precio / open24h - 1) con el precio del tick.
        Si no hay open24h cae al % del ticker (que se refresca cada ~1 s)."""
        pc = self.price_cache
        t  = pc.ticker_cache.get(symbol) if pc is not None else None
        if t is None:
            return 0.0
        if USE_LIVE_CHANGE and t.open_24h > 0 and price > 0:
            return (price / t.open_24h - 1.0) * 100.0
        return t.change_pct

    # ── Radar ─────────────────────────────────────────────────────────────────

    def _watch_add(self, symbol: str) -> None:
        """Mete un símbolo en el radar y suscribe su bookTicker (ms). Solo desde el loop."""
        if symbol in self.watch:
            return
        self.watch = self.watch | {symbol}
        pc = self.price_cache
        if pc is not None:
            try:
                pc.ensure_symbols([symbol])
            except Exception as exc:
                self.last_error = str(exc)
                self._log_throttled("watch_add", f"No pude suscribir {symbol}: {exc}")
        kc = self.kline_cache
        if kc is not None:
            kc.ensure_symbols([symbol])         # no-op si ya estaba (normalmente sí)
        self.log(f"RADAR + {symbol} ({len(self.watch)} vigilados)")

    def _prune_watch(self) -> None:
        """Saca del radar lo que ya no cumple (sin posición y por debajo de
        MIN_GAIN_FILTER - histéresis) y da de baja su bookTicker."""
        pc = self.price_cache
        if pc is None:
            return
        pos_syms = set(self._open_position_symbols())
        floor_pct = MIN_GAIN_FILTER - WATCH_PRUNE_HYSTERESIS
        keep, dropped = set(), []
        for sym in self.watch:
            if sym in pos_syms:
                keep.add(sym)
                continue
            t = pc.ticker_cache.get(sym)
            if t is not None and t.change_pct >= floor_pct:
                keep.add(sym)
            else:
                dropped.append(sym)
        if not dropped:
            return
        self.watch = frozenset(keep)
        try:
            pc.update_symbols(sorted(keep | pos_syms))
        except Exception as exc:
            self.last_error = str(exc)
        self.log(f"RADAR - {len(dropped)} fuera ({', '.join(sorted(dropped)[:6])}"
                 f"{'…' if len(dropped) > 6 else ''}) → {len(self.watch)} vigilados")

    # ── Evaluación de un símbolo (se ejecuta en CADA tick) ────────────────────

    def _evaluate_symbol(self, symbol: str) -> None:
        pc = self.price_cache
        if pc is None:
            return

        with self.lock:
            pos = self.positions.get(symbol)
            has_pos = bool(pos and pos.status == "OPEN" and pos.fills)

        if symbol not in self.watch:
            if has_pos:
                self._watch_add(symbol)
            else:
                t = pc.ticker_cache.get(symbol)
                if t is None or t.change_pct < MIN_GAIN_FILTER or not self._is_tradable(symbol):
                    return
                self._watch_add(symbol)

        price = self._price_for(symbol)
        if price is None or price <= 0:
            return

        self.scan_count  += 1
        self.last_scan_at = time.time()

        if has_pos:
            with self.lock:
                p_ex = self.positions.get(symbol)
                if p_ex is not None and p_ex.status == "OPEN":
                    p_ex.update_excursions(price)
            self._check_exits(symbol, price)

        change = self._change_for(symbol, price)
        if change >= ENTRY_LEVELS[0]:
            self._check_entries(symbol, price, change)

    def _check_exits(self, symbol: str, price: float) -> None:
        """TP / SL. Pre-chequeo barato en el loop; el cierre real va a una tarea."""
        if symbol in self._closing_symbols or not self.store.can_act():
            return
        if time.time() < self._close_backoff.get(symbol, 0.0):
            return
        with self.lock:
            pos = self.positions.get(symbol)
            if not pos or pos.status != "OPEN" or not pos.fills:
                return
            pnl    = pos.unrealized_pnl(price)
            target = pos.notional * TAKE_PROFIT_FRACTION
            sl_usd = pos.sl_usd
        if pnl <= sl_usd:
            reason = "SL"
        elif pnl >= target:
            reason = "TP"
        else:
            return
        if not self._begin_close_guard(symbol):       # se toma AQUÍ, antes de crear la tarea
            return
        self._spawn(self._close_position(symbol, price, reason, acquired=True))

    def _check_entries(self, symbol: str, price: float, change: float) -> None:
        if symbol in self._entry_inflight or symbol in self._closing_symbols:
            return
        now = time.time()
        if not self.store.can_open():
            # Sin persistencia confirmada una posición nueva se perdería en un reinicio.
            if now - self._log_throttle.get("entries_paused", 0.0) >= 60.0:
                self._log_throttled("entries_paused", "Entradas en pausa: persistencia del estado no "
                                    f"confirmada ({self.store.status()['mode']})", secs=60.0)
            return
        if now < self._entry_backoff.get(symbol, 0.0):
            return
        with self.lock:
            if symbol in self.price_blocked:
                return
            if self.symbol_cooldown.get(symbol, 0.0) > now:
                return
            pos = self.positions.get(symbol)
            if pos is not None and pos.status != "OPEN":
                return
            opened = pos.opened_levels() if pos else set()

        due = [
            (lvl, nt) for lvl, nt in zip(ENTRY_LEVELS, ENTRY_NOTIONALS)
            if change >= lvl and lvl not in opened and (symbol, lvl) not in self._entry_reserved
        ]
        if not due:
            return

        if price > MAX_PRICE_BLOCK:
            self._block_by_price(symbol, price)
            return
        if not self._kline_entry_ok(symbol):
            return

        self._entry_inflight.add(symbol)
        self._spawn(self._enter_levels(symbol, due))

    def _block_by_price(self, symbol: str, price: float) -> None:
        with self.lock:
            newly = symbol not in self.price_blocked
            self.price_blocked.add(symbol)
        if newly:
            self.persist_state(critical=False)
            self.log(f"BLOQUEADO permanente {symbol}: precio {price:.4f} > {MAX_PRICE_BLOCK} USD")

    async def _enter_levels(self, symbol: str, due: list) -> None:
        """Abre, en orden, los tramos vencidos. Corre como tarea: el motor no espera a la orden."""
        opened_any = False
        try:
            for level, notional in due:
                price = self._price_for(symbol)
                if price is None:
                    break
                change = self._change_for(symbol, price)
                if change < level:          # el precio retrocedió: lo reevalúa el siguiente tick
                    break
                if price > MAX_PRICE_BLOCK:
                    self._block_by_price(symbol, price)
                    break
                if not await self._ensure_short(symbol, level, notional, price, change):
                    break
                opened_any = True
        finally:
            self._entry_inflight.discard(symbol)
            if opened_any:                  # reevalúa ya (TP/SL del nuevo tramo, niveles que falten)
                self._enqueue(symbol)

    # ── Condición kline ───────────────────────────────────────────────────────

    def _kline_entry_ok(self, symbol: str) -> bool:
        """True si la última vela 1m CERRADA (la del minuto anterior) es alcista.
        Sin esa vela (recién arrancado, o se perdió un cierre en una reconexión)
        decide KLINE_ALLOW_NO_DATA."""
        kc = self.kline_cache
        last = kc.last_closed(symbol, "1m") if kc is not None else None
        if last is None:
            return KLINE_ALLOW_NO_DATA
        return last.bullish

    # ── Cooldown helpers ──────────────────────────────────────────────────────

    def _cooldown_remaining(self, symbol: str) -> float:
        unblock_at = self.symbol_cooldown.get(symbol, 0.0)
        return max(0.0, unblock_at - time.time())

    @staticmethod
    def _fmt_cooldown(seconds: float) -> str:
        s = int(seconds)
        h = s // 3600
        m = (s % 3600) // 60
        r = s % 60
        if h > 0:  return f"{h}h {m:02d}m"
        if m > 0:  return f"{m}m {r:02d}s"
        return f"{r}s"

    def _begin_close_guard(self, symbol: str) -> bool:
        """Evita cierres duplicados del mismo símbolo."""
        with self.lock:
            if symbol in self._closing_symbols:
                return False
            pos = self.positions.get(symbol)
            if not pos or pos.status != "OPEN" or not pos.fills:
                return False
            self._closing_symbols.add(symbol)
            return True

    def _end_close_guard(self, symbol: str) -> None:
        with self.lock:
            self._closing_symbols.discard(symbol)

    # ── Estrategia ────────────────────────────────────────────────────────────

    async def _ensure_short(self, symbol: str, level: float, notional: float,
                            price: float, change: float) -> bool:
        """Abre el tramo `level`. True si se abrió. El nivel se RESERVA antes de
        enviar la orden para que ningún tick lo duplique mientras está en vuelo."""
        key = (symbol, level)
        if not self.store.can_open():
            return False
        with self.lock:
            if self.symbol_cooldown.get(symbol, 0.0) > time.time():
                return False
            if symbol in self._closing_symbols or key in self._entry_reserved:
                return False
            pos = self.positions.get(symbol)
            if pos is not None and (level in pos.opened_levels() or pos.status != "OPEN"):
                return False
            trade_id = pos.trade_id if (pos is not None and pos.trade_id) else 0
            self._entry_reserved.add(key)

        try:
            if trade_id == 0:
                trade_id = self._next_trade_id()
            try:
                qty = await self.client.market_short(symbol, notional, price)
            except Exception as exc:
                self.last_error = str(exc)
                self._entry_backoff[symbol] = time.time() + ENTRY_ERROR_BACKOFF_S
                self.log(f"Error abriendo short {symbol} nivel {level}: {exc}")
                return False

            fill = Fill(level=level, notional=notional, entry_price=price, qty=qty)
            with self.lock:
                pos = self.positions.get(symbol)
                if pos is None:
                    pos = BotPosition(symbol=symbol, trade_id=trade_id)
                    self.positions[symbol] = pos
                elif pos.trade_id == 0:
                    pos.trade_id = trade_id
                else:
                    trade_id = pos.trade_id
                pos.fills.append(fill)
                pos.refresh_auto_sl()           # 1 tramo → notional×0.251 | 2+ → SL estándar | manual → intacto
                sl_now, sl_mode, n_fills = pos.sl_usd, pos.sl_mode, len(pos.fills)

            self._watch_add(symbol)             # la posición abierta siempre queda vigilada
            self.log(
                f"SHORT {symbol}: nivel {level:.0f}% | {notional:.2f} USDT | "
                f"qty={qty} | px={price:.6f} | cambio(WS)={change:.2f}% | trade_id={trade_id} | "
                f"tramos={n_fills} | SL={sl_now:.4f} USD ({sl_mode})"
            )
            self.executor.notify_open(
                trade_id=trade_id,
                symbol=symbol,
                direction="SHORT",
                price=price,
                quantity=qty,
                notional=notional,
                level=level,
            )
            self.persist_state()
            return True
        finally:
            with self.lock:
                self._entry_reserved.discard(key)

    async def _close_position(self, symbol: str, price: float, reason: str,
                              acquired: bool = False) -> bool:
        """Cierre único para TP / SL / MANUAL (antes había 3 copias del mismo código)."""
        if not self.store.can_act():                  # standby: otra instancia es la dueña
            if acquired:
                self._end_close_guard(symbol)
            return False
        if not acquired and not self._begin_close_guard(symbol):
            return False
        try:
            with self.lock:
                pos = self.positions.get(symbol)
                if not pos or pos.status != "OPEN" or not pos.fills:
                    return False
                fills_n   = len(pos.fills)
                snapshot  = list(pos.fills)
                qty       = sum(f.qty for f in snapshot)
                notional  = sum(f.notional for f in snapshot)
                avg_ent   = (sum(f.entry_price * f.qty for f in snapshot) / qty) if qty > 0 else 0.0
                trade_id  = pos.trade_id
                sl_usd    = pos.sl_usd
                target    = notional * TAKE_PROFIT_FRACTION
                pnl       = sum((f.entry_price - price) * f.qty for f in snapshot) if price > 0 else 0.0
                # Re-verifica la condición con el estado actual (pudo cambiar el SL desde el dashboard)
                if reason == "SL" and pnl > sl_usd:
                    return False
                if reason == "TP" and pnl < target:
                    return False
                # Último punto de la operación: el precio de cierre también cuenta
                pos.update_excursions(price)
                now_ts   = time.time()
                duration = now_ts - pos.opened_ts
                exc = {
                    "mfe_usd":    pos.mfe_usd,
                    "mae_usd":    pos.mae_usd,
                    "mfe_pct":    pos.mfe_pct,
                    "mae_pct":    pos.mae_pct,
                    "duration_s": duration,
                }
                stat_rec = {
                    "trade_id":      trade_id,
                    "symbol":        symbol,
                    "direction":     "SHORT",
                    "reason":        reason,
                    "opened_at_ts":  pos.opened_ts,
                    "closed_at_ts":  now_ts,
                    "duration_s":    duration,
                    "avg_entry":     avg_ent,
                    "close_price":   price,
                    "qty":           qty,
                    "notional":      notional,
                    "fills_n":       fills_n,
                    "levels":        [f.level for f in snapshot],
                    "sl_usd":        sl_usd,
                    "target":        target,
                    "pnl":           pnl,
                    "pnl_pct":       (pnl / notional * 100.0) if notional > 0 else 0.0,
                    "mfe_usd":       pos.mfe_usd,
                    "mae_usd":       pos.mae_usd,
                    "mfe_pct":       pos.mfe_pct,
                    "mae_pct":       pos.mae_pct,
                    "time_to_mfe_s": (pos.mfe_ts - pos.opened_ts) if pos.mfe_ts else 0.0,
                    "time_to_mae_s": (pos.mae_ts - pos.opened_ts) if pos.mae_ts else 0.0,
                    "low_price":     pos.low_price,
                    "high_price":    pos.high_price,
                }

            try:
                await self.client.close_short(symbol, qty)
            except Exception as exc:
                self.last_error = str(exc)
                self._close_backoff[symbol] = time.time() + CLOSE_ERROR_BACKOFF_S
                label = {"SL": "STOP LOSS", "TP": "short", "MANUAL": "cierre manual"}.get(reason, reason)
                self.log(f"Error cerrando {label} {symbol}: {exc}")
                return False

            unblock_str = ""
            leftover = 0
            with self.lock:
                pos = self.positions.get(symbol)
                if pos:
                    extra = pos.fills[fills_n:]
                    self.total_realized_pnl += pnl
                    if extra:
                        # Se abrió otro tramo MIENTRAS se cerraba: no estaba en la orden de
                        # cierre, así que la posición sigue abierta solo con esos fills.
                        pos.fills = list(extra)
                        leftover  = len(extra)
                        pos.reset_excursions()      # la posición restante empieza sus excursiones de cero
                    else:
                        self.positions.pop(symbol, None)
                        pos.status       = "CLOSED"
                        pos.realized_pnl = pnl
                        unblock_ts  = time.time() + COOLDOWN_SECONDS
                        self.symbol_cooldown[symbol] = unblock_ts
                        unblock_str = datetime.fromtimestamp(
                            unblock_ts, timezone.utc
                        ).strftime("%Y-%m-%d %H:%M UTC")
                    self.closed_trades.insert(0, {
                        "symbol":      symbol,
                        "pnl":         pnl,
                        "target":      target,
                        "qty":         qty,
                        "avg_entry":   avg_ent,
                        "close_price": price,
                        "notional":    notional,
                        "closed_at":   datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                        "unblock_at":  unblock_str,
                        "reason":      reason,
                        **exc,
                    })
                    self.closed_trades = self.closed_trades[:500]
                # Misma sección crítica que saca la posición: el guardado lleva las dos cosas juntas
                self.store.append_trade(stat_rec)

            self._record_trade_stat(stat_rec)
            self.executor.notify_close(
                trade_id=trade_id,
                symbol=symbol,
                direction="SHORT",
                reason=reason,
                close_price=price,
                pnl=pnl,
            )
            if reason == "TP":
                self.log(
                    f"CIERRE TP {symbol}: PnL={pnl:.4f} | objetivo={target:.4f} | "
                    f"px={price:.6f} | bloqueado {COOLDOWN_SECONDS // 3600}h hasta {unblock_str}"
                )
            elif reason == "SL":
                self.log(
                    f"⛔ STOP LOSS {symbol}: PnL={pnl:.4f} | SL configurado={sl_usd:.4f} | "
                    f"px={price:.6f} | bloqueado {COOLDOWN_SECONDS // 3600}h hasta {unblock_str}"
                )
            else:
                self.log(
                    f"✋ CIERRE MANUAL {symbol}: PnL={pnl:.4f} | "
                    f"px={price:.6f} | bloqueado {COOLDOWN_SECONDS // 3600}h hasta {unblock_str}"
                )
            if leftover:
                self.log(f"⚠️ {symbol}: {leftover} tramo(s) se abrieron durante el cierre; "
                         f"la posición sigue abierta con ellos (sin cooldown)")
            self.persist_state()
            return True
        finally:
            self._end_close_guard(symbol)

    # ── Estadísticas MFE / MAE ────────────────────────────────────────────────

    def _load_trade_stats(self) -> None:
        """Carga el histórico de operaciones (una línea JSON por cierre)."""
        try:
            if not os.path.exists(STATS_FILE):
                return
            rows: List[dict] = []
            with open(STATS_FILE, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rows.append(json.loads(line))
                    except Exception:
                        continue
            self.trade_stats = rows
            max_id = max((int(r.get("trade_id", 0) or 0) for r in rows), default=0)
            if max_id > self._trade_id_seq:
                self._trade_id_seq = max_id        # evita repetir trade_id tras reinicios
        except Exception as exc:
            print(f"No pude leer {STATS_FILE}: {exc}", flush=True)

    def _record_trade_stat(self, rec: dict) -> None:
        with self._stats_lock:
            self.trade_stats.append(rec)
        try:
            with open(STATS_FILE, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        except Exception as exc:
            self._log_throttled("stats_write", f"No pude guardar estadística de {rec.get('symbol')}: {exc}")

    @staticmethod
    def _pctl(sorted_vals: List[float], p: float) -> float:
        if not sorted_vals:
            return 0.0
        k = (len(sorted_vals) - 1) * p
        lo, hi = int(floor(k)), min(int(floor(k)) + 1, len(sorted_vals) - 1)
        return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo)

    def _summ(self, vals: List[float]) -> dict:
        v = sorted(vals)
        if not v:
            return {"mean": 0.0, "median": 0.0, "p75": 0.0, "p90": 0.0, "min": 0.0, "max": 0.0}
        return {
            "mean":   sum(v) / len(v),
            "median": self._pctl(v, 0.5),
            "p75":    self._pctl(v, 0.75),
            "p90":    self._pctl(v, 0.90),
            "min":    v[0],
            "max":    v[-1],
        }

    def compute_stats(self) -> dict:
        """Resumen estadístico de MFE/MAE (MAE se reporta en valor absoluto)."""
        with self._stats_lock:
            recs = [r for r in self.trade_stats if "mfe_usd" in r]
        groups: Dict[str, dict] = {}
        for name, subset in (
            ("ALL",    recs),
            ("TP",     [r for r in recs if r.get("reason") == "TP"]),
            ("SL",     [r for r in recs if r.get("reason") == "SL"]),
            ("MANUAL", [r for r in recs if r.get("reason") == "MANUAL"]),
        ):
            groups[name] = {
                "n":          len(subset),
                "mfe_usd":    self._summ([abs(float(r.get("mfe_usd", 0))) for r in subset]),
                "mae_usd":    self._summ([abs(float(r.get("mae_usd", 0))) for r in subset]),
                "mfe_pct":    self._summ([abs(float(r.get("mfe_pct", 0))) for r in subset]),
                "mae_pct":    self._summ([abs(float(r.get("mae_pct", 0))) for r in subset]),
                "duration_s": self._summ([float(r.get("duration_s", 0)) for r in subset]),
                "pnl":        self._summ([float(r.get("pnl", 0)) for r in subset]),
            }
        tps = groups["TP"]["n"]
        total_pnl = sum(float(r.get("pnl", 0)) for r in recs)

        # ¿Qué SL habría cortado ganadoras (TP)? (MAE <= SL → la operación se habría parado)
        tp_mae = [float(r.get("mae_usd", 0)) for r in recs if r.get("reason") == "TP"]
        sl_sim = []
        for sl in (-1.0, -2.0, -3.0, -4.0, -5.0, -6.0, -8.0, -10.0, -12.0, -15.0, -20.0):
            stopped = sum(1 for m in tp_mae if m <= sl)
            sl_sim.append({
                "sl": sl, "tp_stopped": stopped, "tp_total": tps,
                "pct": (stopped / tps * 100.0) if tps else 0.0,
            })
        return {
            "count":     len(recs),
            "win_rate":  (tps / len(recs) * 100.0) if recs else 0.0,
            "total_pnl": total_pnl,
            "avg_pnl":   (total_pnl / len(recs)) if recs else 0.0,
            "groups":    groups,
            "sl_sim":    sl_sim,
            "updated":   time.time(),
        }

    # ── SL global editable en caliente ────────────────────────────────────────

    def set_default_stop_loss(self, sl_usd: float, override_manual: bool = False) -> dict:
        """Cambia el SL estándar (2+ tramos), lo guarda en disco y lo aplica ya a las
        posiciones abiertas. Los SL manuales se respetan salvo override_manual=True."""
        global DEFAULT_STOP_LOSS_USD, DEFAULT_SL_FROM_UI
        DEFAULT_STOP_LOSS_USD = float(sl_usd)
        DEFAULT_SL_FROM_UI = True
        updated: List[str] = []
        with self.lock:
            for sym, pos in self.positions.items():
                if pos.status != "OPEN" or not pos.fills:
                    continue
                if override_manual:
                    pos.sl_manual = False
                before = pos.sl_usd
                pos.refresh_auto_sl()
                if pos.sl_usd != before:
                    updated.append(sym)
        tmp = f"{SETTINGS_FILE}.tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump({"default_stop_loss_usd": DEFAULT_STOP_LOSS_USD}, fh)
            os.replace(tmp, SETTINGS_FILE)
        except Exception as exc:
            self.log(f"No pude guardar ajustes: {exc}")
        self.log(f"SL GLOBAL = {DEFAULT_STOP_LOSS_USD:.4f} USD "
                 f"(posiciones actualizadas: {', '.join(updated) or 'ninguna'}"
                 f"{' | manuales sobrescritos' if override_manual else ''})")
        self.persist_state()
        for sym in self._open_position_symbols():
            self._enqueue(sym)          # reevalúa ya con el nuevo SL
        return {"default_stop_loss_usd": DEFAULT_STOP_LOSS_USD, "updated": updated}

    def set_stop_loss(self, symbol: str, sl_usd: float) -> bool:
        """Fija un stop loss MANUAL (USD, negativo) para una posición abierta.
        Queda marcado como manual: el SL automático por tramos ya no lo pisa."""
        symbol = symbol.upper().strip()
        with self.lock:
            pos = self.positions.get(symbol)
            if not pos or pos.status != "OPEN":
                return False
            pos.sl_usd    = sl_usd
            pos.sl_manual = True
        self.log(f"Stop loss MANUAL para {symbol}: {sl_usd:.4f} USD")
        self.persist_state()
        # Reevalúa ya con el nuevo SL (por si el precio actual ya lo cruza)
        self._enqueue(symbol)
        return True

    async def close_position_manual(self, symbol: str, force: bool = False) -> bool:
        """Cierre manual de una posición abierta desde la UI. force=True (cierre de
        emergencia) cierra aunque no haya precio en vivo, registrándolo al precio medio
        de entrada (PnL 0): así una posición sin cotización no queda atascada."""
        symbol = symbol.upper().strip()
        price = self._display_price(symbol)
        if price <= 0 and force:
            with self.lock:
                pos = self.positions.get(symbol)
                price = pos.avg_entry if pos is not None else 0.0
            if price > 0:
                self.log(f"⚠️ Cierre forzado de {symbol} sin precio en vivo: se registra al precio "
                         f"medio de entrada {price:.6f} (PnL 0)")
        if price <= 0:
            # Recién arrancado (p. ej. tras recuperar el estado) aún no hay precio por WebSocket.
            raise RuntimeError(f"Todavía no hay precio de {symbol}; reintenta en unos segundos "
                               "(o usa el cierre forzado)")
        return await self._close_position(symbol, price, "MANUAL")

    # ── Mantenimiento (housekeeping; NO es la vía de detección) ───────────────

    async def _maintenance_loop(self) -> None:
        """Red de seguridad + limpieza del radar + métricas + guardado periódico.
        Las entradas y los TP/SL los dispara cada tick del WS, no este bucle."""
        step = SAFETY_TICK_SECS if SAFETY_TICK_SECS > 0 else 1.0
        step = max(0.2, min(step, 1.0))
        last_rate = last_prune = last_persist = time.time()
        last_count = self.scan_count
        while self.running:
            await asyncio.sleep(step)
            now = time.time()
            try:
                if SAFETY_TICK_SECS > 0:
                    for sym in set(self.watch) | set(self._open_position_symbols()):
                        self._enqueue(sym)

                if now - last_rate >= 1.0:
                    dt = now - last_rate
                    self.eval_rate = (self.scan_count - last_count) / dt
                    if self._lat_n:
                        self.latency_avg_ms = self._lat_sum / self._lat_n
                        self.latency_max_ms = self._lat_max
                    self._lat_sum, self._lat_n, self._lat_max = 0.0, 0, 0.0
                    last_count, last_rate = self.scan_count, now

                if now - last_prune >= WATCH_PRUNE_SECS:
                    last_prune = now
                    self._prune_watch()
                    with self.lock:
                        self.symbol_cooldown = {s: ts for s, ts in self.symbol_cooldown.items() if ts > now}
                    self._entry_backoff = {s: ts for s, ts in self._entry_backoff.items() if ts > now}
                    self._close_backoff = {s: ts for s, ts in self._close_backoff.items() if ts > now}

                if now - self._price_block_reset_at >= SYMBOL_REFRESH_HOURS * 3600:
                    # Antes ocurría junto con el refresh REST de símbolos (ya eliminado)
                    self._price_block_reset_at = now
                    with self.lock:
                        self.price_blocked.clear()
                    self.persist_state(critical=False)
                    self.log("price_blocked reseteado (cada SYMBOL_REFRESH_HOURS)")

                if now - last_persist >= STATE_PERSIST_SECS:
                    last_persist = now
                    self.persist_state(critical=False)    # MFE/MAE: a Upstash como mucho cada minuto

                if self.store.restore_pending():
                    self._restore_from_store(late=True)    # tomó el control después de arrancar
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = str(exc)
                self._log_throttled("maint_err", f"Error en mantenimiento: {exc!r}")

    # ── Snapshot ──────────────────────────────────────────────────────────────

    def _winners_now(self, pos_syms: set) -> List[dict]:
        """Radar ordenado por cambio en vivo: ≥ MIN_GAIN_FILTER o con posición."""
        rows = []
        for sym in self.watch:
            price  = self._display_price(sym)
            change = self._change_for(sym, price)
            if change >= MIN_GAIN_FILTER or sym in pos_syms:
                rows.append({
                    "symbol":    sym,
                    "change":    change,
                    "price":     price,
                    "market":    "futures",
                    "can_short": True,
                })
        rows.sort(key=lambda r: r["change"], reverse=True)
        return rows

    def live_payload(self) -> dict:
        """Payload MÍNIMO para el refresco del navegador (precio, cambio y PnL):
        sin eventos, sin cierres, sin rehacer tablas."""
        now = time.time()
        pos_out: Dict[str, dict] = {}
        total_unreal = 0.0
        with self.lock:
            for sym, pos in self.positions.items():
                if pos.status != "OPEN" or not pos.fills:
                    continue
                price = self._display_price(sym)
                pnl   = pos.unrealized_pnl(price)
                total_unreal += pnl
                qty = pos.qty
                pos_out[sym] = {
                    "p":   price,
                    "c":   self._change_for(sym, price),
                    "pnl": pnl,
                    "sl":  (pos.avg_entry - pos.sl_usd / qty) if qty > 0 else 0.0,
                    "mfe": max(pos.mfe_usd, pnl),
                    "mae": min(pos.mae_usd, pnl),
                }
        winners = [{"s": w["symbol"], "p": w["price"], "c": w["change"]}
                   for w in self._winners_now(set(pos_out))]
        return {
            "ts":               now,
            "positions":        pos_out,
            "winners":          winners,
            "total_unrealized": total_unreal,
            "scan_count":       self.scan_count,
            "eval_rate":        self.eval_rate,
            "latency_avg_ms":   self.latency_avg_ms,
            "latency_max_ms":   self.latency_max_ms,
            "watch_count":      len(self.watch),
        }

    def _build_snapshot(self) -> dict:
        ws_stats: dict = {}
        kl_stats: dict = {}
        try:
            if self.price_cache:
                ws_stats = self.price_cache.get_stats()
        except Exception:
            pass
        try:
            if self.kline_cache:
                kl_stats = self.kline_cache.get_stats()
        except Exception:
            pass

        with self.lock:
            positions_raw      = dict(self.positions)
            closed             = list(self.closed_trades[:130])
            events             = list(self.events[:50])
            cooldown_snap      = dict(self.symbol_cooldown)
            price_blocked_snap = set(self.price_blocked)
            all_symbols_count  = len(self.all_symbols)
            total_realized_pnl = self.total_realized_pnl

        now = time.time()
        pos_syms = {s for s, p in positions_raw.items() if p.status == "OPEN" and p.fills}
        winners_raw = self._winners_now(pos_syms)
        change_by_sym = {w["symbol"]: w["change"] for w in winners_raw}

        winners_out = []
        for w in winners_raw:
            sym       = w["symbol"]
            remaining = max(0.0, cooldown_snap.get(sym, 0.0) - now)
            winners_out.append({
                **w,
                "cooldown_remaining": remaining,
                "cooldown_str":       self._fmt_cooldown(remaining) if remaining > 0 else "",
                "price_blocked":      sym in price_blocked_snap,
            })

        open_positions = []
        total_unreal   = 0.0
        total_notional = 0.0
        for symbol, pos in positions_raw.items():
            if pos.status != "OPEN" or not pos.fills:
                continue
            price = self._display_price(symbol)
            pnl   = pos.unrealized_pnl(price)
            total_unreal   += pnl
            total_notional += pos.notional
            # Short SL: (avg_entry - sl_price) * qty = sl_usd → sl_price = avg_entry - sl_usd/qty
            sl_price = (pos.avg_entry - pos.sl_usd / pos.qty) if pos.qty > 0 else 0.0
            open_positions.append({
                "symbol":          symbol,
                "mark_price":      price,
                "avg_entry":       pos.avg_entry,
                "qty":             pos.qty,
                "notional":        pos.notional,
                "target":          pos.notional * TAKE_PROFIT_FRACTION,
                "unrealized_pnl":  pnl,
                "stop_loss_price": sl_price,
                "stop_loss_usd":   pos.sl_usd,
                "sl_mode":         pos.sl_mode,
                "trade_id":        pos.trade_id,
                "mfe_usd":         max(pos.mfe_usd, pnl),
                "mae_usd":         min(pos.mae_usd, pnl),
                "mfe_pct":         pos.mfe_pct,
                "mae_pct":         pos.mae_pct,
                "opened_ts":       pos.opened_ts,
                "fills":           [f.__dict__ for f in pos.fills],
                "change":          change_by_sym.get(symbol, self._change_for(symbol, price)),
            })

        active_cooldowns = {
            sym: {
                "remaining_s":   round(ts - now, 0),
                "remaining_str": self._fmt_cooldown(ts - now),
                "unblock_utc":   datetime.fromtimestamp(ts, timezone.utc).strftime(
                    "%Y-%m-%d %H:%M UTC"
                ),
            }
            for sym, ts in cooldown_snap.items() if ts > now
        }

        last_scan_text = (
            datetime.fromtimestamp(self.last_scan_at, timezone.utc)
            .strftime("%Y-%m-%d %H:%M:%S UTC")
            if self.last_scan_at else "pendiente"
        )
        watch = sorted(self.watch)

        return {
            "mode":              "PAPER" if PAPER_MODE or not LIVE_TRADING else "REAL",
            "running":           self.running,
            "thread_alive":      bool(self.thread and self.thread.is_alive()),
            "started_at":        self.started_at,
            "uptime_seconds":    round(now - self.started_at, 1),
            "scan_count":        self.scan_count,
            "last_scan_text":    last_scan_text,
            # Motor por eventos
            "eval_rate":         self.eval_rate,
            "latency_avg_ms":    self.latency_avg_ms,
            "latency_max_ms":    self.latency_max_ms,
            "live_poll_ms":      LIVE_POLL_MS,
            "status_poll_ms":    STATUS_POLL_MS,
            "min_gain_filter":   MIN_GAIN_FILTER,
            "safety_tick_secs":  SAFETY_TICK_SECS,
            # Símbolos / radar
            "all_symbols_count": all_symbols_count,
            "universe_count":    ws_stats.get("active_tickers", 0),
            "subscribed_count":  len(watch),
            "subscribed_symbols": watch,
            "ws_connected":      bool(ws_stats.get("connected", False)),
            "book_ticks":        ws_stats.get("book_ticks", 0),
            # Resto
            "last_error":        self.last_error,
            "last_startup_err":  self.last_startup_err,
            "exchange_symbols":  self.exchange_symbols,
            "entry_levels":      ENTRY_LEVELS,
            "entry_notionals":   ENTRY_NOTIONALS,
            "take_profit_pct":   TAKE_PROFIT_FRACTION * 100,
            "default_stop_loss_usd":      DEFAULT_STOP_LOSS_USD,
            "first_tranche_sl_fraction":  FIRST_TRANCHE_SL_FRACTION,
            "total_unrealized":   total_unreal,
            "total_realized_pnl": total_realized_pnl,
            "total_notional":    total_notional,
            "executor_url":      EXECUTOR_URL or "",
            "positions":         open_positions,
            "winners":           winners_out,
            "closed_trades":     closed,
            "events":            events,
            "cooldown_count":    len(active_cooldowns),
            "cooldowns":         active_cooldowns,
            "cooldown_hours":    COOLDOWN_SECONDS / 3600,
            "price_blocked":     sorted(price_blocked_snap),
            "price_blocked_count": len(price_blocked_snap),
            "max_price_block":   MAX_PRICE_BLOCK,
            "price_ws": {
                "active_prices":  ws_stats.get("active_prices",  0),
                "active_tickers": ws_stats.get("active_tickers", 0),
                "total":          ws_stats.get("total_symbols",  0),
                "stale":          ws_stats.get("stale_symbols",  0),
            },
            "kline_ws": {
                "pairs_with_data": kl_stats.get("pairs_with_data", 0),
                "total_messages":  kl_stats.get("total_messages",  0),
                "active_conns":    kl_stats.get("active_connections", 0),
            },
            "persistence": self.store.status(),
            "ts": now,
        }


    def _write_state_file(self, snap: dict) -> None:
        '''Escribe el estado en disco de forma atómica.'''
        if not snap["positions"] and not snap["closed_trades"] and snap["scan_count"] <= 0:
            return

        tmp = f"{STATE_FILE}.tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(snap, fh, ensure_ascii=False, default=str)
            os.replace(tmp, STATE_FILE)
        except Exception as exc:
            self.log(f"No pude persistir estado: {exc}")

    async def _persist_state_loop(self) -> None:
        '''Flusher en segundo plano para persistir estado sin bloquear el loop.'''
        if self._persist_event is None:
            self._persist_event = asyncio.Event()

        while self.running:
            try:
                await self._persist_event.wait()
                self._persist_event.clear()

                # Debounce: agrupa ráfagas de cambios consecutivos.
                try:
                    await asyncio.sleep(self._persist_debounce_secs)
                except asyncio.CancelledError:
                    if not self.running:
                        break
                    raise

                while self._persist_event.is_set():
                    self._persist_event.clear()
                    try:
                        await asyncio.sleep(self._persist_debounce_secs)
                    except asyncio.CancelledError:
                        if not self.running:
                            break
                        raise

                if not self.running:
                    break

                snap = self._build_snapshot()
                if not snap["positions"] and not snap["closed_trades"] and snap["scan_count"] <= 0:
                    continue

                await asyncio.to_thread(self._write_state_file, snap)

            except asyncio.CancelledError:
                if not self.running:
                    break
            except Exception as exc:
                self.last_error = str(exc)
                self.log(f"No pude persistir estado en background: {exc}")
                try:
                    await asyncio.sleep(1.0)
                except asyncio.CancelledError:
                    if not self.running:
                        break
                    raise

    # ── Recuperación tras reinicio ────────────────────────────────────────────

    def _recovery_doc(self) -> Tuple[dict, List[dict]]:
        """Documento que se guarda en Upstash: solo lo necesario para retomar.
        Las posiciones cerradas no aparecen (desaparecen del documento al cerrar).
        Devuelve también los cierres pendientes de envío, tomados bajo el mismo
        lock: una posición cerrada nunca sale del documento sin su fila del historial."""
        now = time.time()
        with self.lock:
            positions = {
                sym: asdict(pos) for sym, pos in self.positions.items()
                if pos.status == "OPEN" and pos.fills
            }
            cooldowns     = {s: ts for s, ts in self.symbol_cooldown.items() if ts > now}
            price_blocked = sorted(self.price_blocked)
            total_pnl     = self.total_realized_pnl
            trades        = self.store.peek_trades()
        with self._trade_id_lock:
            seq = self._trade_id_seq
        doc = {
            "v":                     1,
            "trade_id_seq":          seq,
            "total_realized_pnl":    total_pnl,
            "positions":             positions,
            "cooldowns":             cooldowns,
            "price_blocked":         price_blocked,
            "price_block_reset_at":  self._price_block_reset_at,
        }
        if DEFAULT_SL_FROM_UI:
            doc["default_stop_loss_usd"] = DEFAULT_STOP_LOSS_USD
        return doc, trades

    @staticmethod
    def _position_from_dict(d: dict) -> Optional[BotPosition]:
        fills = []
        for f in d.get("fills") or []:
            try:
                fills.append(Fill(
                    level=float(f["level"]), notional=float(f["notional"]),
                    entry_price=float(f["entry_price"]), qty=float(f["qty"]),
                    opened_at=float(f.get("opened_at") or time.time()),
                ))
            except (KeyError, TypeError, ValueError):
                continue
        if not fills or not d.get("symbol"):
            return None
        known = {f.name for f in fields(BotPosition)} - {"fills", "status"}
        pos = BotPosition(fills=fills, **{k: d[k] for k in known if k in d})
        pos.status = "OPEN"
        return pos

    def _restore_from_store(self, late: bool = False) -> bool:
        """Aplica el estado leído al tomar el control y empieza a operar y guardar.
        late=True: llegó con los WebSockets ya abiertos (corre en el loop del bot)."""
        r = self.store.take_restore()
        if r is None:
            return False
        doc, trades, source = r
        self._apply_recovery(doc, trades, source)
        if not self.store.enable_writes():
            self.log("[estado] Perdí el control mientras aplicaba el estado; sigo esperando")
            return False
        if late:
            for sym in self._open_position_symbols():
                self._watch_add(sym)
                self._enqueue(sym)
        return True

    def _apply_recovery(self, doc: Optional[dict], trades: List[dict], source: str) -> None:
        """Restaura el estado guardado, reemplazando el de memoria. Llamar antes de
        arrancar WebSockets y motor, o desde el loop del bot si llega más tarde."""
        global DEFAULT_STOP_LOSS_USD, DEFAULT_SL_FROM_UI
        now = time.time()
        doc = doc or {}
        try:
            sl = float(doc.get("default_stop_loss_usd"))   # solo está si se cambió desde la web
            if sl < 0:
                DEFAULT_STOP_LOSS_USD = sl       # antes que las posiciones (lo usa refresh_auto_sl)
                DEFAULT_SL_FROM_UI = True
        except (TypeError, ValueError):
            pass

        positions: Dict[str, BotPosition] = {}
        for sym, d in (doc.get("positions") or {}).items():
            pos = self._position_from_dict(d) if isinstance(d, dict) else None
            if pos is None:
                self.log(f"[estado] posición {sym} ilegible en el documento; se descarta")
                continue
            positions[pos.symbol] = pos
        cooldowns = {}
        for sym, ts in (doc.get("cooldowns") or {}).items():
            try:
                if float(ts) > now:
                    cooldowns[sym] = float(ts)
            except (TypeError, ValueError):
                continue

        stats = self._merge_trades(trades)

        with self.lock:
            self.positions       = positions
            self.symbol_cooldown = cooldowns
            self.price_blocked   = set(doc.get("price_blocked") or [])
            try:
                self.total_realized_pnl = float(doc.get("total_realized_pnl") or 0.0)
            except (TypeError, ValueError):
                pass
            self.closed_trades = [self._closed_row(r, cooldowns) for r in reversed(stats[-500:])]
        try:
            reset_at = float(doc.get("price_block_reset_at") or 0.0)
            if 0 < reset_at <= now:
                self._price_block_reset_at = reset_at     # el reseteo de 12h no se reinicia con cada arranque
        except (TypeError, ValueError):
            pass

        self._bump_trade_id_seq(int(doc.get("trade_id_seq") or 0),
                                *[int(p.trade_id or 0) for p in positions.values()])

        detail = ", ".join(
            f"{p.symbol} (trade {p.trade_id}, {len(p.fills)} tramo(s), SL {p.sl_usd:.2f})"
            for p in positions.values()
        ) or "ninguna"
        self.log(f"[estado] Recuperado desde {source}: posiciones abiertas: {detail} | "
                 f"cooldowns: {len(cooldowns)} | historial: {len(stats)} cierres | "
                 f"próximo trade_id: {self._trade_id_seq + 1}")

    def _merge_trades(self, trades: List[dict]) -> List[dict]:
        """Une el historial remoto con el local (sin duplicados) y devuelve una copia."""
        with self._stats_lock:
            seen = {(r.get("trade_id"), r.get("closed_at_ts")) for r in self.trade_stats}
            for r in trades:
                key = (r.get("trade_id"), r.get("closed_at_ts")) if isinstance(r, dict) else None
                if key is not None and key not in seen:
                    seen.add(key)
                    self.trade_stats.append(r)
            self.trade_stats.sort(key=lambda r: float(r.get("closed_at_ts") or 0))
            stats = list(self.trade_stats)
        self._bump_trade_id_seq(*[int(r.get("trade_id") or 0) for r in stats])
        return stats

    def _bump_trade_id_seq(self, *ids: int) -> None:
        with self._trade_id_lock:
            self._trade_id_seq = max([self._trade_id_seq, *ids])   # nunca repetir un trade_id

    @staticmethod
    def _closed_row(r: dict, cooldowns: Dict[str, float]) -> dict:
        def fmt(ts: Any, pattern: str) -> str:
            try:
                return datetime.fromtimestamp(float(ts), timezone.utc).strftime(pattern)
            except (TypeError, ValueError, OverflowError, OSError):
                return ""
        sym = r.get("symbol", "")
        return {
            "symbol":      sym,
            "pnl":         r.get("pnl", 0.0),
            "target":      r.get("target", 0.0),
            "qty":         r.get("qty", 0.0),
            "avg_entry":   r.get("avg_entry", 0.0),
            "close_price": r.get("close_price", 0.0),
            "notional":    r.get("notional", 0.0),
            "closed_at":   fmt(r.get("closed_at_ts"), "%Y-%m-%d %H:%M:%S UTC"),
            "unblock_at":  fmt(cooldowns[sym], "%Y-%m-%d %H:%M UTC") if sym in cooldowns else "",
            "reason":      r.get("reason", ""),
            "mfe_usd":     r.get("mfe_usd", 0.0),
            "mae_usd":     r.get("mae_usd", 0.0),
            "mfe_pct":     r.get("mfe_pct", 0.0),
            "mae_pct":     r.get("mae_pct", 0.0),
            "duration_s":  r.get("duration_s", 0.0),
        }

    def _on_fenced(self) -> None:
        self.last_error = "Otra instancia tomó el control del estado: esta queda en standby"

    def shutdown(self) -> None:
        """Apagado ordenado (gunicorn ejecuta atexit al recibir SIGTERM de Render):
        deja de operar, espera las órdenes en vuelo, guarda por última vez y libera
        el control para que la instancia nueva lo tome sin esperar a que caduque."""
        self.store.begin_stop()
        deadline = time.time() + SHUTDOWN_DRAIN_S
        while time.time() < deadline and (self._entry_reserved or self._closing_symbols):
            time.sleep(0.1)
        if self.store.close():
            print("[estado] guardado final enviado"
                  + (" y control liberado" if self.store.remote else " (disco local)"), flush=True)

    # ── Persistencia ──────────────────────────────────────────────────────────

    def persist_state(self, critical: bool = True) -> None:
        """Solicita persistencia asíncrona del estado sin bloquear el loop.
        critical=True (abrir/cerrar/SL) se guarda en Upstash al instante;
        False (MFE/MAE, bloqueos) va con el siguiente latido (STATE_HEARTBEAT_S)."""
        self.store.mark_dirty(critical)
        if self._persist_event is None:
            snap = self._build_snapshot()
            self._write_state_file(snap)
            return

        try:
            if self.loop and self.loop.is_running():
                self.loop.call_soon_threadsafe(self._persist_event.set)
            else:
                self._persist_event.set()
        except Exception:
            snap = self._build_snapshot()
            self._write_state_file(snap)

    def snapshot(self) -> dict:
        live = self._build_snapshot()
        if not live["positions"] and not live["winners"] and os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, "r", encoding="utf-8") as fh:
                    persisted = json.load(fh)
                if isinstance(persisted, dict) and \
                   persisted.get("scan_count", 0) > live.get("scan_count", 0):
                    persisted["state_source"] = "persisted"
                    return persisted
            except Exception:
                pass
        live["state_source"] = "memory"
        return live


# ─────────────────────────────────────────────────────────────────────────────
# FLASK APP
# ─────────────────────────────────────────────────────────────────────────────

bot = TradingBot()
bot.start()
atexit.register(bot.shutdown)

app = Flask(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# HTML + JS
# ─────────────────────────────────────────────────────────────────────────────

HTML = r"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
  <meta name="theme-color" content="#0a0f1e">
  <title>Bot Short · Binance Futures</title>
  <style>
    :root {
      --bg: #0a0f1e; --bg2: #0e1630;
      --card: #121a30; --card2: #17213d; --line: #223052;
      --txt: #e8edf8; --muted: #8b9bbd;
      --green: #34d399; --red: #f87171; --amber: #fbbf24;
      --blue: #60a5fa; --violet: #a78bfa; --teal: #2dd4bf; --orange: #fb923c;
      --radius: 16px;
    }
    * { box-sizing: border-box; -webkit-tap-highlight-color: transparent; }
    html { scroll-behavior: smooth; }
    body {
      margin: 0; color: var(--txt);
      font-family: system-ui, -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      background: radial-gradient(1200px 600px at 10% -10%, #1a2550 0%, transparent 60%),
                  radial-gradient(900px 500px at 100% 0%, #2a1850 0%, transparent 55%),
                  var(--bg);
      background-attachment: fixed;
      font-variant-numeric: tabular-nums;
      padding-bottom: env(safe-area-inset-bottom);
    }
    a { color: inherit; }
    .positive { color: var(--green); } .negative { color: var(--red); }
    .warn { color: var(--amber); } .teal { color: var(--teal); } .violet { color: var(--violet); }
    .muted { color: var(--muted); }

    /* ── Cabecera ── */
    header {
      position: sticky; top: 0; z-index: 50;
      backdrop-filter: blur(14px); -webkit-backdrop-filter: blur(14px);
      background: rgba(10,15,30,.78); border-bottom: 1px solid var(--line);
      padding: calc(10px + env(safe-area-inset-top)) 16px 0;
    }
    .head-row { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
    .logo { font-size: 18px; font-weight: 800; letter-spacing: .2px; }
    .logo small { color: var(--muted); font-weight: 600; font-size: 12px; margin-left: 6px; }
    .grow { flex: 1; }
    .badge { padding: 4px 10px; border-radius: 999px; font-size: 11px; font-weight: 800; letter-spacing: .4px; }
    .b-real { background: rgba(52,211,153,.15); color: var(--green); border: 1px solid rgba(52,211,153,.45); }
    .b-paper { background: rgba(251,191,36,.12); color: var(--amber); border: 1px solid rgba(251,191,36,.4); }
    .dots { display: flex; gap: 6px; align-items: center; }
    .dot { width: 9px; height: 9px; border-radius: 50%; background: #3a4766; transition: background .25s; }
    .dot.on { background: var(--green); box-shadow: 0 0 8px var(--green); }
    #dotEngine.on { background: var(--violet); box-shadow: 0 0 8px var(--violet); }
    nav { display: flex; gap: 6px; overflow-x: auto; padding: 10px 0 10px; scrollbar-width: none; }
    nav::-webkit-scrollbar { display: none; }
    nav a {
      flex: 0 0 auto; text-decoration: none; font-size: 13px; font-weight: 700; color: var(--muted);
      padding: 7px 14px; border-radius: 999px; background: var(--card); border: 1px solid var(--line);
    }
    nav a:active { background: var(--card2); color: var(--txt); }

    /* ── Layout ── */
    main { max-width: 1200px; margin: 0 auto; padding: 16px; display: grid; gap: 18px; }
    .anchor { scroll-margin-top: 110px; }
    .card {
      background: linear-gradient(180deg, var(--card2), var(--card));
      border: 1px solid var(--line); border-radius: var(--radius); padding: 16px;
    }
    h2.sec { margin: 0 0 12px; font-size: 16px; display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
    h2.sec .sub { color: var(--muted); font-size: 12px; font-weight: 500; }
    .label { color: var(--muted); font-size: 12px; margin-bottom: 4px; }

    .kpis { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; }
    .kpi { padding: 14px 16px; }
    .kpi .value { font-size: 24px; font-weight: 800; line-height: 1.15; word-break: break-word; }
    .kpi .hint { color: var(--muted); font-size: 11px; margin-top: 4px; }

    /* ── Tarjeta SL global ── */
    .slcard { border-color: rgba(251,146,60,.45); }
    .slrow { display: flex; gap: 10px; align-items: end; flex-wrap: wrap; }
    .slcur { min-width: 120px; }
    .slcur b { font-size: 26px; color: var(--orange); }
    .field { display: flex; flex-direction: column; gap: 4px; flex: 1 1 160px; }
    .field label { font-size: 12px; color: var(--muted); }
    input[type=number] {
      width: 100%; background: #0b1226; color: var(--txt); border: 1px solid var(--line);
      border-radius: 12px; padding: 12px; font-size: 16px; outline: none;
    }
    input[type=number]:focus { border-color: var(--orange); }
    .check { display: flex; align-items: center; gap: 8px; font-size: 13px; color: var(--muted); margin-top: 10px; }
    .check input { width: 18px; height: 18px; accent-color: var(--orange); }
    .btn {
      appearance: none; border: 1px solid transparent; border-radius: 12px; padding: 12px 18px;
      font-size: 14px; font-weight: 800; cursor: pointer; color: #0a0f1e; min-height: 44px;
    }
    .btn:disabled { opacity: .5; cursor: not-allowed; }
    .btn-orange { background: linear-gradient(135deg, #fdba74, #fb923c); }
    .btn-ghost { background: var(--card2); color: var(--txt); border-color: var(--line); }
    .btn-danger { background: rgba(248,113,113,.14); color: #fca5a5; border-color: rgba(248,113,113,.5); }
    .btn-danger:active { background: rgba(248,113,113,.28); }
    .btn-sm { padding: 6px 12px; min-height: 34px; font-size: 12px; }
    .msg { font-size: 12px; margin-top: 8px; min-height: 16px; }

    /* ── Posiciones (tarjetas) ── */
    .poslist { display: grid; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr)); gap: 12px; }
    .pos { padding: 14px; }
    .pos-top { display: flex; justify-content: space-between; align-items: flex-start; gap: 10px; }
    .sym { font-weight: 800; font-size: 17px; color: var(--blue); text-decoration: none; }
    .chg { display: block; font-size: 13px; font-weight: 700; margin-top: 2px; }
    .pnl { font-size: 24px; font-weight: 800; text-align: right; }
    .pgrid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 8px 14px; margin: 12px 0; }
    .pgrid > div { display: flex; flex-direction: column; gap: 2px; background: rgba(255,255,255,.03);
                   border-radius: 10px; padding: 8px 10px; }
    .pgrid span { font-size: 11px; color: var(--muted); }
    .pgrid b { font-size: 14px; }
    .pills { display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 12px; }
    .pill { padding: 3px 10px; border-radius: 999px; background: #0b1226; border: 1px solid var(--line); font-size: 11px; }
    .slbtn { background: none; border: 1px dashed var(--orange); color: var(--orange); border-radius: 8px;
             font-weight: 800; font-size: 14px; padding: 3px 8px; cursor: pointer; text-align: left; }
    .tag { font-size: 10px; padding: 1px 7px; border-radius: 999px; border: 1px solid #78350f; color: var(--orange); width: fit-content; }
    .pos-actions { display: flex; gap: 8px; }
    .pos-actions .btn { flex: 1; }
    .empty { color: var(--muted); text-align: center; padding: 22px; border: 1px dashed var(--line); border-radius: var(--radius); }

    /* ── Tablas ── */
    .tablewrap { overflow-x: auto; border-radius: 12px; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td { padding: 10px 12px; border-bottom: 1px solid #1a2542; text-align: right; white-space: nowrap; }
    th:first-child, td:first-child { text-align: left; }
    th { color: var(--muted); font-weight: 700; font-size: 11px; text-transform: uppercase; letter-spacing: .4px; }
    tr.in-cooldown { background: rgba(251,146,60,.07); }
    tr.can-trade { background: rgba(52,211,153,.06); }
    .cd-badge { padding: 2px 8px; border-radius: 999px; background: rgba(251,146,60,.12); color: #fdba74;
                border: 1px solid #c2410c; font-size: 11px; font-weight: 800; white-space: nowrap; }
    .r-tp { color: var(--green); font-weight: 800; } .r-sl { color: var(--red); font-weight: 800; }
    .r-manual { color: var(--amber); font-weight: 800; }

    /* ── Estadísticas ── */
    .statgrid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin-bottom: 14px; }
    .stat { background: rgba(255,255,255,.04); border-radius: 12px; padding: 12px; }
    .stat .v { font-size: 20px; font-weight: 800; }
    .twocol { display: grid; grid-template-columns: 1.6fr 1fr; gap: 14px; }
    .note { color: var(--muted); font-size: 12px; margin: 8px 0 0; line-height: 1.5; }

    /* ── Diagnóstico ── */
    details.card summary { cursor: pointer; font-weight: 800; font-size: 15px; list-style: none; }
    details.card summary::-webkit-details-marker { display: none; }
    details.card summary::after { content: "▾"; float: right; color: var(--muted); }
    details[open].card summary::after { content: "▴"; }
    .diag { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; margin-top: 14px; }
    .diag .stat .v { font-size: 15px; }
    .chips { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 12px; }
    .chip { background: #0b1226; border: 1px solid var(--line); border-radius: 10px; padding: 5px 10px; font-size: 12px; }
    .executor-chip { color: var(--teal); }
    pre { margin: 0; white-space: pre-wrap; font-size: 12px; max-height: 280px; overflow: auto; color: #b8c4e0; line-height: 1.5; }
    #errorBox { border-color: var(--red); }

    /* ── Modal ── */
    .overlay { display: none; position: fixed; inset: 0; background: rgba(0,0,0,.65); z-index: 100;
               align-items: center; justify-content: center; padding: 16px; }
    .modal { width: 340px; max-width: 100%; }

    /* ── Responsive ── */
    @media (max-width: 900px) {
      .kpis { grid-template-columns: repeat(2, 1fr); }
      .statgrid { grid-template-columns: repeat(2, 1fr); }
      .twocol { grid-template-columns: 1fr; }
    }
    @media (max-width: 720px) {
      main { padding: 12px; gap: 14px; }
      .poslist { grid-template-columns: 1fr; }
      .kpi .value { font-size: 20px; }
      table.rt thead { display: none; }
      table.rt, .rt tbody, .rt tr, .rt td { display: block; width: 100%; }
      .rt tr { border: 1px solid var(--line); border-radius: 12px; margin: 0 0 10px; padding: 8px 12px; background: rgba(255,255,255,.03); }
      .rt td { display: flex; justify-content: space-between; gap: 12px; padding: 5px 0; border: 0; white-space: normal; text-align: right; }
      .rt td:first-child { text-align: right; }
      .rt td::before { content: attr(data-label); color: var(--muted); font-size: 12px; text-align: left; }
      .logo small { display: none; }
    }
  </style>
</head>
<body>
<header>
  <div class="head-row">
    <div class="logo">⚡ Bot Short<small>Binance USDT-M</small></div>
    <span id="modeBadge" class="badge b-paper">—</span>
    <div class="grow"></div>
    <div class="dots">
      <span id="dotPoll" class="dot" title="Estructura (/api/status) al día"></span>
      <span id="dotLive" class="dot" title="Precios en vivo (/api/live)"></span>
      <span id="dotEngine" class="dot" title="El motor evalúa ticks"></span>
    </div>
  </div>
  <nav>
    <a href="#sec-pos">Posiciones</a>
    <a href="#sec-sl">SL global</a>
    <a href="#sec-radar">Radar</a>
    <a href="#sec-stats">Estadísticas</a>
    <a href="#sec-hist">Historial</a>
    <a href="#sec-diag">Diagnóstico</a>
  </nav>
</header>

<main>
  <!-- KPIs -->
  <div class="kpis">
    <div class="card kpi"><div class="label">PnL no realizado</div><div id="pnl" class="value">—</div><div class="hint">USDT</div></div>
    <div class="card kpi"><div class="label">PnL realizado total</div><div id="realizedPnl" class="value">—</div><div class="hint">USDT</div></div>
    <div class="card kpi"><div class="label">Capital en posiciones</div><div id="notional" class="value">—</div><div class="hint">USDT</div></div>
    <div class="card kpi"><div class="label">Posiciones abiertas</div><div id="openCount" class="value">—</div><div class="hint"><span id="rbWatchK">—</span> en radar</div></div>
  </div>

  <!-- Error -->
  <section id="errorBox" class="card" style="display:none">
    <h2 class="sec negative">⚠️ Error / diagnóstico</h2>
    <pre id="lastError" style="color:var(--red)"></pre>
  </section>

  <!-- Posiciones -->
  <section id="sec-pos" class="anchor">
    <h2 class="sec">Posiciones abiertas <span class="sub" id="slHint"></span></h2>
    <div id="posList" class="poslist"><div class="empty">Sin posiciones abiertas</div></div>
  </section>

  <!-- SL global -->
  <section id="sec-sl" class="anchor card slcard">
    <h2 class="sec">⛔ Stop Loss global <span class="sub">se aplica ya a las posiciones abiertas con 2+ tramos</span></h2>
    <div class="slrow">
      <div class="slcur"><div class="label">Valor actual (USD)</div><b id="gslCurrent">—</b></div>
      <div class="field">
        <label for="gslInput">Nuevo SL global (negativo, ej. -6)</label>
        <input id="gslInput" type="number" step="0.1" inputmode="decimal" placeholder="-8">
      </div>
      <button id="gslSave" class="btn btn-orange">Aplicar</button>
    </div>
    <label class="check"><input id="gslOverride" type="checkbox"> También sobrescribir los SL manuales de las posiciones abiertas</label>
    <div id="gslMsg" class="msg"></div>
    <p class="note" id="gslNote">El valor se guarda y sobrevive a reinicios. Los SL fijados a mano en una posición se respetan salvo que marques la casilla.</p>
  </section>

  <!-- Radar -->
  <section id="sec-radar" class="anchor">
    <h2 class="sec"><span id="winnerCount">0</span> en el radar
      <span class="sub">≥<span id="gainThreshold">20</span>% · cambio 24h en vivo</span></h2>
    <div class="card" style="padding:0">
      <div class="tablewrap">
        <table>
          <thead><tr><th>Símbolo</th><th>Cambio 24h</th><th>Precio</th><th>Estado</th></tr></thead>
          <tbody id="tbWinners"><tr><td colspan="4" class="muted">Esperando símbolos que superen el umbral…</td></tr></tbody>
        </table>
      </div>
    </div>
  </section>

  <!-- Cooldowns -->
  <section id="cooldownSection" style="display:none">
    <h2 class="sec">🔒 En cooldown <span class="sub">bloqueados tras cierre</span></h2>
    <div class="card" style="padding:0">
      <div class="tablewrap">
        <table class="rt">
          <thead><tr><th>Símbolo</th><th>Tiempo restante</th><th>Se desbloquea (UTC)</th></tr></thead>
          <tbody id="tbCooldown"></tbody>
        </table>
      </div>
    </div>
  </section>

  <!-- Estadísticas MFE / MAE -->
  <section id="sec-stats" class="anchor">
    <h2 class="sec">📊 Estadísticas MFE / MAE
      <span class="sub">máximo a favor / máximo en contra por operación</span>
      <span class="grow"></span>
      <a class="btn btn-ghost btn-sm" href="/api/trades.csv" download style="text-decoration:none">⬇ CSV</a>
    </h2>
    <div class="card">
      <div class="statgrid">
        <div class="stat"><div class="label">Operaciones</div><div class="v" id="stCount">0</div></div>
        <div class="stat"><div class="label">% acierto (TP)</div><div class="v" id="stWin">—</div></div>
        <div class="stat"><div class="label">MFE mediana</div><div class="v positive" id="stMfe">—</div></div>
        <div class="stat"><div class="label">MAE mediana</div><div class="v negative" id="stMae">—</div></div>
      </div>
      <div class="twocol">
        <div>
          <div class="label">Por tipo de cierre (USD; MAE en valor absoluto)</div>
          <div class="tablewrap">
            <table>
              <thead><tr><th>Grupo</th><th>N</th><th>MFE med</th><th>MFE p90</th><th>MAE med</th><th>MAE p90</th><th>MFE %</th><th>MAE %</th><th>Duración</th></tr></thead>
              <tbody id="tbStatGroups"><tr><td colspan="9" class="muted">Sin datos aún</td></tr></tbody>
            </table>
          </div>
        </div>
        <div>
          <div class="label">¿Qué SL habría frenado ganadoras (TP)?</div>
          <div class="tablewrap">
            <table>
              <thead><tr><th>SL USD</th><th>TP frenadas</th><th>%</th></tr></thead>
              <tbody id="tbSlSim"><tr><td colspan="3" class="muted">Sin datos aún</td></tr></tbody>
            </table>
          </div>
        </div>
      </div>
      <p class="note">MFE = mejor PnL no realizado alcanzado durante la operación; MAE = peor. Se mide en cada tick del motor y se guarda al cerrar (archivo de estadísticas del servidor). Útil para calibrar TP y SL: si el MAE p90 de tus TP es bajo, un SL más ajustado casi no te quita ganadoras.</p>
    </div>
  </section>

  <!-- Historial -->
  <section id="sec-hist" class="anchor">
    <h2 class="sec">Operaciones cerradas <span id="totalRealizedBadge" class="sub"></span></h2>
    <div class="card" style="padding:0">
      <div class="tablewrap">
        <table class="rt">
          <thead><tr><th>Símbolo</th><th>Motivo</th><th>PnL</th><th>MFE</th><th>MAE</th><th>Duración</th><th>Entrada</th><th>Cierre</th><th>Bloqueado hasta</th><th>Fecha</th></tr></thead>
          <tbody id="tbClosed"><tr><td colspan="10" class="muted">Sin cierres aún</td></tr></tbody>
        </table>
      </div>
    </div>
  </section>

  <!-- Diagnóstico -->
  <details id="sec-diag" class="card anchor">
    <summary>🛠 Diagnóstico del motor</summary>
    <div class="diag">
      <div class="stat"><div class="label">Última evaluación</div><div class="v" id="scan">—</div></div>
      <div class="stat"><div class="label">Evaluaciones</div><div class="v" id="scanCount">—</div></div>
      <div class="stat"><div class="label">Eval / s</div><div class="v violet" id="evalRate">—</div></div>
      <div class="stat"><div class="label">Latencia tick→eval</div><div class="v teal" id="latency">—</div></div>
      <div class="stat"><div class="label">Perpetuos (REST)</div><div class="v" id="allSymbols">—</div></div>
      <div class="stat"><div class="label">Universo WS</div><div class="v" id="universe">—</div></div>
      <div class="stat"><div class="label">Radar (bookTicker)</div><div class="v teal" id="subCount">—</div></div>
      <div class="stat"><div class="label">En cooldown</div><div class="v warn" id="cooldownCount">—</div></div>
      <div class="stat"><div class="label">WebSocket</div><div class="v" id="rbWs">—</div></div>
      <div class="stat"><div class="label">Red de seguridad</div><div class="v" id="rbSafety">—</div></div>
      <div class="stat"><div class="label">Executor</div><div class="v" id="executorStatus">—</div></div>
    </div>
    <div class="chips">
      <div class="chip">ticker 24h: <b id="wsTickers" class="teal">—</b></div>
      <div class="chip">precios: <b id="wsActive" class="violet">—</b></div>
      <div class="chip">bookTicker msgs: <b id="bookTicks">—</b></div>
      <div class="chip">stale: <b id="wsStale">—</b></div>
      <div class="chip">kline pares: <b id="klPairs">—</b></div>
      <div class="chip">kline msgs: <b id="klMsgs">—</b></div>
      <div class="chip">kline conns: <b id="klConns">—</b></div>
      <div class="chip">estado guardado: <b id="persistMode">—</b></div>
      <div class="chip">polls estado: <b id="pollCount">0</b></div>
      <div class="chip">polls precios: <b id="livePollCount">0</b></div>
    </div>
    <h3 style="margin:18px 0 8px;font-size:14px">Eventos del bot</h3>
    <pre id="events"></pre>
  </details>
</main>

<!-- Modal: SL individual -->
<div id="slModalOverlay" class="overlay">
  <div class="card modal">
    <h3 style="margin:0 0 4px;font-size:16px">Editar Stop Loss</h3>
    <p class="muted" style="margin:0 0 12px;font-size:13px">Símbolo: <strong id="slModalSymbol" style="color:var(--txt)">—</strong></p>
    <div class="field">
      <label for="slModalInput">Pérdida máxima en USD (negativo). Un SL manual se respeta siempre.</label>
      <input id="slModalInput" type="number" step="0.1" inputmode="decimal">
    </div>
    <p id="slModalError" class="negative" style="display:none;font-size:12px;margin:8px 0 0"></p>
    <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:14px">
      <button id="slModalCancel" class="btn btn-ghost">Cancelar</button>
      <button id="slModalSave" class="btn btn-orange">Guardar</button>
    </div>
  </div>
</div>

<script>
// ── Utilidades ──────────────────────────────────────────────────────────────
const q     = id => document.getElementById(id);
const n     = v  => { const p = Number(v); return isFinite(p) ? p : 0; };
const fx    = (v, d=8) => n(v).toFixed(d);
const px    = v  => { const x = n(v), a = Math.abs(x); if (!x) return '0'; return x.toFixed(a >= 100 ? 2 : a >= 1 ? 4 : a >= 0.01 ? 6 : 8); };
const usd   = v  => fx(v, 3);
const sgn   = v  => (n(v) > 0 ? '+' : '') + fx(v, 3);
const pct   = v  => fx(v, 2) + '%';
const cls   = v  => n(v) >= 0 ? 'positive' : 'negative';
const setTxt = (id, txt) => { const el = q(id); if (el && el.textContent !== txt) el.textContent = txt; };

function fmtCd(secs) {
  const s = Math.max(0, Math.floor(secs));
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), r = s % 60;
  if (h > 0) return `${h}h ${String(m).padStart(2,'0')}m`;
  if (m > 0) return `${m}m ${String(r).padStart(2,'0')}s`;
  return `${r}s`;
}
function fmtDur(secs) {
  const s = Math.max(0, Math.floor(n(secs)));
  if (s >= 3600) return `${Math.floor(s/3600)}h ${String(Math.floor((s%3600)/60)).padStart(2,'0')}m`;
  if (s >= 60) return `${Math.floor(s/60)}m ${String(s%60).padStart(2,'0')}s`;
  return `${s}s`;
}

let pollCount = 0, livePollCount = 0, _cdData = {}, _lastFetch = 0, _prevScan = 0;
let _entryLevels = [50], _posSyms = new Set(), _winSyms = new Set();
let _posSig = '', _winSig = '', _persisted = false;
let livePollMs = 250, statusPollMs = 1500;

function tickTimers() {
  const elapsed = (Date.now() - _lastFetch) / 1000;
  Object.entries(_cdData).forEach(([sym, info]) => {
    const rem = Math.max(0, info.remaining_s - elapsed);
    const el = q('cd_' + sym);
    if (el) {
      if (rem > 0) { el.textContent = '🔒 ' + fmtCd(rem); el.className = 'cd-badge'; }
      else { el.textContent = '✓ libre'; el.className = ''; el.style.color = 'var(--green)'; }
    }
    const row = q('cdrow_' + sym);
    if (row) { const t = row.querySelector('.cdrem'); if (t) t.textContent = rem > 0 ? fmtCd(rem) : 'Expirado'; }
  });
}
setInterval(tickTimers, 1000);

function slStyle(slPx, mrkPx) {
  const d = (slPx > 0 && mrkPx > 0) ? ((slPx - mrkPx) / mrkPx * 100) : 999;
  return d < 2 ? 'color:var(--red)' : d < 5 ? 'color:var(--orange)' : '';
}

// ── Render completo (estructura) ────────────────────────────────────────────
function render(d) {
  if (!d) return;
  _lastFetch = Date.now();
  _persisted = d.state_source === 'persisted';
  if (n(d.live_poll_ms))   livePollMs   = Math.max(100, n(d.live_poll_ms));
  if (n(d.status_poll_ms)) statusPollMs = Math.max(500, n(d.status_poll_ms));

  const mode = d.mode || '—';
  q('modeBadge').textContent = mode;
  q('modeBadge').className = 'badge ' + (mode === 'REAL' ? 'b-real' : 'b-paper');

  const frac = n(d.first_tranche_sl_fraction || 0.251);
  const defSl = n(d.default_stop_loss_usd ?? -8);
  q('slHint').textContent = `1er tramo = notional×${frac} · 2+ tramos = ${fx(defSl, 2)} USD`;
  setTxt('gslCurrent', fx(defSl, 2) + ' USD');
  const gi = q('gslInput');
  if (gi && document.activeElement !== gi && !gi.dataset.dirty) gi.value = defSl;

  const pu = n(d.total_unrealized);
  q('pnl').textContent = sgn(pu); q('pnl').className = 'value ' + cls(pu);
  const rp = n(d.total_realized_pnl);
  q('realizedPnl').textContent = sgn(rp); q('realizedPnl').className = 'value ' + cls(rp);
  q('notional').textContent = fx(d.total_notional, 2);
  const positions = Array.isArray(d.positions) ? d.positions : [];
  q('openCount').textContent = positions.length;
  q('rbWatchK').textContent = n(d.subscribed_count);

  const pw = d.price_ws || {}, kw = d.kline_ws || {};
  const threshold = n(d.min_gain_filter || 20);
  q('scan').textContent = d.last_scan_text || 'pendiente';
  q('scanCount').textContent = n(d.scan_count);
  q('evalRate').textContent = fx(d.eval_rate, 0) + '/s';
  q('latency').textContent = `${fx(d.latency_avg_ms, 1)} / ${fx(d.latency_max_ms, 1)} ms`;
  q('allSymbols').textContent = n(d.all_symbols_count);
  q('universe').textContent = n(d.universe_count);
  q('subCount').textContent = n(d.subscribed_count);
  q('cooldownCount').textContent = n(d.cooldown_count);
  q('rbWs').textContent = d.ws_connected ? 'conectado' : 'desconectado';
  q('rbWs').style.color = d.ws_connected ? 'var(--green)' : 'var(--red)';
  q('rbSafety').textContent = n(d.safety_tick_secs) > 0 ? `${n(d.safety_tick_secs)} s` : 'off';
  q('gainThreshold').textContent = threshold;
  q('wsTickers').textContent = n(pw.active_tickers);
  q('wsActive').textContent = n(pw.active_prices);
  q('bookTicks').textContent = n(d.book_ticks);
  q('wsStale').textContent = n(pw.stale);
  q('klPairs').textContent = n(kw.pairs_with_data);
  q('klMsgs').textContent = n(kw.total_messages);
  q('klConns').textContent = n(kw.active_conns);
  const ps = d.persistence || {};
  q('persistMode').textContent = ps.mode ? `${ps.backend} · ${ps.mode}` : '—';
  q('persistMode').className = ps.mode === 'ok' || ps.mode === 'local' ? 'teal' : 'violet';
  q('pollCount').textContent = pollCount;
  q('livePollCount').textContent = livePollCount;

  const exUrl = d.executor_url || '';
  q('executorStatus').innerHTML = exUrl
    ? `<span class="executor-chip">🔗 ${exUrl.replace('https://','').split('/')[0]}</span>`
    : `<span class="muted" style="font-size:12px">No configurado</span>`;

  const err = d.last_error || d.last_startup_err || '';
  q('errorBox').style.display = err ? 'block' : 'none';
  q('lastError').textContent = err;

  _cdData = {};
  Object.entries(d.cooldowns || {}).forEach(([sym, info]) => {
    _cdData[sym] = { remaining_s: n(info.remaining_s), unblock_utc: info.unblock_utc || '' };
  });

  // Cooldowns
  const cdEntries = Object.entries(d.cooldowns || {});
  q('cooldownSection').style.display = cdEntries.length ? 'block' : 'none';
  q('tbCooldown').innerHTML = cdEntries
    .sort((a, b) => n(b[1].remaining_s) - n(a[1].remaining_s))
    .map(([sym, info]) => `
      <tr id="cdrow_${sym}">
        <td data-label="Símbolo" style="font-weight:800;color:var(--orange)">${sym}</td>
        <td data-label="Restante" class="cdrem" style="color:var(--orange)">${fmtCd(n(info.remaining_s))}</td>
        <td data-label="Desbloqueo" class="muted">${info.unblock_utc || ''}</td>
      </tr>`).join('');

  // Posiciones (se reconstruye solo si cambia la estructura)
  _posSyms = new Set(positions.map(p => p.symbol));
  const posSig = JSON.stringify(positions.map(p => [
    p.symbol, p.avg_entry, p.qty, p.notional, p.target, p.stop_loss_usd, p.sl_mode,
    (Array.isArray(p.fills) ? p.fills.length : 0)
  ]));
  if (posSig !== _posSig) {
    _posSig = posSig;
    q('posList').innerHTML = positions.length ? positions.map(p => {
      const sym = p.symbol, pnl = n(p.unrealized_pnl);
      const slPx = n(p.stop_loss_price), mrkPx = n(p.mark_price), slUsd = n(p.stop_loss_usd);
      const fills = (Array.isArray(p.fills) ? p.fills : [])
        .map(f => `<span class="pill">+${fx(f.level,0)}% · ${fx(f.notional,2)}</span>`).join('');
      return `<article class="card pos" id="prow_${sym}">
        <div class="pos-top">
          <div>
            <a class="sym" href="https://www.binance.com/en/futures/${sym}" target="_blank" rel="noopener">${sym}</a>
            <span id="pc_${sym}" class="chg ${cls(p.change)}">${pct(p.change)}</span>
          </div>
          <div id="ppnl_${sym}" class="pnl ${cls(pnl)}">${sgn(pnl)}</div>
        </div>
        <div class="pgrid">
          <div><span>Entrada media</span><b>${px(p.avg_entry)}</b></div>
          <div><span>Precio en vivo</span><b id="pp_${sym}">${px(p.mark_price)}</b></div>
          <div><span>Notional</span><b>${fx(p.notional, 2)} USDT</b></div>
          <div><span>Objetivo TP</span><b class="positive">+${fx(p.target, 3)}</b></div>
          <div id="pslc_${sym}" style="${slStyle(slPx, mrkPx)}"><span>SL (precio)</span><b id="psl_${sym}">${px(slPx)}</b></div>
          <div><span>SL (USD)</span>
            <button class="slbtn" onclick="editStopLoss('${sym}', ${slUsd})">${usd(slUsd)}</button>
            <span class="tag">${p.sl_mode || ''}</span></div>
          <div><span>MFE (máx. a favor)</span><b id="pmfe_${sym}" class="positive">${sgn(p.mfe_usd)}</b></div>
          <div><span>MAE (máx. en contra)</span><b id="pmae_${sym}" class="negative">${sgn(p.mae_usd)}</b></div>
        </div>
        <div class="pills">${fills}</div>
        <div class="pos-actions"><button class="btn btn-danger" onclick="closePosition('${sym}', this)">Cerrar posición</button></div>
      </article>`;
    }).join('') : '<div class="empty">Sin posiciones abiertas</div>';
  }

  // Radar
  const winners = Array.isArray(d.winners) ? d.winners : [];
  _entryLevels = Array.isArray(d.entry_levels) && d.entry_levels.length ? d.entry_levels : [50];
  _winSyms = new Set(winners.map(w => w.symbol));
  q('winnerCount').textContent = winners.length;
  const winSig = JSON.stringify(winners.map(w => [
    w.symbol, n(w.cooldown_remaining) > 0, !!w.price_blocked, !!w.can_short, n(w.change) >= _entryLevels[0]
  ]));
  if (winSig !== _winSig) {
    _winSig = winSig;
    q('tbWinners').innerHTML = winners.length ? winners.map(w => {
      const change = n(w.change), cdSecs = n(w.cooldown_remaining);
      const inCd = cdSecs > 0, canTrade = change >= _entryLevels[0] && !inCd;
      const rowCls = inCd ? 'in-cooldown' : (canTrade ? 'can-trade' : '');
      if (inCd && !_cdData[w.symbol]) _cdData[w.symbol] = { remaining_s: cdSecs, unblock_utc: w.cooldown_str || '' };
      let st;
      if (inCd) st = `<span id="cd_${w.symbol}" class="cd-badge">🔒 ${fmtCd(cdSecs)}</span>`;
      else if (w.price_blocked) st = `<span id="cd_${w.symbol}" class="negative">⛔ precio alto</span>`;
      else if (canTrade) st = `<span id="cd_${w.symbol}" class="positive">✓ libre</span>`;
      else st = `<span id="cd_${w.symbol}" class="muted">—</span>`;
      return `<tr id="wrow_${w.symbol}" class="${rowCls}">
        <td><a class="sym" style="font-size:14px" href="https://www.binance.com/en/futures/${w.symbol}" target="_blank" rel="noopener">${w.symbol}</a></td>
        <td id="wc_${w.symbol}" class="${cls(change)}" style="font-weight:700">${pct(change)}</td>
        <td id="wp_${w.symbol}">${px(w.price)}</td>
        <td>${st}</td>
      </tr>`;
    }).join('') : '<tr><td colspan="4" class="muted">Esperando símbolos que superen el umbral…</td></tr>';
  }

  // Cierres
  const closed = Array.isArray(d.closed_trades) ? d.closed_trades : [];
  q('totalRealizedBadge').innerHTML = closed.length
    ? `· total realizado: <b class="${cls(rp)}">${sgn(rp)} USDT</b>` : '';
  const reasonLabel = r => r === 'TP' ? '<span class="r-tp">✅ TP</span>'
    : r === 'SL' ? '<span class="r-sl">⛔ SL</span>'
    : r === 'MANUAL' ? '<span class="r-manual">✋ Manual</span>'
    : `<span class="muted">${r || '—'}</span>`;
  q('tbClosed').innerHTML = closed.length ? closed.map(t => `<tr>
      <td data-label="Símbolo" style="font-weight:800">${t.symbol || ''}</td>
      <td data-label="Motivo">${reasonLabel(t.reason)}</td>
      <td data-label="PnL" class="${cls(t.pnl)}" style="font-weight:700">${sgn(t.pnl)}</td>
      <td data-label="MFE" class="positive">${t.mfe_usd === undefined ? '—' : sgn(t.mfe_usd)}</td>
      <td data-label="MAE" class="negative">${t.mae_usd === undefined ? '—' : sgn(t.mae_usd)}</td>
      <td data-label="Duración">${t.duration_s === undefined ? '—' : fmtDur(t.duration_s)}</td>
      <td data-label="Entrada">${px(t.avg_entry)}</td>
      <td data-label="Cierre">${px(t.close_price)}</td>
      <td data-label="Bloqueado hasta" style="color:var(--orange)">${t.unblock_at || '—'}</td>
      <td data-label="Fecha" class="muted">${t.closed_at || ''}</td>
    </tr>`).join('') : '<tr><td colspan="10" class="muted">Sin cierres aún</td></tr>';

  q('events').textContent = (Array.isArray(d.events) ? d.events : []).join('\n');
}

// ── Refresco en vivo: parchea celdas ────────────────────────────────────────
function applyLive(d) {
  if (!d) return;
  let mismatch = false;
  const pos = d.positions || {};
  const keys = Object.keys(pos);
  for (const sym of keys) {
    const p = pos[sym];
    if (!q('prow_' + sym)) { mismatch = true; continue; }
    setTxt('pp_' + sym, px(p.p));
    const c = q('pc_' + sym);
    if (c) { setTxt('pc_' + sym, pct(p.c)); c.className = 'chg ' + cls(p.c); }
    const pn = q('ppnl_' + sym);
    if (pn) { setTxt('ppnl_' + sym, sgn(p.pnl)); pn.className = 'pnl ' + cls(p.pnl); }
    setTxt('psl_' + sym, px(p.sl));
    const slc = q('pslc_' + sym);
    if (slc) slc.style.cssText = slStyle(n(p.sl), n(p.p));
    setTxt('pmfe_' + sym, sgn(p.mfe));
    setTxt('pmae_' + sym, sgn(p.mae));
  }
  if (keys.length !== _posSyms.size) mismatch = true;

  const wins = Array.isArray(d.winners) ? d.winners : [];
  const body = q('tbWinners');
  let idx = 0;
  for (const w of wins) {
    const row = q('wrow_' + w.s);
    if (!row) { mismatch = true; continue; }
    setTxt('wp_' + w.s, px(w.p));
    const c = q('wc_' + w.s);
    if (c) { setTxt('wc_' + w.s, pct(w.c)); c.className = cls(w.c); }
    if (body.children[idx] !== row) body.insertBefore(row, body.children[idx] || null);
    idx++;
  }
  if (wins.length !== _winSyms.size) mismatch = true;

  const pu = n(d.total_unrealized);
  setTxt('pnl', sgn(pu)); q('pnl').className = 'value ' + cls(pu);
  setTxt('scanCount', String(n(d.scan_count)));
  setTxt('evalRate', fx(d.eval_rate, 0) + '/s');
  setTxt('latency', `${fx(d.latency_avg_ms, 1)} / ${fx(d.latency_max_ms, 1)} ms`);
  setTxt('subCount', String(n(d.watch_count)));
  setTxt('rbWatchK', String(n(d.watch_count)));
  setTxt('livePollCount', String(livePollCount));

  if (n(d.scan_count) !== _prevScan) {
    _prevScan = n(d.scan_count);
    q('dotEngine').classList.add('on');
    setTimeout(() => q('dotEngine').classList.remove('on'), 120);
  }
  if (mismatch && !_persisted) requestFull();
}

// ── Estadísticas MFE / MAE ──────────────────────────────────────────────────
function renderStats(s) {
  if (!s) return;
  setTxt('stCount', String(n(s.count)));
  setTxt('stWin', s.count ? fx(s.win_rate, 1) + '%' : '—');
  const all = (s.groups || {}).ALL;
  setTxt('stMfe', all && all.n ? '+' + fx(all.mfe_usd.median, 3) : '—');
  setTxt('stMae', all && all.n ? '−' + fx(all.mae_usd.median, 3) : '—');

  const names = { ALL: 'Todas', TP: '✅ TP', SL: '⛔ SL', MANUAL: '✋ Manual' };
  const rows = ['ALL', 'TP', 'SL', 'MANUAL'].filter(k => (s.groups || {})[k] && s.groups[k].n)
    .map(k => { const g = s.groups[k]; return `<tr>
      <td style="font-weight:700">${names[k]}</td><td>${g.n}</td>
      <td class="positive">${fx(g.mfe_usd.median, 3)}</td><td class="positive">${fx(g.mfe_usd.p90, 3)}</td>
      <td class="negative">${fx(g.mae_usd.median, 3)}</td><td class="negative">${fx(g.mae_usd.p90, 3)}</td>
      <td>${fx(g.mfe_pct.median, 1)}%</td><td>${fx(g.mae_pct.median, 1)}%</td>
      <td>${fmtDur(g.duration_s.median)}</td></tr>`; });
  q('tbStatGroups').innerHTML = rows.length ? rows.join('') : '<tr><td colspan="9" class="muted">Sin datos aún</td></tr>';

  const sim = Array.isArray(s.sl_sim) ? s.sl_sim : [];
  q('tbSlSim').innerHTML = sim.length && sim[0].tp_total
    ? sim.map(r => `<tr><td>${fx(r.sl, 1)}</td><td>${r.tp_stopped} / ${r.tp_total}</td>
        <td class="${n(r.pct) > 20 ? 'negative' : n(r.pct) > 5 ? 'warn' : 'positive'}">${fx(r.pct, 0)}%</td></tr>`).join('')
    : '<tr><td colspan="3" class="muted">Aún no hay TP registrados</td></tr>';
}

let statsTimer = null;
async function loadStats() {
  try {
    const resp = await fetch('/api/stats', { cache: 'no-store' });
    if (resp.ok) renderStats(await resp.json());
  } catch (e) { /* silencioso */ }
  finally { clearTimeout(statsTimer); statsTimer = setTimeout(loadStats, 15000); }
}

// ── SL global ───────────────────────────────────────────────────────────────
async function saveGlobalSl() {
  const msg = q('gslMsg'), btn = q('gslSave');
  const v = parseFloat(String(q('gslInput').value).replace(',', '.'));
  msg.className = 'msg';
  if (isNaN(v) || v >= 0) {
    msg.className = 'msg negative'; msg.textContent = 'El SL global debe ser un número negativo (por ejemplo -6).'; return;
  }
  btn.disabled = true; btn.textContent = '…';
  try {
    const resp = await fetch('/api/set-default-sl', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, cache: 'no-store',
      body: JSON.stringify({ sl_usd: v, override_manual: q('gslOverride').checked }),
    });
    const data = await resp.json();
    if (data.ok) {
      delete q('gslInput').dataset.dirty;
      const upd = (data.updated || []).length;
      msg.className = 'msg positive';
      msg.textContent = `✓ SL global = ${fx(v, 2)} USD · ${upd} posición(es) abierta(s) actualizada(s)`;
      requestFull(true);
    } else {
      msg.className = 'msg negative'; msg.textContent = data.error || `Error HTTP ${resp.status}`;
    }
  } catch (err) {
    msg.className = 'msg negative'; msg.textContent = 'Error de red: ' + err.message;
  } finally {
    btn.disabled = false; btn.textContent = 'Aplicar';
  }
}

// ── Modal SL por posición ───────────────────────────────────────────────────
let _slModalSymbol = null;
function editStopLoss(symbol, currentSl) {
  _slModalSymbol = symbol;
  q('slModalSymbol').textContent = symbol;
  q('slModalInput').value = currentSl;
  q('slModalError').style.display = 'none';
  q('slModalOverlay').style.display = 'flex';
  setTimeout(() => q('slModalInput').focus(), 50);
}
function closeSlModal() { q('slModalOverlay').style.display = 'none'; _slModalSymbol = null; }

async function saveSlModal() {
  const symbol = _slModalSymbol;
  if (!symbol) return;
  const slUsd = parseFloat(String(q('slModalInput').value).replace(',', '.'));
  if (isNaN(slUsd) || slUsd >= 0) {
    q('slModalError').textContent = 'El Stop Loss debe ser un número negativo (por ejemplo -5).';
    q('slModalError').style.display = 'block'; return;
  }
  const saveBtn = q('slModalSave');
  saveBtn.disabled = true; saveBtn.textContent = '…';
  try {
    const resp = await fetch(`/api/set-sl/${symbol}`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sl_usd: slUsd }), cache: 'no-store',
    });
    const data = await resp.json();
    if (data.ok) { closeSlModal(); requestFull(true); }
    else { q('slModalError').textContent = data.error || `Error HTTP ${resp.status}`; q('slModalError').style.display = 'block'; }
  } catch (err) {
    q('slModalError').textContent = `Error de red: ${err.message}`; q('slModalError').style.display = 'block';
  } finally { saveBtn.disabled = false; saveBtn.textContent = 'Guardar'; }
}

document.addEventListener('DOMContentLoaded', () => {
  q('slModalCancel').addEventListener('click', closeSlModal);
  q('slModalSave').addEventListener('click', saveSlModal);
  q('slModalOverlay').addEventListener('click', e => { if (e.target.id === 'slModalOverlay') closeSlModal(); });
  q('slModalInput').addEventListener('keydown', e => {
    if (e.key === 'Enter') saveSlModal();
    if (e.key === 'Escape') closeSlModal();
  });
  q('gslSave').addEventListener('click', saveGlobalSl);
  q('gslInput').addEventListener('input', () => { q('gslInput').dataset.dirty = '1'; });
  q('gslInput').addEventListener('keydown', e => { if (e.key === 'Enter') saveGlobalSl(); });
});

// ── Cierre manual ───────────────────────────────────────────────────────────
async function closePosition(symbol, btn) {
  if (!confirm(`¿Cerrar posición ${symbol} al precio actual de mercado?\n\nEsta acción es irreversible.`)) return;
  btn.disabled = true; btn.textContent = '…';
  try {
    const resp = await fetch(`/api/close/${symbol}`, { method: 'POST', cache: 'no-store' });
    const data = await resp.json();
    if (data.ok) { btn.textContent = '✓ Cerrada'; requestFull(true); setTimeout(loadStats, 1500); }
    else { btn.disabled = false; btn.textContent = 'Cerrar posición'; alert(`Error al cerrar ${symbol}: ${data.error || `HTTP ${resp.status}`}`); }
  } catch (err) {
    btn.disabled = false; btn.textContent = 'Cerrar posición';
    alert(`Error de red al cerrar ${symbol}: ${err.message}`);
  }
}

// ── Bucle 1: precios en vivo ────────────────────────────────────────────────
let liveTimer = null, liveFails = 0;
async function livePoll() {
  const t0 = performance.now();
  try {
    const resp = await fetch('/api/live', { cache: 'no-store' });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    livePollCount++; liveFails = 0;
    q('dotLive').classList.add('on');
    applyLive(data);
  } catch (err) {
    liveFails++; q('dotLive').classList.remove('on');
  } finally {
    const spent = performance.now() - t0;
    const backoff = liveFails ? Math.min(3000, liveFails * 300) : 0;
    liveTimer = setTimeout(livePoll, Math.max(0, livePollMs - spent) + backoff);
  }
}

// ── Bucle 2: estructura completa ────────────────────────────────────────────
let statusTimer = null, statusDelay = 1500, statusFailing = false, statusInFlight = false, _lastFull = 0;
async function pollStatus() {
  if (statusInFlight) return;
  statusInFlight = true; _lastFull = Date.now();
  try {
    const resp = await fetch('/api/status', { cache: 'no-store' });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    pollCount++;
    q('dotPoll').classList.add('on');
    statusDelay = statusPollMs; statusFailing = false;
    render(data);
  } catch (err) {
    q('dotPoll').classList.remove('on');
    if (!statusFailing) { console.warn('Poll error:', err.message); statusFailing = true; }
    statusDelay = Math.min(statusDelay * 1.5, 15000);
  } finally {
    statusInFlight = false;
    statusTimer = setTimeout(pollStatus, statusDelay);
  }
}
function requestFull(force) {
  if (statusInFlight) return;
  if (!force && Date.now() - _lastFull < 1000) return;
  if (statusTimer) clearTimeout(statusTimer);
  statusTimer = setTimeout(pollStatus, force ? 250 : 0);
}

livePoll();
pollStatus();
loadStats();
window.addEventListener('beforeunload', () => {
  [liveTimer, statusTimer, statsTimer].forEach(t => t && clearTimeout(t));
});
</script>
</body>
</html>
"""

# ─────────────────────────────────────────────────────────────────────────────
# RUTAS FLASK
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/")
def index():
    resp = make_response(HTML)
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


@app.get("/api/status")
def api_status():
    resp = jsonify(bot.snapshot())
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


@app.get("/api/live")
def api_live():
    """Payload mínimo (precio, cambio y PnL) para el refresco en vivo del navegador."""
    resp = jsonify(bot.live_payload())
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


def _not_owner_response():
    """409 si esta instancia no controla el estado (standby, esperando o apagándose):
    un cambio aquí no se guardaría ni se ejecutaría."""
    if bot.store.can_act():
        return None
    mode = bot.store.status()["mode"]
    return jsonify({"ok": False, "error": f"Esta instancia no controla el estado ({mode}); "
                                         "otra instancia opera o se está recuperando. "
                                         "Reintenta en unos segundos."}), 409


@app.post("/api/close/<symbol>")
def api_close(symbol: str):
    """Cierre manual de una posición abierta."""
    symbol = symbol.upper().strip()
    if not bot.loop or not bot.loop.is_running():
        return jsonify({"ok": False, "error": "Bot loop no está activo"}), 503
    busy = _not_owner_response()
    if busy:
        return busy
    future = asyncio.run_coroutine_threadsafe(
        bot.close_position_manual(symbol), bot.loop
    )
    try:
        ok = future.result(timeout=15)
    except concurrent.futures.TimeoutError:
        # La tarea sigue corriendo en el loop del bot; no la cancelamos para
        # no dejar el cierre a medias, pero avisamos con un mensaje claro.
        return jsonify({
            "ok": False,
            "error": "Tiempo de espera agotado cerrando posición (el cierre puede completarse en segundo plano, revisa en unos segundos)",
        }), 504
    except Exception as exc:
        msg = str(exc).strip() or f"{type(exc).__name__} (timeout esperando respuesta del exchange)"
        return jsonify({"ok": False, "error": msg}), 500
    if ok:
        return jsonify({"ok": True, "symbol": symbol, "msg": "Posición cerrada manualmente"})
    return jsonify({"ok": False, "symbol": symbol, "error": "Posición no encontrada o ya cerrada"}), 404


@app.post("/api/set-sl/<symbol>")
def api_set_sl(symbol: str):
    """Establece el stop loss (en USD, valor negativo) de una posición abierta."""
    symbol = symbol.upper().strip()
    data = request.get_json(silent=True) or {}
    try:
        sl_usd = float(data.get("sl_usd"))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "sl_usd inválido"}), 400
    if sl_usd >= 0:
        return jsonify({"ok": False, "error": "sl_usd debe ser un valor negativo (pérdida)"}), 400
    busy = _not_owner_response()
    if busy:
        return busy
    ok = bot.set_stop_loss(symbol, sl_usd)
    if ok:
        return jsonify({"ok": True, "symbol": symbol, "sl_usd": sl_usd})
    return jsonify({"ok": False, "symbol": symbol, "error": "Posición no encontrada o cerrada"}), 404


@app.post("/api/force-close/<symbol>")
def api_force_close(symbol: str):
    """Cierre de EMERGENCIA: ignora el guard anti-doble-cierre y libera el
    símbolo aunque haya quedado 'pegado' en _closing_symbols por un timeout
    o deadlock previo. Úsalo solo si /api/close se queda atascado."""
    symbol = symbol.upper().strip()
    if not bot.loop or not bot.loop.is_running():
        return jsonify({"ok": False, "error": "Bot loop no está activo"}), 503
    busy = _not_owner_response()
    if busy:
        return busy

    # 1) liberar el guard sin esperar a que la tarea vieja termine
    with bot.lock:
        bot._closing_symbols.discard(symbol)

    future = asyncio.run_coroutine_threadsafe(
        bot.close_position_manual(symbol, force=True), bot.loop
    )
    try:
        ok = future.result(timeout=20)
    except concurrent.futures.TimeoutError:
        return jsonify({
            "ok": False,
            "error": "Tiempo de espera agotado en cierre forzado (revisa /api/status en unos segundos)",
        }), 504
    except Exception as exc:
        msg = str(exc).strip() or f"{type(exc).__name__} (timeout esperando respuesta del exchange)"
        return jsonify({"ok": False, "error": msg}), 500
    if ok:
        return jsonify({"ok": True, "symbol": symbol, "msg": "Posición cerrada (forzado)"})
    return jsonify({"ok": False, "symbol": symbol, "error": "Posición no encontrada o ya cerrada"}), 404


@app.get("/api/stats")
def api_stats():
    """Estadísticas agregadas de MFE / MAE de todas las operaciones cerradas."""
    resp = jsonify(bot.compute_stats())
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


@app.get("/api/trades")
def api_trades():
    """Operaciones cerradas con su MFE/MAE (últimas N, por defecto 200)."""
    try:
        limit = max(1, min(5000, int(request.args.get("limit", "200"))))
    except ValueError:
        limit = 200
    with bot._stats_lock:
        rows = list(bot.trade_stats[-limit:])
    rows.reverse()
    resp = jsonify(rows)
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


@app.get("/api/trades.csv")
def api_trades_csv():
    """Descarga CSV de todo el histórico (para analizarlo en Excel / pandas)."""
    cols = [
        "trade_id", "symbol", "direction", "reason", "opened_at_ts", "closed_at_ts",
        "duration_s", "avg_entry", "close_price", "qty", "notional", "fills_n", "levels",
        "sl_usd", "target", "pnl", "pnl_pct", "mfe_usd", "mae_usd", "mfe_pct", "mae_pct",
        "time_to_mfe_s", "time_to_mae_s", "low_price", "high_price",
    ]
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(cols)
    with bot._stats_lock:
        rows = list(bot.trade_stats)
    for r in rows:
        out = []
        for c in cols:
            v = r.get(c, "")
            out.append("|".join(str(x) for x in v) if isinstance(v, list) else v)
        writer.writerow(out)
    resp = make_response(buf.getvalue())
    resp.headers["Content-Type"] = "text/csv; charset=utf-8"
    resp.headers["Content-Disposition"] = "attachment; filename=trade_stats.csv"
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


@app.post("/api/set-default-sl")
def api_set_default_sl():
    """Cambia el SL global (USD, negativo) sin reiniciar el bot."""
    data = request.get_json(silent=True) or {}
    try:
        sl_usd = float(data.get("sl_usd"))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "sl_usd inválido"}), 400
    if sl_usd >= 0:
        return jsonify({"ok": False, "error": "sl_usd debe ser un valor negativo (pérdida)"}), 400
    if sl_usd < -100000:
        return jsonify({"ok": False, "error": "sl_usd fuera de rango"}), 400
    busy = _not_owner_response()
    if busy:
        return busy
    override = bool(data.get("override_manual", False))
    result = bot.set_default_stop_loss(sl_usd, override_manual=override)
    return jsonify({"ok": True, **result})


@app.get("/health")
def health():
    snap = bot.snapshot()
    return jsonify({
        "ok":                True,
        "running":           bot.running,
        "mode":              snap["mode"],
        "ws_connected":      snap.get("ws_connected", False),
        "scan_count":        snap["scan_count"],
        "eval_rate":         snap.get("eval_rate", 0),
        "latency_avg_ms":    snap.get("latency_avg_ms", 0),
        "all_symbols_count": snap["all_symbols_count"],
        "universe_count":    snap.get("universe_count", 0),
        "subscribed_count":  snap["subscribed_count"],
        "last_error":        snap["last_error"],
        "cooldown_count":    snap["cooldown_count"],
        "persistence":       bot.store.status(),
    })


@app.get("/api/recovery")
def api_recovery():
    """Estado de la persistencia y último documento guardado (lo que se recuperaría)."""
    doc = bot.store.last_doc or bot.store.read_local()
    resp = jsonify({"status": bot.store.status(), "document": doc})
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, threaded=True)
