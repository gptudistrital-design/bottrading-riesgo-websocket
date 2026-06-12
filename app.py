"""
Bot web para shortear ganadores de Binance Futures.

ARQUITECTURA:
════════════════════════════════════════════════════════════════════════════
 1. REST inicial → /fapi/v1/exchangeInfo
      Obtiene TODOS los símbolos USDT-M perpetuos.
      Guarda la lista en SYMBOLS_CACHE_FILE como respaldo ante bloqueos.
      Si el REST falla, carga desde ese caché guardado.

 2. Refresh de lista completa cada SYMBOL_REFRESH_HOURS (12 h):
      Actualiza all_symbols vía REST y regenera el caché.
      Si Binance bloquea la IP, el caché anterior sigue siendo válido.

 3. Ciclo de filtrado cada FILTER_CYCLE_SECS (5 min):
      a. Suscribe TODOS los símbolos al WS (markPrice + @ticker 24h)
      b. Espera FULL_SUBSCRIBE_WAIT_SECS (30 s) para recibir datos de ticker
      c. Filtra: conserva solo cambio >= MIN_GAIN_FILTER (15%) O posición abierta
      d. DESUSCRIBE el resto → reinicia WS únicamente con la lista filtrada
      e. Actualiza self.winners con los símbolos activos

 4. _ws_ticker_update_loop (cada WS_TICKER_UPDATE_SECS = 5 s):
      Entre ciclos de filtrado, actualiza self.winners con los datos
      frescos del @ticker de los símbolos actualmente suscritos.

 5. KlineWebSocketCache → klines 1m para confirmación técnica de entrada.

 6. Scanner (cada SCAN_INTERVAL_SECS):
      Lee precio WS + klines + change WS, aplica niveles 50/75/100/150/200/250%.

 7. Realtime TP loop (cada 0.25 s):
      Cierra posiciones cuando PnL >= objetivo usando precios WS.

 8. fetch() polling → /api/status cada 2 s. Actualiza el DOM sin recargar.

 9. Cooldown 24 h tras cierre ganador.

FLUJO DE DATOS:
  Startup ──► REST exchangeInfo (all symbols) ──► guardar caché
                  │
                  ▼
     _filter_cycle_loop (cada 5 min)
       ├─ sub ALL symbols → espera 30s → leer ticker_cache
       ├─ filtrar: change >= 15% OR posición abierta
       ├─ desuscribir el resto (restart WS con lista reducida)
       └─ actualizar self.winners

     _ws_ticker_update_loop (cada 5 s)
       └─ mantiene self.winners actualizado entre ciclos de filtrado

ROBUSTEZ:
  - Caché de símbolos en disco: si Binance bloquea, se usan los guardados
  - asyncio.gather con return_exceptions=True
  - Supervisor (_supervised) relanza cualquier coro que muera inesperadamente
  - HTTP 418 capturado y logueado sin crashear
════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from math import floor
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode
import urllib.error
import urllib.request

from flask import Flask, jsonify, make_response, render_template_string

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from WS import SymbolWebSocketPriceCache                    # noqa: E402
from KlineWebSocketCache_v4 import KlineWebSocketCache      # noqa: E402


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────

BASE_URL      = os.getenv("BASE_URL",    "https://fapi.binance.com")
QUOTE_ASSET   = os.getenv("QUOTE_ASSET", "USDT")
PAPER_MODE    = os.getenv("PAPER_MODE",   "true").lower() == "true"
LIVE_TRADING  = os.getenv("LIVE_TRADING", "false").lower() == "true"
API_KEY       = os.getenv("BINANCE_API_KEY",    "")
API_SECRET    = os.getenv("BINANCE_API_SECRET", "")
LEVERAGE      = int(os.getenv("LEVERAGE", "1"))
STATE_FILE    = os.getenv("STATE_FILE", os.path.join(tempfile.gettempdir(), "botshort_state.json"))
# ── Gestión de símbolos ───────────────────────────────────────────────────────

# ── Gestión de símbolos ───────────────────────────────────────────────────────
INITIAL_SYMBOLS = [
    s.strip()
    for s in os.getenv("INITIAL_SYMBOLS", "").split(",")
    if s.strip()
]

# ── Gestión de símbolos ───────────────────────────────────────────────────────
# Lista completa de símbolos: REST inicial + caché en disco + refresh cada 12 h
SYMBOLS_CACHE_FILE   = os.getenv(
    "SYMBOLS_CACHE_FILE",
    os.path.join(tempfile.gettempdir(), "futures_symbols_cache.json")
)
SYMBOL_REFRESH_HOURS = int(os.getenv("SYMBOL_REFRESH_HOURS", "12"))

# ── Ciclo de filtrado ─────────────────────────────────────────────────────────
# Cada FILTER_CYCLE_SECS: suscribir todos → esperar ticker → filtrar → desuscribir resto
FILTER_CYCLE_SECS        = int(os.getenv("FILTER_CYCLE_SECS",        "300"))  # 5 min
MIN_GAIN_FILTER          = float(os.getenv("MIN_GAIN_FILTER",        "40.0"))  # >=15% para quedar suscrito
FULL_SUBSCRIBE_WAIT_SECS = int(os.getenv("FULL_SUBSCRIBE_WAIT_SECS", "30"))   # espera ticker tras sub ALL

# ── Actualización en tiempo real del ranking (entre ciclos de filtrado) ───────
WS_TICKER_UPDATE_SECS = float(os.getenv("WS_TICKER_UPDATE_SECS", "5.0"))

# ── Resto de parámetros operativos ────────────────────────────────────────────
SCAN_INTERVAL_SECS   = int(os.getenv("SCAN_INTERVAL_SECS",   "2"))
SCAN_LOG_EVERY       = int(os.getenv("SCAN_LOG_EVERY",       "15"))
MIN_GAIN_TO_SHOW     = float(os.getenv("MIN_GAIN_TO_SHOW",   "0"))
COOLDOWN_SECONDS     = int(os.getenv("COOLDOWN_SECONDS",     "86400"))

# Tiempo de gracia al detener un cache WS antes de arrancar el nuevo (segundos)
WS_STOP_GRACE        = float(os.getenv("WS_STOP_GRACE", "0.8"))

# Precio máximo permitido para abrir nuevas entradas (bloqueo permanente si supera)
MAX_PRICE_BLOCK = float(os.getenv("MAX_PRICE_BLOCK", "1.5"))

ENTRY_LEVELS    = [float(x) for x in os.getenv("ENTRY_LEVELS",    "50,75,100,150,200,250").split(",")]
ENTRY_NOTIONALS = [float(x) for x in os.getenv("ENTRY_NOTIONALS", "5,5,10,20,40,80").split(",")]
TAKE_PROFIT_FRACTION = float(os.getenv("TAKE_PROFIT_FRACTION", "0.14284"))


# ─────────────────────────────────────────────────────────────────────────────
# MODELOS DE DATOS
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(slots=True)
class Fill:
    level:       float
    notional:    float
    entry_price: float
    qty:         float
    opened_at:   float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "level":       self.level,
            "notional":    self.notional,
            "entry_price": self.entry_price,
            "qty":         self.qty,
            "opened_at":   self.opened_at,
        }


@dataclass(slots=True)
class BotPosition:
    symbol:       str
    fills:        List[Fill] = field(default_factory=list)
    realized_pnl: float = 0.0
    status:       str   = "OPEN"

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


# ─────────────────────────────────────────────────────────────────────────────
# CLIENTE BINANCE FUTURES
# ─────────────────────────────────────────────────────────────────────────────

class BinanceFuturesClient:
    def __init__(self) -> None:
        self.exchange_filters: Dict[str, Dict[str, float]] = {}

    async def start(self) -> None:
        await self.load_exchange_info()

    async def request(self, method: str, path: str,
                      params: Optional[dict] = None, signed: bool = False) -> Any:
        return await asyncio.to_thread(
            self._sync_request, BASE_URL, method, path, params, signed
        )

    def _sync_request(self, base_url: str, method: str, path: str,
                      params: Optional[dict] = None, signed: bool = False) -> Any:
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
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Binance HTTP {exc.code}: {body[:300]}") from exc

    async def load_exchange_info(self) -> None:
        data    = await self.request("GET", "/fapi/v1/exchangeInfo")
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

    def normalize_qty(self, symbol: str, qty: float) -> float:
        info = self.exchange_filters.get(symbol, {"stepSize": 0.001, "minQty": 0.0})
        step = info["stepSize"]
        norm = floor(qty / step) * step
        decs = max(0, len(f"{step:.12f}".rstrip("0").split(".")[-1]))
        norm = round(norm, decs)
        return norm if norm >= info.get("minQty", 0.0) else 0.0

    async def set_leverage(self, symbol: str) -> None:
        if LEVERAGE > 0 and LIVE_TRADING and not PAPER_MODE:
            await self.request(
                "POST", "/fapi/v1/leverage",
                {"symbol": symbol, "leverage": LEVERAGE}, signed=True
            )

    async def market_short(self, symbol: str, notional: float, price: float) -> float:
        min_notional = self.exchange_filters.get(symbol, {}).get("minNotional", 5.0)
        effective    = max(notional, min_notional)
        qty          = self.normalize_qty(symbol, effective / price)
        if qty <= 0:
            raise RuntimeError(f"Qty inválida {symbol}: notional={effective} price={price}")
        if PAPER_MODE or not LIVE_TRADING:
            return qty
        await self.set_leverage(symbol)
        await self.request("POST", "/fapi/v1/order",
            {"symbol": symbol, "side": "SELL", "type": "MARKET", "quantity": qty},
            signed=True)
        return qty

    async def close_short(self, symbol: str, qty: float) -> None:
        qty = self.normalize_qty(symbol, qty)
        if qty <= 0 or PAPER_MODE or not LIVE_TRADING:
            return
        await self.request("POST", "/fapi/v1/order",
            {"symbol": symbol, "side": "BUY", "type": "MARKET",
             "quantity": qty, "reduceOnly": "true"},
            signed=True)


# ─────────────────────────────────────────────────────────────────────────────
# BOT PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

class TradingBot:
    def __init__(self) -> None:
        self.client   = BinanceFuturesClient()
        self.positions: Dict[str, BotPosition] = {}
        self.winners:   List[dict] = []
        self.closed_trades: List[dict] = []
        self.events:        List[str]  = []
        self.lock = threading.Lock()

        # Cooldown: symbol → timestamp hasta el que está bloqueado
        self.symbol_cooldown: Dict[str, float] = {}

        # Bloqueo permanente por precio
        self.price_blocked: set = set()

        # ── Lista completa de símbolos (cargada en init, refresh c/12h) ──
        self.all_symbols:              List[str] = []
        self.last_symbols_refresh_at:  float     = 0.0

        # ── WS caches ─────────────────────────────────────────────────────
        self.price_cache:        Optional[SymbolWebSocketPriceCache] = None
        self.kline_cache:        Optional[KlineWebSocketCache]       = None
        self.subscribed_symbols: List[str] = []

        # ── Métricas ──────────────────────────────────────────────────────
        self.running              = False
        self.scan_count           = 0
        self.last_scan_at         = 0.0
        self.filter_cycle_count   = 0           # cuántos ciclos de filtrado se han ejecutado
        self.last_filter_cycle_at = 0.0         # timestamp del último ciclo de filtrado
        self.last_ws_ticker_at    = 0.0         # timestamp de última actualización WS ranking
        self.ws_ticker_update_count = 0
        self.last_error           = ""
        self.last_startup_err     = ""
        self.exchange_symbols     = 0
        self.started_at           = time.time()
        self._sse_snapshot: str   = "{}"
        self._last_persist_at: float = 0.0

        self.loop:   Optional[asyncio.AbstractEventLoop] = None
        self.thread: Optional[threading.Thread] = None

    # ── Logging ───────────────────────────────────────────────────────────────

    def log(self, msg: str) -> None:
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        line  = f"{stamp} | {msg}"
        print(line, flush=True)
        with self.lock:
            self.events.insert(0, line)
            del self.events[100:]

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

    async def _main(self) -> None:
        self.log("Bot iniciado — modo " + (
            "PAPER" if PAPER_MODE or not LIVE_TRADING else "REAL"
        ))
        try:
            await self.client.start()
            self.exchange_symbols = len(self.client.exchange_filters)
            self.log(f"ExchangeInfo: {self.exchange_symbols} contratos USDT-M perpetuos")
        except Exception as exc:
            self.last_startup_err = str(exc)
            self.log(f"ExchangeInfo falló ({exc}). Continúo con filtros mínimos.")

        # Cargar lista completa de símbolos (caché en disco o REST)
        await self._init_all_symbols()

        await asyncio.gather(
            self._supervised(self._all_symbols_refresh_loop,  "_all_symbols_refresh_loop"),
            self._supervised(self._filter_cycle_loop,          "_filter_cycle_loop"),
            self._supervised(self._ws_ticker_update_loop,      "_ws_ticker_update_loop"),
            self._supervised(self._scanner,                    "_scanner"),
            self._supervised(self._realtime_price_loop,        "_realtime_price_loop"),
            self._supervised(self._snapshot_loop,              "_snapshot_loop"),
            return_exceptions=True,
        )

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

    def _start_price_cache(self, symbols: List[str]) -> None:
        normalized = [s.upper() for s in symbols]
        if self.price_cache and self.subscribed_symbols == normalized:
            return
        self._stop_price_cache()
        if not normalized:
            return
        self.price_cache = SymbolWebSocketPriceCache(
            normalized,
            symbols_per_connection=30,
        )
        self.price_cache.start()
        self.log(f"PriceCache iniciado con {len(normalized)} símbolos (markPrice + @ticker)")

    def _start_kline_cache(self, symbols: List[str]) -> None:
        normalized = [s.upper() for s in symbols]
        if self.kline_cache and self.subscribed_symbols == normalized:
            return
        self._stop_kline_cache()
        if not normalized:
            return
        pairs = {sym: ["1m"] for sym in normalized}
        self.kline_cache = KlineWebSocketCache(
            pairs                           = pairs,
            max_candles                     = 1,
            include_open_candle             = True,
            backfill_on_start               = False,
            streams_per_connection          = 30,
            rest_concurrency                = 5,
            rest_retries                    = 3,
            backfill_batch_size             = 3,
            backfill_batch_delay            = 0.25,
            safety_refresh_interval_seconds = 1500,
        )
        self.kline_cache.start()
        self.log(f"KlineCache iniciado con {len(normalized)} símbolos (1m)")

    # ── Caché de símbolos en disco ─────────────────────────────────────────────

    def _load_symbols_from_cache(self) -> List[str]:
        """Lee la lista de símbolos desde el archivo de caché en disco."""
        try:
            if not os.path.exists(SYMBOLS_CACHE_FILE):
                return []
            with open(SYMBOLS_CACHE_FILE, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            symbols   = data.get("symbols", [])
            saved_at  = data.get("saved_at", 0)
            age_hours = (time.time() - saved_at) / 3600
            if symbols:
                self.log(
                    f"Caché de símbolos cargado: {len(symbols)} símbolos "
                    f"(guardado hace {age_hours:.1f} h)"
                )
            return symbols if isinstance(symbols, list) else []
        except Exception as exc:
            self.log(f"No pude leer caché de símbolos: {exc}")
            return []

    def _save_symbols_to_cache(self, symbols: List[str]) -> None:
        """Guarda la lista de símbolos en disco para recuperación ante bloqueos."""
        tmp = f"{SYMBOLS_CACHE_FILE}.tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump({"symbols": symbols, "saved_at": time.time()}, fh)
            os.replace(tmp, SYMBOLS_CACHE_FILE)
            self.log(f"Caché de símbolos guardado: {len(symbols)} símbolos → {SYMBOLS_CACHE_FILE}")
        except Exception as exc:
            self.log(f"No pude guardar caché de símbolos: {exc}")

    # ── REST: obtención de todos los símbolos ─────────────────────────────────

    async def _refresh_all_symbols(self) -> bool:
        """
        Obtiene todos los símbolos USDT-M perpetuos vía REST (exchangeInfo).
        Guarda el resultado en SYMBOLS_CACHE_FILE como respaldo.
        Devuelve True si tuvo éxito.
        """
        try:
            self.log("REST: obteniendo lista completa de símbolos de futuros USDT-M...")
            data    = await self.client.request("GET", "/fapi/v1/exchangeInfo")
            filters = self.client.exchange_filters

            symbols: List[str] = []
            for sym_info in data.get("symbols", []):
                if sym_info.get("quoteAsset")   != QUOTE_ASSET:  continue
                if sym_info.get("contractType") != "PERPETUAL":  continue
                if sym_info.get("status")       != "TRADING":    continue
                s = sym_info["symbol"]
                # Si exchange_filters ya está cargado, usarlo como filtro extra
                if filters and s not in filters:
                    continue
                symbols.append(s)

            if not symbols:
                self.log("REST: respuesta vacía al obtener símbolos")
                return False

            self.all_symbols             = symbols
            self.last_symbols_refresh_at = time.time()
            self._save_symbols_to_cache(symbols)
            self.log(f"REST: {len(symbols)} símbolos cargados y guardados en caché")

            if symbols:
               with self.lock:
                   self.price_blocked.clear()   # ← agregar esto al refrescar
               self.log("price_blocked reseteado con el refresh de símbolos")
             
            return True

        except RuntimeError as exc:
            msg = str(exc)
            if "418" in msg:
                self.log(
                    f"REST 418 (IP rate-limit Binance) al obtener símbolos — "
                    f"usando caché si está disponible"
                )
                self.last_error = "HTTP 418 – rate-limit Binance REST al cargar símbolos"
            else:
                self.last_error = msg
                self.log(f"REST _refresh_all_symbols falló: {msg}")
            return False
        except Exception as exc:
            self.last_error = str(exc)
            self.log(f"REST _refresh_all_symbols error: {exc}")
            return False

    async def _init_all_symbols(self) -> None:
          # Intento 1: caché en disco
          cached = self._load_symbols_from_cache()
          if cached:
              self.all_symbols             = cached
              self.last_symbols_refresh_at = time.time()
              asyncio.ensure_future(self._maybe_refresh_symbols_cache())
              return
           # Intento 2: lista inicial fija 
          if INITIAL_SYMBOLS:
              self.all_symbols             = INITIAL_SYMBOLS.copy()
              self.last_symbols_refresh_at = time.time()
              self.log(f"Usando lista inicial fija: {len(self.all_symbols)} símbolos")
              return
          # Intento 3: REST
          success = await self._refresh_all_symbols()
          if success:
              return        
          # Intento 4: exchange_filters como último recurso
          if self.client.exchange_filters:
              self.all_symbols             = list(self.client.exchange_filters.keys())
              self.last_symbols_refresh_at = time.time()
              self.log(f"Usando exchange_filters como fallback: {len(self.all_symbols)} símbolos")
              return
      
          self.log(
              "ADVERTENCIA: No hay símbolos disponibles. "
              "El bot esperará hasta que se obtenga la lista."
          )
     
    async def _maybe_refresh_symbols_cache(self) -> None:
        """Refresca el caché si tiene más de SYMBOL_REFRESH_HOURS horas."""
        age = time.time() - self.last_symbols_refresh_at
        if age > SYMBOL_REFRESH_HOURS * 3600:
            await self._refresh_all_symbols()

    async def _all_symbols_refresh_loop(self) -> None:
        """Refresca la lista completa de símbolos cada SYMBOL_REFRESH_HOURS."""
        while self.running:
            await asyncio.sleep(SYMBOL_REFRESH_HOURS * 3600)
            if not self.running:
                break
            self.log(
                f"Refresh de símbolos programado (cada {SYMBOL_REFRESH_HOURS} h)..."
            )
            await self._refresh_all_symbols()

    # ── Ciclo de filtrado: núcleo de la nueva arquitectura ────────────────────

    async def _filter_cycle_loop(self) -> None:
        """
        Cada FILTER_CYCLE_SECS (5 min):

          Fase 1 — Suscribir TODOS los símbolos (markPrice + @ticker).
                   Esto incluye los que tienen posiciones abiertas.

          Fase 2 — Esperar FULL_SUBSCRIBE_WAIT_SECS para recibir datos de ticker.

          Fase 3 — Filtrar:
                   • Mantiene suscritos: change >= MIN_GAIN_FILTER (15%) OR posición abierta
                   • Descarta el resto

          Fase 4 — Desuscribir el resto: reinicia WS únicamente con la lista filtrada.
                   Reinicia también kline_cache con la lista filtrada.

          Fase 5 — Actualiza self.winners con los símbolos activos.

        Así ningún movimiento sorpresivo de mercado pasa desapercibido,
        y la memoria/conexiones se usan solo para los símbolos relevantes.
        """
        # Esperar hasta que all_symbols esté poblado
        for _ in range(120):
            if not self.running:
                return
            if self.all_symbols:
                break
            await asyncio.sleep(1.0)

        if not self.all_symbols:
            self.log("[filter_cycle] No hay símbolos disponibles. Abortando ciclo.")
            return

        while self.running:
            n_total   = len(self.all_symbols)
            open_syms = set(self._open_position_symbols())

            # Combinar all_symbols + posiciones abiertas (sin duplicados)
            symbols_to_scan = list(dict.fromkeys([*self.all_symbols, *open_syms]))

            self.log(
                f"[filter_cycle] Ciclo #{self.filter_cycle_count + 1}: "
                f"suscribiendo {len(symbols_to_scan)} símbolos para escaneo completo "
                f"(posiciones abiertas: {len(open_syms)})..."
            )

            # ── Fase 1: Suscribir TODOS al WS ────────────────────────────
            await asyncio.to_thread(self._start_price_cache, symbols_to_scan)

            # ── Fase 2: Esperar datos de @ticker ─────────────────────────
            self.log(
                f"[filter_cycle] Esperando {FULL_SUBSCRIBE_WAIT_SECS}s "
                f"para recibir datos de @ticker..."
            )
            try:
                await asyncio.sleep(FULL_SUBSCRIBE_WAIT_SECS)
            except asyncio.CancelledError:
                if not self.running:
                    break

            if not self.running:
                break

            # ── Fase 3: Leer tickers y filtrar ───────────────────────────
            all_tickers: Dict[str, dict] = {}
            try:
                if self.price_cache:
                    all_tickers = self.price_cache.get_all_tickers()
            except Exception:
                pass

            # Refrescar open_syms por si cambió durante la espera
            open_syms = set(self._open_position_symbols())

            tickers_received = len(all_tickers)
            filtered_entries: List[dict] = []

            for sym in symbols_to_scan:
                ticker  = all_tickers.get(sym)
                in_open = sym in open_syms

                change = 0.0
                price  = 0.0
                if ticker:
                    change = ticker.get("change_pct", 0.0)
                    price  = ticker.get("last_price",  0.0)

                # Criterio: cambio >= umbral O posición abierta
                if change >= MIN_GAIN_FILTER or in_open:
                    if price > 0 or in_open:  # evitar entradas sin precio real
                        filtered_entries.append({
                            "symbol":    sym,
                            "change":    change,
                            "price":     price,
                            "market":    "futures",
                            "can_short": True,
                        })

            # Ordenar de mayor a menor cambio
            filtered_entries.sort(key=lambda x: x["change"], reverse=True)
            filtered_symbols = [e["symbol"] for e in filtered_entries]

            n_filtered     = len(filtered_entries)
            n_by_gain      = sum(1 for e in filtered_entries if e["change"] >= MIN_GAIN_FILTER)
            n_by_pos       = len(open_syms)
            top_info       = (
                f"{filtered_entries[0]['symbol']} {filtered_entries[0]['change']:.1f}%"
                if filtered_entries else "ninguno"
            )

            self.log(
                f"[filter_cycle] {tickers_received}/{len(symbols_to_scan)} tickers | "
                f"{n_filtered} clasificados: "
                f">={MIN_GAIN_FILTER:.0f}%={n_by_gain}, posiciones={n_by_pos} | "
                f"top={top_info}"
            )

            # ── Fase 4: Reiniciar WS con lista filtrada (desuscribir resto) ─
            # Si no hay ningún símbolo (mercado plano), suscribir mínimo para no quedarse ciego
            active_list = filtered_symbols if filtered_symbols else (
                list(open_syms) or self.all_symbols[:10]
            )

            await asyncio.to_thread(self._start_price_cache, active_list)
            await asyncio.to_thread(self._start_kline_cache,  active_list)

            # ── Fase 5: Actualizar winners y métricas ─────────────────────
            with self.lock:
                self.winners              = filtered_entries
                self.subscribed_symbols   = list(active_list)
                self.last_filter_cycle_at = time.time()
                self.filter_cycle_count  += 1

            # La persistencia completa se hace solo en eventos de trade.

            # ── Esperar siguiente ciclo ───────────────────────────────────
            try:
                await asyncio.sleep(FILTER_CYCLE_SECS)
            except asyncio.CancelledError:
                if not self.running:
                    break

    # ── WS Ticker: actualización en tiempo real del ranking entre ciclos ──────

    async def _ws_ticker_update_loop(self) -> None:
        """
        Mantiene self.winners actualizado entre ciclos de filtrado.

        Cada WS_TICKER_UPDATE_SECS segundos lee get_all_tickers() del
        price_cache (que solo cubre los símbolos actualmente suscritos)
        y actualiza el campo 'change' de cada winner.
        """
        self.log(
            f"[ws_ticker] Loop de ranking en tiempo real iniciado "
            f"(intervalo={WS_TICKER_UPDATE_SECS:.0f}s)"
        )

        # Esperar hasta que el WS tenga sus primeros datos
        for _ in range(60):
            if not self.running:
                return
            try:
                if self.price_cache and self.price_cache.get_all_tickers():
                    break
            except Exception:
                pass
            await asyncio.sleep(1.0)

        while self.running:
            try:
                if self.price_cache:
                    all_tickers: Dict[str, dict] = {}
                    try:
                        all_tickers = self.price_cache.get_all_tickers()
                    except Exception:
                        pass

                    if all_tickers:
                        ws_updated = 0
                        with self.lock:
                            new_winners: List[dict] = []
                            for w in self.winners:
                                sym    = w["symbol"]
                                ticker = all_tickers.get(sym)
                                if ticker:
                                    new_w            = dict(w)
                                    new_w["change"]  = ticker["change_pct"]
                                    new_w["price"]   = ticker.get("last_price", w.get("price", 0.0))
                                    new_winners.append(new_w)
                                    ws_updated += 1
                                else:
                                    new_winners.append(dict(w))

                            if ws_updated:
                                new_winners.sort(key=lambda x: x["change"], reverse=True)
                                self.winners              = new_winners
                                self.last_ws_ticker_at    = time.time()
                                self.ws_ticker_update_count += 1

            except asyncio.CancelledError:
                if not self.running:
                    break
            except Exception as exc:
                self.last_error = str(exc)
                self.log(f"[ws_ticker] Error actualizando ranking: {exc}")

            try:
                await asyncio.sleep(WS_TICKER_UPDATE_SECS)
            except asyncio.CancelledError:
                if not self.running:
                    break

    # ── Condición kline ───────────────────────────────────────────────────────

    def _kline_entry_ok(self, symbol: str) -> bool:
        """True si la última vela 1m cerrada es alcista (o sin datos)."""
        if not self.kline_cache:
            return True
        try:
            last = self.kline_cache.get_last_closed(symbol, "1m")
            if not last:
                return True
            return float(last["close"]) >= float(last["open"])
        except Exception:
            return True

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

    # ── Scanner ───────────────────────────────────────────────────────────────

    async def _scanner(self) -> None:
        """
        Bucle de entradas. Lee change desde self.winners (actualizado por
        _ws_ticker_update_loop vía WS @ticker en tiempo real).
        """
        self.log("Scanner: esperando datos de price_cache...")
        for _ in range(120):
            if not self.running:
                return
            try:
                if self.price_cache and len(self.price_cache.get_all_prices()) > 0:
                    break
            except Exception:
                pass
            await asyncio.sleep(1.0)
        self.log("Scanner: price_cache con datos — iniciando escaneos")

        while self.running:
            try:
                with self.lock:
                    winners = [dict(w) for w in self.winners]

                all_prices: Dict[str, float] = {}
                try:
                    all_prices = self.price_cache.get_all_prices() if self.price_cache else {}
                except Exception:
                    pass

                for row in winners:
                    if not row.get("can_short", True):
                        continue
                    symbol = row["symbol"]
                    change = row["change"]
                    price  = all_prices.get(symbol) or row.get("price", 0.0)
                    if price <= 0:
                        continue

                    with self.lock:
                        if symbol in self.price_blocked:
                            continue

                    kline_ok = self._kline_entry_ok(symbol)

                    for level, notional in zip(ENTRY_LEVELS, ENTRY_NOTIONALS):
                        if change >= level and kline_ok:
                            await self._ensure_short(symbol, level, notional, price, change)

                    await self._maybe_take_profit(symbol, price)

                # TP de posiciones que ya no están en winners
                with self.lock:
                    pos_syms = list(self.positions.keys())
                winner_syms = {w["symbol"] for w in winners}
                for symbol in pos_syms:
                    if symbol not in winner_syms:
                        price = all_prices.get(symbol)
                        if price:
                            await self._maybe_take_profit(symbol, price)

                with self.lock:
                    self.scan_count  += 1
                    self.last_scan_at = time.time()
                    now = time.time()
                    self.symbol_cooldown = {
                        sym: ts for sym, ts in self.symbol_cooldown.items()
                        if ts > now
                    }
                    n_cool = len(self.symbol_cooldown)

                if SCAN_LOG_EVERY > 0 and self.scan_count % SCAN_LOG_EVERY == 0:
                    n_high = sum(1 for w in winners if w["change"] >= ENTRY_LEVELS[0])
                    self.log(
                        f"Escan #{self.scan_count}: {len(winners)} activos | "
                        f">={ENTRY_LEVELS[0]:.0f}%: {n_high} | "
                        f"posiciones: {len(self.positions)} | cooldown: {n_cool}"
                    )
                # Evita escribir JSON a disco en cada escaneo; solo persistimos en trades.

            except asyncio.CancelledError:
                if not self.running:
                    break
                self.log("Scanner: CancelledError inesperado, continuando...")
                await asyncio.sleep(1.0)
            except Exception as exc:
                self.last_error = str(exc)
                self.log(f"Error en scanner: {exc}")

            try:
                await asyncio.sleep(SCAN_INTERVAL_SECS)
            except asyncio.CancelledError:
                if not self.running:
                    break

    # ── Realtime TP loop ──────────────────────────────────────────────────────

    async def _realtime_price_loop(self) -> None:
        """Comprueba TP cada 0.25 s usando precios markPrice WS."""
        while self.running:
            try:
                if self.price_cache and self.positions:
                    with self.lock:
                        pos_syms = list(self.positions.keys())
                    for symbol in pos_syms:
                        price = self.price_cache.get_price(symbol)
                        if price and price > 0:
                            await self._maybe_take_profit(symbol, price)
            except asyncio.CancelledError:
                if not self.running:
                    break
            except Exception as exc:
                self.last_error = str(exc)
            try:
                await asyncio.sleep(0.25)
            except asyncio.CancelledError:
                if not self.running:
                    break

    # ── Snapshot loop ─────────────────────────────────────────────────────────

    async def _snapshot_loop(self) -> None:
        """Reconstruye el snapshot JSON cada segundo para /api/status."""
        while self.running:
            try:
                snap = self._build_snapshot()
                self._sse_snapshot = json.dumps(snap, ensure_ascii=False, default=str)
            except asyncio.CancelledError:
                if not self.running:
                    break
            except Exception as exc:
                self.log(f"Error construyendo snapshot: {exc}")
            try:
                await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                if not self.running:
                    break

    # ── Estrategia ────────────────────────────────────────────────────────────

    async def _ensure_short(self, symbol: str, level: float, notional: float,
                             price: float, change: float) -> None:

        should_log = False
        with self.lock:
            if price > MAX_PRICE_BLOCK:
                if symbol not in self.price_blocked:
                    self.price_blocked.add(symbol)
                    should_log = True
                return

            if self._cooldown_remaining(symbol) > 0:
                return
            pos = self.positions.setdefault(symbol, BotPosition(symbol=symbol))
            if level in pos.opened_levels() or pos.status != "OPEN":
                return

        try:
            if should_log:
               self.log(f"BLOQUEADO permanente {symbol}: precio {price:.4f} > {MAX_PRICE_BLOCK} USD")
               should_log = False
            qty  = await self.client.market_short(symbol, notional, price)
            fill = Fill(level=level, notional=notional, entry_price=price, qty=qty)
            with self.lock:
                self.positions[symbol].fills.append(fill)
            self.log(
                f"SHORT {symbol}: nivel {level:.0f}% | {notional:.2f} USDT | "
                f"qty={qty} | px={price:.6f} | cambio(WS)={change:.2f}%"
            )
            self.persist_state()
        except Exception as exc:
            self.last_error = str(exc)
            self.log(f"Error abriendo short {symbol} nivel {level}: {exc}")

    async def _maybe_take_profit(self, symbol: str, price: float) -> None:
        with self.lock:
            pos = self.positions.get(symbol)
            if not pos or pos.status != "OPEN" or not pos.fills:
                return
            pnl      = pos.unrealized_pnl(price)
            target   = pos.notional * TAKE_PROFIT_FRACTION
            qty      = pos.qty
            avg_ent  = pos.avg_entry
            notional = pos.notional

        if pnl < target:
            return

        try:
            await self.client.close_short(symbol, qty)
        except Exception as exc:
            self.last_error = str(exc)
            self.log(f"Error cerrando short {symbol}: {exc}")
            return

        with self.lock:
            pos = self.positions.pop(symbol, None)
            if pos:
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
                })
                self.closed_trades = self.closed_trades[:100]

        self.log(
            f"CIERRE {symbol}: PnL={pnl:.4f} | objetivo={target:.4f} | "
            f"px={price:.6f} | bloqueado {COOLDOWN_SECONDS // 3600}h hasta {unblock_str}"
        )
        self.persist_state()

    # ── Snapshot ──────────────────────────────────────────────────────────────

    def _build_snapshot(self) -> dict:
        all_prices: Dict[str, float] = {}
        ws_stats:   dict = {}
        kl_stats:   dict = {}
        try:
            if self.price_cache:
                all_prices = self.price_cache.get_all_prices()
                ws_stats   = self.price_cache.get_stats()
        except Exception:
            pass
        try:
            if self.kline_cache:
                kl_stats = self.kline_cache.get_stats()
        except Exception:
            pass

        with self.lock:
            winners_raw        = [dict(w) for w in self.winners]
            positions_raw      = dict(self.positions)
            closed             = list(self.closed_trades[:30])
            events             = list(self.events[:50])
            cooldown_snap      = dict(self.symbol_cooldown)
            price_blocked_snap = set(self.price_blocked)
            last_ws_ticker_at  = self.last_ws_ticker_at
            ws_ticker_updates  = self.ws_ticker_update_count
            filter_cycle_count = self.filter_cycle_count
            last_filter_at     = self.last_filter_cycle_at
            all_symbols_count  = len(self.all_symbols)

        now = time.time()

        winners_out = []
        for w in winners_raw:
            sym       = w["symbol"]
            price     = all_prices.get(sym) or w.get("price", 0.0)
            remaining = max(0.0, cooldown_snap.get(sym, 0.0) - now)
            winners_out.append({
                **w,
                "price":              price,
                "cooldown_remaining": remaining,
                "cooldown_str":       self._fmt_cooldown(remaining) if remaining > 0 else "",
                "price_blocked":      sym in price_blocked_snap,
            })

        open_positions = []
        total_unreal   = 0.0
        total_notional = 0.0
        for symbol, pos in positions_raw.items():
            price = all_prices.get(symbol) or 0.0
            pnl   = pos.unrealized_pnl(price)
            total_unreal   += pnl
            total_notional += pos.notional
            open_positions.append({
                "symbol":         symbol,
                "mark_price":     price,
                "avg_entry":      pos.avg_entry,
                "qty":            pos.qty,
                "notional":       pos.notional,
                "target":         pos.notional * TAKE_PROFIT_FRACTION,
                "unrealized_pnl": pnl,
                "fills":          [f.to_dict() for f in pos.fills],
                "change":         next(
                    (w["change"] for w in winners_raw if w["symbol"] == symbol), 0.0
                ),
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
        last_filter_text = (
            datetime.fromtimestamp(last_filter_at, timezone.utc)
            .strftime("%H:%M:%S UTC")
            if last_filter_at else "pendiente"
        )
        last_ws_ticker_text = (
            datetime.fromtimestamp(last_ws_ticker_at, timezone.utc)
            .strftime("%H:%M:%S UTC")
            if last_ws_ticker_at else "pendiente"
        )
        next_filter_text = (
            self._fmt_cooldown(FILTER_CYCLE_SECS - (now - last_filter_at))
            if last_filter_at and (now - last_filter_at) < FILTER_CYCLE_SECS
            else "pronto"
        )

        return {
            "mode":              "PAPER" if PAPER_MODE or not LIVE_TRADING else "REAL",
            "running":           self.running,
            "thread_alive":      bool(self.thread and self.thread.is_alive()),
            "started_at":        self.started_at,
            "uptime_seconds":    round(now - self.started_at, 1),
            "scan_count":        self.scan_count,
            "last_scan_text":    last_scan_text,
            # Ciclo de filtrado
            "filter_cycle_count":   filter_cycle_count,
            "last_filter_text":     last_filter_text,
            "next_filter_text":     next_filter_text,
            "filter_cycle_secs":    FILTER_CYCLE_SECS,
            "min_gain_filter":      MIN_GAIN_FILTER,
            "full_subscribe_wait":  FULL_SUBSCRIBE_WAIT_SECS,
            # Símbolos
            "all_symbols_count":    all_symbols_count,
            "subscribed_count":     len(self.subscribed_symbols),
            "subscribed_symbols":   self.subscribed_symbols[:25],
            "subscribed_symbols_truncated": len(self.subscribed_symbols) > 25,
            # WS ticker (actualizaciones entre ciclos)
            "last_ws_ticker_text":  last_ws_ticker_text,
            "ws_ticker_updates":    ws_ticker_updates,
            # Resto
            "last_error":        self.last_error,
            "last_startup_err":  self.last_startup_err,
            "exchange_symbols":  self.exchange_symbols,
            "entry_levels":      ENTRY_LEVELS,
            "entry_notionals":   ENTRY_NOTIONALS,
            "take_profit_pct":   TAKE_PROFIT_FRACTION * 100,
            "total_unrealized":  total_unreal,
            "total_notional":    total_notional,
            "positions":         open_positions,
            "winners":           winners_out,
            "closed_trades":     closed,
            "events":            events,
            "cooldown_count":    len(active_cooldowns),
            "cooldowns":         active_cooldowns,
            "cooldown_hours":    COOLDOWN_SECONDS / 3600,
            "price_blocked":     sorted(price_blocked_snap)[:100],
            "price_blocked_truncated": len(price_blocked_snap) > 100,
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
            "ts": now,
        }

    # ── Persistencia ──────────────────────────────────────────────────────────

    def _build_persisted_state(self) -> dict:
        """Estado mínimo para disco: evita reconstruir/enviar el snapshot web completo."""
        now = time.time()
        with self.lock:
            active_cooldowns = {
                sym: {
                    "remaining_s": round(ts - now, 0),
                    "remaining_str": self._fmt_cooldown(ts - now),
                    "unblock_utc": datetime.fromtimestamp(ts, timezone.utc).strftime(
                        "%Y-%m-%d %H:%M UTC"
                    ),
                }
                for sym, ts in self.symbol_cooldown.items() if ts > now
            }
            positions = [
                {
                    "symbol": pos.symbol,
                    "status": pos.status,
                    "avg_entry": pos.avg_entry,
                    "qty": pos.qty,
                    "notional": pos.notional,
                    "target": pos.notional * TAKE_PROFIT_FRACTION,
                    "mark_price": 0.0,
                    "unrealized_pnl": 0.0,
                    "change": 0.0,
                    "fills": [f.to_dict() for f in pos.fills],
                    "realized_pnl": pos.realized_pnl,
                }
                for pos in self.positions.values()
                if pos.fills
            ]
            return {
                "mode": "PAPER" if PAPER_MODE or not LIVE_TRADING else "REAL",
                "running": self.running,
                "thread_alive": bool(self.thread and self.thread.is_alive()),
                "started_at": self.started_at,
                "uptime_seconds": round(now - self.started_at, 1),
                "scan_count": self.scan_count,
                "last_scan_text": (
                    datetime.fromtimestamp(self.last_scan_at, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                    if self.last_scan_at else "pendiente"
                ),
                "filter_cycle_count": self.filter_cycle_count,
                "last_filter_text": (
                    datetime.fromtimestamp(self.last_filter_cycle_at, timezone.utc).strftime("%H:%M:%S UTC")
                    if self.last_filter_cycle_at else "pendiente"
                ),
                "next_filter_text": "pronto",
                "filter_cycle_secs": FILTER_CYCLE_SECS,
                "min_gain_filter": MIN_GAIN_FILTER,
                "full_subscribe_wait": FULL_SUBSCRIBE_WAIT_SECS,
                "all_symbols_count": len(self.all_symbols),
                "subscribed_count": len(self.subscribed_symbols),
                "subscribed_symbols": self.subscribed_symbols[:25],
                "subscribed_symbols_truncated": len(self.subscribed_symbols) > 25,
                "last_ws_ticker_text": "pendiente",
                "ws_ticker_updates": self.ws_ticker_update_count,
                "last_error": self.last_error,
                "last_startup_err": self.last_startup_err,
                "exchange_symbols": self.exchange_symbols,
                "entry_levels": ENTRY_LEVELS,
                "entry_notionals": ENTRY_NOTIONALS,
                "take_profit_pct": TAKE_PROFIT_FRACTION * 100,
                "total_unrealized": 0.0,
                "total_notional": sum(pos["notional"] for pos in positions),
                "positions": positions,
                "winners": [],
                "closed_trades": list(self.closed_trades[:30]),
                "events": list(self.events[:20]),
                "cooldowns": active_cooldowns,
                "cooldown_count": len(active_cooldowns),
                "cooldown_hours": COOLDOWN_SECONDS / 3600,
                "price_blocked": sorted(self.price_blocked)[:100],
                "price_blocked_truncated": len(self.price_blocked) > 100,
                "price_blocked_count": len(self.price_blocked),
                "max_price_block": MAX_PRICE_BLOCK,
                "price_ws": {"active_prices": 0, "active_tickers": 0, "total": 0, "stale": 0},
                "kline_ws": {"pairs_with_data": 0, "total_messages": 0, "active_conns": 0},
                "ts": now,
            }

    def persist_state(self) -> None:
        snap = self._build_persisted_state()
        if not snap["positions"] and not snap["closed_trades"] and snap["scan_count"] <= 0:
            return
        tmp = f"{STATE_FILE}.tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(snap, fh, ensure_ascii=False, default=str)
            os.replace(tmp, STATE_FILE)
            self._last_persist_at = time.time()
        except Exception as exc:
            self.log(f"No pude persistir estado: {exc}")

    def snapshot(self) -> dict:
        try:
            live = json.loads(self._sse_snapshot)
        except Exception:
            live = {}
        if not isinstance(live, dict) or not live:
            live = self._build_snapshot()

        if not live.get("positions") and not live.get("winners") and os.path.exists(STATE_FILE):
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

app = Flask(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# HTML + JS
# ─────────────────────────────────────────────────────────────────────────────

HTML = r"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Bot Short Ganadores · Binance Futures</title>
  <style>
    :root {
      --bg: #0f172a; --card: #111827; --border: #334155;
      --txt: #e2e8f0; --muted: #94a3b8;
      --green: #22c55e; --red: #ef4444; --yellow: #f59e0b;
      --blue: #60a5fa; --purple: #a78bfa; --orange: #fb923c;
      --teal: #2dd4bf;
    }
    * { box-sizing: border-box; }
    body   { margin: 0; font-family: Arial, sans-serif; background: var(--bg); color: var(--txt); }
    header { padding: 20px 24px; background: var(--card); border-bottom: 1px solid var(--border);
             display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }
    header h1 { margin: 0; font-size: 18px; }
    .badge        { padding: 3px 10px; border-radius: 6px; font-size: 12px; font-weight: 700; }
    .badge-green  { background: #14532d; color: #86efac; }
    .badge-yellow { background: #713f12; color: #fde68a; }
    .badge-blue   { background: #1e3a5f; color: #93c5fd; }
    .badge-teal   { background: #134e4a; color: #99f6e4; }
    .badge-orange { background: #431407; color: #fdba74; }
    .badge-purple { background: #3b0764; color: #d8b4fe; }
    main   { padding: 16px; display: grid; gap: 16px; }
    .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 12px; }
    .card  { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 14px; }
    .label { color: var(--muted); font-size: 12px; margin-bottom: 4px; }
    .value { font-size: 22px; font-weight: 700; }
    .value.sm { font-size: 14px; }
    .positive { color: var(--green); } .negative { color: var(--red); } .warn { color: var(--yellow); }
    .teal { color: var(--teal); } .purple { color: var(--purple); }
    section { background: var(--card); border: 1px solid var(--border); border-radius: 12px; overflow: hidden; }
    section h2 { margin: 0; padding: 12px 16px; font-size: 15px; border-bottom: 1px solid var(--border); }
    table  { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td { padding: 9px 12px; border-bottom: 1px solid #1f2937; text-align: right; white-space: nowrap; }
    th:first-child, td:first-child { text-align: left; }
    th { color: var(--muted); font-weight: 600; font-size: 11px; text-transform: uppercase; }
    pre  { background: var(--card); padding: 12px; overflow: auto; max-height: 260px;
           white-space: pre-wrap; font-size: 12px; margin: 0; }
    .pill { display: inline-block; padding: 2px 8px; border-radius: 999px;
            background: #1e293b; border: 1px solid #475569; font-size: 11px; margin: 1px; }
    .ws-row  { display: flex; gap: 8px; flex-wrap: wrap; }
    .ws-chip { background: #1e293b; border: 1px solid var(--border);
               border-radius: 8px; padding: 4px 10px; font-size: 12px; }
    .ws-chip.highlight { border-color: var(--teal); }
    .ws-chip.highlight2 { border-color: var(--purple); }
    #errorBox { border-color: var(--red); }
    .sym-link { color: var(--blue); text-decoration: none; font-weight: 600; }
    .sym-link:hover { text-decoration: underline; }
    .cd-badge { display: inline-block; padding: 2px 8px; border-radius: 6px;
                background: #431407; color: #fdba74; border: 1px solid #c2410c;
                font-size: 11px; font-weight: 700; white-space: nowrap; }
    tr.in-cooldown { background: rgba(251,146,60,0.07); }
    tr.can-trade   { background: rgba(34,197,94,0.05); }
    #dotPoll { width: 8px; height: 8px; border-radius: 50%; background: var(--red);
               display: inline-block; transition: background .3s; }
    #dotPoll.on { background: var(--green); }
    #dotFilter { width: 8px; height: 8px; border-radius: 50%; background: var(--muted);
                 display: inline-block; transition: background .3s; }
    #dotFilter.on { background: var(--purple); }
    #dotWsTicker { width: 8px; height: 8px; border-radius: 50%; background: var(--muted);
                   display: inline-block; transition: background .3s; }
    #dotWsTicker.on { background: var(--teal); }
    .filter-bar { background: #1e293b; border: 1px solid var(--purple);
                  border-radius: 8px; padding: 8px 14px; margin-bottom: 8px;
                  display: flex; gap: 20px; flex-wrap: wrap; font-size: 13px; }
    .filter-bar span { color: var(--muted); }
    .filter-bar b   { color: var(--purple); }
  </style>
</head>
<body>
<header>
  <span id="dotPoll"     title="Verde = polling REST activo"></span>
  <span id="dotFilter"   title="Púrpura = ciclo de filtrado completado"></span>
  <span id="dotWsTicker" title="Teal = ranking WS @ticker activo"></span>
  <h1>Bot Short Ganadores · Binance USDT-M Futures</h1>
  <span id="modeBadge" class="badge badge-yellow">—</span>
  <span class="badge badge-green">markPrice WS en tiempo real</span>
  <span class="badge badge-teal">Ranking 24h WS @ticker</span>
  <span class="badge badge-purple">Ciclo filtrado cada 5 min (sub ALL → ≥15% → unsub resto)</span>
  <span class="badge badge-blue">Lista símbolos REST inicial + caché 12h</span>
  <span class="badge badge-orange">Cooldown 24h tras cierre</span>
</header>
<main>

  <!-- KPIs -->
  <div class="cards">
    <div class="card"><div class="label">Modo</div><div id="mode" class="value warn sm">—</div></div>
    <div class="card"><div class="label">PnL no realizado</div><div id="pnl" class="value">—</div></div>
    <div class="card"><div class="label">Capital en posiciones</div><div id="notional" class="value">—</div></div>
    <div class="card"><div class="label">Último escaneo</div><div id="scan" class="value sm">—</div></div>
    <div class="card"><div class="label">Escaneos totales</div><div id="scanCount" class="value">—</div></div>
    <div class="card">
      <div class="label">Ciclos de filtrado</div>
      <div id="filterCycles" class="value purple">—</div>
    </div>
    <div class="card">
      <div class="label">Último ciclo filtrado</div>
      <div id="lastFilter" class="value sm purple">—</div>
    </div>
    <div class="card">
      <div class="label">Próximo ciclo</div>
      <div id="nextFilter" class="value sm warn">—</div>
    </div>
    <div class="card">
      <div class="label">Símbolos totales (caché)</div>
      <div id="allSymbols" class="value">—</div>
    </div>
    <div class="card"><div class="label">Suscritos actualmente</div><div id="subCount" class="value teal">—</div></div>
    <div class="card">
      <div class="label">Update WS @ticker</div>
      <div id="lastWsTicker" class="value sm teal">—</div>
    </div>
    <div class="card">
      <div class="label">En cooldown (24h)</div>
      <div id="cooldownCount" class="value warn">—</div>
    </div>
  </div>

  <!-- Estado del ciclo de filtrado -->
  <div class="card filter-bar">
    <div>🔍 Filtrado: sub <b id="fbAll">—</b> símbolos →
         espera <b id="fbWait">—</b>s →
         quedan <b id="fbFiltered">—</b> (≥<b id="fbThreshold">—</b>% o posición abierta) →
         unsub <b id="fbUnsub">—</b></div>
    <div>⏱ Próximo: <b id="fbNext" style="color:var(--yellow)">—</b></div>
  </div>

  <!-- WS status -->
  <div class="card">
    <div class="label">Estado WebSockets</div>
    <div class="ws-row" style="margin-top:8px">
      <div class="ws-chip highlight2">Ciclos filtrado: <b id="wsFilterCycles" style="color:var(--purple)">—</b></div>
      <div class="ws-chip highlight">@ticker 24h activos: <b id="wsTickers" style="color:var(--teal)">—</b>/<span id="wsTotal">—</span></div>
      <div class="ws-chip">markPrice activos: <b id="wsActive">—</b>/<span id="wsTotal2">—</span></div>
      <div class="ws-chip">stale: <b id="wsStale">—</b></div>
      <div class="ws-chip">kline pares: <b id="klPairs">—</b></div>
      <div class="ws-chip">kline msgs: <b id="klMsgs">—</b></div>
      <div class="ws-chip">kline conns: <b id="klConns">—</b></div>
      <div class="ws-chip">fetch polls: <b id="pollCount">0</b></div>
    </div>
  </div>

  <!-- Error -->
  <section id="errorBox" style="display:none">
    <h2 style="color:var(--red)">⚠️ Error / Diagnóstico</h2>
    <pre id="lastError" style="color:var(--red)"></pre>
  </section>

  <!-- Cooldowns activos -->
  <section id="cooldownSection" style="display:none">
    <h2>🔒 Símbolos en cooldown — bloqueados 24h tras cierre</h2>
    <table>
      <thead><tr>
        <th>Símbolo</th>
        <th>Tiempo restante</th>
        <th>Se desbloquea (UTC)</th>
      </tr></thead>
      <tbody id="tbCooldown"></tbody>
    </table>
  </section>

  <!-- Posiciones abiertas -->
  <section>
    <h2>Posiciones abiertas</h2>
    <table>
      <thead><tr>
        <th>Símbolo</th><th>Cambio 24h (WS)</th><th>Entrada media</th>
        <th>Precio WS</th><th>Notional</th><th>Objetivo</th>
        <th>PnL tiempo real</th><th>Tramos</th>
      </tr></thead>
      <tbody id="tbPositions">
        <tr><td colspan="8" style="color:var(--muted)">Sin posiciones</td></tr>
      </tbody>
    </table>
  </section>

  <!-- Ganadores -->
  <section>
    <h2>
      <span id="winnerCount">0</span> símbolos activos (≥<span id="gainThreshold">15</span>% · 24h WS)
      <span style="color:var(--muted);font-weight:400;font-size:13px">
        · Filtrado cada 5 min: sub ALL → mantener ≥15% o posición abierta
        &nbsp;|&nbsp; 🟠 = cooldown
      </span>
    </h2>
    <table>
      <thead><tr>
        <th>Símbolo</th>
        <th>Cambio 24h (WS)</th>
        <th>Precio (markPrice)</th>
        <th>Cond. kline</th>
        <th>Short</th>
        <th>Estado</th>
      </tr></thead>
      <tbody id="tbWinners">
        <tr><td colspan="6" style="color:var(--muted)">Esperando primer ciclo de filtrado…</td></tr>
      </tbody>
    </table>
  </section>

  <!-- Cierres -->
  <section>
    <h2>Operaciones cerradas</h2>
    <table>
      <thead><tr>
        <th>Símbolo</th><th>PnL realizado</th><th>Objetivo</th>
        <th>Entrada media</th><th>Precio cierre</th>
        <th>Bloqueado hasta</th><th>Fecha cierre</th>
      </tr></thead>
      <tbody id="tbClosed">
        <tr><td colspan="7" style="color:var(--muted)">Sin cierres aún</td></tr>
      </tbody>
    </table>
  </section>

  <!-- Eventos -->
  <section>
    <h2>Eventos del bot</h2>
    <pre id="events" style="background:transparent"></pre>
  </section>

</main>

<script>
// ── Utilidades ──────────────────────────────────────────────────────────────
const q     = id => document.getElementById(id);
const n     = v  => { const p = Number(v); return isFinite(p) ? p : 0; };
const fx    = (v, d=8) => n(v).toFixed(d);
const money = v  => fx(v,4) + ' USDT';
const pct   = v  => fx(v,2) + '%';
const cls   = v  => n(v) >= 0 ? 'positive' : 'negative';

function tb(rows, fallback, cols) {
  return rows.length
    ? rows.join('')
    : `<tr><td colspan="${cols}" style="color:var(--muted)">${fallback}</td></tr>`;
}

function fmtCd(secs) {
  const s = Math.max(0, Math.floor(secs));
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const r = s % 60;
  if (h > 0) return `${h}h ${String(m).padStart(2,'0')}m`;
  if (m > 0) return `${m}m ${String(r).padStart(2,'0')}s`;
  return `${r}s`;
}

let pollCount     = 0;
let _cdData       = {};
let _lastFetch    = 0;
let _prevFilter   = 0;
let _prevWsTicker = 0;
let _filterAt     = 0;
let _filterCycle  = 300;

// Decrementa cooldowns y cuenta regresiva del ciclo de filtrado
function tickTimers() {
  const elapsed = (Date.now() - _lastFetch) / 1000;

  // Cooldowns
  Object.entries(_cdData).forEach(([sym, info]) => {
    const rem = Math.max(0, info.remaining_s - elapsed);
    const elBadge = document.getElementById('cd_' + sym);
    if (elBadge) {
      if (rem > 0) {
        elBadge.textContent  = '🔒 ' + fmtCd(rem);
        elBadge.className    = 'cd-badge';
      } else {
        elBadge.textContent  = '✓ libre';
        elBadge.className    = '';
        elBadge.style.color  = 'var(--green)';
      }
    }
    const trCd = document.getElementById('cdrow_' + sym);
    if (trCd) {
      const tdRem = trCd.querySelector('.cdrem');
      if (tdRem) tdRem.textContent = rem > 0 ? fmtCd(rem) : 'Expirado';
    }
  });

  // Cuenta regresiva del próximo ciclo de filtrado
  if (_filterAt > 0) {
    const nextIn = Math.max(0, _filterCycle - elapsed);
    q('nextFilter').textContent = nextIn > 0 ? `en ${fmtCd(nextIn)}` : 'pronto…';
    q('fbNext').textContent     = nextIn > 0 ? fmtCd(nextIn) : 'pronto…';
  }
}
setInterval(tickTimers, 1000);

// ── Render ──────────────────────────────────────────────────────────────────
function render(d) {
  if (!d) return;
  _lastFetch    = Date.now();
  _filterAt     = n(d.last_filter_cycle_at || 0);  // no viene en snapshot directamente, usamos texto
  _filterCycle  = n(d.filter_cycle_secs || 300);

  const mode = d.mode || '—';
  q('mode').textContent      = mode;
  q('modeBadge').textContent = mode;
  q('modeBadge').className   = 'badge ' + (mode === 'REAL' ? 'badge-green' : 'badge-yellow');

  const pu = n(d.total_unrealized);
  q('pnl').textContent            = money(pu);
  q('pnl').className              = 'value ' + cls(pu);
  q('notional').textContent       = money(d.total_notional);
  q('scan').textContent           = d.last_scan_text         || 'pendiente';
  q('scanCount').textContent      = n(d.scan_count);
  q('filterCycles').textContent   = n(d.filter_cycle_count);
  q('lastFilter').textContent     = d.last_filter_text       || 'pendiente';
  q('nextFilter').textContent     = d.next_filter_text       || '—';
  q('allSymbols').textContent     = n(d.all_symbols_count);
  q('subCount').textContent       = n(d.subscribed_count);
  q('lastWsTicker').textContent   = d.last_ws_ticker_text    || 'pendiente';
  q('cooldownCount').textContent  = n(d.cooldown_count);

  // Filter bar info
  const subCount  = n(d.subscribed_count);
  const allCount  = n(d.all_symbols_count);
  const threshold = n(d.min_gain_filter || 15);
  q('fbAll').textContent       = allCount;
  q('fbWait').textContent      = n(d.full_subscribe_wait || 30);
  q('fbFiltered').textContent  = subCount;
  q('fbThreshold').textContent = threshold;
  q('fbUnsub').textContent     = Math.max(0, allCount - subCount);
  q('gainThreshold').textContent = threshold;
  q('fbNext').textContent      = d.next_filter_text || '—';

  // Parpadeo del ciclo de filtrado
  const fc = n(d.filter_cycle_count);
  if (fc !== _prevFilter) {
    q('dotFilter').className = 'on';
    setTimeout(() => { q('dotFilter').className = ''; }, 1500);
    _prevFilter = fc;
  }

  // Parpadeo del WS @ticker
  const wtu = n(d.ws_ticker_updates);
  if (wtu !== _prevWsTicker) {
    q('dotWsTicker').className = 'on';
    setTimeout(() => { q('dotWsTicker').className = ''; }, 800);
    _prevWsTicker = wtu;
  }

  const pw = d.price_ws || {}, kw = d.kline_ws || {};
  q('wsFilterCycles').textContent = n(d.filter_cycle_count);
  q('wsTickers').textContent  = n(pw.active_tickers);
  q('wsTotal').textContent    = n(pw.total);
  q('wsActive').textContent   = n(pw.active_prices);
  q('wsTotal2').textContent   = n(pw.total);
  q('wsStale').textContent    = n(pw.stale);
  q('klPairs').textContent    = n(kw.pairs_with_data);
  q('klMsgs').textContent     = n(kw.total_messages);
  q('klConns').textContent    = n(kw.active_conns);
  q('pollCount').textContent  = pollCount;

  const err = d.last_error || d.last_startup_err || '';
  q('errorBox').style.display = err ? 'block' : 'none';
  q('lastError').textContent  = err;

  _cdData = {};
  Object.entries(d.cooldowns || {}).forEach(([sym, info]) => {
    _cdData[sym] = { remaining_s: n(info.remaining_s), unblock_utc: info.unblock_utc || '' };
  });

  // ── Panel cooldowns ───────────────────────────────────────────────────────
  const cdEntries = Object.entries(d.cooldowns || {});
  q('cooldownSection').style.display = cdEntries.length ? 'block' : 'none';
  q('tbCooldown').innerHTML = cdEntries.length
    ? cdEntries
        .sort((a, b) => n(b[1].remaining_s) - n(a[1].remaining_s))
        .map(([sym, info]) => `
          <tr id="cdrow_${sym}">
            <td style="font-weight:700;color:var(--orange)">${sym}</td>
            <td class="cdrem" style="color:var(--orange)">${fmtCd(n(info.remaining_s))}</td>
            <td style="color:var(--muted)">${info.unblock_utc || ''}</td>
          </tr>`)
        .join('')
    : '<tr><td colspan="3" style="color:var(--muted)">Ninguno activo</td></tr>';

  // ── Posiciones ────────────────────────────────────────────────────────────
  const positions = Array.isArray(d.positions) ? d.positions : [];
  q('tbPositions').innerHTML = tb(positions.map(p => {
    const pnl   = n(p.unrealized_pnl);
    const fills = (Array.isArray(p.fills) ? p.fills : [])
      .map(f => `<span class="pill">+${fx(f.level,0)}% / ${fx(f.notional,2)}</span>`)
      .join(' ');
    return `<tr>
      <td><a class="sym-link" href="https://www.binance.com/en/futures/${p.symbol}"
             target="_blank">${p.symbol}</a></td>
      <td class="${cls(p.change)}">${pct(p.change)}</td>
      <td>${fx(p.avg_entry)}</td>
      <td>${fx(p.mark_price)}</td>
      <td>${money(p.notional)}</td>
      <td>${money(p.target)}</td>
      <td class="${cls(pnl)}">${money(pnl)}</td>
      <td>${fills}</td>
    </tr>`;
  }), 'Sin posiciones abiertas', 8);

  // ── Ganadores (símbolos activos ≥15%) ────────────────────────────────────
  const winners     = Array.isArray(d.winners) ? d.winners : [];
  const entryLevels = Array.isArray(d.entry_levels) ? d.entry_levels : [50];
  q('winnerCount').textContent = winners.length;

  q('tbWinners').innerHTML = tb(winners.map(w => {
    const change     = n(w.change);
    const cdSecs     = n(w.cooldown_remaining);
    const inCooldown = cdSecs > 0;
    const canTrade   = change >= entryLevels[0] && !inCooldown;
    const rowCls     = inCooldown ? 'in-cooldown' : (canTrade ? 'can-trade' : '');

    if (inCooldown && !_cdData[w.symbol]) {
      _cdData[w.symbol] = { remaining_s: cdSecs, unblock_utc: w.cooldown_str || '' };
    }

    let statusHtml;
    if (inCooldown) {
      statusHtml = `<span id="cd_${w.symbol}" class="cd-badge">🔒 ${fmtCd(cdSecs)}</span>`;
    } else if (w.price_blocked) {
      statusHtml = `<span id="cd_${w.symbol}" style="color:var(--red)">⛔ precio alto</span>`;
    } else if (canTrade) {
      statusHtml = `<span id="cd_${w.symbol}" style="color:var(--green)">✓ libre</span>`;
    } else {
      statusHtml = `<span id="cd_${w.symbol}" style="color:var(--muted)">—</span>`;
    }

    return `<tr class="${rowCls}">
      <td><a class="sym-link" href="https://www.binance.com/en/futures/${w.symbol}"
             target="_blank">${w.symbol}</a></td>
      <td class="${cls(change)}" style="font-weight:600">${pct(change)}</td>
      <td>${fx(n(w.price))}</td>
      <td>${canTrade
            ? '<span style="color:var(--green)">✓ alcista</span>'
            : '<span style="color:var(--muted)">—</span>'}</td>
      <td>${w.can_short
            ? '<span style="color:var(--green)">sí</span>'
            : '<span style="color:var(--red)">no</span>'}</td>
      <td>${statusHtml}</td>
    </tr>`;
  }), 'Esperando primer ciclo de filtrado…', 6);

  // ── Cierres ───────────────────────────────────────────────────────────────
  const closed = Array.isArray(d.closed_trades) ? d.closed_trades : [];
  q('tbClosed').innerHTML = tb(closed.map(t => `<tr>
    <td style="font-weight:700">${t.symbol || ''}</td>
    <td class="positive">${money(t.pnl)}</td>
    <td>${money(t.target)}</td>
    <td>${fx(t.avg_entry)}</td>
    <td>${fx(t.close_price)}</td>
    <td style="color:var(--orange)">${t.unblock_at || '—'}</td>
    <td style="color:var(--muted)">${t.closed_at || ''}</td>
  </tr>`), 'Sin cierres aún', 7);

  q('events').textContent = (Array.isArray(d.events) ? d.events : []).join('\n');
}

// ── Polling fetch() cada 2 s ─────────────────────────────────────────────────
let pollTimer   = null;
let pollDelay   = 2000;
let pollFailing = false;

async function poll() {
  try {
    const resp = await fetch('/api/status', { cache: 'no-store' });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    pollCount++;
    q('dotPoll').className = 'on';
    pollDelay   = 2000;
    pollFailing = false;
    render(data);
  } catch (err) {
    q('dotPoll').className = '';
    if (!pollFailing) { console.warn('Poll error:', err.message); pollFailing = true; }
    pollDelay = Math.min(pollDelay * 1.5, 15000);
  } finally {
    pollTimer = setTimeout(poll, pollDelay);
  }
}

poll();
window.addEventListener('beforeunload', () => { if (pollTimer) clearTimeout(pollTimer); });
</script>
</body>
</html>
"""


# ─────────────────────────────────────────────────────────────────────────────
# RUTAS FLASK
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/")
def index():
    resp = make_response(render_template_string(HTML))
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


@app.get("/api/status")
def api_status():
    resp = jsonify(bot.snapshot())
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


@app.get("/health")
def health():
    snap = bot.snapshot()
    return jsonify({
        "ok":                  True,
        "running":             bot.running,
        "mode":                snap["mode"],
        "scan_count":          snap["scan_count"],
        "filter_cycle_count":  snap["filter_cycle_count"],
        "all_symbols_count":   snap["all_symbols_count"],
        "subscribed_count":    snap["subscribed_count"],
        "last_error":          snap["last_error"],
        "cooldown_count":      snap["cooldown_count"],
    })


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, threaded=True)
