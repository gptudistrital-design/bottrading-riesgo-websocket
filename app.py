"""
Bot web para shortear ganadores de Binance Futures.

ARQUITECTURA:
════════════════════════════════════════════════════════════════════════════
 1. REST al inicio → /fapi/v1/ticker/24hr
      Siembra los primeros TOP_WINNERS ganadores y suscribe los streams WS.

 2. SymbolWebSocketPriceCache (WS.py) — streams permanentes suscritos:
        @markPrice@1s  → precio mark en tiempo real (para TP y tabla)
        @ticker        → cambio 24h, high/low, volumen (actualización continua)

 3. _ws_ticker_update_loop  (cada WS_TICKER_UPDATE_SECS = 5 s)
      Lee ticker_cache de WS.py y actualiza el campo 'change' de cada
      winner con el % 24h en tiempo real. Re-rankea la lista por cambio.
      ► Sustituye completamente el ciclo REST de 10 min para mantener
        el ranking fresco — el WS siempre tiene el dato más reciente.

 4. REST cada WINNERS_REFRESH_SECS = 20 min → /fapi/v1/ticker/24hr
      Solo para DESCUBRIR símbolos nuevos no suscritos que emergen en
      el top. Si no hay nuevos, el REST no modifica la lista de winners —
      el WS la mantiene viva con datos frescos.

 5. KlineWebSocketCache (KlineWebSocketCache_v4.py) → klines 1m
      Confirmación técnica de entrada (vela alcista).

 6. Scanner (cada SCAN_INTERVAL_SECS)
      Lee precio WS + klines + change WS, aplica niveles 50/75/100/150/200/250 %.

 7. Realtime TP loop (cada 0.25 s)
      Cierra posiciones cuando PnL >= objetivo usando precios WS.

 8. fetch() polling → /api/status cada 2 s
      Actualiza el DOM sin recargar. Compatible con cualquier worker.

 9. Cooldown 24 h tras cierre ganador.

FLUJO DE DATOS:
  Startup ──► REST seed top N ──► suscribir WS (markPrice + @ticker)
                  │
                  ▼
     _ws_ticker_update_loop (cada 5 s)
       ├─ lee ticker_cache[@ticker] de WS.py
       ├─ actualiza winners[*]["change"] con cambio 24h real
       └─ re-ordena winners de mayor a menor

     REST cada 20 min (solo si hay símbolos nuevos)
       ├─ detecta candidatos no suscritos en el top REST
       └─ amplía suscripciones WS si los hay

ROBUSTEZ:
  - asyncio.gather con return_exceptions=True
  - Supervisor (_supervised) relanza cualquier coro que muera inesperadamente
  - _stop_*_cache con pausa de gracia para que WS limpie sus tasks
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

BASE_URL      = os.getenv("BASE_URL",   "https://fapi.binance.com")
QUOTE_ASSET   = os.getenv("QUOTE_ASSET", "USDT")
PAPER_MODE    = os.getenv("PAPER_MODE",   "true").lower() == "true"
LIVE_TRADING  = os.getenv("LIVE_TRADING", "false").lower() == "true"
API_KEY       = os.getenv("BINANCE_API_KEY",    "")
API_SECRET    = os.getenv("BINANCE_API_SECRET", "")
LEVERAGE      = int(os.getenv("LEVERAGE", "1"))
STATE_FILE    = os.getenv("STATE_FILE", os.path.join(tempfile.gettempdir(), "botshort_state.json"))

TOP_WINNERS          = int(os.getenv("TOP_WINNERS",          "20"))
WINNERS_REFRESH_SECS = int(os.getenv("WINNERS_REFRESH_SECS", "1200"))  # 20 min — solo descubrimiento
WS_TICKER_UPDATE_SECS = float(os.getenv("WS_TICKER_UPDATE_SECS", "5.0"))  # actualización WS del ranking
SCAN_INTERVAL_SECS   = int(os.getenv("SCAN_INTERVAL_SECS",   "10"))
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

        # Bloqueo permanente por precio: símbolo superó MAX_PRICE_BLOCK
        self.price_blocked: set = set()

        # WS caches
        self.price_cache:        Optional[SymbolWebSocketPriceCache] = None
        self.kline_cache:        Optional[KlineWebSocketCache]       = None
        self.subscribed_symbols: List[str] = []

        # Métricas
        self.running              = False
        self.scan_count           = 0
        self.last_scan_at         = 0.0
        self.last_winners_at      = 0.0   # timestamp del último REST de descubrimiento
        self.last_ws_ticker_at    = 0.0   # timestamp de la última actualización WS del ranking
        self.ws_ticker_update_count = 0   # número de actualizaciones WS del ranking
        self.last_error           = ""
        self.last_startup_err     = ""
        self.exchange_symbols     = 0
        self.started_at           = time.time()
        self._sse_snapshot: str   = "{}"

        self.loop:   Optional[asyncio.AbstractEventLoop] = None
        self.thread: Optional[threading.Thread] = None

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
        try:
            self.loop.run_until_complete(self._main())
        except Exception as exc:
            self.running    = False
            self.last_error = str(exc)
            self.log(f"Bot detenido por error no controlado: {exc}")

    # ── Supervisor ────────────────────────────────────────────────────────────

    async def _supervised(self, coro_factory, name: str, restart_delay: float = 2.0):
        """
        Envuelve una corrutina con reinicio automático.
        Si la corrutina termina por cualquier motivo mientras self.running=True,
        la relanza tras restart_delay segundos.
        """
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

        # Primer fetch REST: siembra winners y suscribe WS
        await self._fetch_top_winners()

        await asyncio.gather(
            self._supervised(self._winners_refresh_loop,  "_winners_refresh_loop"),
            self._supervised(self._ws_ticker_update_loop, "_ws_ticker_update_loop"),
            self._supervised(self._scanner,               "_scanner"),
            self._supervised(self._realtime_price_loop,   "_realtime_price_loop"),
            self._supervised(self._snapshot_loop,         "_snapshot_loop"),
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

    def _merged_ws_symbols(self, winners: List[str]) -> List[str]:
        """Devuelve winners + posiciones abiertas (sin duplicados, sin alterar orden)."""
        forced = self._open_position_symbols()
        return list(dict.fromkeys([*winners, *forced]))

    def _start_price_cache(self, symbols: List[str]) -> None:
        self._stop_price_cache()
        if not symbols:
            return
        self.price_cache = SymbolWebSocketPriceCache(
            symbols,
            symbols_per_connection=30,
        )
        self.price_cache.start()
        self.log(f"PriceCache iniciado con {len(symbols)} símbolos (markPrice + @ticker)")

    def _start_kline_cache(self, symbols: List[str]) -> None:
        self._stop_kline_cache()
        if not symbols:
            return
        pairs = {sym: ["1m"] for sym in symbols}
        self.kline_cache = KlineWebSocketCache(
            pairs                          = pairs,
            max_candles                    = 1,
            include_open_candle            = True,
            backfill_on_start              = True,
            streams_per_connection         = 30,
            rest_concurrency               = 5,
            rest_retries                   = 3,
            backfill_batch_size            = 3,
            backfill_batch_delay           = 0.25,
            safety_refresh_interval_seconds = 1200,
        )
        self.kline_cache.start()
        self.log(f"KlineCache iniciado con {len(symbols)} símbolos (1m)")

    async def _update_subscriptions(self, new_symbols: List[str]) -> None:
        """
        Compara la lista nueva con la suscrita y re-suscribe si hay cambios.
        Los stop/start de caches se ejecutan en un thread para no bloquear el event loop.
        """
        merged_symbols = self._merged_ws_symbols(new_symbols)
        old_set = set(self.subscribed_symbols)
        new_set = set(merged_symbols)

        if old_set == new_set:
            return

        added   = new_set - old_set
        removed = old_set - new_set
        self.log(
            f"Suscripciones WS: +{len(added)} nuevos, -{len(removed)} eliminados"
        )

        await asyncio.to_thread(self._start_price_cache, merged_symbols)
        await asyncio.to_thread(self._start_kline_cache, merged_symbols)
        self.subscribed_symbols = list(merged_symbols)

    # ── REST: inicialización y descubrimiento de nuevos símbolos ─────────────

    async def _fetch_top_winners(self) -> None:
        """
        Llama a /fapi/v1/ticker/24hr con dos propósitos según el momento:

        ① Primera vez (startup, subscribed_symbols vacío):
            → Siembra la lista de ganadores completa desde REST.
            → Suscribe los streams WS (markPrice + @ticker) para esos N símbolos.
            → A partir de aquí, _ws_ticker_update_loop actualiza 'change' en
              tiempo real: el REST solo vuelve a correr cada 20 min.

        ② Llamadas sucesivas (cada WINNERS_REFRESH_SECS = 20 min):
            → Solo detecta símbolos de alto rendimiento NO suscritos aún.
            → Si los hay, los añade a winners (con datos REST como semilla) y
              amplía las suscripciones WS para que el stream @ticker los cubra.
            → Si no hay nuevos, el REST no toca nada — WS lleva el control.
        """
        is_initial = not self.subscribed_symbols

        try:
            if is_initial:
                self.log("REST: inicializando lista de ganadores y suscripciones WS...")
            else:
                self.log(
                    f"REST: descubrimiento de nuevos símbolos "
                    f"(intervalo {WINNERS_REFRESH_SECS // 60} min)..."
                )

            data = await self.client.request("GET", "/fapi/v1/ticker/24hr")
            if not isinstance(data, list):
                self.log("REST: respuesta inesperada (no es lista)")
                return

            filters = self.client.exchange_filters

            # ── Construir mapa completo symbol → datos REST ───────────────
            rest_map: Dict[str, dict] = {}
            for item in data:
                symbol = item.get("symbol", "")
                if not symbol.endswith(QUOTE_ASSET):
                    continue
                if filters and symbol not in filters:
                    continue
                try:
                    change = float(item.get("priceChangePercent", 0.0))
                    price  = float(item.get("lastPrice", 0.0))
                except (TypeError, ValueError):
                    continue
                if price <= 0 or change < MIN_GAIN_TO_SHOW:
                    continue
                rest_map[symbol] = {
                    "symbol":    symbol,
                    "change":    change,
                    "price":     price,
                    "market":    "futures",
                    "can_short": True,
                }

            all_candidates = sorted(
                rest_map.values(), key=lambda x: x["change"], reverse=True
            )

            # ── ① Startup: seed completo ──────────────────────────────────
            if is_initial:
                top = all_candidates[:TOP_WINNERS]
                new_symbols = [c["symbol"] for c in top]

                with self.lock:
                    self.winners         = [dict(c) for c in top]
                    self.last_winners_at = time.time()

                await self._update_subscriptions(new_symbols)

                top_str = (
                    f"{top[0]['symbol']} {top[0]['change']:.1f}%"
                    if top else "ninguno"
                )
                self.log(
                    f"REST init: {len(top)} ganadores sembrados | top={top_str} | "
                    f"WS @ticker actualizará el ranking cada {WS_TICKER_UPDATE_SECS:.0f}s"
                )
                return

            # ── ② Sucesivo: solo descubrimiento ──────────────────────────
            with self.lock:
                subscribed_set     = set(self.subscribed_symbols)
                existing_syms      = {w["symbol"] for w in self.winners}

            # Símbolos en el top REST que no están suscritos todavía
            top_rest_pool = all_candidates[: TOP_WINNERS * 2]  # buffer amplio
            new_to_add    = [
                c for c in top_rest_pool
                if c["symbol"] not in subscribed_set
                and c["symbol"] not in existing_syms
            ]

            if not new_to_add:
                self.log(
                    f"REST discovery: sin nuevos símbolos | "
                    f"{len(subscribed_set)} ya suscritos vía WS — "
                    f"ranking actualizado en tiempo real por @ticker"
                )
                with self.lock:
                    self.last_winners_at = time.time()
                return

            self.log(
                f"REST discovery: {len(new_to_add)} nuevos símbolos detectados: "
                f"{[c['symbol'] for c in new_to_add[:5]]}…"
            )

            # Añadir nuevos a winners (WS @ticker los irá actualizando pronto)
            with self.lock:
                for c in new_to_add:
                    self.winners.append(dict(c))

                # Ordenar; respetar posiciones abiertas fuera del top N
                self.winners.sort(key=lambda x: x["change"], reverse=True)
                pos_syms = {
                    sym for sym, pos in self.positions.items()
                    if pos.status == "OPEN"
                }
                top_n_syms    = {w["symbol"] for w in self.winners[:TOP_WINNERS]}
                extra_pos      = [
                    w for w in self.winners[TOP_WINNERS:]
                    if w["symbol"] in pos_syms
                ]
                self.winners         = self.winners[:TOP_WINNERS] + extra_pos
                self.last_winners_at = time.time()

            # Ampliar suscripciones WS para los nuevos símbolos
            with self.lock:
                all_winner_syms = [w["symbol"] for w in self.winners]
            merged = self._merged_ws_symbols(all_winner_syms)
            await self._update_subscriptions(merged)

            with self.lock:
                n_winners = len(self.winners)
                top_sym   = self.winners[0]["symbol"] if self.winners else "—"
                top_chg   = self.winners[0]["change"]  if self.winners else 0.0

            self.log(
                f"REST discovery: añadidos {len(new_to_add)} símbolos | "
                f"winners totales: {n_winners} | top={top_sym} {top_chg:.1f}%"
            )

        except RuntimeError as exc:
            msg = str(exc)
            if "418" in msg:
                self.log(
                    f"REST 418 (IP rate-limit Binance) — "
                    f"esperando {WINNERS_REFRESH_SECS * 2}s"
                )
                self.last_error = (
                    f"HTTP 418 – rate-limit Binance REST. "
                    f"Retry en {WINNERS_REFRESH_SECS * 2}s"
                )
            else:
                self.last_error = msg
                self.log(f"REST _fetch_top_winners falló: {msg}")
        except Exception as exc:
            self.last_error = str(exc)
            self.log(f"REST _fetch_top_winners error: {exc}")

    async def _winners_refresh_loop(self) -> None:
        """
        Llama a _fetch_top_winners cada WINNERS_REFRESH_SECS (20 min).
        En llamadas sucesivas al inicio, el REST solo descubre nuevos símbolos.
        El ranking en tiempo real corre en _ws_ticker_update_loop vía WS @ticker.
        """
        while self.running:
            await asyncio.sleep(WINNERS_REFRESH_SECS)
            if not self.running:
                break
            await self._fetch_top_winners()

    # ── WS Ticker: actualización en tiempo real del ranking ───────────────────

    async def _ws_ticker_update_loop(self) -> None:
        """
        Mantiene el ranking de ganadores en tiempo real usando el stream
        @ticker de SymbolWebSocketPriceCache (WS.py → ticker_cache).

        Cada WS_TICKER_UPDATE_SECS segundos:
          1. Lee get_all_tickers() del price_cache (thread-safe).
          2. Actualiza winners[*]["change"] con el % 24h fresco del WS.
          3. Re-ordena winners de mayor a menor cambio.

        Reemplaza el ciclo REST de 10 min para mantener el ranking activo.
        Aprovecha que los streams @ticker ya están suscritos indefinidamente
        y actualizan su caché continuamente, sin coste adicional de red.
        """
        self.log(
            f"[ws_ticker] Loop de ranking en tiempo real iniciado "
            f"(intervalo={WS_TICKER_UPDATE_SECS:.0f}s)"
        )

        # Esperar hasta que WS tenga sus primeros datos de ticker
        for _ in range(60):
            if not self.running:
                return
            try:
                if self.price_cache:
                    if self.price_cache.get_all_tickers():
                        break
            except Exception:
                pass
            await asyncio.sleep(1.0)

        while self.running:
            try:
                if self.price_cache:
                    # get_all_tickers() usa su propio lock internamente → thread-safe
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
                                    # Crear nuevo dict (nunca mutar in-place) para
                                    # evitar condiciones de carrera con lecturas externas
                                    new_w = dict(w)
                                    new_w["change"] = ticker["change_pct"]
                                    new_winners.append(new_w)
                                    ws_updated += 1
                                else:
                                    # Sin datos WS aún para este símbolo → conservar
                                    new_winners.append(dict(w))

                            if ws_updated:
                                new_winners.sort(
                                    key=lambda x: x["change"], reverse=True
                                )
                                self.winners            = new_winners
                                self.last_ws_ticker_at  = time.time()
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
            df = self.kline_cache.get_dataframe(symbol, "1m", only_closed=True)
            if df.empty or len(df) < 2:
                return True
            last = df.iloc[-1]
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
        for _ in range(60):
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
                # Snapshot de winners (dicts independientes para evitar race)
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
                    change = row["change"]        # actualizado por WS @ticker en tiempo real
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

                # TP de posiciones fuera de winners
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

                n_high = sum(1 for w in winners if w["change"] >= ENTRY_LEVELS[0])
                self.log(
                    f"Escan #{self.scan_count}: {len(winners)} winners (WS change) | "
                    f">={ENTRY_LEVELS[0]:.0f}%: {n_high} | "
                    f"posiciones: {len(self.positions)} | cooldown: {n_cool}"
                )
                self.persist_state()

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
                    all_prices = self.price_cache.get_all_prices()
                    with self.lock:
                        pos_syms = list(self.positions.keys())
                    for symbol in pos_syms:
                        price = all_prices.get(symbol)
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
        with self.lock:
            if price > MAX_PRICE_BLOCK:
                if symbol not in self.price_blocked:
                    self.price_blocked.add(symbol)
                    self.log(
                        f"BLOQUEADO permanente {symbol}: precio {price:.4f} > "
                        f"{MAX_PRICE_BLOCK} USD"
                    )
                return

            if self._cooldown_remaining(symbol) > 0:
                return
            pos = self.positions.setdefault(symbol, BotPosition(symbol=symbol))
            if level in pos.opened_levels() or pos.status != "OPEN":
                return

        try:
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
                "fills":          [f.__dict__ for f in pos.fills],
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
        last_winners_text = (
            datetime.fromtimestamp(self.last_winners_at, timezone.utc)
            .strftime("%H:%M:%S UTC")
            if self.last_winners_at else "pendiente"
        )
        last_ws_ticker_text = (
            datetime.fromtimestamp(last_ws_ticker_at, timezone.utc)
            .strftime("%H:%M:%S UTC")
            if last_ws_ticker_at else "pendiente"
        )

        return {
            "mode":              "PAPER" if PAPER_MODE or not LIVE_TRADING else "REAL",
            "running":           self.running,
            "thread_alive":      bool(self.thread and self.thread.is_alive()),
            "started_at":        self.started_at,
            "uptime_seconds":    round(now - self.started_at, 1),
            "scan_count":        self.scan_count,
            "last_scan_text":    last_scan_text,
            "last_winners_text": last_winners_text,       # último REST discovery
            "last_ws_ticker_text": last_ws_ticker_text,  # última actualización WS del ranking
            "ws_ticker_updates": ws_ticker_updates,       # nº total de updates WS
            "last_error":        self.last_error,
            "last_startup_err":  self.last_startup_err,
            "exchange_symbols":  self.exchange_symbols,
            "subscribed_count":  len(self.subscribed_symbols),
            "subscribed_symbols": self.subscribed_symbols,
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
            "price_blocked":     sorted(price_blocked_snap),
            "price_blocked_count": len(price_blocked_snap),
            "max_price_block":   MAX_PRICE_BLOCK,
            "price_ws": {
                "active_prices":  ws_stats.get("active_prices",  0),   # markPrice activos
                "active_tickers": ws_stats.get("active_tickers", 0),   # @ticker activos
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

    def persist_state(self) -> None:
        snap = self._build_snapshot()
        if not snap["positions"] and not snap["closed_trades"] and snap["scan_count"] <= 0:
            return
        tmp = f"{STATE_FILE}.tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(snap, fh, ensure_ascii=False, default=str)
            os.replace(tmp, STATE_FILE)
        except Exception as exc:
            self.log(f"No pude persistir estado: {exc}")

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
    main   { padding: 16px; display: grid; gap: 16px; }
    .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 12px; }
    .card  { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 14px; }
    .label { color: var(--muted); font-size: 12px; margin-bottom: 4px; }
    .value { font-size: 22px; font-weight: 700; }
    .value.sm { font-size: 14px; }
    .positive { color: var(--green); } .negative { color: var(--red); } .warn { color: var(--yellow); }
    .teal { color: var(--teal); }
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
    #dotWsTicker { width: 8px; height: 8px; border-radius: 50%; background: var(--muted);
                   display: inline-block; transition: background .3s; }
    #dotWsTicker.on { background: var(--teal); }
  </style>
</head>
<body>
<header>
  <span id="dotPoll" title="Verde = polling REST activo"></span>
  <span id="dotWsTicker" title="Teal = ranking WS @ticker activo"></span>
  <h1>Bot Short Ganadores · Binance USDT-M Futures</h1>
  <span id="modeBadge" class="badge badge-yellow">—</span>
  <span class="badge badge-green">markPrice WS en tiempo real</span>
  <span class="badge badge-teal">Ranking 24h por WS @ticker</span>
  <span class="badge badge-blue">REST cada 20 min (solo descubrimiento)</span>
  <span class="badge badge-orange">Cooldown 24 h tras cierre</span>
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
      <div class="label">Último update WS @ticker</div>
      <div id="lastWsTicker" class="value sm teal">—</div>
    </div>
    <div class="card">
      <div class="label">Updates WS @ticker</div>
      <div id="wsTickerUpdates" class="value teal">—</div>
    </div>
    <div class="card">
      <div class="label">Último REST descubrimiento</div>
      <div id="lastWinners" class="value sm">—</div>
    </div>
    <div class="card"><div class="label">Contratos cargados</div><div id="contracts" class="value">—</div></div>
    <div class="card"><div class="label">Símbolos suscritos WS</div><div id="subCount" class="value">—</div></div>
    <div class="card">
      <div class="label">En cooldown (24 h)</div>
      <div id="cooldownCount" class="value warn">—</div>
    </div>
  </div>

  <!-- WS status -->
  <div class="card">
    <div class="label">Estado WebSockets</div>
    <div class="ws-row" style="margin-top:8px">
      <div class="ws-chip highlight">@ticker 24h: <b id="wsTickers" style="color:var(--teal)">—</b>/<span id="wsTotal">—</span></div>
      <div class="ws-chip">markPrice WS: <b id="wsActive">—</b>/<span id="wsTotal2">—</span></div>
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
    <h2>🔒 Símbolos en cooldown — bloqueados 24 h tras cierre</h2>
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
      Top <span id="winnerCount">0</span> ganadores
      <span style="color:var(--muted);font-weight:400;font-size:13px">
        · Ranking 24h en tiempo real por WS @ticker &nbsp;|&nbsp;
        REST cada 20 min (solo nuevos símbolos) &nbsp;|&nbsp; 🟠 = cooldown
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
        <tr><td colspan="6" style="color:var(--muted)">Cargando…</td></tr>
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

let pollCount  = 0;
let _cdData    = {};
let _lastFetch = 0;
let _prevWsTicker = 0;

// Decrementa el contador de cooldown en el DOM cada segundo
function tickCooldowns() {
  const elapsed = (Date.now() - _lastFetch) / 1000;
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
}
setInterval(tickCooldowns, 1000);

// ── Render ──────────────────────────────────────────────────────────────────
function render(d) {
  if (!d) return;
  _lastFetch = Date.now();

  const mode = d.mode || '—';
  q('mode').textContent      = mode;
  q('modeBadge').textContent = mode;
  q('modeBadge').className   = 'badge ' + (mode === 'REAL' ? 'badge-green' : 'badge-yellow');

  const pu = n(d.total_unrealized);
  q('pnl').textContent         = money(pu);
  q('pnl').className           = 'value ' + cls(pu);
  q('notional').textContent    = money(d.total_notional);
  q('scan').textContent        = d.last_scan_text         || 'pendiente';
  q('scanCount').textContent   = n(d.scan_count);
  q('lastWsTicker').textContent = d.last_ws_ticker_text   || 'pendiente';
  q('wsTickerUpdates').textContent = n(d.ws_ticker_updates);
  q('lastWinners').textContent = d.last_winners_text      || 'pendiente';
  q('contracts').textContent   = n(d.exchange_symbols);
  q('subCount').textContent    = n(d.subscribed_count);
  q('cooldownCount').textContent = n(d.cooldown_count);

  // Parpadeo del indicador WS @ticker
  const wtu = n(d.ws_ticker_updates);
  if (wtu !== _prevWsTicker) {
    q('dotWsTicker').className = 'on';
    setTimeout(() => { q('dotWsTicker').className = ''; }, 800);
    _prevWsTicker = wtu;
  }

  const pw = d.price_ws || {}, kw = d.kline_ws || {};
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

  // ── Ganadores ─────────────────────────────────────────────────────────────
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
  }), 'No hay ganadores aún…', 6);

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
        "ok":                 True,
        "running":            bot.running,
        "mode":               snap["mode"],
        "scan_count":         snap["scan_count"],
        "ws_ticker_updates":  snap["ws_ticker_updates"],
        "last_error":         snap["last_error"],
        "cooldown_count":     snap["cooldown_count"],
    })


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, threaded=True)
