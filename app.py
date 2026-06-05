"""
Bot web para shortear ganadores de Binance Futures.

ARQUITECTURA CORRECTA:
════════════════════════════════════════════════════════════════════════════
 1. REST cada 60 s  → /fapi/v1/ticker/24hr
      Solo para descubrir los TOP 30 símbolos ganadores por cambio 24h.
      Una sola llamada, ~5 tokens REST. Guarda symbol + change + precio inicial.

 2. SymbolWebSocketPriceCache (WS.py)  → markPrice@1s de los 30 símbolos
      Precio en tiempo real para: evaluar TP de posiciones abiertas y
      mostrar precios actualizados en la tabla de ganadores y posiciones.
      Se re-suscribe cuando la lista de 30 símbolos cambia.

 3. KlineWebSocketCache (KlineWebSocketCache_v4.py)  → klines 1m de los 30
      Velas en tiempo real para verificar condiciones técnicas de entrada
      antes de abrir un tramo short.
      Se re-inicia cuando la lista cambia (backfill rápido).

 4. Scanner (bucle interno cada SCAN_INTERVAL_SECONDS)
      Lee precios del price_cache y klines del kline_cache.
      Aplica la estrategia de niveles (50/75/100/150/200/250 %).

 5. Realtime TP loop (cada 0.25 s)
      Comprueba take-profit de posiciones abiertas usando precios WS.
      No usa REST.

 6. fetch() polling desde el navegador → /api/status cada 2 s
      Actualiza el DOM por JS sin recargar la página.
      Compatible con cualquier worker de Gunicorn (sync, gthread, gevent).
      El navegador NO se conecta directamente a Binance.
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

# ── Importar los módulos WS del mismo directorio ──────────────────────────────
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

# Cuántos ganadores seguir (top N por cambio 24h)
TOP_WINNERS          = int(os.getenv("TOP_WINNERS",          "30"))
# Cada cuántos segundos refrescar la lista de ganadores por REST
WINNERS_REFRESH_SECS = int(os.getenv("WINNERS_REFRESH_SECS", "60"))
# Cada cuántos segundos corre el scanner de entrada
SCAN_INTERVAL_SECS   = int(os.getenv("SCAN_INTERVAL_SECS",   "10"))
# Cambio mínimo 24h para mostrar en tabla (0 = todos los ganadores)
MIN_GAIN_TO_SHOW     = float(os.getenv("MIN_GAIN_TO_SHOW",   "0"))

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
# CLIENTE BINANCE FUTURES (solo REST firmado/sin firmar)
# ─────────────────────────────────────────────────────────────────────────────

class BinanceFuturesClient:
    def __init__(self) -> None:
        self.exchange_filters: Dict[str, Dict[str, float]] = {}

    async def start(self) -> None:
        await self.load_exchange_info()

    async def request(self, method: str, path: str, params: Optional[dict] = None, signed: bool = False) -> Any:
        return await asyncio.to_thread(self._sync_request, BASE_URL, method, path, params, signed)

    def _sync_request(self, base_url: str, method: str, path: str, params: Optional[dict] = None, signed: bool = False) -> Any:
        params = dict(params or {})
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
        data = await self.request("GET", "/fapi/v1/exchangeInfo")
        filters: Dict[str, Dict[str, float]] = {}
        for sym in data.get("symbols", []):
            if sym.get("quoteAsset") != QUOTE_ASSET:
                continue
            if sym.get("contractType") != "PERPETUAL":
                continue
            if sym.get("status") != "TRADING":
                continue
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
            await self.request("POST", "/fapi/v1/leverage", {"symbol": symbol, "leverage": LEVERAGE}, signed=True)

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
            {"symbol": symbol, "side": "SELL", "type": "MARKET", "quantity": qty}, signed=True)
        return qty

    async def close_short(self, symbol: str, qty: float) -> None:
        qty = self.normalize_qty(symbol, qty)
        if qty <= 0 or PAPER_MODE or not LIVE_TRADING:
            return
        await self.request("POST", "/fapi/v1/order",
            {"symbol": symbol, "side": "BUY", "type": "MARKET",
             "quantity": qty, "reduceOnly": "true"}, signed=True)


# ─────────────────────────────────────────────────────────────────────────────
# BOT PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

class TradingBot:
    def __init__(self) -> None:
        self.client   = BinanceFuturesClient()
        self.positions: Dict[str, BotPosition] = {}
        self.winners:   List[dict] = []          # top 30 con change 24h (REST)
        self.closed_trades: List[dict] = []
        self.events:        List[str] = []
        self.lock = threading.Lock()

        # WS caches — se crean/recrean dinámicamente
        self.price_cache:  Optional[SymbolWebSocketPriceCache] = None
        self.kline_cache:  Optional[KlineWebSocketCache]       = None
        self.subscribed_symbols: List[str] = []   # símbolos actuales suscritos

        # Métricas
        self.running          = False
        self.scan_count       = 0
        self.last_scan_at     = 0.0
        self.last_winners_at  = 0.0
        self.last_error       = ""
        self.last_startup_err = ""
        self.exchange_symbols = 0
        self.started_at       = time.time()

        # Snapshot SSE (actualizado cada segundo por _snapshot_loop)
        self._sse_snapshot: str = "{}"

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
        self.thread  = threading.Thread(target=self._run_loop, daemon=True, name="BotLoop")
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
            self.running   = False
            self.last_error = str(exc)
            self.log(f"Bot detenido por error no controlado: {exc}")

    # ── Main ──────────────────────────────────────────────────────────────────

    async def _main(self) -> None:
        self.log("Bot iniciado — modo " + ("PAPER" if PAPER_MODE or not LIVE_TRADING else "REAL"))
        try:
            await self.client.start()
            self.exchange_symbols = len(self.client.exchange_filters)
            self.log(f"ExchangeInfo: {self.exchange_symbols} contratos USDT-M perpetuos")
        except Exception as exc:
            self.last_startup_err = str(exc)
            self.log(f"ExchangeInfo falló ({exc}). Continúo con filtros mínimos.")

        # Primera carga REST de ganadores (bloquea hasta tener datos)
        await self._fetch_top_winners()

        # Arrancar loops concurrentes
        await asyncio.gather(
            self._winners_refresh_loop(),
            self._scanner(),
            self._realtime_price_loop(),
            self._snapshot_loop(),
        )

    # ── Gestión de WebSocket caches ───────────────────────────────────────────

    def _stop_price_cache(self) -> None:
        if self.price_cache:
            try:
                self.price_cache.stop()
            except Exception:
                pass
            self.price_cache = None

    def _stop_kline_cache(self) -> None:
        if self.kline_cache:
            try:
                self.kline_cache.stop()
            except Exception:
                pass
            self.kline_cache = None

    def _open_position_symbols(self) -> List[str]:
        """Símbolos con posición abierta que deben mantenerse suscritos."""
        with self.lock:
            return [
                sym for sym, pos in self.positions.items()
                if pos.status == "OPEN" and pos.fills
            ]

    def _merged_ws_symbols(self, winners: List[str]) -> List[str]:
        """Une winners + posiciones abiertas y elimina duplicados preservando orden."""
        forced = self._open_position_symbols()
        return list(dict.fromkeys([*winners, *forced]))

    def _start_price_cache(self, symbols: List[str]) -> None:
        """Arranca (o re-arranca) el cache de precios mark para los símbolos dados."""
        self._stop_price_cache()
        if not symbols:
            return
        self.price_cache = SymbolWebSocketPriceCache(
            symbols,
            symbols_per_connection=30,   # todos en una sola conexión combinada
        )
        self.price_cache.start()
        self.log(f"PriceCache iniciado con {len(symbols)} símbolos")

    def _start_kline_cache(self, symbols: List[str]) -> None:
        """Arranca (o re-arranca) el cache de klines 1m para los símbolos dados."""
        self._stop_kline_cache()
        if not symbols:
            return
        pairs = {sym: ["1m"] for sym in symbols}
        self.kline_cache = KlineWebSocketCache(
            pairs             = pairs,
            max_candles       = 10,
            include_open_candle = True,
            backfill_on_start = True,
            streams_per_connection = 30,
            rest_concurrency  = 10,
            rest_retries      = 3,
            backfill_batch_size = 5,
            backfill_batch_delay = 0.10,
            safety_refresh_interval_seconds = 600,
        )
        self.kline_cache.start()
        self.log(f"KlineCache iniciado con {len(symbols)} símbolos (1m)")

    async def _update_subscriptions(self, new_symbols: List[str]) -> None:
        """
        Compara la lista nueva con la suscrita.
        Si hay cambios: re-suscribe price_cache y kline_cache.

        Protección importante:
        - Siempre mantiene suscritos también los símbolos con posición abierta.
        - Así, aunque salgan del top 30, siguen recibiendo markPrice y klines.
        """
        merged_symbols = self._merged_ws_symbols(new_symbols)

        old_set = set(self.subscribed_symbols)
        new_set = set(merged_symbols)
        added   = new_set - old_set
        removed = old_set - new_set

        if not added and not removed:
            return  # Sin cambios

        self.log(
            f"Actualización de suscripciones: +{len(added)} síms nuevos, -{len(removed)} eliminados"
        )

        # Reiniciar en threads para no bloquear el event loop
        await asyncio.to_thread(self._start_price_cache, merged_symbols)
        await asyncio.to_thread(self._start_kline_cache, merged_symbols)
        self.subscribed_symbols = list(merged_symbols)

    # ── REST: Top 30 ganadores ────────────────────────────────────────────────

    async def _fetch_top_winners(self) -> None:
        """
        Llama UNA VEZ a /fapi/v1/ticker/24hr.
        Filtra USDT-M perpetuos, ordena por cambio 24h desc, toma TOP_WINNERS.
        Si la lista cambia → re-suscribe los WS caches.
        """
        try:
            self.log("REST: obteniendo top ganadores 24h...")
            data = await self.client.request("GET", "/fapi/v1/ticker/24hr")
            if not isinstance(data, list):
                self.log("REST: respuesta inesperada (no es lista)")
                return

            filters = self.client.exchange_filters  # puede estar vacío en cold-start

            candidates = []
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
                candidates.append({
                    "symbol":      symbol,
                    "change":      change,
                    "price":       price,   # precio inicial; se actualiza con WS
                    "market":      "futures",
                    "can_short":   True,
                })

            candidates.sort(key=lambda x: x["change"], reverse=True)
            top = candidates[:TOP_WINNERS]
            new_symbols = [w["symbol"] for w in top]

            # Actualizar suscripciones WS si la lista cambió
            await self._update_subscriptions(new_symbols)

            with self.lock:
                self.winners          = top
                self.last_winners_at  = time.time()

            top_str = (f"{top[0]['symbol']} {top[0]['change']:.1f}%" if top else "ninguno")
            self.log(f"REST ganadores: {len(top)} símbolos | top={top_str}")

        except Exception as exc:
            self.last_error = str(exc)
            self.log(f"REST _fetch_top_winners falló: {exc}")

    async def _winners_refresh_loop(self) -> None:
        """Refresca la lista de ganadores cada WINNERS_REFRESH_SECS."""
        while self.running:
            await asyncio.sleep(WINNERS_REFRESH_SECS)
            await self._fetch_top_winners()

    # ── Condición kline (confirmación técnica de entrada) ─────────────────────

    def _kline_entry_ok(self, symbol: str) -> bool:
        """
        Verifica condición técnica mínima usando klines 1m:
        - Retorna True si la última vela cerrada es alcista (close >= open).
        - Si no hay datos de kline aún → permite la entrada (True).
        Ajusta esta lógica según tu estrategia.
        """
        if not self.kline_cache:
            return True
        try:
            df = self.kline_cache.get_dataframe(symbol, "1m", only_closed=True)
            if df.empty or len(df) < 2:
                return True   # sin datos suficientes → permitir
            last = df.iloc[-1]
            # Condición: la última vela es alcista → buen momento para short
            return float(last["close"]) >= float(last["open"])
        except Exception:
            return True  # error → no bloquear entrada

    # ── Scanner (evalúa entradas) ──────────────────────────────────────────────

    async def _scanner(self) -> None:
        """
        Cada SCAN_INTERVAL_SECS:
          1. Lee la lista de 30 ganadores (ya actualizada por REST).
          2. Para cada uno, obtiene precio en tiempo real del price_cache WS.
          3. Verifica condición kline.
          4. Aplica la estrategia de niveles y abre shorts si procede.
        """
        self.log("Scanner: esperando datos de price_cache...")
        # Esperar hasta que el price_cache tenga datos
        for _ in range(60):
            if not self.running:
                return
            if self.price_cache and len(self.price_cache.get_all_prices()) > 0:
                break
            await asyncio.sleep(1.0)
        self.log("Scanner: price_cache con datos — iniciando escaneos")

        while self.running:
            try:
                with self.lock:
                    winners = list(self.winners)

                all_prices = self.price_cache.get_all_prices() if self.price_cache else {}

                for row in winners:
                    if not row.get("can_short", True):
                        continue
                    symbol = row["symbol"]
                    change = row["change"]
                    # Precio en tiempo real (WS) o fallback al precio REST
                    price = all_prices.get(symbol) or row.get("price", 0.0)
                    if price <= 0:
                        continue

                    # Verificar condición kline antes de entrar
                    kline_ok = self._kline_entry_ok(symbol)

                    for level, notional in zip(ENTRY_LEVELS, ENTRY_NOTIONALS):
                        if change >= level and kline_ok:
                            await self._ensure_short(symbol, level, notional, price, change)

                    # Revisar TP también en el scanner (respaldo)
                    await self._maybe_take_profit(symbol, price)

                # También revisar TP de posiciones que ya no están en winners
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

                n_high = sum(1 for w in winners if w["change"] >= ENTRY_LEVELS[0])
                self.log(
                    f"Escan #{self.scan_count}: {len(winners)} ganadores | "
                    f">={ENTRY_LEVELS[0]:.0f}%: {n_high} | "
                    f"posiciones abiertas: {len(self.positions)}"
                )
                self.persist_state()

            except Exception as exc:
                self.last_error = str(exc)
                self.log(f"Error en scanner: {exc}")

            await asyncio.sleep(SCAN_INTERVAL_SECS)

    # ── Realtime TP loop (alta frecuencia, WS prices) ─────────────────────────

    async def _realtime_price_loop(self) -> None:
        """
        Cada 0.25 s comprueba TP de posiciones abiertas usando precios WS.
        No usa REST. Este es el loop que asegura no perder el momento de cierre.
        """
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
            except Exception as exc:
                self.last_error = str(exc)
            await asyncio.sleep(0.25)

    # ── Snapshot loop (mantiene _sse_snapshot y sirve /api/status) ─────────────

    async def _snapshot_loop(self) -> None:
        """Reconstruye el snapshot JSON cada segundo para /api/status."""
        while self.running:
            try:
                snap = self._build_snapshot()
                self._sse_snapshot = json.dumps(snap, ensure_ascii=False, default=str)
            except Exception as exc:
                self.log(f"Error construyendo snapshot: {exc}")
            await asyncio.sleep(1.0)

    # ── Estrategia ────────────────────────────────────────────────────────────

    async def _ensure_short(self, symbol: str, level: float, notional: float, price: float, change: float) -> None:
        with self.lock:
            pos = self.positions.setdefault(symbol, BotPosition(symbol=symbol))
            if level in pos.opened_levels() or pos.status != "OPEN":
                return

        try:
            qty  = await self.client.market_short(symbol, notional, price)
            fill = Fill(level=level, notional=notional, entry_price=price, qty=qty)
            with self.lock:
                self.positions[symbol].fills.append(fill)
            self.log(f"SHORT {symbol}: nivel {level:.0f}% | {notional:.2f} USDT | qty={qty} | px={price:.6f} | cambio={change:.2f}%")
            self.persist_state()
        except Exception as exc:
            self.last_error = str(exc)
            self.log(f"Error abriendo short {symbol} nivel {level}: {exc}")

    async def _maybe_take_profit(self, symbol: str, price: float) -> None:
        with self.lock:
            pos = self.positions.get(symbol)
            if not pos or pos.status != "OPEN" or not pos.fills:
                return
            pnl     = pos.unrealized_pnl(price)
            target  = pos.notional * TAKE_PROFIT_FRACTION
            qty     = pos.qty
            avg_ent = pos.avg_entry
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
                self.closed_trades.insert(0, {
                    "symbol":      symbol,
                    "pnl":         pnl,
                    "target":      target,
                    "qty":         qty,
                    "avg_entry":   avg_ent,
                    "close_price": price,
                    "notional":    notional,
                    "closed_at":   datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                })
                self.closed_trades = self.closed_trades[:100]

        self.log(f"CIERRE {symbol}: PnL={pnl:.4f} | objetivo={target:.4f} | px={price:.6f}")
        self.persist_state()

    # ── Snapshot ──────────────────────────────────────────────────────────────

    def _build_snapshot(self) -> dict:
        """Construye el snapshot completo para /api/status. Thread-safe."""
        # Precios en tiempo real del WS cache
        all_prices = self.price_cache.get_all_prices() if self.price_cache else {}
        ws_stats   = self.price_cache.get_stats()       if self.price_cache else {}
        kl_stats   = self.kline_cache.get_stats()       if self.kline_cache else {}

        with self.lock:
            winners_raw  = list(self.winners)
            positions_raw = dict(self.positions)
            closed       = list(self.closed_trades[:30])
            events       = list(self.events[:50])

        # Enriquecer ganadores con precio en tiempo real del WS
        winners_out = []
        for w in winners_raw:
            sym   = w["symbol"]
            price = all_prices.get(sym) or w.get("price", 0.0)
            winners_out.append({**w, "price": price})

        # Posiciones con PnL en tiempo real
        open_positions = []
        total_unreal   = 0.0
        total_notional = 0.0
        for symbol, pos in positions_raw.items():
            price = all_prices.get(symbol) or 0.0
            pnl   = pos.unrealized_pnl(price)
            total_unreal   += pnl
            total_notional += pos.notional
            open_positions.append({
                "symbol":        symbol,
                "mark_price":    price,
                "avg_entry":     pos.avg_entry,
                "qty":           pos.qty,
                "notional":      pos.notional,
                "target":        pos.notional * TAKE_PROFIT_FRACTION,
                "unrealized_pnl": pnl,
                "fills":         [f.__dict__ for f in pos.fills],
                "change":        next((w["change"] for w in winners_raw if w["symbol"] == symbol), 0.0),
            })

        last_scan_text = (
            datetime.fromtimestamp(self.last_scan_at, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            if self.last_scan_at else "pendiente"
        )
        last_winners_text = (
            datetime.fromtimestamp(self.last_winners_at, timezone.utc).strftime("%H:%M:%S UTC")
            if self.last_winners_at else "pendiente"
        )

        return {
            "mode":              "PAPER" if PAPER_MODE or not LIVE_TRADING else "REAL",
            "running":           self.running,
            "thread_alive":      bool(self.thread and self.thread.is_alive()),
            "started_at":        self.started_at,
            "uptime_seconds":    round(time.time() - self.started_at, 1),
            "scan_count":        self.scan_count,
            "last_scan_text":    last_scan_text,
            "last_winners_text": last_winners_text,
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
            "price_ws": {
                "active":   ws_stats.get("active_symbols", 0),
                "total":    ws_stats.get("total_symbols",  0),
                "stale":    ws_stats.get("stale_symbols",  0),
            },
            "kline_ws": {
                "pairs_with_data": kl_stats.get("pairs_with_data", 0),
                "total_messages":  kl_stats.get("total_messages",  0),
                "active_conns":    kl_stats.get("active_connections", 0),
            },
            "ts": time.time(),
        }

    # ── Persistencia ─────────────────────────────────────────────────────────

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
        """Snapshot público (para /api/status y carga inicial de la página)."""
        live = self._build_snapshot()
        # Intentar usar estado persistido si no tenemos datos aún
        if not live["positions"] and not live["winners"] and os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, "r", encoding="utf-8") as fh:
                    persisted = json.load(fh)
                if isinstance(persisted, dict) and persisted.get("scan_count", 0) > live.get("scan_count", 0):
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
# HTML + JS (fetch polling — sin conexión directa a Binance desde el navegador)
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
      --blue: #60a5fa; --purple: #a78bfa;
    }
    * { box-sizing: border-box; }
    body  { margin: 0; font-family: Arial, sans-serif; background: var(--bg); color: var(--txt); }
    header { padding: 20px 24px; background: var(--card); border-bottom: 1px solid var(--border); display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }
    header h1 { margin: 0; font-size: 18px; }
    .badge { padding: 3px 10px; border-radius: 6px; font-size: 12px; font-weight: 700; }
    .badge-green  { background: #14532d; color: #86efac; }
    .badge-yellow { background: #713f12; color: #fde68a; }
    .badge-blue   { background: #1e3a5f; color: #93c5fd; }
    main  { padding: 16px; display: grid; gap: 16px; }
    .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 12px; }
    .card  { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 14px; }
    .label { color: var(--muted); font-size: 12px; margin-bottom: 4px; }
    .value { font-size: 22px; font-weight: 700; }
    .value.sm { font-size: 14px; }
    .positive { color: var(--green); } .negative { color: var(--red); } .warn { color: var(--yellow); }
    section { background: var(--card); border: 1px solid var(--border); border-radius: 12px; overflow: hidden; }
    section h2 { margin: 0; padding: 12px 16px; font-size: 15px; border-bottom: 1px solid var(--border); }
    table  { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td { padding: 9px 12px; border-bottom: 1px solid #1f2937; text-align: right; white-space: nowrap; }
    th:first-child, td:first-child { text-align: left; }
    th { color: var(--muted); font-weight: 600; font-size: 11px; text-transform: uppercase; }
    pre  { background: var(--card); padding: 12px; overflow: auto; max-height: 260px; white-space: pre-wrap; font-size: 12px; margin: 0; }
    .pill { display: inline-block; padding: 2px 8px; border-radius: 999px; background: #1e293b; border: 1px solid #475569; font-size: 11px; margin: 1px; }
    #sseDot { width: 8px; height: 8px; border-radius: 50%; background: var(--red); display: inline-block; transition: background .3s; }
    #sseDot.on { background: var(--green); }
    .ws-row  { display: flex; gap: 8px; flex-wrap: wrap; }
    .ws-chip { background: #1e293b; border: 1px solid var(--border); border-radius: 8px; padding: 4px 10px; font-size: 12px; }
    #errorBox { border-color: var(--red); }
    .sym-link { color: var(--blue); text-decoration: none; font-weight: 600; }
    .sym-link:hover { text-decoration: underline; }
  </style>
</head>
<body>
<header>
  <span id="dotPoll" title="Verde = polling activo"></span>
  <h1>Bot Short Ganadores · Binance USDT-M Futures</h1>
  <span id="modeBadge" class="badge badge-yellow">—</span>
  <span class="badge badge-green">Precios WS en tiempo real</span>
  <span class="badge badge-blue">Sin polling REST</span>
</header>
<main>

  <!-- KPIs -->
  <div class="cards">
    <div class="card"><div class="label">Modo</div><div id="mode" class="value warn sm">—</div></div>
    <div class="card"><div class="label">PnL no realizado</div><div id="pnl" class="value">—</div></div>
    <div class="card"><div class="label">Capital en posiciones</div><div id="notional" class="value">—</div></div>
    <div class="card"><div class="label">Último escaneo</div><div id="scan" class="value sm">—</div></div>
    <div class="card"><div class="label">Escaneos totales</div><div id="scanCount" class="value">—</div></div>
    <div class="card"><div class="label">Ganadores REST</div><div id="lastWinners" class="value sm">—</div></div>
    <div class="card"><div class="label">Contratos cargados</div><div id="contracts" class="value">—</div></div>
    <div class="card"><div class="label">Símbolos suscritos WS</div><div id="subCount" class="value">—</div></div>
  </div>

  <!-- WS status -->
  <div class="card">
    <div class="label">Estado WebSockets</div>
    <div class="ws-row" style="margin-top:8px">
      <div class="ws-chip">markPrice WS: <b id="wsActive">—</b>/<span id="wsTotal">—</span></div>
      <div class="ws-chip">stale: <b id="wsStale">—</b></div>
      <div class="ws-chip">kline pares: <b id="klPairs">—</b></div>
      <div class="ws-chip">kline msgs: <b id="klMsgs">—</b></div>
      <div class="ws-chip">kline conns: <b id="klConns">—</b></div>
      <div class="ws-chip">Actualizaciones fetch: <b id="pollCount">0</b></div>
    </div>
  </div>

  <!-- Error -->
  <section id="errorBox" style="display:none">
    <h2 style="color:var(--red)">Error / Diagnóstico</h2>
    <pre id="lastError" style="color:var(--red)"></pre>
  </section>

  <!-- Posiciones abiertas -->
  <section>
    <h2>Posiciones abiertas</h2>
    <table>
      <thead><tr>
        <th>Símbolo</th><th>Cambio 24h</th><th>Entrada media</th>
        <th>Precio WS</th><th>Notional</th><th>Objetivo</th>
        <th>PnL tiempo real</th><th>Tramos</th>
      </tr></thead>
      <tbody id="tbPositions"><tr><td colspan="8" style="color:var(--muted)">Sin posiciones</td></tr></tbody>
    </table>
  </section>

  <!-- Ganadores -->
  <section>
    <h2>Top <span id="winnerCount">0</span> ganadores (actualizado por REST cada 60 s · precios por WS)</h2>
    <table>
      <thead><tr>
        <th>Símbolo</th><th>Cambio 24h</th><th>Precio WS</th>
        <th>Condición kline</th><th>Short</th>
      </tr></thead>
      <tbody id="tbWinners"><tr><td colspan="5" style="color:var(--muted)">Cargando…</td></tr></tbody>
    </table>
  </section>

  <!-- Cierres -->
  <section>
    <h2>Operaciones cerradas</h2>
    <table>
      <thead><tr>
        <th>Símbolo</th><th>PnL realizado</th><th>Objetivo</th>
        <th>Entrada media</th><th>Precio cierre</th><th>Fecha</th>
      </tr></thead>
      <tbody id="tbClosed"><tr><td colspan="6" style="color:var(--muted)">Sin cierres aún</td></tr></tbody>
    </table>
  </section>

  <!-- Eventos -->
  <section>
    <h2>Eventos del bot</h2>
    <pre id="events" style="background:transparent"></pre>
  </section>

</main>

<script>
// ── Utilidades ─────────────────────────────────────────────────────────────────
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

let pollCount = 0;
const dot = q('dotPoll');

// ── Render ─────────────────────────────────────────────────────────────────────
function render(d) {
  if (!d) return;
  const mode = d.mode || '—';
  q('mode').textContent        = mode;
  q('modeBadge').textContent   = mode;
  q('modeBadge').className     = 'badge ' + (mode === 'REAL' ? 'badge-green' : 'badge-yellow');

  const pu = n(d.total_unrealized);
  q('pnl').textContent         = money(pu);
  q('pnl').className           = 'value ' + cls(pu);
  q('notional').textContent    = money(d.total_notional);
  q('scan').textContent        = d.last_scan_text    || 'pendiente';
  q('scanCount').textContent   = n(d.scan_count);
  q('lastWinners').textContent = d.last_winners_text || 'pendiente';
  q('contracts').textContent   = n(d.exchange_symbols);
  q('subCount').textContent    = n(d.subscribed_count);

  const pw = d.price_ws || {}, kw = d.kline_ws || {};
  q('wsActive').textContent  = n(pw.active);
  q('wsTotal').textContent   = n(pw.total);
  q('wsStale').textContent   = n(pw.stale);
  q('klPairs').textContent   = n(kw.pairs_with_data);
  q('klMsgs').textContent    = n(kw.total_messages);
  q('klConns').textContent   = n(kw.active_conns);
  q('pollCount').textContent = pollCount;

  const err = d.last_error || d.last_startup_err || '';
  q('errorBox').style.display = err ? 'block' : 'none';
  q('lastError').textContent  = err;

  // ── Posiciones ───────────────────────────────────────────────────────────────
  const positions = Array.isArray(d.positions) ? d.positions : [];
  q('tbPositions').innerHTML = tb(positions.map(p => {
    const pnl   = n(p.unrealized_pnl);
    const fills = (Array.isArray(p.fills) ? p.fills : [])
      .map(f => `<span class="pill">+${fx(f.level,0)}% / ${fx(f.notional,2)}</span>`)
      .join(' ');
    return `<tr>
      <td><a class="sym-link" href="https://www.binance.com/en/futures/${p.symbol}" target="_blank">${p.symbol}</a></td>
      <td class="${cls(p.change)}">${pct(p.change)}</td>
      <td>${fx(p.avg_entry)}</td>
      <td>${fx(p.mark_price)}</td>
      <td>${money(p.notional)}</td>
      <td>${money(p.target)}</td>
      <td class="${cls(pnl)}">${money(pnl)}</td>
      <td>${fills}</td>
    </tr>`;
  }), 'Sin posiciones abiertas', 8);

  // ── Ganadores ────────────────────────────────────────────────────────────────
  const winners     = Array.isArray(d.winners) ? d.winners : [];
  const entryLevels = Array.isArray(d.entry_levels) ? d.entry_levels : [50];
  q('winnerCount').textContent = winners.length;
  q('tbWinners').innerHTML = tb(winners.map(w => {
    const change   = n(w.change);
    const canTrade = change >= entryLevels[0];
    return `<tr style="${canTrade ? 'background:rgba(34,197,94,0.05)' : ''}">
      <td><a class="sym-link" href="https://www.binance.com/en/futures/${w.symbol}" target="_blank">${w.symbol}</a></td>
      <td class="${cls(change)}">${pct(change)}</td>
      <td>${fx(n(w.price))}</td>
      <td>${canTrade ? '<span style="color:var(--green)">✓ alcista</span>' : '<span style="color:var(--muted)">—</span>'}</td>
      <td>${w.can_short ? '<span style="color:var(--green)">sí</span>' : '<span style="color:var(--red)">no</span>'}</td>
    </tr>`;
  }), 'No hay ganadores aún…', 5);

  // ── Cierres ──────────────────────────────────────────────────────────────────
  const closed = Array.isArray(d.closed_trades) ? d.closed_trades : [];
  q('tbClosed').innerHTML = tb(closed.map(t => `<tr>
    <td>${t.symbol || ''}</td>
    <td class="positive">${money(t.pnl)}</td>
    <td>${money(t.target)}</td>
    <td>${fx(t.avg_entry)}</td>
    <td>${fx(t.close_price)}</td>
    <td style="color:var(--muted)">${t.closed_at || ''}</td>
  </tr>`), 'Sin cierres aún', 6);

  q('events').textContent = (Array.isArray(d.events) ? d.events : []).join('\n');
}

// ── Polling con fetch() cada 2 s ───────────────────────────────────────────────
// No usa SSE ni WebSocket. Llama /api/status con fetch() y actualiza el DOM.
// Funciona con CUALQUIER tipo de worker de Gunicorn (sync, gthread, gevent).
// No genera WORKER TIMEOUT porque cada petición dura < 100 ms.
let pollTimer   = null;
let pollDelay   = 2000;   // ms entre llamadas
let pollFailing = false;

async function poll() {
  try {
    const resp = await fetch('/api/status', { cache: 'no-store' });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    pollCount++;
    dot.className = 'on';
    pollDelay     = 2000;
    pollFailing   = false;
    render(data);
  } catch (err) {
    dot.className = '';
    if (!pollFailing) { console.warn('Poll error:', err.message); pollFailing = true; }
    pollDelay = Math.min(pollDelay * 1.5, 15000);  // backoff suave al fallar
  } finally {
    pollTimer = setTimeout(poll, pollDelay);
  }
}

poll();  // arranca inmediatamente
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
    from flask import jsonify
    resp = jsonify(bot.snapshot())
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


@app.get("/health")
def health():
    from flask import jsonify
    snap = bot.snapshot()
    return jsonify({
        "ok":         True,
        "running":    bot.running,
        "mode":       snap["mode"],
        "scan_count": snap["scan_count"],
        "last_error": snap["last_error"],
    })


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, threaded=True)
