"""
Bot web para shortear ganadores de Binance Futures.

Estrategia:
- Escanea los símbolos USDT-M perpetual con mayor cambio 24h.
- Si el cambio supera niveles configurados (50, 75, 100, 150, 200, 250 %),
  abre tramos short con nocionales (5, 5, 10, 20, 40, 80 USDT).
- Cierra toda la posición cuando la ganancia no realizada alcanza el 50 % del
  capital colocado en esa posición (ej.: 5 USDT -> 2.5 USDT).
- Por seguridad arranca en PAPER_MODE=true. Para operar real se requieren
  BINANCE_API_KEY, BINANCE_API_SECRET y LIVE_TRADING=true.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import threading
import time
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from math import floor
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import urllib.error
import urllib.request

import websockets
from flask import Flask, jsonify, make_response, render_template_string


BASE_URL = os.getenv("BASE_URL", "https://fapi.binance.com")
WS_URL = os.getenv("WS_URL", "wss://fstream.binance.com/ws/!ticker@arr")
SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "60"))
PAPER_MODE = os.getenv("PAPER_MODE", "true").lower() == "true"
LIVE_TRADING = os.getenv("LIVE_TRADING", "false").lower() == "true"
API_KEY = os.getenv("BINANCE_API_KEY", "")
API_SECRET = os.getenv("BINANCE_API_SECRET", "")
QUOTE_ASSET = os.getenv("QUOTE_ASSET", "USDT")
MIN_GAIN_TO_SHOW = float(os.getenv("MIN_GAIN_TO_SHOW", "0"))
SPOT_BASE_URL = os.getenv("SPOT_BASE_URL", "https://api.binance.com")
INCLUDE_SPOT_WINNERS = os.getenv("INCLUDE_SPOT_WINNERS", "false").lower() == "true"
MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "120"))
LEVERAGE = int(os.getenv("LEVERAGE", "1"))
STATE_FILE = os.getenv("STATE_FILE", os.path.join(tempfile.gettempdir(), "bottradingriesgo_state.json"))

ENTRY_LEVELS = [float(x) for x in os.getenv("ENTRY_LEVELS", "20,75,100,150,200,250").split(",")]
ENTRY_NOTIONALS = [float(x) for x in os.getenv("ENTRY_NOTIONALS", "5,5,10,20,40,80").split(",")]
TAKE_PROFIT_FRACTION = float(os.getenv("TAKE_PROFIT_FRACTION", "0.14284"))


@dataclass
class Fill:
    level: float
    notional: float
    entry_price: float
    qty: float
    opened_at: float = field(default_factory=time.time)


@dataclass
class BotPosition:
    symbol: str
    fills: List[Fill] = field(default_factory=list)
    realized_pnl: float = 0.0
    status: str = "OPEN"

    @property
    def qty(self) -> float:
        return sum(fill.qty for fill in self.fills)

    @property
    def notional(self) -> float:
        return sum(fill.notional for fill in self.fills)

    @property
    def avg_entry(self) -> float:
        if self.qty <= 0:
            return 0.0
        return sum(fill.entry_price * fill.qty for fill in self.fills) / self.qty

    def unrealized_pnl(self, mark_price: float) -> float:
        if mark_price <= 0:
            return 0.0
        return sum((fill.entry_price - mark_price) * fill.qty for fill in self.fills)

    def opened_levels(self) -> set[float]:
        return {fill.level for fill in self.fills}


class BinanceFuturesClient:
    def __init__(self) -> None:
        self.exchange_filters: Dict[str, Dict[str, float]] = {}
        self.last_warning = ""

    async def start(self) -> None:
        await self.load_exchange_info()

    async def close(self) -> None:
        return None

    async def request(self, method: str, path: str, params: Optional[dict] = None, signed: bool = False) -> Any:
        return await asyncio.to_thread(self._request_sync, BASE_URL, method, path, params, signed)

    async def public_request(self, base_url: str, path: str, params: Optional[dict] = None) -> Any:
        return await asyncio.to_thread(self._request_sync, base_url, "GET", path, params, False)

    def _request_sync(self, base_url: str, method: str, path: str, params: Optional[dict] = None, signed: bool = False) -> Any:
        params = dict(params or {})
        headers = {"User-Agent": "BOTTRADINGRIESGO/1.0"}
        if signed:
            if not API_KEY or not API_SECRET:
                raise RuntimeError("Faltan BINANCE_API_KEY/BINANCE_API_SECRET para trading real")
            params["timestamp"] = int(time.time() * 1000)
            params["recvWindow"] = 5000
            query = urlencode(params, doseq=True)
            signature = hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
            params["signature"] = signature
            headers["X-MBX-APIKEY"] = API_KEY
        elif API_KEY:
            headers["X-MBX-APIKEY"] = API_KEY

        query = urlencode(params, doseq=True)
        url = f"{base_url}{path}" + (f"?{query}" if query else "")
        request = urllib.request.Request(url, headers=headers, method=method.upper())
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                payload = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            payload = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Binance HTTP {exc.code}: {payload[:300]}") from exc
        return json.loads(payload)

    async def load_exchange_info(self) -> None:
        data = await self.request("GET", "/fapi/v1/exchangeInfo")
        filters: Dict[str, Dict[str, float]] = {}
        for symbol_info in data.get("symbols", []):
            if symbol_info.get("quoteAsset") != QUOTE_ASSET:
                continue
            if symbol_info.get("contractType") != "PERPETUAL":
                continue
            if symbol_info.get("status") != "TRADING":
                continue
            row = {"stepSize": 0.001, "minQty": 0.0, "minNotional": 5.0}
            for item in symbol_info.get("filters", []):
                if item.get("filterType") == "LOT_SIZE":
                    row["stepSize"] = float(item.get("stepSize", row["stepSize"]))
                    row["minQty"] = float(item.get("minQty", row["minQty"]))
                if item.get("filterType") == "MIN_NOTIONAL":
                    row["minNotional"] = float(item.get("notional", row["minNotional"]))
            filters[symbol_info["symbol"]] = row
        self.exchange_filters = filters

    def _ticker_rows(self, data: list, *, market: str) -> List[dict]:
        winners = []
        for item in data:
            symbol = item.get("symbol", "")
            if not symbol.endswith(QUOTE_ASSET):
                continue
            try:
                change = float(item.get("priceChangePercent", 0.0))
                last = float(item.get("lastPrice", 0.0))
                quote_volume = float(item.get("quoteVolume", item.get("volume", 0.0)))
            except (TypeError, ValueError):
                continue
            row = self._build_ticker_row(symbol, change, last, quote_volume, market=market)
            if row:
                winners.append(row)
        return winners

    def _build_ticker_row(self, symbol: str, change: float, price: float, quote_volume: float, *, market: str) -> Optional[dict]:
        if not symbol.endswith(QUOTE_ASSET):
            return None
        if price <= 0 or change < MIN_GAIN_TO_SHOW:
            return None
        is_futures = symbol in self.exchange_filters if self.exchange_filters else market == "futures"
        if market == "futures" and not is_futures:
            return None
        return {
            "symbol": symbol,
            "change": change,
            "price": price,
            "quoteVolume": quote_volume,
            "market": market,
            "is_futures": is_futures,
            "can_short": is_futures and market == "futures",
        }

    def winners_from_ws_tickers(self, tickers: Dict[str, dict]) -> List[dict]:
        """Construye la lista de ganadores usando solo el cache alimentado por WebSocket."""
        winners: List[dict] = []
        for symbol, item in tickers.items():
            try:
                change = float(item.get("change", 0.0))
                price = float(item.get("price", 0.0))
                quote_volume = float(item.get("quoteVolume", 0.0))
            except (TypeError, ValueError):
                continue
            row = self._build_ticker_row(symbol, change, price, quote_volume, market="futures")
            if row:
                winners.append(row)
        winners.sort(key=lambda row: row["change"], reverse=True)
        self.last_warning = ""
        return winners[:MAX_SYMBOLS]

    async def winners_24h(self) -> List[dict]:
        """Fallback manual por REST. El bot operativo no usa este método para escanear."""
        winners: List[dict] = []
        warnings: List[str] = []

        try:
            futures_data = await self.request("GET", "/fapi/v1/ticker/24hr")
            winners.extend(self._ticker_rows(futures_data, market="futures"))
        except Exception as exc:
            warnings.append(f"Futures ticker no disponible: {exc}")

        if INCLUDE_SPOT_WINNERS:
            try:
                spot_data = await self.public_request(SPOT_BASE_URL, "/api/v3/ticker/24hr")
                seen_futures = {row["symbol"] for row in winners}
                for row in self._ticker_rows(spot_data, market="spot"):
                    if row["symbol"] not in seen_futures:
                        winners.append(row)
            except Exception as exc:
                warnings.append(f"Spot ticker no disponible: {exc}")

        self.last_warning = " | ".join(warnings)
        if not winners and warnings:
            raise RuntimeError(self.last_warning)

        winners.sort(key=lambda row: row["change"], reverse=True)
        return winners[:MAX_SYMBOLS]

    def normalize_qty(self, symbol: str, qty: float) -> float:
        info = self.exchange_filters.get(symbol, {"stepSize": 0.001, "minQty": 0.0})
        step = info["stepSize"]
        normalized = floor(qty / step) * step
        decimals = max(0, len(f"{step:.12f}".rstrip("0").split(".")[-1]))
        normalized = round(normalized, decimals)
        if normalized < info.get("minQty", 0.0):
            return 0.0
        return normalized

    async def set_leverage(self, symbol: str) -> None:
        if LEVERAGE > 0 and LIVE_TRADING and not PAPER_MODE:
            await self.request("POST", "/fapi/v1/leverage", {"symbol": symbol, "leverage": LEVERAGE}, signed=True)

    async def market_short(self, symbol: str, notional: float, price: float) -> float:
        min_notional = self.exchange_filters.get(symbol, {}).get("minNotional", 5.0)
        order_notional = max(notional, min_notional)
        qty = self.normalize_qty(symbol, order_notional / price)
        if qty <= 0:
            raise RuntimeError(f"Cantidad inválida para {symbol}: notional={order_notional}, price={price}")
        if PAPER_MODE or not LIVE_TRADING:
            return qty
        await self.set_leverage(symbol)
        await self.request(
            "POST",
            "/fapi/v1/order",
            {"symbol": symbol, "side": "SELL", "type": "MARKET", "quantity": qty},
            signed=True,
        )
        return qty

    async def close_short(self, symbol: str, qty: float) -> None:
        qty = self.normalize_qty(symbol, qty)
        if qty <= 0:
            return
        if PAPER_MODE or not LIVE_TRADING:
            return
        await self.request(
            "POST",
            "/fapi/v1/order",
            {"symbol": symbol, "side": "BUY", "type": "MARKET", "quantity": qty, "reduceOnly": "true"},
            signed=True,
        )


class TradingBot:
    def __init__(self) -> None:
        self.client = BinanceFuturesClient()
        self.positions: Dict[str, BotPosition] = {}
        self.prices: Dict[str, float] = {}
        self.changes: Dict[str, float] = {}
        self.ticker_cache: Dict[str, dict] = {}
        self.tracked_symbols: set[str] = set()
        self.winners: List[dict] = []
        self.closed_trades: List[dict] = []
        self.events: List[str] = []
        self.lock = threading.Lock()
        self.running = False
        self.last_scan_at = 0.0
        self.last_error = ""
        self.last_startup_error = ""
        self.last_data_warning = ""
        self.scan_count = 0
        self.websocket_messages = 0
        self.exchange_symbols_count = 0
        self.started_at = time.time()
        self.state_file = STATE_FILE
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.thread: Optional[threading.Thread] = None

    def log(self, message: str) -> None:
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        line = f"{stamp} | {message}"
        print(line, flush=True)
        with self.lock:
            self.events = [line, *self.events[:99]]

    def start(self) -> None:
        if self.running:
            return
        self.running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True, name="TradingBotLoop")
        self.thread.start()

    def _run_loop(self) -> None:
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self._main())
        except Exception as exc:
            self.running = False
            self.last_error = str(exc)
            self.log(f"Bot detenido por error no controlado: {exc}")

    async def _main(self) -> None:
        self.log("Arrancando bot en modo PAPER" if PAPER_MODE or not LIVE_TRADING else "Arrancando bot en modo REAL")
        try:
            await self.client.start()
            self.exchange_symbols_count = len(self.client.exchange_filters)
            self.log(f"ExchangeInfo cargado: {self.exchange_symbols_count} contratos futures {QUOTE_ASSET}")
        except Exception as exc:
            self.last_startup_error = str(exc)
            self.last_error = str(exc)
            self.log(f"No se pudo cargar exchangeInfo ({exc}). Continúo con filtros mínimos para mostrar ganadores.")
        await asyncio.gather(self._price_ws(), self._scanner())

    async def _price_ws(self) -> None:
        while self.running:
            try:
                async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=10) as ws:
                    self.log("WebSocket de precios conectado")
                    while self.running:
                        raw = await ws.recv()
                        payload = json.loads(raw)
                        if isinstance(payload, dict) and isinstance(payload.get("data"), list):
                            payload = payload["data"]
                        if not isinstance(payload, list):
                            continue
                        now = time.time()
                        with self.lock:
                            for item in payload:
                                symbol = item.get("s")
                                if self.client.exchange_filters and symbol not in self.client.exchange_filters:
                                    continue
                                if self.tracked_symbols and symbol not in self.tracked_symbols:
                                    continue
                                price = float(item.get("c", 0.0))
                                change = float(item.get("P", 0.0))
                                quote_volume = float(item.get("q", 0.0))
                                if price > 0:
                                    self.prices[symbol] = price
                                    self.changes[symbol] = change
                                    self.ticker_cache[symbol] = {
                                        "price": price,
                                        "change": change,
                                        "quoteVolume": quote_volume,
                                        "updated_at": now,
                                        "source": "websocket",
                                    }
                                    self.websocket_messages += 1
            except Exception as exc:
                self.last_error = str(exc)
                self.log(f"WebSocket desconectado: {exc}; reconectando")
                await asyncio.sleep(5)

    async def _scanner(self) -> None:
        while self.running:
            try:
                with self.lock:
                    tickers = dict(self.ticker_cache)

                winners = self.client.winners_from_ws_tickers(tickers)
                tradable_winners = [row for row in winners if row.get("can_short", True)]
                high_gain = [row for row in winners if row["change"] >= ENTRY_LEVELS[0]]
                now = time.time()
                with self.lock:
                    self.winners = winners
                    self.scan_count += 1
                    self.tracked_symbols = {row["symbol"] for row in winners} | set(self.positions.keys())
                    for row in winners:
                        symbol = row["symbol"]
                        self.prices[symbol] = row["price"]
                        self.changes[symbol] = row["change"]
                        self.ticker_cache[symbol] = {
                            "price": row["price"],
                            "change": row["change"],
                            "quoteVolume": row.get("quoteVolume", 0.0),
                            "updated_at": now,
                            "source": "rest_minute_scan",
                        }
                    self.last_scan_at = now

                if self.client.last_warning and self.client.last_warning != self.last_data_warning:
                    self.last_data_warning = self.client.last_warning
                    self.last_error = self.client.last_warning
                    self.log(f"Advertencia de datos: {self.client.last_warning}")
                elif not self.client.last_warning:
                    self.last_data_warning = ""

                top_text = f"{winners[0]['symbol']} {winners[0]['change']:.2f}% ({winners[0].get('market', 'futures')})" if winners else "sin ganadores"
                self.log(f"Escaneo REST minuto #{self.scan_count}: {len(winners)} símbolos mostrados, {len(high_gain)} >= {ENTRY_LEVELS[0]:.0f}%, shorteables={len(tradable_winners)}, top: {top_text}")
                self.persist_state()
                await self._apply_strategy(tradable_winners)
                self.persist_state()
            except Exception as exc:
                self.last_error = str(exc)
                self.log(f"Error en escaneo REST minuto: {exc}")
            await asyncio.sleep(SCAN_INTERVAL_SECONDS)

    async def _apply_strategy(self, winners: List[dict]) -> None:
        for row in winners:
            if not row.get("can_short", True):
                continue
            symbol = row["symbol"]
            change = row["change"]
            price = self.prices.get(symbol, row["price"])
            if price <= 0:
                continue
            for level, notional in zip(ENTRY_LEVELS, ENTRY_NOTIONALS):
                if change >= level:
                    await self._ensure_short(symbol, level, notional, price, change)
            await self._maybe_take_profit(symbol, price)

        with self.lock:
            symbols = list(self.positions.keys())
        for symbol in symbols:
            price = self.prices.get(symbol)
            if price:
                await self._maybe_take_profit(symbol, price)

    async def _ensure_short(self, symbol: str, level: float, notional: float, price: float, change: float) -> None:
        with self.lock:
            position = self.positions.setdefault(symbol, BotPosition(symbol=symbol))
            if level in position.opened_levels() or position.status != "OPEN":
                return
        qty = await self.client.market_short(symbol, notional, price)
        fill = Fill(level=level, notional=notional, entry_price=price, qty=qty)
        with self.lock:
            self.positions[symbol].fills.append(fill)
        self.log(f"SHORT {symbol}: nivel {level:.0f}% activado con {notional:.2f} USDT, qty={qty}, precio={price}, cambio={change:.2f}%")
        self.persist_state()

    async def _maybe_take_profit(self, symbol: str, price: float) -> None:
        with self.lock:
            position = self.positions.get(symbol)
            if not position or position.status != "OPEN" or not position.fills:
                return
            pnl = position.unrealized_pnl(price)
            target = position.notional * TAKE_PROFIT_FRACTION
            qty = position.qty
            avg_entry = position.avg_entry
            notional = position.notional
        if pnl < target:
            return
        await self.client.close_short(symbol, qty)
        with self.lock:
            position = self.positions.pop(symbol, None)
            if position:
                position.status = "CLOSED"
                position.realized_pnl = pnl
                self.closed_trades.insert(0, {
                    "symbol": symbol,
                    "pnl": pnl,
                    "target": target,
                    "qty": qty,
                    "avg_entry": avg_entry,
                    "close_price": price,
                    "notional": notional,
                    "closed_at": datetime.now(timezone.utc).isoformat(),
                })
                self.closed_trades = self.closed_trades[:100]
        self.log(f"CIERRE {symbol}: PnL={pnl:.4f} USDT, objetivo={target:.4f}, precio cierre={price}")
        self.persist_state()

    def _current_snapshot_unlocked(self) -> dict:
        open_positions = []
        total_unrealized = 0.0
        total_notional = 0.0
        for symbol, position in self.positions.items():
            price = self.prices.get(symbol, 0.0)
            pnl = position.unrealized_pnl(price)
            total_unrealized += pnl
            total_notional += position.notional
            open_positions.append({
                "symbol": symbol,
                "mark_price": price,
                "change": self.changes.get(symbol, 0.0),
                "avg_entry": position.avg_entry,
                "qty": position.qty,
                "notional": position.notional,
                "target": position.notional * TAKE_PROFIT_FRACTION,
                "unrealized_pnl": pnl,
                "fills": [fill.__dict__ for fill in position.fills],
            })
        return {
            "mode": "PAPER" if PAPER_MODE or not LIVE_TRADING else "REAL",
            "running": self.running,
            "thread_alive": bool(self.thread and self.thread.is_alive()),
            "started_at": self.started_at,
            "uptime_seconds": round(time.time() - self.started_at, 1),
            "last_scan_at": self.last_scan_at,
            "last_scan_text": datetime.fromtimestamp(self.last_scan_at, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC") if self.last_scan_at else "pendiente",
            "last_error": self.last_error,
            "last_startup_error": self.last_startup_error,
            "last_data_warning": self.last_data_warning,
            "scan_count": self.scan_count,
            "websocket_messages": self.websocket_messages,
            "websocket_tickers_count": len(self.ticker_cache),
            "tracked_symbols_count": len(self.tracked_symbols),
            "exchange_symbols_count": self.exchange_symbols_count,
            "min_gain_to_show": MIN_GAIN_TO_SHOW,
            "include_spot_winners": INCLUDE_SPOT_WINNERS,
            "entry_levels": ENTRY_LEVELS,
            "entry_notionals": ENTRY_NOTIONALS,
            "take_profit_fraction": TAKE_PROFIT_FRACTION,
            "total_unrealized": total_unrealized,
            "total_notional": total_notional,
            "winners": list(self.winners[:30]),
            "positions": open_positions,
            "closed_trades": list(self.closed_trades[:30]),
            "events": list(self.events),
            "state_source": "memory",
            "state_file": self.state_file,
        }

    def _load_persisted_snapshot(self) -> Optional[dict]:
        try:
            if not os.path.exists(self.state_file):
                return None
            with open(self.state_file, "r", encoding="utf-8") as handle:
                data = json.load(handle)
            if not isinstance(data, dict):
                return None
            data["state_source"] = "persisted"
            return data
        except Exception as exc:
            self.last_error = f"No pude leer estado persistido: {exc}"
            return None

    def persist_state(self) -> None:
        with self.lock:
            data = self._current_snapshot_unlocked()
        # Evita reemplazar un estado útil por un arranque vacío.
        if data["scan_count"] <= 0 and not data["positions"] and not data["winners"]:
            return
        tmp_path = f"{self.state_file}.tmp"
        try:
            with open(tmp_path, "w", encoding="utf-8") as handle:
                json.dump(data, handle, ensure_ascii=False)
            os.replace(tmp_path, self.state_file)
        except Exception as exc:
            self.last_error = f"No pude persistir estado: {exc}"

    def snapshot(self) -> dict:
        with self.lock:
            data = self._current_snapshot_unlocked()
        persisted = self._load_persisted_snapshot()
        if not persisted:
            return data

        current_scan_at = float(data.get("last_scan_at") or 0.0)
        persisted_scan_at = float(persisted.get("last_scan_at") or 0.0)
        if persisted_scan_at > current_scan_at:
            return persisted
        if current_scan_at <= 0 and not data.get("positions") and persisted.get("positions"):
            return persisted
        return data


bot = TradingBot()
bot.start()
app = Flask(__name__)


HTML = """
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="15">
  <title>Bot Short Ganadores Binance</title>
  <style>
    body { margin: 0; font-family: Arial, sans-serif; background: #0f172a; color: #e2e8f0; }
    header { padding: 24px; background: #111827; border-bottom: 1px solid #334155; }
    main { padding: 20px; display: grid; gap: 18px; }
    .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: 14px; }
    .card, table, pre { background: #111827; border: 1px solid #334155; border-radius: 12px; }
    .card { padding: 16px; }
    .label { color: #94a3b8; font-size: 13px; }
    .value { font-size: 26px; font-weight: 700; margin-top: 6px; }
    .positive { color: #22c55e; } .negative { color: #ef4444; } .warn { color: #f59e0b; }
    table { width: 100%; border-collapse: collapse; overflow: hidden; }
    th, td { padding: 10px 12px; border-bottom: 1px solid #1f2937; text-align: right; }
    th:first-child, td:first-child { text-align: left; }
    h2 { margin: 8px 0; }
    pre { padding: 14px; overflow: auto; max-height: 300px; white-space: pre-wrap; }
    .pill { display: inline-block; padding: 4px 10px; border-radius: 999px; background: #1e293b; border: 1px solid #475569; }
  </style>
</head>
<body>
<header>
  <h1>Bot Short de Símbolos Ganadores Binance Futures</h1>
  <p>Abre shorts al superar +50% y agrega tramos hasta +250%. Cierra cuando el PnL llega al 50% del notional.</p>
</header>
<main>
  <section class="cards">
    <div class="card"><div class="label">Modo</div><div id="mode" class="value warn">{{ snapshot.mode }}</div></div>
    <div class="card"><div class="label">PnL no realizado</div><div id="pnl" class="value {{ 'positive' if snapshot.total_unrealized >= 0 else 'negative' }}">{{ "%.4f"|format(snapshot.total_unrealized) }} USDT</div></div>
    <div class="card"><div class="label">Capital en posiciones</div><div id="notional" class="value">{{ "%.4f"|format(snapshot.total_notional) }} USDT</div></div>
    <div class="card"><div class="label">Último escaneo</div><div id="scan" class="value" style="font-size:16px">{{ snapshot.last_scan_text }}</div></div>
    <div class="card"><div class="label">Escaneos</div><div id="scanCount" class="value">{{ snapshot.scan_count }}</div></div>
    <div class="card"><div class="label">Contratos futures cargados</div><div id="contracts" class="value">{{ snapshot.exchange_symbols_count }}</div></div>
    <div class="card"><div class="label">Mensajes WebSocket</div><div id="wsMessages" class="value">{{ snapshot.websocket_messages }}</div></div>
    <div class="card"><div class="label">Fuente estado</div><div id="stateSource" class="value" style="font-size:16px">{{ snapshot.state_source }}</div></div>
    <div class="card"><div class="label">Thread bot</div><div id="threadAlive" class="value" style="font-size:16px">{{ 'vivo' if snapshot.thread_alive else 'muerto' }}</div></div>
  </section>

  <section id="errorBox" class="card" style="display:{{ 'block' if (snapshot.last_error or snapshot.last_data_warning or snapshot.last_startup_error) else 'none' }}">
    <div class="label">Último error / diagnóstico</div>
    <div id="lastError" class="value negative" style="font-size:16px; word-break: break-word">{{ snapshot.last_error or snapshot.last_data_warning or snapshot.last_startup_error }}</div>
  </section>

  <section>
    <h2>Posiciones abiertas</h2>
    <table><thead><tr><th>Símbolo</th><th>Cambio 24h</th><th>Entrada media</th><th>Precio</th><th>Notional</th><th>Objetivo</th><th>PnL</th><th>Tramos</th></tr></thead><tbody id="positions">
      {% for p in snapshot.positions %}
      <tr><td>{{ p.symbol }}</td><td>{{ "%.2f"|format(p.change) }}%</td><td>{{ "%.8f"|format(p.avg_entry) }}</td><td>{{ "%.8f"|format(p.mark_price) }}</td><td>{{ "%.4f"|format(p.notional) }} USDT</td><td>{{ "%.4f"|format(p.target) }} USDT</td><td class="{{ 'positive' if p.unrealized_pnl >= 0 else 'negative' }}">{{ "%.4f"|format(p.unrealized_pnl) }} USDT</td><td>{% for f in p.fills %}<span class="pill">+{{ "%.0f"|format(f.level) }}% / {{ "%.2f"|format(f.notional) }}</span> {% endfor %}</td></tr>
      {% else %}
      <tr><td colspan="8">Sin posiciones abiertas</td></tr>
      {% endfor %}
    </tbody></table>
  </section>

  <section>
    <h2>Ganadores detectados</h2>
    <table><thead><tr><th>Símbolo</th><th>Mercado</th><th>Short</th><th>Cambio 24h</th><th>Precio</th><th>Volumen quote</th></tr></thead><tbody id="winners">
      {% for w in snapshot.winners %}
      <tr><td>{{ w.symbol }}</td><td>{{ w.market or 'futures' }}</td><td>{{ 'sí' if w.can_short else 'no' }}</td><td class="{{ 'positive' if w.change >= 0 else 'negative' }}">{{ "%.2f"|format(w.change) }}%</td><td>{{ "%.8f"|format(w.price) }}</td><td>{{ "%.2f"|format(w.quoteVolume) }}</td></tr>
      {% else %}
      <tr><td colspan="6">No hay símbolos para mostrar todavía. Revisa el bloque de errores y eventos.</td></tr>
      {% endfor %}
    </tbody></table>
  </section>

  <section>
    <h2>Operaciones cerradas</h2>
    <table><thead><tr><th>Símbolo</th><th>PnL</th><th>Objetivo</th><th>Entrada media</th><th>Cierre</th><th>Fecha</th></tr></thead><tbody id="closed">
      {% for t in snapshot.closed_trades %}
      <tr><td>{{ t.symbol }}</td><td class="positive">{{ "%.4f"|format(t.pnl) }} USDT</td><td>{{ "%.4f"|format(t.target) }} USDT</td><td>{{ "%.8f"|format(t.avg_entry) }}</td><td>{{ "%.8f"|format(t.close_price) }}</td><td>{{ t.closed_at }}</td></tr>
      {% else %}
      <tr><td colspan="6">Sin cierres todavía</td></tr>
      {% endfor %}
    </tbody></table>
  </section>

  <section>
    <h2>Eventos</h2>
    <pre id="events">{{ snapshot.events|join("\n") }}</pre>
  </section>

  <section>
    <h2>Estado API crudo</h2>
    <pre id="rawStatus">{{ initial_status_json }}</pre>
  </section>
</main>
<script id="initial-status" type="application/json">{{ initial_status_json | safe }}</script>
<script>
function num(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}
function fixed(value, decimals = 8) { return num(value).toFixed(decimals); }
function money(value) { return fixed(value, 4) + ' USDT'; }
function pct(value) { return fixed(value, 2) + '%'; }
function cls(value) { return num(value) >= 0 ? 'positive' : 'negative'; }
function rowsOrFallback(rows, fallback) { return rows.length ? rows.join('') : fallback; }
function renderStatus(data) {
  data = data || {};
  const positions = Array.isArray(data.positions) ? data.positions : [];
  const winners = Array.isArray(data.winners) ? data.winners : [];
  const closedTrades = Array.isArray(data.closed_trades) ? data.closed_trades : [];
  const events = Array.isArray(data.events) ? data.events : [];

  document.getElementById('mode').textContent = data.mode || 'sin datos';
  document.getElementById('pnl').textContent = money(data.total_unrealized);
  document.getElementById('pnl').className = 'value ' + cls(data.total_unrealized);
  document.getElementById('notional').textContent = money(data.total_notional);
  document.getElementById('scan').textContent = data.last_scan_text || 'pendiente';
  document.getElementById('scanCount').textContent = num(data.scan_count);
  document.getElementById('contracts').textContent = num(data.exchange_symbols_count);
  document.getElementById('wsMessages').textContent = num(data.websocket_messages);
  document.getElementById('stateSource').textContent = data.state_source || 'memory';
  document.getElementById('threadAlive').textContent = data.thread_alive ? 'vivo' : 'muerto';
  const errorText = data.last_error || data.last_data_warning || data.last_startup_error || '';
  document.getElementById('errorBox').style.display = errorText ? 'block' : 'none';
  document.getElementById('lastError').textContent = errorText;

  document.getElementById('positions').innerHTML = rowsOrFallback(positions.map(p => `
    <tr><td>${p.symbol || ''}</td><td>${pct(p.change)}</td><td>${fixed(p.avg_entry)}</td><td>${fixed(p.mark_price)}</td><td>${money(p.notional)}</td><td>${money(p.target)}</td><td class="${cls(p.unrealized_pnl)}">${money(p.unrealized_pnl)}</td><td>${(Array.isArray(p.fills) ? p.fills : []).map(f => '<span class="pill">+' + fixed(f.level, 0) + '% / ' + fixed(f.notional, 2) + '</span>').join(' ')}</td></tr>
  `), '<tr><td colspan="8">Sin posiciones abiertas</td></tr>');

  document.getElementById('winners').innerHTML = rowsOrFallback(winners.map(w => `
    <tr><td>${w.symbol || ''}</td><td>${w.market || 'futures'}</td><td>${w.can_short ? 'sí' : 'no'}</td><td class="${cls(w.change)}">${pct(w.change)}</td><td>${fixed(w.price)}</td><td>${num(w.quoteVolume).toLocaleString()}</td></tr>
  `), '<tr><td colspan="6">No hay símbolos para mostrar todavía. Revisa el bloque de errores y eventos.</td></tr>');

  document.getElementById('closed').innerHTML = rowsOrFallback(closedTrades.map(t => `
    <tr><td>${t.symbol || ''}</td><td class="positive">${money(t.pnl)}</td><td>${money(t.target)}</td><td>${fixed(t.avg_entry)}</td><td>${fixed(t.close_price)}</td><td>${t.closed_at || ''}</td></tr>
  `), '<tr><td colspan="6">Sin cierres todavía</td></tr>');
  document.getElementById('events').textContent = events.join('\n');
  document.getElementById('rawStatus').textContent = JSON.stringify(data, null, 2);
}
function renderClientError(error) {
  const message = 'Error de la página consultando /api/status: ' + error.message;
  document.getElementById('errorBox').style.display = 'block';
  document.getElementById('lastError').textContent = message;
  document.getElementById('events').textContent = message + '\n' + document.getElementById('events').textContent;
}
async function refresh() {
  try {
    const response = await fetch('/api/status', {cache: 'no-store'});
    if (!response.ok) {
      throw new Error('HTTP ' + response.status);
    }
    renderStatus(await response.json());
  } catch (error) {
    renderClientError(error);
  }
}
try {
  renderStatus(JSON.parse(document.getElementById('initial-status').textContent || '{}'));
} catch (error) {
  renderClientError(error);
}
refresh();
setInterval(refresh, 3000);
</script>
</body>
</html>
"""


@app.get("/")
def index():
    snapshot = bot.snapshot()
    initial_status_json = json.dumps(snapshot, ensure_ascii=False).replace("</", "<\\/")
    response = make_response(render_template_string(
        HTML,
        snapshot=snapshot,
        initial_status_json=initial_status_json,
    ))
    response.headers["Cache-Control"] = "no-store, max-age=0"
    return response


@app.get("/api/status")
def status():
    response = jsonify(bot.snapshot())
    response.headers["Cache-Control"] = "no-store, max-age=0"
    return response


@app.get("/health")
def health():
    snapshot = bot.snapshot()
    return {"ok": True, "running": bot.running, "mode": snapshot["mode"], "last_error": snapshot["last_error"], "scan_count": snapshot["scan_count"]}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
