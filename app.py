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
from dataclasses import dataclass, field
from datetime import datetime, timezone
from math import floor
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import urllib.error
import urllib.request

import websockets
from flask import Flask, jsonify, render_template_string


BASE_URL = os.getenv("BASE_URL", "https://fapi.binance.com")
WS_URL = os.getenv("WS_URL", "wss://fstream.binance.com/ws/!ticker@arr")
SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "20"))
PAPER_MODE = os.getenv("PAPER_MODE", "true").lower() == "true"
LIVE_TRADING = os.getenv("LIVE_TRADING", "false").lower() == "true"
API_KEY = os.getenv("BINANCE_API_KEY", "")
API_SECRET = os.getenv("BINANCE_API_SECRET", "")
QUOTE_ASSET = os.getenv("QUOTE_ASSET", "USDT")
MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "120"))
LEVERAGE = int(os.getenv("LEVERAGE", "1"))

ENTRY_LEVELS = [float(x) for x in os.getenv("ENTRY_LEVELS", "50,75,100,150,200,250").split(",")]
ENTRY_NOTIONALS = [float(x) for x in os.getenv("ENTRY_NOTIONALS", "5,5,10,20,40,80").split(",")]
TAKE_PROFIT_FRACTION = float(os.getenv("TAKE_PROFIT_FRACTION", "0.5"))


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

    async def start(self) -> None:
        await self.load_exchange_info()

    async def close(self) -> None:
        return None

    async def request(self, method: str, path: str, params: Optional[dict] = None, signed: bool = False) -> Any:
        return await asyncio.to_thread(self._request_sync, method, path, params, signed)

    def _request_sync(self, method: str, path: str, params: Optional[dict] = None, signed: bool = False) -> Any:
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
        url = f"{BASE_URL}{path}" + (f"?{query}" if query else "")
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

    async def winners_24h(self) -> List[dict]:
        data = await self.request("GET", "/fapi/v1/ticker/24hr")
        winners = []
        for item in data:
            symbol = item.get("symbol", "")
            if symbol not in self.exchange_filters:
                continue
            try:
                change = float(item.get("priceChangePercent", 0.0))
                last = float(item.get("lastPrice", 0.0))
                quote_volume = float(item.get("quoteVolume", 0.0))
            except (TypeError, ValueError):
                continue
            if symbol.endswith(QUOTE_ASSET) and last > 0:
                winners.append({"symbol": symbol, "change": change, "price": last, "quoteVolume": quote_volume})
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
        self.winners: List[dict] = []
        self.closed_trades: List[dict] = []
        self.events: List[str] = []
        self.lock = threading.Lock()
        self.running = False
        self.last_scan_at = 0.0
        self.last_error = ""
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.thread: Optional[threading.Thread] = None

    def log(self, message: str) -> None:
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        line = f"{stamp} | {message}"
        print(line)
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
        self.loop.run_until_complete(self._main())

    async def _main(self) -> None:
        await self.client.start()
        self.log("Bot iniciado en modo PAPER" if PAPER_MODE or not LIVE_TRADING else "Bot iniciado en modo REAL")
        await asyncio.gather(self._price_ws(), self._scanner())

    async def _price_ws(self) -> None:
        while self.running:
            try:
                async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=10) as ws:
                    self.log("WebSocket de precios conectado")
                    while self.running:
                        raw = await ws.recv()
                        payload = json.loads(raw)
                        if not isinstance(payload, list):
                            continue
                        with self.lock:
                            for item in payload:
                                symbol = item.get("s")
                                if symbol not in self.client.exchange_filters:
                                    continue
                                price = float(item.get("c", 0.0))
                                change = float(item.get("P", 0.0))
                                if price > 0:
                                    self.prices[symbol] = price
                                    self.changes[symbol] = change
            except Exception as exc:
                self.last_error = str(exc)
                self.log(f"WebSocket desconectado: {exc}; reconectando")
                await asyncio.sleep(5)

    async def _scanner(self) -> None:
        while self.running:
            try:
                winners = await self.client.winners_24h()
                with self.lock:
                    self.winners = winners
                    for row in winners:
                        self.prices[row["symbol"]] = row["price"]
                        self.changes[row["symbol"]] = row["change"]
                    self.last_scan_at = time.time()
                await self._apply_strategy(winners)
            except Exception as exc:
                self.last_error = str(exc)
                self.log(f"Error en escaneo: {exc}")
            await asyncio.sleep(SCAN_INTERVAL_SECONDS)

    async def _apply_strategy(self, winners: List[dict]) -> None:
        for row in winners:
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

    def snapshot(self) -> dict:
        with self.lock:
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
                "last_scan_at": self.last_scan_at,
                "last_scan_text": datetime.fromtimestamp(self.last_scan_at, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC") if self.last_scan_at else "pendiente",
                "last_error": self.last_error,
                "entry_levels": ENTRY_LEVELS,
                "entry_notionals": ENTRY_NOTIONALS,
                "take_profit_fraction": TAKE_PROFIT_FRACTION,
                "total_unrealized": total_unrealized,
                "total_notional": total_notional,
                "winners": list(self.winners[:30]),
                "positions": open_positions,
                "closed_trades": list(self.closed_trades[:30]),
                "events": list(self.events),
            }


bot = TradingBot()
bot.start()
app = Flask(__name__)


HTML = """
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
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
    <div class="card"><div class="label">Modo</div><div id="mode" class="value warn">...</div></div>
    <div class="card"><div class="label">PnL no realizado</div><div id="pnl" class="value">...</div></div>
    <div class="card"><div class="label">Capital en posiciones</div><div id="notional" class="value">...</div></div>
    <div class="card"><div class="label">Último escaneo</div><div id="scan" class="value" style="font-size:16px">...</div></div>
  </section>

  <section>
    <h2>Posiciones abiertas</h2>
    <table><thead><tr><th>Símbolo</th><th>Cambio 24h</th><th>Entrada media</th><th>Precio</th><th>Notional</th><th>Objetivo</th><th>PnL</th><th>Tramos</th></tr></thead><tbody id="positions"></tbody></table>
  </section>

  <section>
    <h2>Ganadores detectados</h2>
    <table><thead><tr><th>Símbolo</th><th>Cambio 24h</th><th>Precio</th><th>Volumen quote</th></tr></thead><tbody id="winners"></tbody></table>
  </section>

  <section>
    <h2>Operaciones cerradas</h2>
    <table><thead><tr><th>Símbolo</th><th>PnL</th><th>Objetivo</th><th>Entrada media</th><th>Cierre</th><th>Fecha</th></tr></thead><tbody id="closed"></tbody></table>
  </section>

  <section>
    <h2>Eventos</h2>
    <pre id="events"></pre>
  </section>
</main>
<script>
function money(value) { return Number(value || 0).toFixed(4) + ' USDT'; }
function pct(value) { return Number(value || 0).toFixed(2) + '%'; }
function cls(value) { return Number(value || 0) >= 0 ? 'positive' : 'negative'; }
async function refresh() {
  const response = await fetch('/api/status');
  const data = await response.json();
  document.getElementById('mode').textContent = data.mode;
  document.getElementById('pnl').textContent = money(data.total_unrealized);
  document.getElementById('pnl').className = 'value ' + cls(data.total_unrealized);
  document.getElementById('notional').textContent = money(data.total_notional);
  document.getElementById('scan').textContent = data.last_scan_text;
  document.getElementById('positions').innerHTML = data.positions.map(p => `
    <tr><td>${p.symbol}</td><td>${pct(p.change)}</td><td>${p.avg_entry.toFixed(8)}</td><td>${p.mark_price.toFixed(8)}</td><td>${money(p.notional)}</td><td>${money(p.target)}</td><td class="${cls(p.unrealized_pnl)}">${money(p.unrealized_pnl)}</td><td>${p.fills.map(f => '<span class="pill">+' + f.level + '% / ' + f.notional + '</span>').join(' ')}</td></tr>
  `).join('') || '<tr><td colspan="8">Sin posiciones abiertas</td></tr>';
  document.getElementById('winners').innerHTML = data.winners.map(w => `
    <tr><td>${w.symbol}</td><td class="${cls(w.change)}">${pct(w.change)}</td><td>${Number(w.price).toFixed(8)}</td><td>${Number(w.quoteVolume).toLocaleString()}</td></tr>
  `).join('');
  document.getElementById('closed').innerHTML = data.closed_trades.map(t => `
    <tr><td>${t.symbol}</td><td class="positive">${money(t.pnl)}</td><td>${money(t.target)}</td><td>${Number(t.avg_entry).toFixed(8)}</td><td>${Number(t.close_price).toFixed(8)}</td><td>${t.closed_at}</td></tr>
  `).join('') || '<tr><td colspan="6">Sin cierres todavía</td></tr>';
  document.getElementById('events').textContent = data.events.join('\n');
}
refresh();
setInterval(refresh, 3000);
</script>
</body>
</html>
"""


@app.get("/")
def index():
    return render_template_string(HTML)


@app.get("/api/status")
def status():
    return jsonify(bot.snapshot())


@app.get("/health")
def health():
    return {"ok": True, "running": bot.running, "mode": bot.snapshot()["mode"]}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
