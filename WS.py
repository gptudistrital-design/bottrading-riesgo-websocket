import asyncio
import websockets
import json
import threading
import time
import math
from datetime import datetime
from typing import NamedTuple
import os


# ── Helpers de módulo ──────────────────────────────────────────────────────

def _safe_float(d: dict, key: str, default: float = 0.0) -> float:
    try:
        return float(d.get(key, default))
    except (ValueError, TypeError):
        return default


class TickerData(NamedTuple):
    change_pct: float   # % cambio 24h  (campo 'P')
    change_abs: float   # cambio abs 24h (campo 'p')
    last_price: float   # último precio  (campo 'c')
    high_24h:   float   # máximo 24h     (campo 'h')
    low_24h:    float   # mínimo 24h     (campo 'l')
    volume_24h: float   # volumen base   (campo 'v')
    quote_vol:  float   # volumen cotizado(campo 'q')
    ts:         float   # timestamp UNIX


# Límite de Binance: 1024 streams por conexión combinada
_BINANCE_MAX_STREAMS = 1024


class SymbolWebSocketPriceCache:
    """WebSocket para múltiples símbolos usando el máximo de streams por conexión.

    Binance permite 1024 streams por conexión combinada.
    Con 570 símbolos:
      • 1 conexión para todos los @markPrice@1s  (570 streams)
      • 1 conexión para todos los @ticker         (570 streams)
    Total: 2 conexiones independientemente del número de símbolos (hasta 1024).

    Streams activos por símbolo:
      • @markPrice@1s  → precio mark en tiempo real
      • @ticker        → cambio 24h, high/low, volumen
    """

    _WS_MAX_SIZE  = 131_072   # 128 KB — con 570 símbolos los msgs siguen siendo < 2 KB c/u
    _WS_MAX_QUEUE = 64        # cola pequeña; se procesa en tiempo real

    def __init__(self, symbols: list[str], symbols_per_connection: int | None = None):
        # symbols_per_connection se mantiene por compatibilidad con código existente.
        # Ya no se usa: con el límite de 1024 streams de Binance, todos los símbolos
        # caben en 1 conexión por tipo de stream (markPrice + ticker = 2 conexiones total).
        if symbols_per_connection is not None:
            print(
                f"[WS] ⚠️  symbols_per_connection={symbols_per_connection} ignorado — "
                f"todos los símbolos van en 1 conexión por stream (límite Binance: 1024)."
            )

        self.symbols = [s.upper() for s in symbols]

        if len(self.symbols) > _BINANCE_MAX_STREAMS:
            # Si superas 1024 símbolos Binance rechaza la conexión;
            # en ese caso habría que dividir en 2 conexiones por tipo.
            raise ValueError(
                f"Binance permite máximo {_BINANCE_MAX_STREAMS} streams por conexión. "
                f"Tienes {len(self.symbols)} símbolos. "
                f"Separa en dos instancias o implementa chunking."
            )

        # price_cache:  symbol -> (mark_price, timestamp)
        self.price_cache:  dict[str, tuple[float, float]] = {}
        # ticker_cache: symbol -> TickerData (incluye ts)
        self.ticker_cache: dict[str, TickerData] = {}

        self.tasks:   list  = []
        self.lock           = threading.Lock()
        self.running        = False
        self._loop          = None

        # Estadísticas simples por stream (solo 2 keys: "markprice", "ticker")
        self.connection_stats: dict[str, dict] = {
            "markprice": {"reconnects": 0, "last_error": None},
            "ticker":    {"reconnects": 0, "last_error": None},
        }

    # ──────────────────────────────────────────────────────────────────────
    # Construcción de URLs
    # ──────────────────────────────────────────────────────────────────────

    def _build_url(self, stream_suffix: str) -> str:
        """URL combinada con todos los símbolos para un tipo de stream."""
        streams = "/".join(f"{s.lower()}{stream_suffix}" for s in self.symbols)
        return f"wss://fstream.binance.com/market/stream?streams={streams}"

    # ──────────────────────────────────────────────────────────────────────
    # Conexión WS compartida
    # ──────────────────────────────────────────────────────────────────────

    def _ws_connect(self, url: str):
        return websockets.connect(
            url,
            ping_interval=30,
            ping_timeout=10,
            close_timeout=10,
            max_size=self._WS_MAX_SIZE,
            max_queue=self._WS_MAX_QUEUE,
            compression=None,
        )

    async def _reconnect_loop(self, stream_id: str, coro_factory):
        """Bucle genérico de reconexión con backoff exponencial."""
        reconnect_delay    = 1
        consecutive_errors = 0

        while self.running:
            try:
                await coro_factory()
                reconnect_delay    = 1
                consecutive_errors = 0
            except Exception as e:
                consecutive_errors += 1
                self.connection_stats[stream_id]["reconnects"] += 1
                self.connection_stats[stream_id]["last_error"]  = str(e)
                reconnect_delay = min(reconnect_delay * 1.5, 30)
                if consecutive_errors > 5:
                    reconnect_delay = 60
                print(f"🔴 [{stream_id}] Error: {e}")
                print(f"   Reconectando en {reconnect_delay:.1f}s (intento #{consecutive_errors})")
                await asyncio.sleep(reconnect_delay)

    # ──────────────────────────────────────────────────────────────────────
    # Stream 1 – Mark Price (UNA sola conexión para TODOS los símbolos)
    # ──────────────────────────────────────────────────────────────────────

    async def _ws_markprice(self):
        url = self._build_url("@markPrice@1s")

        async def _connect():
            async with self._ws_connect(url) as ws:
                print(f"✅ [markPrice] Conectado — {len(self.symbols)} símbolos en 1 conexión")
                last_ping = time.time()

                while self.running:
                    try:
                        msg  = await asyncio.wait_for(ws.recv(), timeout=45)
                        data = json.loads(msg)

                        if "data" in data:
                            pd     = data["data"]
                            symbol = pd.get("s", "").upper()
                            price  = float(pd.get("p", 0.0))

                            if symbol and math.isfinite(price) and price > 0:
                                with self.lock:
                                    self.price_cache[symbol] = (price, time.time())

                        now = time.time()
                        if now - last_ping > 30:
                            await ws.ping()
                            last_ping = now

                    except asyncio.TimeoutError:
                        print("⏰ [markPrice] Timeout, enviando ping…")
                        await ws.ping()
                        last_ping = time.time()

                    except websockets.ConnectionClosed as e:
                        print(f"🔶 [markPrice] Conexión cerrada: {e}")
                        raise

        await self._reconnect_loop("markprice", _connect)

    # ──────────────────────────────────────────────────────────────────────
    # Stream 2 – Ticker 24h (UNA sola conexión para TODOS los símbolos)
    # ──────────────────────────────────────────────────────────────────────

    async def _ws_ticker(self):
        url = self._build_url("@ticker")

        async def _connect():
            async with self._ws_connect(url) as ws:
                print(f"✅ [ticker24h] Conectado — {len(self.symbols)} símbolos en 1 conexión")
                last_ping = time.time()

                while self.running:
                    try:
                        msg  = await asyncio.wait_for(ws.recv(), timeout=45)
                        data = json.loads(msg)

                        if "data" in data:
                            d      = data["data"]
                            symbol = d.get("s", "").upper()
                            if not symbol:
                                continue

                            with self.lock:
                                self.ticker_cache[symbol] = TickerData(
                                    change_pct = _safe_float(d, "P"),
                                    change_abs = _safe_float(d, "p"),
                                    last_price = _safe_float(d, "c"),
                                    high_24h   = _safe_float(d, "h"),
                                    low_24h    = _safe_float(d, "l"),
                                    volume_24h = _safe_float(d, "v"),
                                    quote_vol  = _safe_float(d, "q"),
                                    ts         = time.time(),
                                )

                        now = time.time()
                        if now - last_ping > 30:
                            await ws.ping()
                            last_ping = now

                    except asyncio.TimeoutError:
                        print("⏰ [ticker24h] Timeout, enviando ping…")
                        await ws.ping()
                        last_ping = time.time()

                    except websockets.ConnectionClosed as e:
                        print(f"🔶 [ticker24h] Conexión cerrada: {e}")
                        raise

        await self._reconnect_loop("ticker", _connect)

    # ──────────────────────────────────────────────────────────────────────
    # Monitor de salud
    # ──────────────────────────────────────────────────────────────────────

    async def _monitor_health(self):
        """Monitorea la salud de las 2 conexiones cada 60 segundos."""
        while self.running:
            await asyncio.sleep(60)
            now          = time.time()
            stale_price  = []
            stale_ticker = []

            with self.lock:
                for symbol in self.symbols:
                    entry = self.price_cache.get(symbol)
                    if entry is None or now - entry[1] > 120:
                        stale_price.append(symbol)

                    ticker = self.ticker_cache.get(symbol)
                    if ticker is None or now - ticker.ts > 120:
                        stale_ticker.append(symbol)

            if stale_price:
                print(f"⚠️ [markPrice] Sin actualización ({len(stale_price)} símbolos): "
                      f"{stale_price[:5]}{'…' if len(stale_price) > 5 else ''}")
            if stale_ticker:
                print(f"⚠️ [ticker24h] Sin actualización ({len(stale_ticker)} símbolos): "
                      f"{stale_ticker[:5]}{'…' if len(stale_ticker) > 5 else ''}")

    # ──────────────────────────────────────────────────────────────────────
    # Ciclo de vida
    # ──────────────────────────────────────────────────────────────────────

    def start(self):
        """Inicia las 2 conexiones WebSocket (markPrice + ticker 24h)."""
        self.running = True

        loop      = asyncio.new_event_loop()
        self._loop = loop

        threading.Thread(target=loop.run_forever, daemon=True).start()

        submit = lambda coro: self.tasks.append(
            asyncio.run_coroutine_threadsafe(coro, loop)
        )

        submit(self._ws_markprice())
        submit(self._ws_ticker())
        submit(self._monitor_health())

        print(f"✅ WebSocket cache iniciado — {len(self.symbols)} símbolos, 2 conexiones")

    def stop(self):
        """Detiene las conexiones."""
        print("🛑 Deteniendo WebSocket cache…")
        self.running = False
        time.sleep(2)

        for task in self.tasks:
            try:
                task.cancel()
            except Exception:
                pass

        if self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)

        print("✅ WebSocket cache detenido")

    # ──────────────────────────────────────────────────────────────────────
    # Getters – markPrice
    # ──────────────────────────────────────────────────────────────────────

    def get_price(self, symbol: str) -> float | None:
        with self.lock:
            entry = self.price_cache.get(symbol.upper())
            return entry[0] if entry else None

    def get_all_prices(self) -> dict[str, float]:
        with self.lock:
            return {sym: v[0] for sym, v in self.price_cache.items()}

    # ──────────────────────────────────────────────────────────────────────
    # Getters – Ticker 24h
    # ──────────────────────────────────────────────────────────────────────

    def get_change_24h(self, symbol: str) -> float | None:
        with self.lock:
            t = self.ticker_cache.get(symbol.upper())
            return t.change_pct if t else None

    def get_ticker(self, symbol: str) -> dict | None:
        """Ticker completo 24h: change_pct, change_abs, last_price,
        high_24h, low_24h, volume_24h, quote_vol. None si no hay datos."""
        with self.lock:
            t = self.ticker_cache.get(symbol.upper())
            if t is None:
                return None
            return {
                "change_pct": t.change_pct,
                "change_abs": t.change_abs,
                "last_price": t.last_price,
                "high_24h":   t.high_24h,
                "low_24h":    t.low_24h,
                "volume_24h": t.volume_24h,
                "quote_vol":  t.quote_vol,
            }

    def get_all_changes_24h(self) -> dict[str, float]:
        """% cambio 24h de todos los símbolos, ordenado mayor → menor."""
        with self.lock:
            result = {sym: t.change_pct for sym, t in self.ticker_cache.items()}
        return dict(sorted(result.items(), key=lambda x: x[1], reverse=True))

    def get_all_tickers(self) -> dict[str, dict]:
        with self.lock:
            return {
                sym: {
                    "change_pct": t.change_pct,
                    "change_abs": t.change_abs,
                    "last_price": t.last_price,
                    "high_24h":   t.high_24h,
                    "low_24h":    t.low_24h,
                    "volume_24h": t.volume_24h,
                    "quote_vol":  t.quote_vol,
                }
                for sym, t in self.ticker_cache.items()
            }

    # ──────────────────────────────────────────────────────────────────────
    # Utilidades
    # ──────────────────────────────────────────────────────────────────────

    def get_stale_symbols(self, max_age_seconds: int = 60) -> list[str]:
        """Símbolos cuyo markPrice no se ha actualizado en max_age_seconds."""
        now = time.time()
        with self.lock:
            return [
                s for s in self.symbols
                if now - self.price_cache.get(s, (0, 0))[1] > max_age_seconds
            ]

    def get_stats(self) -> dict:
        with self.lock:
            active_prices  = len(self.price_cache)
            active_tickers = len(self.ticker_cache)

        return {
            "total_symbols":    len(self.symbols),
            "active_prices":    active_prices,
            "active_tickers":   active_tickers,
            "stale_symbols":    len(self.get_stale_symbols()),
            "connection_stats": self.connection_stats,
        }


# ══════════════════════════════════════════════════════════════════════════
# Ejemplo de uso
# ══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Ejemplo con 40 símbolos — reemplaza con tus 570
    symbols = [
        "BTCUSDT",  "ETHUSDT",  "BNBUSDT",  "ADAUSDT",  "DOGEUSDT",
        "XRPUSDT",  "DOTUSDT",  "UNIUSDT",  "LINKUSDT", "LTCUSDT",
        "SOLUSDT",  "MATICUSDT","AVAXUSDT", "ATOMUSDT", "FILUSDT",
        "VETUSDT",  "TRXUSDT",  "ETCUSDT",  "XLMUSDT",  "THETAUSDT",
        "AAVEUSDT", "ALGOUSDT", "ICPUSDT",  "SHIBUSDT", "NEARUSDT",
        "LUNAUSDT", "AXSUSDT",  "SANDUSDT", "MANAUSDT", "GALAUSDT",
        "APEUSDT",  "GMTUSDT",  "OPUSDT",   "ARBUSDT",  "APTUSDT",
        "LDOUSDT",  "STXUSDT",  "IMXUSDT",  "INJUSDT",  "SUIUSDT",
    ]

    cache = SymbolWebSocketPriceCache(symbols)
    cache.start()

    print("⏳ Esperando datos iniciales…")
    time.sleep(3)

    try:
        while True:
            time.sleep(1)
            os.system("cls" if os.name == "nt" else "clear")

            now     = datetime.now().strftime("%H:%M:%S")
            changes = cache.get_all_changes_24h()

            col_w = 32
            print("=" * (col_w * 2 + 4))
            print(f"  📊 Cambio 24h – todos los símbolos ({now})")
            print("=" * (col_w * 2 + 4))
            print(f"  {'SÍMBOLO':<12} {'PRECIO':>14} {'24H %':>9}   "
                  f"{'SÍMBOLO':<12} {'PRECIO':>14} {'24H %':>9}")
            print("-" * (col_w * 2 + 4))

            items = list(changes.items())
            half  = (len(items) + 1) // 2

            for i in range(half):
                sym_l, pct_l = items[i]
                price_l      = cache.get_price(sym_l)
                price_str_l  = f"${price_l:.4f}" if price_l else "–"
                arrow_l      = "▲" if pct_l >= 0 else "▼"
                pct_str_l    = f"{arrow_l}{abs(pct_l):.2f}%"
                color_l      = "\033[92m" if pct_l >= 0 else "\033[91m"

                row = (f"  {color_l}{sym_l:<12}{'\033[0m'} "
                       f"{price_str_l:>14} "
                       f"{color_l}{pct_str_l:>9}{'\033[0m'}")

                if i + half < len(items):
                    sym_r, pct_r = items[i + half]
                    price_r      = cache.get_price(sym_r)
                    price_str_r  = f"${price_r:.4f}" if price_r else "–"
                    arrow_r      = "▲" if pct_r >= 0 else "▼"
                    pct_str_r    = f"{arrow_r}{abs(pct_r):.2f}%"
                    color_r      = "\033[92m" if pct_r >= 0 else "\033[91m"

                    row += (f"   {color_r}{sym_r:<12}{'\033[0m'} "
                            f"{price_str_r:>14} "
                            f"{color_r}{pct_str_r:>9}{'\033[0m'}")

                print(row)

            stats = cache.get_stats()
            print("=" * (col_w * 2 + 4))
            print(f"  📈 Precios activos : {stats['active_prices']}/{stats['total_symbols']}  |  "
                  f"Tickers 24h : {stats['active_tickers']}/{stats['total_symbols']}  |  "
                  f"Obsoletos : {stats['stale_symbols']}")

            stale = cache.get_stale_symbols(max_age_seconds=30)
            if stale:
                print(f"  ⚠️  Sin update (>30s): {stale[:6]}")

            print("=" * (col_w * 2 + 4))

    except KeyboardInterrupt:
        print("\n🛑 Deteniendo…")
        cache.stop()
        print("✅ Finalizado")
