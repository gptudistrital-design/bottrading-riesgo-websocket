import asyncio
import websockets
import json
import threading
import time
import numpy as np
from collections import defaultdict
from datetime import datetime
import os


class SymbolWebSocketPriceCache:
    """WebSocket optimizado para múltiples símbolos con conexiones agrupadas.
    
    Streams activos por símbolo:
      • @markPrice@1s  → precio mark en tiempo real
      • @ticker        → cambio 24h, high/low, volumen (actualización en tiempo real)
    """

    def __init__(self, symbols, symbols_per_connection=10):
        self.symbols = [s.upper() for s in symbols]
        self.symbols_per_connection = symbols_per_connection

        # ── Caché de precios mark ──────────────────────────────────────────
        self.price_cache: dict[str, float] = {}        # symbol -> mark price
        self.last_update: dict[str, float] = {}        # symbol -> timestamp

        # ── Caché de ticker 24h ───────────────────────────────────────────
        self.ticker_cache: dict[str, dict] = {}        # symbol -> ticker data
        self.ticker_last_update: dict[str, float] = {} # symbol -> timestamp

        self.tasks = []
        self.lock = threading.Lock()
        self.running = False
        self._loop = None
        self.connection_stats = defaultdict(lambda: {"reconnects": 0, "last_error": None})

    # ──────────────────────────────────────────────────────────────────────
    # Helpers internos
    # ──────────────────────────────────────────────────────────────────────

    def _create_symbol_groups(self):
        """Agrupa símbolos para conexiones multiplexadas."""
        groups = []
        for i in range(0, len(self.symbols), self.symbols_per_connection):
            groups.append(self.symbols[i:i + self.symbols_per_connection])
        return groups

    # ──────────────────────────────────────────────────────────────────────
    # Stream 1 – Mark Price (@markPrice@1s)
    # ──────────────────────────────────────────────────────────────────────

    async def _ws_combined_stream(self, symbols_group):
        """Stream combinado de mark-price para un grupo de símbolos."""
        streams = [f"{s.lower()}@markPrice@1s" for s in symbols_group]
        url = f"wss://fstream.binance.com/market/stream?streams={'/'.join(streams)}"

        group_id = f"markprice_{symbols_group[0][:3]}_{len(symbols_group)}"
        reconnect_delay = 1
        consecutive_errors = 0

        while self.running:
            try:
                async with websockets.connect(
                    url,
                    ping_interval=30,
                    ping_timeout=10,
                    close_timeout=10,
                    max_size=10 ** 7,
                    max_queue=2000,
                    compression=None,
                ) as ws:
                    print(f"✅ [markPrice] Conectado: {group_id} ({len(symbols_group)} símbolos)")
                    reconnect_delay = 1
                    consecutive_errors = 0
                    last_ping = time.time()

                    while self.running:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=45)
                            data = json.loads(msg)

                            if "data" in data:
                                price_data = data["data"]
                                symbol = price_data.get("s", "").upper()
                                price = float(price_data.get("p", 0.0))

                                if symbol and np.isfinite(price) and price > 0:
                                    with self.lock:
                                        self.price_cache[symbol] = price
                                        self.last_update[symbol] = time.time()

                            if time.time() - last_ping > 30:
                                await ws.ping()
                                last_ping = time.time()

                        except asyncio.TimeoutError:
                            print(f"⏰ Timeout en {group_id}, enviando ping…")
                            await ws.ping()
                            last_ping = time.time()

                        except websockets.ConnectionClosed as e:
                            print(f"🔶 Conexión cerrada para {group_id}: {e}")
                            raise

            except Exception as e:
                consecutive_errors += 1
                self.connection_stats[group_id]["reconnects"] += 1
                self.connection_stats[group_id]["last_error"] = str(e)
                reconnect_delay = min(reconnect_delay * 1.5, 30)
                if consecutive_errors > 5:
                    reconnect_delay = 60
                print(f"🔴 Error en {group_id}: {e}")
                print(f"   Reconectando en {reconnect_delay:.1f}s (intento #{consecutive_errors})")
                await asyncio.sleep(reconnect_delay)

    # ──────────────────────────────────────────────────────────────────────
    # Stream 2 – Ticker 24h (@ticker)   ← NUEVO
    # ──────────────────────────────────────────────────────────────────────

    async def _ws_combined_ticker_stream(self, symbols_group):
        """Stream combinado de ticker 24h para un grupo de símbolos.

        Campos capturados por símbolo:
          change_pct  – porcentaje de cambio en 24 h  (campo 'P' de Binance)
          change_abs  – cambio absoluto en 24 h        (campo 'p')
          last_price  – último precio                  (campo 'c')
          high_24h    – máximo 24 h                    (campo 'h')
          low_24h     – mínimo 24 h                    (campo 'l')
          volume_24h  – volumen base 24 h              (campo 'v')
          quote_vol   – volumen cotizado 24 h          (campo 'q')
        """
        streams = [f"{s.lower()}@ticker" for s in symbols_group]
        url = f"wss://fstream.binance.com/market/stream?streams={'/'.join(streams)}"

        group_id = f"ticker_{symbols_group[0][:3]}_{len(symbols_group)}"
        reconnect_delay = 1
        consecutive_errors = 0

        while self.running:
            try:
                async with websockets.connect(
                    url,
                    ping_interval=30,
                    ping_timeout=10,
                    close_timeout=10,
                    max_size=10 ** 7,
                    max_queue=2000,
                    compression=None,
                ) as ws:
                    print(f"✅ [ticker24h] Conectado: {group_id} ({len(symbols_group)} símbolos)")
                    reconnect_delay = 1
                    consecutive_errors = 0
                    last_ping = time.time()

                    while self.running:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=45)
                            data = json.loads(msg)

                            if "data" in data:
                                d = data["data"]
                                symbol = d.get("s", "").upper()

                                if not symbol:
                                    continue

                                # Extraer y validar campos numéricos
                                def safe_float(key, default=0.0):
                                    try:
                                        return float(d.get(key, default))
                                    except (ValueError, TypeError):
                                        return default

                                change_pct = safe_float("P")
                                change_abs = safe_float("p")
                                last_price  = safe_float("c")
                                high_24h    = safe_float("h")
                                low_24h     = safe_float("l")
                                volume_24h  = safe_float("v")
                                quote_vol   = safe_float("q")

                                with self.lock:
                                    self.ticker_cache[symbol] = {
                                        "change_pct": change_pct,   # % cambio 24h
                                        "change_abs": change_abs,   # cambio absoluto 24h
                                        "last_price": last_price,   # último precio
                                        "high_24h":   high_24h,     # máximo 24h
                                        "low_24h":    low_24h,      # mínimo 24h
                                        "volume_24h": volume_24h,   # volumen base 24h
                                        "quote_vol":  quote_vol,    # volumen cotizado 24h
                                    }
                                    self.ticker_last_update[symbol] = time.time()

                            if time.time() - last_ping > 30:
                                await ws.ping()
                                last_ping = time.time()

                        except asyncio.TimeoutError:
                            print(f"⏰ Timeout en {group_id}, enviando ping…")
                            await ws.ping()
                            last_ping = time.time()

                        except websockets.ConnectionClosed as e:
                            print(f"🔶 Conexión cerrada para {group_id}: {e}")
                            raise

            except Exception as e:
                consecutive_errors += 1
                self.connection_stats[group_id]["reconnects"] += 1
                self.connection_stats[group_id]["last_error"] = str(e)
                reconnect_delay = min(reconnect_delay * 1.5, 30)
                if consecutive_errors > 5:
                    reconnect_delay = 60
                print(f"🔴 Error en {group_id}: {e}")
                print(f"   Reconectando en {reconnect_delay:.1f}s (intento #{consecutive_errors})")
                await asyncio.sleep(reconnect_delay)

    # ──────────────────────────────────────────────────────────────────────
    # Fallback individual (markPrice)
    # ──────────────────────────────────────────────────────────────────────

    async def _ws_single_symbol(self, symbol):
        """Fallback para símbolos individuales si el stream combinado falla."""
        url = f"wss://fstream.binance.com/ws/{symbol.lower()}@markPrice"
        reconnect_delay = 1

        while self.running:
            try:
                async with websockets.connect(
                    url, ping_interval=30, ping_timeout=10, close_timeout=10
                ) as ws:
                    print(f"🟢 WS individual para {symbol}")
                    reconnect_delay = 1

                    while self.running:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=45)
                            data = json.loads(msg)
                            price = float(data.get("p", 0.0))

                            if np.isfinite(price) and price > 0:
                                with self.lock:
                                    self.price_cache[symbol] = price
                                    self.last_update[symbol] = time.time()

                        except asyncio.TimeoutError:
                            await ws.ping()

            except Exception as e:
                print(f"🔴 Error WS individual {symbol}: {e}")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)

    # ──────────────────────────────────────────────────────────────────────
    # Monitor de salud
    # ──────────────────────────────────────────────────────────────────────

    async def _monitor_health(self):
        """Monitorea la salud de las conexiones cada 60 segundos."""
        while self.running:
            await asyncio.sleep(60)
            current_time = time.time()
            stale_price  = []
            stale_ticker = []

            with self.lock:
                for symbol in self.symbols:
                    if current_time - self.last_update.get(symbol, 0) > 120:
                        stale_price.append(symbol)
                    if current_time - self.ticker_last_update.get(symbol, 0) > 120:
                        stale_ticker.append(symbol)

            if stale_price:
                print(f"⚠️ [markPrice] Sin actualización: {stale_price}")
            if stale_ticker:
                print(f"⚠️ [ticker24h]  Sin actualización: {stale_ticker}")

    # ──────────────────────────────────────────────────────────────────────
    # Ciclo de vida
    # ──────────────────────────────────────────────────────────────────────

    def start(self):
        """Inicia todas las conexiones WebSocket (markPrice + ticker 24h)."""
        self.running = True

        loop = asyncio.new_event_loop()
        self._loop = loop

        def run_loop():
            asyncio.set_event_loop(loop)
            loop.run_forever()

        thread = threading.Thread(target=run_loop, daemon=True)
        thread.start()

        symbol_groups = self._create_symbol_groups()
        print(f"📊 Iniciando streams para {len(self.symbols)} símbolos "
              f"en {len(symbol_groups)} grupo(s) de hasta {self.symbols_per_connection}")

        for group in symbol_groups:
            # Stream de precios mark
            self.tasks.append(
                asyncio.run_coroutine_threadsafe(self._ws_combined_stream(group), loop)
            )
            # Stream de ticker 24h ← NUEVO
            self.tasks.append(
                asyncio.run_coroutine_threadsafe(self._ws_combined_ticker_stream(group), loop)
            )

        # Monitor de salud
        self.tasks.append(
            asyncio.run_coroutine_threadsafe(self._monitor_health(), loop)
        )

        print("✅ WebSocket cache iniciado (markPrice + ticker 24h)")

    def stop(self):
        """Detiene todas las conexiones."""
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
        """Precio mark actual de un símbolo."""
        with self.lock:
            return self.price_cache.get(symbol.upper())

    def get_all_prices(self) -> dict:
        """Todos los precios mark actuales."""
        with self.lock:
            return self.price_cache.copy()

    # ──────────────────────────────────────────────────────────────────────
    # Getters – Ticker 24h   ← NUEVOS
    # ──────────────────────────────────────────────────────────────────────

    def get_change_24h(self, symbol: str) -> float | None:
        """Porcentaje de cambio en 24 h de un símbolo (p.ej. -2.35 o +4.10)."""
        with self.lock:
            ticker = self.ticker_cache.get(symbol.upper())
            return ticker["change_pct"] if ticker else None

    def get_ticker(self, symbol: str) -> dict | None:
        """Ticker completo 24h de un símbolo.
        
        Retorna dict con: change_pct, change_abs, last_price,
                          high_24h, low_24h, volume_24h, quote_vol
        O None si todavía no hay datos.
        """
        with self.lock:
            ticker = self.ticker_cache.get(symbol.upper())
            return dict(ticker) if ticker else None

    def get_all_changes_24h(self) -> dict[str, float]:
        """Porcentaje de cambio 24h de todos los símbolos disponibles.
        
        Retorna {symbol: change_pct, ...}, ordenado de mayor a menor cambio.
        """
        with self.lock:
            result = {
                sym: data["change_pct"]
                for sym, data in self.ticker_cache.items()
            }
        return dict(sorted(result.items(), key=lambda x: x[1], reverse=True))

    def get_all_tickers(self) -> dict[str, dict]:
        """Todos los tickers 24h disponibles."""
        with self.lock:
            return {sym: dict(data) for sym, data in self.ticker_cache.items()}

    # ──────────────────────────────────────────────────────────────────────
    # Utilidades
    # ──────────────────────────────────────────────────────────────────────

    def get_stale_symbols(self, max_age_seconds: int = 60) -> list[str]:
        """Símbolos cuyo markPrice no se ha actualizado en max_age_seconds."""
        current_time = time.time()
        return [
            s for s in self.symbols
            if current_time - self.last_update.get(s, 0) > max_age_seconds
        ]

    def get_stats(self) -> dict:
        """Estadísticas generales de las conexiones."""
        with self.lock:
            active_prices  = len(self.price_cache)
            active_tickers = len(self.ticker_cache)
            total_symbols  = len(self.symbols)
            stale_count    = len(self.get_stale_symbols())

        return {
            "total_symbols":   total_symbols,
            "active_prices":   active_prices,
            "active_tickers":  active_tickers,
            "stale_symbols":   stale_count,
            "connection_stats": dict(self.connection_stats),
        }


# ══════════════════════════════════════════════════════════════════════════
# Ejemplo de uso
# ══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
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

    cache = SymbolWebSocketPriceCache(symbols, symbols_per_connection=20)
    cache.start()

    # Esperar un momento para que lleguen los primeros datos
    print("⏳ Esperando datos iniciales…")
    time.sleep(3)

    try:
        while True:
            time.sleep(1)
            os.system("cls" if os.name == "nt" else "clear")

            now = datetime.now().strftime("%H:%M:%S")
            changes = cache.get_all_changes_24h()   # dict ordenado mayor → menor

            col_w = 32  # ancho de cada columna
            print("=" * (col_w * 2 + 4))
            print(f"  📊 Cambio 24h – todos los símbolos ({now})")
            print("=" * (col_w * 2 + 4))
            print(f"  {'SÍMBOLO':<12} {'PRECIO':>14} {'24H %':>9}   "
                  f"{'SÍMBOLO':<12} {'PRECIO':>14} {'24H %':>9}")
            print("-" * (col_w * 2 + 4))

            # Mostrar en dos columnas
            items = list(changes.items())
            half  = (len(items) + 1) // 2

            for i in range(half):
                # Columna izquierda
                sym_l, pct_l = items[i]
                price_l      = cache.get_price(sym_l)
                price_str_l  = f"${price_l:.4f}" if price_l else "–"
                arrow_l      = "▲" if pct_l >= 0 else "▼"
                pct_str_l    = f"{arrow_l}{abs(pct_l):.2f}%"
                color_l      = "\033[92m" if pct_l >= 0 else "\033[91m"  # verde / rojo

                row = (f"  {color_l}{sym_l:<12}{'\033[0m'} "
                       f"{price_str_l:>14} "
                       f"{color_l}{pct_str_l:>9}{'\033[0m'}")

                # Columna derecha (si existe)
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

            # ── Estadísticas ──────────────────────────────────────────────
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
