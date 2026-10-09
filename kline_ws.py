"""
kline_ws.py — Velas de Binance USDⓈ-M Futures SOLO por WebSocket (cero REST).

Conexiones
  • UNA conexión WebSocket por intervalo con TODOS los símbolos suscritos
    (<symbol>@kline_1m en una, <symbol>@kline_5m en otra, …). Binance admite
    hasta 1024 streams por conexión; solo si hubiera más símbolos que eso se
    abre una segunda conexión para ese intervalo.
  • No hay backfill REST: la primera vela cerrada llega al cerrar el periodo
    en curso.
  • Binance manda un mensaje de kline cada ~250 ms por símbolo. Los que no son
    de cierre ("x":false) se descartan con una búsqueda de texto ANTES de
    parsear el JSON, así que solo se parsea 1 mensaje por símbolo y periodo.

Almacenamiento (buffer circular numpy, sin objetos Python por vela)
  Por intervalo hay un único bloque contiguo de memoria:
      ohlcv[símbolo, slot, 5]  (open, high, low, close, volume)
      times[símbolo, slot]     (open_time en ms, int64)
  Cada vela ocupa 48 bytes en float64 (28 en float32) frente a ~300 bytes de
  una tupla Python con sus floats. Subir KLINE_HISTORY a 1500 para EMA/RSI/MACD
  no cambia el código, y el formato permite calcular indicadores de todos los
  símbolos a la vez con operaciones vectorizadas (ver snapshot()).

API
  start() / stop()
  ensure_symbols(symbols)                 — añade símbolos sin reconectar
  closed(symbol, interval="1m")           — array (n, 5) de velas cerradas, vieja → reciente
  last_closed(symbol, interval="1m")      — Candle de la última vela cerrada (None si no hay o es vieja)
  snapshot(interval="1m")                 — (symbols, ohlcv, times) de TODOS los símbolos en orden
                                            cronológico, para indicadores vectorizados
  get_stats()

Ejemplo de uso completo al final del archivo (monitor en consola):
  python kline_ws.py --help
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import threading
import time
from typing import Callable, Dict, Iterable, List, NamedTuple, Optional, Tuple

import numpy as np
import websockets

try:
    import orjson as _orjson
    _loads = _orjson.loads
except ImportError:  # pragma: no cover
    _loads = json.loads


WS_URL           = os.getenv("WS_KLINE_URL", os.getenv("WS_FSTREAM_URL", "wss://fstream.binance.com/market/stream"))
MAX_STREAMS      = 1024    # límite de Binance por conexión
STREAMS_PER_CONN = min(int(os.getenv("KLINE_STREAMS_PER_CONN", str(MAX_STREAMS))), MAX_STREAMS)
# Compresión permessage-deflate: menos tráfico de entrada pero más CPU para
# descomprimir ~3.300 mensajes/s por intervalo. Por defecto apagada (prioriza CPU).
WS_COMPRESSION   = os.getenv("KLINE_WS_COMPRESSION", "false").lower() == "true"
SUB_CHUNK_SIZE   = 100     # streams por mensaje SUBSCRIBE
SUB_CHUNK_GAP_S  = 0.25    # Binance cierra la conexión con > 10 mensajes/s entrantes
CONN_STAGGER_S   = 1.0     # separación entre aperturas de conexiones

INTERVAL_MS = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000, "6h": 21_600_000,
    "8h": 28_800_000, "12h": 43_200_000, "1d": 86_400_000,
}

O, H, L, C, V = range(5)    # columnas de ohlcv


class Candle(NamedTuple):
    open_time:  int     # ms
    close_time: int     # ms
    open:       float
    high:       float
    low:        float
    close:      float
    volume:     float

    @property
    def bullish(self) -> bool:
        return self.close >= self.open


def parse_kline_message(raw) -> Optional[tuple]:
    """(symbol, interval, open_time, o, h, l, c, v) si `raw` es el CIERRE de una
    vela; None en otro caso. Descarta las velas en formación sin parsear JSON."""
    if isinstance(raw, (bytes, bytearray)):
        if b'"x":true' not in raw:
            return None
    elif '"x":true' not in raw:
        return None
    try:
        msg = _loads(raw)
    except Exception:
        return None
    data = msg.get("data", msg) if isinstance(msg, dict) else None
    if not isinstance(data, dict) or data.get("e") != "kline":
        return None
    k = data.get("k") or {}
    if not k.get("x"):
        return None
    try:
        return (str(k["s"]).upper(), str(k["i"]), int(k["t"]), float(k["o"]),
                float(k["h"]), float(k["l"]), float(k["c"]), float(k["v"]))
    except (KeyError, TypeError, ValueError):
        return None


class KlineRing:
    """Buffer circular de velas cerradas de UN intervalo para muchos símbolos.
    Thread-safe; sin red."""

    def __init__(self, interval: str = "1m", history: int = 2, dtype: str = "float64",
                 capacity: int = 64) -> None:
        if interval not in INTERVAL_MS:
            raise ValueError(f"Intervalo no soportado: {interval}")
        self.interval    = interval
        self.interval_ms = INTERVAL_MS[interval]
        self.history     = max(1, int(history))
        self.dtype       = np.dtype(dtype)
        self._rows: Dict[str, int] = {}
        self._lock = threading.Lock()
        self.closed_count = 0
        self._alloc(max(1, capacity))

    def _alloc(self, cap: int) -> None:
        old = getattr(self, "ohlcv", None)
        ohlcv = np.zeros((cap, self.history, 5), dtype=self.dtype)
        times = np.zeros((cap, self.history), dtype=np.int64)
        head  = np.zeros(cap, dtype=np.int32)    # próximo slot a escribir
        count = np.zeros(cap, dtype=np.int32)    # slots llenos (≤ history)
        if old is not None:
            n = old.shape[0]
            ohlcv[:n], times[:n] = self.ohlcv, self.times
            head[:n], count[:n]  = self.head, self.count
        self.ohlcv, self.times, self.head, self.count = ohlcv, times, head, count

    def _row(self, symbol: str) -> int:
        row = self._rows.get(symbol)
        if row is None:
            row = len(self._rows)
            if row >= self.ohlcv.shape[0]:
                self._alloc(self.ohlcv.shape[0] * 2)
            self._rows[symbol] = row
        return row

    def reserve(self, symbols: Iterable[str]) -> None:
        """Pre-asigna filas (evita copias al crecer mientras llegan velas)."""
        with self._lock:
            new = [s for s in symbols if s not in self._rows]
            need = len(self._rows) + len(new)
            if need > self.ohlcv.shape[0]:
                self._alloc(need)
            for s in new:
                self._row(s)

    def add(self, symbol: str, open_time: int, o: float, h: float,
            l: float, c: float, v: float) -> None:
        with self._lock:
            r = self._row(symbol)
            n = int(self.count[r])
            if n:
                last = (int(self.head[r]) - 1) % self.history
                last_t = int(self.times[r, last])
                if open_time < last_t:
                    return                          # llegó tarde, ya hay una más nueva
                if open_time == last_t:
                    self.ohlcv[r, last] = (o, h, l, c, v)    # duplicado tras reconexión
                    return
            slot = int(self.head[r])
            self.ohlcv[r, slot] = (o, h, l, c, v)
            self.times[r, slot] = open_time
            self.head[r]  = (slot + 1) % self.history
            self.count[r] = min(n + 1, self.history)
            self.closed_count += 1

    def _order(self, r: int) -> np.ndarray:
        n = int(self.count[r])
        return (int(self.head[r]) - n + np.arange(n)) % self.history

    def closed(self, symbol: str) -> np.ndarray:
        """Copia (n, 5) de las velas cerradas, de la más vieja a la más reciente."""
        with self._lock:
            r = self._rows.get(symbol)
            if r is None:
                return np.empty((0, 5), dtype=self.dtype)
            return self.ohlcv[r, self._order(r)]

    def open_times(self, symbol: str) -> np.ndarray:
        with self._lock:
            r = self._rows.get(symbol)
            if r is None:
                return np.empty(0, dtype=np.int64)
            return self.times[r, self._order(r)]

    def snapshot(self) -> Tuple[List[str], np.ndarray, np.ndarray]:
        """Copia de TODOS los símbolos en orden cronológico, para calcular
        indicadores vectorizados (una fila por símbolo):

          symbols  list[str]            símbolo de cada fila
          ohlcv    (S, history, 5)      columna -1 = vela cerrada más reciente; si
                                        un símbolo tiene menos de `history` velas,
                                        los huecos (a la izquierda) son NaN
          times    (S, history) int64   open_time en ms (0 en los huecos)

        Las filas se alinean por recencia, no por hora: si un símbolo perdió un
        cierre, su columna -1 es más vieja que la de los demás. Filtra con
        times[:, -1] antes de decidir."""
        with self._lock:                            # bajo el lock solo la copia en bloque
            symbols = list(self._rows)              # las filas se asignan en orden de alta
            n = len(symbols)
            ohlcv = self.ohlcv[:n].copy()
            times = self.times[:n].copy()
            head  = self.head[:n].astype(np.int64)[:, None]
            count = self.count[:n].astype(np.int64)[:, None]
        h   = self.history
        pos = np.arange(h)[None, :]
        idx = (head - h + pos) % h                  # slot de cada posición cronológica
        ohlcv = np.take_along_axis(ohlcv, idx[:, :, None], axis=1)
        times = np.take_along_axis(times, idx, axis=1)
        gap = pos < (h - count)
        ohlcv[gap] = np.nan
        times[gap] = 0
        return symbols, ohlcv, times

    def last_closed(self, symbol: str, fresh: bool = True,
                    now_ms: Optional[int] = None, grace_ms: int = 5_000) -> Optional[Candle]:
        """Última vela cerrada. Con fresh=True solo se devuelve si es la del
        periodo inmediatamente anterior (si se perdió un cierre por una
        reconexión, el dato viejo no se usa para decidir)."""
        with self._lock:
            r = self._rows.get(symbol)
            if r is None or not self.count[r]:
                return None
            last = (int(self.head[r]) - 1) % self.history
            t = int(self.times[r, last])
            o, h, l, c, v = (float(x) for x in self.ohlcv[r, last])
        candle = Candle(t, t + self.interval_ms - 1, o, h, l, c, v)
        if fresh:
            now_ms = int(time.time() * 1000) if now_ms is None else now_ms
            if now_ms - candle.close_time > self.interval_ms + grace_ms:
                return None
        return candle

    def symbols_with_data(self) -> int:
        with self._lock:
            return int(np.count_nonzero(self.count[:len(self._rows)]))

    @property
    def nbytes(self) -> int:
        return self.ohlcv.nbytes + self.times.nbytes + self.head.nbytes + self.count.nbytes


class _Conn:
    __slots__ = ("cid", "interval", "symbols", "subscribed", "ws", "messages", "reconnects")

    def __init__(self, cid: int, interval: str) -> None:
        self.cid        = cid
        self.interval   = interval
        self.symbols: List[str] = []
        self.subscribed: set    = set()
        self.ws         = None
        self.messages   = 0
        self.reconnects = 0


class KlineWebSocketStream:
    """Una conexión por intervalo con todos los símbolos; llena un KlineRing por intervalo."""

    def __init__(self, symbols: Iterable[str], intervals: Iterable[str] = ("1m",),
                 history: int = 2, dtype: str = "float64",
                 streams_per_conn: int = STREAMS_PER_CONN,
                 on_close: Optional[Callable[[str, str, Candle], None]] = None) -> None:
        self.intervals = list(dict.fromkeys(intervals))
        symbols = sorted({s.upper() for s in symbols if s})
        self.rings: Dict[str, KlineRing] = {
            iv: KlineRing(iv, history=history, dtype=dtype, capacity=max(64, len(symbols) + 64))
            for iv in self.intervals
        }
        self.per_conn = max(1, min(int(streams_per_conn), MAX_STREAMS))
        self.on_close = on_close
        self._conns: List[_Conn] = []
        self._lock = threading.Lock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._tasks: Dict[int, object] = {}
        self.running = False
        self.last_error = ""
        self._assign(symbols)

    # ── Reparto de símbolos ──────────────────────────────────────────────────

    def _assign(self, symbols: Iterable[str]) -> List[_Conn]:
        """Mete los símbolos nuevos en la conexión de cada intervalo (abre otra
        solo si se superan los 1024 streams). Devuelve las conexiones tocadas."""
        symbols = [s.upper() for s in symbols if s]
        touched: List[_Conn] = []
        with self._lock:
            for iv in self.intervals:
                conns = [c for c in self._conns if c.interval == iv]
                known = {s for c in conns for s in c.symbols}
                new = [s for s in dict.fromkeys(symbols) if s not in known]
                if not new:
                    continue
                self.rings[iv].reserve(new)
                for sym in new:
                    if not conns or len(conns[-1].symbols) >= self.per_conn:
                        conns.append(_Conn(len(self._conns), iv))
                        self._conns.append(conns[-1])
                    conns[-1].symbols.append(sym)
                    if conns[-1] not in touched:
                        touched.append(conns[-1])
        return touched

    # ── Conexiones ───────────────────────────────────────────────────────────

    async def _subscribe(self, conn: _Conn, ws) -> None:
        with self._lock:
            todo = [f"{s.lower()}@kline_{conn.interval}" for s in conn.symbols]
        todo = [s for s in todo if s not in conn.subscribed]
        for i in range(0, len(todo), SUB_CHUNK_SIZE):
            if conn.ws is not ws:
                return
            part = todo[i:i + SUB_CHUNK_SIZE]
            await ws.send(json.dumps({"method": "SUBSCRIBE", "params": part,
                                      "id": conn.cid * 100_000 + i}))
            conn.subscribed.update(part)
            await asyncio.sleep(SUB_CHUNK_GAP_S)

    async def _conn_loop(self, conn: _Conn) -> None:
        await asyncio.sleep(conn.cid * CONN_STAGGER_S)
        rings = self.rings
        delay = 1.0
        while self.running:
            connected_at = 0.0
            try:
                async with websockets.connect(
                    WS_URL, ping_interval=20, ping_timeout=20, close_timeout=5,
                    max_queue=4096, compression="deflate" if WS_COMPRESSION else None,
                ) as ws:
                    connected_at = time.time()
                    conn.ws = ws
                    conn.subscribed = set()
                    await self._subscribe(conn, ws)
                    print(f"✅ [KLINE {conn.interval}] conexión {conn.cid} activa — "
                          f"{len(conn.subscribed)} streams", flush=True)
                    delay = 1.0
                    async for raw in ws:
                        conn.messages += 1
                        p = parse_kline_message(raw)
                        if p is None:
                            continue
                        ring = rings.get(p[1])
                        if ring is None:
                            continue
                        ring.add(p[0], p[2], p[3], p[4], p[5], p[6], p[7])
                        cb = self.on_close
                        if cb is not None:
                            try:
                                cb(p[0], p[1], Candle(p[2], p[2] + ring.interval_ms - 1, *p[3:]))
                            except Exception:
                                pass
                    if self.running:    # cierre limpio del servidor (p. ej. el corte de 24 h de Binance)
                        self.last_error = (f"{conn.interval}/{conn.cid}: cerrada por el servidor "
                                           f"(código {getattr(ws, 'close_code', None)})")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = f"{conn.interval}/{conn.cid}: {exc}"
            finally:
                conn.ws = None
            if not self.running:
                break
            conn.reconnects += 1
            # Conexión que aguantó (Binance corta cada 24 h) → reconexión rápida.
            delay = 1.0 if connected_at and time.time() - connected_at > 60 else min(delay * 2, 60.0)
            wait = delay + random.uniform(0, 1.0)
            print(f"🔴 [KLINE {conn.interval}] conexión {conn.cid} caída ({self.last_error}) — "
                  f"reconectando en {wait:.1f}s", flush=True)
            await asyncio.sleep(wait)

    def _launch(self, conn: _Conn) -> None:
        self._tasks[conn.cid] = asyncio.run_coroutine_threadsafe(self._conn_loop(conn), self._loop)

    @staticmethod
    def _run_loop(loop: asyncio.AbstractEventLoop) -> None:
        try:
            loop.run_forever()
        finally:
            loop.close()

    # ── API pública ──────────────────────────────────────────────────────────

    def ensure_symbols(self, symbols: Iterable[str]) -> None:
        touched = self._assign(symbols)
        if not touched or self._loop is None or not self.running:
            return
        for conn in touched:
            if conn.cid not in self._tasks:
                self._launch(conn)
            elif conn.ws is not None:
                asyncio.run_coroutine_threadsafe(self._subscribe(conn, conn.ws), self._loop)
            # si está reconectando, al conectar se suscribe a todos sus símbolos

    def start(self) -> None:
        if self.running:
            return
        self.running = True
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, args=(self._loop,),
                                        daemon=True, name="ws-kline")
        self._thread.start()
        for conn in list(self._conns):
            self._launch(conn)
        for iv in self.intervals:
            conns = [c for c in self._conns if c.interval == iv]
            print(f"✅ [KLINE {iv}] {sum(len(c.symbols) for c in conns)} símbolos en "
                  f"{len(conns)} conexión(es), {self.rings[iv].history} velas cerradas "
                  f"por símbolo — sin REST", flush=True)

    def stop(self, timeout: float = 5.0) -> None:
        """Cierra las conexiones, espera a que terminen TODAS las tareas del loop
        (también las que estaban esperando para reconectar) y detiene su hilo.
        Se puede llamar desde on_close: en ese caso no espera."""
        self.running = False
        loop, thread = self._loop, self._thread
        if loop is None:
            return
        self._loop = self._thread = None
        self._tasks.clear()

        async def _shutdown() -> None:
            sockets = [c.ws for c in self._conns if c.ws is not None]
            await asyncio.gather(*(asyncio.wait_for(ws.close(), 3) for ws in sockets),
                                 return_exceptions=True)
            me = asyncio.current_task()
            pending = [t for t in asyncio.all_tasks() if t is not me]
            for t in pending:
                t.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            await loop.shutdown_asyncgens()

        if threading.current_thread() is thread:          # llamado desde on_close
            loop.create_task(_shutdown()).add_done_callback(lambda _t: loop.stop())
            return
        try:
            asyncio.run_coroutine_threadsafe(_shutdown(), loop).result(timeout=timeout)
        except Exception:
            pass
        loop.call_soon_threadsafe(loop.stop)
        if thread is not None:
            thread.join(timeout=timeout)                  # el hilo cierra el loop al salir

    # ── Lectura ──────────────────────────────────────────────────────────────

    def closed(self, symbol: str, interval: str = "1m") -> np.ndarray:
        return self.rings[interval].closed(symbol)

    def last_closed(self, symbol: str, interval: str = "1m", fresh: bool = True) -> Optional[Candle]:
        return self.rings[interval].last_closed(symbol, fresh=fresh)

    def snapshot(self, interval: str = "1m") -> Tuple[List[str], np.ndarray, np.ndarray]:
        return self.rings[interval].snapshot()

    def get_stats(self) -> dict:
        with self._lock:
            conns = list(self._conns)
        per_iv = {}
        for iv, ring in self.rings.items():
            cs = [c for c in conns if c.interval == iv]
            per_iv[iv] = {
                "symbols":        sum(len(c.symbols) for c in cs),
                "connections":    len(cs),
                "active":         sum(1 for c in cs if c.ws is not None),
                "messages":       sum(c.messages for c in cs),
                "closed_candles": ring.closed_count,
                "with_data":      ring.symbols_with_data(),
                "memory_kb":      round(ring.nbytes / 1024, 1),
            }
        return {
            "pairs_with_data":    sum(v["with_data"] for v in per_iv.values()),
            "total_messages":     sum(v["messages"] for v in per_iv.values()),
            "active_connections": sum(v["active"] for v in per_iv.values()),
            "connections":        len(conns),
            "reconnects":         sum(c.reconnects for c in conns),
            "last_error":         self.last_error,
            "intervals":          per_iv,
        }


# ═════════════════════════════════════════════════════════════════════════════
#  EJEMPLO DE USO COMPLETO  ·  python kline_ws.py --help
# ═════════════════════════════════════════════════════════════════════════════
#
#  Ejecutado directamente, el módulo arranca un monitor en consola que recorre
#  toda la API y sirve de plantilla para futuras implementaciones:
#
#    1. on_close → queue.Queue → hilo principal. El callback corre en el hilo
#       del WebSocket; si se bloquea, retrasa la lectura de TODOS los símbolos
#       de esa conexión, así que ahí solo se encola.
#    2. Deduplicación por (intervalo, símbolo, open_time) antes de actuar.
#    3. Resumen por lote de cierres: cuántos símbolos cerraron, cuáles faltan y
#       cuánto tardó el cierre en llegar desde el fin del periodo.
#    4. closed() / open_times() / last_closed() de un símbolo.
#    5. snapshot() + EMA/RSI vectorizados sobre TODOS los símbolos a la vez.
#    6. ensure_symbols() en caliente, get_stats() periódico y stop() limpio.
#
#  Línea de comandos:
#    python kline_ws.py                                  # 6 pares, 1m y 5m, hasta Ctrl+C
#    python kline_ws.py --symbols BTCUSDT ETHUSDT --intervals 1m 15m --history 300
#    python kline_ws.py --all --stats-every 15           # todos los perpetuos USDT
#    python kline_ws.py --add ADAUSDT LINKUSDT --add-after 90
#    python kline_ws.py --duration 600                   # 10 min y resumen final
#
#  Plantilla mínima desde otro módulo:
#
#    import queue
#    from kline_ws import KlineWebSocketStream, C
#
#    q = queue.Queue()
#    stream = KlineWebSocketStream(["BTCUSDT", "ETHUSDT"], intervals=("1m", "5m"),
#                                  history=500, on_close=lambda s, iv, c: q.put((s, iv, c)))
#    stream.start()
#    try:
#        while True:
#            symbol, interval, candle = q.get()       # el trabajo pesado va aquí
#            closes = stream.closed(symbol, interval)[:, C]
#            ...
#    finally:
#        stream.stop()


def ema_matrix(x: np.ndarray, period: int) -> np.ndarray:
    """EMA fila a fila de una matriz (S, T) — una fila por símbolo, huecos NaN a
    la izquierda, como la devuelve snapshot(). Semilla = primer valor válido de
    cada fila (igual que pandas ewm(span=period, adjust=False)). Vectorizada
    sobre los símbolos: el único bucle es sobre el tiempo."""
    x = np.asarray(x, dtype=np.float64)
    alpha = 2.0 / (period + 1.0)
    out = np.empty_like(x)
    ema = np.full(x.shape[0], np.nan)
    for t in range(x.shape[1]):
        xt = x[:, t]
        ema = np.where(np.isnan(ema), xt, np.where(np.isnan(xt), ema, ema + alpha * (xt - ema)))
        out[:, t] = ema
    return out


def rsi_matrix(x: np.ndarray, period: int = 14) -> np.ndarray:
    """RSI de Wilder fila a fila de (S, T), con huecos NaN a la izquierda.
    Primera media = media simple de las `period` primeras diferencias de cada
    fila y después suavizado de Wilder (el método clásico, como TA-Lib y
    TradingView). NaN mientras la fila tenga menos de period+1 velas."""
    x = np.asarray(x, dtype=np.float64)
    S, T = x.shape
    out = np.full((S, T), np.nan)
    k = np.zeros(S, dtype=np.int64)                 # diferencias válidas vistas por fila
    sum_g, sum_l = np.zeros(S), np.zeros(S)
    avg_g, avg_l = np.full(S, np.nan), np.full(S, np.nan)
    with np.errstate(divide="ignore", invalid="ignore"):
        for t in range(1, T):
            d = x[:, t] - x[:, t - 1]
            ok = ~np.isnan(d)
            g = np.where(ok, np.maximum(d, 0.0), 0.0)
            l = np.where(ok, np.maximum(-d, 0.0), 0.0)
            k += ok
            seed = ok & (k <= period)
            sum_g += np.where(seed, g, 0.0)
            sum_l += np.where(seed, l, 0.0)
            first, roll = ok & (k == period), ok & (k > period)
            avg_g = np.where(first, sum_g / period,
                             np.where(roll, (avg_g * (period - 1) + g) / period, avg_g))
            avg_l = np.where(first, sum_l / period,
                             np.where(roll, (avg_l * (period - 1) + l) / period, avg_l))
            rsi = 100.0 - 100.0 / (1.0 + avg_g / avg_l)
            out[:, t] = np.where(avg_l == 0, np.where(avg_g == 0, 50.0, 100.0), rsi)
    return out


def discover_symbols_ws(quote: str = "USDT", seconds: float = 4.0, url: str = WS_URL) -> List[str]:
    """Perpetuos activos que cotizan en `quote`, descubiertos escuchando unos
    segundos el stream !miniTicker@arr (sin REST). Solo aparecen los símbolos
    que cotizaron en esos segundos. Devuelve [] si falla."""

    async def _listen() -> List[str]:
        found: set = set()
        async with websockets.connect(url, ping_interval=20, close_timeout=2) as ws:
            await ws.send(json.dumps({"method": "SUBSCRIBE", "params": ["!miniTicker@arr"], "id": 1}))
            deadline = time.monotonic() + seconds
            while True:
                left = deadline - time.monotonic()
                if left <= 0:
                    break
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=left)
                except asyncio.TimeoutError:
                    break
                msg = _loads(raw)
                data = msg.get("data") if isinstance(msg, dict) else msg
                for tk in data if isinstance(data, list) else ():
                    s = str(tk.get("s", "")) if isinstance(tk, dict) else ""
                    if s.endswith(quote) and "_" not in s:   # fuera trimestrales (BTCUSDT_251226)
                        found.add(s)
        return sorted(found)

    try:
        return asyncio.run(_listen())
    except Exception as exc:
        print(f"⚠️  Descubrimiento por WebSocket falló: {exc}", flush=True)
        return []


def _hhmm(ms: int) -> str:
    return time.strftime("%H:%M", time.localtime(ms / 1000))


def _dur(seconds: float) -> str:
    s = int(max(0.0, seconds))
    h, rem = divmod(s, 3600)
    m, s = divmod(rem, 60)
    return f"{h}h{m:02d}m" if h else f"{m}m{s:02d}s" if m else f"{s}s"


def _qty(v: float) -> str:
    for div, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(v) >= div:
            return f"{v / div:.2f}{suffix}"
    return f"{v:.4g}"


def main(argv: Optional[List[str]] = None) -> None:
    """Monitor en consola que usa toda la API (python kline_ws.py --help)."""
    import argparse
    import queue

    ap = argparse.ArgumentParser(
        prog="kline_ws.py",
        description="Monitor de velas cerradas de Binance USDⓈ-M por WebSocket "
                    "(ejemplo de uso de kline_ws).")
    ap.add_argument("--symbols", nargs="+", metavar="SYM",
                    default=["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT"],
                    help="pares a seguir (def.: BTC ETH SOL BNB XRP DOGE contra USDT)")
    ap.add_argument("--all", action="store_true",
                    help="todos los perpetuos USDT, descubiertos por WebSocket (sin REST)")
    ap.add_argument("--intervals", nargs="+", metavar="IV", default=["1m", "5m"],
                    choices=list(INTERVAL_MS), help=f"de {', '.join(INTERVAL_MS)}")
    ap.add_argument("--history", type=int, default=int(os.getenv("KLINE_HISTORY", "100")),
                    help="velas cerradas guardadas por símbolo (def.: KLINE_HISTORY o 100)")
    ap.add_argument("--dtype", default="float64", choices=["float64", "float32"])
    ap.add_argument("--stats-every", type=float, default=30.0, metavar="S",
                    help="segundos entre resúmenes de get_stats() (0 = nunca)")
    ap.add_argument("--duration", type=float, default=0.0, metavar="S",
                    help="segundos de ejecución (0 = hasta Ctrl+C)")
    ap.add_argument("--add", nargs="+", default=[], metavar="SYM",
                    help="símbolos a añadir en caliente con ensure_symbols()")
    ap.add_argument("--add-after", type=float, default=60.0, metavar="S",
                    help="segundos tras el arranque para el --add (def.: 60)")
    ap.add_argument("--quiet", action="store_true",
                    help="no imprimir cada vela (automático con más de 20 símbolos)")
    args = ap.parse_args(argv)

    EMA_FAST, EMA_SLOW, RSI_N = 9, 21, 14
    NEED = max(EMA_SLOW, RSI_N) + 1     # velas para EMA lenta en las 2 últimas posiciones y RSI
    BATCH_WAIT_S = 5.0                  # tras el 1er cierre de un periodo, espera máxima al resto

    stream: Optional[KlineWebSocketStream] = None
    symbols: List[str] = []
    expected: set = set()               # quién debería cerrar en cada lote
    last_seen: Dict[tuple, int] = {}    # (intervalo, símbolo) → último open_time procesado
    batches: Dict[tuple, dict] = {}     # (intervalo, open_time) → lote de cierres en curso
    reported: Dict[str, int] = {}       # intervalo → open_time del último lote resumido
    api_shown: set = set()
    ind_shown: Dict[str, int] = {}      # intervalo → open_time de la última tabla de indicadores
    count = {"closes": 0, "repeated": 0, "late": 0, "missed": 0}
    t_start = time.monotonic()
    prev = {"msgs": 0, "t": t_start}
    events: "queue.Queue[tuple]" = queue.Queue()

    # ── Callback: corre en el hilo del WebSocket → solo encolar ─────────────
    def on_close(symbol: str, interval: str, candle: Candle) -> None:
        events.put_nowait((symbol, interval, candle, time.time()))

    # ── Consumidor: aquí va la lógica de la estrategia ──────────────────────
    def handle(symbol: str, iv: str, c: Candle, arrived: float) -> None:
        prev_t = last_seen.get((iv, symbol))
        if prev_t is not None and c.open_time <= prev_t:
            count["repeated"] += 1                      # reenvío (p. ej. tras reconexión)
            return
        last_seen[(iv, symbol)] = c.open_time
        count["closes"] += 1
        # Sin REST no hay backfill: si se perdieron cierres (reconexión), la serie salta.
        gap = 0 if prev_t is None else max(0, (c.open_time - prev_t) // INTERVAL_MS[iv] - 1)
        count["missed"] += gap
        lag = arrived - (c.close_time + 1) / 1000       # s desde el fin del periodo
        late = c.open_time <= reported.get(iv, -1)      # su lote ya se resumió
        if verbose:
            chg = (c.close / c.open - 1) * 100 if c.open else 0.0
            print(f"  [{iv:>3}] {symbol:<12} {_hhmm(c.open_time)}  O {c.open:<11.6g} "
                  f"H {c.high:<11.6g} L {c.low:<11.6g} C {c.close:<11.6g} V {_qty(c.volume):>8}  "
                  f"{'▲' if c.bullish else '▼'}{chg:+6.2f}%  llegó {lag:+.2f}s"
                  f"{f'  (hueco: {gap} vela(s) perdidas)' if gap else ''}"
                  f"{'  (tardía)' if late else ''}", flush=True)
        if late:
            count["late"] += 1
            return
        b = batches.setdefault((iv, c.open_time),
                               {"t0": time.monotonic(), "exp": frozenset(expected),
                                "syms": set(), "up": 0, "lags": [], "gaps": 0})
        b["syms"].add(symbol)
        b["up"] += c.bullish
        b["lags"].append(lag)
        b["gaps"] += gap > 0

    def flush(now: float) -> None:
        for key in sorted(batches, key=lambda k: k[1]):
            iv, t = key
            b = batches[key]
            exp = b["exp"]                              # quién estaba suscrito cuando cerró
            if not exp <= b["syms"] and now - b["t0"] < BATCH_WAIT_S:
                continue
            del batches[key]
            reported[iv] = max(reported.get(iv, -1), t)
            n, lags = len(b["syms"]), b["lags"]
            missing = sorted(exp - b["syms"])
            extra = (f" · faltan {len(missing)}: {', '.join(missing[:6])}"
                     f"{' …' if len(missing) > 6 else ''}") if missing else ""
            if b["gaps"]:
                extra += f" · {b['gaps']} con hueco previo"
            print(f"📦 [{iv} {_hhmm(t)}] {n}/{len(exp)} cierres · ▲{b['up']} ▼{n - b['up']} · "
                  f"llegada media {sum(lags) / n:+.2f}s, máx {max(lags):+.2f}s{extra}", flush=True)
            if iv not in api_shown:
                api_shown.add(iv)
                show_read_api(iv)
            show_indicators(iv)

    def show_read_api(iv: str) -> None:
        sym = next((s for s in symbols if len(stream.closed(s, iv))), symbols[0])
        arr = stream.closed(sym, iv)                    # (n, 5) O H L C V, vieja → reciente
        times = stream.rings[iv].open_times(sym)        # open_time (ms) de cada fila de closed()
        last = stream.last_closed(sym, iv)              # Candle, o None si la última está vieja
        if last is None:                                # fresh=False la devuelve igualmente
            last = f"None (vela vieja) · con fresh=False → {stream.last_closed(sym, iv, fresh=False)}"
        print(f"🔍 [{iv}] API de lectura con {sym}:\n"
              f"     closed(sym, iv)            → array {arr.shape} {arr.dtype}, columnas O H L C V\n"
              f"     rings[iv].open_times(sym)  → {[_hhmm(int(x)) for x in times[-3:]]}\n"
              f"     last_closed(sym, iv)       → {last}", flush=True)

    def show_indicators(iv: str) -> None:
        t0 = time.perf_counter()
        syms, ohlcv, times = stream.snapshot(iv)        # (S, history, 5), NaN a la izquierda
        latest = int(times[:, -1].max(initial=0))       # open_time más reciente del intervalo
        if ind_shown.get(iv) == latest:
            return                                      # ya se mostró esta misma vela
        ind_shown[iv] = latest
        close = ohlcv[:, :, C].astype(np.float64)
        n_ok = np.count_nonzero(~np.isnan(close), axis=1)
        ema_f, ema_s = ema_matrix(close, EMA_FAST), ema_matrix(close, EMA_SLOW)
        rsi = rsi_matrix(close, RSI_N)[:, -1]
        ms = (time.perf_counter() - t0) * 1000
        # Al día = su última vela es la más reciente del intervalo (no perdió el último cierre).
        fresh = (n_ok > 0) & (times[:, -1] == latest)
        ready = fresh & (n_ok >= NEED)
        stale = int(np.count_nonzero((n_ok > 0) & ~fresh))
        title = (f"📈 [{iv} {_hhmm(latest)}] snapshot() + EMA{EMA_FAST}/EMA{EMA_SLOW}/RSI{RSI_N} "
                 f"de {len(syms)} símbolos en {ms:.1f} ms"
                 + (f" · {stale} sin la última vela" if stale else ""))
        if not ready.any():
            print(f"{title} — calentando: {int(n_ok[fresh].max(initial=0))}/{NEED} velas", flush=True)
            return
        print(f"{title} — {int(ready.sum())} listos", flush=True)
        idx = np.flatnonzero(ready)
        idx = idx[np.argsort(-rsi[idx], kind="stable")]  # RSI de mayor a menor
        rows = idx if len(idx) <= 10 else np.concatenate([idx[:5], idx[-5:]])
        for j, i in enumerate(rows):
            if len(idx) > 10 and j == 5:
                print("     …", flush=True)
            trend = "▲" if ema_f[i, -1] > ema_s[i, -1] else "▼"
            print(f"     {syms[i]:<12} C {close[i, -1]:<11.6g} EMA{EMA_FAST} {ema_f[i, -1]:<11.6g} "
                  f"EMA{EMA_SLOW} {ema_s[i, -1]:<11.6g} {trend}  RSI {rsi[i]:5.1f}", flush=True)
        up = ready & (ema_f[:, -2] <= ema_s[:, -2]) & (ema_f[:, -1] > ema_s[:, -1])
        down = ready & (ema_f[:, -2] >= ema_s[:, -2]) & (ema_f[:, -1] < ema_s[:, -1])
        if up.any() or down.any():
            def names(mask: np.ndarray) -> str:
                return ", ".join(syms[i] for i in np.flatnonzero(mask)[:8]) or "—"
            print(f"     cruces EMA{EMA_FAST}/EMA{EMA_SLOW} en esta vela → alcistas: {names(up)} · "
                  f"bajistas: {names(down)}", flush=True)

    def show_stats(now: float) -> None:
        st = stream.get_stats()
        rate = (st["total_messages"] - prev["msgs"]) / max(now - prev["t"], 1e-9)
        prev["msgs"], prev["t"] = st["total_messages"], now
        parsed = sum(d["closed_candles"] for d in st["intervals"].values())
        skipped = 100 * (1 - parsed / st["total_messages"]) if st["total_messages"] else 0.0
        print(f"📊 [{time.strftime('%H:%M:%S')}] {st['active_connections']}/{st['connections']} "
              f"conexiones activas · {rate:,.0f} msg/s · {skipped:.1f}% descartados sin parsear JSON · "
              f"reconexiones {st['reconnects']} · velas perdidas {count['missed']} · "
              f"repetidas {count['repeated']} · tardías {count['late']}", flush=True)
        for iv, d in st["intervals"].items():
            print(f"     {iv:>3}: {d['symbols']} símbolos · {d['active']}/{d['connections']} conexión(es) · "
                  f"{d['closed_candles']} velas cerradas · {d['with_data']} con datos · "
                  f"{d['memory_kb']} KB", flush=True)
        if st["last_error"]:
            print(f"     último error: {st['last_error']}", flush=True)

    try:
        # ── 1. Símbolos ──────────────────────────────────────────────────────
        symbols = list(dict.fromkeys(s.upper() for s in args.symbols))
        if args.all:
            print("🔎 Descubriendo perpetuos USDT por WebSocket (!miniTicker@arr)…", flush=True)
            found = discover_symbols_ws("USDT")
            if found:
                symbols = found
                print(f"🔎 {len(found)} perpetuos USDT encontrados", flush=True)
            else:
                print("⚠️  No se encontró ninguno; se usan los de --symbols", flush=True)
        to_add = [s for s in dict.fromkeys(a.upper() for a in args.add) if s not in symbols]
        verbose = not args.quiet and len(symbols) + len(to_add) <= 20
        expected.update(symbols)

        # ── 2. Arranque ──────────────────────────────────────────────────────
        stream = KlineWebSocketStream(symbols, intervals=args.intervals, history=args.history,
                                      dtype=args.dtype, on_close=on_close)
        stream.start()
        now_ms = int(time.time() * 1000)
        waits = " · ".join(f"{iv} en {_dur((INTERVAL_MS[iv] - now_ms % INTERVAL_MS[iv] + 999) // 1000)}"
                           for iv in stream.intervals)
        print(f"⏳ Sin REST, la primera vela llega al cerrar el periodo en curso: {waits}. "
              f"Ctrl+C para salir.", flush=True)
        if args.history < NEED:
            print(f"ℹ️  Con --history {args.history} no hay velas suficientes para "
                  f"EMA{EMA_SLOW}/RSI{RSI_N} (hacen falta {NEED}).", flush=True)

        # ── 3. Bucle consumidor ──────────────────────────────────────────────
        add_at = t_start + args.add_after if to_add else float("inf")
        next_stats = t_start + args.stats_every if args.stats_every > 0 else float("inf")
        while True:
            try:
                item = events.get(timeout=0.5)
                while True:
                    handle(*item)
                    item = events.get_nowait()
            except queue.Empty:
                pass
            now = time.monotonic()
            flush(now)
            if now >= add_at:
                add_at = float("inf")
                stream.ensure_symbols(to_add)
                symbols.extend(to_add)
                expected.update(to_add)
                print(f"➕ ensure_symbols({to_add}) — suscritos en caliente, sin reconectar", flush=True)
            if now >= next_stats:
                next_stats = now + args.stats_every
                show_stats(now)
            if args.duration and now - t_start >= args.duration:
                break
    except KeyboardInterrupt:
        print("\n⏹  Ctrl+C", flush=True)
    finally:
        # ── 4. Parada limpia y resumen ───────────────────────────────────────
        if stream is not None:
            stream.stop()
            st = stream.get_stats()
            print(f"🏁 {_dur(time.monotonic() - t_start)} · {count['closes']} cierres procesados · "
                  f"{count['missed']} perdidos · {count['repeated']} repetidos · {count['late']} tardíos · "
                  f"{st['total_messages']:,} mensajes · {st['reconnects']} reconexiones"
                  + (f" · último error: {st['last_error']}" if st["last_error"] else ""), flush=True)


if __name__ == "__main__":
    main()
