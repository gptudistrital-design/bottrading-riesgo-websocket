"""
KlineWebSocketCache — v4  (WebSocket-first · Zero-REST en operación normal)
===========================================================================

ARQUITECTURA
────────────
  ┌─────────────────────────────────────────────────────────────────────┐
  │  ARRANQUE                                                           │
  │    1. Backfill inicial por REST (histórico completo)                │
  │    2. Conexiones WebSocket (multiplexadas)                          │
  │    3. Monitor de reloj – 1s, cierra velas por close_time           │
  │    4. Safety refresh – cada N min, último recurso                   │
  └─────────────────────────────────────────────────────────────────────┘
  ┌─────────────────────────────────────────────────────────────────────┐
  │  OPERACIÓN NORMAL (WS conectado)                                    │
  │    • WS abre, actualiza y cierra velas → CERO peticiones REST       │
  │    • Monitor de reloj cierra velas donde Binance omite x=true       │
  └─────────────────────────────────────────────────────────────────────┘
  ┌─────────────────────────────────────────────────────────────────────┐
  │  DESCONEXIÓN / RECONEXIÓN                                           │
  │    • Al caer el WS → se guarda el timestamp de desconexión          │
  │    • Al reconectar  → REST quirúrgico SOLO del período ausente      │
  │    • Health monitor → detecta streams muertos y fuerza reconexión   │
  └─────────────────────────────────────────────────────────────────────┘

DIFERENCIAS vs v3
─────────────────
  ❌ Eliminado : _per_interval_scheduler  (1 tarea por par×intervalo → REST en
                  cada cierre aunque el WS funcione → principal desperdicio)
  ❌ Eliminado : integrity checks en flujo normal
  ❌ Eliminado : _upsert_rows_into_buffer en mensajes WS fuera de orden
  ❌ Eliminado : _build_backfill_groups / _refresh_groups (complejidad inútil)
  ✅ Añadido   : _fill_reconnect_gap — REST quirúrgico post-desconexión
  ✅ Mejorado  : _handle_ws_kline — O(1), sin sort, sin upsert
  ✅ Mejorado  : _safety_refresh  — solo actúa con gaps REALES (> 1 intervalo)
  ✅ Mejorado  : health monitor   — umbral por grupo (no por stream individual)
"""

from __future__ import annotations

import asyncio
import gc
import json
import random
import threading
import time
from collections import defaultdict, deque
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import aiohttp
import pandas as pd
import websockets


# =============================================================================
# TOKEN BUCKET  —  rate-limiter global para peticiones REST
# =============================================================================

class _TokenBucket:
    """
    Limita peticiones REST a Binance Futures.

    Parámetros conservadores para USDT-M Futures:
      capacity    = 1 200  →  slots en ventana de 1 min
      refill_rate =    20  →  tokens nuevos / segundo

    Peso por limit:
      ≤  100  → 1  |  ≤  500 → 2  |  ≤ 1000 → 5  |  > 1000 → 10
    """

    def __init__(self, capacity: int = 1_200, refill_rate: float = 20.0) -> None:
        self.capacity    = capacity
        self.refill_rate = refill_rate
        self._tokens     = float(capacity)
        self._last       = time.monotonic()
        self._lock       = asyncio.Lock()

    @staticmethod
    def weight_for_limit(limit: int) -> int:
        if limit <= 100:  return 1
        if limit <= 500:  return 2
        if limit <= 1000: return 5
        return 10

    async def acquire(self, weight: int = 1) -> None:
        async with self._lock:
            now = time.monotonic()
            self._tokens = min(
                self.capacity,
                self._tokens + (now - self._last) * self.refill_rate,
            )
            self._last = now
            if self._tokens >= weight:
                self._tokens -= weight
                return
            wait = (weight - self._tokens) / self.refill_rate
            await asyncio.sleep(wait)
            self._tokens = 0.0

    def refund(self, weight: int) -> None:
        """Devuelve tokens si la petición fue cancelada antes de enviarse."""
        self._tokens = min(self.capacity, self._tokens + weight)


# =============================================================================
# CLASE PRINCIPAL
# =============================================================================

class KlineWebSocketCache:
    """
    Cache de klines en tiempo real para Binance USDT-M Futures.

    Operación normal : WebSocket exclusivo → CERO REST.
    REST solo en     : backfill inicial · gap post-desconexión · safety 10 min.
    """

    BASE_WS_URL   = "wss://fstream.binance.com/stream"
    BASE_REST_URL = "https://fapi.binance.com"

    # ms de gracia tras close_time para marcar una vela como cerrada.
    # Evita cerrar velas cuyo close_time llega levemente adelantado.
    CLOSE_GRACE_MS = 500

    # Intervalo del monitor de reloj (segundos)
    CLOCK_MONITOR_INTERVAL = 1

    # Advertir si el peso REST supera este valor (de 2 400 total)
    REST_WEIGHT_WARN = 1_900

    def __init__(
        self,
        pairs: Dict[str, List[str]],
        *,
        max_candles: int = 1_500,
        include_open_candle: bool = True,
        backfill_on_start: bool = True,
        streams_per_connection: int = 50,
        rest_limits: Optional[Dict[str, int]] = None,
        rest_timeout: float = 6.0,
        rest_min_sleep: float = 0.05,
        rest_concurrency: int = 20,
        rest_retries: int = 4,
        rest_backoff_max: float = 30.0,
        backfill_batch_size: int = 5,
        backfill_batch_delay: float = 0.10,
        rate_limit_capacity: int = 1_200,
        rate_limit_refill: float = 20.0,
        stream_silence_threshold_seconds: int = 120,
        stream_health_check_seconds: int = 60,
        safety_refresh_interval_seconds: int = 600,
    ) -> None:

        # --- Pares ---
        self.pairs: Dict[str, List[str]] = {
            s.upper(): ([i] if isinstance(i, str) else list(i))
            for s, i in pairs.items()
        }
        self.max_candles            = int(max_candles)
        self.include_open           = bool(include_open_candle)
        self.streams_per_connection = int(streams_per_connection)
        self.backfill_on_start      = bool(backfill_on_start)

        # --- REST ---
        self.rest_limits      = rest_limits or {}
        self.rest_timeout     = float(rest_timeout)
        self.rest_min_sleep   = float(rest_min_sleep)
        self.rest_concurrency = int(rest_concurrency)
        self.rest_retries     = int(rest_retries)
        self.rest_backoff_max = float(rest_backoff_max)

        # --- Backfill ---
        self.backfill_batch_size  = max(1, int(backfill_batch_size))
        self.backfill_batch_delay = float(backfill_batch_delay)

        # --- Salud / safety ---
        self.stream_silence_threshold_seconds = int(stream_silence_threshold_seconds)
        self.stream_health_check_seconds      = int(stream_health_check_seconds)
        self.safety_refresh_interval_seconds  = int(safety_refresh_interval_seconds)

        # --- Rate limiter ---
        self._bucket = _TokenBucket(
            capacity=rate_limit_capacity,
            refill_rate=rate_limit_refill,
        )
        # asyncio.Event para pausa global 429/418 (se crea en el loop)
        self._rate_limit_pause: Optional[asyncio.Event] = None

        # --- Buffers ---
        self.buffers: Dict[Tuple[str, str], deque] = defaultdict(
            lambda: deque(maxlen=self.max_candles)
        )
        self.lock = threading.Lock()

        # --- Métricas ---
        self.last_message_time: Dict[Tuple[str, str], float] = {}
        self.message_counts:    Dict[Tuple[str, str], int]   = defaultdict(int)
        self.clock_closes:      Dict[Tuple[str, str], int]   = defaultdict(int)
        self.gap_fills:         Dict[Tuple[str, str], int]   = defaultdict(int)

        # --- WS: timestamp de desconexión por grupo ---
        # None = conectado  |  float = epoch de desconexión
        self._ws_disconnect_time: Dict[int, Optional[float]] = {}

        # --- Infraestructura ---
        self._loop:   Optional[asyncio.AbstractEventLoop] = None
        self._running = False
        self._tasks:  Dict[tuple, asyncio.Future] = {}
        self._thread: Optional[threading.Thread]  = None
        self._startup_future = None

        self.connection_stats: Dict[str, dict] = defaultdict(lambda: {
            "reconnects": 0, "last_error": None, "streams": [], "active": False,
        })

        # --- Mapeo WS ---
        self.stream_mapping:     Dict[str, Tuple[str, str]] = {}
        self.subscribed_streams: set = set()

        # --- Sesión REST compartida ---
        self._session: Optional[aiohttp.ClientSession] = None

    # =========================================================================
    # UTILIDADES
    # =========================================================================

    def _interval_ms(self, interval: str) -> int:
        """Convierte un intervalo de Binance a milisegundos."""
        units = {
            "s": 1_000,
            "m": 60_000,
            "h": 3_600_000,
            "d": 86_400_000,
            "w": 604_800_000,
        }
        n = int("".join(filter(str.isdigit, interval)))
        u = "".join(filter(str.isalpha, interval))
        return n * units.get(u, 60_000)

    @staticmethod
    def _parse_rest_row(k: list, symbol: str, interval: str, is_closed: bool) -> dict:
        return {
            "open_time":              int(k[0]),
            "close_time":             int(k[6]),
            "symbol":                 symbol.upper(),
            "interval":               interval,
            "open":                   float(k[1]),
            "high":                   float(k[2]),
            "low":                    float(k[3]),
            "close":                  float(k[4]),
            "volume":                 float(k[5]),
            "quote_volume":           float(k[7]),
            "trades":                 int(k[8]),
            "taker_buy_volume":       float(k[9]),
            "taker_buy_quote_volume": float(k[10]),
            "is_closed":              is_closed,
        }

    @staticmethod
    def _build_ws_row(k: dict, is_closed: bool) -> dict:
        return {
            "open_time":              int(k["t"]),
            "close_time":             int(k["T"]),
            "symbol":                 str(k["s"]).upper(),
            "interval":               str(k["i"]),
            "open":                   float(k["o"]),
            "high":                   float(k["h"]),
            "low":                    float(k["l"]),
            "close":                  float(k["c"]),
            "volume":                 float(k["v"]),
            "quote_volume":           float(k["q"]),
            "trades":                 int(k["n"]),
            "taker_buy_volume":       float(k["V"]),
            "taker_buy_quote_volume": float(k["Q"]),
            "is_closed":              is_closed,
        }

    def _upsert_buffer(self, key: Tuple[str, str], rows: List[dict]) -> None:
        """
        Merge + sort en el buffer.
        SOLO para datos REST (backfill y gap fill).
        Los mensajes WS se insertan O(1) en _handle_ws_kline.
        """
        if not rows:
            return
        with self.lock:
            buf = self.buffers[key]
            merged = {r["open_time"]: r for r in buf}
            for r in rows:
                merged[r["open_time"]] = r
            sorted_rows = sorted(merged.values(), key=lambda x: x["open_time"])
            if len(sorted_rows) > self.max_candles:
                sorted_rows = sorted_rows[-self.max_candles:]
            buf.clear()
            buf.extend(sorted_rows)

    def _register_task(self, key: tuple, task: "asyncio.Task") -> "asyncio.Task":
        """
        Registra una tarea para poder cancelarla durante el apagado.
        La tarea se elimina sola del registro cuando termina.
        """
        self._tasks[key] = task

        def _cleanup(done_task: "asyncio.Task", task_key: tuple = key) -> None:
            self._tasks.pop(task_key, None)

        task.add_done_callback(_cleanup)
        return task

    async def _shutdown_async(self) -> None:
        """
        Cierre limpio ejecutado dentro del loop:
          • cancela todas las tareas activas
          • cierra la sesión REST
          • libera buffers y métricas
          • ayuda al GC a soltar memoria
        """
        current = asyncio.current_task()

        pending = [
            task for task in asyncio.all_tasks()
            if task is not current and not task.done()
        ]
        for task in pending:
            task.cancel()

        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

        if self._session and not self._session.closed:
            try:
                await self._session.close()
            except Exception:
                pass
        self._session = None

        with self.lock:
            for buf in self.buffers.values():
                buf.clear()
            self.buffers.clear()

        self.last_message_time.clear()
        self.message_counts.clear()
        self.clock_closes.clear()
        self.gap_fills.clear()
        self._ws_disconnect_time.clear()
        self.connection_stats.clear()
        self.stream_mapping.clear()
        self.subscribed_streams.clear()
        self._tasks.clear()

        self._rate_limit_pause = None
        gc.collect()

    # =========================================================================
    # RATE LIMITER — pausa global 429 / 418
    # =========================================================================

    async def _wait_global_pause(self) -> None:
        if self._rate_limit_pause is not None:
            await self._rate_limit_pause.wait()

    async def _trigger_pause(self, seconds: float, code: str) -> None:
        if self._rate_limit_pause is None:
            return
        self._rate_limit_pause.clear()
        print(f"🚫 REST pausado {seconds:.0f}s (HTTP {code})")
        await asyncio.sleep(seconds)
        self._rate_limit_pause.set()
        print(f"✅ REST reanudado tras {seconds:.0f}s")

    # =========================================================================
    # REST — FETCH CON REINTENTOS, RATE LIMIT Y 429/418
    # =========================================================================

    async def _fetch(
        self,
        url: str,
        params: dict,
        weight: int = 1,
    ) -> list:
        """
        GET JSON a Binance con:
          • Espera de pausa global (429/418).
          • Token bucket.
          • Reintentos exponenciales + jitter.
          • Manejo explícito de 429 y 418.
        """
        if self._session is None or self._session.closed:
            raise RuntimeError("Sesión REST no disponible")

        await self._wait_global_pause()
        await self._bucket.acquire(weight)

        attempt = 0
        while True:
            try:
                timeout = aiohttp.ClientTimeout(total=self.rest_timeout)
                async with self._session.get(url, params=params, timeout=timeout) as resp:

                    # Advertencia de peso alto
                    used_w = resp.headers.get("X-MBX-USED-WEIGHT-1M")
                    if used_w and int(used_w) > self.REST_WEIGHT_WARN:
                        print(f"⚠️  Peso REST: {used_w}/2400 (umbral {self.REST_WEIGHT_WARN})")

                    # 429 — demasiadas peticiones
                    if resp.status == 429:
                        retry_after = float(resp.headers.get("Retry-After", 60))
                        pause = retry_after + 5
                        asyncio.ensure_future(self._trigger_pause(pause, "429"))
                        self._bucket.refund(weight)
                        await asyncio.sleep(pause)
                        attempt += 1
                        if attempt > self.rest_retries:
                            raise RuntimeError("HTTP 429 persistente")
                        await self._bucket.acquire(weight)
                        continue

                    # 418 — IP baneada
                    if resp.status == 418:
                        retry_after = float(resp.headers.get("Retry-After", 300))
                        pause = retry_after + random.uniform(10, 30)
                        asyncio.ensure_future(self._trigger_pause(pause, "418"))
                        self._bucket.refund(weight)
                        await asyncio.sleep(pause)
                        attempt += 1
                        if attempt > self.rest_retries:
                            raise RuntimeError("HTTP 418 (IP ban) persistente")
                        await self._bucket.acquire(weight)
                        continue

                    resp.raise_for_status()
                    data = await resp.json()
                    await asyncio.sleep(self.rest_min_sleep)
                    return data

            except (aiohttp.ClientResponseError, aiohttp.ClientError, asyncio.TimeoutError) as e:
                attempt += 1
                if attempt > self.rest_retries:
                    raise
                backoff = min(self.rest_backoff_max, 0.5 * (2 ** attempt))
                jitter  = random.uniform(0.0, 0.5)
                print(
                    f"⚠️  REST reintento {attempt}/{self.rest_retries} "
                    f"en {backoff + jitter:.1f}s — {type(e).__name__}: {e}"
                )
                await asyncio.sleep(backoff + jitter)

    # =========================================================================
    # REST — FETCH Y RELLENO DE GAP  (uso: backfill · reconexión · safety)
    # =========================================================================

    async def _fetch_and_fill(
        self,
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: Optional[int] = None,
        *,
        label: str = "gap",
    ) -> int:
        """
        Descarga velas REST en [start_ms, end_ms] y las inserta en el buffer.
        Retorna el número de velas insertadas.
        """
        key         = (symbol.upper(), interval)
        interval_ms = self._interval_ms(interval)
        now_ms      = int(time.time() * 1_000)
        end_ms      = end_ms or now_ms

        if start_ms >= end_ms:
            return 0

        # Cuántas velas entran en el rango (+ 2 de margen)
        n_candles = max(1, int((end_ms - start_ms) / interval_ms) + 2)
        limit     = min(n_candles, 1_500)
        weight    = _TokenBucket.weight_for_limit(limit)

        params: dict = {
            "symbol":    symbol.upper(),
            "interval":  interval,
            "startTime": start_ms,
            "endTime":   end_ms,
            "limit":     limit,
        }

        try:
            data = await self._fetch(f"{self.BASE_REST_URL}/fapi/v1/klines", params, weight=weight)
        except Exception as e:
            print(f"❌ REST {label} {symbol} {interval}: {e}")
            return 0

        if not data:
            return 0

        now_ms2 = int(time.time() * 1_000)
        rows    = []
        for k in data:
            try:
                rows.append(self._parse_rest_row(k, symbol, interval, int(k[6]) < now_ms2))
            except Exception:
                continue

        if rows:
            self._upsert_buffer(key, rows)
            closed = sum(1 for r in rows if r["is_closed"])
            self.gap_fills[key] += 1
            print(f"📥 [{label}] {symbol} {interval}: {closed} cerradas / {len(rows)} total")

        return len(rows)

    # =========================================================================
    # REST — BACKFILL INICIAL
    # =========================================================================

    async def _backfill_one(
        self,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        symbol: str,
        interval: str,
    ) -> None:
        """Descarga el histórico completo de un par·intervalo al arrancar."""
        key   = (symbol.upper(), interval)
        limit = int(min(
            self.rest_limits.get(interval, min(1_500, self.max_candles)),
            self.max_candles,
        ))
        if limit <= 0:
            return

        weight = _TokenBucket.weight_for_limit(limit)
        params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}

        async with sem:
            try:
                data = await self._fetch(
                    f"{self.BASE_REST_URL}/fapi/v1/klines", params, weight=weight
                )
            except Exception as e:
                print(f"🔴 Backfill {symbol} {interval}: {e}")
                return

        if not data:
            print(f"⚠️  Backfill sin datos: {symbol} {interval}")
            return

        now_ms = int(time.time() * 1_000)
        rows   = []
        for k in data:
            try:
                rows.append(self._parse_rest_row(k, symbol, interval, int(k[6]) < now_ms))
            except Exception:
                continue

        self._upsert_buffer(key, rows)
        print(f"✅ Backfill {symbol} {interval}: {len(rows)} velas")

    async def _backfill_all(self) -> None:
        """Backfill inicial de todos los pares en paralelo (con batch + semáforo)."""
        all_pairs = [(s, i) for s, ivs in self.pairs.items() for i in ivs]
        total     = len(all_pairs)
        print(f"📥 Backfill inicial: {total} pares")

        # Sesión temporal solo para el backfill (alta concurrencia inicial)
        connector = aiohttp.TCPConnector(limit=self.rest_concurrency * 2)
        sem       = asyncio.Semaphore(self.rest_concurrency)

        async with aiohttp.ClientSession(connector=connector) as session:
            # Guardamos sesión temporalmente para que _backfill_one llame a _fetch
            # a través de la misma lógica de rate limit
            _prev, self._session = self._session, session
            try:
                for batch_start in range(0, total, self.backfill_batch_size):
                    if not self._running:
                        break
                    batch = all_pairs[batch_start : batch_start + self.backfill_batch_size]
                    results = await asyncio.gather(
                        *[self._backfill_one(session, sem, s, i) for s, i in batch],
                        return_exceptions=True,
                    )
                    for r in results:
                        if isinstance(r, Exception):
                            print(f"❌ Error backfill: {r}")
                    if batch_start + self.backfill_batch_size < total:
                        await asyncio.sleep(self.backfill_batch_delay)
            finally:
                self._session = _prev

        print(f"✅ Backfill completado ({total} pares)")

    # =========================================================================
    # WEBSOCKET — HANDLER DE KLINE  (O(1), sin sort, sin upsert)
    # =========================================================================

    def _handle_ws_kline(self, symbol: str, interval: str, k: dict) -> None:
        """
        Actualiza el buffer con un mensaje kline del WebSocket.

        Lógica:
          • Mismo open_time  → actualiza la vela en su lugar (in-place update).
          • open_time nuevo  → cierra la vela anterior y añade la nueva.
          • open_time viejo  → mensaje desordenado, se descarta silenciosamente.
                               (los gaps se rellenan por REST en reconexión/safety)

        Complejidad: O(1). Sin sort, sin merge de dicts completos.
        """
        now_ms     = int(time.time() * 1_000)
        is_closed  = bool(k.get("x", False))
        close_time = int(k.get("T", 0))

        # Cerrar por reloj si close_time ya pasó (pares ilíquidos sin x=true)
        if close_time > 0 and (close_time + self.CLOSE_GRACE_MS) < now_ms:
            is_closed = True

        row = self._build_ws_row(k, is_closed)

        # Si include_open=False solo nos interesan velas ya cerradas
        if not is_closed and not self.include_open:
            return

        key = (symbol, interval)

        with self.lock:
            buf = self.buffers[key]

            if not buf:
                buf.append(row)
                return

            last_ot = buf[-1]["open_time"]

            if row["open_time"] == last_ot:
                # Misma vela: actualización in-place
                buf[-1] = row

            elif row["open_time"] > last_ot:
                # Nueva vela: cierra la anterior si seguía abierta
                if not buf[-1].get("is_closed", False):
                    prev            = dict(buf[-1])
                    prev["is_closed"] = True
                    buf[-1]         = prev
                buf.append(row)

            # else: open_time < last_ot → mensaje desordenado → descartar

    # =========================================================================
    # WEBSOCKET — CONEXIÓN Y RECONEXIÓN
    # =========================================================================

    async def _ws_stream(self, stream_names: List[str], group_id: int) -> None:
        """
        Mantiene una conexión WebSocket multiplexada para un grupo de streams.

        Al reconectar: solicita _fill_reconnect_gap para rellenar el período
        en que estuvo desconectado mediante una sola llamada REST quirúrgica.
        """
        url   = f"{self.BASE_WS_URL}?streams={'/'.join(stream_names)}"
        gname = f"group_{group_id}"

        self.connection_stats[gname]["streams"] = stream_names

        reconnect_delay    = 1.0
        consecutive_errors = 0

        while self._running:
            try:
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=10,
                    max_size=10 ** 7,
                    max_queue=2_000,
                    compression=None,
                ) as ws:
                    # ── Reconexión: rellenar gap ──────────────────────────────
                    disconnect_time = self._ws_disconnect_time.pop(group_id, None)
                    if disconnect_time is not None:
                        elapsed = time.time() - disconnect_time
                        print(
                            f"🔄 {gname}: reconectado tras {elapsed:.1f}s "
                            f"— rellenando gap por REST..."
                        )
                        self._register_task(
                            ("gapfill", group_id, int(time.time() * 1000)),
                            asyncio.create_task(
                                self._fill_reconnect_gap(group_id, stream_names, disconnect_time)
                            ),
                        )

                    self.connection_stats[gname]["active"] = True
                    reconnect_delay    = 1.0
                    consecutive_errors = 0
                    print(f"✅ WS {gname}: {len(stream_names)} streams")

                    # ── Bucle de mensajes ─────────────────────────────────────
                    while self._running:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=45)
                        except asyncio.TimeoutError:
                            # Keepalive: Binance no respondió; enviamos ping manual
                            await ws.ping()
                            continue
                        except websockets.ConnectionClosed as e:
                            print(f"🔶 WS cerrado {gname}: {e}")
                            raise

                        try:
                            msg = json.loads(raw)
                            if "stream" not in msg or "data" not in msg:
                                continue
                            ev = msg["data"]
                            if ev.get("e") != "kline":
                                continue
                            stream_name = msg["stream"]
                            if stream_name not in self.stream_mapping:
                                continue

                            symbol, interval = self.stream_mapping[stream_name]
                            k = ev.get("k", {})

                            key = (symbol, interval)
                            self.last_message_time[key] = time.time()
                            self.message_counts[key]   += 1

                            self._handle_ws_kline(symbol, interval, k)

                        except Exception:
                            pass   # No dejar caer el bucle por un mensaje malformado

            except asyncio.CancelledError:
                break
            except Exception as e:
                consecutive_errors += 1

                # Guardar timestamp de desconexión (para gap fill al reconectar)
                if group_id not in self._ws_disconnect_time:
                    self._ws_disconnect_time[group_id] = time.time()

                self.connection_stats[gname].update({
                    "reconnects": self.connection_stats[gname]["reconnects"] + 1,
                    "last_error": str(e),
                    "active":     False,
                })
                reconnect_delay = min(reconnect_delay * 1.5, 30.0)
                if consecutive_errors > 5:
                    reconnect_delay = 60.0

                print(
                    f"🔴 {gname}: {e} "
                    f"— reconectando en {reconnect_delay:.1f}s (intento {consecutive_errors})"
                )
                await asyncio.sleep(reconnect_delay)

        self.connection_stats[gname]["active"] = False

    # =========================================================================
    # GAP FILL POST-RECONEXIÓN
    # =========================================================================

    async def _fill_reconnect_gap(
        self,
        group_id: int,
        stream_names: List[str],
        disconnect_time: float,
    ) -> None:
        """
        Descarga via REST SOLO las velas del período de desconexión.
        Se llama una única vez al reconectar, con concurrencia limitada.
        """
        # 2 segundos antes de la desconexión como margen de seguridad
        start_ms     = int(disconnect_time * 1_000) - 2_000
        pairs        = [self.stream_mapping[s] for s in stream_names if s in self.stream_mapping]
        sem          = asyncio.Semaphore(5)   # concurrencia baja: no saturar REST

        async def _one(symbol: str, interval: str) -> None:
            async with sem:
                await self._fetch_and_fill(symbol, interval, start_ms, label="reconnect")

        results = await asyncio.gather(
            *[_one(s, i) for s, i in pairs],
            return_exceptions=True,
        )
        for r in results:
            if isinstance(r, Exception):
                print(f"❌ Gap fill reconexión: {r}")

    # =========================================================================
    # MONITOR DE RELOJ  —  cierre duro de velas por close_time
    # =========================================================================

    async def _candle_close_monitor(self) -> None:
        """
        Cierra velas por reloj cada CLOCK_MONITOR_INTERVAL segundos.

        Por qué existe: Binance puede omitir el campo x=true para pares ilíquidos.
        Este monitor es la fuente autoritativa de cierre.

        Costo: O(pares activos) cada segundo, solo lectura del último elemento.
        """
        print(f"⏰ Monitor de reloj: cada {self.CLOCK_MONITOR_INTERVAL}s")
        while self._running:
            try:
                await asyncio.sleep(self.CLOCK_MONITOR_INTERVAL)
                now_ms = int(time.time() * 1_000)
                with self.lock:
                    for key, buf in self.buffers.items():
                        if not buf:
                            continue
                        last = buf[-1]
                        if last.get("is_closed", False):
                            continue
                        ct = last.get("close_time", 0)
                        if ct > 0 and (ct + self.CLOSE_GRACE_MS) < now_ms:
                            closed             = dict(last)
                            closed["is_closed"] = True
                            buf[-1]            = closed
                            self.clock_closes[key] += 1
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"❌ Monitor reloj: {e}")
                await asyncio.sleep(5)

    # =========================================================================
    # SAFETY REFRESH  —  red de seguridad de último recurso (cada N min)
    # =========================================================================

    async def _safety_refresh(self) -> None:
        """
        Se ejecuta cada safety_refresh_interval_seconds (default: 10 min).

        Actúa SOLO si encuentra un gap REAL: la última vela cerrada es más
        antigua de lo que debería ser dado el tiempo transcurrido.
        Esto cubre el caso teórico de que tanto WS como el gap fill fallen.

        En operación normal → nunca hace peticiones REST.
        """
        print(f"🛡  Safety refresh: cada {self.safety_refresh_interval_seconds}s (último recurso)")
        await asyncio.sleep(120)   # Esperar estabilización del sistema

        while self._running:
            try:
                await asyncio.sleep(self.safety_refresh_interval_seconds)
                if not self._running:
                    break

                now_ms    = int(time.time() * 1_000)
                gaps_found = 0

                for symbol, intervals in self.pairs.items():
                    for interval in intervals:
                        key         = (symbol.upper(), interval)
                        interval_ms = self._interval_ms(interval)

                        with self.lock:
                            buf = list(self.buffers.get(key, deque()))

                        if not buf:
                            continue

                        # Buscar última vela cerrada
                        closed = [r for r in buf if r.get("is_closed", False)]
                        if not closed:
                            continue

                        last_closed      = closed[-1]
                        expected_next_ot = last_closed["open_time"] + interval_ms
                        expected_next_ct = expected_next_ot + interval_ms - 1

                        # Solo actuar si hay al menos UN intervalo completo sin cubrir
                        if now_ms < expected_next_ct + interval_ms:
                            continue   # Demasiado pronto, no es un gap real

                        # Hay un gap real: descargarlo
                        start_ms = expected_next_ot
                        gaps_found += 1
                        self._register_task(
                            ("safety", symbol.upper(), interval, int(time.time() * 1000)),
                            asyncio.create_task(
                                self._fetch_and_fill(symbol, interval, start_ms, label="safety")
                            ),
                        )

                if gaps_found:
                    print(f"🛡  Safety: {gaps_found} pares con gaps reales → reparando")

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"❌ Safety refresh: {e}")
                await asyncio.sleep(60)

    # =========================================================================
    # MONITOR DE SALUD DE STREAMS
    # =========================================================================

    async def _stream_health_monitor(self) -> None:
        """
        Detecta grupos WS silenciosos y fuerza reconexión.

        Un grupo se considera muerto si la MAYORÍA de sus pares llevan más de
        stream_silence_threshold_seconds sin mensajes.
        Usar mayoría (≥ 50 %) evita reconexiones falsas por pares muy ilíquidos.
        """
        print(
            f"🏥 Health monitor: check cada {self.stream_health_check_seconds}s "
            f"(umbral silencio: {self.stream_silence_threshold_seconds}s)"
        )
        await asyncio.sleep(90)   # Esperar a que el sistema esté estable

        while self._running:
            try:
                await asyncio.sleep(self.stream_health_check_seconds)
                if not self._running:
                    break

                now = time.time()

                for gname, stats in list(self.connection_stats.items()):
                    if not stats.get("active"):
                        continue
                    streams = stats.get("streams", [])
                    if not streams:
                        continue

                    pairs_in_group = [
                        self.stream_mapping[s]
                        for s in streams
                        if s in self.stream_mapping
                    ]
                    if not pairs_in_group:
                        continue

                    # Contar pares que han recibido al menos un mensaje y están silenciosos
                    heard = [p for p in pairs_in_group if p in self.last_message_time]
                    if not heard:
                        continue   # Nunca recibieron mensajes (pares muy ilíquidos normales)

                    silent = [
                        p for p in heard
                        if (now - self.last_message_time[p]) > self.stream_silence_threshold_seconds
                    ]

                    # Solo reconectar si ≥ 50 % de los pares con historia están silenciosos
                    if len(silent) < max(1, len(heard) // 2):
                        continue

                    try:
                        gid = int(gname.split("_")[1])
                    except (IndexError, ValueError):
                        continue

                    print(
                        f"⚠️  {gname}: {len(silent)}/{len(heard)} streams silenciosos "
                        f"→ forzando reconexión"
                    )

                    old_task = self._tasks.get(("stream", gid))
                    if old_task and not old_task.done():
                        old_task.cancel()
                        await asyncio.sleep(1.0)

                    if self._running:
                        new_task = self._register_task(
                            ("stream", gid),
                            asyncio.create_task(self._ws_stream(streams, gid)),
                        )

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"❌ Health monitor: {e}")
                await asyncio.sleep(60)

    # =========================================================================
    # MONITOR DE CONEXIONES  (log periódico de estado)
    # =========================================================================

    async def _monitor_connections(self) -> None:
        while self._running:
            await asyncio.sleep(60)
            if not self._running:
                break
            active     = sum(1 for s in self.connection_stats.values() if s.get("active"))
            total      = len(self.connection_stats)
            gap_total  = sum(self.gap_fills.values())
            clock_total = sum(self.clock_closes.values())
            msg_total  = sum(self.message_counts.values())
            print(
                f"🔌 WS: {active}/{total} activas "
                f"| Msgs: {msg_total} "
                f"| Cierres reloj: {clock_total} "
                f"| Gap fills: {gap_total} "
                f"| Tokens RL: {self._bucket._tokens:.0f}"
            )

    # =========================================================================
    # GRUPOS DE STREAMS
    # =========================================================================

    def _create_stream_groups(self) -> List[List[str]]:
        """Construye los grupos multiplexados de streams WS."""
        all_streams: List[str] = []
        self.stream_mapping.clear()
        self.subscribed_streams.clear()

        for symbol, intervals in self.pairs.items():
            for interval in intervals:
                name = f"{symbol.lower()}@kline_{interval}"
                all_streams.append(name)
                self.stream_mapping[name]  = (symbol.upper(), interval)
                self.subscribed_streams.add((symbol.upper(), interval))

        groups = [
            all_streams[i : i + self.streams_per_connection]
            for i in range(0, len(all_streams), self.streams_per_connection)
        ]
        print(f"📋 {len(all_streams)} streams → {len(groups)} conexiones WS")
        for idx, g in enumerate(groups, 1):
            print(f"   Grupo {idx}: {len(g)} streams")
        return groups

    # =========================================================================
    # CICLO DE VIDA
    # =========================================================================

    def start(self) -> None:
        print("\n" + "=" * 70)
        print("🚀 KlineWebSocketCache v4  —  WS-first · Zero-REST normal")
        print("=" * 70)

        self._running = True
        loop          = asyncio.new_event_loop()
        self._loop    = loop

        thread = threading.Thread(
            target=lambda: (asyncio.set_event_loop(loop), loop.run_forever()),
            daemon=True,
            name="KlineWSLoop",
        )
        thread.start()
        self._thread = thread

        async def _startup() -> None:
            # Event para pausa global 429/418 (set = libre; clear = bloqueado)
            self._rate_limit_pause = asyncio.Event()
            self._rate_limit_pause.set()

            # Sesión REST compartida de larga vida
            connector = aiohttp.TCPConnector(
                limit=self.rest_concurrency * 2,
                limit_per_host=self.rest_concurrency,
                keepalive_timeout=30,
            )
            self._session = aiohttp.ClientSession(connector=connector)

            stream_groups = self._create_stream_groups()

            # ── Backfill inicial ──────────────────────────────────────────────
            if self.backfill_on_start:
                print("\n📥 Ejecutando backfill inicial…")
                await self._backfill_all()

            # ── WebSocket connections ─────────────────────────────────────────
            for idx, group in enumerate(stream_groups, 1):
                task = self._register_task(
                    ("stream", idx),
                    asyncio.create_task(self._ws_stream(group, idx)),
                )

            # ── Tareas de mantenimiento ───────────────────────────────────────
            self._register_task(("maintenance", "candle_close"), asyncio.create_task(self._candle_close_monitor()))   # siempre activo
            self._register_task(("maintenance", "safety"), asyncio.create_task(self._safety_refresh()))          # último recurso
            self._register_task(("maintenance", "health"), asyncio.create_task(self._stream_health_monitor()))   # detecta WS muertos
            self._register_task(("maintenance", "monitor"), asyncio.create_task(self._monitor_connections()))     # log periódico

            n = sum(len(ivs) for ivs in self.pairs.values())
            print(f"\n✅ KlineWebSocketCache v4 iniciado")
            print(f"   • Pares/intervalos   : {len(self.pairs)}/{n}")
            print(f"   • Fuente primaria    : WebSocket (zero REST en normal)")
            print(f"   • Gap fill           : REST quirúrgico al reconectar")
            print(f"   • Monitor de reloj   : cada {self.CLOCK_MONITOR_INTERVAL}s")
            print(f"   • Safety refresh     : cada {self.safety_refresh_interval_seconds}s (último recurso)")
            print(f"   • Rate limiter       : {self._bucket.capacity} cap / {self._bucket.refill_rate} t·s⁻¹")
            print(f"   • Buffer             : ventana {self.max_candles} velas/par")
            print("=" * 70 + "\n")

        self._startup_future = asyncio.run_coroutine_threadsafe(_startup(), loop)

    def stop(self) -> None:
        print("🛑 Deteniendo KlineWebSocketCache…")
        self._running = False

        if self._loop and self._loop.is_running():
            shutdown = asyncio.run_coroutine_threadsafe(self._shutdown_async(), self._loop)
            try:
                shutdown.result(timeout=15)
            except Exception as e:
                print(f"⚠️  Error en apagado limpio: {e}")

            self._loop.call_soon_threadsafe(self._loop.stop)

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3.0)

        self._loop = None
        self._thread = None
        self._startup_future = None

        # Fallback: si stop() se llama sin loop activo, igual libera estado.
        with self.lock:
            for buf in self.buffers.values():
                buf.clear()
            self.buffers.clear()

        self.last_message_time.clear()
        self.message_counts.clear()
        self.clock_closes.clear()
        self.gap_fills.clear()
        self._ws_disconnect_time.clear()
        self.connection_stats.clear()
        self.stream_mapping.clear()
        self.subscribed_streams.clear()
        self._tasks.clear()
        self._rate_limit_pause = None

        gc.collect()
        print("✅ KlineWebSocketCache detenido")

    def force_refresh(
        self,
        symbol: Optional[str] = None,
        interval: Optional[str] = None,
    ) -> None:
        """
        Fuerza una descarga REST inmediata.
        - symbol+interval → rellena solo las velas faltantes de ese par.
        - Sin argumentos  → backfill completo de todos los pares.
        """
        if not self._loop:
            print("⚠️  Loop no activo. Llama a start() primero.")
            return

        async def _do() -> None:
            if symbol and interval:
                key         = (symbol.upper(), interval)
                interval_ms = self._interval_ms(interval)
                with self.lock:
                    buf = list(self.buffers.get(key, deque()))
                closed = [r for r in buf if r.get("is_closed", False)]
                if closed:
                    start_ms = closed[-1]["open_time"] + interval_ms
                    await self._fetch_and_fill(symbol, interval, start_ms, label="force_refresh")
                else:
                    # Sin datos: descarga completa
                    limit  = min(self.rest_limits.get(interval, 1_500), self.max_candles)
                    weight = _TokenBucket.weight_for_limit(limit)
                    data   = await self._fetch(
                        f"{self.BASE_REST_URL}/fapi/v1/klines",
                        {"symbol": symbol.upper(), "interval": interval, "limit": limit},
                        weight=weight,
                    )
                    now_ms = int(time.time() * 1_000)
                    rows   = [
                        self._parse_rest_row(k, symbol, interval, int(k[6]) < now_ms)
                        for k in data
                    ]
                    self._upsert_buffer(key, rows)
                    print(f"✅ force_refresh {symbol} {interval}: {len(rows)} velas")
            else:
                await self._backfill_all()

        asyncio.run_coroutine_threadsafe(_do(), self._loop)

    # =========================================================================
    # CONSULTA DE DATOS
    # =========================================================================

    def get_dataframe(
        self,
        symbol: str,
        interval: str,
        only_closed: bool = False,
    ) -> pd.DataFrame:
        """Retorna el buffer como DataFrame. Thread-safe."""
        key = (symbol.upper(), interval)
        with self.lock:
            rows = list(self.buffers.get(key, deque()))

        if not rows:
            return pd.DataFrame(columns=[
                "timestamp", "open", "high", "low", "close", "volume",
                "close_time", "trades", "quote_volume",
                "taker_buy_volume", "taker_buy_quote_volume", "is_closed",
            ])

        df = pd.DataFrame(rows)
        if only_closed:
            df = df[df["is_closed"]].copy()

        df["timestamp"]  = pd.to_datetime(df["open_time"],  unit="ms")
        df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")

        return df[[
            "timestamp", "open", "high", "low", "close", "volume",
            "close_time", "trades", "quote_volume",
            "taker_buy_volume", "taker_buy_quote_volume", "is_closed",
        ]].reset_index(drop=True)

    def get_last_closed(self, symbol: str, interval: str) -> Optional[dict]:
        """Retorna la última vela CERRADA como dict, o None si no hay."""
        df = self.get_dataframe(symbol, interval, only_closed=True)
        return None if df.empty else df.iloc[-1].to_dict()

    def get_stream_health(self) -> dict:
        """Estado de salud por (symbol, interval)."""
        now = time.time()
        return {
            key: {
                "last_message_ago": (
                    f"{now - self.last_message_time[key]:.0f}s"
                    if key in self.last_message_time else "never"
                ),
                "message_count": self.message_counts.get(key, 0),
                "clock_closes":  self.clock_closes.get(key, 0),
                "gap_fills":     self.gap_fills.get(key, 0),
                "is_healthy": (
                    (now - self.last_message_time.get(key, 0))
                    < self.stream_silence_threshold_seconds
                    if key in self.last_message_time else False
                ),
            }
            for key in self.subscribed_streams
        }

    def get_stats(self) -> dict:
        """Resumen general del estado del sistema."""
        with self.lock:
            total_candles = sum(len(b) for b in self.buffers.values())
            with_data     = sum(1 for b in self.buffers.values() if b)
        return {
            "total_pairs":          len(self.buffers),
            "pairs_with_data":      with_data,
            "total_candles":        total_candles,
            "avg_candles_per_pair": total_candles / max(with_data, 1),
            "total_messages":       sum(self.message_counts.values()),
            "total_clock_closes":   sum(self.clock_closes.values()),
            "total_gap_fills":      sum(self.gap_fills.values()),
            "active_connections":   sum(1 for s in self.connection_stats.values() if s.get("active")),
            "total_connections":    len(self.connection_stats),
            "rate_limiter_tokens":  round(self._bucket._tokens, 1),
        }


# =============================================================================
# EJEMPLO DE USO
# =============================================================================

if __name__ == "__main__":
    import os

    pairs = {
        "BTCUSDT": ["1m", "5m", "15m", "1h"],
        "ETHUSDT": ["1m", "5m"],
        "BNBUSDT": ["1m"],
        "SOLUSDT": ["1m", "5m"],
    }

    cache = KlineWebSocketCache(
        pairs=pairs,
        max_candles=1_500,
        include_open_candle=True,
        backfill_on_start=True,
        streams_per_connection=40,
        rest_concurrency=20,
        rest_retries=4,
        rest_backoff_max=30.0,
        rest_min_sleep=0.05,
        backfill_batch_size=5,
        backfill_batch_delay=0.1,
        rate_limit_capacity=1_200,
        rate_limit_refill=20.0,
        stream_silence_threshold_seconds=120,
        stream_health_check_seconds=60,
        safety_refresh_interval_seconds=600,
    )

    cache.start()

    try:
        while True:
            time.sleep(10)
            os.system("cls" if os.name == "nt" else "clear")

            print("=" * 70)
            print(f"📊 KlineWebSocketCache v4 — {datetime.now().strftime('%H:%M:%S')}")
            print("=" * 70)

            stats = cache.get_stats()
            print(f"\n📈 General:")
            print(f"  WS activas          : {stats['active_connections']}/{stats['total_connections']}")
            print(f"  Pares con datos     : {stats['pairs_with_data']}/{stats['total_pairs']}")
            print(f"  Total velas         : {stats['total_candles']}")
            print(f"  Prom. velas/par     : {stats['avg_candles_per_pair']:.1f}")
            print(f"  Msgs WS recibidos   : {stats['total_messages']}")
            print(f"  Cierres por reloj   : {stats['total_clock_closes']}")
            print(f"  Gap fills REST      : {stats['total_gap_fills']}")
            print(f"  Tokens rate-limiter : {stats['rate_limiter_tokens']}")

            health  = cache.get_stream_health()
            healthy = sum(1 for h in health.values() if h["is_healthy"])
            print(f"\n🏥 Streams saludables: {healthy}/{len(health)}")

            unhealthy = [(k, v) for k, v in health.items() if not v["is_healthy"]]
            if unhealthy:
                print("  ⚠️  Con problemas:")
                for key, info in unhealthy[:5]:
                    sym, itv = key
                    print(f"     • {sym} {itv}: {info['last_message_ago']} sin mensajes")

            print(f"\n📊 Últimas velas cerradas:")
            print("-" * 70)
            for symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
                for interval in ["1m", "5m", "15m", "1h"]:
                    if symbol in cache.pairs and interval in cache.pairs[symbol]:
                        last = cache.get_last_closed(symbol, interval)
                        if last:
                            df = cache.get_dataframe(symbol, interval, only_closed=True)
                            print(
                                f"{symbol:10s} {interval:3s}: "
                                f"${last['close']:10.2f}  "
                                f"Vol:{last['volume']:10.2f}  "
                                f"Velas:{len(df):4d}  "
                                f"Msgs:{cache.message_counts.get((symbol, interval), 0):5d}  "
                                f"Gaps:{cache.gap_fills.get((symbol, interval), 0):3d}  "
                                f"[{last['timestamp']}]"
                            )

            print("\n" + "=" * 70)
            print("Ctrl+C para detener")

    except KeyboardInterrupt:
        print("\n🛑 Deteniendo…")
        cache.stop()
        print("✅ Finalizado")
