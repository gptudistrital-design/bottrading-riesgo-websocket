"""
state_store.py — Persistencia externa del estado del bot para sobrevivir reinicios.

En Render free el disco es efímero: cada reinicio, redeploy o spin-down borra
los archivos locales. Este módulo guarda fuera del contenedor, en Upstash Redis
(API REST, sin dependencias extra), lo necesario para retomar la operación:

  <prefijo>:state   documento JSON con las posiciones ABIERTAS (con todos sus
                    tramos y datos), cooldowns, secuencia de trade_id, SL global
                    (si se cambió desde la web), bloqueos por precio y PnL
                    realizado. Al cerrarse una posición desaparece del documento.
  <prefijo>:trades  lista con el historial de cierres (MFE/MAE), últimos N.
  <prefijo>:owner   "<instancia>|<secuencia>" con caducidad (lease): qué instancia
                    controla el estado. Caduca sola si esa instancia muere. Al
                    apagarse se marca "|released": libre para la siguiente, pero
                    rechaza cualquier escritura atrasada de la que se fue.

Garantías
  • Una sola instancia opera a la vez. La que arranca espera a que la anterior
    suelte el control (apagado ordenado: deja de operar, guarda y lo libera) o a
    que caduque su lease (OWNER_LEASE_S) si murió sin avisar. Solo entonces lee
    el estado, así que nada de lo que hizo la anterior se pierde.
  • Cada escritura comprueba, en el mismo script Lua, que esta instancia sigue
    siendo la dueña y que no es más antigua que la última aplicada. Si otra
    instancia tomó el control, esta pasa a standby y lo retoma si la otra lo
    suelta o deja de renovarlo.
  • Un único hilo escribe, siempre el documento COMPLETO. El cierre (posición
    fuera del documento) y su fila del historial van en la misma operación.
  • Si Upstash no responde, se reintenta con backoff sin perder cambios. Sin
    confirmación reciente, o con una apertura/cierre sin guardar más de
    CRIT_STALL_S, no se abren posiciones nuevas; las abiertas se siguen cerrando.
  • Coste: aperturas, cierres y cambios de SL se guardan al instante; lo demás
    (MFE/MAE) va con el latido, uno cada HEARTBEAT_S. Upstash cuenta cada comando
    de los scripts: ~200-270k comandos/mes de los 500k del plan gratis.
"""

from __future__ import annotations

import json
import os
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from typing import Any, Callable, Deque, List, Optional, Tuple


HEARTBEAT_S     = float(os.getenv("STATE_HEARTBEAT_S", "40"))
OWNER_LEASE_S   = float(os.getenv("STATE_OWNER_LEASE_S", "120"))  # sin renovar, el control caduca
CRIT_STALL_S    = 10.0     # apertura/cierre sin guardar más de esto → sin entradas nuevas
CLOSE_RETRY_S   = 12.0     # reintentos del guardado final (Render da 30 s entre SIGTERM y SIGKILL)
DEBOUNCE_S      = 0.3
WAIT_POLL_S     = 3.0      # sondeo mientras otra instancia es la dueña (deploy con solapamiento)
WAIT_FAST_FOR_S = 600.0    # después, sondeo al ritmo del lease (ahorra comandos)
BACKOFF_MAX_S   = 30.0

Restore = Tuple[Optional[dict], List[dict], str]


class StoreError(RuntimeError):
    pass


# Los marcadores "-- botshort:<nombre>" identifican cada script en los logs de Upstash.
# KEYS: owner, state, trades.  El dueño se guarda como "<instancia>|<secuencia>"
# ("…|released" tras un apagado ordenado: libre para tomarlo).
_ACQUIRE_LUA = """-- botshort:acquire
local cur = redis.call('GET', KEYS[1])
if cur and string.sub(cur, -9) ~= '|released' then
  local who = string.match(cur, '^(.-)|%d+$') or cur
  if who ~= ARGV[1] then return {0, who, redis.call('PTTL', KEYS[1])} end
end
redis.call('SET', KEYS[1], ARGV[1] .. '|' .. ARGV[2], 'PX', ARGV[3])
local trades = {}
local n = tonumber(ARGV[4])
if n > 0 then trades = redis.call('LRANGE', KEYS[3], -n, -1) end
return {1, redis.call('GET', KEYS[2]) or '', trades}
"""

# ARGV: instancia, secuencia, lease_ms, liberar(0/1), documento ('' = solo latido),
#       máximo del historial, filas nuevas del historial...
# Devuelve 1 = guardado, 0 = otra instancia es la dueña, 2 = escritura atrasada
# (ignorada), 3 = esta instancia ya liberó el control (ignorada).
_WRITE_LUA = """-- botshort:write
local cur = redis.call('GET', KEYS[1])
if cur then
  if string.sub(cur, -9) == '|released' then
    if string.sub(cur, 1, #ARGV[1] + 1) == ARGV[1] .. '|' then return 3 end
    return 0
  end
  local who, last = string.match(cur, '^(.-)|(%d+)$')
  if not who then who, last = cur, '0' end
  if who ~= ARGV[1] then return 0 end
  if tonumber(ARGV[2]) <= tonumber(last) then return 2 end
end
if ARGV[5] ~= '' then redis.call('SET', KEYS[2], ARGV[5]) end
if #ARGV > 6 then
  for i = 7, #ARGV do redis.call('RPUSH', KEYS[3], ARGV[i]) end
  redis.call('LTRIM', KEYS[3], -tonumber(ARGV[6]), -1)
end
if ARGV[4] == '1' then
  redis.call('SET', KEYS[1], ARGV[1] .. '|' .. ARGV[2] .. '|released', 'PX', ARGV[3])
else
  redis.call('SET', KEYS[1], ARGV[1] .. '|' .. ARGV[2], 'PX', ARGV[3])
end
return 1
"""


class UpstashRedis:
    """Cliente mínimo de la API REST de Upstash Redis (solo urllib)."""

    def __init__(self, url: str, token: str, timeout: float = 5.0) -> None:
        self.url = url.strip().rstrip("/")
        self.token = token.strip()
        self.timeout = timeout

    def cmd(self, *args: Any) -> Any:
        body = json.dumps([str(a) if not isinstance(a, str) else a for a in args]).encode("utf-8")
        req = urllib.request.Request(
            self.url, data=body, method="POST",
            headers={"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:200]
            raise StoreError(f"Upstash HTTP {exc.code}: {detail}") from exc
        except Exception as exc:
            raise StoreError(f"Upstash no responde: {exc}") from exc
        if isinstance(data, dict) and data.get("error"):
            raise StoreError(f"Upstash: {data['error']}")
        return data.get("result") if isinstance(data, dict) else data


def dedupe_trades(rows: List[dict]) -> List[dict]:
    """Quita filas repetidas del historial (un reintento ambiguo puede repetir un envío)."""
    seen, out = set(), []
    for r in rows:
        if not isinstance(r, dict):
            continue
        key = (r.get("trade_id"), r.get("closed_at_ts"))
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


class StateStore:
    """Guarda y recupera el documento de estado. Un hilo propio hace toda la E/S.

    Ciclo de vida: start() → (otra instancia es la dueña: espera) → toma el
    control → take_restore() devuelve el estado guardado → el bot lo aplica y
    llama a enable_writes() → can_act()/can_open() → begin_stop() + close().
    """

    def __init__(self, backend: Optional[UpstashRedis], local_path: str, owner_id: str,
                 build_doc: Callable[[], Tuple[dict, List[dict]]], log: Callable[[str], None],
                 on_fenced: Callable[[], None], key_prefix: str = "botshort",
                 max_trades: int = 5000) -> None:
        self.backend    = backend
        self.local_path = local_path
        self.owner_id   = owner_id.replace("|", "-")
        self.build_doc  = build_doc          # → (documento, peek_trades()) tomados a la vez
        self.log        = log
        self.on_fenced  = on_fenced
        self.max_trades = max(1, int(max_trades))
        self.k_owner    = f"{key_prefix}:owner"
        self.k_state    = f"{key_prefix}:state"
        self.k_trades   = f"{key_prefix}:trades"

        self.acquired    = threading.Event()   # tomó el control al menos una vez
        self.boot_source = ""
        self.is_owner    = False               # tiene el lease en Upstash
        self.writes_enabled = False            # el bot ya aplicó el estado recuperado
        self.fenced      = False               # otra instancia le quitó el control
        self.stopping    = False
        self.holder      = ""                  # dueña actual mientras esta espera

        self.last_ok    = 0.0                  # último contacto confirmado como dueña
        self.last_write = 0.0                  # último documento guardado
        self.last_error = ""
        self.writes = 0
        self.failures = 0
        self.last_doc: Optional[dict] = None

        self._lock = threading.Lock()
        self._io_lock = threading.Lock()       # una sola operación remota a la vez (hilo + apagado)
        self._wake = threading.Event()
        self._crit_ver = 0                     # cambios importantes (abrir/cerrar/SL)
        self._soft_ver = 0                     # cambios menores (MFE/MAE, bloqueos)
        self._done_crit = 0
        self._done_soft = 0
        self._crit_since = 0.0
        self._trades: Deque[dict] = deque()
        self._restore: Optional[Restore] = None
        self._gen = 0                          # nº de veces que tomó el control
        self._taken_gen = -1
        self._seq = 0                          # secuencia de escrituras (rechaza las atrasadas)
        self._last_body: Optional[str] = None  # último documento guardado (sin marcas de tiempo)
        self._waiting_since = 0.0
        self._next_try = 0.0
        self._backoff = 1.0
        self._stop = False
        self._thread: Optional[threading.Thread] = None

    # ── Estado ───────────────────────────────────────────────────────────────

    @property
    def remote(self) -> bool:
        return self.backend is not None

    def can_act(self) -> bool:
        """¿Puede esta instancia cerrar posiciones, cambiar SL o mandar señales?"""
        return self.writes_enabled and not self.stopping

    def can_open(self) -> bool:
        """¿Se pueden abrir posiciones nuevas? Solo si su persistencia está garantizada."""
        if not self.can_act():
            return False
        if not self.remote:
            return True
        now = time.time()
        if now - self.last_ok > OWNER_LEASE_S / 2:
            return False
        with self._lock:
            stalled = self._crit_ver != self._done_crit and now - self._crit_since > CRIT_STALL_S
        return not stalled

    def status(self) -> dict:
        now = time.time()
        if self.stopping:
            mode = "apagando"
        elif not self.remote:
            mode = "local" if self.writes_enabled else "recuperando"
        elif not self.is_owner:
            mode = "standby" if self.fenced else "esperando control"
        elif not self.writes_enabled:
            mode = "recuperando"
        elif now - self.last_ok <= OWNER_LEASE_S / 2:
            mode = "ok"
        else:
            mode = "sin conexión"
        return {
            "backend":       "upstash" if self.remote else "local",
            "mode":          mode,
            "owner":         self.owner_id,
            "holder":        self.holder,
            "restored_from": self.boot_source,
            "last_ok_ago_s": round(now - self.last_ok, 1) if self.last_ok else None,
            "last_write_ago_s": round(now - self.last_write, 1) if self.last_write else None,
            "writes":        self.writes,
            "failures":      self.failures,
            "pending":       self._pending_critical() or bool(self._trades),
            "last_error":    self.last_error,
        }

    # ── API para el bot ──────────────────────────────────────────────────────

    def mark_dirty(self, critical: bool = True) -> None:
        with self._lock:
            if critical:
                if self._crit_ver == self._done_crit:
                    self._crit_since = time.time()
                self._crit_ver += 1
            else:
                self._soft_ver += 1
        if critical:
            self._wake.set()

    def append_trade(self, rec: dict) -> None:
        """Fila del historial. Llamar en la MISMA sección crítica que saca la
        posición del estado, para que ambas cosas viajen juntas."""
        with self._lock:
            self._trades.append(rec)
        self.mark_dirty(critical=True)

    def peek_trades(self) -> List[dict]:
        """Filas pendientes de envío (para build_doc, bajo el mismo lock que el documento)."""
        with self._lock:
            return list(self._trades)

    def restore_pending(self) -> bool:
        return self._restore is not None

    def take_restore(self) -> Optional[Restore]:
        """(documento, historial, origen) recién leído al tomar el control, una sola vez."""
        with self._lock:
            r, self._restore = self._restore, None
            if r is not None:
                self._taken_gen = self._gen
            return r

    def enable_writes(self) -> bool:
        """El bot aplicó el estado: a partir de aquí opera y guarda."""
        with self._lock:
            if (self._taken_gen != self._gen or self._restore is not None
                    or (self.remote and not self.is_owner)):
                return False                    # perdió el control mientras tanto
            self.writes_enabled = True
        self.mark_dirty(critical=True)
        return True

    def start(self) -> None:
        if self._thread is not None:
            return
        if not self.remote:
            doc = self.read_local()
            source = "copia local" if doc else "vacío (sin estado previo)"
            with self._lock:
                self._gen += 1
                self._restore = (doc, [], source)
            self.boot_source = source
            self.acquired.set()
        self._thread = threading.Thread(target=self._run, name="state-store", daemon=True)
        self._thread.start()

    def begin_stop(self) -> None:
        """Deja de abrir y cerrar posiciones (antes del guardado final)."""
        self.stopping = True

    def close(self) -> bool:
        """Apagado ordenado: guardado final y libera el control para la siguiente
        instancia. Si Upstash falla, reintenta durante CLOSE_RETRY_S."""
        self.stopping = True
        ok = False
        deadline = time.time() + CLOSE_RETRY_S
        with self._io_lock:
            try:
                while not self._stop and (self.is_owner or (not self.remote and self.writes_enabled)):
                    try:
                        ok = self._flush_locked(force=True, release=self.remote)
                        break
                    except Exception as exc:
                        self.last_error = str(exc)
                        if time.time() + 1.0 >= deadline:
                            self.log(f"[estado] no pude hacer el guardado final: {exc}")
                            break
                        self.log(f"[estado] guardado final falló ({exc}); reintento")
                        time.sleep(1.0)
            finally:
                self._stop = True
                self._wake.set()
        return ok

    def read_local(self) -> Optional[dict]:
        try:
            with open(self.local_path, "r", encoding="utf-8") as fh:
                doc = json.load(fh)
            return doc if isinstance(doc, dict) else None
        except FileNotFoundError:
            return None
        except Exception as exc:
            self.log(f"[estado] copia local ilegible ({exc}); se ignora")
            return None

    # ── Hilo de E/S ──────────────────────────────────────────────────────────

    def _pending_critical(self) -> bool:
        with self._lock:
            return self._crit_ver != self._done_crit

    def _lease_ms(self) -> int:
        return max(100, int(OWNER_LEASE_S * 1000))

    def _run(self) -> None:
        while not self._stop:
            wait = self._next_try - time.time()
            self._wake.wait(timeout=min(1.0, wait) if wait > 0 else 1.0)
            self._wake.clear()
            if self._stop:
                break
            if time.time() < self._next_try:
                continue
            try:
                if self.remote and not self.is_owner:
                    self._try_acquire()
                else:
                    self._flush()
                self._backoff = 1.0
            except StoreError as exc:
                self._failed(str(exc))
            except Exception as exc:          # nunca debe morir el hilo
                self._failed(f"error inesperado: {exc!r}")

    def _failed(self, msg: str) -> None:
        self.failures += 1
        self.last_error = msg
        self._next_try = time.time() + self._backoff
        self.log(f"[estado] {msg} — reintento en {self._backoff:.0f}s")
        self._backoff = min(self._backoff * 2, BACKOFF_MAX_S)

    def _try_acquire(self) -> bool:
        """Toma el control si nadie lo tiene (o caducó) y lee el estado en la misma operación."""
        with self._io_lock:
            if self._stop or self.stopping or self.is_owner:
                return self.is_owner
            self._seq += 1
            res = self.backend.cmd("EVAL", _ACQUIRE_LUA, 3, self.k_owner, self.k_state, self.k_trades,
                                   self.owner_id, self._seq, self._lease_ms(), self.max_trades)
            if not isinstance(res, list) or not res:
                raise StoreError(f"respuesta inesperada de Upstash: {res!r}")
            now = time.time()
            if str(res[0]) != "1":
                self._wait_for_holder(str(res[1]) if len(res) > 1 else "?",
                                      res[2] if len(res) > 2 else None, now)
                return False
            remote_doc = self._parse_doc(res[1] if len(res) > 1 else "")
            trades = self._parse_trades(res[2] if len(res) > 2 else [])
            doc, source = self._pick_doc(remote_doc)
            with self._lock:
                self._gen += 1
                self._restore = (doc, trades, source)
                self._trades.clear()
                self._done_crit, self._done_soft = self._crit_ver, self._soft_ver
                self.is_owner, self.fenced, self.writes_enabled = True, False, False
            self.holder = ""
            self._waiting_since = 0.0
            self._last_body = None
            self.last_ok = now
            self.boot_source = source
            self.acquired.set()
        self.log(f"[estado] Upstash: esta instancia controla el estado ({self.owner_id}). "
                 f"Estado: {source}, historial: {len(trades)} cierres")
        return True

    def _wait_for_holder(self, holder: str, pttl: Any, now: float) -> None:
        try:
            ttl = max(0.0, float(pttl) / 1000.0)
        except (TypeError, ValueError):
            ttl = OWNER_LEASE_S
        if not self._waiting_since or holder != self.holder:
            self.log(f"[estado] Otra instancia ({holder}) controla el estado. Espero a que lo suelte "
                     f"(apagado ordenado) o a que caduque su control (≤ {ttl:.0f}s); mientras tanto "
                     f"no abro ni cierro posiciones.")
        self._waiting_since = self._waiting_since or now
        self.holder = holder
        if now - self._waiting_since < WAIT_FAST_FOR_S:
            delay = WAIT_POLL_S
        else:
            delay = max(WAIT_POLL_S, min(ttl + 1.0, OWNER_LEASE_S))
        self._next_try = now + delay

    def _parse_doc(self, raw: Any) -> Optional[dict]:
        if not raw:
            return None
        try:
            doc = json.loads(raw)
            return doc if isinstance(doc, dict) else None
        except Exception as exc:
            self.log(f"[estado] documento remoto ilegible ({exc}); se ignora")
            return None

    @staticmethod
    def _parse_trades(rows: Any) -> List[dict]:
        out: List[dict] = []
        for row in rows or []:
            try:
                out.append(json.loads(row))
            except Exception:
                continue
        return dedupe_trades(out)

    def _pick_doc(self, remote_doc: Optional[dict]) -> Tuple[Optional[dict], str]:
        """La copia local solo gana si es de la misma instancia que escribió la
        remota y más reciente (su último envío falló antes de reiniciarse)."""
        local_doc = self.read_local()
        if local_doc and (remote_doc is None or (
                local_doc.get("owner") == remote_doc.get("owner")
                and float(local_doc.get("saved_at", 0)) > float(remote_doc.get("saved_at", 0)))):
            return local_doc, ("copia local" if remote_doc is None
                               else "copia local (más reciente que Upstash)")
        if remote_doc is None:
            return None, "vacío (sin estado previo)"
        return remote_doc, "upstash"

    def _flush(self, force: bool = False) -> bool:
        with self._io_lock:
            return self._flush_locked(force)

    def _flush_locked(self, force: bool = False, release: bool = False) -> bool:
        if self._stop or (self.remote and not self.is_owner):
            return False
        now = time.time()
        with self._lock:
            crit_ver, soft_ver = self._crit_ver, self._soft_ver
            crit = crit_ver != self._done_crit
            soft = soft_ver != self._done_soft
        if crit and not (force or release) and now - self._crit_since < DEBOUNCE_S:
            self._next_try = self._crit_since + DEBOUNCE_S     # agrupa ráfagas de cambios
            return False
        due = now - (self.last_ok if self.remote else self.last_write) >= HEARTBEAT_S

        if not self.writes_enabled:
            # Tomó el control pero el bot aún aplica el estado: solo mantiene el lease.
            if self.remote and (due or release):
                return self._remote_write("", [], release)
            return True

        want_doc = force or release or crit or (soft and due)
        if not (want_doc or (self.remote and due)):
            return True
        payload, body, trades = "", None, []
        if want_doc:
            doc, trades = self.build_doc()
            body = json.dumps(doc, ensure_ascii=False, separators=(",", ":"), default=str, sort_keys=True)
            if trades or body != self._last_body:            # sin cambios: basta el latido
                doc["saved_at"] = time.time()
                doc["owner"] = self.owner_id
                payload = json.dumps(doc, ensure_ascii=False, separators=(",", ":"), default=str)
                self._write_local(payload)
                self.last_doc = doc

        if self.remote and (payload or trades or due or release):
            if not self._remote_write(payload, trades, release):
                return False

        with self._lock:
            if want_doc:
                self._done_crit, self._done_soft = crit_ver, soft_ver
            for _ in range(len(trades)):
                self._trades.popleft()
            if self._crit_ver != self._done_crit:
                self._crit_since = time.time()
        if payload:
            self.writes += 1
            self.last_write = time.time()
            self._last_body = body
        return True

    def _remote_write(self, payload: str, trades: List[dict], release: bool = False) -> bool:
        rows = [json.dumps(t, ensure_ascii=False, separators=(",", ":"), default=str) for t in trades]
        self._seq += 1
        res = self.backend.cmd("EVAL", _WRITE_LUA, 3, self.k_owner, self.k_state, self.k_trades,
                               self.owner_id, self._seq, self._lease_ms(), "1" if release else "0",
                               payload, self.max_trades, *rows)
        code = str(res)
        if code == "3" and release:
            code = "1"                          # un intento anterior ya liberó (respuesta perdida)
        if code in ("0", "3"):
            self._lose_ownership()
            return False
        if code == "2":
            self.log("[estado] Upstash ya tenía una escritura más nueva de esta instancia; se ignora esta")
        elif code != "1":
            raise StoreError(f"respuesta inesperada de Upstash: {res!r}")
        self.last_ok = time.time()
        if release:
            with self._lock:
                self.is_owner, self.writes_enabled = False, False
        return True

    def _lose_ownership(self) -> None:
        with self._lock:
            dropped = len(self._trades)
            self._trades.clear()
            self._restore = None
            self._done_crit, self._done_soft = self._crit_ver, self._soft_ver
            self.is_owner, self.writes_enabled, self.fenced = False, False, True
        self._waiting_since = time.time()
        self._next_try = time.time() + WAIT_POLL_S
        self.log("[estado] ⚠️ Otra instancia tomó el control del estado. Esta queda en STANDBY: no abre "
                 "ni cierra posiciones, y lo retomará si la otra lo suelta o deja de renovarlo"
                 + (f" ({dropped} cierre(s) sin enviar quedan solo en el historial local)" if dropped else ""))
        try:
            self.on_fenced()
        except Exception:
            pass

    def _write_local(self, payload: str) -> None:
        tmp = f"{self.local_path}.tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                fh.write(payload)
            os.replace(tmp, self.local_path)
        except Exception as exc:
            self.last_error = f"copia local: {exc}"


def make_owner_id() -> str:
    inst = os.getenv("RENDER_INSTANCE_ID", "1") or os.uname().nodename
    return f"{inst}-{os.getpid()}-{int(time.time())}".replace("|", "-")


def backend_from_env(log: Callable[[str], None]) -> Tuple[Optional[UpstashRedis], str]:
    """Upstash si hay credenciales. Fuera de Render exige STATE_STORE_FORCE=true para
    que una ejecución local no compita por el control con la instancia de producción."""
    url = os.getenv("UPSTASH_REDIS_REST_URL", "https://improved-egret-216443.upstash.io").strip()
    token = os.getenv("UPSTASH_REDIS_REST_TOKEN", "gQAAAAAAA017AAIgcDI3MjAyOTQ3N2UzZTM0NDM3YWI4OThkZmI3ZGI2M2ZjNA").strip()
    if not url or not token:
        return None, "sin UPSTASH_REDIS_REST_URL/TOKEN: el estado solo se guarda en disco local (se pierde al reiniciar en Render)"
    on_render = bool(os.getenv("RENDER", "true").lower() == "true")
    if not on_render and os.getenv("STATE_STORE_FORCE", "false").lower() != "true":
        return None, ("credenciales de Upstash presentes pero no estoy en Render: uso solo disco local "
                      "para no competir con producción (STATE_STORE_FORCE=true para forzar)")
    return UpstashRedis(url, token), "Upstash Redis"
