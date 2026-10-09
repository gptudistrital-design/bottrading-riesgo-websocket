import json

import numpy as np

from kline_ws import KlineRing, KlineWebSocketStream, parse_kline_message

M = 60_000


def _msg(symbol="BTCUSDT", interval="1m", t=M, o="1", c="2", closed=True):
    return json.dumps({
        "stream": f"{symbol.lower()}@kline_{interval}",
        "data": {"e": "kline", "s": symbol, "k": {
            "t": t, "T": t + M - 1, "s": symbol, "i": interval,
            "o": o, "h": "3", "l": "0.5", "c": c, "v": "10", "x": closed,
        }},
    }, separators=(",", ":"))


def test_parse_ignores_open_candle_and_acks():
    assert parse_kline_message(_msg(closed=False)) is None
    assert parse_kline_message('{"result":null,"id":1}') is None


def test_parse_closed_candle():
    sym, iv, t, o, h, l, c, v = parse_kline_message(_msg(interval="5m", o="1.5", c="1.2"))
    assert (sym, iv, t) == ("BTCUSDT", "5m", M)
    assert (o, c) == (1.5, 1.2)


def test_ring_keeps_last_n_in_order_with_wraparound():
    r = KlineRing("1m", history=3)
    for i in range(1, 8):
        r.add("X", i * M, i, i, i, float(i), 1)
    got = r.closed("X")
    assert got.shape == (3, 5)
    assert list(got[:, 3]) == [5.0, 6.0, 7.0]
    assert list(r.open_times("X")) == [5 * M, 6 * M, 7 * M]


def test_ring_dedupes_and_ignores_late():
    r = KlineRing("1m", history=2)
    r.add("X", 3 * M, 1, 1, 1, 1, 1)
    r.add("X", 4 * M, 1, 1, 1, 1, 1)
    r.add("X", 4 * M, 2, 2, 2, 2, 2)    # duplicado tras reconexión: sobrescribe
    r.add("X", 2 * M, 9, 9, 9, 9, 9)    # tardío: se ignora
    got = r.closed("X")
    assert list(r.open_times("X")) == [3 * M, 4 * M]
    assert got[-1, 0] == 2.0


def test_ring_grows_beyond_capacity():
    r = KlineRing("1m", history=2, capacity=2)
    for n in range(10):
        r.add(f"S{n}", M, n, n, n, n, n)
    assert r.symbols_with_data() == 10
    assert r.closed("S9")[0, 0] == 9.0
    assert r.closed("S0")[0, 0] == 0.0


def test_last_closed_freshness_and_bullish():
    r = KlineRing("1m", history=2)
    r.add("X", 0, 1.0, 2.0, 0.5, 1.5, 3.0)
    c = r.last_closed("X", now_ms=M + 500)              # minuto recién cerrado
    assert c is not None and c.bullish and c.close_time == M - 1
    assert r.last_closed("X", now_ms=2 * M + 5_001) is None   # se perdió un cierre
    assert r.last_closed("X", fresh=False, now_ms=10 * M) == c
    assert r.last_closed("Y") is None


def test_one_connection_per_interval_for_819_symbols():
    syms = [f"S{i}USDT" for i in range(819)]
    st = KlineWebSocketStream(syms, intervals=["1m", "5m", "15m"], history=2)
    stats = st.get_stats()
    assert stats["connections"] == 3
    assert {iv: v["connections"] for iv, v in stats["intervals"].items()} == {"1m": 1, "5m": 1, "15m": 1}
    st.ensure_symbols(["NEWUSDT", "S1USDT"])
    assert st.get_stats()["intervals"]["1m"]["symbols"] == 820
    # Más de 1024 símbolos: solo entonces se abre una segunda conexión.
    st2 = KlineWebSocketStream([f"X{i}" for i in range(1100)], intervals=["1m"])
    assert st2.get_stats()["intervals"]["1m"]["connections"] == 2


def test_float32_halves_memory():
    a = KlineRing("1m", history=1500, dtype="float64", capacity=819)
    b = KlineRing("1m", history=1500, dtype="float32", capacity=819)
    assert b.ohlcv.nbytes * 2 == a.ohlcv.nbytes
    assert a.ohlcv.dtype == np.float64
