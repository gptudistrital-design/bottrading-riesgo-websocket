# Bot Trading Riesgo - Shorts a ganadores de Binance Futures

Aplicación web lista para Render que monitorea los ganadores de Binance USDT-M Futures y abre tramos **short** cuando un símbolo supera niveles de ganancia de 24h.

## Estrategia implementada

- Actualiza los ganadores de Binance Futures por `priceChangePercent` de 24h con **una** consulta REST controlada por minuto.
- Usa WebSocket de Binance (`!ticker@arr`) para mantener precios, cambios 24h y volumen en tiempo real entre escaneos, evitando polling agresivo.
- Abre short en tramos configurables cuando el cambio 24h supera estos niveles:
  - `50%, 75%, 100%, 150%, 200%, 250%`
- Tamaño de cada tramo:
  - `5, 5, 10, 20, 40, 80 USDT`
- Cierra toda la posición cuando la ganancia no realizada llega al 50% del capital colocado:
  - Ejemplo: posición de `5 USDT` -> cierre con `2.5 USDT` de ganancia.
- Muestra en una página web:
  - Ganadores detectados.
  - Posiciones abiertas.
  - PnL no realizado.
  - Operaciones cerradas.
  - Eventos del bot.

## Seguridad

El bot arranca por defecto en **PAPER_MODE=true**, por lo que simula las órdenes y no envía operaciones reales.

Para operar real en Binance Futures debes configurar todas estas variables de entorno:

```bash
PAPER_MODE=false
LIVE_TRADING=true
BINANCE_API_KEY=tu_api_key
BINANCE_API_SECRET=tu_api_secret
```

> Usa primero paper trading. Un short contra monedas que suben 100%-250% puede liquidarse si no hay control de margen, apalancamiento y pérdidas.

## Variables de entorno principales

| Variable | Default | Descripción |
| --- | --- | --- |
| `PAPER_MODE` | `true` | Simula órdenes si está en `true`. |
| `LIVE_TRADING` | `false` | Habilita órdenes reales si también `PAPER_MODE=false`. |
| `ENTRY_LEVELS` | `50,75,100,150,200,250` | Niveles de subida 24h para abrir tramos. |
| `ENTRY_NOTIONALS` | `5,5,10,20,40,80` | USDT por tramo. |
| `TAKE_PROFIT_FRACTION` | `0.5` | Ganancia objetivo sobre el notional total. |
| `SCAN_INTERVAL_SECONDS` | `60` | Frecuencia mínima de consulta REST para refrescar ganadores y actualizar la lista seguida por WebSocket. |
| `MAX_SYMBOLS` | `120` | Máximo de ganadores a evaluar por escaneo. |
| `MIN_GAIN_TO_SHOW` | `0` | Filtro mínimo de porcentaje para mostrar ganadores en la tabla. |
| `INCLUDE_SPOT_WINNERS` | `false` | Conservado solo para el fallback manual REST; el escaneo operativo usa futures por WebSocket. |
| `LEVERAGE` | `1` | Apalancamiento que intentará configurar en modo real. |
| `STATE_FILE` | `/tmp/bottradingriesgo_state.json` | Archivo usado para compartir el último estado útil entre reinicios/workers. |

## Ejecutar local

```bash
pip install -r requirements.txt
python app.py
```

Abre `http://localhost:8000`.

## Diagnóstico de pantalla vacía

Si el bot abre posiciones en los logs pero la página no las muestra, revisa en la web el bloque **Estado API crudo**. La página ahora renderiza un snapshot inicial del servidor y luego refresca `/api/status`; si falla JavaScript, fetch o el endpoint, el error queda visible en **Último error / diagnóstico**.

## Deploy en Render

El archivo `render.yaml` incluye el servicio web y fija `PYTHON_VERSION=3.12.13` para evitar que Render use Python 3.14, donde dependencias con extensiones nativas pueden compilar desde fuente y fallar. En Render configura las variables de entorno necesarias y despliega el repositorio.

