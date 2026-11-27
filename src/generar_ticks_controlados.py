"""
Paso 15 (A2): genera ticks CONTROLADOS para validar que Spark construye bien las velas OHLC.

Qué hace:
  1. Crea ticks de EURUSD con precios y horas conocidos para N minutos completos.
  2. Calcula de antemano la vela que Spark DEBERÍA producir para cada minuto
     y la guarda en datos_prueba/velas_esperadas.json.
  3. Envía los ticks a Kafka (tópico TOPICO_TICKS, por defecto se exige uno de prueba).

Casos incluidos a propósito:
  - Minuto 1: un tick inválido (ask < bid) y un tick de GBPUSD -> Spark debe ignorarlos.
  - Minuto 2: los ticks se envían en orden INVERSO -> open/close deben salir por hora
    del evento, no por orden de llegada.
  - Un tick final en el minuto siguiente, 15 s después del cierre, para que avance el
    watermark (10 s) y Spark publique las velas anteriores. Esa última vela NO se espera.

El precio de la vela es el MID = (bid + ask) / 2, igual que en publicar_velas_spark.py.

Uso (Git Bash, con Kafka encendido):
  export TOPICO_TICKS=ticks_prueba
  python src/generar_ticks_controlados.py                  # validación OHLC (envío inmediato)
  python src/generar_ticks_controlados.py --tiempo-real    # flujo completo (Bloque B)

Modo --tiempo-real (Bloque B):
  Envía un tick cada 10 s con la hora ACTUAL durante N minutos (10 por defecto),
  con precios que suben 6 minutos y luego bajan. Así la estrategia en modo demo
  (SMA 3/5) emite un BUY y después un SELL, y las señales llegan al ejecutor dentro
  de su ventana de vigencia (120 s). Al inicio muestra las señales que se esperan.
"""

import argparse
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from topicos import TOPICO_TICKS

SEGUNDOS = [5, 15, 25, 35, 45, 55]          # seis ticks válidos por minuto
# Precios MID en unidades de 0.00001 (cinco decimales exactos, sin errores de redondeo)
PATRONES = [
    [108500, 108530, 108480, 108510, 108560, 108520],
    [108520, 108490, 108570, 108600, 108540, 108550],
    [108550, 108610, 108450, 108580, 108500, 108470],
    [108470, 108480, 108520, 108460, 108530, 108510],
]
MEDIO_SPREAD = 10                            # 1 pip a cada lado: spread de 2 pips


def texto_fecha(fecha):
    return fecha.strftime("%Y-%m-%dT%H:%M:%SZ")


def tick(symbol, mid, fecha, bid_extra=0):
    return {
        "symbol": symbol,
        "bid": round((mid - MEDIO_SPREAD + bid_extra) / 100000, 5),
        "ask": round((mid + MEDIO_SPREAD) / 100000, 5),
        "timestamp": texto_fecha(fecha),
    }


def construir_ticks(base, minutos):
    """Devuelve (ticks en orden de envío, velas esperadas)."""
    enviar, esperadas = [], []

    for m in range(minutos):
        inicio = base + timedelta(minutes=m)
        mids = PATRONES[m % len(PATRONES)]
        validos = [tick("EURUSD", mid, inicio + timedelta(seconds=s)) for mid, s in zip(mids, SEGUNDOS)]

        esperadas.append({
            "symbol": "EURUSD",
            "inicio": texto_fecha(inicio),
            "open": mids[0] / 100000,
            "high": max(mids) / 100000,
            "low": min(mids) / 100000,
            "close": mids[-1] / 100000,
            "num_ticks": len(mids),
        })

        if m == 2:
            validos = list(reversed(validos))     # llegan al revés
        enviar.extend(validos)

        if m == 1:                                 # deben ser descartados por Spark
            enviar.append(tick("EURUSD", 109999, inicio + timedelta(seconds=30), bid_extra=50))  # ask < bid
            enviar.append(tick("GBPUSD", 127000, inicio + timedelta(seconds=40)))

    # Tick de cierre: hace avanzar el watermark más allá del último minuto esperado
    enviar.append(tick("EURUSD", 108500, base + timedelta(minutes=minutos, seconds=15)))
    return enviar, esperadas


def construir_tendencia(base, minutos, subida=6):
    """Ticks con tendencia: sube `subida` minutos y luego baja. Devuelve (ticks, velas esperadas)."""
    ticks, esperadas = [], []
    precio = 108500
    for m in range(minutos):
        inicio = base + timedelta(minutes=m)
        paso = 5 if m < subida else -8                      # pips*10 por tick
        mids = []
        for i, s in enumerate(SEGUNDOS):
            precio += paso + (3 if i % 2 else -2)           # pequeño vaivén dentro del minuto
            mids.append(precio)
            ticks.append(tick("EURUSD", precio, inicio + timedelta(seconds=s)))
        esperadas.append({"symbol": "EURUSD", "inicio": texto_fecha(inicio),
                          "open": mids[0] / 100000, "high": max(mids) / 100000,
                          "low": min(mids) / 100000, "close": mids[-1] / 100000,
                          "num_ticks": len(mids)})
    ticks.append(tick("EURUSD", precio, base + timedelta(minutes=minutos, seconds=15)))   # cierre
    return ticks, esperadas


def senales_esperadas(esperadas, corta=3, larga=5):
    """Pasa las velas esperadas por la misma estrategia para saber qué señales deben salir."""
    from estrategia_sma import EstrategiaSMA
    estrategia = EstrategiaSMA(id_algoritmo=f"SMA_{corta}_{larga}", periodo_corto=corta, periodo_largo=larga)
    resultado = []
    for vela in esperadas:
        senal = estrategia.procesar_vela(vela)
        if senal:
            resultado.append((senal["signal"], senal["timestamp"], senal["precio_referencia"]))
    return resultado


def enviar_en_tiempo_real(productor, ticks):
    for t in ticks:
        momento = datetime.strptime(t["timestamp"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        espera = (momento - datetime.now(timezone.utc)).total_seconds()
        if espera > 0:
            time.sleep(espera)
        productor.produce(TOPICO_TICKS, value=json.dumps(t))
        productor.poll(0)
        print(f"  tick {t['timestamp']}  bid={t['bid']:.5f} ask={t['ask']:.5f}", flush=True)
    return productor.flush(15)


def main():
    parser = argparse.ArgumentParser(description="Ticks controlados para validar Spark OHLC.")
    parser.add_argument("--minutos", type=int, default=3)
    parser.add_argument("--esperadas", default="datos_prueba/velas_esperadas.json")
    parser.add_argument("--tiempo-real", action="store_true",
                        help="Envía un tick cada 10 s con la hora actual (flujo completo, Bloque B)")
    parser.add_argument("--permitir-oficial", action="store_true",
                        help="Permite enviar al tópico oficial 'ticks' (no recomendado)")
    args = parser.parse_args()

    if TOPICO_TICKS == "ticks" and not args.permitir_oficial:
        raise SystemExit("[BLOQUEADO] TOPICO_TICKS es el oficial 'ticks'. "
                         "Define TOPICO_TICKS=ticks_prueba para no mezclar datos ficticios.")

    from confluent_kafka import Producer   # aquí, para poder probar sin Kafka instalado

    ahora = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    base = ahora + timedelta(minutes=1)     # siempre posterior a corridas anteriores
    if args.tiempo_real:
        minutos = args.minutos if args.minutos != 3 else 10
        enviar, esperadas = construir_tendencia(base, minutos)
    else:
        enviar, esperadas = construir_ticks(base, args.minutos)

    ruta = Path(args.esperadas)
    ruta.parent.mkdir(parents=True, exist_ok=True)
    ruta.write_text(json.dumps({"topico_ticks": TOPICO_TICKS, "velas": esperadas}, indent=2), encoding="utf-8")

    productor = Producer({"bootstrap.servers": "127.0.0.1:9092", "broker.address.family": "v4"})

    if args.tiempo_real:
        print(f"Modo tiempo real: {len(esperadas)} minutos desde {esperadas[0]['inicio']} "
              f"(empieza en menos de 1 minuto, termina ~{len(esperadas) + 1} min después)")
        print("Señales esperadas con la estrategia SMA 3/5:")
        for tipo, momento, precio in senales_esperadas(esperadas):
            print(f"  {tipo:<4} timestamp={momento} precio={precio}")
        pendientes = enviar_en_tiempo_real(productor, enviar)
        if pendientes:
            raise SystemExit(f"[ERROR] {pendientes} ticks sin confirmar por Kafka")
        print("Envío terminado.")
        return

    for t in enviar:
        productor.produce(TOPICO_TICKS, value=json.dumps(t))
    pendientes = productor.flush(15)
    if pendientes:
        raise SystemExit(f"[ERROR] {pendientes} ticks sin confirmar por Kafka")

    print(f"Enviados {len(enviar)} ticks a '{TOPICO_TICKS}' "
          f"({len(esperadas) * len(SEGUNDOS)} válidos, 2 que deben descartarse, 1 de cierre)")
    print(f"Velas esperadas: {len(esperadas)} (desde {esperadas[0]['inicio']}) -> guardadas en {ruta}")


if __name__ == "__main__":
    main()
