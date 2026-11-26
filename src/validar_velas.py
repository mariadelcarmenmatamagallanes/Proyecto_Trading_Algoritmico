"""
Paso 15 (A3): compara las velas que publicó Spark con las velas esperadas.

Lee datos_prueba/velas_esperadas.json (creado por generar_ticks_controlados.py),
consume el tópico TOPICO_VELAS desde el inicio y revisa, para cada minuto esperado:
  open, high, low, close, num_ticks, y que la vela aparezca UNA sola vez.

Uso (PowerShell):
  $env:TOPICO_VELAS="velas_prueba"
  python src/validar_velas.py

Resultado final: "RESULTADO: APROBADO" o "RESULTADO: FALLA" (y código de salida 0 o 1).
"""

import argparse
import json
import time
import uuid
from pathlib import Path

from topicos import TOPICO_VELAS

CAMPOS = ("open", "high", "low", "close", "num_ticks")


def comparar(esperadas, recibidas):
    """
    esperadas: lista de velas esperadas.
    recibidas: lista de velas leídas de Kafka (pueden incluir otras corridas).
    Devuelve (aprobado, filas) donde cada fila describe un minuto.
    """
    por_inicio = {}
    for v in recibidas:
        por_inicio.setdefault(v.get("inicio"), []).append(v)

    filas, aprobado = [], True
    for e in esperadas:
        llegadas = por_inicio.get(e["inicio"], [])
        if not llegadas:
            filas.append((e["inicio"], "FALTA", "Spark no publicó esta vela"))
            aprobado = False
            continue
        v = llegadas[0]
        errores = [f"{c}: esperado {e[c]}, recibido {v.get(c)}"
                   for c in CAMPOS if abs(float(v.get(c, "nan")) - float(e[c])) > 1e-9]
        if len(llegadas) > 1:
            errores.append(f"vela repetida {len(llegadas)} veces")
        if errores:
            filas.append((e["inicio"], "FALLA", "; ".join(errores)))
            aprobado = False
        else:
            filas.append((e["inicio"], "OK", f"O={v['open']} H={v['high']} L={v['low']} "
                                             f"C={v['close']} ticks={v['num_ticks']}"))
    return aprobado, filas


def main():
    parser = argparse.ArgumentParser(description="Valida las velas publicadas por Spark.")
    parser.add_argument("--esperadas", default="datos_prueba/velas_esperadas.json")
    parser.add_argument("--espera", type=int, default=120, help="Segundos máximos de espera")
    args = parser.parse_args()

    from confluent_kafka import Consumer

    esperadas = json.loads(Path(args.esperadas).read_text(encoding="utf-8"))["velas"]
    objetivo = {e["inicio"] for e in esperadas}

    consumidor = Consumer({
        "bootstrap.servers": "127.0.0.1:9092",
        "broker.address.family": "v4",
        "group.id": f"validador_{uuid.uuid4().hex[:8]}",   # grupo nuevo: lee todo desde el inicio
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumidor.subscribe([TOPICO_VELAS])
    print(f"Leyendo '{TOPICO_VELAS}' (hasta {args.espera} s) buscando {len(objetivo)} velas...")

    recibidas, limite = [], time.time() + args.espera
    try:
        while time.time() < limite:
            mensaje = consumidor.poll(1.0)
            if mensaje is None or mensaje.error():
                if objetivo <= {v.get("inicio") for v in recibidas} and mensaje is None:
                    break       # ya llegaron todas y no hay más mensajes por ahora
                continue
            try:
                vela = json.loads(mensaje.value())
            except ValueError:
                continue
            if vela.get("inicio") in objetivo:
                recibidas.append(vela)
    finally:
        consumidor.close()

    aprobado, filas = comparar(esperadas, recibidas)
    print(f"\n{'Minuto (inicio)':<22} {'Estado':<7} Detalle")
    for inicio, estado, detalle in filas:
        print(f"{inicio:<22} {estado:<7} {detalle}")
    print(f"\nRESULTADO: {'APROBADO' if aprobado else 'FALLA'}")
    raise SystemExit(0 if aprobado else 1)


if __name__ == "__main__":
    main()
