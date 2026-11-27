
import argparse
import json
import math
import os
import re
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
from confluent_kafka import Consumer
from ib_async import IB
from seguridad_ibkr import verificar_conexion_paper
from sincronizacion_ibkr import (
    posicion_real_eurusd,
    revisar_sincronizacion,
    control_antes_de_operar,
)
from calculo_riesgo import calcular_riesgo
from topicos import TOPICO_SIGNALS, modo_prueba, describir
from config_riesgo import (
    SIMBOLO_PERMITIDO,
    CAPITAL_POR_OPERACION_USD,
    RIESGO_MAXIMO_TEORICO_USD,
    STOP_LOSS,
    TAKE_PROFIT,
    MAX_POSICIONES,
    VIGENCIA_SENAL_SEGUNDOS,
    TOLERANCIA_FUTURA_SEGUNDOS,
)

TOPICO_PAPER = "signals_paper_prueba"
revalidacion = re.fullmatch(r"signals_paper_revalidacion_([0-9a-f]{10})", TOPICO_SIGNALS)
topico_paper_autorizado = TOPICO_SIGNALS == TOPICO_PAPER or revalidacion is not None
modo_paper = False
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ejecutor EURUSD para Dry Run y Paper.")
    parser.add_argument("--paper", action="store_true", help="Activar órdenes en la cuenta Paper autorizada")
    modo_paper = parser.parse_args().paper
if modo_paper != topico_paper_autorizado:
    raise SystemExit("[BLOQUEADO] El modo --paper requiere un tópico Paper autorizado")
if revalidacion and os.environ.get("RECUPERAR_BUY_PAPER"):
    raise SystemExit("[BLOQUEADO] Una revalidación nueva no recupera órdenes de otra corrida")

# Conexion segura con IBKR Paper Trading
ib = IB()
try:
    ib.connect("127.0.0.1", 7497, clientId=32 if modo_paper else 31,
               timeout=10, readonly=not modo_paper)
    if not verificar_conexion_paper(ib):
        raise RuntimeError("Cuenta Paper no autorizada")
except Exception:
    ib.disconnect()
    raise SystemExit("[BLOQUEADO] Conexion Paper no autorizada")

print("[SEGURIDAD] Conexion Paper autorizada")

# Base de datos local del proyecto
if revalidacion:
    nombre_bd = f"ejecutor_paper_revalidacion_{revalidacion[1]}.sqlite3"
elif modo_paper:
    nombre_bd = "ejecutor_paper.sqlite3"
else:
    nombre_bd = "ejecutor_prueba.sqlite3" if modo_prueba() else "ejecutor.sqlite3"
ruta_bd = (
    Path(__file__).resolve().parent.parent
    / "datos_prueba"
    # En modo prueba se usa otra base para no contaminar el estado de la sesión real
    / nombre_bd
)

ruta_bd.parent.mkdir(parents=True, exist_ok=True)
db = sqlite3.connect(ruta_bd)

# Crear las tablas si todavía no existen
with db:
    db.execute("""
        CREATE TABLE IF NOT EXISTS estado (
            id INTEGER PRIMARY KEY,
            en_posicion INTEGER NOT NULL
        )
    """)

    db.execute("""
        CREATE TABLE IF NOT EXISTS aceptadas (
            clave TEXT PRIMARY KEY,
            tipo TEXT NOT NULL,
            precio REAL NOT NULL
        )
    """)

    db.execute(
        "INSERT OR IGNORE INTO estado VALUES (1, 0)"
    )


def posicion_abierta():
    fila = db.execute(
        "SELECT en_posicion FROM estado WHERE id = 1"
    ).fetchone()
    return bool(fila[0])


def validar(s):
    if not isinstance(s, dict):
        return "Mensaje inválido"

    campos = (
        "id_algoritmo", "symbol", "signal",
        "precio_referencia", "timestamp"
    )
    if any(c not in s for c in campos):
        return "Faltan campos obligatorios"

    if not isinstance(s["id_algoritmo"], str) or not s["id_algoritmo"]:
        return "Algoritmo inválido"

    if s["symbol"] != SIMBOLO_PERMITIDO:
        return "Símbolo no autorizado"

    if s["signal"] not in ("BUY", "SELL"):
        return "Señal inválida"

    if isinstance(s["precio_referencia"], bool):
        return "Precio inválido"

    try:
        precio = float(s["precio_referencia"])
        fecha = datetime.fromisoformat(
            s["timestamp"].replace("Z", "+00:00")
        )

        if not math.isfinite(precio) or precio <= 0:
            return "Precio inválido"

        if fecha.tzinfo is None:
            return "Fecha sin zona horaria"

        diferencia = (
            datetime.now(timezone.utc) - fecha
        ).total_seconds()

        if diferencia > VIGENCIA_SENAL_SEGUNDOS or diferencia < -TOLERANCIA_FUTURA_SEGUNDOS:
            return "Señal antigua o futura"

    except (TypeError, ValueError, AttributeError):
        return "Precio o fecha incorrectos"

    return None

def procesar(s):
    error = validar(s)
    if error:
        return f"[RECHAZADA] {error}"

    fecha = datetime.fromisoformat(
        s["timestamp"].replace("Z", "+00:00")
    ).astimezone(timezone.utc)

    clave = json.dumps([
        s["id_algoritmo"], s["symbol"],
        s["signal"], fecha.isoformat()
    ])

    tipo = s["signal"]
    precio = float(s["precio_referencia"])


    cantidad_compra = None
    if tipo == "BUY":
        try:
            riesgo = calcular_riesgo(precio)
        except (ValueError, OverflowError):
            return "[RECHAZADA] No se pudo calcular el riesgo"
        if not riesgo["riesgo_autorizado"]:
            return "[RECHAZADA] Riesgo monetario excedido"
        cantidad_compra = riesgo["cantidad_eur"]

    if paper is None:
        try:
            rechazo_ibkr = control_antes_de_operar(ib, tipo, False)
        except Exception as error:
            return f"[RECHAZADA] No se pudo consultar IBKR: {error}"
        if rechazo_ibkr:
            return f"[RECHAZADA] {rechazo_ibkr}"

    duplicada = db.execute("SELECT 1 FROM aceptadas WHERE clave = ?", (clave,)).fetchone()
    if duplicada or (paper is not None and paper.ya_registrada(clave)):
        return "[RECHAZADA] Señal duplicada"

    abierta = posicion_abierta()
    if tipo == "BUY" and abierta:
        return "[RECHAZADA] Ya existe una posición"
    if tipo == "SELL":
        if not abierta:
            return "[RECHAZADA] No existe posición para vender"
        compra = db.execute(
            "SELECT clave FROM aceptadas WHERE tipo = 'BUY' ORDER BY rowid DESC LIMIT 1"
        ).fetchone()
        if compra is None:
            return "[RECHAZADA] No se encontró el BUY de la posición abierta"
        fecha_compra = datetime.fromisoformat(json.loads(compra[0])[3].replace("Z", "+00:00"))
        if fecha < fecha_compra:
            return "[RECHAZADA] Timestamp de SELL anterior al BUY de la posición abierta"

    if paper is not None:
        fecha_limite = fecha.timestamp() + VIGENCIA_SENAL_SEGUNDOS
        ejecucion, referencia = paper.enviar(clave, tipo, precio, cantidad_compra, fecha_limite)
        if ejecucion is None:
            return f"[RECHAZADA] {referencia}"
        cantidad, precio = ejecucion

    # Confirmar conjuntamente la ejecución, la señal y el cambio de posición.
    with db:
        if paper is not None:
            paper.guardar_ejecucion(clave, cantidad)
        db.execute(
            "INSERT INTO aceptadas VALUES (?, ?, ?)",
            (clave, tipo, precio)
        )

        nuevo_estado = 1 if tipo == "BUY" else 0

        db.execute(
            "UPDATE estado SET en_posicion = ? WHERE id = 1",
            (nuevo_estado,)
        )

    if paper is not None:
        return f"[PAPER] {tipo} ejecutada a {precio} ({referencia})"
    return f"[DRY-RUN] {tipo} aceptada a {precio}"


def comprobar_commit(particiones):
    if not particiones:
        raise RuntimeError("Kafka no devolvió el resultado de la confirmación")
    for particion in particiones:
        if particion.error is not None:
            raise RuntimeError(f"Kafka no confirmó la partición {particion.partition}: {particion.error}")


paper = None
try:
    if modo_paper:
        from ordenes_paper import OrdenesPaper

        ib.RequestTimeout = 10
        ib.RaiseRequestErrors = True
        paper = OrdenesPaper(ib, db, posicion_abierta())
        print("[SINCRONIZADO] Cantidad Paper coincide entre SQLite e IBKR")
    else:
        _, mensaje_sync = revisar_sincronizacion(
            posicion_abierta(), posicion_real_eurusd(ib), False
        )
        print(mensaje_sync)
except Exception as error:
    db.close()
    ib.disconnect()
    raise SystemExit(f"[BLOQUEADO] {error}") from error

if revalidacion:
    grupo_kafka = f"ejecutor_paper_revalidacion_{revalidacion[1]}"
elif modo_paper:
    grupo_kafka = "ejecutor_paper_v1"
else:
    grupo_kafka = "ejecutor_persistente_v1" + ("_prueba" if modo_prueba() else "")
config_kafka = {
    "bootstrap.servers": "127.0.0.1:9092",
    "broker.address.family": "v4",
    "group.id": grupo_kafka,
    "auto.offset.reset": "latest",
    "enable.auto.commit": False,
    "enable.auto.offset.store": False,
}
if modo_paper:
    config_kafka["allow.auto.create.topics"] = False
consumidor = Consumer(config_kafka)

consumidor.subscribe([TOPICO_SIGNALS])
print(describir())

print(f"Ejecutor persistente iniciado ({'PAPER' if modo_paper else 'DRY-RUN'})")
print("Posición guardada:", posicion_abierta())
print("Esperando señales... Ctrl + C para salir")

particiones_iniciadas = set()
try:
    while True:
        mensaje = consumidor.poll(1.0)
        ib.sleep(0)
        if not verificar_conexion_paper(ib):
            print("[BLOQUEADO] Conexion Paper perdida o no autorizada")
            break


        if mensaje is None:
            continue

        if mensaje.error():
            print("Error de Kafka:", mensaje.error())
            continue

        try:
            particion = (mensaje.topic(), mensaje.partition())
            if particion not in particiones_iniciadas:
                from confluent_kafka import TopicPartition

                # Guardar el offset que se debe leer al reiniciar, sin avanzar.
                # Así latest no salta el primer mensaje de un grupo nuevo si SQLite falla.
                reanudar = TopicPartition(*particion, mensaje.offset())
                comprobar_commit(consumidor.commit(offsets=[reanudar], asynchronous=False))
                particiones_iniciadas.add(particion)

            try:
                senal = json.loads(mensaje.value())
            except (ValueError, TypeError):
                resultado = "[RECHAZADA] JSON incorrecto"
            else:
                resultado = procesar(senal)
            print(resultado, flush=True)

            # Solo avanzar después de guardar el estado o rechazar el mensaje.
            comprobar_commit(consumidor.commit(message=mensaje, asynchronous=False))

        except Exception as error:
            print(f"[BLOQUEADO] Error en partición {mensaje.partition()}, "
                  f"offset {mensaje.offset()}: {error}", flush=True)
            # Un commit posterior de la misma partición también saltaría este mensaje.
            raise SystemExit(1) from error

except KeyboardInterrupt:
    print("\nEjecutor detenido")

finally:
    consumidor.close()
    db.close()
    ib.disconnect()
    print("[SEGURIDAD] Conexion IBKR cerrada")
