
import json
import math
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
    ORDENES_HABILITADAS,
)

# Conexion segura con IBKR Paper Trading
ib = IB()
try:
    ib.connect("127.0.0.1", 7497, clientId=31,
               timeout=10, readonly=True)
    if not verificar_conexion_paper(ib):
        raise RuntimeError("Cuenta Paper no autorizada")
except Exception:
    ib.disconnect()
    raise SystemExit("[BLOQUEADO] Conexion Paper no autorizada")

print("[SEGURIDAD] Conexion Paper autorizada")

# Base de datos local del proyecto
ruta_bd = (
    Path(__file__).resolve().parent.parent
    / "datos_prueba"
    # En modo prueba se usa otra base para no contaminar el estado de la sesión real
    / ("ejecutor_prueba.sqlite3" if modo_prueba() else "ejecutor.sqlite3")
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

    clave = json.dumps([
        s["id_algoritmo"], s["symbol"],
        s["signal"], s["timestamp"]
    ])

    tipo = s["signal"]
    precio = float(s["precio_referencia"])

    if tipo == "BUY":
        try:
            riesgo = calcular_riesgo(precio)
        except (ValueError, OverflowError):
            return "[RECHAZADA] No se pudo calcular el riesgo"

        if not riesgo["riesgo_autorizado"]:
            return "[RECHAZADA] Riesgo monetario excedido"

    # Paso 18.9/18.10: controles contra el estado real de IBKR (solo lectura)
    try:
        rechazo_ibkr = control_antes_de_operar(ib, tipo, ORDENES_HABILITADAS)
    except Exception as error:
        return f"[RECHAZADA] No se pudo consultar IBKR: {error}"
    if rechazo_ibkr:
        return f"[RECHAZADA] {rechazo_ibkr}"

    # Guardar posición y señal en una misma transacción
    with db:
        duplicada = db.execute(
            "SELECT 1 FROM aceptadas WHERE clave = ?",
            (clave,)
        ).fetchone()

        if duplicada:
            return "[RECHAZADA] Señal duplicada"

        abierta = posicion_abierta()

        if tipo == "BUY" and abierta:
            return "[RECHAZADA] Ya existe una posición"

        if tipo == "SELL" and not abierta:
            return "[RECHAZADA] No existe posición para vender"

        db.execute(
            "INSERT INTO aceptadas VALUES (?, ?, ?)",
            (clave, tipo, precio)
        )

        nuevo_estado = 1 if tipo == "BUY" else 0

        db.execute(
            "UPDATE estado SET en_posicion = ? WHERE id = 1",
            (nuevo_estado,)
        )

    return f"[DRY-RUN] {tipo} aceptada a {precio}"


# Paso 18.9: comparar la posición lógica (SQLite) con la posición real en IBKR
bloquear, mensaje_sync = revisar_sincronizacion(
    posicion_abierta(), posicion_real_eurusd(ib), ORDENES_HABILITADAS
)
print(mensaje_sync)
if bloquear:
    db.close()
    ib.disconnect()
    raise SystemExit("[SEGURIDAD] Ejecutor detenido hasta corregir la diferencia")

# Escuchar solo las señales de prueba
consumidor = Consumer({
    "bootstrap.servers": "127.0.0.1:9092",
    "broker.address.family": "v4",
    "group.id": "ejecutor_persistente_v1" + ("_prueba" if modo_prueba() else ""),
    "auto.offset.reset": "latest",
    "enable.auto.commit": False
})

consumidor.subscribe([TOPICO_SIGNALS])
print(describir())

print("Ejecutor persistente iniciado (DRY-RUN)")
print("Posición guardada:", posicion_abierta())
print("Esperando señales... Ctrl + C para salir")

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
            senal = json.loads(mensaje.value())
            resultado = procesar(senal)
            print(resultado, flush=True)

            # Confirmar la lectura después de guardar el estado
            consumidor.commit(
                message=mensaje,
                asynchronous=False
            )

        except (ValueError, UnicodeDecodeError):
            print("[RECHAZADA] JSON incorrecto")
            consumidor.commit(
                message=mensaje,
                asynchronous=False
            )

        except Exception as error:
            print("Error de procesamiento:", error)
            # No confirmar el mensaje si falla la base de datos

except KeyboardInterrupt:
    print("\nEjecutor detenido")

finally:
    consumidor.close()
    db.close()
    ib.disconnect()
    print("[SEGURIDAD] Conexion IBKR cerrada")
