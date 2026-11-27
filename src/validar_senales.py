"""Valida la corrida controlada SMA 3/5 en signals_prueba, después del generador."""

import argparse
import json
import re
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from generar_ticks_controlados import senales_esperadas


TOPICO = "signals_prueba"
ALGORITMO = "SMA_3_5"
SIMBOLO = "EURUSD"
CAMPOS = {"id_algoritmo", "symbol", "signal", "precio_referencia", "timestamp"}
FORMATO_UTC = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)")
MINUTO = timedelta(minutes=1)
MARGEN_PUBLICACION = timedelta(minutes=2)


def fecha_iso(valor):
    if not isinstance(valor, str):
        return None
    if valor.endswith("Z"):
        valor = valor[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(valor)
    except ValueError:
        return None


def es_utc(valor, fecha):
    return fecha is not None and FORMATO_UTC.fullmatch(valor) is not None


def cargar_corrida(ruta):
    datos = json.loads(ruta.read_text(encoding="utf-8"))
    velas = datos["velas"]
    if not isinstance(velas, list) or not velas:
        raise ValueError("El archivo de la corrida no contiene velas")

    fechas = []
    for vela in velas:
        texto = vela["inicio"]
        fecha = fecha_iso(texto)
        cierre = vela["close"]
        if (vela["symbol"] != SIMBOLO or not es_utc(texto, fecha)
                or fecha.second or fecha.microsecond
                or type(cierre) not in (int, float) or not (0 < cierre < float("inf"))):
            raise ValueError("Vela esperada inválida: se requiere EURUSD, minuto UTC y cierre positivo")
        if fechas and fecha != fechas[-1] + MINUTO:
            raise ValueError("Las velas esperadas deben estar en minutos consecutivos")
        fechas.append(fecha)

    senales = senales_esperadas(velas)
    if [tipo for tipo, _, _ in senales] != ["BUY", "SELL"]:
        raise ValueError("Las velas esperadas deben producir un BUY seguido de un SELL con SMA 3/5")
    esperadas = {tipo: (fecha_iso(fecha), Decimal(str(precio))) for tipo, fecha, precio in senales}
    return fechas[0], fechas[-1] + MINUTO, esperadas


def leer_kafka(servidor, espera):
    from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

    configuracion = {
        "bootstrap.servers": servidor,
        "broker.address.family": "v4",
        "group.id": f"validador_senales_{uuid4().hex}",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "allow.auto.create.topics": False,
        "enable.partition.eof": True,
        "auto.offset.reset": "error",
    }
    consumidor = None
    limite = time.monotonic() + espera
    try:
        consumidor = Consumer(configuracion)
        metadatos = consumidor.list_topics(TOPICO, timeout=espera).topics.get(TOPICO)
        if metadatos is None or metadatos.error or not metadatos.partitions:
            detalle = metadatos.error if metadatos and metadatos.error else "sin particiones disponibles"
            raise ValueError(f"No se pudo leer {TOPICO}: {detalle}")

        asignacion, pendientes = [], {}
        # Leer todos los registros que ya existen al consultar cada partición.
        for numero in sorted(metadatos.partitions):
            restante = limite - time.monotonic()
            if restante <= 0:
                raise TimeoutError("Tiempo agotado al consultar las particiones")
            particion = TopicPartition(TOPICO, numero)
            primero, final = consumidor.get_watermark_offsets(particion, timeout=restante)
            if primero < final:
                asignacion.append(TopicPartition(TOPICO, numero, primero))
                pendientes[numero] = final

        if asignacion:
            consumidor.assign(asignacion)
        while pendientes:
            restante = limite - time.monotonic()
            if restante <= 0:
                raise TimeoutError(f"Lectura incompleta en particiones {sorted(pendientes)}")
            mensaje = consumidor.poll(min(1.0, restante))
            if mensaje is None:
                continue
            error = mensaje.error()
            if error and error.code() != KafkaError._PARTITION_EOF:
                raise KafkaException(error)

            numero, offset = mensaje.partition(), mensaje.offset()
            final = pendientes.get(numero)
            if final is None:
                continue
            if not error and offset < final:
                yield mensaje
            if offset >= final or (not error and offset == final - 1):
                del pendientes[numero]
    except KafkaException as error:
        raise RuntimeError(f"Error de Kafka en {TOPICO}: {error}") from error
    finally:
        if consumidor is not None:
            consumidor.close()


def validar(mensajes, inicio, fin, esperadas):
    errores, recibidas = [], {}
    inicio_kafka = inicio.timestamp() * 1000
    fin_kafka = (fin + MARGEN_PUBLICACION).timestamp() * 1000
    for mensaje in mensajes:
        ubicacion = (mensaje.partition(), mensaje.offset())
        referencia = f"partición {ubicacion[0]}, offset {ubicacion[1]}"
        try:
            senal = json.loads(mensaje.value(), parse_float=Decimal)
        except (ValueError, TypeError, ArithmeticError):
            senal = None

        texto = senal.get("timestamp") if isinstance(senal, dict) else None
        fecha = fecha_iso(texto)
        utc = es_utc(texto, fecha)
        # Un mensaje con fecha inválida se atribuye a la corrida por su hora en Kafka.
        if utc:
            pertenece = inicio <= fecha <= fin
        else:
            _, milisegundos = mensaje.timestamp()
            pertenece = inicio_kafka <= milisegundos <= fin_kafka
            if fecha is not None:
                fecha_con_zona = fecha if fecha.tzinfo else fecha.replace(tzinfo=timezone.utc)
                pertenece = pertenece or inicio <= fecha_con_zona <= fin
        if not pertenece:
            continue
        if not isinstance(senal, dict):
            errores.append(f"{referencia}: se esperaba un objeto JSON válido")
            continue
        faltantes = CAMPOS - senal.keys()
        if faltantes:
            errores.append(f"{referencia}: faltan campos {', '.join(sorted(faltantes))}")
            continue
        if senal["id_algoritmo"] != ALGORITMO or senal["symbol"] != SIMBOLO:
            errores.append(f"{referencia}: se esperaba {ALGORITMO} para {SIMBOLO}")
            continue
        tipo = senal["signal"]
        if tipo not in ("BUY", "SELL"):
            errores.append(f"{referencia}: signal debe ser BUY o SELL")
            continue
        if tipo in recibidas:
            errores.append(f"{referencia}: señal {tipo} duplicada o adicional")
        else:
            recibidas[tipo] = ubicacion

        fecha_esperada, precio_esperado = esperadas[tipo]
        if not utc:
            errores.append(f"{referencia}: timestamp de {tipo} debe ser ISO 8601 en UTC")
        elif fecha != fecha_esperada:
            errores.append(f"{referencia}: {tipo} a {texto}, esperado {fecha_esperada.isoformat()}")

        precio = senal["precio_referencia"]
        if type(precio) not in (int, Decimal) or precio != precio_esperado:
            errores.append(f"{referencia}: precio de {tipo} {precio!r}, esperado {precio_esperado}")

    for tipo in esperadas:
        if tipo not in recibidas:
            errores.append(f"Falta la señal {tipo}")
    if "BUY" in recibidas and "SELL" in recibidas:
        particion_buy, offset_buy = recibidas["BUY"]
        particion_sell, offset_sell = recibidas["SELL"]
        # Kafka conserva el orden de los offsets dentro de cada partición.
        if particion_buy == particion_sell and offset_buy >= offset_sell:
            errores.append("Kafka registró SELL antes de BUY en la misma partición")
    return errores


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--esperadas", type=Path, default=Path("datos_prueba/velas_esperadas.json"),
                        help="Archivo de velas esperadas generado para la corrida")
    parser.add_argument("--servidor", default="127.0.0.1:9092", help="Servidor Kafka")
    parser.add_argument("--espera", type=int, default=30, help="Tiempo máximo de lectura en segundos")
    args = parser.parse_args()
    if args.espera <= 0:
        parser.error("--espera debe ser mayor que cero")

    try:
        inicio, fin, esperadas = cargar_corrida(args.esperadas)
        print(f"Leyendo {TOPICO} para la corrida de {inicio.isoformat()} a {fin.isoformat()}", flush=True)
        for tipo, (fecha, precio) in esperadas.items():
            print(f"Esperado {tipo} {SIMBOLO} a {precio} en {fecha.isoformat()}")
        mensajes = leer_kafka(args.servidor, args.espera)
        try:
            errores = validar(mensajes, inicio, fin, esperadas)
        finally:
            mensajes.close()
    except (OSError, RuntimeError, ImportError, ValueError, KeyError, TypeError, ArithmeticError) as error:
        errores = [str(error)]

    for error in errores:
        print(f"FALLA: {error}")
    print(f"RESULTADO: {'FALLA' if errores else 'APROBADO'}")
    return 1 if errores else 0


if __name__ == "__main__":
    raise SystemExit(main())
