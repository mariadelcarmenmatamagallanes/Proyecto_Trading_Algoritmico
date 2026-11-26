
import json
import math
from datetime import datetime, timezone

from ib_async import IB, Forex
from confluent_kafka import Producer


# Configuracion de Interactive Brokers
IBKR_HOST = "127.0.0.1"
IBKR_PORT = 7497
CLIENT_ID = 1
SYMBOL = "EURUSD"

# Configuracion de Kafka
KAFKA_SERVER = "127.0.0.1:9092"  # IPv4: en Windows "localhost" puede fallar
KAFKA_TOPIC = "ticks"

productor = Producer({
    "bootstrap.servers": KAFKA_SERVER,
    "broker.address.family": "v4",
    "enable.idempotence": True
})

ib = IB()
ultimo_precio = None


def confirmar_entrega(error, mensaje):
    """Comprueba si Kafka recibio la cotizacion."""

    if error is not None:
        print(f"ERROR Kafka: {error}")
    else:
        print(f"Cotizacion confirmada: {mensaje.value().decode()}")


def procesar_cotizacion(ticker):
    """Valida y publica las cotizaciones recibidas de IBKR."""

    global ultimo_precio

    bid = ticker.bid
    ask = ticker.ask

    # Validar que los precios sean numericos y positivos
    if not math.isfinite(bid) or not math.isfinite(ask):
        return

    if bid <= 0 or ask <= 0 or ask < bid:
        return

    # Evitar publicaciones consecutivas identicas
    precio_actual = (bid, ask)

    if precio_actual == ultimo_precio:
        return

    # Respetar el formato requerido por el profesor
    mensaje = {
        "symbol": SYMBOL,
        "bid": bid,
        "ask": ask,
        "timestamp": datetime.now(timezone.utc).isoformat(
            timespec="seconds"
        ).replace("+00:00", "Z")
    }

    try:
        productor.produce(
            KAFKA_TOPIC,
            key=SYMBOL,
            value=json.dumps(mensaje, allow_nan=False),
            callback=confirmar_entrega
        )

        ultimo_precio = precio_actual
        productor.poll(0)

    except BufferError:
        print("ERROR: Cola de Kafka llena.")
        productor.poll(0)

    except Exception as error:
        print(f"ERROR al publicar cotizacion: {error}")


def main():
    contrato = None

    try:
        # Verificar disponibilidad de Kafka
        metadata = productor.list_topics(
            KAFKA_TOPIC,
            timeout=10
        )

        topico = metadata.topics.get(KAFKA_TOPIC)

        if topico is None or topico.error is not None:
            raise RuntimeError("El topico ticks no esta disponible.")

        print("Conexion con Kafka verificada.")

        # Conectar exclusivamente al puerto Paper Trading
        ib.connect(
            IBKR_HOST,
            IBKR_PORT,
            clientId=CLIENT_ID,
            readonly=True,
            timeout=10
        )

        print("Conexion con IBKR establecida.")

        # Solicitar datos diferidos si no hay datos en tiempo real
        ib.reqMarketDataType(3)

        contrato = Forex(SYMBOL)

        if not ib.qualifyContracts(contrato):
            raise RuntimeError("No se pudo validar EURUSD.")

        ticker = ib.reqMktData(contrato)
        ticker.updateEvent += procesar_cotizacion

        print("Recibiendo cotizaciones de EURUSD...")
        print("Presiona Ctrl+C para detener el programa.")

        while ib.isConnected():
            ib.sleep(0.2)
            productor.poll(0)

    except KeyboardInterrupt:
        print("\nDetencion solicitada por el usuario.")

    except Exception as error:
        print(f"ERROR: {error}")

    finally:
        if ib.isConnected():
            if contrato is not None:
                ib.cancelMktData(contrato)

            ib.disconnect()

        pendientes = productor.flush(10)

        if pendientes:
            print(f"Mensajes pendientes de entrega: {pendientes}")

        print("Programa finalizado.")


if __name__ == "__main__":
    main()
