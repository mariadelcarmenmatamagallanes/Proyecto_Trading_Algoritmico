"""
Conecta la estrategia SMA con Kafka:  tópico `velas`  ->  EstrategiaSMA  ->  tópico `signals`

Uso (con Kafka encendido, desde la raíz del proyecto):
  python src/estrategia_kafka.py                       # estrategia oficial SMA 10/50
  python src/estrategia_kafka.py --corta 3 --larga 5   # MODO DEMOSTRACIÓN (señales en minutos)

¿Por qué un modo demostración?
  Con velas de 1 minuto, la SMA de 50 necesita 50 velas = 50 minutos antes de
  poder emitir la primera señal. Para la sesión de integración usamos medias
  más cortas: la lógica es exactamente la misma, solo cambian los periodos.

Detener con Ctrl + C.
"""

import argparse
import json

from confluent_kafka import Consumer, Producer

from estrategia_sma import EstrategiaSMA

# 127.0.0.1 + IPv4: en Windows "localhost" puede intentar IPv6 y fallar con Docker
SERVIDOR = "127.0.0.1:9092"


def leer_argumentos():
    parser = argparse.ArgumentParser(description="Estrategia SMA conectada a Kafka.")
    parser.add_argument("--corta", type=int, default=10, help="Periodo de la SMA corta (por defecto 10)")
    parser.add_argument("--larga", type=int, default=50, help="Periodo de la SMA larga (por defecto 50)")
    parser.add_argument("--stop-loss", type=float, default=0.02, help="0.02 = 2 %% de variación del precio")
    parser.add_argument("--take-profit", type=float, default=0.04, help="0.04 = 4 %%")
    parser.add_argument("--symbol", default="EURUSD")
    parser.add_argument("--solo-nuevas", action="store_true",
                        help="Ignorar las velas viejas del tópico y empezar con las que lleguen a partir de ahora")
    return parser.parse_args()


def main():
    args = leer_argumentos()
    id_algoritmo = f"SMA_{args.corta}_{args.larga}"

    estrategia = EstrategiaSMA(
        id_algoritmo=id_algoritmo,
        symbol=args.symbol,
        periodo_corto=args.corta,
        periodo_largo=args.larga,
        stop_loss_pct=args.stop_loss,
        take_profit_pct=args.take_profit,
    )

    consumidor = Consumer({
        "bootstrap.servers": SERVIDOR,
        "broker.address.family": "v4",
        # Un grupo por configuración: Kafka recuerda hasta dónde leyó cada versión
        "group.id": f"estrategia_{id_algoritmo.lower()}",
        # Primera vez: "earliest" usa velas guardadas para llenar las medias más rápido;
        # "latest" empieza solo con las velas nuevas
        "auto.offset.reset": "latest" if args.solo_nuevas else "earliest",
    })
    productor = Producer({"bootstrap.servers": SERVIDOR, "broker.address.family": "v4"})

    consumidor.subscribe(["velas"])
    modo = "OFICIAL" if (args.corta, args.larga) == (10, 50) else "DEMOSTRACIÓN"
    print(f"Estrategia {id_algoritmo} ({modo}) sobre {args.symbol}")
    print("Esperando velas en el tópico 'velas'... (Ctrl + C para salir)")

    try:
        while True:
            mensaje = consumidor.poll(1.0)     # esperar hasta 1 segundo por una vela
            if mensaje is None:
                continue
            if mensaje.error():
                print("Error de Kafka:", mensaje.error())
                continue

            try:
                vela = json.loads(mensaje.value())
                senal = estrategia.procesar_vela(vela)
            except (ValueError, KeyError, TypeError) as error:
                print("Vela con formato incorrecto, se ignora:", error)
                continue

            texto = (f"Vela {vela.get('inicio')} close={vela.get('close')} "
                     f"({len(estrategia.cierres)}/{estrategia.periodo_largo} velas en memoria)")
            if estrategia.ultima_sma_corta is not None and len(estrategia.cierres) >= estrategia.periodo_largo:
                texto += (f" | SMA{args.corta}={estrategia.ultima_sma_corta:.5f}"
                          f" SMA{args.larga}={estrategia.ultima_sma_larga:.5f}")
            print(texto, flush=True)

            if senal:
                productor.produce("signals", value=json.dumps(senal))
                productor.flush(10)
                print(f"  -> SEÑAL publicada: {senal}  motivo={estrategia.ultimo_motivo}", flush=True)
    except KeyboardInterrupt:
        print("Estrategia detenida por el usuario.")
    finally:
        consumidor.close()


if __name__ == "__main__":
    main()
