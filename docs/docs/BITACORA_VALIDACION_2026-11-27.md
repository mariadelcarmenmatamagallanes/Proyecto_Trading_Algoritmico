
# Bitácora de validación Paper del 27 de noviembre de 2026

El Bloque B está aprobado y TWS Paper completó automáticamente un BUY y un SELL con señales controladas. También se comprobó que las cotizaciones en tiempo real de IBKR llegan a la estrategia SMA 3/5. La corrida `50c598d3dc` comprobó ambos tramos juntos y completó el ciclo de mercado a TWS Paper del paso 20.

## Evidencias del corte

| Prueba | Evidencia | Estado |
|---|---|---|
| Bloque B en Dry Run | BUY EURUSD a 1.08665 a las 16:25 UTC y SELL a 1.08563 a las 16:29 UTC. `validar_senales.py` devolvió `APROBADO` y el ejecutor aceptó las dos señales. | Aprobada |
| Controles del ejecutor | 43 pruebas existentes y regresiones aisladas de precios booleanos, SELL anterior al BUY y fallo de SQLite sin avanzar la lectura en Kafka. | Verificados en simulación |
| Cotización de mercado | TWS devolvió tipo 1 para EURUSD. La muestra de control fue bid 1.09381 y ask 1.09383. | Tiempo real confirmado en esa sesión |
| IBKR, Kafka, Spark y estrategia | `ticks_ibkr_prueba` recibió cotizaciones y Spark publicó velas en `velas_ibkr_prueba`. SMA 3/5 mostró 5/5 velas y publicó BUY y SELL en `signals_ibkr_prueba`. | Verificada |
| Ciclo Paper controlado automático | Corrida `1d62ddb5af`. BUY 914 EUR a 1.09380 y SELL 914 EUR a 1.09350. Resultado `APROBADO`, 0 EUR en TWS y posición cerrada en SQLite, sin conciliación manual. | Aprobada |
| Ciclo Paper originado por mercado | Corrida `50c598d3dc`. SMA 3/5 generó el BUY a las 18:10 UTC y el SELL a las 18:12 UTC. TWS confirmó ambos y SQLite registró el cierre. | Aprobada. Saldo final en TWS de 0 EUR y sin órdenes EURUSD abiertas. |

## Ejecuciones de la corrida integral

| Señal SMA | Timestamp de la señal (UTC) | Ejecución TWS (UTC) | Cantidad y precio |
|---|---|---|---|
| BUY | 18:10:00 | 18:10:23 | 894 EUR a 1.09365 y 20 EUR a 1.09363. Promedio de los 914 EUR redondeado a 1.09365. |
| SELL | 18:12:00 | 18:12:24 | 914 EUR a 1.09345. |

La vela de la señal BUY cerró a 1.09361 y el límite Paper se fijó en 1.09385. La base de esta corrida conserva un BUY y un SELL ejecutados y una cantidad final de 0 EUR.

## Incidencias y correcciones

- La previsualización devolvió error 321 cuando TWS estaba en solo lectura. Se habilitó el envío únicamente para la prueba explícita en la sesión Paper. [Configuración de la API de TWS](https://interactivebrokers.github.io/tws-api/initial_setup.html)
- Las corridas controladas de 913 EUR y 914 EUR previas a la prueba automática tuvieron ejecuciones completas que quedaron pendientes en SQLite. La conciliación comprobó cada referencia en TWS y registró el resultado local sin volver a enviar las órdenes.
- La posición por contrato pudo devolver 0 con efectivo en EUR. El ejecutor Paper pasó a consultar el saldo de divisas. Después de un llenado completo, espera de forma acotada la actualización del efectivo y de su propia orden en la lista de abiertas. [Saldos por divisa en TWS](https://www.interactivebrokers.com/support/faq/content/see-actual-cash-balances-tws-118722609)
- IBKR avisó que el tamaño era inferior al mínimo de IDEALPRO. Las órdenes del ciclo automático se ejecutaron como lotes pequeños. [Lotes pequeños en IBKR](https://www.interactivebrokers.com/support/faq/content/odd-lot-fx-transactions-1163260635)

## Alcance del cierre y pendientes del paso 20

La corrida conservó evidencia de cotizaciones en tiempo real, velas de Spark, BUY y SELL generados por SMA 3/5, ambas ejecuciones en TWS y el cierre de posición confirmado. Quedan pendientes las pruebas de recuperación de servicios y la reproducción por otro integrante.
