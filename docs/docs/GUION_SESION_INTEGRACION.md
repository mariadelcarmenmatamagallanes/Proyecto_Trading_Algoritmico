# Guion de la sesión de integración de extremo a extremo

**Objetivo**

Reproducir la verificación en Dry Run de las secciones 0 a 7 y, en la sección 8, el ciclo de integración con la cuenta Paper autorizada.

```
IBKR Paper → productor → Kafka `ticks` → Spark → Kafka `velas` → estrategia → Kafka `signals` → ejecutor (Dry Run)
```

**Duración estimada del guion Dry Run:** 60–90 minutos. Para la corrida Paper, contar el arranque de Spark y hasta 25 minutos por defecto para recibir señales de mercado.
**Responsable del guion:** Laura M. Sánchez Martínez.

Realiza la corrida de mercado cuando TWS esté recibiendo cotizaciones EURUSD en tiempo real. La estrategia necesita cinco velas de un minuto para empezar a producir señales.

---

## 0. Antes de empezar (una sola computadora)

| # | Revisar | Cómo | Debe verse |
|---|---|---|---|
| 0.1 | TWS abierto en modo **Paper** con la API habilitada (puerto 7497) | Ventana de TWS | Sesión Paper iniciada |
| 0.2 | Archivo `.env` con la cuenta Paper autorizada | `git check-ignore -v .env` | `.gitignore` aparece (no se sube) |
| 0.3 | Docker Desktop encendido | `docker info` | Secciones Client y Server |
| 0.4 | Kafka encendido | `docker start kafka-trading` y `docker ps` | `kafka-trading` en estado `Up` |
| 0.5 | Tópicos creados | `docker exec kafka-trading /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092` | `ticks`, `velas`, `signals` |
| 0.6 | Entorno Python actualizado | `git pull origin main` y luego `python -m pip install -r requirements.txt` | Sin errores |
| 0.7 | Pruebas de la estrategia | `python -m unittest discover tests` | `OK` |
| 0.8 | Estado limpio del ejecutor | Ver nota abajo | — |
| 0.9 | Memoria libre | Cerrar navegador y apps pesadas | Docker con ~3.5 GB disponibles |

**Nota 0.8, reiniciar la posición lógica del ejecutor.** El ejecutor guarda en SQLite si "cree" que hay una posición abierta. Si una prueba anterior la dejó abierta, rechazará el primer BUY. Antes de la sesión, respaldar el archivo para empezar en limpio (no se borra):

```powershell
if (Test-Path datos_prueba\ejecutor.sqlite3) { Rename-Item datos_prueba\ejecutor.sqlite3 ("ejecutor_respaldo_" + (Get-Date -Format "yyyyMMdd_HHmm") + ".sqlite3") }
```

---

## 1. Encender los componentes (en este orden)

Cada componente va en **su propia terminal** de VS Code (botón `+`), todas en la raíz del proyecto y con `.venv` activado.

El orden importa: Spark, el ejecutor y la estrategia deben estar escuchando **antes** de que lleguen datos.

### Terminal 1: Spark (ticks → velas)

Crear primero el checkpoint persistente desde la raíz (PowerShell):

```powershell
New-Item -ItemType Directory -Force .checkpoints/velas
```

```powershell
docker run --rm --network container:kafka-trading --memory=1792m --cpus=2 -e HOME=/tmp -v "${PWD}\src:/app:ro" -v "${PWD}\.checkpoints:/checkpoint" --entrypoint /opt/spark/bin/spark-submit apache/spark:4.2.1-scala2.13-java17-python3-ubuntu --master "local[1]" --driver-memory 768m --conf spark.ui.enabled=false --conf spark.jars.ivy=/tmp/spark-ivy --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1 /app/publicar_velas_spark.py
```

✅ Esperar el mensaje `Streaming iniciado: ticks -> velas` (puede tardar 1–2 minutos la primera vez).

### Terminal 2: Ejecutor de riesgo (Dry Run)

```powershell
python src/ejecutor_riesgo.py
```

✅ Debe mostrar `[SEGURIDAD] Conexion Paper autorizada`, `Posición guardada: False` y `Esperando señales...`

### Terminal 3: Estrategia en modo demostración

```powershell
python src/estrategia_kafka.py --corta 3 --larga 5 --solo-nuevas
```

✅ Debe mostrar `Estrategia SMA_3_5 (DEMOSTRACIÓN) sobre EURUSD`.

> **¿Por qué 3/5 y no 10/50?** Con SMA 10/50 la primera señal tarda 50 minutos. Con 3/5 la lógica es idéntica y las señales llegan en minutos. En el reporte se aclara que la estrategia oficial es 10/50 y que 3/5 se usó solo para la demostración.
> **¿Por qué `--solo-nuevas`?** Para que la estrategia no use velas viejas del tópico. Si las usara, podría emitir señales antiguas que el ejecutor rechaza, y la estrategia quedaría "creyendo" que compró.

### Terminal 4: Productor IBKR (empieza el flujo)

```powershell
python src/productor_ibkr.py
```

✅ Deben aparecer líneas `Cotizacion confirmada: {"symbol": "EURUSD", "bid": ..., "ask": ..., "timestamp": ...}`

### Terminales 5 y 6: verificación de tópicos (solo para observar)

```powershell
docker exec -it kafka-trading /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic velas
```

```powershell
docker exec -it kafka-trading /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic signals
```

---

## 2. Qué debe pasar y cuándo

| Minuto aprox. | Qué se ve | Dónde |
|---|---|---|
| 0 | Cotizaciones de EURUSD confirmadas | Terminal 4 |
| 1–2 | Primera vela JSON `{symbol, inicio, open, high, low, close, num_ticks}` | Terminal 5 |
| 1–2 | `Vela ... (1/5 velas en memoria)` | Terminal 3 |
| ~6 | Ya aparecen los valores `SMA3=... SMA5=...` | Terminal 3 |
| Cuando SMA3 > SMA5 | `-> SEÑAL publicada: {... "signal": "BUY" ...}` | Terminales 3 y 6 |
| Inmediatamente después | `[DRY-RUN] BUY aceptada a 1.xxxx` | Terminal 2 |
| Cuando SMA3 < SMA5 | Señal SELL y `[DRY-RUN] SELL aceptada` | Terminales 3, 6 y 2 |

Dejar correr el flujo hasta ver **al menos un BUY y un SELL aceptados** (normalmente entre 15 y 40 minutos).

---

## 3. Pruebas rápidas de error (paso 20)

Con todo encendido, abrir una terminal más y un productor de consola sobre `signals`:

```powershell
docker exec -it kafka-trading /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic signals
```

Pegar una línea a la vez y observar la respuesta en la Terminal 2:

| Prueba | Línea a pegar | Respuesta esperada |
|---|---|---|
| Símbolo no permitido | `{"id_algoritmo":"PRUEBA","symbol":"GBPUSD","signal":"BUY","precio_referencia":1.27,"timestamp":"2026-11-27T15:00:00Z"}` | `[RECHAZADA] Símbolo no autorizado` |
| Faltan campos | `{"symbol":"EURUSD","signal":"BUY"}` | `[RECHAZADA] Faltan campos obligatorios` |
| Señal inválida | `{"id_algoritmo":"PRUEBA","symbol":"EURUSD","signal":"HOLD","precio_referencia":1.05,"timestamp":"2026-11-27T15:00:00Z"}` | `[RECHAZADA] Señal inválida` |
| Señal antigua | La primera línea, pero con `"symbol":"EURUSD"` y una hora de hace más de 2 minutos | `[RECHAZADA] Señal antigua o futura` |
| JSON roto | `esto no es json` | `[RECHAZADA] JSON incorrecto` |

Para terminar: cerrar TWS y confirmar que el ejecutor muestra `[BLOQUEADO] Conexion Paper perdida o no autorizada`.

---

## 4. Evidencias que hay que capturar

Hacer capturas **sin mostrar el número de cuenta** (tapar o recortar).

| # | Captura | Criterio de terminación que cubre |
|---|---|---|
| E1 | Terminal 4 con cotizaciones confirmadas | IBKR proporciona cotizaciones; el productor publica en `ticks` |
| E2 | Terminal 1 con `Streaming iniciado` y Terminal 5 con velas JSON | Spark genera velas de 1 minuto; las velas se publican en `velas` |
| E3 | Terminal 3 con velas, SMAs y la señal publicada | La estrategia consume velas |
| E4 | Terminal 6 con la señal JSON | Las señales se publican en `signals` |
| E5 | Terminal 2 con `[DRY-RUN] BUY aceptada` y `SELL aceptada` | El ejecutor valida los controles de riesgo |
| E6 | Terminal 2 con los `[RECHAZADA]` de la sección 3 | Escenarios de error |
| E7 | Terminal 2 con `[BLOQUEADO]` al cerrar TWS | Escenarios de error |
| E8 | Las 4 terminales al mismo tiempo (captura de pantalla completa) | Demostración de extremo a extremo |

Guardar las capturas en `docs/evidencias/` con nombres como `E1_productor_ibkr.png`.

---

## 5. Bitácora de la sesión

| Campo | Llenar |
|---|---|
| Fecha y hora (inicio / fin) | |
| Computadora utilizada | |
| Quién ejecutó / quién documentó | |
| Primera vela recibida (hora) | |
| Primera señal BUY (hora y precio) | |
| Primera señal SELL (hora y precio) | |
| Errores encontrados | |
| Solución aplicada | |
| Pendientes detectados | |

---

## 6. Problemas frecuentes

| Síntoma | Causa probable | Qué hacer |
|---|---|---|
| El productor no muestra cotizaciones | Mercado cerrado, TWS sin sesión o API deshabilitada | Revisar horario y configuración de API en TWS |
| `[BLOQUEADO] Conexion Paper no autorizada` al iniciar el ejecutor | `.env` falta o la cuenta no coincide | Revisar `.env` según la guía de instalación |
| Spark no publica velas | Spark se encendió después del productor, o falta memoria | Reiniciar Spark antes del productor; cerrar aplicaciones |
| Llegan velas pero la estrategia no muestra nada | Estrategia apagada o grupo de Kafka con lectura anterior | Reiniciar con `--solo-nuevas` |
| La estrategia publica, pero el ejecutor dice `Señal antigua o futura` | Retraso mayor a 120 s o reloj de la computadora desajustado | Sincronizar la hora de Windows; no usar velas viejas |
| `[RECHAZADA] Ya existe una posición` en el primer BUY | Estado SQLite de una prueba anterior | Aplicar la nota 0.8 y reiniciar el ejecutor |
| Error de memoria de Docker | 8 GB de RAM con TWS + Kafka + Spark | Cerrar navegador y apps; no abrir más terminales Spark |


## 7. Variante Git Bash y condiciones previas (26/11/2026)

```bash
mkdir -p .checkpoints/velas
MSYS_NO_PATHCONV=1 docker run --rm --network container:kafka-trading --memory=1792m --cpus=2 -e HOME=/tmp -v "$(pwd -W)/src:/app:ro" -v "$(pwd -W)/.checkpoints:/checkpoint" --entrypoint /opt/spark/bin/spark-submit apache/spark:4.2.1-scala2.13-java17-python3-ubuntu --master "local[1]" --driver-memory 768m --conf spark.ui.enabled=false --conf spark.jars.ivy=/tmp/spark-ivy --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1 /app/publicar_velas_spark.py
```


En Git Bash usa `MSYS_NO_PATHCONV=1` para los comandos Docker con rutas Linux y activa `venv` (o `.venv`, según el equipo). La salida OHLC se verificó el 27/11. El reinicio desde checkpoint figura entre las pruebas de resistencia pendientes.

## 8. Prueba integral con órdenes Paper

La corrida usa cotizaciones tipo 1 de TWS, un tópico Kafka de ticks, Spark, un tópico de velas y la estrategia SMA 3/5. Cada corrida crea sus propios tópicos, grupo del ejecutor y base SQLite. La estrategia aporta el lado, el identificador, el timestamp UTC y el cierre de la vela. El lote de prueba usa el bid o ask vigente para calcular el precio límite y conserva el cierre original en `cierre_sma`.

1. Confirma en TWS Paper el saldo de 0 EUR y que no haya órdenes EURUSD abiertas. Para la prueba explícita de órdenes, habilita la escritura de la API de esa sesión simulada.
2. Desde la raíz del proyecto y con `venv` activo, ejecuta el archivo entregado `probar_flujo_integral_paper.py`. Conserva `revalidar_ciclo_paper.py` en la misma carpeta que ese archivo.

   ```bash
   python -B "/ruta/entregada/probar_flujo_integral_paper.py"
   ```

3. Espera las velas y el BUY y SELL de la estrategia. La sonda inicial puede producir `[RECHAZADA] Faltan campos obligatorios`. Es un mensaje de control sin orden.
4. Guarda el identificador de corrida, ambas ejecuciones y la línea `RESULTADO: APROBADO`. El criterio de cierre es que TWS muestre 0 EUR y SQLite registre la posición cerrada con exactamente un BUY y un SELL ejecutados.
5. Si aparece `INCOMPLETO`, revisa el saldo y las ejecuciones de TWS antes de repetir. Una orden ejecutada puede quedar pendiente en SQLite si la respuesta de TWS llegó tarde. La conciliación usa su referencia y el llenado comprobado.

La prueba espera hasta 25 minutos por defecto. Si el mercado solo genera BUY, la ejecución puede terminar con esa posición Paper abierta. El resultado se registra como incompleto hasta resolver el saldo.

Consulta los resultados registrados en la [bitácora de validación](BITACORA_VALIDACION_2026-11-27.md).
