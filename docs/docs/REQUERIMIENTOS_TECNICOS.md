# Requerimientos técnicos y entorno de desarrollo

**Fecha del corte:** 27/11/2026
**Versión documental:** 1.4
**Entornos:** equipo inicial Windows 11 con aproximadamente 8 GB RAM para Kafka/Spark y equipo independiente Windows para pruebas IBKR.
**Objetivo:** indicar qué se necesita, qué funciona hoy y cómo repetir las pruebas existentes.

**Para instalar desde cero en otra computadora:** seguir primero **[INSTALACION_PASO_A_PASO.md](INSTALACION_PASO_A_PASO.md)**. Este documento es un inventario técnico, no sustituye la guía de instalación. Los integrantes que solo siguen pantalla no necesitan instalar las herramientas.

## 1. Requisitos solicitados por el profesor

| Elemento | Papel en el proyecto | Estado |
|---|---|---|
| Interactive Brokers (IBKR) demo/paper | Fuente de precios y destino de órdenes **simuladas** | Cotizaciones en tiempo real y ejecución controlada en Paper verificadas |
| Mercado FOREX | Mercado en que debe operar el algoritmo | EURUSD seleccionado provisionalmente |
| Apache Kafka | Recibe eventos en `ticks`, `velas` y `signals` | Funcionando |
| Apache Spark Structured Streaming | Transforma ticks en velas de 1 minuto | Velas OHLC publicadas con ticks controlados y de mercado |
| Algoritmo de trading | Consume ticks o velas y publica BUY/SELL | SMA 10/50 implementada |
| Ejecutor con gestión de riesgo | Revisa señales y envía órdenes a cuenta paper | Dry Run aprobado y ciclo de órdenes controladas en Paper aprobado |
| Backtesting | Prueba con datos históricos | Implementado; deseable, no obligatorio |

**No obligatorio en el enunciado:** Windows 11, Python, Docker, VS Code, Git, GitHub, `confluent-kafka`. Estas son decisiones de implementación del equipo.

## 2. Equipo y software verificados

| Recurso / herramienta | Valor comprobado |
|---|---|
| Sistema operativo | Windows 11 |
| Python | 3.11.9 |
| VS Code | 1.145.1 (x64) |
| Docker Engine / CLI | 29.8.0 |
| Docker Compose | v5.5.1 (instalado, **no usado todavía**) |
| WSL | WSL 2 con distribución `docker-desktop` |
| Git | 2.54.0.windows.1 |
| RAM física aproximada | 8 GB (Windows informó 8,260,263,936 bytes) |
| RAM reportada por el motor Docker | 3.673 GiB |
| CPU visibles por Docker | 12 |
| Apache Kafka | Imagen `apache/kafka:4.4.0` |
| Apache Spark | Imagen `apache/spark:4.2.1-scala2.13-java17-python3-ubuntu` |
| Cliente Kafka Python | confluent-kafka==2.16.2 |
| Procesamiento de datos | pandas==3.1.1 |
| Visualización | matplotlib==3.11.2 |
| Datos históricos | yfinance==1.8.0 |
| API Interactive Brokers | ib_async==2.1.0 |

**Recursos:** en la medición del 21/11/2026, el contenedor Kafka utilizó **608.6 MiB** de memoria y **3.70 % de CPU**. Esta es una medición puntual, no una garantía de consumo constante. Con aproximadamente 8 GB físicos de RAM, se debe probar Spark en modo local y evitar lanzar múltiples procesos grandes simultáneamente.

## 3. Carpetas y entorno virtual

En la computadora ejecutora el proyecto está en:

```powershell
C:\Users\maria\Desktop\Proyecto_Trading_Algoritmico
```

En la computadora ejecutora el entorno `.venv` **ya existe**. Para trabajar desde la carpeta raíz en **PowerShell**:

```powershell
cd C:\Users\maria\Desktop\Proyecto_Trading_Algoritmico
.\.venv\Scripts\Activate.ps1
python --version
```

**Entorno virtual (`.venv`):** instalación aislada de librerías para este proyecto. Se detectó otra carpeta `venv/`; no subir ninguna de las dos al repositorio.

Instalación de las dependencias Python registradas en requirements.txt:

```powershell
python -m pip install -r requirements.txt
python -m pip show confluent-kafka
```

Para una computadora nueva, primero ejecutar `python -m venv .venv` desde la raíz y después activar `.venv` (véase la guía). No se ha necesitado instalar `pyspark` en Windows: Spark se ejecuta dentro de su imagen Docker. Las cinco dependencias directas fueron verificadas y fijadas en requirements.txt el 26/11/2026. La comprobación mediante pip check no detectó incompatibilidades en el entorno local.

## 4. Kafka: cómo revisar el entorno

Kafka se creó con un contenedor local llamado `kafka-trading` y el puerto `9092` publicado a Windows:

```powershell
docker ps
docker exec kafka-trading /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

Deben aparecer los tres tópicos:

```text
signals
ticks
velas
```

Si el contenedor **ya existe pero está detenido**, puede iniciarse con:

```powershell
docker start kafka-trading
```

**No ejecutar de nuevo `docker run --name kafka-trading ...` si el contenedor ya existe**, porque Docker rechazará duplicar el nombre. No borrar ni recrear el contenedor mientras dependamos de sus mensajes de prueba.

Para inspeccionar su consumo:

```powershell
docker stats --no-stream kafka-trading
```

## 5. Prueba del productor Python

Desde la **raíz** del proyecto, con Kafka activo y `.venv` activado:

```powershell
python src/productor_ticks.py
```

El programa utilizado hasta este corte genera cinco cotizaciones **ficticias**, en intervalos de un segundo. No hay conexión con IBKR.

Para inspeccionar los mensajes de `ticks`:

```powershell
docker exec -it kafka-trading /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ticks --from-beginning
```

`--from-beginning` lee también mensajes históricos todavía retenidos por Kafka. Para finalizar el consumidor: `Ctrl + C`.

## 6. Prueba de lectura de Spark

Desde la **raíz del proyecto**, en PowerShell, con el contenedor `kafka-trading` activo:

```powershell
docker run --rm --network container:kafka-trading --memory=1792m --cpus=2 -e HOME=/tmp -v "${PWD}\src:/app:ro" --entrypoint /opt/spark/bin/spark-submit apache/spark:4.2.1-scala2.13-java17-python3-ubuntu --master "local[1]" --driver-memory 768m --conf spark.ui.enabled=false --conf spark.jars.ivy=/tmp/spark-ivy --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1 /app/leer_ticks_spark.py
```

**Qué significa:**

- `--network container:kafka-trading`: Spark comparte temporalmente la red de Kafka. Es una **solución para pruebas**, no la arquitectura final entre servicios.
- `--memory=1792m`, `--cpus=2`, `--driver-memory 768m`: límites de recursos para la prueba.
- `-v "${PWD}\src:/app:ro"`: pone los scripts `src` a disposición del contenedor, en modo solo lectura.
- `--packages ...`: incorpora el conector de Spark con Kafka.
- `-e HOME=/tmp` y `spark.jars.ivy=/tmp/spark-ivy`: solucionan el error observado cuando Ivy intentó escribir en `/nonexistent/.ivy2.5.2/cache`.

**Resultado comprobado:** Spark mostró **11 mensajes JSON** del tópico `ticks` en consola. El script leer_ticks_spark.py solo realiza lectura. El 27/11 se comprobó que `publicar_velas_spark.py` publica velas OHLC y que SMA 3/5 consume las velas de IBKR.

**Limitación de reproducibilidad:** como se usa `--rm`, el contenedor de Spark se elimina al terminar. La primera descarga de dependencias puede repetirse. Más adelante se propondrá un volumen persistente y una red Docker propia/Compose, probados con los recursos de esta computadora.

## 7. Interactive Brokers: configuración y seguridad

| Requerimiento | Configuración |
|---|---|
| Aplicación | Trader Workstation (TWS) |
| Sesión | Exclusivamente Paper Trading |
| Biblioteca Python | ib_async==2.1.0 |
| Host | 127.0.0.1 |
| Puerto API | 7497 |
| Permisos API | Solo lectura para cotizaciones y Dry Run. Escritura únicamente para pruebas explícitas en TWS Paper. |
| Conexiones permitidas | Localhost |
| Client ID del ejecutor | 31 en Dry Run, 32 en Paper |
| Cuenta autorizada | Configuración local mediante .env |
| Modalidad del ejecutor | Dry Run por defecto. Paper mediante `--paper` y un tópico Paper autorizado. |

La conexión API y la recepción de cotizaciones EURUSD
fueron verificadas el 24/11/2026.

El 26/11/2026 se comprobó el inicio autorizado del ejecutor,
el bloqueo automático ante el cierre de TWS y el rechazo
de inicio cuando TWS no está disponible.

El 27/11 TWS confirmó EURUSD en tiempo real mediante el tipo de dato 1. Los permisos de mercado pueden variar entre cuentas.

Documentación oficial proporcionada por el profesor:
https://interactivebrokers.github.io/tws-api/

Consultar también:
[Integración API IBKR](../INTEGRACION_API_IBKR.md).

## 8. Requerimientos del ejecutor de riesgo

### 8.1. Componentes

| Archivo | Función |
|---|---|
| src/ejecutor_riesgo.py | Consumo de señales y evaluación de riesgos |
| src/config_riesgo.py | Parámetros de riesgo |
| src/calculo_riesgo.py | Dimensionamiento y riesgo monetario |
| src/seguridad_ibkr.py | Validación de conexión y cuenta autorizada |
| src/ordenes_paper.py | Envío Paper, verificación de ejecuciones, consulta del efectivo EUR y conciliación |
| src/validar_senales.py | Validación del Bloque B en `signals_prueba` |
| bases SQLite en `datos_prueba/` | Persistencia por modo y corrida |

### 8.2. Parámetros de simulación

| Parámetro | Valor |
|---|---|
| Instrumento | EURUSD |
| Exposición nominal máxima | 1000 USD |
| Riesgo monetario máximo teórico | 20 USD |
| Stop Loss | 2 % del precio |
| Take Profit | 4 % del precio |
| Posiciones lógicas simultáneas | 1 |
| Vigencia máxima de señal | 120 segundos |
| Tolerancia temporal futura | 30 segundos |
| Tópicos Kafka | Dry Run en `signals` o `signals_prueba`. Paper en `signals_paper_prueba` o `signals_paper_revalidacion_<id>`. |
| Servidor Kafka | localhost:9092 |
| Grupos consumidores | Dry Run en `ejecutor_persistente_v1` o su variante de prueba. Paper en `ejecutor_paper_v1` o `ejecutor_paper_revalidacion_<id>`. |

Los parámetros corresponden a decisiones de implementación
para simulación y no constituyen garantías de pérdida máxima.

### 8.3. Persistencia local

Se utiliza sqlite3, incluido en la biblioteca estándar de Python.
No requiere instalar un servidor de bases de datos ni una
dependencia adicional mediante pip.

En Dry Run, SQLite conserva señales aceptadas y la posición lógica. Paper usa una base separada y registra la intención de envío, la referencia de TWS y el resultado. El ejecutor actualiza la posición en una transacción después del llenado completo y la conciliación del saldo EUR. Un resultado incierto queda pendiente y bloquea el reenvío.

### 8.4. Configuración privada

El ejecutor requiere un archivo .env en la raíz del proyecto
con la variable IBKR_PAPER_ACCOUNT.

Cada integrante deberá configurar su propia cuenta Paper
autorizada, sin compartir identificadores privados.

Mantener `.env` y las bases SQLite fuera de los archivos a publicar. Comprobar las exclusiones de Git antes de preparar una entrega.

No se requiere python-dotenv: la configuración se lee
mediante pathlib, incluido en Python.

### 8.5. Restricciones de seguridad

- No utilizar cuentas reales.
- Habilitar permisos de escritura únicamente durante una prueba explícita en la cuenta Paper autorizada.
- No enviar órdenes mientras el ejecutor permanezca en Dry Run.
- Validar la cuenta autorizada antes de procesar señales.
- Bloquear el procesamiento ante pérdida de conexión detectada.
- No publicar credenciales, identificadores de cuenta ni archivos privados.
- No interpretar el puerto o prefijo de cuenta como prueba suficiente de Paper Trading.

## 9. Pendientes técnicos

1. Reproducir el reinicio de Spark desde su checkpoint y probar interrupciones de Kafka y TWS.
2. Repetir la instalación y las pruebas con otro integrante y su propia cuenta Paper.

## 10. Alcance verificado

El Bloque B aprobó el envío de ambas señales controladas por Kafka y su aceptación en Dry Run. El ciclo Paper de 914 EUR aprobó el envío, la ejecución completa y la conciliación automática con TWS. El aviso por tamaño inferior al mínimo de IDEALPRO apareció durante la prueba y ambas órdenes se ejecutaron. La corrida integral posterior verificó el mismo cierre con señales SMA originadas por cotizaciones de IBKR.

Para EURUSD en Paper, la consulta de posiciones por contrato puede devolver cero mientras existe efectivo en EUR. El ejecutor consulta el saldo de divisas de la cuenta. Tras un llenado completo, espera de forma acotada la actualización del saldo y la desaparición de su propia orden de la lista de abiertas. Una orden ajena o un saldo inesperado bloquean el procesamiento.

Consulta la [bitácora de validación](BITACORA_VALIDACION_2026-11-27.md) y el [mapa de trabajo](MAPA_DE_TRABAJO.md).

## 11. Segundo entorno verificado — 26/11/2026

| Recurso | Valor |
|---|---|
| Docker Desktop | 4.100.1 |
| WSL | 3.0.5 |
| Docker Engine / CLI | 29.9.3 |
| Compose | 5.7.1 (no utilizado) |
| Recursos Docker | 16 CPU; 6.695 GiB RAM |
| Kafka | apache/kafka:4.4.0; kafka-trading; 127.0.0.1:9092 |
| Spark | 4.2.1; Scala 2.13.18; Java 17.0.20.1 |
| Entorno Python | venv; cinco dependencias directas verificadas; pip check correcto |
| Checkpoint | .checkpoints/velas en host; /checkpoint/velas en Spark |

Git Bash requiere MSYS_NO_PATHCONV=1 para rutas Linux y pwd -W para montajes Windows. El productor ficticio utiliza IPv4 explícita y broker.address.family=v4; diez mensajes confirmados. La configuración de los otros clientes Kafka no fue modificada ni validada en esta sesión. No se requiere PySpark local. Inicio OHLC comprobado, salida y recuperación pendientes. Consultar sección G de la guía de instalación.
