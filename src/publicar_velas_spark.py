
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

spark = (
    SparkSession.builder
    .appName("VelasOHLCStreaming")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Tópicos configurables (docker run -e TOPICO_TICKS=... -e TOPICO_VELAS=...).
# Sin variables: tópicos oficiales. Cada tópico de velas usa su propio checkpoint.
TOPICO_TICKS = os.environ.get("TOPICO_TICKS", "ticks")
TOPICO_VELAS = os.environ.get("TOPICO_VELAS", "velas")
CHECKPOINT = f"/checkpoint/{TOPICO_VELAS}"

esquema = StructType([
    StructField("symbol", StringType()),
    StructField("bid", DoubleType()),
    StructField("ask", DoubleType()),
    StructField("timestamp", StringType())
])

# Leer nuevos ticks desde Kafka
datos = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", TOPICO_TICKS)
    .option("startingOffsets", "latest")
    .load()
)

# Preparar cotizaciones
ticks = (
    datos
    .select(
        F.from_json(F.col("value").cast("string"), esquema).alias("dato"),
        "partition", "offset"
    )
    .select("dato.*", "partition", "offset")
    .withColumn("fecha", F.to_timestamp("timestamp"))
    .withColumn(
        "precio",
        F.round((F.col("bid") + F.col("ask")) / 2, 5)
    )
    .filter(
        (F.col("symbol") == "EURUSD") &
        (F.col("bid") > 0) &
        (F.col("ask") >= F.col("bid")) &
        F.col("fecha").isNotNull()
    )
    .withWatermark("fecha", "10 seconds")
    .withColumn(
        "orden",
        F.struct("fecha", "partition", "offset")
    )
)

# Agrupar cotizaciones en velas de un minuto
velas = (
    ticks
    .groupBy("symbol", F.window("fecha", "1 minute"))
    .agg(
        F.min_by("precio", "orden").alias("open"),
        F.max("precio").alias("high"),
        F.min("precio").alias("low"),
        F.max_by("precio", "orden").alias("close"),
        F.count("*").alias("num_ticks")
    )
    .select(
        "symbol",
        F.date_format(
            F.col("window.start"),
            "yyyy-MM-dd'T'HH:mm:ss'Z'"
        ).alias("inicio"),
        "open", "high", "low", "close", "num_ticks"
    )
)

# Convertir cada vela al JSON requerido
mensajes = velas.select(
    F.col("symbol").alias("key"),
    F.to_json(
        F.struct(
            "symbol", "inicio", "open",
            "high", "low", "close", "num_ticks"
        )
    ).alias("value")
)

# Publicar las velas completas en Kafka
consulta = (
    mensajes.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", TOPICO_VELAS)
    .option("checkpointLocation", CHECKPOINT)
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .start()
)

print(f"Streaming iniciado: {TOPICO_TICKS} -> {TOPICO_VELAS} (checkpoint {CHECKPOINT})", flush=True)

consulta.awaitTermination()
