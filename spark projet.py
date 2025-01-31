import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType
from pyspark.sql.functions import (
    col, from_json, from_unixtime, to_timestamp, expr, to_json, struct
)

# ---------------------------------------------------------------------------
# Étape 1 : Configuration de Spark
# ---------------------------------------------------------------------------
# TODO : Modifier "tp-esme" par le nom de votre répertoire GitHub ou chemin local
os.environ["SPARK_HOME"] = "/workspaces/tp-esme/spark-3.2.3-bin-hadoop2.7"

spark = SparkSession.builder \
    .appName("KafkaWeatherConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ---------------------------------------------------------------------------
# Étape 2 : Définition du schéma JSON
# ---------------------------------------------------------------------------
weather_schema = StructType([
    StructField("coord", StructType([
        StructField("lon", DoubleType()),
        StructField("lat", DoubleType())
    ])),
    StructField("weather", ArrayType(StructType([
        StructField("main", StringType())
    ]))),
    StructField("main", StructType([
        StructField("temp", DoubleType()),
        StructField("pressure", LongType()),
        StructField("humidity", LongType())
    ])),
    StructField("wind", StructType([
        StructField("speed", DoubleType())
    ])),
    StructField("dt", LongType()),
    StructField("sys", StructType([
        StructField("country", StringType())
    ])),
    StructField("name", StringType())
])

# ---------------------------------------------------------------------------
# Étape 3 : Lecture des données en streaming depuis Kafka
# ---------------------------------------------------------------------------
# TODO : Remplacer "tp-meteo" par le nom du topic que vous souhaitez utiliser
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "tp-meteo")
    .option("startingOffsets", "earliest")
    .load()
)

# ---------------------------------------------------------------------------
# Étape 4 : Parsing et extraction des données
# ---------------------------------------------------------------------------
parsed_df = df.select(
    from_json(col("value").cast(StringType()), weather_schema).alias("data"),
    to_timestamp(from_unixtime(col("timestamp").cast(LongType()))).alias("date")
)

processed_df = parsed_df.select(
    col("date"),
    col("data.name").alias("city"),
    col("data.sys.country").alias("pays"),
    col("data.coord.lon").alias("lon"),
    col("data.coord.lat").alias("lat"),
    expr("filter(data.weather, x -> x.main is not null)[0].main").alias("weather"),
    col("data.main.temp").alias("temperature"),
    col("data.main.pressure").alias("pressure"),
    col("data.main.humidity").alias("humidity"),
    col("data.wind.speed").alias("speed")
)

# ---------------------------------------------------------------------------
# Étape 5 : Création de nouvelles variables
# ---------------------------------------------------------------------------

# TODO : Ajouter une colonne "heat_index" avec la formule :
# température + (0.5555 * ((6.11 * (10 ^ ((7.5 * température) / (237.7 + température))) * (humidité / 100)) - 10))

# TODO : Ajouter une colonne "severity_index" avec la formule :
# (vitesse du vent * 0.5) + ((1015 - pression) * 0.3) + (humidité * 0.2)

# TODO : Ajouter une colonne "time_of_day" pour catégoriser la période de la journée :
# Matin (6h-12h), Après-midi (12h-18h), Soirée (18h-24h), Nuit (0h-6h)

# ---------------------------------------------------------------------------
# Étape 6 : Transformation des données en JSON pour Kafka
# ---------------------------------------------------------------------------
# Todo: ajouter toutes les variables calculer
kafka_output_df = processed_df.select(
    to_json(struct(
        col("date"),
        col("city"),
        col("pays"),
        col("lon"),
        col("lat"),
        col("weather"),
        col("temperature"),
        col("pressure"),
        col("humidity"),
        col("speed")
    )).alias("value")  # Convertir les données en JSON pour Kafka
)

# ---------------------------------------------------------------------------
# Étape 7 : Écriture des résultats dans un topic Kafka
# ---------------------------------------------------------------------------
# TODO : Remplacer "tp-meteo-final" par le nom du topic Kafka où envoyer les données
query = (kafka_output_df
    .writeStream
    .format("kafka")
    .outputMode("append")
    .option("kafka.bootstrap.servers", "localhost:9092")  # Adresse du broker Kafka
    .option("topic", "tp-meteo-final")                   # Nom du topic Kafka de destination
    .option("checkpointLocation", "/tmp/checkpoints")    # Emplacement des checkpoints
    .start())

# Afficher les résultats dans la console (pour vérification)
console_query = (processed_df 
    .writeStream 
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .start())

query.awaitTermination()
console_query.awaitTermination()
