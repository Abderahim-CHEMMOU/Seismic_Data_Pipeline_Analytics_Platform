from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

def create_spark_session():
    return SparkSession.builder \
        .appName("PredictionSismique") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .getOrCreate()

def detect_seismic_patterns():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    # Définir le schéma pour les données entrantes
    schema = StructType([
        StructField("timestamp", StringType()),
        StructField("secousse", BooleanType()),
        StructField("magnitude", DoubleType()),
        StructField("tension_entre_plaque", DoubleType())
    ])

    # Lire le flux Kafka
    df_streaming = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "topic1") \
        .load()

    # Parser les données JSON
    df_parsed = df_streaming \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*")

    # Convertir le timestamp en timestamp Spark
    df_parsed = df_parsed \
        .withColumn("event_time", to_timestamp(col("timestamp")))

    # Créer une fenêtre temporelle de 5 minutes
    window_duration = "5 minutes"
    slide_duration = "1 minute"

    # Calculer les statistiques sur la fenêtre glissante
    df_stats = df_parsed \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(window("event_time", window_duration, slide_duration)) \
        .agg(
            avg("tension_entre_plaque").alias("tension_avg"),
            max("tension_entre_plaque").alias("tension_max"),
            min("tension_entre_plaque").alias("tension_min"),
            count(when(col("secousse") == True, 1)).alias("nb_secousses"),
            max("magnitude").alias("max_magnitude")
        )

    # Calculer les indicateurs de risque
    risk_analysis = df_stats \
        .withColumn("tension_variation", col("tension_max") - col("tension_min")) \
        .withColumn("risk_level",
            when(
                # Risque élevé : forte variation de tension + tension moyenne élevée
                (col("tension_variation") > 0.02) & 
                (col("tension_avg") > 1.05),
                "ÉLEVÉ"
            ).when(
                # Risque modéré : tension élevée ou activité sismique récente
                (col("tension_max") > 1.08) | 
                (col("nb_secousses") > 0),
                "MODÉRÉ"
            ).when(
                # Risque faible : tension stable mais élevée
                (col("tension_avg") > 1.06) & 
                (col("tension_variation") < 0.01),
                "FAIBLE"
            ).otherwise("NORMAL")
        )

    # Générer les alertes
    alerts = risk_analysis \
        .withColumn("alert_message",
            when(col("risk_level") == "ÉLEVÉ",
                concat(
                    lit("⚠️ ALERTE RISQUE ÉLEVÉ - "),
                    lit("Tension moy: "), round(col("tension_avg"), 3),
                    lit(" - Variation: "), round(col("tension_variation"), 3),
                    lit(" - Secousses: "), col("nb_secousses")
                )
            ).when(col("risk_level") == "MODÉRÉ",
                concat(
                    lit("⚠️ RISQUE MODÉRÉ - "),
                    lit("Tension max: "), round(col("tension_max"), 3),
                    lit(" - Secousses: "), col("nb_secousses")
                )
            ).when(col("risk_level") == "FAIBLE",
                concat(
                    lit("ℹ️ Risque faible - "),
                    lit("Tension stable: "), round(col("tension_avg"), 3)
                )
            )
        )

    # Afficher les résultats
    query = alerts \
        .select(
            "window", 
            "tension_avg",
            "tension_variation",
            "nb_secousses",
            "max_magnitude",
            "risk_level",
            "alert_message"
        ) \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    detect_seismic_patterns()