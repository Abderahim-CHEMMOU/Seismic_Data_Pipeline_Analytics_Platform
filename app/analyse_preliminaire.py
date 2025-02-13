from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, count, avg, max, when, hour, 
    date_format, expr, to_timestamp
)

# Initialiser SparkSession
spark = SparkSession.builder.appName("AnalyseEvenementsSismiques").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Chemins des données
chemin_sismique = "hdfs://namenode:9000/user/hive/warehouse/seismic_data/dataset_sismique_nettoye"
chemin_sismique_villes = "hdfs://namenode:9000/user/hive/warehouse/seismic_data/dataset_sismique_villes_nettoye"

# Charger les données
df_sismique = spark.read.csv(chemin_sismique, header=True, inferSchema=True)

# Assurer que la colonne date est bien un timestamp
df_sismique = df_sismique.withColumn("date", to_timestamp(col("date")))

# Créer les statistiques horaires avec la fenêtre de temps correcte
df_stats_horaires = df_sismique \
    .groupBy(window("date", "1 hour")) \
    .agg(
        count("*").alias("nombre_observations"),
        count(when(col("secousse") == True, 1)).alias("nombre_secousses"),
        avg("magnitude").alias("magnitude_moyenne"),
        max("magnitude").alias("magnitude_max"),
        avg("tension_entre_plaque").alias("tension_moyenne")
    ) \
    .withColumn("window_start", date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss")) \
    .drop("window")

# Afficher quelques lignes pour vérification
print("Aperçu des statistiques horaires :")
df_stats_horaires.show(5, truncate=False)

# Sauvegarder au format Parquet
df_stats_horaires.write.mode("overwrite") \
    .parquet("hdfs://namenode:9000/user/hive/warehouse/seismic_data/analyse_stats_horaires")

spark.stop()