from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, hour, minute, date_format, count, avg, max, sum, 
    window, when, expr, unix_timestamp
)
from pyspark.sql.window import Window

# Initialiser SparkSession
spark = SparkSession.builder.appName("AggregationDonneesSismiques").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Chemins des données
chemin_sismique = "hdfs://namenode:9000/user/hive/warehouse/seismic_data/dataset_sismique_nettoye"
chemin_sismique_villes = "hdfs://namenode:9000/user/hive/warehouse/seismic_data/dataset_sismique_villes_nettoye"

def analyser_agregations(df, nom_dataset, avec_ville=False):
    print(f"\n=== Analyse des agrégations pour {nom_dataset} ===")
    
    # 1. Agrégation par heure
    print("\n1. Statistiques par heure:")
    agg_horaire = df.groupBy(hour("date").alias("heure")) \
        .agg(
            count("*").alias("nombre_observations"),
            count(when(col("secousse") == True, 1)).alias("nombre_secousses"),
            avg("magnitude").alias("magnitude_moyenne"),
            max("magnitude").alias("magnitude_max"),
            avg("tension_entre_plaque").alias("tension_moyenne")
        ).orderBy("heure")
    
    agg_horaire.show()

    # 2. Agrégation par fenêtres de 15 minutes
    print("\n2. Statistiques par fenêtres de 15 minutes:")
    agg_15min = df.groupBy(
        window("date", "15 minutes")
    ).agg(
        count("*").alias("nombre_observations"),
        count(when(col("secousse") == True, 1)).alias("nombre_secousses"),
        avg("magnitude").alias("magnitude_moyenne"),
        max("magnitude").alias("magnitude_max"),
        avg("tension_entre_plaque").alias("tension_moyenne")
    ).orderBy("window")
    
    agg_15min.show(5)

    # 3. Analyse des pics d'activité
    print("\n3. Périodes de forte activité (> 90e percentile de magnitude):")
    seuil_magnitude = df.selectExpr("percentile_approx(magnitude, 0.90)").collect()[0][0]
    
    pics_activite = df.filter(col("magnitude") > seuil_magnitude) \
        .groupBy(window("date", "30 minutes")) \
        .agg(
            count("*").alias("nombre_evenements"),
            avg("magnitude").alias("magnitude_moyenne"),
            max("magnitude").alias("magnitude_max")
        ).orderBy(col("nombre_evenements").desc())
    
    pics_activite.show(5)

    # 4. Analyse des tendances de tension
    print("\n4. Évolution de la tension moyenne par période:")
    tendances_tension = df.groupBy(
        window("date", "1 hour")
    ).agg(
        avg("tension_entre_plaque").alias("tension_moyenne"),
        avg(when(col("secousse") == True, col("tension_entre_plaque"))).alias("tension_moyenne_secousses"),
        count(when(col("secousse") == True, 1)).alias("nombre_secousses")
    ).orderBy("window")
    
    tendances_tension.show(5)

    if avec_ville:
        # 5. Analyse par ville et par heure
        print("\n5. Statistiques par ville et par heure:")
        agg_ville_heure = df.groupBy("ville", hour("date").alias("heure")) \
            .agg(
                count("*").alias("nombre_observations"),
                count(when(col("secousse") == True, 1)).alias("nombre_secousses"),
                avg("magnitude").alias("magnitude_moyenne"),
                max("magnitude").alias("magnitude_max"),
                avg("tension_entre_plaque").alias("tension_moyenne")
            ).orderBy("ville", "heure")
        
        agg_ville_heure.show()

        # 6. Identification des périodes critiques par ville
        print("\n6. Périodes critiques par ville (magnitude > 90e percentile):")
        periodes_critiques = df.filter(col("magnitude") > seuil_magnitude) \
            .groupBy("ville", window("date", "1 hour")) \
            .agg(
                count("*").alias("nombre_evenements"),
                max("magnitude").alias("magnitude_max"),
                avg("tension_entre_plaque").alias("tension_moyenne")
            ).orderBy(col("nombre_evenements").desc())
        
        periodes_critiques.show()

    return agg_horaire, agg_15min, pics_activite, tendances_tension

# Analyser les deux datasets
print("Analyse du dataset général:")
df_sismique = spark.read.csv(chemin_sismique, header=True, inferSchema=True)
agg_general = analyser_agregations(df_sismique, "dataset général")

print("\nAnalyse du dataset par villes:")
df_villes = spark.read.csv(chemin_sismique_villes, header=True, inferSchema=True)
agg_villes = analyser_agregations(df_villes, "dataset par villes", avec_ville=True)

# Sauvegarder les résultats
resultats_path = "hdfs://namenode:9000/user/hive/warehouse/seismic_data/analyse_agregations/"

for i, (nom, df) in enumerate([
    ("agg_horaire", agg_general[0]),
    ("agg_15min", agg_general[1]),
    ("pics_activite", agg_general[2]),
    ("tendances_tension", agg_general[3])
]):
    df.write.mode("overwrite").parquet(f"{resultats_path}{nom}")

spark.stop()