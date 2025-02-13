from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, lead, window, count, avg, max, min, sum, when, expr, unix_timestamp
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Initialiser SparkSession
spark = SparkSession.builder.appName("CorrelationEvenementsSismiques").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Chemins des données
chemin_sismique = "hdfs://namenode:9000/user/hive/warehouse/seismic_data/dataset_sismique_nettoye"
chemin_sismique_villes = "hdfs://namenode:9000/user/hive/warehouse/seismic_data/dataset_sismique_villes_nettoye"

def analyser_precurseurs(df, nom_dataset, window_minutes=60):
    print(f"\n=== Analyse des précurseurs pour {nom_dataset} ===")
    
    # 1. Convertir le timestamp en secondes pour la fenêtre
    df = df.withColumn("time_seconds", unix_timestamp("date"))
    
    # 2. Définir les fenêtres d'analyse
    window_spec = Window.orderBy("time_seconds")
    window_prev = Window.orderBy("time_seconds").rangeBetween(-window_minutes * 60, 0)
    
    # 3. Calculer les métriques précédentes
    df_precurseurs = df \
        .withColumn("magnitude_precedente", lag("magnitude", 1).over(window_spec)) \
        .withColumn("tension_precedente", lag("tension_entre_plaque", 1).over(window_spec)) \
        .withColumn("nb_secousses_precedentes", 
                    count(when(col("secousse") == True, 1))
                    .over(window_prev)) \
        .withColumn("magnitude_moy_precedente", 
                    avg("magnitude").over(window_prev)) \
        .withColumn("tension_moy_precedente", 
                    avg("tension_entre_plaque").over(window_prev))
    
    # 4. Identifier les événements majeurs (magnitude > 90e percentile)
    seuil_magnitude = df.selectExpr(f"percentile_approx(magnitude, 0.90)").collect()[0][0]
    
    print(f"\nAnalyse des précurseurs pour événements majeurs (magnitude > {seuil_magnitude:.2f}):")
    
    # 5. Analyser les conditions précédant les événements majeurs
    conditions_precurseurs = df_precurseurs \
        .filter(col("magnitude") > seuil_magnitude) \
        .agg(
            avg("nb_secousses_precedentes").alias("moy_secousses_avant"),
            avg("magnitude_moy_precedente").alias("moy_magnitude_avant"),
            avg("tension_moy_precedente").alias("moy_tension_avant"),
            count("*").alias("nombre_evenements_majeurs")
        )
    
    print("\nConditions moyennes précédant les événements majeurs:")
    conditions_precurseurs.show()
    
    # 6. Analyse des séquences de secousses
    df_sequences = df_precurseurs \
        .filter(col("secousse") == True) \
        .select(
            "date",
            "magnitude",
            "tension_entre_plaque",
            "nb_secousses_precedentes",
            "magnitude_moy_precedente",
            "tension_moy_precedente"
        )
    
    print("\nAnalyse des séquences de secousses:")
    print("Statistiques sur les séquences:")
    df_sequences.describe().show()
    
    # Calculer et afficher les corrélations importantes
    colonnes_correlation = ["magnitude", "tension_entre_plaque", "nb_secousses_precedentes", 
                          "magnitude_moy_precedente", "tension_moy_precedente"]
    
    print("\nCorrélations significatives entre variables:")
    for col1 in colonnes_correlation:
        for col2 in colonnes_correlation:
            if col1 < col2:
                correlation = df_sequences.stat.corr(col1, col2)
                if abs(correlation) > 0.3:  # Ne montrer que les corrélations significatives
                    print(f"Corrélation {col1} - {col2}: {correlation:.3f}")
    
    # 7. Analyser la distribution temporelle des événements majeurs
    print("\nDistribution temporelle des événements majeurs:")
    df_precurseurs \
        .filter(col("magnitude") > seuil_magnitude) \
        .groupBy(F.hour("date").alias("heure")) \
        .agg(count("*").alias("nombre_evenements")) \
        .orderBy("heure") \
        .show()
    
    return df_precurseurs

# Analyser les deux datasets
print("Analyse du dataset général:")
df_sismique = spark.read.csv(chemin_sismique, header=True, inferSchema=True)
precurseurs_general = analyser_precurseurs(df_sismique, "dataset général")

print("\nAnalyse du dataset par villes:")
df_villes = spark.read.csv(chemin_sismique_villes, header=True, inferSchema=True)
precurseurs_villes = analyser_precurseurs(df_villes, "dataset par villes")

# Analyse spécifique par ville
if "ville" in df_villes.columns:
    print("\n=== Analyse des corrélations par ville ===")
    for ville in df_villes.select("ville").distinct().collect():
        ville_nom = ville["ville"]
        print(f"\nAnalyse pour {ville_nom}:")
        df_ville = df_villes.filter(col("ville") == ville_nom)
        analyser_precurseurs(df_ville, f"ville de {ville_nom}")

spark.stop()