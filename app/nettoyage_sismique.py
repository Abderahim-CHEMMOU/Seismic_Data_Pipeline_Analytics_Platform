from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, when

# Initialiser SparkSession
spark = SparkSession.builder.appName("NettoyageDonneesSismiques").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
chemin_dataset_sismique = "hdfs://namenode:9000/user/hive/warehouse/seismic_data/dataset_sismique"
chemin_dataset_sismique_villes = "hdfs://namenode:9000/user/hive/warehouse/seismic_data/dataset_sismique_villes"

# ** Fonction pour nettoyer un dataset **
def nettoyer_dataset(chemin_csv, dataset_nom, avec_ville=False):
    print(f"** Nettoyage du dataset : {dataset_nom} **")

    # 1. Lecture du fichier CSV dans un DataFrame Spark
    df = spark.read.csv(chemin_csv, header=True, inferSchema=True)
    print("Schéma initial du DataFrame:")
    df.printSchema()

    # 2. Renommer la colonne tension pour les deux datasets
    df = df.withColumnRenamed("tension entre plaque", "tension_entre_plaque")
        
    # 3. Correction des types de données
    df_type_corrige = df.withColumn("date", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss")) \
                        .withColumn("secousse", when(col("secousse") == "True", True).otherwise(when(col("secousse") == "False", False).otherwise(col("secousse").cast("boolean")))) \
                        .withColumn("magnitude", col("magnitude").cast("double")) \
                        .withColumn("tension_entre_plaque", col("tension_entre_plaque").cast("double"))

    print("\nSchéma après correction des types:")
    df_type_corrige.printSchema()

    # 4. Gestion des valeurs manquantes (remplacement par 0 pour les colonnes numériques)
    colonnes_numeriques = ["magnitude", "tension_entre_plaque"]
        
    df_nettoye = df_type_corrige
    for col_num in colonnes_numeriques:
        df_nettoye = df_nettoye.fillna(0, subset=[col_num])

    # Afficher quelques lignes du DataFrame nettoyé
    print(f"\nPremières lignes du dataset {dataset_nom} nettoyé:")
    df_nettoye.show(5)

    # Retourner le DataFrame nettoyé
    return df_nettoye

# ** Nettoyer les deux datasets **
dataset_sismique_nettoye = nettoyer_dataset(chemin_dataset_sismique, "dataset_sismique")
dataset_sismique_villes_nettoye = nettoyer_dataset(chemin_dataset_sismique_villes, "dataset_sismique_villes", avec_ville=True)

# ** Enregistrer les DataFrames nettoyés **
chemin_output_sismique = "hdfs://namenode:9000/user/hive/warehouse/seismic_data/dataset_sismique_nettoye"
chemin_output_sismique_villes = "hdfs://namenode:9000/user/hive/warehouse/seismic_data/dataset_sismique_villes_nettoye"

dataset_sismique_nettoye.write.csv(chemin_output_sismique, header=True, mode="overwrite")
dataset_sismique_villes_nettoye.write.csv(chemin_output_sismique_villes, header=True, mode="overwrite")

print(f"\nDataset dataset_sismique nettoyé et sauvegardé dans : {chemin_output_sismique}")
print(f"Dataset dataset_sismique_villes nettoyé et sauvegardé dans : {chemin_output_sismique_villes}")

spark.stop()