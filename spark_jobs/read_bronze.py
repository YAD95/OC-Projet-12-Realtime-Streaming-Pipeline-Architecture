from pyspark.sql import SparkSession

# On allume une petite session Spark
spark = (
    SparkSession.builder.appName("ReadBronze")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# On charge tes données compressées
print("Lecture de la table Delta en cours...")
df = spark.read.format("delta").load("/opt/data/bronze/activities")

# On affiche un beau tableau de 10 lignes et le total
print("\n--- APERÇU DES 10 PREMIÈRES ACTIVITÉS ---")
df.show(10, truncate=False)

print(f"\n--- NOMBRE TOTAL DE LIGNES TRAITÉES : {df.count()} ---")