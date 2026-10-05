"""
Job Spark batch : calcule l'éligibilité aux deux avantages (prime sportive,
jours bien-être) à partir du bronze (activités) et du référentiel RH enrichi
(distance domicile-entreprise). Écrit deux tables Delta "gold" :

  - gold/eligibilite : détail par salarié, à USAGE INTERNE uniquement
    (contient le salaire brut -> ne pas brancher tel quel dans un Power BI
    visible par tous).
  - gold/kpis_par_bu : agrégats par service, sans donnée individuelle
    sensible, c'est CETTE table qu'on branche dans Power BI.

Les paramètres métier (taux de prime, seuil d'activités...) viennent de
config/parametres.json et PAS du code : modifie ce fichier puis relance ce
job pour tout recalculer avec les nouvelles règles.

Usage (depuis le conteneur spark) :
    spark-submit --packages io.delta:delta-spark_2.12:3.2.0 \
        /opt/spark_jobs/enrichment_gold.py
"""

import argparse
import json

from pyspark.sql import SparkSession, functions as F

MODES_ACTIFS = ["Marche/running", "Vélo/Trottinette/Autres"]


def get_spark(app_name="sds-enrichment-gold"):
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def charger_parametres(path):
    with open(path) as f:
        return json.load(f)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze-path", default="/opt/data/bronze/activities")
    parser.add_argument("--referentiel-path", default="/opt/data/referentiel/rh")
    parser.add_argument("--params-file", default="/opt/spark_jobs/config/parametres.json")
    parser.add_argument("--gold-eligibilite-path", default="/opt/data/gold/eligibilite")
    parser.add_argument("--gold-kpis-path", default="/opt/data/gold/kpis_par_bu")
    args = parser.parse_args()

    params = charger_parametres(args.params_file)
    taux_prime = params["taux_prime"]
    seuil_activites = params["seuil_activites_bien_etre"]
    jours_accordes = params["jours_bien_etre_accordes"]

    print(f"Paramètres utilisés : taux={taux_prime}, seuil={seuil_activites}, jours={jours_accordes}")

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    bronze = spark.read.format("delta").load(args.bronze_path)
    referentiel = spark.read.format("delta").load(args.referentiel_path)

    # Nombre d'activités sur les 12 derniers mois glissants, par salarié
    date_limite = F.date_sub(F.current_date(), 365)
    nb_activites = (
        bronze.filter(F.col("date_debut") >= date_limite)
        .groupBy("id_salarie")
        .agg(F.count("*").alias("nb_activites_12mois"))
    )

    referentiel_std = referentiel.select(
        F.col("ID salarié").alias("id_salarie"),
        F.col("BU").alias("bu"),
        F.col("Salaire brut").alias("salaire_brut"),
        F.col("Moyen de déplacement").alias("moyen_declare"),
        F.col("distance_km"),
        F.col("distance_anormale"),
    )

    eligibilite = (
        referentiel_std.join(nb_activites, on="id_salarie", how="left")
        .fillna({"nb_activites_12mois": 0})
        .withColumn("mode_actif_declare", F.col("moyen_declare").isin(MODES_ACTIFS))
        .withColumn(
            "distance_a_valider",
            F.col("mode_actif_declare") & F.col("distance_anormale").isNull(),
        )
        .withColumn(
            "eligible_prime",
            F.col("mode_actif_declare") & (F.col("distance_anormale") == False),  # noqa: E712
        )
        .withColumn(
            "montant_prime",
            F.when(F.col("eligible_prime"), F.col("salaire_brut") * F.lit(taux_prime)).otherwise(F.lit(0.0)),
        )
        .withColumn("eligible_bien_etre", F.col("nb_activites_12mois") >= F.lit(seuil_activites))
        .withColumn(
            "jours_bien_etre",
            F.when(F.col("eligible_bien_etre"), F.lit(jours_accordes)).otherwise(F.lit(0)),
        )
        .withColumn("calcule_le", F.current_timestamp())
    )

    eligibilite.write.format("delta").mode("overwrite").save(args.gold_eligibilite_path)

    # Agrégats par BU : rien de nominatif, c'est cette table qu'on donne à Power BI
    kpis = (
        eligibilite.groupBy("bu")
        .agg(
            F.count("*").alias("nb_salaries"),
            F.sum(F.col("eligible_prime").cast("int")).alias("nb_eligibles_prime"),
            F.sum("montant_prime").alias("cout_total_prime"),
            F.sum(F.col("eligible_bien_etre").cast("int")).alias("nb_eligibles_bien_etre"),
            F.sum("jours_bien_etre").alias("total_jours_bien_etre"),
            F.sum(F.col("distance_a_valider").cast("int")).alias("nb_en_attente_validation_distance"),
        )
        .withColumn("calcule_le", F.current_timestamp())
    )

    kpis.write.format("delta").mode("overwrite").save(args.gold_kpis_path)

    print("Gold éligibilité et KPIs par BU écrits avec succès.")
    kpis.show(truncate=False)


if __name__ == "__main__":
    main()