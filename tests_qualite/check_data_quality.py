"""
Tests de qualité et de cohérence des données du pipeline (bronze, référentiel,
gold). Couvre les points demandés par la note de cadrage (distances non
négatives, dates valides, etc.), implémentés en pandas pur pour rester léger
et fiable sur tous les environnements.

Génère un rapport dans tests_qualite/rapport_qualite.md.

Usage :
    python tests_qualite/check_data_quality.py
"""

import json
import os
from datetime import datetime

import pandas as pd
from deltalake import DeltaTable

BRONZE_PATH = os.getenv("BRONZE_PATH", "./data/bronze/activities")
REFERENTIEL_PATH = os.getenv("REFERENTIEL_PATH", "./data/referentiel/rh")
GOLD_ELIGIBILITE_PATH = os.getenv("GOLD_ELIGIBILITE_PATH", "./data/gold/eligibilite")
PARAMS_PATH = os.getenv("PARAMS_PATH", "./spark_jobs/config/parametres.json")

SPORTS_CONNUS = {
    "Course à pied", "Randonnée", "Vélo", "Natation", "Triathlon",
    "Tennis", "Football", "Rugby", "Badminton", "Voile",
    "Judo", "Boxe", "Escalade", "Équitation", "Tennis de table", "Basketball",
}

resultats = []


def verifier(nom, description, condition_violee):
    """condition_violee : Series booléenne, True = ligne en anomalie."""
    nb_violations = int(condition_violee.sum())
    statut = "ÉCHEC" if nb_violations > 0 else "OK"
    resultats.append({"check": nom, "description": description, "statut": statut, "violations": nb_violations})


def charger_delta(path):
    return DeltaTable(path).to_pandas()


def tests_bronze(bronze):
    verifier(
        "distance_non_negative",
        "La distance parcourue ne doit jamais être négative",
        bronze["distance_m"].fillna(0) < 0,
    )
    verifier(
        "date_debut_renseignee",
        "La date de début d'activité doit toujours être renseignée",
        bronze["date_debut"].isna(),
    )
    verifier(
        "date_debut_pas_future",
        "La date de début ne doit pas être dans le futur",
        pd.to_datetime(bronze["date_debut"]) > pd.Timestamp.now(tz="UTC"),
    )
    verifier(
        "date_fin_apres_debut",
        "La date de fin doit être postérieure (ou égale) à la date de début",
        bronze["date_fin"].notna() & (pd.to_datetime(bronze["date_fin"]) < pd.to_datetime(bronze["date_debut"])),
    )
    verifier(
        "sport_type_connu",
        "Le type de sport doit faire partie de la liste des sports gérés",
        ~bronze["sport_type"].isin(SPORTS_CONNUS),
    )


def tests_referentiel(referentiel):
    verifier(
        "id_salarie_unique",
        "Chaque ID salarié doit apparaître une seule fois dans le référentiel",
        referentiel["ID salarié"].duplicated(),
    )
    verifier(
        "salaire_positif",
        "Le salaire brut doit être strictement positif",
        referentiel["Salaire brut"].fillna(0) <= 0,
    )
    verifier(
        "distance_coherente",
        "Si une distance a été calculée, elle doit être positive",
        referentiel["distance_km"].notna() & (referentiel["distance_km"] <= 0),
    )


def tests_integrite(bronze, referentiel):
    ids_connus = set(referentiel["ID salarié"])
    verifier(
        "id_salarie_existe_dans_rh",
        "Chaque activité doit référencer un salarié existant dans le référentiel RH",
        ~bronze["id_salarie"].isin(ids_connus),
    )


def tests_gold(gold, jours_accordes):
    verifier(
        "montant_prime_positif_ou_nul",
        "Le montant de prime calculé ne doit jamais être négatif",
        gold["montant_prime"].fillna(0) < 0,
    )
    verifier(
        "jours_bien_etre_valides",
        f"Les jours bien-être accordés doivent être 0 ou {jours_accordes} (valeur paramétrée)",
        ~gold["jours_bien_etre"].isin([0, jours_accordes]),
    )


def ecrire_rapport(resultats, path):
    lignes = [
        "# Rapport de qualité des données",
        f"Généré le {datetime.now():%d/%m/%Y à %H:%M}",
        "",
        "| Check | Description | Statut | Violations |",
        "|---|---|---|---|",
    ]
    for r in resultats:
        lignes.append(f"| {r['check']} | {r['description']} | {r['statut']} | {r['violations']} |")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lignes))
    print(f"\nRapport écrit dans {path}")


def main():
    with open(PARAMS_PATH) as f:
        params = json.load(f)

    print("Chargement des données...")
    bronze = charger_delta(BRONZE_PATH)
    referentiel = charger_delta(REFERENTIEL_PATH)
    gold = charger_delta(GOLD_ELIGIBILITE_PATH)

    print("Exécution des tests de qualité...\n")
    tests_bronze(bronze)
    tests_referentiel(referentiel)
    tests_integrite(bronze, referentiel)
    tests_gold(gold, params["jours_bien_etre_accordes"])

    for r in resultats:
        marqueur = "✅" if r["statut"] == "OK" else "❌"
        print(f"{marqueur} {r['check']:30s} ({r['violations']} violation(s)) — {r['description']}")

    nb_echecs = sum(1 for r in resultats if r["statut"] == "ÉCHEC")
    print(f"\n{len(resultats) - nb_echecs}/{len(resultats)} checks passés.")

    ecrire_rapport(resultats, "./tests_qualite/rapport_qualite.md")


if __name__ == "__main__":
    main()