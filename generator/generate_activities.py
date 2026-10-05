"""
Générateur de données Strava-like pour le POC Sport Data Solution.

Simule un historique d'activités sportives sur N mois (12 par défaut) pour
les salariés listés dans le fichier RH, et écrit directement dans la table
Postgres `activities` (celle captée par Debezium -> Redpanda).

Format des lignes (aligné sur la note de cadrage) :
    id_salarie, date_debut, sport_type, distance_m, date_fin, commentaire

Usage :
    python generate_activities.py --rh-file "Données RH.xlsx" --sport-file "Données Sportive.xlsx"
    python generate_activities.py --rh-file "Données RH.xlsx" --live   # + une activité "maintenant" pour tester Slack
"""

import argparse
import os
import random
from datetime import datetime, timedelta

import pandas as pd
import psycopg2

# Sports avec une distance pertinente vs sports où la note dit de laisser vide
SPORTS_AVEC_DISTANCE = {
    "Course à pied": (3000, 18000, 4, 7),        # distance min/max (m), min/max (min/km)
    "Randonnée": (5000, 20000, 8, 15),
    "Vélo": (10000, 60000, 2, 4),
    "Natation": (500, 3000, 2, 3),
    "Triathlon": (15000, 40000, 3, 6),
}
SPORTS_SANS_DISTANCE = [
    "Tennis", "Football", "Rugby", "Badminton", "Voile",
    "Judo", "Boxe", "Escalade", "Équitation", "Tennis de table", "Basketball",
]
DUREE_SANS_DISTANCE_MIN = (30, 120)  # minutes

COMMENTAIRES_POSSIBLES = [
    None, None, None, None, None,  # la plupart des lignes n'ont pas de commentaire
    "Reprise du sport :)",
    "Belle séance, en forme aujourd'hui !",
    "Nouveau record personnel",
]


def charger_salaries(rh_file, sport_file=None):
    """Renvoie une liste de dicts {id_salarie, profil} où profil influe sur
    la fréquence d'activité simulée (basé sur la pratique déclarée)."""
    rh = pd.read_excel(rh_file)
    ids = rh["ID salarié"].tolist()

    pratique_par_id = {}
    if sport_file and os.path.exists(sport_file):
        sport = pd.read_excel(sport_file)
        for _, row in sport.iterrows():
            pratique_par_id[row["ID salarié"]] = row["Pratique d'un sport"]

    salaries = []
    for id_salarie in ids:
        a_declare_un_sport = pd.notna(pratique_par_id.get(id_salarie))
        # Un salarié ayant déclaré une pratique sportive a plus de chances
        # d'être un profil actif (utile pour tester le seuil des 15 activités/an)
        profil = "actif" if a_declare_un_sport else random.choices(
            ["actif", "occasionnel", "inactif"], weights=[0.25, 0.35, 0.4]
        )[0]
        salaries.append({"id_salarie": int(id_salarie), "profil": profil})
    return salaries


def nb_activites_par_mois(profil):
    if profil == "actif":
        return random.randint(2, 8)
    if profil == "occasionnel":
        return random.randint(0, 2)
    return random.choices([0, 1], weights=[0.85, 0.15])[0]


def generer_une_activite(id_salarie, date_debut):
    if random.random() < 0.55:
        sport_type = random.choice(list(SPORTS_AVEC_DISTANCE.keys()))
        dist_min, dist_max, pace_min, pace_max = SPORTS_AVEC_DISTANCE[sport_type]
        distance_m = random.randint(dist_min, dist_max)
        pace = random.uniform(pace_min, pace_max)  # min par km
        duree_min = (distance_m / 1000) * pace
    else:
        sport_type = random.choice(SPORTS_SANS_DISTANCE)
        distance_m = None
        duree_min = random.randint(*DUREE_SANS_DISTANCE_MIN)

    date_fin = date_debut + timedelta(minutes=duree_min)
    commentaire = random.choice(COMMENTAIRES_POSSIBLES)

    return (id_salarie, date_debut, sport_type, distance_m, date_fin, commentaire)


def generer_historique(salaries, mois=12):
    activites = []
    aujourdhui = datetime.now()
    debut_periode = aujourdhui - timedelta(days=30 * mois)

    for salarie in salaries:
        nb_mois_actifs = mois
        for m in range(nb_mois_actifs):
            n = nb_activites_par_mois(salarie["profil"])
            for _ in range(n):
                jour_aleatoire = debut_periode + timedelta(
                    days=30 * m + random.randint(0, 29),
                    hours=random.randint(6, 20),
                    minutes=random.randint(0, 59),
                )
                if jour_aleatoire > aujourdhui:
                    continue
                activites.append(generer_une_activite(salarie["id_salarie"], jour_aleatoire))
    return activites


def inserer_en_base(activites, conn_params):
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()
    cur.executemany(
        """
        INSERT INTO activities (id_salarie, date_debut, sport_type, distance_m, date_fin, commentaire)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        activites,
    )
    conn.commit()
    cur.close()
    conn.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rh-file", required=True, help="Chemin vers Données RH.xlsx")
    parser.add_argument("--sport-file", default=None, help="Chemin vers Données Sportive.xlsx (optionnel)")
    parser.add_argument("--months", type=int, default=12, help="Nombre de mois d'historique à générer")
    parser.add_argument("--live", action="store_true", help="Ajoute une activité 'maintenant' pour tester le flux Slack")
    parser.add_argument("--db-host", default=os.getenv("DB_HOST", "localhost"))
    parser.add_argument("--db-port", default=os.getenv("DB_PORT", "5435"))
    parser.add_argument("--db-user", default=os.getenv("POSTGRES_USER", "sds_user"))
    parser.add_argument("--db-password", default=os.getenv("POSTGRES_PASSWORD", "sds_password"))
    parser.add_argument("--db-name", default=os.getenv("POSTGRES_DB", "sports_data"))
    args = parser.parse_args()

    conn_params = {
        "host": args.db_host,
        "port": args.db_port,
        "user": args.db_user,
        "password": args.db_password,
        "dbname": args.db_name,
    }

    print("Chargement des salariés depuis le fichier RH...")
    salaries = charger_salaries(args.rh_file, args.sport_file)
    print(f"{len(salaries)} salariés chargés.")

    print(f"Génération de {args.months} mois d'historique...")
    activites = generer_historique(salaries, mois=args.months)
    print(f"{len(activites)} activités générées.")

    if args.live:
        salarie_demo = random.choice(salaries)
        activites.append(generer_une_activite(salarie_demo["id_salarie"], datetime.now()))
        print("+1 activité 'live' ajoutée pour tester la notification Slack.")

    print("Insertion en base Postgres...")
    inserer_en_base(activites, conn_params)
    print("Terminé.")


if __name__ == "__main__":
    main()
