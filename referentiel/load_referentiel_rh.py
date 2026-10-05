"""
Script one-shot : charge le référentiel RH (+ pratique sportive optionnelle),
calcule la distance domicile -> entreprise via Google Maps pour les salariés
concernés par la prime sportive, et écrit le résultat dans une table Delta
Lake "référentiel" (relue ensuite par le job d'enrichissement/éligibilité).

À relancer à chaque fois que le fichier RH change (nouvel embauché,
déménagement, changement de mode de déplacement déclaré...) : la table est
intégralement réécrite (overwrite).

Fonctionne SANS clé Google Maps : la distance est alors marquée comme
"à calculer" plutôt que bloquer le script.

Usage :
    python load_referentiel_rh.py --rh-file "Données RH.xlsx" --sport-file "Données Sportive.xlsx"
"""

import argparse
import os
import time

import pandas as pd
import requests
from deltalake import write_deltalake
from dotenv import load_dotenv

load_dotenv()

COMPANY_ADDRESS_DEFAULT = "1362 Av. des Platanes, 34970 Lattes"

# Seuils et mode Google Maps correspondant, d'après la note de cadrage.
# Les autres moyens de déplacement (véhicule thermique/électrique, transports
# en commun) ne sont de toute façon pas éligibles à la prime : pas besoin de
# calculer leur distance.
SEUILS_KM = {
    "Marche/running": 15,
    "Vélo/Trottinette/Autres": 25,
}
MODE_GOOGLE = {
    "Marche/running": "walking",
    "Vélo/Trottinette/Autres": "bicycling",
}


def distance_google_maps(origin, destination, mode, api_key):
    url = "https://maps.googleapis.com/maps/api/distancematrix/json"
    params = {"origins": origin, "destinations": destination, "mode": mode, "key": api_key}
    resp = requests.get(url, params=params, timeout=10)
    data = resp.json()
    try:
        element = data["rows"][0]["elements"][0]
        if element["status"] != "OK":
            return None
        return element["distance"]["value"] / 1000  # mètres -> km
    except (KeyError, IndexError):
        return None


def calculer_distances(df, company_address, api_key):
    distances, anomalies = [], []
    for _, row in df.iterrows():
        mode = row["Moyen de déplacement"]
        if mode not in SEUILS_KM:
            distances.append(None)
            anomalies.append(False)
            continue
        if not api_key:
            distances.append(None)
            anomalies.append(None)  # None = "pas encore calculé", pas une anomalie
            continue
        dist_km = distance_google_maps(
            row["Adresse du domicile"], company_address, MODE_GOOGLE[mode], api_key
        )
        distances.append(dist_km)
        anomalies.append(dist_km is not None and dist_km > SEUILS_KM[mode])
        time.sleep(0.1)  # petite pause pour rester poli avec l'API
    df["distance_km"] = pd.Series(distances, dtype="float64")
    df["distance_anormale"] = pd.Series(anomalies, dtype="boolean")
    return df


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rh-file", required=True, help="Chemin vers Données RH.xlsx")
    parser.add_argument("--sport-file", default=None, help="Chemin vers Données Sportive.xlsx (optionnel)")
    parser.add_argument("--company-address", default=COMPANY_ADDRESS_DEFAULT)
    parser.add_argument("--api-key", default=os.getenv("GOOGLE_MAPS_API_KEY"))
    parser.add_argument("--output-path", default="./data/referentiel/rh")
    args = parser.parse_args()

    print("Lecture du fichier RH...")
    rh = pd.read_excel(args.rh_file)

    if args.sport_file and os.path.exists(args.sport_file):
        sport = pd.read_excel(args.sport_file)
        rh = rh.merge(sport, on="ID salarié", how="left")

    if not args.api_key:
        print(
            "⚠️  Pas de clé Google Maps trouvée (variable GOOGLE_MAPS_API_KEY absente du .env) : "
            "les distances seront marquées 'à calculer' plutôt que calculées."
        )
    else:
        print(f"Calcul des distances domicile -> {args.company_address} via Google Maps...")

    rh = calculer_distances(rh, args.company_address, args.api_key)

    print(f"Écriture du référentiel enrichi dans {args.output_path} ...")
    write_deltalake(args.output_path, rh, mode="overwrite")

    nb_anomalies = int(pd.Series(rh["distance_anormale"]).fillna(False).sum())
    print(f"Terminé : {len(rh)} salariés traités, {nb_anomalies} déclaration(s) incohérente(s) détectée(s).")


if __name__ == "__main__":
    main()