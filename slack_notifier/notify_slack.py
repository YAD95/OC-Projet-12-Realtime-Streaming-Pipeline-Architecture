"""
Écoute en continu le topic Redpanda alimenté par Debezium (CDC sur la table
Postgres `activities`) et poste un message Slack pour chaque NOUVELLE
activité, dans l'esprit des exemples de la note de cadrage.

Ne rejoue pas l'historique (auto_offset_reset="latest") : seules les
activités insérées APRÈS le lancement du script déclenchent un message.
Utile pour tester : lance ce script, puis dans un autre terminal
`python generator/generate_activities.py ... --live`.

Si SLACK_WEBHOOK_URL n'est pas configuré dans le .env, le message est juste
affiché dans le terminal (mode simulation) au lieu d'échouer.

Usage :
    python slack_notifier/notify_slack.py
"""

import json
import os
import random
from datetime import datetime

import requests
from deltalake import DeltaTable
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("ACTIVITIES_TOPIC", "sds.public.activities")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
REFERENTIEL_PATH = os.getenv("REFERENTIEL_PATH", "./data/referentiel/rh")

PHRASES_DISTANCE = [
    "Bravo {nom} ! Tu viens de faire {sport} : {distance} km en {duree} min ! Quelle énergie ! 🔥🏅",
    "Magnifique {nom} ! {distance} km de {sport} dans les pattes aujourd'hui ! 🌄",
    "Superbe séance {nom} ! {distance} km en {duree} min, tu progresses ! 💪",
]
PHRASES_SANS_DISTANCE = [
    "Bravo {nom} ! Une séance de {sport} de {duree} min bien méritée ! 💪",
    "GG {nom} ! {sport} au programme aujourd'hui, {duree} min bien remplies ! 🔥",
]


def charger_annuaire(path):
    """id_salarie -> 'Prénom Nom', à partir du référentiel Delta."""
    try:
        df = DeltaTable(path).to_pandas()
    except Exception as e:
        print(f"⚠️  Impossible de charger le référentiel ({e}). Les noms afficheront juste l'ID salarié.")
        return {}
    return {int(row["ID salarié"]): f"{row['Prénom']} {row['Nom']}" for _, row in df.iterrows()}


def formatter_message(activite, annuaire):
    id_salarie = activite["id_salarie"]
    nom = annuaire.get(id_salarie, f"Salarié #{id_salarie}")
    sport = activite["sport_type"]
    distance_m = activite.get("distance_m")
    debut = datetime.fromtimestamp(activite["date_debut"] / 1_000_000)
    fin = datetime.fromtimestamp(activite["date_fin"] / 1_000_000) if activite.get("date_fin") else None
    duree_min = round((fin - debut).total_seconds() / 60) if fin else "?"

    if distance_m:
        distance_km = round(distance_m / 1000, 1)
        phrase = random.choice(PHRASES_DISTANCE).format(nom=nom, sport=sport, distance=distance_km, duree=duree_min)
    else:
        phrase = random.choice(PHRASES_SANS_DISTANCE).format(nom=nom, sport=sport, duree=duree_min)

    commentaire = activite.get("commentaire")
    if commentaire:
        phrase += f' ("{commentaire}")'
    return phrase


def envoyer_slack(message):
    if not SLACK_WEBHOOK_URL:
        print(f"[SIMULATION - pas de webhook configuré] {message}")
        return
    resp = requests.post(SLACK_WEBHOOK_URL, json={"text": message}, timeout=10)
    if resp.status_code != 200:
        print(f"⚠️  Échec envoi Slack ({resp.status_code}): {resp.text}")

# Plafonds réalistes par discipline (en mètres)
PLAFONDS_METRES = {
    "Course": 42000,
    "Running": 42000,
    "Course à pied": 42000,
    "Marche": 25000,
    "Walking": 25000,
    "Vélo": 150000,
    "Cyclisme": 150000,
    "Bike": 150000,
}
PLAFOND_DEFAUT_METRES = 10000


def detecter_anomalies(activite):
    erreurs = []
    id_salarie = activite.get("id_salarie")
    sport = activite.get("sport_type")
    distance_m = activite.get("distance_m")
    debut_ts = activite.get("date_debut")

    if not id_salarie:
        erreurs.append("identifiant salarié absent")
    if not sport:
        erreurs.append("discipline non renseignée")

    if distance_m is not None:
        if distance_m < 0:
            erreurs.append(f"distance négative ({distance_m} m)")
        else:
            plafond = PLAFONDS_METRES.get(sport, PLAFOND_DEFAUT_METRES)
            if distance_m > plafond:
                erreurs.append(
                    f"distance irréaliste pour {sport} ({distance_m / 1000:.1f} km déclarés, max : {plafond / 1000:.0f} km)"
                )

    if debut_ts and (debut_ts / 1_000_000) > (datetime.now().timestamp() + 300):
        erreurs.append("horodatage situé dans le futur")

    return erreurs

def main():
    print(f"Chargement de l'annuaire salariés depuis {REFERENTIEL_PATH}...")
    annuaire = charger_annuaire(REFERENTIEL_PATH)
    print(f"{len(annuaire)} salariés dans l'annuaire.")

    print(f"Connexion au topic '{TOPIC}' sur {KAFKA_BOOTSTRAP}...")
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
        auto_offset_reset="latest",
        group_id="sds-slack-notifier",
    )

    print("En écoute des nouvelles activités... (Ctrl+C pour arrêter)")
    for message in consumer:
        envelope = message.value
        if not envelope or "payload" not in envelope:
            continue
        payload = envelope["payload"]
        if payload.get("op") == "d":
            continue
        activite = payload.get("after")
        if not activite:
            continue
        erreurs = detecter_anomalies(activite)

        if erreurs:
            id_sal = activite.get("id_salarie", "inconnu")
            nom = annuaire.get(id_sal, f"Salarié #{id_sal}")
            alerte = (
                f"🚨 *[ALERTE MONITORING - QUARANTAINE]*\n"
                f"Une activité anormale a été bloquée à l'ingestion.\n"
                f"• *Collaborateur* : {nom}\n"
                f"• *Anomalie(s)* : {', '.join(erreurs)}\n"
                f"• *Statut* : Déroutée vers la table Quarantaine (exclue des calculs RH)."
            )
            envoyer_slack(alerte)
            print(f"[ALERTE] {alerte}")
        else:
            texte = formatter_message(activite, annuaire)
            envoyer_slack(texte)
            print(texte)


if __name__ == "__main__":
    main()
