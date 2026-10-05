-- Table source alimentée par le simulateur Strava-like (étape 4).
-- Format aligné sur la note de cadrage : ID ; ID salarié ; date début ;
-- type ; distance (m, vide si non pertinent) ; date fin ; commentaire.
CREATE TABLE IF NOT EXISTS activities (
    id SERIAL PRIMARY KEY,
    id_salarie INTEGER NOT NULL,
    date_debut TIMESTAMP NOT NULL,
    sport_type VARCHAR(50) NOT NULL,
    distance_m INTEGER,
    date_fin TIMESTAMP,
    commentaire TEXT
);

-- REPLICA IDENTITY FULL : nécessaire pour que Debezium capture l'intégralité
-- de la ligne (utile en cas d'UPDATE/DELETE, pas seulement les INSERT).
ALTER TABLE activities REPLICA IDENTITY FULL;
