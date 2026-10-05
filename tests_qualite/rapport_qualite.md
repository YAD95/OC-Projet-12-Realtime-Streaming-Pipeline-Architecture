# Rapport de qualité des données
Généré le 05/10/2026 à 15:30

| Check | Description | Statut | Violations |
|---|---|---|---|
| distance_non_negative | La distance parcourue ne doit jamais être négative | OK | 0 |
| date_debut_renseignee | La date de début d'activité doit toujours être renseignée | OK | 0 |
| date_debut_pas_future | La date de début ne doit pas être dans le futur | OK | 0 |
| date_fin_apres_debut | La date de fin doit être postérieure (ou égale) à la date de début | OK | 0 |
| sport_type_connu | Le type de sport doit faire partie de la liste des sports gérés | OK | 0 |
| id_salarie_unique | Chaque ID salarié doit apparaître une seule fois dans le référentiel | OK | 0 |
| salaire_positif | Le salaire brut doit être strictement positif | OK | 0 |
| distance_coherente | Si une distance a été calculée, elle doit être positive | OK | 0 |
| id_salarie_existe_dans_rh | Chaque activité doit référencer un salarié existant dans le référentiel RH | OK | 0 |
| montant_prime_positif_ou_nul | Le montant de prime calculé ne doit jamais être négatif | OK | 0 |
| jours_bien_etre_valides | Les jours bien-être accordés doivent être 0 ou 5 (valeur paramétrée) | OK | 0 |