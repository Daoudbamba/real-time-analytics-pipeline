# Procédure de démonstration

Démonstration d'environ 30 minutes, du démarrage à la panne simulée. Les commandes se
lancent depuis la racine du dépôt, dans bash (Git Bash sous Windows). Les durées
indiquées ont été mesurées sur le poste de développement (Windows 11, Docker Desktop,
VM de 7,8 Go).

Retour au [README](../README.md).

## 0. Avant la démonstration

Docker Desktop doit être démarré, avec au moins 7 Go de mémoire pour sa VM. Fermez les
autres conteneurs gourmands.

Créer `.env` une seule fois (les secrets sont générés et jamais affichés) :

```bash
bash scripts/init_env.sh
```

Pour partir d'un état vierge (supprime les données des démonstrations précédentes) :

```bash
bash scripts/stop_pipeline.sh --volumes --yes
```

## 1. Démarrer la pile (environ 3 à 5 minutes)

```bash
bash scripts/start_pipeline.sh
```

Le script attend que tous les services soient sains, puis affiche les URL (mesuré :
200 s avec les images en cache). À dire
pendant l'attente : un seul fichier Compose démarre Kafka, le cluster Spark,
l'application de streaming, Airflow et sa base, Prometheus, Grafana et deux exporters.

Au démarrage, Airflow exécute une fois le dernier intervalle de chacun des 3 DAG. Ces
exécutions de rattrapage occupent le pool Spark, ce qui est normal : une vingtaine de
minutes sans flux, et près d'une heure (55 à 60 min mesurées) avec le producteur à
50 événements par seconde.

## 2. Les topics Kafka

```bash
bash scripts/create_topics.sh
```

La commande est idempotente : elle recrée seulement ce qui manque, puis liste les
4 topics avec leurs partitions.

## 3. Envoyer des événements en continu

```bash
bash scripts/generate_data.sh --continuous --rate 50
```

Un message brut, tel que le producteur l'envoie :

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ecommerce-events --from-beginning --max-messages 1
```

Un événement rejeté, avec ses motifs (topic `invalid-events`, alimenté par Spark après
le premier micro-batch, soit environ 30 s) :

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic invalid-events --from-beginning --max-messages 1
```

À montrer : le champ `rejection_reasons` et la valeur brute conservée.

## 4. Les tables Delta

Compter les lignes de chaque couche et afficher les dernières fenêtres Gold. La commande
démarre une session Spark locale dans l'image du streaming. Elle a pris 3 minutes
(180 à 203 s mesurés) pendant que les exécutions de rattrapage d'Airflow tournaient :

```bash
docker compose run --rm --no-deps -T -e PYSPARK_SUBMIT_ARGS="--master local[1] --driver-memory 768m --conf spark.ui.showConsoleProgress=false pyspark-shell" streaming python -c "
from jobs.session import build_spark
from rtap.config import load_config
config = load_config()
spark = build_spark('demo', config.storage)
spark.sparkContext.setLogLevel('ERROR')
def read(layer, table):
    return spark.read.format('delta').load(config.storage.table_path(layer, table))
for layer in ('bronze', 'silver', 'quarantine'):
    print(layer, read(layer, 'events').count())
read('quarantine', 'events').groupBy('rejection_reasons').count().orderBy('count', ascending=False).show(5, False)
read('gold', 'sales_per_minute').orderBy('window_start', ascending=False).show(5, False)
spark.stop()
" 2>/dev/null
```

À montrer : Bronze ≈ Silver + quarantaine + doublons supprimés. Sur un flux vivant,
des micro-batches sont en cours et l'égalité n'est qu'approchée : l'égalité exacte est
établie par la vérification de l'étape 8. Les fenêtres Gold sont en UTC.

Une fenêtre d'une minute se ferme quand Gold a lu des événements postérieurs à sa fin
de plus de 2 minutes (le watermark). En théorie, la première fenêtre sort donc environ
3 minutes après le début du flux, mais Gold a du retard quand la machine est chargée.
Mesuré lors de la répétition : aucune fenêtre au bout de 5 min 30, plusieurs fenêtres au
bout de 13 minutes. Si Gold est vide, passez aux étapes 5 et 6 et relancez la commande
ensuite.

Capture à prendre : aucune, la sortie terminal suffit.

## 5. Le dashboard Grafana

Ouvrir <http://localhost:3000> (ou le port `GRAFANA_HOST_PORT` du `.env`). Identifiant
`GRAFANA_ADMIN_USER`, mot de passe `GRAFANA_ADMIN_PASSWORD` du `.env`. La page d'accueil
est le dashboard « Real-Time Analytics Pipeline ».

À montrer, rangée par rangée :

- **Overview** : événements entrés dans Bronze, événements valides et rejetés, taux de
  rejet, transactions, chiffre d'affaires, requêtes et workers actifs, alertes.
- **Throughput** : débit d'ingestion (environ 50 événements par seconde) et messages
  par topic Kafka.
- **Latency** : latence Kafka → Silver, durée des micro-batches, latence d'accusé de
  réception du producteur.
- **Data quality** : rejets par motif, événements valides par type, doublons et retards
  écartés.
- **Business** : transactions et chiffre d'affaires par minute.
- **Services** et **Error history** : état des cibles, composants Airflow, cœurs Spark,
  erreurs et alertes.

Captures : `docs/images/grafana-overview.png` et `docs/images/grafana-quality.png`.

Spark UI de l'application de streaming : <http://localhost:4040>, onglet
« Structured Streaming », avec les 5 requêtes actives. Capture :
`docs/images/spark-streaming.png`.

## 6. Airflow

Ouvrir <http://localhost:8080> (sans connexion, usage local). Les 3 DAG sont actifs.

Déclencher les contrôles qualité :

```bash
docker compose exec -T airflow-scheduler airflow dags trigger lakehouse_quality
```

Si les exécutions de rattrapage du démarrage tournent encore, ce run attend son tour
dans le pool `spark_jobs`. Chaque contrôle dure 2 à 3 minutes. Mesuré sous flux : 15 minutes
de bout en bout, car les contrôles partagent le pool avec `gold_batch_aggregates`. Dans l'UI, ouvrir
la vue graphe : `check_bronze` → `check_silver` → `check_gold`.

Captures : `docs/images/airflow-dags.png` et `docs/images/airflow-quality-run.png`.

## 7. Simuler une panne (environ 3 minutes)

Arrêter le worker Spark :

```bash
docker compose stop spark-worker
```

Environ 1 minute plus tard (78 s mesurées), `SparkNoAliveWorker` et `ServiceDown`
passent à l'état « firing » :

```bash
bash scripts/status.sh
```

Les alertes sont aussi visibles sur <http://localhost:9090/alerts> (capture :
`docs/images/prometheus-alerts.png`) et dans la rangée « Error history » du
dashboard.

Redémarrer le worker :

```bash
docker compose start spark-worker
```

Le worker se réenregistre auprès du master, l'application de streaming récupère ses
exécuteurs et reprend là où elle s'était arrêtée, sans redémarrer. Mesuré : alertes
résolues 34 s après le redémarrage, 5 requêtes actives, aucun redémarrage du conteneur
`streaming`.

## 8. Vérification de bout en bout (facultative, environ 50 minutes)

Elle exige une pile vierge : Kafka doit être vide.

```bash
bash scripts/generate_data.sh --stop
```
```bash
bash scripts/stop_pipeline.sh --volumes --yes
```
```bash
bash scripts/start_pipeline.sh
```
```bash
bash scripts/verify_pipeline.sh
```

Chaque contrôle s'affiche en `PASS` ou `FAIL` avec la valeur observée et la valeur
attendue. Capture : `docs/images/verify-output.png`.

## 9. Terminer

```bash
bash scripts/generate_data.sh --stop
```

Arrêter en gardant les données :

```bash
bash scripts/stop_pipeline.sh
```

Ou tout supprimer :

```bash
bash scripts/stop_pipeline.sh --volumes --yes
```
