# ============================================
# 0. Imports
# ============================================
from datetime import datetime, timezone, timedelta
import yaml, sys, pytz, os, requests, json, time
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType
import traceback

# ============================================
# Variables globales monitoring
# ============================================
monitoring_table = "catalog_badsdataeng_dev.data_squad_monitoring.monitoring_all"
dt_debt_traitement = datetime.now(pytz.timezone("Europe/Paris")).strftime("%Y-%m-%d %H:%M:%S.%f")

# ============================================
# Fonction de monitoring
# ============================================
def integration_monitoring_parquet(monitoring_table, table_target, statut, dt_debt_traitement, nb_lignes=None):
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

        def safe_get(attr):
            try:
                m = getattr(ctx, attr, lambda: None)()
                return str(m.get()) if m and hasattr(m, "isDefined") and m.isDefined() else "UNKNOWN"
            except:
                return "UNKNOWN"

        run_id, job_id = safe_get("currentRunId"), safe_get("jobId")
        job_name, task_name = safe_get("jobName"), safe_get("taskKey")
        task_run_id = safe_get("taskRunId")
        notebook_name = ctx.notebookPath().get().split("/")[-1]
    except:
        run_id = job_id = job_name = task_name = task_run_id = "UNKNOWN"
        notebook_name = "UNKNOWN_NOTEBOOK"

    print(f"4- {datetime.now().strftime('%y/%m/%d %H:%M:%S')} INFO MONITORING Intégration dans {monitoring_table}")
    try:
        spark.sql(f"refresh table {monitoring_table}")
    except:
        pass

    # Schéma explicite : sur certains clusters (Spark Connect), l'inférence
    # échoue quand nb_lignes vaut None (statut KO/IS) car elle ne peut pas
    # déterminer un type à partir d'une seule valeur nulle.
    monitoring_schema = StructType([StructField("nombre_ligne", LongType(), True)])
    base_df = spark.createDataFrame([(nb_lignes,)], schema=monitoring_schema)
    dataframe = (
        base_df
        .withColumn("nom_script", F.lit(notebook_name))
        .withColumn("nombre_ligne",
            F.when((F.lit(statut).isin("KO", "IS")), F.lit(None))
             .otherwise(F.col("nombre_ligne")))
        .withColumn("nom_table", F.lit(table_target))
        .withColumn("date_maj", F.lit(datetime.now(pytz.timezone("Europe/Paris")).strftime("%Y-%m-%d %H:%M:%S.%f")))
        .withColumn("statut", F.lit(statut))
        .withColumn("erreur",
            F.when(F.lit(statut) == "OK", F.lit(None))
             .when(F.lit(statut) == "IS", F.lit("indisponibilité donnée(s) source(s)"))
             .otherwise(F.lit(str(sys.exc_info()[1]))))
        .withColumn("dt_jour", F.date_format(F.current_date(), "yyyyMMdd"))
        .withColumn("dt_debt_traitement", F.lit(dt_debt_traitement))
        .withColumn("dt_fin_traitement", F.lit(datetime.now(pytz.timezone("Europe/Paris")).strftime("%Y-%m-%d %H:%M:%S.%f")))
        .withColumn("run_id", F.lit(run_id))
        .withColumn("job_id", F.lit(job_id))
        .withColumn("job_name", F.lit(job_name))
        .withColumn("task_name", F.lit(task_name))
        .withColumn("task_run_id", F.lit(task_run_id))
        .select(spark.table(monitoring_table).columns)
        .repartition(1)
    )

    for col_name, col_type in spark.table(monitoring_table).dtypes:
        dataframe = dataframe.withColumn(col_name, F.col(col_name).cast(col_type))

    dataframe.write.mode("append").format("delta").insertInto(monitoring_table)
    print(f"5- {datetime.now().strftime('%y/%m/%d %H:%M:%S')} INFO Table {monitoring_table} MAJ")

# ============================================
# 1. Lecture du paramètre --env passé par le job Databricks
# ============================================
print("Commande Python complète :", " ".join(sys.argv))
env_cli = sys.argv[sys.argv.index("--env") + 1].upper() if "--env" in sys.argv else None
if env_cli:
    env = env_cli
else:
    try:
        env = dbutils.widgets.get("env").upper()
    except:
        dbutils.widgets.dropdown("env", "DEV", ["DEV", "PROD"], "Environnement")
        env = dbutils.widgets.get("env").upper()
print(f"Environnement utilisé : {env}")

# ============================================
# 2. Timestamp
# ============================================
timestamp = (datetime.now(timezone.utc) + timedelta(hours=1)).strftime("%Y%m%d_%H%M%S")

# ============================================
# 3. Authentification API (OAuth2 client credentials)
# ============================================
# TENANT_ID et CLIENT_ID ne sont pas sensibles (identifiants publics OAuth) et
# restent en dur. CLIENT_SECRET et API_KEY sont stockés dans le secret scope
# Databricks dédié "wolf-api-scope" (voir commandes de création plus bas).
TENANT_ID     = "329e91b0-e21f-48fb-a071-456717ecc28e"
CLIENT_ID     = "25187cfb-b224-44b6-8310-565bc4bf2c1e"
CLIENT_SECRET = dbutils.secrets.get(scope="wolf-api-scope", key="wolf-api-client-secret")
API_KEY       = dbutils.secrets.get(scope="wolf-api-scope", key="wolf-api-key")
SCOPE = "api://25187cfb-b224-44b6-8310-565bc4bf2c1e/.default"

HOST = "https://prod.apix.iasp.tgscloud.net"  # même host pour DEV et PROD
API_BASE = f"{HOST}/rcdatahub/v0.0.1/v1/api/tables"
PAGE_SIZE = 1000
# Sur les grosses tables (ex: product_details, ~4.8M lignes -> ~4831 pages),
# logguer chaque page ralentit le rendu du notebook. On ne logue qu'une page
# sur LOG_EVERY, plus systématiquement la première et la dernière.
LOG_EVERY = 20

# Plafond réel de $top par table, vérifié empiriquement (voir
# test_page_size_ceiling.py). Ne pas généraliser à 5000 partout : le
# commentaire historique du script JDBC signalait déjà une "saturation" à
# 5000 sur vw_wagon_ticket - le plafond semble varier par table. Ajouter une
# entrée ici (ou un champ "page_size" dans le YAML) uniquement après l'avoir
# vérifié comme on l'a fait pour product_details.
PAGE_SIZE_OVERRIDES = {
    "product_details": 5000,
}


def get_access_token():
    resp = requests.post(
        f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token",
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": SCOPE,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def build_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "x-apif-apikey": API_KEY,
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0",
    }


# ============================================
# 3.b Test connexion API (fail fast)
# ============================================
try:
    access_token = get_access_token()
    # Session HTTP réutilisée pour tous les appels : évite de rouvrir une
    # connexion (handshake TCP/TLS) à chaque page paginée, ce qui compte
    # sur les tables à des milliers de pages.
    session = requests.Session()
    session.headers.update(build_headers(access_token))
    print("Connexion API OK (token obtenu)")

except Exception as e:
    print("Connexion API KO")
    traceback.print_exc()

    integration_monitoring_parquet(
        monitoring_table,
        "API_CONNECTION",
        "KO",
        dt_debt_traitement,
        None
    )

    raise Exception("Arrêt du job : authentification API impossible") from e

# ============================================
# 4. Chargement YAML
# ============================================
config_path = "../Config/Config_WOLF.yml"
try:
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
except FileNotFoundError:
    dbutils.notebook.exit(f"FAILED - ERREUR: Fichier de configuration '{config_path}' non trouvé.")
except Exception as e:
    dbutils.notebook.exit(f"FAILED - ERREUR lors de la lecture du fichier de configuration: {e}")

base_path = config["environments"][env]["adls_base_path"]
tables_config = config["wolf"]
print(f"ADLS base path : {base_path}")


# ============================================
# 5. Ingestion WOLF via API
# ============================================

def _get_with_retry(session, url, params, refreshed_flag):
    """
    GET avec (a) rafraîchissement du token sur 401 et (b) retry avec backoff
    sur les erreurs serveur transitoires (502/503/504 - "Unexpected EOF at
    target" côté passerelle API). refreshed_flag est une liste [bool] pour
    rester mutable entre appels (un seul refresh de token par table).
    """
    max_retries = 4
    r = None
    for attempt in range(max_retries + 1):
        r = session.get(url, params=params, timeout=60)

        if r.status_code == 401 and not refreshed_flag[0]:
            print("  Token expiré, rafraîchissement...")
            session.headers.update({"Authorization": f"Bearer {get_access_token()}"})
            refreshed_flag[0] = True
            r = session.get(url, params=params, timeout=60)

        if r.status_code in (502, 503, 504) and attempt < max_retries:
            wait = 2 ** attempt  # 1s, 2s, 4s, 8s
            print(f"  Erreur HTTP {r.status_code}, nouvelle tentative dans "
                  f"{wait}s ({attempt + 1}/{max_retries})...")
            time.sleep(wait)
            continue

        return r
    return r


def fetch_table_rows(table_name, session, page_size=None):
    """
    Récupère toutes les lignes d'une table via l'API, en pagination $skip/$top
    classique avec retry sur erreurs transitoires.

    NB : la pagination par curseur ($filter=id gt ...) a été testée et
    abandonnée - l'API renvoie une erreur PostgreSQL ("column param0 does not
    exist", HTTP 500) qui indique un bug de binding de paramètre côté backend,
    pas un problème de syntaxe côté client. À signaler à l'équipe API.

    page_size est configurable par table (voir PAGE_SIZE_OVERRIDES et le champ
    optionnel "page_size" dans le YAML) car le plafond réel de $top varie
    selon les tables : product_details plafonne proprement à 5000 (vérifié
    empiriquement), alors que d'autres tables ont montré des erreurs dès 5000
    (vw_wagon_ticket). Utiliser un page_size supérieur au plafond réel d'une
    table tronque silencieusement chaque page, ce qui reste sûr tant que le
    plafond est atteint de façon *constante* (toute page pleine renvoie
    exactement ce plafond, donc "n < page_size" reste un signal de fin
    valide) - mais ça suppose que le plafond a été vérifié au préalable.
    """
    page_size = page_size or PAGE_SIZE
    url = f"{API_BASE}/{table_name}/data"
    all_rows = []
    page_num = 0
    fetch_start = datetime.now()
    refreshed_flag = [False]

    def log_progress(n):
        is_last = n == 0 or n < page_size
        if page_num == 1 or is_last or page_num % LOG_EVERY == 0:
            elapsed = (datetime.now() - fetch_start).total_seconds()
            print(f"  page {page_num} -> {n} lignes reçues "
                  f"[{len(all_rows) + n} cumulées, {elapsed:.0f}s écoulées]")

    skip = 0
    while True:
        params = {"$top": page_size, "$skip": skip}
        r = _get_with_retry(session, url, params, refreshed_flag)

        if r.status_code != 200:
            raise Exception(f"Erreur HTTP {r.status_code} à skip={skip} : {r.text}")

        page = r.json()
        n = len(page)
        page_num += 1
        log_progress(n)

        if n == 0:
            break

        all_rows.extend(page)

        if n < page_size:
            break

        skip += page_size

    return all_rows


success_count = 0
error_count = 0
failed_tables = []
global_start = datetime.now()
print("\n" + "=" * 80)
print("Début de l'ingestion WOLF")

try:

    total_tables = len(tables_config)
    for table in tables_config:
        table_start = datetime.now()
        source_table = table.get("source_table", "UNKNOWN")
        try:

            print("\n" + "=" * 80)
            print(f"1- Traitement table : {source_table}")

            table_page_size = table.get("page_size", PAGE_SIZE_OVERRIDES.get(source_table))
            rows = fetch_table_rows(source_table, session, table_page_size)
            nb_lignes_ecrites = len(rows)
            print(f"2- ✔️ {nb_lignes_ecrites} lignes lues")

            if nb_lignes_ecrites == 0:
                print("Table vide côté API : aucun fichier écrit")
                success_count += 1
                print(f"SUCCÈS en {(datetime.now() - table_start).total_seconds():.1f}s")
                integration_monitoring_parquet(
                    monitoring_table,
                    source_table,
                    "OK",
                    dt_debt_traitement,
                    0
                )
                continue

            # Écriture brute des lignes en JSON lines puis relecture via
            # spark.read.json : le lecteur JSON de Spark fait lui-même la
            # promotion de types (Long -> Double, etc.) que
            # spark.createDataFrame() sur des objets Python ne fait pas de
            # façon fiable sous Spark Connect. Pas de pandas, pas de RDD.
            # Écriture en plusieurs fichiers (au lieu d'un seul dbutils.fs.put
            # géant) : Spark Connect limite la taille d'un message/plan à
            # 512 Mo (CONNECT_INVALID_PLAN.PLAN_SIZE_LARGER_THAN_MAX),
            # rencontré sur product_details (~370 Mo de JSON en un seul
            # appel). spark.read.json() lit un dossier entier contenant
            # plusieurs fichiers JSON lines et les fusionne en un seul
            # DataFrame, donc le résultat final ne change pas.
            raw_dir = f"{base_path}{source_table}/IN/_raw_{timestamp}"
            CHUNK_SIZE_BYTES = 200 * 1024 * 1024  # 200 Mo, marge sous la limite de 512 Mo

            chunk_lines = []
            chunk_bytes = 0
            chunk_index = 0

            for row in rows:
                line = json.dumps(row, default=str, ensure_ascii=False)
                chunk_lines.append(line)
                chunk_bytes += len(line.encode("utf-8")) + 1  # +1 pour le \n

                if chunk_bytes >= CHUNK_SIZE_BYTES:
                    chunk_path = f"{raw_dir}/part_{chunk_index:05d}.json"
                    dbutils.fs.put(chunk_path, "\n".join(chunk_lines), overwrite=True)
                    chunk_index += 1
                    chunk_lines = []
                    chunk_bytes = 0

            if chunk_lines:
                chunk_path = f"{raw_dir}/part_{chunk_index:05d}.json"
                dbutils.fs.put(chunk_path, "\n".join(chunk_lines), overwrite=True)
                chunk_index += 1

            print(f"  {chunk_index} fichier(s) intermédiaire(s) écrit(s) dans {raw_dir}")
            df = spark.read.json(raw_dir)

            tmp_dir = f"{base_path}{source_table}/IN/_tmp_{timestamp}"
            df.coalesce(1).write.mode("overwrite").json(tmp_dir)
            json_files = [
                f for f in dbutils.fs.ls(tmp_dir)
                if f.name.endswith(".json")
            ]
            if not json_files:
                raise Exception("Aucun fichier JSON généré")
            final_name = (
                f"wolf_wolfdb_{source_table}"
                f"_DBC_FULL_{timestamp}.json"
            )
            final_path = (
                f"{base_path}{source_table}/IN/{final_name}"
            )
            dbutils.fs.cp(
                json_files[0].path,
                final_path
            )
            dbutils.fs.rm(
                tmp_dir,
                recurse=True
            )
            dbutils.fs.rm(
                raw_dir,
                recurse=True
            )
            print(f"3- Fichier final écrit : {final_path}")
            success_count += 1
            print(
                f"SUCCÈS en "
                f"{(datetime.now() - table_start).total_seconds():.1f}s"
            )
            integration_monitoring_parquet(
                monitoring_table,
                source_table,
                "OK",
                dt_debt_traitement,
                nb_lignes_ecrites
            )
        except KeyError as e:
            error_count += 1
            failed_tables.append(source_table)
            print(
                f"ERREUR DE CONFIGURATION : "
                f"clé {e} manquante pour {source_table}"
            )
            traceback.print_exc()
            integration_monitoring_parquet(
                monitoring_table,
                source_table,
                "KO",
                dt_debt_traitement,
                None
            )
            continue
        except Exception as e:
            error_count += 1
            failed_tables.append(source_table)
            print(
                f"ERREUR lors du traitement de "
                f"{source_table} : {e}"
            )
            traceback.print_exc()
            print(
                f"ÉCHEC en "
                f"{(datetime.now() - table_start).total_seconds():.1f}s"
            )
            integration_monitoring_parquet(
                monitoring_table,
                source_table,
                "KO",
                dt_debt_traitement,
                None
            )
            continue

except Exception as global_e:
    print("\n" + "=" * 80)
    print(f"ERREUR CRITIQUE GLOBALE durant l'ingestion : {global_e}")
    traceback.print_exc()

# ============================================
# 6. Fin de job
# ============================================

print("\n" + "=" * 80)
print("******** RÉSUMÉ DE L'INGESTION ********")
print(f"Tables réussies : {success_count}/{total_tables}")
print(f"Tables en erreur : {error_count}/{total_tables}")
print(
    f"Durée totale : "
    f"{(datetime.now() - global_start).total_seconds():.1f}s"
)
if failed_tables:
    print("Tables en échec en echeec 2eme commit pull request changement :")
    for t in failed_tables:
        print(f"  - {t}")
if error_count > 0:
    print(f"Ingestion terminée avec erreurs : {failed_tables}")
else:
    print("Ingestion WOLF terminée avec succès")
    print("ok amal")
    print("ok amal")
    print("ok amal")


