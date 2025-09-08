from flask import Blueprint, render_template, request, jsonify, current_app, send_file
import os, io, json, threading, time
from pathlib import Path
import importlib.util, sys
import sqlite3
from datetime import datetime, timedelta
import csv
import logging
from typing import Dict, Optional, List

# === Logger ===
logger = logging.getLogger(__name__)

# === Blueprint exposé sous le nom 'bp' ===
bp = Blueprint(
    "studio",
    __name__,
    url_prefix="/studio",
    template_folder="templates",
    static_folder="static",
)

# === Chemins (fallback si variables d'env non fournies) ===
DB_PATH = os.environ.get(
    "SCRAPMASTER_DB",
    os.path.join(os.path.dirname(__file__), "database", "scrapmaster.db"),
)
SCRAPERS_DIR = os.environ.get(
    "SCRAPMASTER_SCRAPERS",
    os.path.join(os.path.dirname(__file__), "scrapers"),
)

# --- Utilities ---
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def list_scrapers():
    if not os.path.isdir(SCRAPERS_DIR):
        return []
    return [
        f for f in os.listdir(SCRAPERS_DIR)
        if f.endswith(".py") and not f.startswith("__")
    ]


def load_script_content(name):
    path = os.path.join(SCRAPERS_DIR, name)
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return f.read()


def save_script_content(name, content):
    path = os.path.join(SCRAPERS_DIR, name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):
        # allow creating new script
        open(path, "a", encoding="utf-8").close()
    # backup
    backup = path + ".bak"
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                old = f.read()
            with open(backup, "w", encoding="utf-8") as b:
                b.write(old)
    except Exception:
        pass
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    return True


# ============================
#  JobManager thread-safe ✅
# ============================
class JobManager:
    def __init__(self, cleanup_interval: int = 300):  # 5 min
        self._jobs: Dict[str, dict] = {}
        self._lock = threading.RLock()
        self._cleanup_interval = cleanup_interval
        self._last_cleanup = datetime.utcnow()

    def create_job(self, job_id: str, initial_data: dict) -> None:
        with self._lock:
            self._jobs[job_id] = {
                **initial_data,
                'created_at': datetime.utcnow(),
                'last_updated': datetime.utcnow(),
                'log': list(initial_data.get('log') or [])
            }
            self._maybe_cleanup()

    def update_job(self, job_id: str, updates: dict) -> bool:
        with self._lock:
            if job_id not in self._jobs:
                return False
            self._jobs[job_id].update(updates)
            self._jobs[job_id]['last_updated'] = datetime.utcnow()
            return True

    def add_log(self, job_id: str, message: str) -> None:
        with self._lock:
            if job_id not in self._jobs:
                return
            self._jobs[job_id].setdefault('log', []).append(message)
            self._jobs[job_id]['last_updated'] = datetime.utcnow()

    def get_job(self, job_id: str) -> Optional[dict]:
        with self._lock:
            job = self._jobs.get(job_id)
            return job.copy() if job else None

    def drain_logs(self, job_id: str) -> List[str]:
        """Retourne et vide les logs de façon atomique (pour ?drain=1)."""
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return []
            logs = job.get('log', [])
            drained = logs[:]
            logs.clear()
            job['last_updated'] = datetime.utcnow()
            return drained

    def _maybe_cleanup(self):
        """Nettoie les vieux jobs (appelé périodiquement)"""
        now = datetime.utcnow()
        if (now - self._last_cleanup).seconds < self._cleanup_interval:
            return

        cutoff = now - timedelta(hours=24)
        to_remove = [
            job_id for job_id, job in self._jobs.items()
            if job.get('last_updated', now) < cutoff
        ]

        for job_id in to_remove:
            del self._jobs[job_id]

        self._last_cleanup = now
        if to_remove:
            logger.info(f"Jobs nettoyés: {len(to_remove)}")


# Instance globale (remplace l'ancien dict global JOBS)
job_manager = JobManager()


# =========================================================
#  Décomposition de run_job monstrueux → fonctions dédiées
# =========================================================
def run_job(job_id: str, payload: dict) -> None:
    """
    Point d'entrée principal - orchestration only
    """
    try:
        job_manager.create_job(job_id, {
            "status": "running",
            "started_at": datetime.utcnow().isoformat() + "Z",
            "progress": 0,
            "log": [],
        })

        config = _validate_scraping_payload(payload)
        job_manager.add_log(job_id,
            f"🎯 Paramètres: types={config['types']}, countries={config['countries']}, "
            f"languages={config['languages']}, keywords='{config['keywords']}'"
        )

        _execute_scraping_combinations(job_id, config)

        job_manager.update_job(job_id, {
            "status": "done",
            "finished_at": datetime.utcnow().isoformat() + "Z",
            "progress": 100
        })
        job_manager.add_log(job_id, "🎯 Tâche terminée avec succès")

    except Exception as e:
        logger.exception("Erreur critique job", extra={"job_id": job_id})
        job_manager.update_job(job_id, {
            "status": "error",
            "error": str(e)[:500],
            "finished_at": datetime.utcnow().isoformat() + "Z"
        })
        job_manager.add_log(job_id, f"💥 Erreur critique: {str(e)[:300]}")


def _validate_scraping_payload(payload: dict) -> dict:
    """Valide et normalise le payload (compatible ancien comportement)"""
    if not isinstance(payload, dict):
        raise ValueError("Payload invalide")

    types = payload.get("types") or ["Associations"]
    countries = payload.get("countries") or ["Thaïlande"]
    languages = payload.get("languages") or ["fr"]
    keywords = (payload.get("keywords") or "").strip()

    # Gestion du "toutes les langues" (comportement historique)
    if not languages or any(str(x).strip().upper() in ("ALL", "*", "") for x in languages):
        try:
            conn = get_db()
            cur = conn.cursor()
            cur.execute("SELECT code FROM languages ORDER BY code")
            languages = [r[0] for r in cur.fetchall()] or ["th", "en", "fr"]
            conn.close()
        except Exception:
            languages = ["th", "en", "fr"]

    return {
        "types": types,
        "countries": countries,
        "languages": languages,
        "keywords": keywords
    }


def _execute_scraping_combinations(job_id: str, config: dict) -> None:
    """Exécute toutes les combinaisons pays/type/langue (garde la logique existante)"""
    types = config["types"]
    countries = config["countries"]
    languages = config["languages"]
    keywords = config["keywords"]

    total_combos = max(1, len(countries) * len(types) * len(languages))
    processed = 0
    job_manager.add_log(job_id, f"📊 {total_combos} combinaisons à traiter")

    for country in countries:
        for profession in types:
            for language in languages:
                job_manager.add_log(job_id, f"🚀 Démarrage: {profession} | {country} | {language}")
                _process_single_combination(job_id, country, profession, language, keywords)

                processed += 1
                job_manager.update_job(job_id, {
                    "progress": int(100 * processed / total_combos)
                })

                # Petite pause pour limiter la charge (comportement historique)
                time.sleep(0.5)


def _process_single_combination(job_id: str, country: str, profession: str, language: str, keywords: str) -> None:
    """
    Traite une combinaison spécifique : création projet, appel moteur, insert résultats,
    mise à jour compteurs — fidèle au comportement d'origine (insert étendu + fallback).
    """
    # Import différé pour éviter les circulaires (comme avant)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    if current_dir not in sys.path:
        sys.path.insert(0, current_dir)

    from scraper_engine import ScrapingEngine
    engine = ScrapingEngine()

    conn = get_db()
    cur = conn.cursor()
    project_id = None

    try:
        # 1) Créer un projet « temp »
        project_name = f"Studio-{profession}-{country}-{language}-{datetime.utcnow().strftime('%H%M%S')}"
        cur.execute(
            """
            INSERT INTO projects (name, profession, country, language, sources, status, created_at)
            VALUES (?, ?, ?, ?, ?, 'running', CURRENT_TIMESTAMP)
            """,
            (
                project_name,
                profession,
                country,
                language,
                json.dumps({"keywords": keywords}, ensure_ascii=False),
            ),
        )
        project_id = cur.lastrowid
        conn.commit()

        # ► Marquer le début du run
        cur.execute(
            "UPDATE projects SET started_at = CURRENT_TIMESTAMP WHERE id = ?",
            (project_id,),
        )
        conn.commit()

        # 2) Récupérer la ligne complète
        cur.execute("SELECT * FROM projects WHERE id = ?", (project_id,))
        project_row = cur.fetchone()
        if not project_row:
            raise Exception("Projet non créé en base")

        # Adapter la Row en dict + décoder sources JSON
        project_cfg = dict(project_row)
        try:
            project_cfg["sources"] = json.loads(project_cfg.get("sources") or "[]")
        except Exception:
            project_cfg["sources"] = []

        # Surcharges de sécurité
        project_cfg["profession"] = project_cfg.get("profession") or profession
        project_cfg["country"] = project_cfg.get("country") or country
        project_cfg["language"] = project_cfg.get("language") or language
        project_cfg["keep_incomplete"] = True  # garder entrées incomplètes

        job_manager.add_log(job_id, f"📋 Projet #{project_id} créé")

        # 3) Appeler le moteur
        results = []
        try:
            # Essai avec dict (nouveau moteur)
            results = engine.run_scraping(project_cfg) or []
        except TypeError:
            # Fallback : moteur ancien qui attend un tuple/Row
            results = engine.run_scraping(project_row) or []

        job_manager.add_log(job_id, f"📊 Scraping terminé: {len(results)} résultats bruts")

        # 4) Sauvegarder les résultats (logique d'origine, insert étendu + fallback)
        saved_count = 0
        for i, r in enumerate(results):
            try:
                # Anti-doublons basique sur website pour le même pays
                w = (r.get("website") or "").strip()
                if w:
                    cur.execute(
                        "SELECT 1 FROM results WHERE LOWER(COALESCE(website,'')) = LOWER(?) AND COALESCE(country,'') = COALESCE(?, '') LIMIT 1",
                        (w, r.get("country", country)),
                    )
                    if cur.fetchone():
                        continue

                # Tentative : schéma étendu, y compris telegram + wechat + raw_json
                try:
                    cur.execute(
                        """
                        INSERT INTO results (
                            project_id, name, category, description, website,
                            email, phone, city, country, language, source_url,
                            facebook, instagram, linkedin, line_id, whatsapp, telegram, wechat,
                            other_contact, contact_name, province, address,
                            latitude, longitude, raw_json, scraped_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        """,
                        (
                            project_id,
                            (r.get("name") or "")[:500],
                            r.get("category"),
                            (r.get("description") or "")[:2000],
                            r.get("website"),
                            r.get("email"),
                            r.get("phone"),
                            r.get("city"),
                            r.get("country", country),
                            r.get("language", language),
                            r.get("source_url"),
                            r.get("facebook"),
                            r.get("instagram"),
                            r.get("linkedin"),
                            r.get("line_id"),
                            r.get("whatsapp"),
                            r.get("telegram"),
                            r.get("wechat"),
                            r.get("other_contact"),
                            r.get("contact_name"),
                            r.get("province"),
                            r.get("address"),
                            r.get("latitude"),
                            r.get("longitude"),
                            json.dumps(r, ensure_ascii=False),
                        ),
                    )
                except Exception:
                    # Fallback : schéma minimal (compat ancien)
                    cur.execute(
                        """
                        INSERT INTO results (
                            project_id, name, category, description, website,
                            email, phone, city, country, language, source_url, scraped_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        """,
                        (
                            project_id,
                            (r.get("name") or "")[:500],
                            r.get("category"),
                            (r.get("description") or "")[:2000],
                            r.get("website"),
                            r.get("email"),
                            r.get("phone"),
                            r.get("city"),
                            r.get("country", country),
                            r.get("language", language),
                            r.get("source_url"),
                        ),
                    )
                saved_count += 1
            except Exception as e:
                job_manager.add_log(job_id, f"⚠️ Erreur sauvegarde résultat #{i+1}: {str(e)[:160]}")

        # 5) Compteurs & timings puis finaliser le projet (identique à l’origine)
        cur.execute(
            """
            SELECT 
              COUNT(*) AS total,
              SUM(CASE WHEN TRIM(IFNULL(email,''))    <> '' THEN 1 ELSE 0 END) AS emails_count,
              SUM(CASE WHEN TRIM(IFNULL(phone,''))    <> '' THEN 1 ELSE 0 END) AS phones_count,
              SUM(CASE WHEN TRIM(IFNULL(whatsapp,'')) <> '' THEN 1 ELSE 0 END) AS whatsapp_count,
              SUM(CASE WHEN TRIM(IFNULL(line_id,''))  <> '' THEN 1 ELSE 0 END) AS line_id_count,
              SUM(CASE WHEN TRIM(IFNULL(telegram,'')) <> '' THEN 1 ELSE 0 END) AS telegram_count,
              SUM(CASE WHEN TRIM(IFNULL(wechat,''))   <> '' THEN 1 ELSE 0 END) AS wechat_count
            FROM results
            WHERE project_id = ?
            """,
            (project_id,),
        )
        row = cur.fetchone() or (0, 0, 0, 0, 0, 0, 0)
        total, emails_c, phones_c, wa_c, line_c, tg_c, wc_c = row

        cur.execute(
            """
            UPDATE projects 
            SET status='completed',
                total_results=?,
                emails_count=?,
                phones_count=?,
                whatsapp_count=?,
                line_id_count=?,
                telegram_count=?,
                wechat_count=?,
                finished_at=CURRENT_TIMESTAMP,
                last_run=CURRENT_TIMESTAMP,
                run_ms = CAST((strftime('%s', CURRENT_TIMESTAMP) - strftime('%s', COALESCE(started_at, CURRENT_TIMESTAMP))) * 1000 AS INTEGER)
            WHERE id=?
            """,
            (total, emails_c, phones_c, wa_c, line_c, tg_c, wc_c, project_id),
        )
        conn.commit()
        job_manager.add_log(job_id,
            f"✅ {country}/{profession}/{language}: {total} entrées (emails:{emails_c} / phones:{phones_c} / wa:{wa_c} / line:{line_c} / tg:{tg_c} / wc:{wc_c})"
        )

    except Exception as e:
        error_msg = str(e)[:200]
        job_manager.add_log(job_id, f"❌ Erreur {country}/{profession}/{language}: {error_msg}")
        try:
            if project_id:
                cur.execute(
                    """
                    UPDATE projects 
                    SET status='error', last_run=CURRENT_TIMESTAMP, finished_at=CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (project_id,),
                )
                conn.commit()
        except Exception:
            pass
    finally:
        conn.close()


# --- ROUTES ---

@bp.route("/", methods=["GET"])
def studio():
    return render_template("ui_studio.html")


@bp.route("/api/filters", methods=["GET"])
def api_filters():
    """Récupère les filtres depuis la base de données avec fallbacks"""
    conn = get_db()
    cur = conn.cursor()

    # Types (professions)
    try:
        cur.execute("SELECT DISTINCT name FROM professions ORDER BY name")
        types = [r[0] for r in cur.fetchall()]
        if not types:
            raise ValueError("empty")
    except Exception:
        types = [
            "Associations",
            "YouTubeurs",
            "Avocats",
            "Traducteurs",
            "Interprètes",
            "Digital Nomads",
            "Restaurateurs",
            "Hôteliers",
        ]

    # Countries
    try:
        cur.execute("SELECT DISTINCT name FROM countries ORDER BY name")
        countries = [r[0] for r in cur.fetchall()]
        if not countries:
            raise ValueError("empty")
    except Exception:
        countries = [
            "Thaïlande",
            "France",
            "Expatriés Thaïlande",
            "Digital Nomads Asie",
            "Voyageurs Asie du Sud-Est",
            "Royaume-Uni",
            "États-Unis",
            "Allemagne",
        ]

    # Languages
    try:
        cur.execute("SELECT DISTINCT code FROM languages ORDER BY code")
        languages = [r[0] for r in cur.fetchall()]
        if not languages:
            raise ValueError("empty")
    except Exception:
        languages = ["fr", "en", "th", "de", "es", "it", "ru", "zh", "ja"]

    conn.close()
    return jsonify({"types": types, "countries": countries, "languages": languages})


@bp.route("/api/scripts", methods=["GET"])
def api_scripts():
    """Liste tous les fichiers Python dans le dossier scrapers"""
    return jsonify({"scripts": list_scrapers()})


@bp.route("/api/script", methods=["GET", "POST"])
def api_script():
    """Charge ou sauvegarde un script de scraping"""
    if request.method == "GET":
        name = request.args.get("name")
        if not name:
            return jsonify({"error": "missing_name"}), 400
        content = load_script_content(name)
        if content is None:
            return jsonify({"error": "not_found"}), 404
        return jsonify({"name": name, "content": content})

    else:  # POST
        try:
            data = request.get_json(force=True)
            name = data.get("name", "").strip()
            content = data.get("content", "")

            if not name or not name.endswith(".py"):
                return jsonify({"error": "invalid_name"}), 400

            # Validation basique du contenu Python
            try:
                compile(content, name, "exec")
            except SyntaxError as e:
                return jsonify({"error": "syntax_error", "details": str(e)}), 400

            success = save_script_content(name, content)
            return jsonify({"ok": success})

        except Exception as e:
            return jsonify({"error": "save_failed", "details": str(e)}), 500


@bp.route("/api/run", methods=["POST"])
def api_run():
    """Lance une tâche de scraping en arrière-plan"""
    try:
        data = request.get_json(force=True)

        # Validation des paramètres (identique au flux existant côté API)
        types = data.get("types", [])
        countries = data.get("countries", [])
        # languages facultatif (géré dans _validate_scraping_payload)

        if not types:
            return jsonify({"error": "missing_types"}), 400
        if not countries:
            return jsonify({"error": "missing_countries"}), 400

        # Génération d'un ID unique pour la tâche
        job_id = f"job_{int(time.time()*1000)}_{hash(str(data)) % 10000}"

        # Lancement du thread
        thread = threading.Thread(target=run_job, args=(job_id, data), daemon=True)
        thread.start()

        return jsonify({"job_id": job_id, "message": "Tâche lancée avec succès"})

    except Exception as e:
        return jsonify({"error": "launch_failed", "details": str(e)}), 500


@bp.route("/api/jobs/<job_id>", methods=["GET"])
def api_job(job_id):
    """Récupère l'état d'une tâche avec option de vidage des logs (drain)"""
    job = job_manager.get_job(job_id)
    if not job:
        return jsonify({"error": "not_found"}), 404

    response = dict(job)
    if request.args.get("drain") == "1":
        response["log"] = job_manager.drain_logs(job_id)

    return jsonify(response)


# === Export unifié via mapping FR/EN ==========================
# Crée `config/export_schemas.py` avec EXPORT_SCHEMA + get_export_mapping(lang)
try:
    from config.export_schemas import get_export_mapping
except Exception:
    # Fallback minimal si le fichier n'est pas encore en place
    def get_export_mapping(language: str) -> Dict[str, str]:
        base = {
            "name": "name" if language != "fr" else "nom_organisation",
            "category": "category" if language != "fr" else "categorie_principale",
            "email": "email",
            "phone": "phone" if language != "fr" else "telephone",
            "website": "website" if language != "fr" else "site_web",
            "facebook": "facebook" if language != "fr" else "facebook",
            "whatsapp": "whatsapp",
            "telegram": "telegram" if language != "fr" else "telegram",
            "wechat": "wechat",
            "address": "address" if language != "fr" else "adresse_postale",
            "country": "coverage_area" if language != "fr" else "zone_couverte",
            "language": "languages" if language != "fr" else "langues",
            "source_url": "source_url" if language != "fr" else "source_url_principale",
        }
        return base


@bp.route("/api/export", methods=["GET"])
def api_export():
    """
    Exporte la table results avec un mapping **unifié** FR/EN.
    - template=fr (défaut) : CSV FR
    - template=en : XLSX si possible (openpyxl), sinon CSV EN
    """
    language = (request.args.get("template") or "fr").lower()
    mapping = get_export_mapping(language)  # DB field -> export header
    headers_out = list(mapping.values())
    db_fields = list(mapping.keys())

    conn = get_db()
    try:
        # Charge avec pandas si possible (pour XLSX EN)
        try:
            import pandas as pd  # type: ignore
            df = pd.read_sql_query(
                f"""
                SELECT {", ".join(set(db_fields))} FROM results
                ORDER BY id DESC
                LIMIT 10000
                """,
                conn,
            )
        except Exception:
            df = None

        # Si EN et pandas + openpyxl dispo → XLSX
        if language == "en":
            try:
                import pandas as pd  # type: ignore
                out_df = None
                if df is not None:
                    # Renommer colonnes selon mapping
                    out_df = df.rename(columns=mapping)
                else:
                    # Fallback: construire via cursor
                    cur = conn.cursor()
                    cur.execute(
                        f"SELECT {', '.join(set(db_fields))} FROM results ORDER BY id DESC LIMIT 10000"
                    )
                    rows = cur.fetchall()
                    in_headers = [d[0] for d in cur.description]
                    records = [dict(zip(in_headers, r)) for r in rows]
                    out_rows = [{mapping.get(k, k): records[i].get(k, "") for k in db_fields} for i in range(len(records))]
                    out_df = pd.DataFrame(out_rows, columns=headers_out)

                # Export XLSX (openpyxl)
                try:
                    with pd.ExcelWriter("export_scrapmaster_en.xlsx", engine="openpyxl") as writer:
                        out_df.to_excel(writer, index=False, sheet_name="Sheet1")
                    return send_file(
                        "export_scrapmaster_en.xlsx",
                        as_attachment=True,
                        download_name="export_scrapmaster_en.xlsx",
                    )
                except Exception:
                    # Fallback CSV EN
                    csv_buf = io.StringIO()
                    out_df.to_csv(csv_buf, index=False)
                    csv_buf.seek(0)
                    return current_app.response_class(
                        csv_buf.getvalue(),
                        mimetype="text/csv; charset=utf-8",
                        headers={
                            "Content-Disposition": "attachment; filename=export_scrapmaster_en.csv"
                        },
                    )
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

        # ===== CSV FR (ou EN si pas de pandas / openpyxl) =====
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(headers_out)

        if df is not None:
            # Garantir l'ordre des colonnes de sortie
            safe_df = df.copy()
            for col in db_fields:
                if col not in safe_df.columns:
                    safe_df[col] = ""
            safe_df = safe_df[db_fields].rename(columns=mapping)
            safe_df.to_csv(output, index=False)
        else:
            cur = conn.cursor()
            cur.execute(
                f"SELECT {', '.join(set(db_fields))} FROM results ORDER BY id DESC LIMIT 10000"
            )
            rows = cur.fetchall()
            in_headers = [d[0] for d in cur.description]
            for r in rows:
                row_dict = dict(zip(in_headers, r))
                writer.writerow([row_dict.get(k, "") for k in db_fields])

        output.seek(0)
        return current_app.response_class(
            output.getvalue(),
            mimetype="text/csv; charset=utf-8",
            headers={"Content-Disposition": "attachment; filename=export_scrapmaster.csv"},
        )

    except Exception as e:
        # Dernier filet : export brut
        try:
            cur = conn.cursor()
            cur.execute("SELECT * FROM results ORDER BY id DESC LIMIT 1000")
            rows = cur.fetchall()
            output = io.StringIO()
            if rows:
                headers = [desc[0] for desc in cur.description]
                output.write(",".join(f'"{h}"' for h in headers) + "\n")
                for row in rows:
                    csv_row = [
                        f'"{str(val or "").replace(chr(34), chr(34)+chr(34))}"' for val in row
                    ]
                    output.write(",".join(csv_row) + "\n")
            output.seek(0)
            return current_app.response_class(
                output.getvalue(),
                mimetype="text/csv; charset=utf-8",
                headers={"Content-Disposition": "attachment; filename=export_scrapmaster_fallback.csv"},
            )
        finally:
            try:
                conn.close()
            except Exception:
                pass


# ========= Nouvelles routes : historique & export direct projet =========

@bp.route("/api/history", methods=["GET"])
def api_history():
    """
    Renvoie l'historique des recherches (projets), avec:
      - horodatage (created_at, started_at, finished_at)
      - pays / langue / type
      - status, durée (ms)
      - total_results et compteurs email/phone/whatsapp/line/telegram/wechat
    Query params optionnels:
      - limit (int, défaut 100)
      - q (filtre LIKE sur name)
    """
    limit = 100
    try:
        limit = max(1, min(1000, int(request.args.get("limit", "100"))))
    except Exception:
        pass

    q = (request.args.get("q") or "").strip()
    conn = get_db()
    cur = conn.cursor()

    base_sql = """
      SELECT 
        id, name, profession, country, language, status,
        created_at, started_at, finished_at, last_run, run_ms,
        total_results,
        COALESCE(emails_count,0)   AS emails_count,
        COALESCE(phones_count,0)   AS phones_count,
        COALESCE(whatsapp_count,0) AS whatsapp_count,
        COALESCE(line_id_count,0)  AS line_id_count,
        COALESCE(telegram_count,0) AS telegram_count,
        COALESCE(wechat_count,0)   AS wechat_count
      FROM projects
      WHERE 1=1
    """
    params = []
    if q:
        base_sql += " AND name LIKE ? "
        params.append(f"%{q}%")

    base_sql += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)

    cur.execute(base_sql, params)
    rows = cur.fetchall()
    conn.close()

    # format JSON-friendly
    out = []
    for r in rows:
        d = {
            "id": r[0],
            "name": r[1],
            "profession": r[2],
            "country": r[3],
            "language": r[4],
            "status": r[5],
            "created_at": r[6],
            "started_at": r[7],
            "finished_at": r[8],
            "last_run": r[9],
            "run_ms": r[10],
            "total_results": r[11],
            "emails_count": r[12],
            "phones_count": r[13],
            "whatsapp_count": r[14],
            "line_id_count": r[15],
            "telegram_count": r[16],
            "wechat_count": r[17],
        }
        out.append(d)

    return jsonify({"items": out})


@bp.route("/api/export_project/<int:project_id>", methods=["GET"])
def api_export_project(project_id: int):
    """
    Proxy d'export par projet (CSV) depuis le Studio.
    Utilise Pandas si dispo, sinon CSV manuel.
    """
    try:
        conn = get_db()
        try:
            import pandas as pd
            df = pd.read_sql_query("""
                SELECT r.*, p.name as project_name, p.profession, p.country as project_country, p.language as project_language
                FROM results r
                JOIN projects p ON r.project_id = p.id
                WHERE r.project_id = ?
                ORDER BY r.id DESC
            """, conn, params=(project_id,))
            csv_buf = io.StringIO()
            df.to_csv(csv_buf, index=False, encoding="utf-8")
            csv_buf.seek(0)
            return current_app.response_class(
                csv_buf.getvalue(),
                mimetype="text/csv; charset=utf-8",
                headers={"Content-Disposition": f'attachment; filename="project_{project_id}.csv"'}
            )
        except Exception:
            cur = conn.cursor()
            cur.execute("""
                SELECT r.*, p.name as project_name, p.profession, p.country as project_country, p.language as project_language
                FROM results r
                JOIN projects p ON r.project_id = p.id
                WHERE r.project_id = ?
                ORDER BY r.id DESC
            """, (project_id,))
            rows = cur.fetchall()
            headers = [d[0] for d in cur.description]
            conn.close()

            buf = io.StringIO()
            w = csv.writer(buf)
            w.writerow(headers)
            for row in rows:
                w.writerow(row)
            buf.seek(0)
            return current_app.response_class(
                buf.getvalue(),
                mimetype="text/csv; charset=utf-8",
                headers={"Content-Disposition": f'attachment; filename="project_{project_id}.csv"'}
            )
    except Exception as e:
        return jsonify({"error": str(e)}), 500
