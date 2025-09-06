from flask import Blueprint, render_template, request, jsonify, current_app, send_file
import os, io, json, threading, time
from pathlib import Path
import importlib.util, sys
import sqlite3
from datetime import datetime
import csv

# === Blueprint exposé sous le nom 'bp' ===
bp = Blueprint("studio", __name__, url_prefix="/studio", template_folder="templates", static_folder="static")

# === Chemins (fallback si variables d'env non fournies) ===
DB_PATH = os.environ.get("SCRAPMASTER_DB", os.path.join(os.path.dirname(__file__), "database", "scrapmaster.db"))
SCRAPERS_DIR = os.environ.get("SCRAPMASTER_SCRAPERS", os.path.join(os.path.dirname(__file__), "scrapers"))

# --- Utilities ---
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def list_scrapers():
    if not os.path.isdir(SCRAPERS_DIR):
        return []
    return [f for f in os.listdir(SCRAPERS_DIR) if f.endswith(".py") and not f.startswith("__")]

def load_script_content(name):
    path = os.path.join(SCRAPERS_DIR, name)
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return f.read()

def save_script_content(name, content):
    path = os.path.join(SCRAPERS_DIR, name)
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

# Simple in-memory jobs registry
JOBS = {}

def run_job(job_id, payload):
    """
    Exécute une tâche de scraping en thread en s'alignant sur TON moteur.
    Ici on crée un projet en DB par combinaison (type × pays × langue),
    puis on appelle ScrapingEngine.run_scraping(project_row),
    enfin on insère les résultats dans 'results'.
    payload = {types:[], countries:[], languages:[], keywords:str}
    """
    JOBS[job_id] = {
        "status": "running",
        "started_at": datetime.utcnow().isoformat() + "Z",
        "progress": 0,
        "log": []
    }
    try:
        # Lazy import to avoid circulars
        from scraper_engine import ScrapingEngine
        engine = ScrapingEngine()

        types = payload.get("types") or ["Associations"]
        countries = payload.get("countries") or ["Thaïlande"]
        languages = payload.get("languages") or ["multi"]
        keywords = (payload.get("keywords") or "").strip()

        # >>> Ajout : accepter ALL / * / vide => toutes les langues de la DB
        try:
            wants_all = (not languages) or any(str(x).strip().upper() in ("ALL", "*") for x in languages)
            if wants_all:
                conn_all = get_db()
                cur_all = conn_all.cursor()
                cur_all.execute("SELECT code FROM languages ORDER BY code")
                languages = [r[0] for r in cur_all.fetchall()] or ["th", "en", "fr"]
                conn_all.close()
        except Exception:
            languages = ["th", "en", "fr"]

        combos = [(c, t, l) for c in countries for t in types for l in languages]
        total = max(1, len(combos))
        done = 0

        for country in countries or ["Thaïlande"]:
            for t in types or ["Associations"]:
                for lang in languages or ["multi"]:
                    JOBS[job_id]["log"].append(
                        f"Lancement: country={country}, type={t}, language={lang}, keywords='{keywords}'"
                    )

                    conn = get_db()
                    cur = conn.cursor()

                    # 1) créer un projet synthétique
                    proj_name = f"Studio – {t} – {country} – {lang} – {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}"
                    cur.execute("""
                        INSERT INTO projects (name, profession, country, language, sources, status, created_at)
                        VALUES (?, ?, ?, ?, ?, 'created', CURRENT_TIMESTAMP)
                    """, (
                        proj_name,
                        t,
                        country,
                        lang,
                        json.dumps({"keywords": keywords}, ensure_ascii=False)
                    ))
                    project_id = cur.lastrowid
                    conn.commit()

                    # 2) récupérer la ligne projet (tuple) pour le moteur
                    cur.execute("SELECT * FROM projects WHERE id = ?", (project_id,))
                    project_row = cur.fetchone()

                    try:
                        # 3) appel du moteur réel
                        results = engine.run_scraping(project_row) or []

                        # 4) insérer les résultats
                        inserted = 0
                        for r in results:
                            cur.execute("""
                                INSERT INTO results (
                                    project_id, name, category, description, website,
                                    email, phone, city, country, language, source_url
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (
                                project_id,
                                r.get('name'),
                                r.get('category'),
                                r.get('description'),
                                r.get('website'),
                                r.get('email'),
                                r.get('phone'),
                                r.get('city'),
                                r.get('country'),
                                r.get('language'),
                                r.get('source_url')
                            ))
                            inserted += 1

                        # 5) statut du projet
                        cur.execute("""
                            UPDATE projects
                            SET status='completed', total_results=?, last_run=CURRENT_TIMESTAMP
                            WHERE id=?
                        """, (inserted, project_id))
                        conn.commit()

                        JOBS[job_id]["log"].append(
                            f"Terminé: {country}/{t}/{lang} → {inserted} éléments"
                        )

                    except Exception as e:
                        cur.execute("UPDATE projects SET status='error', last_run=CURRENT_TIMESTAMP WHERE id=?", (project_id,))
                        conn.commit()
                        JOBS[job_id]["log"].append(f"Erreur moteur {country}/{t}/{lang}: {e}")

                    finally:
                        conn.close()

                    done += 1
                    JOBS[job_id]["progress"] = int(100 * done / total)

        JOBS[job_id]["status"] = "done"
        JOBS[job_id]["finished_at"] = datetime.utcnow().isoformat() + "Z"

    except Exception as e:
        JOBS[job_id]["status"] = "error"
        JOBS[job_id]["error"] = str(e)

# --- ROUTES ---

@bp.route("/", methods=["GET"])
def studio():
    return render_template("ui_studio.html")

@bp.route("/api/filters", methods=["GET"])
def api_filters():
    # Read distinct values from DB if available; else fallback defaults
    conn = get_db()
    cur = conn.cursor()
    # Types (professions)
    try:
        cur.execute("SELECT DISTINCT name FROM professions ORDER BY name")
        types = [r[0] for r in cur.fetchall()]
        if not types:
            raise ValueError("empty")
    except Exception:
        types = ["Associations", "YouTubeurs", "Avocats", "Traducteurs", "Digital Nomads"]
    # Countries
    try:
        cur.execute("SELECT DISTINCT name FROM countries ORDER BY name")
        countries = [r[0] for r in cur.fetchall()]
        if not countries:
            raise ValueError("empty")
    except Exception:
        countries = ["Thaïlande"]
    # Languages
    try:
        cur.execute("SELECT DISTINCT code FROM languages ORDER BY code")
        languages = [r[0] for r in cur.fetchall()]
        if not languages:
            raise ValueError("empty")
    except Exception:
        languages = ["th", "en", "fr", "de", "ru", "zh", "ja", "ko", "es", "it", "pt", "ar", "hi"]
    conn.close()
    return jsonify({"types": types, "countries": countries, "languages": languages})

@bp.route("/api/scripts", methods=["GET"])
def api_scripts():
    return jsonify({"scripts": list_scrapers()})

@bp.route("/api/script", methods=["GET", "POST"])
def api_script():
    if request.method == "GET":
        name = request.args.get("name")
        content = load_script_content(name) if name else None
        if content is None:
            return jsonify({"error": "not_found"}), 404
        return jsonify({"name": name, "content": content})
    else:
        data = request.get_json(force=True)
        name = data.get("name")
        content = data.get("content", "")
        if not name or not name.endswith(".py"):
            return jsonify({"error": "invalid_name"}), 400
        ok = save_script_content(name, content)
        return jsonify({"ok": bool(ok)})

@bp.route("/api/run", methods=["POST"])
def api_run():
    data = request.get_json(force=True)
    job_id = f"job_{int(time.time()*1000)}"
    t = threading.Thread(target=run_job, args=(job_id, data), daemon=True)
    t.start()
    return jsonify({"job_id": job_id})

@bp.route("/api/jobs/<job_id>", methods=["GET"])
def api_job(job_id):
    job = JOBS.get(job_id)
    if not job:
        return jsonify({"error": "not_found"}), 404

    # On fait une copie superficielle de l'état du job
    resp = dict(job)

    # Option "drain": si ?drain=1, on renvoie les logs et on les vide côté serveur
    if request.args.get("drain") == "1":
        logs = job.get("log", [])
        resp["log"] = logs[:]   # renvoyer une copie
        logs.clear()            # vider pour éviter de les revoir au prochain poll

    return jsonify(resp)

@bp.route("/api/export", methods=["GET"])
def api_export():
    """
    Exporte la table results vers un CSV mappé sur la trame Excel.
    Remplit les colonnes manquantes avec des vides et renomme les champs clés.
    """
    mapping = [
        "id","nom_organisation","statut_juridique","categorie_principale","sous_categories",
        "langues","public_cible","zone_couverte","adresse_postale","telephone","email","site_web",
        "facebook","instagram","linkedin","line_id_ou_lien","whatsapp","autre_contact",
        "personne_contact","horaires","cout_adhesion","conditions_acces","services_offerts",
        "description","annee_creation","numero_enregistrement","taille_membres","evenements_reguliers",
        "partenaires_affiliations","organisation_parente","chapitres_locaux","mots_cles",
        "source_url_principale","sources_secondaires","date_verification","fiabilite","commentaire_notes"
    ]
    conn = get_db()
    try:
        import pandas as pd
        df = pd.read_sql_query("SELECT * FROM results ORDER BY id DESC", conn)
    except Exception:
        import pandas as pd
        df = pd.DataFrame()
    finally:
        conn.close()

    # Renommer (colonnes DB -> trame Excel cible)
    rename = {
        "name": "nom_organisation",
        "category": "categorie_principale",
        "website": "site_web",
        "phone": "telephone",
        "language": "langues",
        "country": "zone_couverte",
        "source_url": "source_url_principale"
    }
    for a, b in rename.items():
        if a in df.columns and b not in df.columns:
            df[b] = df[a]

    # Garantir toutes les colonnes demandées
    for col in mapping:
        if col not in df.columns:
            df[col] = None

    # Réordonner selon la trame
    df = df[mapping]

    # Stream as CSV
    out = io.StringIO()
    df.to_csv(out, index=False)
    out.seek(0)
    return current_app.response_class(
        out.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=export_scrapmaster.csv"}
    )
