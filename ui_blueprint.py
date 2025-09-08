from flask import Blueprint, render_template, request, jsonify, current_app, send_file
import os, io, json, threading, time
from pathlib import Path
import importlib.util, sys
import sqlite3
from datetime import datetime
import csv

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


# Simple in-memory jobs registry
JOBS = {}


def run_job(job_id, payload):
    """
    Exécute une tâche de scraping en s'alignant sur le moteur existant.
    payload = {types:[], countries:[], languages:[], keywords:str}
    """
    JOBS[job_id] = {
        "status": "running",
        "started_at": datetime.utcnow().isoformat() + "Z",
        "progress": 0,
        "log": [],
    }

    try:
        # Import différé pour éviter les circulaires
        current_dir = os.path.dirname(os.path.abspath(__file__))
        if current_dir not in sys.path:
            sys.path.insert(0, current_dir)

        from scraper_engine import ScrapingEngine
        engine = ScrapingEngine()

        # Extraction et normalisation des paramètres
        types = payload.get("types") or ["Associations"]
        countries = payload.get("countries") or ["Thaïlande"]
        languages = payload.get("languages") or ["fr"]
        keywords = (payload.get("keywords") or "").strip()

        JOBS[job_id]["log"].append(
            f"🎯 Paramètres: types={types}, countries={countries}, languages={languages}, keywords='{keywords}'"
        )

        # Gestion du "toutes les langues"
        if not languages or any(str(x).strip().upper() in ("ALL", "*", "") for x in languages):
            try:
                conn = get_db()
                cur = conn.cursor()
                cur.execute("SELECT code FROM languages ORDER BY code")
                languages = [r[0] for r in cur.fetchall()] or ["th", "en", "fr"]
                conn.close()
                JOBS[job_id]["log"].append(f"📝 Langues étendues: {languages}")
            except Exception as e:
                languages = ["th", "en", "fr"]
                JOBS[job_id]["log"].append(f"⚠️ Fallback langues: {e}")

        # Calcul du total pour le progress
        total_combos = max(1, len(countries) * len(types) * len(languages))
        processed = 0

        JOBS[job_id]["log"].append(f"📊 {total_combos} combinaisons à traiter")

        # Boucle principale : une combinaison = un projet
        for country in countries:
            for profession in types:
                for language in languages:
                    JOBS[job_id]["log"].append(
                        f"🚀 Démarrage: {profession} | {country} | {language}"
                    )

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

                        JOBS[job_id]["log"].append(f"📋 Projet #{project_id} créé")

                        # 3) Appeler le moteur
                        results = []
                        try:
                            # Essai avec dict (nouveau moteur)
                            results = engine.run_scraping(project_cfg) or []
                        except TypeError:
                            # Fallback : moteur ancien qui attend un tuple/Row
                            results = engine.run_scraping(project_row) or []

                        JOBS[job_id]["log"].append(
                            f"📊 Scraping terminé: {len(results)} résultats bruts"
                        )

                        # 4) Sauvegarder les résultats
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
                                JOBS[job_id]["log"].append(
                                    f"⚠️ Erreur sauvegarde résultat #{i+1}: {str(e)[:160]}"
                                )

                        # 5) Compteurs & timings puis finaliser le projet
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
                        JOBS[job_id]["log"].append(
                            f"✅ {country}/{profession}/{language}: {total} entrées (emails:{emails_c} / phones:{phones_c} / wa:{wa_c} / line:{line_c} / tg:{tg_c} / wc:{wc_c})"
                        )

                    except Exception as e:
                        error_msg = str(e)[:200]
                        JOBS[job_id]["log"].append(
                            f"❌ Erreur {country}/{profession}/{language}: {error_msg}"
                        )
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

                    # Mise à jour du progrès
                    processed += 1
                    JOBS[job_id]["progress"] = int(100 * processed / total_combos)

                    # Petite pause pour limiter la charge
                    time.sleep(0.5)

        # Finalisation globale
        JOBS[job_id]["status"] = "done"
        JOBS[job_id]["finished_at"] = datetime.utcnow().isoformat() + "Z"
        JOBS[job_id]["log"].append(
            f"🎯 Tâche terminée avec succès: {processed} combinaisons traitées"
        )

    except Exception as e:
        error_msg = str(e)[:300]
        JOBS[job_id]["status"] = "error"
        JOBS[job_id]["error"] = error_msg
        JOBS[job_id]["log"].append(f"💥 Erreur critique: {error_msg}")
        JOBS[job_id]["finished_at"] = datetime.utcnow().isoformat() + "Z"


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

        # Validation des paramètres
        types = data.get("types", [])
        countries = data.get("countries", [])
        languages = data.get("languages", [])

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
    """Récupère l'état d'une tâche avec option de vidage des logs"""
    job = JOBS.get(job_id)
    if not job:
        return jsonify({"error": "not_found"}), 404

    # Copie de l'état actuel
    response = dict(job)

    # Option "drain": renvoyer les logs et les vider côté serveur
    if request.args.get("drain") == "1":
        logs = job.get("log", [])
        response["log"] = logs[:]  # Copie pour la réponse
        logs.clear()  # Vider pour éviter les répétitions

    return jsonify(response)


@bp.route("/api/export", methods=["GET"])
def api_export():
    """
    Exporte la table results.
      - template=fr (défaut) : CSV FR (trame actuelle)
      - template=en : XLSX « modèle EN » enrichi (avec telegram_url, wechat)
    """
    template = (request.args.get("template") or "fr").lower()

    conn = get_db()
    try:
        # Charge les données avec pandas si dispo
        try:
            import pandas as pd  # type: ignore
            df = pd.read_sql_query(
                """
                SELECT r.*, p.name as project_name, p.profession
                FROM results r
                LEFT JOIN projects p ON r.project_id = p.id
                ORDER BY r.id DESC
                LIMIT 10000
                """,
                conn,
            )
        except Exception:
            df = None

        if template == "en":
            # ===== Export XLSX modèle EN =====
            try:
                import pandas as pd
                import json as _json

                cols = [
                    "name","category","short_description","languages","city","province","coverage_area","address",
                    "latitude","longitude","email","phone","whatsapp","line_id","website",
                    "facebook_url","instagram_url","linkedin_url","line_link","whatsapp_link","telegram_url","wechat","other_contact",
                    "contact_name","opening_hours","membership_fee","access_conditions","services_offered",
                    "description","founded_year","registration_number","members_size",
                    "regular_events","affiliations","parent_org","local_chapters","keywords",
                    "source_urls","last_verified_date","verification_method","status","risk_flags","quality_score",
                    "firm_name","lawyer_name","practice_areas","bar_or_license_no","years_experience",
                    "consultation_modes","consultation_languages","fee_structure","emergency_hotline"
                ]

                # Construire records (avec ou sans pandas)
                records = []
                if df is None:
                    cur = conn.cursor()
                    cur.execute(
                        """
                        SELECT r.*, p.name as project_name, p.profession
                        FROM results r
                        LEFT JOIN projects p ON r.project_id = p.id
                        ORDER BY r.id DESC
                        LIMIT 10000
                        """
                    )
                    rows = cur.fetchall()
                    headers = [d[0] for d in cur.description]
                    for row in rows:
                        records.append(dict(zip(headers, row)))
                else:
                    records = df.to_dict(orient="records")

                def take(row, *keys):
                    # Prend la 1ère valeur non vide parmi keys; sinon tente dans raw_json
                    for k in keys:
                        v = row.get(k)
                        if v is not None and str(v).strip():
                            return v
                    try:
                        raw = row.get("raw_json")
                        if raw:
                            rawj = _json.loads(raw)
                            for k in keys:
                                v = rawj.get(k)
                                if v:
                                    return v
                    except Exception:
                        pass
                    return None

                out_rows = []
                for row in records:
                    out_rows.append({
                        "name": take(row, "name", "nom_organisation"),
                        "category": take(row, "category", "categorie_principale"),
                        "short_description": take(row, "short_description", "description_courte"),
                        "languages": take(row, "language", "langues"),
                        "city": take(row, "city", "ville"),
                        "province": take(row, "province", "departement", "state"),
                        "coverage_area": take(row, "zone_couverte", "coverage_area", "country"),
                        "address": take(row, "address", "adresse_postale"),
                        "latitude": take(row, "latitude"),
                        "longitude": take(row, "longitude"),
                        "email": take(row, "email"),
                        "phone": take(row, "phone", "telephone"),
                        "whatsapp": take(row, "whatsapp"),
                        "line_id": take(row, "line_id"),
                        "website": take(row, "website", "site_web"),
                        "facebook_url": take(row, "facebook", "facebook_url"),
                        "instagram_url": take(row, "instagram", "instagram_url"),
                        "linkedin_url": take(row, "linkedin", "linkedin_url"),
                        "line_link": take(row, "line_link", "line_url"),
                        "whatsapp_link": take(row, "whatsapp_link"),
                        "telegram_url": take(row, "telegram", "telegram_url"),
                        "wechat": take(row, "wechat"),
                        "other_contact": take(row, "other_contact"),
                        "contact_name": take(row, "contact_name", "personne_contact"),
                        "opening_hours": take(row, "horaires", "opening_hours"),
                        "membership_fee": take(row, "cout_adhesion", "membership_fee"),
                        "access_conditions": take(row, "conditions_acces", "access_conditions"),
                        "services_offered": take(row, "services_offerts", "services_offered"),
                        "description": take(row, "description"),
                        "founded_year": take(row, "annee_creation", "founded_year"),
                        "registration_number": take(row, "numero_enregistrement", "registration_number"),
                        "members_size": take(row, "taille_membres", "members_size"),
                        "regular_events": take(row, "evenements_reguliers", "regular_events"),
                        "affiliations": take(row, "partenaires_affiliations", "affiliations"),
                        "parent_org": take(row, "organisation_parente", "parent_org"),
                        "local_chapters": take(row, "chapitres_locaux", "local_chapters"),
                        "keywords": take(row, "mots_cles", "keywords"),
                        "source_urls": take(row, "source_urls", "source_url", "source_url_principale"),
                        "last_verified_date": take(row, "date_verification", "last_verified_date"),
                        "verification_method": take(row, "verification_method"),
                        "status": take(row, "status"),
                        "risk_flags": take(row, "risk_flags"),
                        "quality_score": take(row, "quality_score"),
                        "firm_name": take(row, "firm_name"),
                        "lawyer_name": take(row, "lawyer_name"),
                        "practice_areas": take(row, "practice_areas"),
                        "bar_or_license_no": take(row, "bar_or_license_no"),
                        "years_experience": take(row, "years_experience"),
                        "consultation_modes": take(row, "consultation_modes"),
                        "consultation_languages": take(row, "consultation_languages"),
                        "fee_structure": take(row, "fee_structure"),
                        "emergency_hotline": take(row, "emergency_hotline"),
                    })

                out_df = pd.DataFrame(out_rows, columns=cols)

                # Export XLSX (openpyxl) ou fallback CSV si indispo
                try:
                    with pd.ExcelWriter("export_scrapmaster_en.xlsx", engine="openpyxl") as writer:
                        out_df.to_excel(writer, index=False, sheet_name="Feuil1")
                    return send_file(
                        "export_scrapmaster_en.xlsx",
                        as_attachment=True,
                        download_name="export_scrapmaster_en.xlsx",
                    )
                except Exception:
                    csv_buf = io.StringIO()
                    out_df.to_csv(csv_buf, index=False)
                    csv_buf.seek(0)
                    return current_app.response_class(
                        csv_buf.getvalue(),
                        mimetype="text/csv",
                        headers={
                            "Content-Disposition": "attachment; filename=export_scrapmaster_en.csv"
                        },
                    )
            finally:
                conn.close()

        # ===== Export FR (CSV) : trame actuelle enrichie =====
        try:
            import pandas as pd  # type: ignore
            if df is None:
                raise RuntimeError("pandas non disponible")

            excel_columns = [
                "id","nom_organisation","statut_juridique","categorie_principale","sous_categories",
                "langues","public_cible","zone_couverte","adresse_postale","telephone","email","site_web",
                "facebook","instagram","linkedin","line_id_ou_lien","whatsapp","telegram","wechat","autre_contact",
                "personne_contact","horaires","cout_adhesion","conditions_acces","services_offerts",
                "description","annee_creation","numero_enregistrement","taille_membres","evenements_reguliers",
                "partenaires_affiliations","organisation_parente","chapitres_locaux","mots_cles",
                "source_url_principale","sources_secondaires","date_verification","fiabilite","commentaire_notes"
            ]

            # Mapping DB -> colonnes FR usuelles
            column_mapping = {
                "name": "nom_organisation",
                "category": "categorie_principale",
                "website": "site_web",
                "phone": "telephone",
                "language": "langues",
                "country": "zone_couverte",
                "source_url": "source_url_principale",
                "line_id": "line_id_ou_lien",
                "whatsapp": "whatsapp",
                "telegram": "telegram",
                "wechat": "wechat",
                "other_contact": "autre_contact",
                "contact_name": "personne_contact",
                "address": "adresse_postale",
            }

            # Créer les colonnes cibles si absentes en copiant depuis la DB
            for db_col, excel_col in column_mapping.items():
                if db_col in df.columns and excel_col not in df.columns:
                    df[excel_col] = df[db_col]

            # S'assurer que toutes les colonnes de sortie existent
            for col in excel_columns:
                if col not in df.columns:
                    df[col] = ""

            df = df[excel_columns]

            output = io.StringIO()
            df.to_csv(output, index=False, encoding="utf-8")
            output.seek(0)

        except Exception:
            # Fallback sans pandas
            cur = conn.cursor()
            cur.execute(
                """
                SELECT r.*, p.name as project_name, p.profession
                FROM results r
                LEFT JOIN projects p ON r.project_id = p.id
                ORDER BY r.id DESC
                LIMIT 10000
                """
            )
            rows = cur.fetchall()

            output = io.StringIO()
            if rows:
                # Entêtes FR enrichies
                excel_columns = [
                    "id","nom_organisation","statut_juridique","categorie_principale","sous_categories",
                    "langues","public_cible","zone_couverte","adresse_postale","telephone","email","site_web",
                    "facebook","instagram","linkedin","line_id_ou_lien","whatsapp","telegram","wechat","autre_contact",
                    "personne_contact","horaires","cout_adhesion","conditions_acces","services_offerts",
                    "description","annee_creation","numero_enregistrement","taille_membres","evenements_reguliers",
                    "partenaires_affiliations","organisation_parente","chapitres_locaux","mots_cles",
                    "source_url_principale","sources_secondaires","date_verification","fiabilite","commentaire_notes"
                ]
                output.write(",".join(f'"{h}"' for h in excel_columns) + "\n")

                headers = [desc[0] for desc in cur.description]
                # DB -> FR
                col_mapping = {
                    "name": "nom_organisation",
                    "category": "categorie_principale",
                    "website": "site_web",
                    "phone": "telephone",
                    "language": "langues",
                    "country": "zone_couverte",
                    "source_url": "source_url_principale",
                    "line_id": "line_id_ou_lien",
                    "whatsapp": "whatsapp",
                    "telegram": "telegram",
                    "wechat": "wechat",
                    "other_contact": "autre_contact",
                    "contact_name": "personne_contact",
                    "address": "adresse_postale",
                }

                for row in rows:
                    row_dict = dict(zip(headers, row))
                    csv_row = []
                    for col in excel_columns:
                        value = ""
                        if col in row_dict:
                            value = str(row_dict[col] or "")
                        else:
                            for db_col, excel_col in col_mapping.items():
                                if excel_col == col and db_col in row_dict:
                                    value = str(row_dict[db_col] or "")
                                    break
                        value = value.replace('"', '""')
                        csv_row.append(f'"{value}"')
                    output.write(",".join(csv_row) + "\n")

        finally:
            try:
                conn.close()
            except Exception:
                pass

    except Exception as e:
        # Dernier filet de sécurité : export brut
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
        finally:
            try:
                conn.close()
            except Exception:
                pass

    # Réponse FR (CSV) par défaut
    return current_app.response_class(
        output.getvalue(),
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=export_scrapmaster.csv"},
    )


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
