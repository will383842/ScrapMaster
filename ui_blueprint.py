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
    Exécute une tâche de scraping en s'alignant parfaitement sur le moteur existant.
    payload = {types:[], countries:[], languages:[], keywords:str}
    """
    JOBS[job_id] = {
        "status": "running",
        "started_at": datetime.utcnow().isoformat() + "Z",
        "progress": 0,
        "log": []
    }
    
    try:
        # Import différé pour éviter les circulaires - Correction critique
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

        JOBS[job_id]["log"].append(f"🎯 Paramètres: types={types}, countries={countries}, languages={languages}")

        # Gestion du "toutes les langues" - Correction
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
        total_combos = len(countries) * len(types) * len(languages)
        processed = 0

        JOBS[job_id]["log"].append(f"📊 {total_combos} combinaisons à traiter")

        # Boucle principale : une combinaison = un projet
        for country in countries:
            for profession in types:
                for language in languages:
                    
                    JOBS[job_id]["log"].append(f"🚀 Démarrage: {profession} | {country} | {language}")
                    
                    conn = get_db()
                    cur = conn.cursor()
                    project_id = None

                    try:
                        # 1. Créer un projet temporaire
                        project_name = f"Studio-{profession}-{country}-{language}-{datetime.utcnow().strftime('%H%M%S')}"
                        
                        cur.execute("""
                            INSERT INTO projects (name, profession, country, language, sources, status, created_at)
                            VALUES (?, ?, ?, ?, ?, 'running', CURRENT_TIMESTAMP)
                        """, (
                            project_name,
                            profession,
                            country, 
                            language,
                            json.dumps({"keywords": keywords}, ensure_ascii=False)
                        ))
                        
                        project_id = cur.lastrowid
                        conn.commit()

                        # 2. Récupérer la ligne complète (format tuple attendu par le moteur)
                        cur.execute("SELECT * FROM projects WHERE id = ?", (project_id,))
                        project_row = cur.fetchone()
                        
                        if not project_row:
                            raise Exception("Projet non créé en base")

                        JOBS[job_id]["log"].append(f"📋 Projet #{project_id} créé")

                        # 3. Appeler le moteur avec l'interface existante (tuple)
                        # Le moteur attend: (id, name, profession, country, language, sources, status, created_at, last_run, total_results)
                        results = engine.run_scraping(project_row) or []
                        
                        JOBS[job_id]["log"].append(f"📊 Scraping terminé: {len(results)} résultats bruts")

                        # 4. Sauvegarder les résultats avec gestion d'erreurs
                        saved_count = 0
                        for i, result in enumerate(results):
                            try:
                                cur.execute("""
                                    INSERT INTO results (
                                        project_id, name, category, description, website,
                                        email, phone, city, country, language, source_url, scraped_at
                                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                                """, (
                                    project_id,
                                    result.get('name', '')[:500],  # Limitation de taille
                                    result.get('category', ''),
                                    result.get('description', '')[:2000],
                                    result.get('website', ''),
                                    result.get('email', ''),
                                    result.get('phone', ''),
                                    result.get('city', ''),
                                    result.get('country', country),
                                    result.get('language', language),
                                    result.get('source_url', '')
                                ))
                                saved_count += 1
                            except Exception as e:
                                JOBS[job_id]["log"].append(f"⚠️ Erreur sauvegarde résultat #{i+1}: {str(e)[:100]}")

                        # 5. Finaliser le projet
                        cur.execute("""
                            UPDATE projects 
                            SET status = 'completed', total_results = ?, last_run = CURRENT_TIMESTAMP
                            WHERE id = ?
                        """, (saved_count, project_id))
                        
                        conn.commit()
                        
                        JOBS[job_id]["log"].append(f"✅ {country}/{profession}/{language}: {saved_count} entrées sauvées")
                        
                    except Exception as e:
                        error_msg = str(e)[:200]
                        JOBS[job_id]["log"].append(f"❌ Erreur {country}/{profession}/{language}: {error_msg}")
                        try:
                            if project_id:
                                cur.execute("UPDATE projects SET status = 'error', last_run = CURRENT_TIMESTAMP WHERE id = ?", (project_id,))
                                conn.commit()
                        except:
                            pass
                    
                    finally:
                        conn.close()

                    # Mise à jour du progrès
                    processed += 1
                    JOBS[job_id]["progress"] = int(100 * processed / total_combos)
                    
                    # Petite pause pour éviter la surcharge
                    time.sleep(0.5)

        # Finalisation
        JOBS[job_id]["status"] = "done"
        JOBS[job_id]["finished_at"] = datetime.utcnow().isoformat() + "Z"
        JOBS[job_id]["log"].append(f"🎯 Tâche terminée avec succès: {processed} combinaisons traitées")

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
        types = ["Associations", "YouTubeurs", "Avocats", "Traducteurs", "Interprètes", "Digital Nomads", "Restaurateurs", "Hôteliers"]
    
    # Countries
    try:
        cur.execute("SELECT DISTINCT name FROM countries ORDER BY name")
        countries = [r[0] for r in cur.fetchall()]
        if not countries:
            raise ValueError("empty")
    except Exception:
        countries = ["Thaïlande", "France", "Expatriés Thaïlande", "Digital Nomads Asie", "Voyageurs Asie du Sud-Est", "Royaume-Uni", "États-Unis", "Allemagne"]
    
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
                compile(content, name, 'exec')
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
        response["log"] = logs[:]   # Copie pour la réponse
        logs.clear()               # Vider pour éviter les répétitions

    return jsonify(response)

@bp.route("/api/export", methods=["GET"])
def api_export():
    """
    Exporte la table results vers un CSV mappé sur la trame Excel.
    Remplit les colonnes manquantes avec des vides et renomme les champs clés.
    """
    # Définition de la trame Excel cible
    excel_columns = [
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
        # Chargement des données avec pandas si disponible
        try:
            import pandas as pd
            df = pd.read_sql_query("""
                SELECT r.*, p.name as project_name, p.profession 
                FROM results r 
                LEFT JOIN projects p ON r.project_id = p.id 
                ORDER BY r.id DESC 
                LIMIT 10000
            """, conn)
        except ImportError:
            # Fallback sans pandas
            cur = conn.cursor()
            cur.execute("""
                SELECT r.*, p.name as project_name, p.profession 
                FROM results r 
                LEFT JOIN projects p ON r.project_id = p.id 
                ORDER BY r.id DESC 
                LIMIT 10000
            """)
            rows = cur.fetchall()
            
            # Conversion manuelle en CSV
            output = io.StringIO()
            if rows:
                # Headers
                headers = [desc[0] for desc in cur.description]
                output.write(','.join(f'"{h}"' for h in excel_columns) + '\n')
                
                # Mapping des colonnes
                col_mapping = {
                    "name": "nom_organisation",
                    "category": "categorie_principale", 
                    "website": "site_web",
                    "phone": "telephone",
                    "language": "langues",
                    "country": "zone_couverte",
                    "source_url": "source_url_principale"
                }
                
                # Données
                for row in rows:
                    row_dict = dict(zip(headers, row))
                    csv_row = []
                    for col in excel_columns:
                        # Recherche de la valeur avec mapping
                        value = ""
                        if col in row_dict:
                            value = str(row_dict[col] or "")
                        else:
                            # Recherche inverse dans le mapping
                            for db_col, excel_col in col_mapping.items():
                                if excel_col == col and db_col in row_dict:
                                    value = str(row_dict[db_col] or "")
                                    break
                        
                        # Échapper les guillemets
                        value = value.replace('"', '""')
                        csv_row.append(f'"{value}"')
                    
                    output.write(','.join(csv_row) + '\n')
            
            conn.close()
            output.seek(0)
            
            return current_app.response_class(
                output.getvalue(),
                mimetype="text/csv",
                headers={"Content-Disposition": "attachment; filename=export_scrapmaster.csv"}
            )
        
        # Version pandas
        # Renommage des colonnes pour correspondre à la trame Excel
        column_mapping = {
            "name": "nom_organisation",
            "category": "categorie_principale",
            "website": "site_web", 
            "phone": "telephone",
            "language": "langues",
            "country": "zone_couverte",
            "source_url": "source_url_principale"
        }
        
        for db_col, excel_col in column_mapping.items():
            if db_col in df.columns and excel_col not in df.columns:
                df[excel_col] = df[db_col]
        
        # Ajout des colonnes manquantes avec valeurs vides
        for col in excel_columns:
            if col not in df.columns:
                df[col] = ""
        
        # Réordonnancement selon la trame Excel
        df = df[excel_columns]
        
        # Export en CSV
        output = io.StringIO()
        df.to_csv(output, index=False, encoding='utf-8')
        output.seek(0)
        
    except Exception as e:
        # En cas d'erreur, export basique
        cur = conn.cursor() 
        cur.execute("SELECT * FROM results ORDER BY id DESC LIMIT 1000")
        rows = cur.fetchall()
        
        output = io.StringIO()
        if rows:
            headers = [desc[0] for desc in cur.description]
            output.write(','.join(f'"{h}"' for h in headers) + '\n')
            for row in rows:
                csv_row = [f'"{str(val or "").replace(chr(34), chr(34)+chr(34))}"' for val in row]
                output.write(','.join(csv_row) + '\n')
        output.seek(0)
    
    finally:
        conn.close()
    
    return current_app.response_class(
        output.getvalue(),
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=export_scrapmaster.csv"}
    )