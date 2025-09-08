# APPLICATION PRINCIPALE - PLATEFORME SCRAPMASTER AVEC INTERFACE UNIFIÉE
# Nom du fichier : app.py

from flask import Flask, render_template, request, jsonify, send_file, redirect
import sqlite3
import json
import os
from datetime import datetime
import pandas as pd
import threading
import traceback
import sys
import logging
import time
import uuid

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Imports critiques / optionnels
# -----------------------------------------------------------------------------
# Imports critiques - échec = arrêt
try:
    from scraper_engine import ScrapingEngine
except ImportError as e:
    logger.critical("Composant critique manquant: ScrapingEngine", exc_info=True)
    print("❌ ERREUR FATALE: ScrapingEngine introuvable")
    print("Vérifiez que scraper_engine.py existe et est valide")
    sys.exit(1)

# Imports optionnels - échec = warning
try:
    from ui_blueprint import bp as studio_bp
except ImportError as e:
    logger.warning("Studio UI non disponible", exc_info=True)
    studio_bp = None

# === Base paths (compatibles Windows/Linux) ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE = os.path.join(BASE_DIR, "database", "scrapmaster.db")
EXPORT_FOLDER = os.path.join(BASE_DIR, "exports")

app = Flask(__name__)
app.secret_key = 'scrapmaster_secret_key_2025'

# Rendre les chemins visibles au blueprint Studio
os.environ.setdefault("SCRAPMASTER_DB", DATABASE)
os.environ.setdefault("SCRAPMASTER_SCRAPERS", os.path.join(BASE_DIR, "scrapers"))

# Enregistrer le blueprint Studio s'il est disponible
if studio_bp:
    app.register_blueprint(studio_bp)
    print("✅ Blueprint Studio enregistré sur /studio")
else:
    print("⚠️ Blueprint Studio non disponible")

# -----------------------------------------------------------------------------
# SYSTÈME DE STATUT TEMPS RÉEL (mémoire)
# -----------------------------------------------------------------------------
project_status = {}
status_lock = threading.Lock()

def update_project_progress(project_id, progress, step, message=None):
    """(EXISTANT) Met à jour le statut d'un projet en cours pour l'endpoint /start_scraping."""
    with status_lock:
        if project_id not in project_status:
            project_status[project_id] = {
                'progress': 0,
                'current_step': 'Initialisation…',
                'log_messages': [],
                'started_at': datetime.now()
            }
        project_status[project_id]['progress'] = int(progress)
        project_status[project_id]['current_step'] = step
        if message:
            project_status[project_id]['log_messages'].append({
                'timestamp': datetime.now().isoformat(),
                'message': message
            })
            if len(project_status[project_id]['log_messages']) > 50:
                project_status[project_id]['log_messages'] = project_status[project_id]['log_messages'][-50:]

# ✅ Wrapper demandé (cap à 99, message optionnel)
def update_project_live(pid: int, progress: int = None, step: str = None, message: str | None = None):
    with status_lock:
        st = project_status.get(pid) or {}
        if "started_at" not in st:
            st["started_at"] = datetime.now()
        if progress is not None:
            st["progress"] = max(0, min(99, int(progress)))
        if step is not None:
            st["current_step"] = step
        if message:
            logs = st.get("log_messages", [])
            logs.append({"timestamp": datetime.now().isoformat(), "message": message})
            st["log_messages"] = logs[-50:]
        project_status[pid] = st

# -----------------------------------------------------------------------------
# Helpers sûrs
# -----------------------------------------------------------------------------
def add_column_if_missing(cursor, table: str, column: str, coltype: str):
    """
    Ajoute une colonne si manquante avec whitelist STRICTE (évite SQL injection).
    """
    # Whitelist stricte
    SAFE_TABLES = frozenset(['projects', 'results', 'professions', 'countries', 'languages'])
    SAFE_COLUMNS = frozenset([
        # projets
        'emails_count', 'phones_count', 'whatsapp_count', 'line_id_count',
        'telegram_count', 'wechat_count', 'started_at', 'finished_at', 'run_ms',
        # results
        'facebook', 'instagram', 'linkedin', 'line_id', 'whatsapp', 'telegram',
        'wechat', 'other_contact', 'contact_name', 'province', 'address',
        'latitude', 'longitude', 'raw_json'
    ])

    if table not in SAFE_TABLES:
        raise ValueError(f"Table interdite: {table}")
    if column not in SAFE_COLUMNS:
        raise ValueError(f"Colonne interdite: {column}")

    cursor.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cursor.fetchall()]
    if column not in cols:
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coltype}")

def insert_result_safe(cursor, project_id: int, result_data: dict):
    """
    Insertion de résultat via dictionnaire nommé (ordre de colonnes robuste).
    """
    columns = {
        'project_id': project_id,
        'name': (result_data.get('name') or '')[:500],
        'category': result_data.get('category'),
        'description': (result_data.get('description') or '')[:2000],
        'website': result_data.get('website'),
        'email': result_data.get('email'),
        'phone': result_data.get('phone'),
        'city': result_data.get('city'),
        'country': result_data.get('country'),
        'language': result_data.get('language'),
        'source_url': result_data.get('source_url'),
        'facebook': result_data.get('facebook'),
        'instagram': result_data.get('instagram'),
        'linkedin': result_data.get('linkedin'),
        'line_id': result_data.get('line_id'),
        'whatsapp': result_data.get('whatsapp'),
        'telegram': result_data.get('telegram'),
        'wechat': result_data.get('wechat'),
        'other_contact': result_data.get('other_contact'),
        'contact_name': result_data.get('contact_name'),
        'province': result_data.get('province'),
        'address': result_data.get('address'),
        'latitude': result_data.get('latitude'),
        'longitude': result_data.get('longitude'),
        'raw_json': json.dumps(result_data, ensure_ascii=False)
    }

    col_names = ', '.join(columns.keys())
    placeholders = ', '.join('?' * len(columns))

    cursor.execute(f"""
        INSERT INTO results ({col_names}, scraped_at)
        VALUES ({placeholders}, CURRENT_TIMESTAMP)
    """, tuple(columns.values()))

class ScrapMasterApp:
    def __init__(self):
        self.init_database()
        self.scraping_engine = None
        try:
            self.scraping_engine = ScrapingEngine()
            print("✅ Moteur de scraping initialisé")
        except Exception as e:
            print(f"⚠️ Erreur initialisation moteur: {e}")

    def init_database(self):
        """Initialise la base de données et les dossiers"""
        try:
            os.makedirs(os.path.join(BASE_DIR, 'database'), exist_ok=True)
            os.makedirs(EXPORT_FOLDER, exist_ok=True)

            conn = sqlite3.connect(DATABASE)
            cursor = conn.cursor()

            # Table des projets
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS projects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                profession TEXT NOT NULL,
                country TEXT NOT NULL,
                language TEXT NOT NULL,
                sources TEXT NOT NULL,
                status TEXT DEFAULT 'created',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_run TIMESTAMP,
                total_results INTEGER DEFAULT 0
            )
            ''')

            # Table des résultats (schéma de base)
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER,
                name TEXT,
                category TEXT,
                description TEXT,
                website TEXT,
                email TEXT,
                phone TEXT,
                city TEXT,
                country TEXT,
                language TEXT,
                source_url TEXT,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (project_id) REFERENCES projects (id)
            )
            ''')

            # --- Migration douce : ajouter des colonnes si manquantes (anti-SQLi) ---
            add_column_if_missing(cursor, "projects", "emails_count",   "INTEGER DEFAULT 0")
            add_column_if_missing(cursor, "projects", "phones_count",   "INTEGER DEFAULT 0")
            add_column_if_missing(cursor, "projects", "whatsapp_count", "INTEGER DEFAULT 0")
            add_column_if_missing(cursor, "projects", "line_id_count",  "INTEGER DEFAULT 0")
            add_column_if_missing(cursor, "projects", "telegram_count", "INTEGER DEFAULT 0")
            add_column_if_missing(cursor, "projects", "wechat_count",   "INTEGER DEFAULT 0")
            add_column_if_missing(cursor, "projects", "started_at",     "TIMESTAMP")
            add_column_if_missing(cursor, "projects", "finished_at",    "TIMESTAMP")
            add_column_if_missing(cursor, "projects", "run_ms",         "INTEGER")

            # Colonnes étendues pour results
            add_column_if_missing(cursor, "results", "facebook", "TEXT")
            add_column_if_missing(cursor, "results", "instagram", "TEXT")
            add_column_if_missing(cursor, "results", "linkedin", "TEXT")
            add_column_if_missing(cursor, "results", "line_id", "TEXT")
            add_column_if_missing(cursor, "results", "whatsapp", "TEXT")
            add_column_if_missing(cursor, "results", "telegram", "TEXT")
            add_column_if_missing(cursor, "results", "wechat", "TEXT")
            add_column_if_missing(cursor, "results", "other_contact", "TEXT")
            add_column_if_missing(cursor, "results", "contact_name", "TEXT")
            add_column_if_missing(cursor, "results", "province", "TEXT")
            add_column_if_missing(cursor, "results", "address", "TEXT")
            add_column_if_missing(cursor, "results", "latitude", "TEXT")
            add_column_if_missing(cursor, "results", "longitude", "TEXT")
            add_column_if_missing(cursor, "results", "raw_json", "TEXT")

            # Table des métiers
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS professions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                description TEXT,
                scraper_template TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')

            # Table des pays/zones
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS countries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                description TEXT,
                sources TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')

            # Table des langues
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS languages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')

            # Données par défaut
            self.insert_default_data(cursor)

            conn.commit()
            conn.close()
            print("✅ Base de données initialisée")

        except Exception as e:
            print(f"❌ Erreur initialisation base de données: {e}")
            print(traceback.format_exc())

    def insert_default_data(self, cursor):
        """Insère les données par défaut"""
        try:
            professions = [
                ('YouTubeurs', 'Créateurs de contenu YouTube', 'youtube_scraper'),
                ('Avocats', 'Professionnels du droit', 'lawyer_scraper'),
                ('Associations', 'Organisations à but non lucratif', 'association_scraper'),
                ('Traducteurs', 'Professionnels de la traduction', 'translator_scraper'),
                ('Interprètes', "Professionnels de l'interprétation", 'translator_scraper'),
                ('Digital Nomads', 'Travailleurs nomades numériques', 'nomad_scraper'),
                ('Restaurateurs', 'Propriétaires de restaurants', 'restaurant_scraper'),
                ('Hôteliers', "Professionnels de l'hôtellerie", 'hotel_scraper')
            ]
            for prof in professions:
                cursor.execute(
                    'INSERT OR IGNORE INTO professions (name, description, scraper_template) VALUES (?, ?, ?)', prof
                )

            countries = [
                ('Thaïlande', 'Royaume de Thaïlande', 'thailand_sources.json'),
                ('France', 'République française', 'france_sources.json'),
                ("Expatriés Thaïlande", "Communauté d'expatriés en Thaïlande", 'expat_thailand_sources.json'),
                ('Digital Nomads Asie', 'Nomades numériques en Asie', 'nomads_asia_sources.json'),
                ('Voyageurs Asie du Sud-Est', 'Voyageurs en Asie du Sud-Est', 'travelers_sea_sources.json'),
                ('Royaume-Uni', 'Royaume-Uni de Grande-Bretagne', 'uk_sources.json'),
                ('États-Unis', "États-Unis d'Amérique", 'usa_sources.json'),
                ('Allemagne', "République fédérale d'Allemagne", 'germany_sources.json')
            ]
            for country in countries:
                cursor.execute(
                    'INSERT OR IGNORE INTO countries (name, description, sources) VALUES (?, ?, ?)', country
                )

            languages = [
                ('fr', 'Français', 'Langue française'),
                ('en', 'Anglais', 'Langue anglaise'),
                ('th', 'Thaï', 'Langue thaïlandaise'),
                ('de', 'Allemand', 'Langue allemande'),
                ('es', 'Espagnol', 'Langue espagnole'),
                ('it', 'Italien', 'Langue italienne'),
                ('ru', 'Russe', 'Langue russe'),
                ('zh', 'Chinois', 'Langue chinoise'),
                ('ja', 'Japonais', 'Langue japonaise'),
                ('ko', 'Coréen', 'Langue coréenne'),
                ('pt', 'Portugais', 'Langue portugaise'),
                ('ar', 'Arabe', 'Langue arabe'),
                ('hi', 'Hindi', 'Langue hindi')
            ]
            for lang in languages:
                cursor.execute(
                    'INSERT OR IGNORE INTO languages (code, name, description) VALUES (?, ?, ?)', lang
                )

            print("✅ Données par défaut insérées")

        except Exception as e:
            print(f"⚠️ Erreur insertion données par défaut: {e}")

# Instance globale
scrap_master = ScrapMasterApp()

# -----------------------------------------------------------------------------
# NOUVELLE IMPLÉMENTATION : lancer le scraping DIRECTEMENT (pas d’auto-appel HTTP)
# -----------------------------------------------------------------------------
def real_start_scraping(project_id: int):
    """Exécute le moteur de scraping et finalise la base, quoi qu'il arrive."""
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    try:
        # Marquer "running" dès le départ
        cursor.execute(
            'UPDATE projects SET status = "running", started_at = ? WHERE id = ?',
            (datetime.now(), project_id)
        )
        conn.commit()
        update_project_live(project_id, 5, "Initialisation…", "Projet marqué comme en cours")

        # Charger la config du projet
        cursor.execute('SELECT * FROM projects WHERE id = ?', (project_id,))
        row = cursor.fetchone()
        if not row:
            update_project_live(project_id, 99, "Projet introuvable")
            return

        # Construire la config attendue par le moteur
        project_cfg = dict(row)
        try:
            project_cfg["sources"] = json.loads(project_cfg.get("sources") or "[]")
        except Exception:
            project_cfg["sources"] = []
        project_cfg["keep_incomplete"] = True

        update_project_live(project_id, 15, "Collecte des sources…")

        # Lancer le scraping
        try:
            update_project_live(project_id, 30, "Scraping en cours…", "Lancement du moteur")
            results = scrap_master.scraping_engine.run_scraping(project_cfg) or []
        except Exception:
            traceback.print_exc()
            results = []

        update_project_live(project_id, 70, "Sauvegarde des résultats…", f"{len(results)} items à insérer")

        # Sauvegarder les résultats
        saved = 0
        for i, r in enumerate(results):
            try:
                insert_result_safe(cursor, project_id, r)
                saved += 1
                if saved % 10 == 0 and len(results) > 0:
                    p = 70 + int((saved / max(1, len(results))) * 20)
                    update_project_live(project_id, p, "Sauvegarde…", f"{saved}/{len(results)} enregistrés")
            except Exception:
                traceback.print_exc()
                continue
        conn.commit()

        # Statistiques (blindées)
        update_project_live(project_id, 95, "Finalisation…", "Calcul des statistiques")
        try:
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as emails,
                    SUM(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) as phones,
                    SUM(CASE WHEN whatsapp IS NOT NULL AND whatsapp != '' THEN 1 ELSE 0 END) as whatsapp
                FROM results WHERE project_id = ?
            """, (project_id,))
            stats = cursor.fetchone() or (0, 0, 0, 0)
            # sqlite3.Row -> peut être indexé
            total = stats[0] if isinstance(stats, tuple) else stats["total"]
            emails = stats[1] if isinstance(stats, tuple) else stats["emails"]
            phones = stats[2] if isinstance(stats, tuple) else stats["phones"]
            whatsapp = stats[3] if isinstance(stats, tuple) else stats["whatsapp"]
        except Exception:
            traceback.print_exc()
            total, emails, phones, whatsapp = 0, 0, 0, 0

        # Marquer terminé (PAS de colonne progress ici)
        cursor.execute(
            '''UPDATE projects SET 
                   status = "completed", 
                   total_results = ?, 
                   emails_count = ?,
                   phones_count = ?,
                   whatsapp_count = ?,
                   finished_at = ?
               WHERE id = ?''',
            (total, emails, phones, whatsapp, datetime.now(), project_id)
        )
        conn.commit()
        update_project_live(project_id, 99, "Finalisation…", f"✅ Terminé: {total} résultats")

    except Exception as e:
        traceback.print_exc()
        # En cas d'erreur : marquer error mais essayer de pousser à la fin
        try:
            cursor.execute('UPDATE projects SET status = "error", finished_at = ? WHERE id = ?', 
                           (datetime.now(), project_id))
            conn.commit()
        except Exception:
            pass
        update_project_live(project_id, 0, "Erreur", str(e))
    finally:
        try:
            conn.close()
        except Exception:
            pass
        # Nettoyage différé du live status
        def cleanup_status():
            time.sleep(300)
            with status_lock:
                if project_id in project_status:
                    del project_status[project_id]
        threading.Thread(target=cleanup_status, daemon=True).start()

# -----------------------------------------------------------------------------
# ROUTES PRINCIPALES (NOUVELLE INTERFACE UNIFIÉE)
# -----------------------------------------------------------------------------

@app.route('/')
def index():
    """Interface principale - Redirection vers l'interface simplifiée"""
    return redirect('/simple')

@app.route('/simple')
def simple_interface():
    """Interface unifiée simplifiée et ludique"""
    return render_template('simple.html')

@app.route('/api/quick_search', methods=['POST'])
def quick_search():
    """API simplifiée pour lancement rapide depuis l'interface unifiée"""
    try:
        data = request.json
        
        # Validation simplifiée
        required = ['profession', 'country']
        for field in required:
            if not data.get(field):
                return jsonify({'success': False, 'error': f'Champ requis: {field}'}), 400
        
        # Création automatique du nom si absent
        project_name = data.get('projectName')
        if not project_name:
            project_name = f"{data['profession']} {data['country']} {datetime.now().strftime('%d/%m/%Y')}"
        
        # Créer le projet
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()
        
        cursor.execute('''
        INSERT INTO projects (name, profession, country, language, sources, status)
        VALUES (?, ?, ?, ?, ?, 'created')
        ''', (
            project_name,
            data['profession'],
            data['country'],
            data.get('language', 'fr'),
            json.dumps({'keywords': data.get('keywords', '')})
        ))
        
        project_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        # ✅ Lancer immédiatement le scraping réel dans un thread Python
        threading.Thread(target=real_start_scraping, args=(project_id,), daemon=True).start()
        
        return jsonify({
            'success': True, 
            'project_id': project_id,
            'message': 'Recherche lancée avec le moteur réel'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/search_status/<int:project_id>')
def search_status(project_id):
    """Statut pour l'interface unifiée — plus de cap à 90 %, on utilise le live + fallback 45s→99%."""
    try:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()
        cursor.execute('SELECT status, started_at, name, total_results, emails_count, phones_count FROM projects WHERE id = ?', (project_id,))
        row = cursor.fetchone()
        conn.close()

        if not row:
            return jsonify({'error': 'Project not found'}), 404

        status, started_at, name, total_results, emails_count, phones_count = row

        # Terminé → 100 %
        if status == "completed":
            return jsonify({
                'status': 'completed',
                'progress': 100,
                'current_step': 'Terminé',
                'total_results': total_results or 0,
                'emails_count': emails_count or 0,
                'phones_count': phones_count or 0,
                'name': name
            })

        # Live
        with status_lock:
            live = project_status.get(project_id, {})

        progress = int(live.get('progress', 0))
        current_step = live.get('current_step', 'Initialisation…')

        # Fallback temporel si pas de live (ou très bas)
        if progress < 5:
            try:
                if isinstance(started_at, str):
                    # sqlite peut renvoyer str
                    start_dt = datetime.fromisoformat(started_at.replace('Z', '+00:00') if 'Z' in started_at else started_at)
                else:
                    start_dt = started_at or datetime.now()
            except Exception:
                start_dt = datetime.now()
            elapsed = (datetime.now() - start_dt).total_seconds()
            progress = min(99, int(elapsed / 45 * 100))
            if progress >= 99:
                current_step = "Finalisation…"

        return jsonify({
            'status': 'running',
            'progress': progress,
            'current_step': current_step,
            'total_results': total_results or 0,
            'emails_count': emails_count or 0,
            'phones_count': phones_count or 0,
            'name': name
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

# -----------------------------------------------------------------------------
# ROUTES EXISTANTES (COMPATIBILITÉ) — inchangées
# -----------------------------------------------------------------------------

@app.route('/dashboard')
def dashboard():
    """Dashboard principal avec gestion d'erreur"""
    try:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()

        # Statistiques générales
        cursor.execute('SELECT COUNT(*) FROM projects')
        total_projects = cursor.fetchone()[0]

        cursor.execute('SELECT COUNT(*) FROM results')
        total_results = cursor.fetchone()[0]

        cursor.execute('SELECT COUNT(*) FROM projects WHERE status = "running"')
        active_projects = cursor.fetchone()[0]

        # Projets récents
        cursor.execute('''
        SELECT name, profession, country, language, status, total_results, created_at 
        FROM projects 
        ORDER BY created_at DESC 
        LIMIT 10
        ''')
        recent_projects = cursor.fetchall()

        conn.close()

        return render_template(
            'dashboard.html',
            total_projects=total_projects,
            total_results=total_results,
            active_projects=active_projects,
            recent_projects=recent_projects
        )
    except Exception as e:
        print(f"❌ Erreur dashboard: {e}")
        return render_template(
            'dashboard.html',
            total_projects=0,
            total_results=0,
            active_projects=0,
            recent_projects=[],
            error=str(e)
        )

@app.route('/new_project')
def new_project():
    """Formulaire de nouveau projet"""
    try:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()

        cursor.execute('SELECT name FROM professions ORDER BY name')
        professions = [row[0] for row in cursor.fetchall()]

        cursor.execute('SELECT name FROM countries ORDER BY name')
        countries = [row[0] for row in cursor.fetchall()]

        cursor.execute('SELECT code, name FROM languages ORDER BY name')
        languages = cursor.fetchall()

        conn.close()

        return render_template(
            'new_project.html',
            professions=professions,
            countries=countries,
            languages=languages
        )
    except Exception as e:
        print(f"❌ Erreur new_project: {e}")
        return render_template(
            'new_project.html',
            professions=[],
            countries=[],
            languages=[],
            error=str(e)
        )

@app.route('/create_project', methods=['POST'])
def create_project():
    """Crée un nouveau projet"""
    try:
        data = request.json

        # Validation des données requises
        required_fields = ['name', 'profession', 'country', 'language']
        for field in required_fields:
            if not data.get(field):
                return jsonify({'success': False, 'error': f'Champ requis manquant: {field}'}), 400

        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()

        cursor.execute('''
        INSERT INTO projects (name, profession, country, language, sources)
        VALUES (?, ?, ?, ?, ?)
        ''', (
            data['name'][:500],  # Limiter la taille
            data['profession'],
            data['country'],
            data['language'],
            json.dumps(data.get('sources', []), ensure_ascii=False)
        ))

        project_id = cursor.lastrowid
        conn.commit()
        conn.close()

        return jsonify({'success': True, 'project_id': project_id})

    except Exception as e:
        print(f"❌ Erreur create_project: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/project_status/<int:project_id>')
def get_project_status(project_id):
    """Récupère le statut en temps réel d'un projet"""
    try:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT id, name, status, created_at, started_at, finished_at, 
               total_results, emails_count, phones_count, whatsapp_count
        FROM projects 
        WHERE id = ?
        ''', (project_id,))
        
        project = cursor.fetchone()
        if not project:
            return jsonify({'error': 'Project not found'}), 404
        
        # Statut en mémoire (pour progression temps réel)
        with status_lock:
            live_status = project_status.get(project_id, {})
        
        status_data = {
            'id': project[0],
            'name': project[1],
            'status': project[2],
            'created_at': project[3],
            'started_at': project[4],
            'finished_at': project[5],
            'total_results': project[6] or 0,
            'emails_count': project[7] or 0,
            'phones_count': project[8] or 0,
            'whatsapp_count': project[9] or 0,
            
            # Données temps réel
            'progress': live_status.get('progress', 0),
            'current_step': live_status.get('current_step', 'En attente'),
            'log_messages': live_status.get('log_messages', []),
            'estimated_completion': live_status.get('estimated_completion')
        }
        
        conn.close()
        return jsonify(status_data)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# === Endpoint de compatibilité : lance un scraping avec logs (garde-le pour usage externe) ===
@app.route('/start_scraping/<int:project_id>', methods=['POST'])
def start_scraping(project_id):
    """Lance le scraping d'un projet avec feedback temps réel (endpoint conservé, non auto-appelé)."""
    if not scrap_master.scraping_engine:
        return jsonify({'success': False, 'error': 'Moteur de scraping non disponible'}), 500

    def run_scraping_with_feedback():
        conn = None
        try:
            update_project_progress(project_id, 5, "Préparation", "Initialisation du scraping...")
            
            conn = sqlite3.connect(DATABASE)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute(
                'UPDATE projects SET status = "running", started_at = ? WHERE id = ?',
                (datetime.now(), project_id)
            )
            conn.commit()
            update_project_progress(project_id, 10, "Démarrage", "Projet marqué comme en cours")

            cursor.execute('SELECT * FROM projects WHERE id = ?', (project_id,))
            project_row = cursor.fetchone()
            if not project_row:
                raise Exception(f"Projet {project_id} introuvable")

            update_project_progress(project_id, 20, "Configuration", "Configuration du projet récupérée")

            project_cfg = dict(project_row)
            try:
                project_cfg["sources"] = json.loads(project_cfg.get("sources") or "[]")
            except Exception:
                project_cfg["sources"] = []
            project_cfg["keep_incomplete"] = True

            update_project_progress(project_id, 30, "Recherche", "Lancement du moteur de scraping...")

            results = scrap_master.scraping_engine.run_scraping(project_cfg) or []
            
            update_project_progress(project_id, 70, "Traitement", f"{len(results)} résultats trouvés, sauvegarde en cours...")

            saved_count = 0
            for i, r in enumerate(results):
                try:
                    insert_result_safe(cursor, project_id, r)
                    saved_count += 1
                    if i % 10 == 0 and len(results) > 0:
                        progress = 70 + (i / len(results)) * 20
                        update_project_progress(project_id, progress, "Sauvegarde", f"{saved_count}/{len(results)} résultats sauvegardés")
                except Exception as e:
                    print(f"⚠️ Erreur sauvegarde résultat: {e}")

            update_project_progress(project_id, 95, "Finalisation", "Calcul des statistiques...")
            
            cursor.execute('''
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as emails,
                    SUM(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) as phones,
                    SUM(CASE WHEN whatsapp IS NOT NULL AND whatsapp != '' THEN 1 ELSE 0 END) as whatsapp
                FROM results WHERE project_id = ?
            ''', (project_id,))
            
            stats = cursor.fetchone()
            total, emails, phones, whatsapp = stats if stats else (0, 0, 0, 0)

            cursor.execute(
                '''UPDATE projects SET 
                   status = "completed", 
                   total_results = ?, 
                   emails_count = ?,
                   phones_count = ?,
                   whatsapp_count = ?,
                   finished_at = ?
                   WHERE id = ?''',
                (total, emails, phones, whatsapp, datetime.now(), project_id)
            )
            conn.commit()
            
            update_project_progress(project_id, 100, "Terminé", 
                f"✅ Scraping terminé: {total} résultats ({emails} emails, {phones} téléphones)")
            
            print(f"✅ Scraping terminé: {saved_count} résultats sauvés")

        except Exception as e:
            print(f"❌ Erreur scraping projet {project_id}: {e}")
            update_project_progress(project_id, 0, "Erreur", f"❌ Erreur: {str(e)}")
            try:
                if conn:
                    cursor = conn.cursor()
                    cursor.execute('UPDATE projects SET status = "error", finished_at = ? WHERE id = ?', 
                                   (datetime.now(), project_id))
                    conn.commit()
            except Exception:
                pass
        finally:
            if conn:
                conn.close()
            def cleanup_status():
                time.sleep(300)
                with status_lock:
                    if project_id in project_status:
                        del project_status[project_id]
            threading.Thread(target=cleanup_status, daemon=True).start()

    try:
        thread = threading.Thread(target=run_scraping_with_feedback, daemon=True)
        thread.start()
        return jsonify({'success': True, 'message': 'Scraping démarré avec suivi temps réel'})
    except Exception as e:
        print(f"❌ Erreur lancement scraping: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/projects')
def projects():
    """Liste tous les projets"""
    try:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()

        cursor.execute('''
        SELECT id, name, profession, country, language, status, total_results, created_at, last_run
        FROM projects
        ORDER BY created_at DESC
        ''')
        all_projects = cursor.fetchall()

        conn.close()

        return render_template('projects.html', projects=all_projects)

    except Exception as e:
        print(f"❌ Erreur projects: {e}")
        return render_template('projects.html', projects=[], error=str(e))

@app.route('/results/<int:project_id>')
def results(project_id):
    """Affiche les résultats d'un projet"""
    try:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()

        cursor.execute(
            'SELECT name, profession, country, language, total_results FROM projects WHERE id = ?',
            (project_id,)
        )
        project = cursor.fetchone()

        if not project:
            conn.close()
            return render_template('results.html', project=None, results=[], error="Projet introuvable")

        cursor.execute('''
        SELECT name, category, description, website, email, phone, city, source_url, scraped_at
        FROM results
        WHERE project_id = ?
        ORDER BY scraped_at DESC
        LIMIT 1000
        ''', (project_id,))
        results_data = cursor.fetchall()

        conn.close()

        return render_template('results.html', project=project, results=results_data)

    except Exception as e:
        print(f"❌ Erreur results: {e}")
        return render_template('results.html', project=None, results=[], error=str(e))

@app.route('/export/<int:project_id>')
def export_results(project_id):
    """Exporte les résultats en Excel"""
    try:
        conn = sqlite3.connect(DATABASE)

        try:
            df = pd.read_sql_query('''
            SELECT r.*, 
                   p.name as project_name, 
                   p.profession, 
                   p.country as project_country, 
                   p.language as project_language
            FROM results r
            JOIN projects p ON r.project_id = p.id
            WHERE r.project_id = ?
            ORDER BY r.scraped_at DESC
            ''', conn, params=(project_id,))
        except ImportError:
            cursor = conn.cursor()
            cursor.execute('''
            SELECT r.*, p.name as project_name, p.profession
            FROM results r
            JOIN projects p ON r.project_id = p.id
            WHERE r.project_id = ?
            ORDER BY r.scraped_at DESC
            ''', (project_id,))

            rows = cursor.fetchall()
            headers = [description[0] for description in cursor.description]

            import csv
            import io
            from flask import Response

            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(headers)
            writer.writerows(rows)

            conn.close()

            return Response(
                output.getvalue(),
                mimetype="text/csv",
                headers={"Content-Disposition": f"attachment; filename=export_project_{project_id}.csv"}
            )

        conn.close()

        COLS = [
            "project_name", "profession", "project_country", "project_language",
            "name", "category", "description",
            "website", "email", "phone", "whatsapp", "line_id", "telegram", "wechat",
            "facebook", "instagram", "linkedin",
            "address", "city", "province", "source_url", "scraped_at"
        ]
        for c in COLS:
            if c not in df.columns:
                df[c] = ""
        df = df[COLS]

        filename = f'export_project_{project_id}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        filepath = os.path.join(EXPORT_FOLDER, filename)
        os.makedirs(EXPORT_FOLDER, exist_ok=True)

        df.to_excel(filepath, index=False)

        return send_file(filepath, as_attachment=True)

    except Exception as e:
        print(f"❌ Erreur export: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/settings')
def settings():
    """Page de paramètres"""
    try:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM professions ORDER BY name')
        professions = cursor.fetchall()

        cursor.execute('SELECT * FROM countries ORDER BY name')
        countries = cursor.fetchall()

        cursor.execute('SELECT * FROM languages ORDER BY name')
        languages = cursor.fetchall()

        conn.close()

        return render_template(
            'settings.html',
            professions=professions,
            countries=countries,
            languages=languages,
            db_path=DATABASE,
            scrapers_path=os.environ.get("SCRAPMASTER_SCRAPERS"),
            exports_path=EXPORT_FOLDER
        )

    except Exception as e:
        print(f"❌ Erreur settings: {e}")
        return render_template(
            'settings.html',
            professions=[],
            countries=[],
            languages=[],
            error=str(e)
        )

@app.route('/add_profession', methods=['POST'])
def add_profession():
    """Ajoute un nouveau métier"""
    try:
        data = request.json

        if not data.get('name'):
            return jsonify({'success': False, 'error': 'Nom requis'}), 400

        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()

        cursor.execute(
            'INSERT INTO professions (name, description, scraper_template) VALUES (?, ?, ?)',
            (data['name'][:200], data.get('description', '')[:500], data.get('template', 'generic_scraper'))
        )

        conn.commit()
        conn.close()

        return jsonify({'success': True})

    except sqlite3.IntegrityError:
        return jsonify({'success': False, 'error': 'Ce métier existe déjà'}), 400
    except Exception as e:
        print(f"❌ Erreur add_profession: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/add_country', methods=['POST'])
def add_country():
    """Ajoute un nouveau pays/zone"""
    try:
        data = request.json

        if not data.get('name'):
            return jsonify({'success': False, 'error': 'Nom requis'}), 400

        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()

        cursor.execute(
            'INSERT INTO countries (name, description, sources) VALUES (?, ?, ?)',
            (data['name'][:200], data.get('description', '')[:500], data.get('sources', ''))
        )

        conn.commit()
        conn.close()

        return jsonify({'success': True})

    except sqlite3.IntegrityError:
        return jsonify({'success': False, 'error': 'Ce pays/zone existe déjà'}), 400
    except Exception as e:
        print(f"❌ Erreur add_country: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/add_language', methods=['POST'])
def add_language():
    """Ajoute une nouvelle langue"""
    try:
        data = request.json

        if not data.get('code') or not data.get('name'):
            return jsonify({'success': False, 'error': 'Code et nom requis'}), 400

        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()

        cursor.execute(
            'INSERT INTO languages (code, name, description) VALUES (?, ?, ?)',
            (data['code'][:5].lower(), data['name'][:100], data.get('description', '')[:300])
        )

        conn.commit()
        conn.close()

        return jsonify({'success': True})

    except sqlite3.IntegrityError:
        return jsonify({'success': False, 'error': 'Ce code de langue existe déjà'}), 400
    except Exception as e:
        print(f"❌ Erreur add_language: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/stats')
def api_stats():
    """API pour les statistiques temps réel"""
    try:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()

        cursor.execute('''
        SELECT profession, COUNT(*) as count, SUM(total_results) as total_results
        FROM projects 
        GROUP BY profession
        ORDER BY total_results DESC
        ''')
        prof_stats = cursor.fetchall()

        cursor.execute('''
        SELECT country, COUNT(*) as projects, SUM(total_results) as results
        FROM projects 
        GROUP BY country
        ORDER BY results DESC
        ''')
        country_stats = cursor.fetchall()

        conn.close()

        return jsonify({
            'profession_stats': prof_stats,
            'country_stats': country_stats
        })

    except Exception as e:
        print(f"❌ Erreur api_stats: {e}")
        return jsonify({
            'profession_stats': [],
            'country_stats': [],
            'error': str(e)
        }), 500

@app.route('/api/recent_results')
def recent_results():
    """Renvoie les derniers résultats (pour l'affichage dans /studio)"""
    try:
        conn = sqlite3.connect(DATABASE)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute('''
            SELECT id, name, language, category, country, website, email, phone, source_url, scraped_at
            FROM results
            ORDER BY id DESC
            LIMIT 200
        ''')
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return jsonify({"items": rows})

    except Exception as e:
        print(f"❌ Erreur recent_results: {e}")
        return jsonify({"items": [], "error": str(e)}), 500

@app.route('/debug/<int:project_id>')
def debug_project(project_id):
    """Route de débogage pour vérifier les données"""
    try:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM projects WHERE id = ?', (project_id,))
        project = cursor.fetchone()
        
        cursor.execute('SELECT COUNT(*) FROM results WHERE project_id = ?', (project_id,))
        results_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT name, email, phone FROM results WHERE project_id = ? LIMIT 5', (project_id,))
        sample_results = cursor.fetchall()
        
        conn.close()
        
        return jsonify({
            'project_id': project_id,
            'project_exists': project is not None,
            'results_count': results_count,
            'sample_results': sample_results
        })
        
    except Exception as e:
        return jsonify({'error': str(e)})

# === Gestion d'erreurs globale ===
@app.errorhandler(404)
def page_not_found(e):
    return render_template('dashboard.html', error="Page non trouvée"), 404

@app.errorhandler(500)
def internal_server_error(e):
    return render_template('dashboard.html', error="Erreur serveur interne"), 500

if __name__ == '__main__':
    print("🚀 Démarrage ScrapMaster...")
    print(f"📂 Base de données: {DATABASE}")
    print(f"📂 Scrapers: {os.environ.get('SCRAPMASTER_SCRAPERS')}")
    print(f"📂 Exports: {EXPORT_FOLDER}")

    if studio_bp:
        print("🧰 Interface Studio disponible sur /studio")

    print("🎯 Interface principale: http://localhost:5000/")
    print("🎯 Interface simplifiée: http://localhost:5000/simple")
    print("📊 Dashboard classique: http://localhost:5000/dashboard")

    app.run(debug=True, host='127.0.0.1', port=5000)
