# APPLICATION PRINCIPALE - PLATEFORME SCRAPMASTER
# Nom du fichier : app.py

from flask import Flask, render_template, request, jsonify, send_file
import sqlite3
import json
import os
from datetime import datetime
import pandas as pd
import threading
import traceback

# Import conditionnel du moteur de scraping
try:
    from scraper_engine import ScrapingEngine
except ImportError as e:
    print(f"⚠️ Erreur import ScrapingEngine: {e}")
    ScrapingEngine = None

# Import du blueprint Studio
try:
    from ui_blueprint import bp as studio_bp
except ImportError as e:
    print(f"⚠️ Blueprint Studio non disponible: {e}")
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


class ScrapMasterApp:
    def __init__(self):
        self.init_database()
        self.scraping_engine = None
        if ScrapingEngine:
            try:
                self.scraping_engine = ScrapingEngine()
                print("✅ Moteur de scraping initialisé")
            except Exception as e:
                print(f"⚠️ Erreur initialisation moteur: {e}")
        else:
            print("⚠️ Moteur de scraping non disponible")
    
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

            # --- Migration douce : ajouter des colonnes si manquantes ---
            def add_column_if_missing(cursor, table, column, coltype):
                cursor.execute(f"PRAGMA table_info({table})")
                cols = [r[1] for r in cursor.fetchall()]
                if column not in cols:
                    try:
                        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coltype}")
                    except Exception:
                        pass  # déjà là ou autre

            # ►► Colonnes "compteurs & timings" au niveau PROJETS (historique)
            add_column_if_missing(cursor, "projects", "emails_count",   "INTEGER DEFAULT 0")
            add_column_if_missing(cursor, "projects", "phones_count",   "INTEGER DEFAULT 0")
            add_column_if_missing(cursor, "projects", "whatsapp_count", "INTEGER DEFAULT 0")
            add_column_if_missing(cursor, "projects", "line_id_count",  "INTEGER DEFAULT 0")
            add_column_if_missing(cursor, "projects", "telegram_count", "INTEGER DEFAULT 0")
            add_column_if_missing(cursor, "projects", "wechat_count",   "INTEGER DEFAULT 0")
            add_column_if_missing(cursor, "projects", "started_at",     "TIMESTAMP")
            add_column_if_missing(cursor, "projects", "finished_at",    "TIMESTAMP")
            add_column_if_missing(cursor, "projects", "run_ms",         "INTEGER")
            # ◄◄

            # Colonnes étendues pour stocker plus d'infos de contact (RESULTS)
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
            # Métiers par défaut
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
            
            # Pays/zones par défaut
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
            
            # Langues par défaut
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


@app.route('/')
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


@app.route('/start_scraping/<int:project_id>', methods=['POST'])
def start_scraping(project_id):
    """Lance le scraping d'un projet (thread séparé)"""
    if not scrap_master.scraping_engine:
        return jsonify({'success': False, 'error': 'Moteur de scraping non disponible'}), 500
    
    def run_scraping():
        conn = None
        try:
            conn = sqlite3.connect(DATABASE)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Marquer comme en cours
            cursor.execute(
                'UPDATE projects SET status = "running", last_run = ? WHERE id = ?', 
                (datetime.now(), project_id)
            )
            conn.commit()
            
            # Récupérer les infos du projet
            cursor.execute('SELECT * FROM projects WHERE id = ?', (project_id,))
            project_row = cursor.fetchone()
            if not project_row:
                raise Exception(f"Projet {project_id} introuvable")

            # Adapter la Row en dict + décoder sources JSON
            project_cfg = dict(project_row)
            try:
                project_cfg["sources"] = json.loads(project_cfg.get("sources") or "[]")
            except Exception:
                project_cfg["sources"] = []
            project_cfg["keep_incomplete"] = True

            # Lancer le scraping via le moteur
            results = scrap_master.scraping_engine.run_scraping(project_cfg) or []

            # Sauvegarder les résultats
            saved_count = 0
            for r in results:
                try:
                    try:
                        # Tentative : insert avec colonnes étendues (incl. telegram & wechat)
                        cursor.execute("""
                            INSERT INTO results (
                                project_id, name, category, description, website,
                                email, phone, city, country, language, source_url,
                                facebook, instagram, linkedin, line_id, whatsapp, telegram, wechat,
                                other_contact, contact_name, province, address,
                                latitude, longitude, raw_json, scraped_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        """, (
                            project_id,
                            (r.get('name') or '')[:500],
                            r.get('category'),
                            (r.get('description') or '')[:2000],
                            r.get('website'),
                            r.get('email'),
                            r.get('phone'),
                            r.get('city'),
                            r.get('country'),
                            r.get('language'),
                            r.get('source_url'),
                            r.get('facebook'),
                            r.get('instagram'),
                            r.get('linkedin'),
                            r.get('line_id'),
                            r.get('whatsapp'),
                            r.get('telegram'),
                            r.get('wechat'),
                            r.get('other_contact'),
                            r.get('contact_name'),
                            r.get('province'),
                            r.get('address'),
                            r.get('latitude'),
                            r.get('longitude'),
                            json.dumps(r, ensure_ascii=False)
                        ))
                    except Exception:
                        # Fallback : schéma minimal (compat ancien)
                        cursor.execute('''
                            INSERT INTO results (
                                project_id, name, category, description, website, 
                                email, phone, city, country, language, source_url, scraped_at
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        ''', (
                            project_id,
                            (r.get('name') or '')[:500],
                            r.get('category'),
                            (r.get('description') or '')[:2000],
                            r.get('website'),
                            r.get('email'),
                            r.get('phone'),
                            r.get('city'),
                            r.get('country'),
                            r.get('language'),
                            r.get('source_url')
                        ))
                    saved_count += 1
                except Exception as e:
                    print(f"⚠️ Erreur sauvegarde résultat: {e}")
            
            # Mettre à jour le statut
            cursor.execute(
                'UPDATE projects SET status = "completed", total_results = ? WHERE id = ?',
                (saved_count, project_id)
            )
            conn.commit()
            print(f"✅ Scraping terminé: {saved_count} résultats sauvés")
            
        except Exception as e:
            print(f"❌ Erreur scraping projet {project_id}: {e}")
            try:
                if conn:
                    cursor = conn.cursor()
                    cursor.execute('UPDATE projects SET status = "error" WHERE id = ?', (project_id,))
                    conn.commit()
            except Exception:
                pass
        finally:
            if conn:
                conn.close()
    
    try:
        # Lancer en arrière-plan
        thread = threading.Thread(target=run_scraping, daemon=True)
        thread.start()
        return jsonify({'success': True, 'message': 'Scraping démarré'})
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
        
        # Info du projet
        cursor.execute(
            'SELECT name, profession, country, language, total_results FROM projects WHERE id = ?', 
            (project_id,)
        )
        project = cursor.fetchone()
        
        if not project:
            conn.close()
            return render_template('results.html', project=None, results=[], error="Projet introuvable")
        
        # Résultats
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
        
        # Récupérer les données
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
            # Fallback sans pandas
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
            
            # Export CSV simple
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

        # ►► Imposer un ordre de colonnes stable pour le livrable XLSX
        COLS = [
            "project_name", "profession", "project_country", "project_language",
            "name", "category", "description",
            "website", "email", "phone", "whatsapp", "line_id", "telegram", "wechat",
            "facebook", "instagram", "linkedin",
            "address", "city", "province", "postal_code", "source_url", "scraped_at"
        ]
        for c in COLS:
            if c not in df.columns:
                df[c] = ""
        df = df[COLS]
        # ◄◄

        # Créer le fichier Excel avec pandas
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
        
        # Récupérer toutes les données configurables
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
        
        # Stats par profession
        cursor.execute('''
        SELECT profession, COUNT(*) as count, SUM(total_results) as total_results
        FROM projects 
        GROUP BY profession
        ORDER BY total_results DESC
        ''')
        prof_stats = cursor.fetchall()
        
        # Stats par pays
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


# === Endpoint optionnel pour l'UI Studio : résultats récents ===
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
    
    # Lancer l'app
    app.run(debug=True, host='127.0.0.1', port=5000)
