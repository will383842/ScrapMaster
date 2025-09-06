# APPLICATION PRINCIPALE - PLATEFORME SCRAPMASTER
# Nom du fichier : app.py

from flask import Flask, render_template, request, jsonify, send_file
import sqlite3
import json
import os
from datetime import datetime
import pandas as pd
import threading
from scraper_engine import ScrapingEngine
from ui_blueprint import bp as studio_bp  # ← Blueprint UI Studio (/studio)

# === Base paths (compatibles Windows) ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE = os.path.join(BASE_DIR, "database", "scrapmaster.db")
EXPORT_FOLDER = os.path.join(BASE_DIR, "exports")

app = Flask(__name__)
app.secret_key = 'scrapmaster_secret_key_2025'

# Rendre les chemins visibles au blueprint Studio (ui_blueprint.py)
os.environ.setdefault("SCRAPMASTER_DB", DATABASE)
os.environ.setdefault("SCRAPMASTER_SCRAPERS", os.path.join(BASE_DIR, "scrapers"))

# Enregistrer le blueprint de l'UI Studio (/studio)
app.register_blueprint(studio_bp)


class ScrapMasterApp:
    def __init__(self):
        self.init_database()
        self.scraping_engine = ScrapingEngine()
    
    def init_database(self):
        """Initialise la base de données et les dossiers"""
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
        
        # Table des résultats
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
    
    def insert_default_data(self, cursor):
        """Insère les données par défaut"""
        # Métiers par défaut
        professions = [
            ('YouTubeurs', 'Créateurs de contenu YouTube', 'youtube_scraper'),
            ('Avocats', 'Professionnels du droit', 'lawyer_scraper'),
            ('Associations', 'Organisations à but non lucratif', 'association_scraper'),
            ('Traducteurs', 'Professionnels de la traduction', 'translator_scraper'),
            ('Interprètes', "Professionnels de l'interprétation", 'interpreter_scraper'),
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
            ('Allemagne', 'République fédérale d\'Allemagne', 'germany_sources.json')
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
            ('ja', 'Japonais', 'Langue japonaise')
        ]
        for lang in languages:
            cursor.execute(
                'INSERT OR IGNORE INTO languages (code, name, description) VALUES (?, ?, ?)', lang
            )


# Instance globale
scrap_master = ScrapMasterApp()


@app.route('/')
def dashboard():
    """Dashboard principal"""
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


@app.route('/new_project')
def new_project():
    """Formulaire de nouveau projet"""
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


@app.route('/create_project', methods=['POST'])
def create_project():
    """Crée un nouveau projet"""
    data = request.json
    
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    
    cursor.execute('''
    INSERT INTO projects (name, profession, country, language, sources)
    VALUES (?, ?, ?, ?, ?)
    ''', (
        data['name'],
        data['profession'],
        data['country'], 
        data['language'],
        json.dumps(data.get('sources', []))
    ))
    
    project_id = cursor.lastrowid
    conn.commit()
    conn.close()
    
    return jsonify({'success': True, 'project_id': project_id})


@app.route('/start_scraping/<int:project_id>', methods=['POST'])
def start_scraping(project_id):
    """Lance le scraping d'un projet"""
    def run_scraping():
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()
        
        # Marquer comme en cours
        cursor.execute(
            'UPDATE projects SET status = "running", last_run = ? WHERE id = ?', 
            (datetime.now(), project_id)
        )
        conn.commit()
        
        # Récupérer les infos du projet
        cursor.execute('SELECT * FROM projects WHERE id = ?', (project_id,))
        project = cursor.fetchone()
        
        try:
            # Lancer le scraping via le moteur
            results = scrap_master.scraping_engine.run_scraping(project)
            
            # Sauvegarder les résultats
            for result in results:
                cursor.execute('''
                INSERT INTO results (
                    project_id, name, category, description, website, 
                    email, phone, city, country, language, source_url
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    project_id,
                    result.get('name'),
                    result.get('category'),
                    result.get('description'),
                    result.get('website'),
                    result.get('email'),
                    result.get('phone'),
                    result.get('city'),
                    result.get('country'),
                    result.get('language'),
                    result.get('source_url')
                ))
            
            # Mettre à jour le statut
            cursor.execute(
                'UPDATE projects SET status = "completed", total_results = ? WHERE id = ?',
                (len(results), project_id)
            )
            
        except Exception as e:
            cursor.execute('UPDATE projects SET status = "error" WHERE id = ?', (project_id,))
            print(f"Erreur scraping: {e}")
        
        conn.commit()
        conn.close()
    
    # Lancer en arrière-plan
    thread = threading.Thread(target=run_scraping, daemon=True)
    thread.start()
    
    return jsonify({'success': True, 'message': 'Scraping démarré'})


@app.route('/projects')
def projects():
    """Liste tous les projets"""
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


@app.route('/results/<int:project_id>')
def results(project_id):
    """Affiche les résultats d'un projet"""
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    
    # Info du projet
    cursor.execute(
        'SELECT name, profession, country, language, total_results FROM projects WHERE id = ?', 
        (project_id,)
    )
    project = cursor.fetchone()
    
    # Résultats
    cursor.execute('''
    SELECT name, category, description, website, email, phone, city, source_url, scraped_at
    FROM results 
    WHERE project_id = ?
    ORDER BY scraped_at DESC
    ''', (project_id,))
    results_data = cursor.fetchall()
    
    conn.close()
    
    return render_template('results.html', project=project, results=results_data)


@app.route('/export/<int:project_id>')
def export_results(project_id):
    """Exporte les résultats en Excel"""
    conn = sqlite3.connect(DATABASE)
    
    # Récupérer les données
    df = pd.read_sql_query('''
    SELECT r.*, p.name as project_name, p.profession, p.country as project_country, p.language as project_language
    FROM results r
    JOIN projects p ON r.project_id = p.id
    WHERE r.project_id = ?
    ''', conn, params=(project_id,))
    
    conn.close()
    
    # Créer le fichier Excel
    filename = f'export_project_{project_id}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
    filepath = os.path.join(EXPORT_FOLDER, filename)
    os.makedirs(EXPORT_FOLDER, exist_ok=True)
    df.to_excel(filepath, index=False)
    
    return send_file(filepath, as_attachment=True)


@app.route('/settings')
def settings():
    """Page de paramètres"""
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
        languages=languages
    )


@app.route('/add_profession', methods=['POST'])
def add_profession():
    """Ajoute un nouveau métier"""
    data = request.json
    
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    
    cursor.execute(
        'INSERT INTO professions (name, description, scraper_template) VALUES (?, ?, ?)',
        (data['name'], data['description'], data['template'])
    )
    
    conn.commit()
    conn.close()
    
    return jsonify({'success': True})


@app.route('/add_country', methods=['POST'])
def add_country():
    """Ajoute un nouveau pays/zone"""
    data = request.json
    
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    
    cursor.execute(
        'INSERT INTO countries (name, description, sources) VALUES (?, ?, ?)',
        (data['name'], data['description'], data['sources'])
    )
    
    conn.commit()
    conn.close()
    
    return jsonify({'success': True})


@app.route('/add_language', methods=['POST'])
def add_language():
    """Ajoute une nouvelle langue"""
    data = request.json
    
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    
    cursor.execute(
        'INSERT INTO languages (code, name, description) VALUES (?, ?, ?)',
        (data['code'], data['name'], data['description'])
    )
    
    conn.commit()
    conn.close()
    
    return jsonify({'success': True})


@app.route('/api/stats')
def api_stats():
    """API pour les statistiques temps réel"""
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


# === Endpoint optionnel pour l'UI Studio : résultats récents ===
@app.route('/api/recent_results')
def recent_results():
    """Renvoie les 200 derniers résultats (pour l'affichage dans /studio)"""
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


if __name__ == '__main__':
    # Lancer l'app
    app.run(debug=True, host='127.0.0.1', port=5000)
