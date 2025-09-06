# MOTEUR DE SCRAPING - SCRAPMASTER ENGINE
# Nom du fichier : scraper_engine.py

import requests
from bs4 import BeautifulSoup
import time
import re
import json
import importlib
from datetime import datetime

class ScrapingEngine:
    """Moteur principal de scraping modulaire"""
    
    def __init__(self):
        self.scrapers = {}
        self.load_scrapers()
    
    def load_scrapers(self):
        """Charge tous les scrapers modulaires"""
        scrapers_config = {
            'youtube_scraper': 'YouTubeChannelScraper',
            'lawyer_scraper': 'LawyerScraper',
            'association_scraper': 'AssociationScraper',
            'translator_scraper': 'TranslatorScraper',
            'nomad_scraper': 'NomadScraper',
            'restaurant_scraper': 'RestaurantScraper',
            'generic_scraper': 'GenericScraper'
        }
        
        for scraper_name, class_name in scrapers_config.items():
            try:
                module = importlib.import_module(f'scrapers.{scraper_name}')
                scraper_class = getattr(module, class_name)
                self.scrapers[scraper_name] = scraper_class()
            except ImportError:
                print(f"Scraper {scraper_name} non trouvé, utilisation du scraper générique")
                self.scrapers[scraper_name] = GenericScraper()
    
    def run_scraping(self, project):
        """Lance le scraping pour un projet donné"""
        project_id, name, profession, country, language, sources, status, created_at, last_run, total_results = project
        
        print(f"🚀 Début scraping: {name}")
        print(f"📊 Métier: {profession} | Pays: {country} | Langue: {language}")
        
        # Déterminer le scraper à utiliser
        scraper_template = self.get_scraper_template(profession)
        scraper = self.scrapers.get(scraper_template, self.scrapers['generic_scraper'])
        
        # Charger les sources pour le pays
        sources_list = self.load_sources(country)
        
        # Configuration du scraping
        config = {
            'profession': profession,
            'country': country,
            'language': language,
            'sources': sources_list,
            'filters': {
                'language_filter': language,
                'country_filter': country
            }
        }
        
        # Lancer le scraping
        results = scraper.scrape(config)
        
        print(f"✅ Scraping terminé: {len(results)} résultats")
        
        return results
    
    def get_scraper_template(self, profession):
        """Détermine le template de scraper selon le métier"""
        profession_mapping = {
            'YouTubeurs': 'youtube_scraper',
            'Avocats': 'lawyer_scraper',
            'Associations': 'association_scraper',
            'Traducteurs': 'translator_scraper',
            'Interprètes': 'translator_scraper',
            'Digital Nomads': 'nomad_scraper',
            'Restaurateurs': 'restaurant_scraper',
            'Hôteliers': 'generic_scraper'
        }
        
        return profession_mapping.get(profession, 'generic_scraper')
    
    def load_sources(self, country):
        """Charge les sources pour un pays donné"""
        sources_files = {
            'Thaïlande': 'thailand_sources.json',
            'France': 'france_sources.json',
            'Expatriés Thaïlande': 'expat_thailand_sources.json',
            'Digital Nomads Asie': 'nomads_asia_sources.json',
            'Voyageurs Asie du Sud-Est': 'travelers_sea_sources.json'
        }
        
        filename = sources_files.get(country, 'generic_sources.json')
        filepath = f'sources/{filename}'
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            # Sources par défaut si le fichier n'existe pas
            return self.get_default_sources(country)
    
    def get_default_sources(self, country):
        """Sources par défaut selon le pays"""
        if 'Thaïlande' in country:
            return [
                {
                    'name': 'Annuaire Thailand Guide',
                    'base_url': 'https://annuaire.thailande-guide.com',
                    'categories': [
                        {'name': 'associations', 'url': 'https://annuaire.thailande-guide.com/fr/cat-61/'},
                        {'name': 'services', 'url': 'https://annuaire.thailande-guide.com/fr/cat-28/'},
                        {'name': 'clubs', 'url': 'https://annuaire.thailande-guide.com/fr/cat-120/'}
                    ]
                }
            ]
        elif country == 'France':
            return [
                {
                    'name': 'Pages Jaunes',
                    'base_url': 'https://www.pagesjaunes.fr',
                    'categories': []
                }
            ]
        else:
            return []


class GenericScraper:
    """Scraper générique réutilisable"""
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
    
    def scrape(self, config):
        """Lance le scraping générique"""
        all_results = []
        
        for source in config['sources']:
            print(f"📂 Source: {source['name']}")
            
            for category in source.get('categories', []):
                print(f"  📄 Catégorie: {category['name']}")
                
                results = self.scrape_category(
                    category['url'], 
                    category['name'], 
                    config
                )
                
                all_results.extend(results)
                time.sleep(1)  # Pause respectueuse
        
        return all_results
    
    def scrape_category(self, base_url, category_name, config):
        """Scrape une catégorie spécifique"""
        results = []
        
        # Scraper jusqu'à 10 pages par catégorie
        for page in range(1, 11):
            try:
                url = f"{base_url}{page}/index.html"
                
                response = requests.get(url, headers=self.headers, timeout=10)
                
                if response.status_code == 404:
                    break
                
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                
                page_results = self.extract_data_from_page(soup, url, category_name, config)
                
                if not page_results:
                    break
                
                results.extend(page_results)
                print(f"    Page {page}: {len(page_results)} entrées")
                
                time.sleep(0.5)
                
            except Exception as e:
                print(f"    Erreur page {page}: {e}")
                break
        
        return results
    
    def extract_data_from_page(self, soup, source_url, category, config):
        """Extrait les données d'une page"""
        entries = []
        
        text = soup.get_text()
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        
        i = 0
        while i < len(lines):
            line = lines[i]
            
            if self.is_valid_name(line):
                website = ""
                description = ""
                
                # Chercher URL dans les lignes suivantes
                for j in range(i+1, min(i+5, len(lines))):
                    if j < len(lines) and self.is_url(lines[j]):
                        website = self.clean_url(lines[j])
                        
                        # Chercher description
                        desc_parts = []
                        for k in range(j+1, min(j+8, len(lines))):
                            if k < len(lines) and self.is_description(lines[k]):
                                desc_parts.append(lines[k])
                            elif self.is_valid_name(lines[k]) or self.is_url(lines[k]):
                                break
                        
                        description = ' '.join(desc_parts)
                        break
                
                # Filtrer selon les critères
                if self.matches_filters(line, description, config['filters']):
                    entry = self.create_entry(line, website, description, category, source_url, config)
                    entries.append(entry)
            
            i += 1
        
        return entries
    
    def is_valid_name(self, text):
        """Vérifie si un texte est un nom valide"""
        if not text or len(text) < 3 or len(text) > 200:
            return False
        
        exclusions = [
            'copyright', 'accueil', 'contact', 'mentions', 'signaler',
            'modifier', 'visites depuis', 'nouveautés', 'nous contacter',
            'proposer', 'filtrer', 'circuits', 'prix', 'mesure'
        ]
        
        text_lower = text.lower()
        if any(word in text_lower for word in exclusions):
            return False
        
        if not re.search(r'[a-zA-Z]', text):
            return False
        
        if text_lower.startswith(('www.', 'http', 'https')):
            return False
        
        return True
    
    def is_url(self, text):
        """Vérifie si un texte est une URL"""
        if not text:
            return False
        
        text = text.lower().strip()
        return (text.startswith(('www.', 'http://', 'https://')) or
                any(ext in text for ext in ['.com', '.org', '.net', '.fr', '.th']))
    
    def clean_url(self, url):
        """Nettoie une URL"""
        url = url.strip()
        if not url.startswith(('http://', 'https://')) and url.startswith('www.'):
            url = 'http://' + url
        return url
    
    def is_description(self, text):
        """Vérifie si un texte est une description valide"""
        if not text or len(text) < 10 or len(text) > 800:
            return False
        
        exclusions = [
            'visites depuis', 'signaler', 'modifier', 'copyright',
            'accueil', 'top clics', 'nouveautés', 'nous contacter'
        ]
        
        text_lower = text.lower()
        return not any(word in text_lower for word in exclusions)
    
    def matches_filters(self, name, description, filters):
        """Vérifie si l'entrée correspond aux filtres"""
        text_combined = f"{name} {description}".lower()
        
        # Filtre langue
        language_filter = filters.get('language_filter', '')
        if language_filter:
            language_keywords = {
                'fr': ['français', 'french', 'france', 'franco'],
                'en': ['english', 'anglais', 'british', 'anglo'],
                'th': ['thai', 'thaï', 'thailand', 'thaïlande'],
                'de': ['deutsch', 'german', 'allemand'],
                'es': ['spanish', 'espagnol', 'español']
            }
            
            keywords = language_keywords.get(language_filter, [])
            if keywords and not any(keyword in text_combined for keyword in keywords):
                # Pas de filtre strict - accepter quand même
                pass
        
        return True
    
    def create_entry(self, name, website, description, category, source_url, config):
        """Crée une entrée structurée"""
        return {
            'name': name.strip(),
            'category': category,
            'description': description.strip(),
            'website': website,
            'email': self.extract_email(description),
            'phone': self.extract_phone(description),
            'city': self.extract_city(description),
            'country': config['country'],
            'language': self.detect_language(f"{name} {description}"),
            'source_url': source_url,
            'profession': config['profession'],
            'scraped_at': datetime.now().isoformat()
        }
    
    def extract_email(self, text):
        """Extrait un email"""
        pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        match = re.search(pattern, text)
        return match.group() if match else ''
    
    def extract_phone(self, text):
        """Extrait un téléphone"""
        pattern = r'[+]?[0-9][0-9\s\-\(\)]{7,15}'
        match = re.search(pattern, text)
        return match.group().strip() if match else ''
    
    def extract_city(self, text):
        """Extrait une ville"""
        cities = [
            'Bangkok', 'Chiang Mai', 'Phuket', 'Pattaya', 'Hua Hin',
            'Paris', 'Lyon', 'Marseille', 'London', 'New York'
        ]
        
        text_lower = text.lower()
        for city in cities:
            if city.lower() in text_lower:
                return city
        return ''
    
    def detect_language(self, text):
        """Détecte la langue du texte"""
        text_lower = text.lower()
        
        if any(word in text_lower for word in ['français', 'french', 'france']):
            return 'fr'
        elif any(word in text_lower for word in ['english', 'british', 'anglo']):
            return 'en'
        elif any(word in text_lower for word in ['thai', 'thaï', 'thailand']):
            return 'th'
        elif any(word in text_lower for word in ['deutsch', 'german']):
            return 'de'
        
        return 'unknown'