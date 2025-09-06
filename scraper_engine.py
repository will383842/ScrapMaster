# MOTEUR DE SCRAPING - SCRAPMASTER ENGINE
# Nom du fichier : scraper_engine.py

import requests
from bs4 import BeautifulSoup
import time
import re
import json
import importlib
import os
from datetime import datetime

# Import du GenericScraper comme fallback
try:
    from scrapers.generic_scraper import GenericScraper
except ImportError:
    # Fallback si le module n'est pas trouvé
    class GenericScraper:
        def scrape(self, config):
            print("⚠️ GenericScraper fallback utilisé")
            return []

class ScrapingEngine:
    """Moteur principal de scraping modulaire"""
    
    def __init__(self):
        self.scrapers = {}
        self.load_scrapers()
    
    def load_scrapers(self):
        """Charge tous les scrapers modulaires avec gestion d'erreur robuste"""
        # Configuration des scrapers avec les vraies classes
        scrapers_config = {
            'youtube_scraper': 'YouTubeChannelScraper',
            'lawyer_scraper': 'LawyerScraper', 
            'association_scraper': 'AssociationScraper',
            'translator_scraper': 'TranslatorScraper',
            'nomad_scraper': 'NomadScraper',
            'restaurant_scraper': 'RestaurantScraper',
            'hotel_scraper': 'HotelScraper',
            'generic_scraper': 'GenericScraper'
        }
        
        for scraper_name, class_name in scrapers_config.items():
            try:
                # Tentative d'import du module
                module = importlib.import_module(f'scrapers.{scraper_name}')
                
                # Recherche de la classe (avec fallback sur 'Scraper')
                scraper_class = getattr(module, class_name, None)
                if not scraper_class:
                    scraper_class = getattr(module, 'Scraper', None)
                
                if scraper_class:
                    self.scrapers[scraper_name] = scraper_class()
                    print(f"✅ Scraper chargé: {scraper_name} -> {class_name}")
                else:
                    print(f"⚠️ Classe {class_name} non trouvée dans {scraper_name}")
                    self.scrapers[scraper_name] = GenericScraper()
                    
            except ImportError as e:
                print(f"⚠️ Module {scraper_name} non trouvé: {e}")
                self.scrapers[scraper_name] = GenericScraper()
            except Exception as e:
                print(f"❌ Erreur chargement {scraper_name}: {e}")
                self.scrapers[scraper_name] = GenericScraper()
        
        # Assurer qu'on a toujours un scraper générique
        if 'generic_scraper' not in self.scrapers:
            self.scrapers['generic_scraper'] = GenericScraper()
            
        print(f"🔧 {len(self.scrapers)} scrapers chargés")
    
    def run_scraping(self, project):
        """
        Lance le scraping pour un projet donné
        project peut être soit un tuple (old format) soit un dict/Row object
        """
        # Normalisation du format project (compatibilité tuple/dict)
        if hasattr(project, '_fields') or isinstance(project, tuple):
            # Format tuple: (id, name, profession, country, language, sources, status, created_at, last_run, total_results)
            project_id, name, profession, country, language, sources, status, created_at, last_run, total_results = project[:10]
        else:
            # Format dict/Row
            project_id = project.get('id')
            name = project.get('name')
            profession = project.get('profession')
            country = project.get('country')
            language = project.get('language')
            sources = project.get('sources')
            status = project.get('status')
            created_at = project.get('created_at')
            last_run = project.get('last_run')
            total_results = project.get('total_results', 0)
        
        print(f"🚀 Début scraping: {name}")
        print(f"📊 Métier: {profession} | Pays: {country} | Langue: {language}")
        
        # Déterminer le scraper à utiliser
        scraper_template = self.get_scraper_template(profession)
        scraper = self.scrapers.get(scraper_template, self.scrapers.get('generic_scraper'))
        
        print(f"🔧 Utilisation du scraper: {scraper_template}")
        
        # Charger les sources pour le pays
        sources_list = self.load_sources(country)
        
        # Parse sources JSON si c'est une string
        project_sources = []
        try:
            if isinstance(sources, str):
                project_sources = json.loads(sources)
            elif isinstance(sources, dict):
                project_sources = [sources]  # Convertir en liste
            elif isinstance(sources, list):
                project_sources = sources
        except Exception as e:
            print(f"⚠️ Erreur parsing sources projet: {e}")
            project_sources = []
        
        # Configuration du scraping
        config = {
            'project_id': project_id,
            'profession': profession,
            'country': country,
            'language': language,
            'sources': sources_list,
            'project_sources': project_sources,  # Sources spécifiques du projet
            'filters': {
                'language_filter': language,
                'country_filter': country
            },
            'keywords': self.extract_keywords_from_sources(project_sources)
        }
        
        try:
            # Lancer le scraping
            results = scraper.scrape(config) or []
            
            print(f"✅ Scraping terminé: {len(results)} résultats")
            
            # Post-traitement et validation des résultats
            validated_results = self.validate_results(results, config)
            
            print(f"🎯 Résultats validés: {len(validated_results)} résultats")
            
            return validated_results
            
        except Exception as e:
            print(f"❌ Erreur lors du scraping: {e}")
            return []
    
    def extract_keywords_from_sources(self, project_sources):
        """Extrait les keywords des sources du projet"""
        keywords = ""
        try:
            if isinstance(project_sources, list) and project_sources:
                for source in project_sources:
                    if isinstance(source, dict) and 'keywords' in source:
                        keywords = source['keywords']
                        break
            elif isinstance(project_sources, dict) and 'keywords' in project_sources:
                keywords = project_sources['keywords']
        except Exception:
            pass
        return keywords
    
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
            'Hôteliers': 'hotel_scraper'
        }
        
        return profession_mapping.get(profession, 'generic_scraper')
    
    def load_sources(self, country):
        """Charge les sources pour un pays donné"""
        sources_files = {
            'Thaïlande': 'thailand_sources.json',
            'France': 'france_sources.json', 
            'Expatriés Thaïlande': 'expat_thailand_sources.json',
            'Digital Nomads Asie': 'nomads_asia_sources.json',
            'Voyageurs Asie du Sud-Est': 'travelers_sea_sources.json',
            'Royaume-Uni': 'uk_sources.json',
            'États-Unis': 'usa_sources.json',
            'Allemagne': 'germany_sources.json'
        }
        
        filename = sources_files.get(country, 'generic_sources.json')
        
        # Chercher le fichier sources dans plusieurs emplacements
        possible_paths = [
            f'sources/{filename}',
            f'../sources/{filename}',
            f'./sources/{filename}',
            os.path.join(os.path.dirname(__file__), 'sources', filename)
        ]
        
        for filepath in possible_paths:
            try:
                if os.path.exists(filepath):
                    with open(filepath, 'r', encoding='utf-8') as f:
                        sources = json.load(f)
                        print(f"📂 Sources chargées: {filepath}")
                        return sources
            except Exception as e:
                print(f"⚠️ Erreur lecture {filepath}: {e}")
                continue
        
        # Fallback: sources par défaut
        print(f"📂 Utilisation sources par défaut pour: {country}")
        return self.get_default_sources(country)
    
    def get_default_sources(self, country):
        """Sources par défaut selon le pays"""
        if 'Thaïlande' in country or 'Thailand' in country:
            return [
                {
                    'name': 'Annuaire Thailand Guide',
                    'base_url': 'https://annuaire.thailande-guide.com',
                    'categories': [
                        {'name': 'associations', 'url': 'https://annuaire.thailande-guide.com/fr/cat-61/'},
                        {'name': 'services', 'url': 'https://annuaire.thailande-guide.com/fr/cat-28/'},
                        {'name': 'clubs', 'url': 'https://annuaire.thailande-guide.com/fr/cat-120/'},
                        {'name': 'universites', 'url': 'https://annuaire.thailande-guide.com/fr/cat-58/'},
                        {'name': 'sante', 'url': 'https://annuaire.thailande-guide.com/fr/cat-71/'}
                    ]
                }
            ]
        elif country == 'France':
            return [
                {
                    'name': 'Pages Jaunes France',
                    'base_url': 'https://www.pagesjaunes.fr',
                    'categories': [
                        {'name': 'associations', 'url': 'https://www.pagesjaunes.fr/annuaire/chercherlespros?quoiqui=association'},
                        {'name': 'services', 'url': 'https://www.pagesjaunes.fr/annuaire/chercherlespros?quoiqui=service'}
                    ]
                }
            ]
        elif 'Expatriés' in country:
            return [
                {
                    'name': 'Communautés d\'expatriés',
                    'base_url': 'https://www.expat.com',
                    'categories': [
                        {'name': 'communautes', 'url': 'https://www.expat.com/forum/'},
                        {'name': 'services_expat', 'url': 'https://www.expat.com/services/'}
                    ]
                }
            ]
        else:
            return [
                {
                    'name': 'Sources génériques',
                    'base_url': 'https://example.com',
                    'categories': [
                        {'name': 'general', 'url': 'https://example.com/directory/'}
                    ]
                }
            ]
    
    def validate_results(self, results, config):
        """Post-traite et valide les résultats"""
        validated = []
        
        for result in results:
            # Validation de base
            if not result or not isinstance(result, dict):
                continue
                
            # Obligatoire: nom
            name = result.get('name', '').strip()
            if not name or len(name) < 2:
                continue
            
            # Nettoyage des données
            cleaned_result = {
                'name': self.clean_text(name)[:500],
                'category': self.clean_text(result.get('category', ''))[:200],
                'description': self.clean_text(result.get('description', ''))[:2000],
                'website': self.clean_url(result.get('website', '')),
                'email': self.clean_email(result.get('email', '')),
                'phone': self.clean_phone(result.get('phone', '')),
                'city': self.clean_text(result.get('city', ''))[:200],
                'country': result.get('country', config.get('country', '')),
                'language': result.get('language', config.get('language', '')),
                'source_url': self.clean_url(result.get('source_url', '')),
                'profession': config.get('profession', ''),
                'scraped_at': result.get('scraped_at', datetime.now().isoformat()),
                'quality_score': result.get('quality_score', 5)
            }
            
            # Validation finale
            if self.is_valid_result(cleaned_result):
                validated.append(cleaned_result)
        
        return validated
    
    def clean_text(self, text):
        """Nettoie un texte"""
        if not text:
            return ''
        
        # Suppression des caractères de contrôle
        cleaned = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x84\x86-\x9f]', '', str(text))
        
        # Normalisation des espaces
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        return cleaned
    
    def clean_url(self, url):
        """Nettoie une URL"""
        if not url:
            return ''
        
        url = str(url).strip()
        if not url:
            return ''
            
        # Ajouter http:// si nécessaire
        if not url.startswith(('http://', 'https://')):
            if url.startswith('www.'):
                url = 'https://' + url
            elif '.' in url and not url.startswith('//'):
                url = 'https://' + url
        
        # Validation basique d'URL
        if not re.match(r'https?://[^\s]+\.[^\s]+', url):
            return ''
            
        return url[:500]
    
    def clean_email(self, email):
        """Nettoie un email"""
        if not email:
            return ''
        
        email = str(email).strip().lower()
        
        # Validation basique d'email
        if re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}, email):
            return email[:200]
        
        return ''
    
    def clean_phone(self, phone):
        """Nettoie un numéro de téléphone"""
        if not phone:
            return ''
        
        phone = str(phone).strip()
        
        # Garder seulement les chiffres, espaces, +, -, (, )
        phone = re.sub(r'[^\d\s+()-]', '', phone)
        
        # Validation basique (au moins 7 chiffres)
        digits_only = re.sub(r'[^\d]', '', phone)
        if len(digits_only) >= 7:
            return phone[:50]
        
        return ''
    
    def is_valid_result(self, result):
        """Vérifie si un résultat est valide"""
        # Au minimum un nom
        if not result.get('name'):
            return False
        
        # Au moins un moyen de contact ou une description substantielle
        has_contact = (
            result.get('website') or 
            result.get('email') or 
            result.get('phone') or
            (result.get('description') and len(result.get('description')) > 50)
        )
        
        return has_contact


# Classe GenericScraper intégrée comme fallback
class FallbackGenericScraper:
    """Scraper générique de fallback intégré"""
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
    
    def scrape(self, config):
        """Lance le scraping générique"""
        print("🔧 Utilisation du scraper générique de fallback")
        
        all_results = []
        sources = config.get('sources', [])
        
        if not sources:
            print("⚠️ Aucune source configurée")
            return []
        
        for source in sources[:3]:  # Limiter à 3 sources pour éviter la surcharge
            print(f"📂 Source: {source.get('name', 'Sans nom')}")
            
            categories = source.get('categories', [])
            if not categories:
                continue
                
            for category in categories[:2]:  # Limiter à 2 catégories par source
                print(f"  📄 Catégorie: {category.get('name', 'general')}")
                
                try:
                    results = self.scrape_category(
                        category.get('url', ''), 
                        category.get('name', 'general'), 
                        config
                    )
                    all_results.extend(results)
                    print(f"    ✅ {len(results)} résultats")
                    
                except Exception as e:
                    print(f"    ❌ Erreur: {e}")
                
                time.sleep(1)  # Pause respectueuse
        
        print(f"🎯 Total scraper générique: {len(all_results)} résultats")
        return all_results
    
    def scrape_category(self, base_url, category_name, config):
        """Scrape une catégorie spécifique"""
        if not base_url:
            return []
            
        results = []
        
        # Essayer de scraper quelques pages
        for page in range(1, 4):  # Limiter à 3 pages
            try:
                if '/cat-' in base_url and 'thailande-guide.com' in base_url:
                    # Format spécifique Thailand Guide
                    url = f"{base_url}{page}/index.html"
                else:
                    # Format générique
                    url = base_url
                    if page > 1:
                        url += f"?page={page}"
                
                response = requests.get(url, headers=self.headers, timeout=10)
                
                if response.status_code == 404:
                    break
                    
                if response.status_code != 200:
                    print(f"    ⚠️ Status {response.status_code} pour {url}")
                    continue
                
                soup = BeautifulSoup(response.content, 'html.parser')
                page_results = self.extract_from_page(soup, url, category_name, config)
                
                if not page_results:
                    if page == 1:
                        print(f"    ⚠️ Aucune donnée extraite de {url}")
                    break
                
                results.extend(page_results)
                
                # Arrêter si on a assez de résultats
                if len(results) >= 50:
                    break
                    
                time.sleep(0.5)
                
            except Exception as e:
                print(f"    ❌ Erreur page {page}: {e}")
                break
        
        return results
    
    def extract_from_page(self, soup, source_url, category, config):
        """Extraction basique de données depuis une page"""
        results = []
        
        # Tentative d'extraction de liens avec titres
        links = soup.find_all('a', href=True)
        
        for link in links[:20]:  # Limiter le nombre de liens traités
            href = link.get('href', '').strip()
            title = link.get_text(strip=True)
            
            if not title or len(title) < 5 or len(title) > 200:
                continue
                
            # Filtrer les liens de navigation évidents
            if any(word in title.lower() for word in ['accueil', 'contact', 'mentions', 'top clics', 'nouveautés']):
                continue
            
            # Construire l'URL complète
            if href.startswith('http'):
                website = href
            elif href.startswith('/'):
                from urllib.parse import urljoin
                website = urljoin(source_url, href)
            else:
                continue
            
            # Créer un résultat basique
            result = {
                'name': title,
                'category': category,
                'description': f"Trouvé dans la catégorie {category}",
                'website': website,
                'email': '',
                'phone': '',
                'city': '',
                'country': config.get('country', ''),
                'language': config.get('language', ''),
                'source_url': source_url,
                'profession': config.get('profession', ''),
                'scraped_at': datetime.now().isoformat(),
                'quality_score': 3
            }
            
            results.append(result)
        
        return results