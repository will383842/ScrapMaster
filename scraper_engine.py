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

# --- Scrapers principaux ---
# GenericScraper (fallback solide)
try:
    from scrapers.generic_scraper import GenericScraper
except ImportError:
    class GenericScraper:
        def scrape(self, config):
            print("⚠️ GenericScraper fallback utilisé")
            return []

# SearchScraper (orchestrateur "search-first")
try:
    from scrapers.search_scraper import SearchScraper
except ImportError:
    class SearchScraper:
        def search(self, profession, country, language, extra_keywords=""):
            print("⚠️ SearchScraper non disponible, aucune URL trouvée via recherche")
            return []


class ScrapingEngine:
    """Moteur principal de scraping modulaire"""

    def __init__(self):
        # Scrapers "métier" dynamiques
        self.scrapers = {}
        self.load_scrapers()

        # Orchestrateurs clés
        self.generic = GenericScraper()
        self.searcher = SearchScraper()

    def load_scrapers(self):
        """Charge tous les scrapers modulaires avec gestion d'erreur robuste"""
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
                module = importlib.import_module(f'scrapers.{scraper_name}')
                scraper_class = getattr(module, class_name, None) or getattr(module, 'Scraper', None)
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

        if 'generic_scraper' not in self.scrapers:
            self.scrapers['generic_scraper'] = GenericScraper()

        print(f"🔧 {len(self.scrapers)} scrapers chargés")

    # ---------------------------------------------------------------------
    # ORCHESTRATION "SEARCH → GENERIC"
    # ---------------------------------------------------------------------
    def run_scraping(self, project):
        """
        Orchestration principale.
        1) Normalise le projet (tuple/dict) -> dict project_cfg
        2) Lance une recherche auto (SearchScraper) pour collecter des URLs cibles
        3) Construit des sources "consommables" par le GenericScraper
        4) Fusionne avec d'éventuels seeds existants
        5) Exécute le GenericScraper sur l'ensemble
        6) Valide et renvoie les résultats
        """

        # --- Normalisation du format project ---
        if hasattr(project, '_fields') or isinstance(project, tuple):
            # Tuple legacy: (id, name, profession, country, language, sources, status, created_at, last_run, total_results)
            project_id, name, profession, country, language, sources, status, created_at, last_run, total_results = project[:10]
            try:
                srcs = json.loads(sources) if isinstance(sources, str) else (sources or [])
            except Exception:
                srcs = []
            project_cfg = {
                "id": project_id,
                "name": name,
                "profession": profession,
                "country": country,
                "language": language,
                "sources": srcs,
                "status": status,
                "created_at": created_at,
                "last_run": last_run,
                "total_results": total_results
            }
        else:
            # Dict/Row
            project_cfg = dict(project)
            if isinstance(project_cfg.get("sources"), str):
                try:
                    project_cfg["sources"] = json.loads(project_cfg["sources"])
                except Exception:
                    project_cfg["sources"] = []

        # --- Lecture des paramètres ---
        profession = (project_cfg.get("profession") or "").strip()
        country    = (project_cfg.get("country") or "").strip()
        language   = (project_cfg.get("language") or "en").strip()

        # Récupération des keywords éventuels (depuis sources si dict)
        keywords = ""
        srcs = project_cfg.get("sources")
        if isinstance(srcs, dict):
            keywords = (srcs.get("keywords") or "").strip()

        print(f"🚀 Début scraping: {project_cfg.get('name')}")
        print(f"📊 Métier: {profession} | Pays: {country} | Langue: {language}")

        # --- 1) Recherche automatique → URLs candidates ---
        try:
            found_urls = self.searcher.search(profession, country, language, extra_keywords=keywords) or []
        except Exception as e:
            print(f"⚠️ SearchScraper erreur: {e}")
            found_urls = []

        # --- 2) Construire des sources "consommables" par le GenericScraper ---
        search_sources = [{
            "name": f"Search:{profession}-{country}-{language}",
            "categories": [
                {"name": "search", "url": u if u.endswith("/") else u + "/"}
                for u in found_urls[:50]
            ]
        }]

        # --- 3) Fusionner avec d'éventuelles sources existantes (seeds) ---
        seeds = []
        if isinstance(srcs, list):
            seeds = srcs
        elif isinstance(srcs, dict) and srcs.get("seed_sources"):
            seeds = srcs["seed_sources"]

        # pays -> sources par défaut (facultatif)
        default_country_sources = self.load_sources(country) or []

        # Priorité :
        #   seeds (si fournis dans le projet)
        # + default_country_sources (fichiers sources/…json)
        # + search_sources (URLs issues de la recherche)
        sources = (seeds or []) + (default_country_sources or [])
        sources += search_sources

        # --- 4) Lancer le GenericScraper sur ces sources ---
        cfg = dict(project_cfg)
        cfg["sources"] = sources
        cfg["keep_incomplete"] = True

        try:
            raw_results = self.generic.scrape(cfg) or []
            print(f"✅ Scraping terminé (generic): {len(raw_results)} résultats")
        except Exception as e:
            print(f"❌ Erreur GenericScraper: {e}")
            raw_results = []

        # --- 5) Validation / nettoyage ---
        validated_results = self.validate_results(raw_results, cfg)
        print(f"🎯 Résultats validés: {len(validated_results)} résultats")

        return validated_results

    # ---------------------------------------------------------------------
    # OUTILS EXISTANTS / COMPAT
    # ---------------------------------------------------------------------
    def extract_keywords_from_sources(self, project_sources):
        """Extrait les keywords des sources du projet (compat ancien)."""
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
        """Détermine le template de scraper selon le métier (si jamais utilisé ailleurs)."""
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
        """Charge les sources pour un pays donné (fichiers JSON optionnels)."""
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

        print(f"📂 Utilisation sources par défaut pour: {country}")
        return self.get_default_sources(country)

    def get_default_sources(self, country):
        """Sources par défaut minimales selon le pays (compat ancien)."""
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
                    'name': "Communautés d'expatriés",
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

    # ---------------------------------------------------------------------
    # Validation / nettoyage des résultats
    # ---------------------------------------------------------------------
    def validate_results(self, results, config):
        """Post-traite et valide les résultats"""
        validated = []

        for result in results:
            if not result or not isinstance(result, dict):
                continue

            name = result.get('name', '').strip()
            if not name or len(name) < 2:
                continue

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

            if self.is_valid_result(cleaned_result):
                validated.append(cleaned_result)

        return validated

    def clean_text(self, text):
        """Nettoie un texte"""
        if not text:
            return ''
        cleaned = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x84\x86-\x9f]', '', str(text))
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        return cleaned

    def clean_url(self, url):
        """Nettoie une URL"""
        if not url:
            return ''
        url = str(url).strip()
        if not url:
            return ''
        if not url.startswith(('http://', 'https://')):
            if url.startswith('www.'):
                url = 'https://' + url
            elif '.' in url and not url.startswith('//'):
                url = 'https://' + url
        if not re.match(r'https?://[^\s]+\.[^\s]+', url):
            return ''
        return url[:500]

    def clean_email(self, email):
        """Nettoie un email"""
        if not email:
            return ''
        email = str(email).strip().lower()
        if re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', email):
            return email[:200]
        return ''

    def clean_phone(self, phone):
        """Nettoie un numéro de téléphone"""
        if not phone:
            return ''
        phone = str(phone).strip()
        phone = re.sub(r'[^\d\s+()-]', '', phone)
        digits_only = re.sub(r'[^\d]', '', phone)
        if len(digits_only) >= 7:
            return phone[:50]
        return ''

    def is_valid_result(self, result):
        """Vérifie si un résultat est valide"""
        if not result.get('name'):
            return False
        has_contact = (
            result.get('website') or
            result.get('email') or
            result.get('phone') or
            (result.get('description') and len(result.get('description')) > 50)
        )
        return has_contact


# ---------------------------------------------------------------------
# Scraper générique de fallback intégré (pour compat ancienne)
# ---------------------------------------------------------------------
class FallbackGenericScraper:
    """Scraper générique de fallback intégré"""

    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/91.0.4472.124 Safari/537.36'
        }

    def scrape(self, config):
        """Lance le scraping générique"""
        print("🔧 Utilisation du scraper générique de fallback")

        all_results = []
        sources = config.get('sources', [])

        if not sources:
            print("⚠️ Aucune source configurée")
            return []

        for source in sources[:3]:
            print(f"📂 Source: {source.get('name', 'Sans nom')}")
            categories = source.get('categories', [])
            if not categories:
                continue

            for category in categories[:2]:
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

                time.sleep(1)

        print(f"🎯 Total scraper générique: {len(all_results)} résultats")
        return all_results

    def scrape_category(self, base_url, category_name, config):
        """Scrape une catégorie spécifique"""
        if not base_url:
            return []

        results = []
        for page in range(1, 4):
            try:
                if '/cat-' in base_url and 'thailande-guide.com' in base_url:
                    url = f"{base_url}{page}/index.html"
                else:
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
        links = soup.find_all('a', href=True)

        for link in links[:20]:
            href = link.get('href', '').strip()
            title = link.get_text(strip=True)

            if not title or len(title) < 5 or len(title) > 200:
                continue

            if any(word in title.lower() for word in ['accueil', 'contact', 'mentions', 'top clics', 'nouveautés']):
                continue

            if href.startswith('http'):
                website = href
            elif href.startswith('/'):
                from urllib.parse import urljoin
                website = urljoin(source_url, href)
            else:
                continue

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
