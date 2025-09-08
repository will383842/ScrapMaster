# SCRAPER GÉNÉRIQUE - SCRAPMASTER
# Nom du fichier : generic_scraper.py

import os
import random
import time
import re
from datetime import datetime
from typing import List, Optional, Dict
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import logging

from utils.ua import pick_user_agent
from utils.normalize import (
    normalize_url, extract_emails, extract_phones, normalize_phone_list,
    extract_socials, detect_language as detect_lang, normalize_location, normalize_name,
    extract_whatsapp, extract_line_id, extract_telegram, extract_wechat, find_contact_like_links
)
from utils.dedupe import fuzzy_duplicate

logger = logging.getLogger(__name__)


class PageNotFoundError(Exception):
    """404 / page inexistante."""
    pass


class GenericScraper:
    """Scraper générique réutilisable pour tous types d'annuaires.
    Objectif : remplir un maximum de champs (email, téléphone en E.164, réseaux, langue, ville…)
    """

    # Mapping léger pays -> région pour la normalisation des téléphones
    COUNTRY_TO_REGION = {
        'Thaïlande': 'TH', 'Thailand': 'TH',
        'France': 'FR',
        'États-Unis': 'US', 'United States': 'US', 'USA': 'US',
        'Royaume-Uni': 'GB', 'United Kingdom': 'GB', 'UK': 'GB',
        'Allemagne': 'DE', 'Germany': 'DE',
        'Espagne': 'ES', 'Spain': 'ES',
        'Italie': 'IT', 'Italy': 'IT',
        'Russie': 'RU', 'Russia': 'RU',
        'Chine': 'CN', 'China': 'CN',
        'Japon': 'JP', 'Japan': 'JP'
    }

    # Heuristiques pour détecter un nom de contact
    CONTACT_PATTERNS = [
        r"(?:Contact(?: person)?|Responsable|Président|Présidente|Secrétariat|Secrétaire|Directeur|Directrice)\s*[:\-]\s*([A-Z][a-zÀ-ÖØ-öø-ÿ]+(?:\s+[A-Z][a-zÀ-ÖØ-öø-ÿ]+)+)",
        r"(?:Mr\.?|Mme\.?|Mlle\.?|Dr\.?)\s+([A-Z][a-zÀ-ÖØ-öø-ÿ]+(?:\s+[A-Z][a-zÀ-ÖØ-öø-ÿ]+)+)"
    ]

    def __init__(self):
        self.headers = {
            'User-Agent': pick_user_agent(),
            'Accept-Language': 'en,fr;q=0.9',
            'Cache-Control': 'no-cache'
        }
        try:
            self.delay_s = max(0.2, int(os.getenv("SCRAPMASTER_DELAY_MS", "1200")) / 1000.0)
        except Exception:
            self.delay_s = 1.2
        try:
            self.max_pages = max(1, int(os.getenv("SCRAPMASTER_MAX_PAGES", "6")))
        except Exception:
            self.max_pages = 6
        proxy = os.getenv("SCRAPMASTER_PROXY", "").strip()
        self.proxies = {"http": proxy, "https": proxy} if proxy else None

    # --------------------- Public API ---------------------

    def scrape(self, config):
        """Lance le scraping générique pour un projet."""
        print("🚀 Début scraping générique")
        print(f"📊 Métier: {config['profession']} | Pays: {config['country']}")

        all_results = []

        # Sources par défaut si pas de sources dans config
        if not config.get('sources'):
            config['sources'] = self.get_default_sources(config['country'])

        for source in config['sources']:
            print(f"📂 Source: {source.get('name','(sans nom)')}")
            for category in source.get('categories', []):
                cat_name = category.get('name', 'general')
                cat_url = category.get('url')
                if not cat_url:
                    continue

                print(f"  📄 Catégorie: {cat_name}")

                results = self.scrape_category(
                    base_url=cat_url,
                    category_name=cat_name,
                    config=config
                )
                all_results.extend(results)
                print(f"    ✅ {len(results)} entrées trouvées")
                time.sleep(1)  # Pause respectueuse (garde 1s ici)

        # Dédup finale (fuzzy)
        deduped = self._deduplicate_entries(all_results)

        print(f"🎯 Total: {len(deduped)} résultats (dédupliqués)")
        return deduped

    # --------------------- Sources par défaut ---------------------

    def get_default_sources(self, country):
        """Sources par défaut selon le pays (exemples)."""
        if 'Thaïlande' in country or 'Thailand' in country:
            return [
                {
                    'name': 'Annuaire Thailand Guide',
                    'categories': [
                        {'name': 'associations',     'url': 'https://annuaire.thailande-guide.com/fr/cat-61/'},
                        {'name': 'services_publics', 'url': 'https://annuaire.thailande-guide.com/fr/cat-28/'},
                        {'name': 'clubs',            'url': 'https://annuaire.thailande-guide.com/fr/cat-120/'},
                        {'name': 'universites',      'url': 'https://annuaire.thailande-guide.com/fr/cat-58/'},
                        {'name': 'sante',            'url': 'https://annuaire.thailande-guide.com/fr/cat-71/'}
                    ]
                }
            ]
        else:
            return [
                {
                    'name': 'Source générique',
                    'categories': [
                        {'name': 'general', 'url': 'https://example.com/'}
                    ]
                }
            ]

    # --------------------- Scraping par catégorie ---------------------

    # Remplacement total : détection intelligente de pagination
    def scrape_category(self, base_url: str, category_name: str, config: dict) -> List[dict]:
        """Scrape avec détection automatique du type de pagination"""
        pagination_strategy = self._detect_pagination_strategy(base_url)
        results: List[dict] = []

        for page_num in range(1, self.max_pages + 1):
            page_url = self._build_page_url(base_url, page_num, pagination_strategy)
            if not page_url:
                break

            try:
                response = self._fetch_page(page_url)
                if not response:
                    break

                page_results = self.extract_data_from_page(response.soup, page_url, category_name, config)

                if not page_results and page_num == 1:
                    # Première page vide = pas un annuaire paginé
                    break
                elif not page_results:
                    # Page suivante vide = fin de pagination
                    break

                # Dédup au fil de l'eau
                results.extend(self._deduplicate_entries(page_results, existing=results))

            except PageNotFoundError:
                break
            except Exception as e:
                logger.warning("Erreur page", extra={"page": page_num, "url": page_url, "error": str(e)})
                break

            time.sleep(self.delay_s + random.random() * 0.4)

        return results

    def _detect_pagination_strategy(self, url: str) -> str:
        """Détecte le type de pagination selon l'URL"""
        url_lower = url.lower()

        # Thailand Guide pattern
        if "/cat-" in url_lower and url.endswith("/"):
            return "thailand_guide"

        # WordPress/standard pagination
        if any(pattern in url_lower for pattern in ["/page/", "/category/", "/tag/"]):
            return "wordpress"

        # Query parameter pagination
        if "?" in url:
            return "query_param"

        # Single page
        return "single"

    def _build_page_url(self, base_url: str, page_num: int, strategy: str) -> Optional[str]:
        """Construit URL de page selon la stratégie détectée"""
        if strategy == "thailand_guide":
            return f"{base_url}{page_num}/index.html"

        elif strategy == "wordpress":
            if base_url.endswith("/"):
                return f"{base_url}page/{page_num}/"
            else:
                return f"{base_url}/page/{page_num}/"

        elif strategy == "query_param":
            sep = "&" if "?" in base_url else "?"
            return f"{base_url}{sep}page={page_num}"

        else:  # single
            return base_url if page_num == 1 else None

    def _fetch_page(self, url: str):
        """Télécharge une page et renvoie un objet avec soup."""
        try:
            resp = requests.get(url, headers=self.headers, timeout=10, proxies=self.proxies, allow_redirects=True)
            if resp.status_code == 404:
                raise PageNotFoundError()
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            return type("PageResponse", (), {"soup": soup, "status_code": resp.status_code})
        except requests.exceptions.Timeout:
            logger.debug("Timeout", extra={"url": url})
            return None
        except requests.exceptions.RequestException as e:
            logger.debug("Erreur requête", extra={"url": url, "error": str(e)})
            return None

    # --------------------- Extraction structurelle ---------------------

    def extract_data_from_page(self, soup: BeautifulSoup, source_url: str, category: str, config: dict) -> List[dict]:
        """Extraction basée sur la structure HTML"""
        entries: List[dict] = []

        extractors = [
            self._extract_from_lists,      # <ul>, <ol> avec liens
            self._extract_from_tables,     # <table> avec données
            self._extract_from_cards,      # Divs avec classe card/item
            self._extract_from_links,      # Tous les liens avec contexte
            self._extract_from_text        # Fallback ligne par ligne
        ]

        for extractor in extractors:
            try:
                results = extractor(soup, source_url, category, config)
                if results:
                    entries.extend(results)
                    logger.info("Extractor OK", extra={"name": extractor.__name__, "count": len(results)})
                    break  # Premier extracteur qui fonctionne
            except Exception as e:
                logger.warning("Extractor échoué", extra={"name": extractor.__name__, "error": str(e)})
                continue

        return self._deduplicate_entries(entries)

    def _extract_from_lists(self, soup: BeautifulSoup, source_url: str, category: str, config: dict) -> List[dict]:
        """Extraction depuis listes HTML structurées"""
        entries: List[dict] = []

        for list_elem in soup.find_all(['ul', 'ol']):
            items = list_elem.find_all('li', recursive=False) or list_elem.find_all('li')
            if len(items) < 3:
                continue

            for item in items:
                link = item.find('a')
                if not link or not link.get('href'):
                    continue

                name = self._extract_clean_text(link)
                if not self._is_valid_organization_name(name):
                    continue

                url = urljoin(source_url, link['href'])
                description = self._extract_description_from_context(item, link)

                entry = self.create_entry(
                    name=name,
                    website=url,
                    description=description,
                    category=category,
                    source_url=source_url,
                    config=config
                )
                entry = self.enrich_contacts(entry, config)
                entries.append(entry)

        return entries

    def _extract_from_tables(self, soup: BeautifulSoup, source_url: str, category: str, config: dict) -> List[dict]:
        """Extraction depuis tableaux"""
        entries: List[dict] = []

        for table in soup.find_all('table'):
            rows = table.find_all('tr')
            if len(rows) < 2:
                continue

            header_row = rows[0]
            column_mapping = self._detect_table_columns(header_row)

            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])
                if len(cells) < 2:
                    continue

                entry_data = self._extract_from_table_row(cells, column_mapping, source_url)
                if entry_data and self._is_valid_organization_name(entry_data.get('name')):
                    entry = self.create_entry(**entry_data, category=category, source_url=source_url, config=config)
                    entry = self.enrich_contacts(entry, config)
                    entries.append(entry)

        return entries

    def _extract_from_cards(self, soup: BeautifulSoup, source_url: str, category: str, config: dict) -> List[dict]:
        """Extraction basée sur des cartes (div.card, .item, .listing etc.)"""
        entries: List[dict] = []
        selectors = [
            ('div', {'class': re.compile(r'(card|result|item|listing)', re.I)}),
            ('article', {}),
        ]
        for tag, attrs in selectors:
            for card in soup.find_all(tag, attrs=attrs):
                # Nom = texte d'un <h2>/<h3> ou 1er lien
                title = card.find(['h1', 'h2', 'h3']) or card.find('a')
                name = self._extract_clean_text(title) if title else None
                if not self._is_valid_organization_name(name):
                    continue

                link = (title if title and title.name == 'a' else card.find('a'))
                url = urljoin(source_url, link['href']) if link and link.get('href') else None
                desc = self._extract_description_from_context(card, link or title)

                entry = self.create_entry(
                    name=name, website=url, description=desc,
                    category=category, source_url=source_url, config=config
                )
                entry = self.enrich_contacts(entry, config)
                entries.append(entry)
        return entries

    def _extract_from_links(self, soup: BeautifulSoup, source_url: str, category: str, config: dict) -> List[dict]:
        """Extraction opportuniste via tous les liens pertinents"""
        entries: List[dict] = []
        for a in soup.find_all('a', href=True):
            name = self._extract_clean_text(a)
            if not self._is_valid_organization_name(name):
                continue
            href = urljoin(source_url, a['href'])
            # éviter ancres / mailto / tel
            if href.startswith('mailto:') or href.startswith('tel:') or href.endswith('#'):
                continue

            desc = self._extract_description_from_context(a.parent or soup, a)
            entry = self.create_entry(name=name, website=href, description=desc,
                                      category=category, source_url=source_url, config=config)
            entry = self.enrich_contacts(entry, config)
            entries.append(entry)
        return entries

    def _extract_from_text(self, soup: BeautifulSoup, source_url: str, category: str, config: dict) -> List[dict]:
        """Fallback ligne par ligne (ton ancien comportement, affiné)."""
        entries: List[dict] = []
        text = soup.get_text(separator="\n")
        lines = [line.strip() for line in text.split('\n') if line.strip()]

        i = 0
        while i < len(lines):
            line = lines[i]
            if self._is_valid_organization_name(line):
                website = ""
                description = ""

                # Chercher une URL proche
                for j in range(i + 1, min(i + 6, len(lines))):
                    if j < len(lines) and self.is_url(lines[j]):
                        website = self.clean_url(lines[j])

                        desc_parts = []
                        for k in range(j + 1, min(j + 8, len(lines))):
                            if k < len(lines) and self.is_description(lines[k]):
                                desc_parts.append(lines[k])
                            elif (self._is_valid_organization_name(lines[k]) or
                                  self.is_url(lines[k]) or
                                  len(desc_parts) > 3):
                                break
                        description = ' '.join(desc_parts).strip()
                        break

                if self.matches_filters(line, description, config):
                    keep_incomplete = bool(config.get("keep_incomplete", True))
                    if website or len(description) > 15 or keep_incomplete:
                        entry = self.create_entry(
                            name=line, website=website or None, description=description or None,
                            category=category, source_url=source_url, config=config
                        )
                        entry = self.enrich_contacts(entry, config)
                        entries.append(entry)
            i += 1
        return entries

    # --------------------- Heuristiques & utilitaires ---------------------

    def extract_contact_name(self, text):
        """Essaye de retrouver un nom de contact dans un bloc de texte HTML/texte."""
        if not text:
            return None
        t = " ".join(str(text).split())
        for pat in self.CONTACT_PATTERNS:
            m = re.search(pat, t, flags=re.IGNORECASE)
            if m:
                return m.group(1).strip()
        return None

    def is_valid_name(self, text):
        """(Ancienne API) Heuristique : ressemble à un nom d'organisation ?"""
        return self._is_valid_organization_name(text)

    def _is_valid_organization_name(self, name: Optional[str]) -> bool:
        """Validation stricte nom d'organisation"""
        if not name or len(name.strip()) < 3 or len(name.strip()) > 250:
            return False
        s = name.strip()
        exclusions = {
            'home', 'accueil', 'back', 'retour', 'next', 'suivant', 'previous', 'précédent',
            'menu', 'search', 'recherche', 'login', 'connexion', 'register', 'inscription',
            'copyright', 'mentions légales', 'privacy policy', 'terms of service',
            'contact us', 'nous contacter', 'about us', 'à propos',
            'page', 'pages', 'results', 'résultats', 'total', 'showing', 'affichage',
            'sort by', 'trier par', 'filter', 'filtrer', 'category', 'catégorie',
            'today', "aujourd'hui", 'yesterday', 'hier', 'tomorrow', 'demain',
            'monday', 'lundi', 'tuesday', 'mardi', 'wednesday', 'mercredi', 'thursday', 'jeudi',
            'friday', 'vendredi', 'saturday', 'samedi', 'sunday', 'dimanche',
            'one', 'two', 'three', 'un', 'deux', 'trois',
        }
        s_lower = s.lower()
        if s_lower in exclusions:
            return False

        bad_patterns = [
            r'^\d+$',
            r'^[^\w\s]+$',
            r'^(page|p\.)\s*\d+',
            r'^\d+\s*(results?|résultats?)',
            r'^(show|afficher)\s+',
            r'^(click|cliquer)\s+',
        ]
        for pattern in bad_patterns:
            if re.match(pattern, s_lower):
                return False

        # Doit contenir au moins des lettres (latin étendu + thaï)
        if not re.search(r'[a-zA-Z\u00C0-\u024F\u0E00-\u0E7F]', s):
            return False

        # Pas une URL
        if s_lower.startswith(('www.', 'http', 'https', 'ftp')):
            return False

        return True

    def _extract_clean_text(self, node) -> str:
        if not node:
            return ""
        txt = " ".join(node.get_text(" ", strip=True).split())
        return txt[:500]

    def _extract_description_from_context(self, container, ref_node) -> str:
        """Desc = texte voisin raisonnable autour de ref_node."""
        if not container:
            return ""
        # prendre paragraphes proches
        desc_parts = []
        for p in container.find_all(['p', 'small'], limit=3):
            t = self._extract_clean_text(p)
            if 10 <= len(t) <= 500 and t.lower() not in ("read more", "en savoir plus"):
                desc_parts.append(t)
        # si rien, tenter texte du container
        if not desc_parts:
            t = self._extract_clean_text(container)
            if 10 <= len(t) <= 500:
                desc_parts.append(t)
        return " ".join(desc_parts)[:1000]

    def _detect_table_columns(self, header_row) -> Dict[int, str]:
        """Détecte grossièrement les colonnes: name/website/description/phone/email"""
        mapping: Dict[int, str] = {}
        cells = header_row.find_all(['th', 'td'])
        for idx, c in enumerate(cells):
            h = (c.get_text(" ", strip=True) or "").lower()
            if any(k in h for k in ['name', 'nom', 'organisation', 'company']):
                mapping[idx] = 'name'
            elif any(k in h for k in ['site', 'website', 'url', 'lien', 'link']):
                mapping[idx] = 'website'
            elif any(k in h for k in ['description', 'about', 'info', 'présentation']):
                mapping[idx] = 'description'
            elif 'email' in h:
                mapping[idx] = 'email'
            elif any(k in h for k in ['phone', 'téléphone', 'tel']):
                mapping[idx] = 'phone'
        return mapping

    def _extract_from_table_row(self, cells, colmap: Dict[int, str], base_url: str) -> Optional[dict]:
        data: Dict[str, Optional[str]] = {}
        for idx, cell in enumerate(cells):
            key = colmap.get(idx)
            if not key:
                continue
            if key == 'website':
                a = cell.find('a', href=True)
                if a:
                    data['website'] = urljoin(base_url, a['href'])
                else:
                    data['website'] = normalize_url(self._extract_clean_text(cell))
            else:
                data[key] = self._extract_clean_text(cell)
        name = data.get('name') or ""
        if not self._is_valid_organization_name(name):
            return None
        return {
            "name": name,
            "website": data.get("website"),
            "description": data.get("description") or "",
        }

    def is_url(self, text):
        """Heuristique : ressemble à une URL ?"""
        if not text:
            return False
        text = text.strip().lower()
        return (
            text.startswith(('www.', 'http://', 'https://')) or
            any(ext in text for ext in ['.com', '.org', '.net', '.fr', '.th', '.co.uk', '.io', '.co', '.info'])
        )

    def clean_url(self, url):
        """Nettoie & normalise une URL (schéma, utm…)."""
        return normalize_url(url)

    def is_description(self, text):
        """Heuristique : ligne de description plausible ?"""
        if not text or len(text) < 10 or len(text) > 1000:
            return False
        exclusions = [
            'visites depuis', 'signaler', 'modifier', 'copyright',
            'accueil', 'top clics', 'nouveautés', 'nous contacter',
            'proposer un site', 'env.', 'jours', 'partir de'
        ]
        text_lower = text.lower()
        if any(excl in text_lower for excl in exclusions):
            return False
        words = text.split()
        return len(words) >= 3

    def matches_filters(self, name, description, config):
        """Filtre optionnel sur des mots-clés ; sinon accepte pour maximiser la remontée."""
        kw = (config.get('keywords') or '').strip().lower()
        if not kw:
            return True
        blob = f"{name or ''} {description or ''}".lower()
        return kw in blob

    def country_to_region(self, country):
        """Convertit un nom de pays en code région (E.164)."""
        return self.COUNTRY_TO_REGION.get((country or '').strip(), 'TH')

    def http_get(self, url, timeout=10):
        """Récupère une page pour enrichissement (emails/téléphones/réseaux…)."""
        try:
            u = normalize_url(url)
            if not u:
                return ""
            r = requests.get(u, headers=self.headers, timeout=timeout, proxies=self.proxies)
            r.raise_for_status()
            return r.text or ""
        except Exception:
            return ""

    # --------------------- Construction & enrichissement ---------------------

    def create_entry(self, name, website, description, category, source_url, config):
        """Crée une entrée brute ; l'enrichissement (emails/tels/langue…) se fait ensuite."""
        # Pré-seed minimal (un email/tel si présent dans la description)
        pre_emails = extract_emails(description or "")
        region = self.country_to_region(config.get('country'))
        pre_phones = normalize_phone_list(extract_phones(description or ""), default_region=region)

        language_guess = detect_lang(f"{name} {description}", website) or 'unknown'

        return {
            'name': (name or "").strip(),
            'category': category,
            'description': (description or "").strip(),
            'website': website or None,
            'email': pre_emails[0] if pre_emails else None,
            'phone': pre_phones[0] if pre_phones else None,
            'city': normalize_location(f"{name} {description}") or None,
            'country': config.get('country'),
            'language': language_guess,
            'source_url': source_url,
            'profession': config.get('profession'),
            'scraped_at': datetime.now().isoformat(),
            'quality_score': self.calculate_quality_score(name, website, description),
            'normalized_name': normalize_name(name or "")
        }

    # === NOUVELLE orchestration enrichissement (décomposée) ===
    def enrich_contacts(self, entry: dict, config: dict) -> dict:
        """Point d'entrée enrichissement - orchestration only"""
        try:
            entry = self._enrich_from_description(entry)
            if entry.get('website'):
                entry = self._enrich_from_website(entry, config)
            entry = self._normalize_contact_fields(entry, config)
            return entry
        except Exception as e:
            logger.warning("Erreur enrichissement contacts",
                           extra={"name": entry.get('name'), "error": str(e)})
            return entry

    def _enrich_from_description(self, entry: dict) -> dict:
        """Enrichit depuis description uniquement"""
        desc = entry.get('description', '')
        if not desc:
            return entry

        contact_data = {
            'emails': extract_emails(desc),
            'phones': extract_phones(desc),
            'whatsapp': extract_whatsapp(desc),
            'line_id': extract_line_id(desc),
            'telegram': extract_telegram(desc),
            'wechat': extract_wechat(desc),
            'socials': extract_socials(desc)
        }

        for field, values in contact_data.items():
            if not values:
                continue
            if field == 'socials':
                for platform, links in values.items():
                    if links and not entry.get(platform):
                        entry[platform] = links[0] if isinstance(links, list) else links
            else:
                if not entry.get(field):
                    entry[field] = values[0] if isinstance(values, list) else values

        # Nom de contact dans la description
        if not entry.get('contact_name'):
            cn = self.extract_contact_name(desc)
            if cn:
                entry['contact_name'] = cn

        return entry

    def _enrich_from_website(self, entry: dict, config: dict) -> dict:
        """Enrichit depuis pages web avec timeout et retry (léger)"""
        website = entry['website']

        try:
            main_content = self._fetch_page_content(website, timeout=8)
            if main_content:
                entry = self._extract_contacts_from_html(entry, main_content)

            if main_content:
                contact_pages = self._find_contact_pages(main_content, website)
                for page_url in contact_pages[:3]:  # Max 3 pages
                    try:
                        page_content = self._fetch_page_content(page_url, timeout=5)
                        if page_content:
                            entry = self._extract_contacts_from_html(entry, page_content)
                            if entry.get('email') and entry.get('phone'):
                                break
                    except Exception as e:
                        logger.debug("Erreur page contact", extra={"url": page_url, "error": str(e)})
                        continue
                    time.sleep(0.5)

        except Exception as e:
            logger.warning("Erreur enrichissement website", extra={"website": website, "error": str(e)})

        return entry

    def _fetch_page_content(self, url: str, timeout: int = 10) -> Optional[str]:
        """Récupère contenu page avec cache et gestion d'erreur"""
        cache_key = f"page_{hash(url)}"
        if hasattr(self, '_page_cache') and cache_key in getattr(self, '_page_cache', {}):
            return self._page_cache[cache_key]

        try:
            response = requests.get(
                url,
                headers=self.headers,
                timeout=timeout,
                proxies=self.proxies,
                allow_redirects=True
            )
            response.raise_for_status()
            content = response.text

            if not hasattr(self, '_page_cache'):
                self._page_cache = {}
            if len(self._page_cache) < 100:  # Limite cache
                self._page_cache[cache_key] = content

            return content

        except requests.exceptions.Timeout:
            logger.debug("Timeout", extra={"url": url})
            return None
        except requests.exceptions.RequestException as e:
            logger.debug("Erreur requête", extra={"url": url, "error": str(e)})
            return None

    def _extract_contacts_from_html(self, entry: dict, html: str) -> dict:
        """Parse du HTML pour grappiller emails, phones, réseaux, nom de contact."""
        blob = html or ""

        # Emails
        emails = set(extract_emails(blob))
        if entry.get('email'):
            for e in str(entry['email']).replace(',', ';').split(';'):
                e = e.strip()
                if e:
                    emails.add(e)
        if emails:
            entry['email'] = "; ".join(sorted(e for e in emails if e))

        # Phones
        raw_phones = extract_phones(blob)
        if raw_phones:
            region = self.country_to_region(entry.get('country'))
            phones_norm = normalize_phone_list(raw_phones, default_region=region)
            merged = set((entry.get('phone') or "").replace(",", ";").split(";"))
            for p in phones_norm:
                merged.add(p)
            merged_clean = sorted(x.strip() for x in merged if x and x.strip())
            entry['phone'] = "; ".join(merged_clean) if merged_clean else entry.get('phone')

        # Canaux directs
        if not entry.get('whatsapp'):
            wa = extract_whatsapp(blob)
            if wa:
                entry['whatsapp'] = "; ".join(wa)
        if not entry.get('line_id'):
            li = extract_line_id(blob)
            if li:
                entry['line_id'] = "; ".join(li)
        if not entry.get('telegram'):
            tg = extract_telegram(blob)
            if tg:
                entry['telegram'] = "; ".join(tg)
        if not entry.get('wechat'):
            wc = extract_wechat(blob)
            if wc:
                entry['wechat'] = "; ".join(wc)

        # Réseaux sociaux
        socials = extract_socials(blob)
        if isinstance(socials, dict):
            for k, v in socials.items():
                if v and not entry.get(k):
                    link = v[0] if isinstance(v, (list, tuple)) and v else v
                    if link:
                        entry[k] = normalize_url(link)

        # Nom de contact supplémentaire
        if not entry.get('contact_name'):
            cn = self.extract_contact_name(blob)
            if cn:
                entry['contact_name'] = cn

        return entry

    def _find_contact_pages(self, html: str, base_url: str) -> List[str]:
        """Trouve des pages type contact/about/legal à partir d'un HTML existant (utilise aussi utilitaire)."""
        urls = []
        try:
            urls = find_contact_like_links(html, base_url) or []
        except Exception:
            pass

        # Ajouts courants si non présents
        common = ["contact", "about", "a-propos", "contacts", "legal", "mentions", "imprint"]
        for c in common:
            candidate = urljoin(base_url, f"/{c}")
            if candidate not in urls:
                urls.append(candidate)
        # normaliser
        return [normalize_url(u) for u in urls if u]

    def _normalize_contact_fields(self, entry: dict, config: dict) -> dict:
        """Normalisation finale avec validation stricte"""
        # Téléphones en E.164
        if entry.get('phone'):
            region = self.country_to_region(config.get('country'))
            phones = normalize_phone_list([p for p in (entry['phone'].replace(",", ";").split(";")) if p], default_region=region)
            entry['phone'] = "; ".join(phones) if phones else None

        # Validation emails
        if entry.get('email'):
            emails = extract_emails(entry['email'])
            entry['email'] = "; ".join(sorted(set(emails))) if emails else None

        # URLs normalisées
        for field in ['website', 'facebook', 'instagram', 'linkedin']:
            if entry.get(field):
                normalized = normalize_url(entry[field])
                entry[field] = normalized

        # Détection langue finale
        text_for_detection = f"{entry.get('name', '')} {entry.get('description', '')}"
        detected_lang = detect_lang(text_for_detection, entry.get('website'))
        if detected_lang:
            entry['language'] = detected_lang

        # Ville normalisée
        if entry.get('city'):
            entry['city'] = normalize_location(entry['city'])

        # Nom normalisé
        entry['normalized_name'] = normalize_name(entry.get('name') or "")

        return entry

    # --------------------- Déduplication ---------------------

    def _deduplicate_entries(self, items: List[dict], existing: Optional[List[dict]] = None, threshold: int = 90) -> List[dict]:
        """Déduplique avec fuzzy_duplicate contre une liste en cours (existing) et localement."""
        out: List[dict] = list(existing) if existing is not None else []
        base = out[:] if existing is not None else []
        for r in (items if existing is None else items):
            if not fuzzy_duplicate(r, base, threshold=threshold):
                out.append(r)
                base.append(r)
        return out if existing is None else out[len(existing):] if existing is not None else out

    # --------------------- Score ---------------------

    def calculate_quality_score(self, name, website, description):
        """Calcule un score de qualité 1..10 (heuristique simple)."""
        score = 0

        # Nom
        if name and len(name) > 3:
            score += 2

        # Site web
        if website:
            score += 3

        # Description
        if description:
            if len(description) > 20:
                score += 2
            if len(description) > 100:
                score += 1

        # Indices de contact dans la description
        if '@' in (description or ""):
            score += 1
        if re.search(r'[0-9]{2,}', (description or "")):
            score += 1

        return min(score, 10)
