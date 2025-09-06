# SCRAPER GÉNÉRIQUE - SCRAPMASTER
# Nom du fichier : generic_scraper.py

import time
import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from utils.normalize import (
    normalize_url, extract_emails, extract_phones, normalize_phone_list,
    extract_socials, detect_language as detect_lang, normalize_location, normalize_name
)
from utils.dedupe import fuzzy_duplicate


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

    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

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
                time.sleep(1)  # Pause respectueuse

        # Dédup finale (fuzzy)
        deduped = []
        for r in all_results:
            if not fuzzy_duplicate(r, deduped, threshold=90):
                deduped.append(r)

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

    def scrape_category(self, base_url, category_name, config):
        """Scrape une catégorie spécifique (parcoupe jusqu'à 10 pages typées /page/index.html)."""
        results = []

        for page in range(1, 11):
            try:
                url = f"{base_url}{page}/index.html"

                response = requests.get(url, headers=self.headers, timeout=10)
                if response.status_code == 404:
                    if page == 1:
                        print("    ⭕ Catégorie vide")
                    break

                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')

                page_results = self.extract_data_from_page(soup, url, category_name, config)
                if not page_results:
                    if page == 1:
                        print("    ⭕ Aucune donnée extraite")
                    break

                # Dédup au fil de l’eau
                for r in page_results:
                    if not fuzzy_duplicate(r, results, threshold=90):
                        results.append(r)

                print(f"    📄 Page {page}: {len(page_results)} entrées")
                time.sleep(0.8)

            except Exception as e:
                print(f"    ❌ Erreur page {page}: {str(e)[:80]}...")
                break

        return results

    def extract_data_from_page(self, soup, source_url, category, config):
        """Extrait les données d'une page 'annuaire' de manière robuste (ligne par ligne)."""
        entries = []

        text = soup.get_text(separator="\n")
        lines = [line.strip() for line in text.split('\n') if line.strip()]

        i = 0
        while i < len(lines):
            line = lines[i]

            # Nom d'organisation plausible ?
            if self.is_valid_name(line):
                website = ""
                description = ""

                # Chercher une URL dans les 5 lignes suivantes
                for j in range(i + 1, min(i + 6, len(lines))):
                    if j < len(lines) and self.is_url(lines[j]):
                        website = self.clean_url(lines[j])

                        # Description dans les 7 lignes suivantes
                        desc_parts = []
                        for k in range(j + 1, min(j + 8, len(lines))):
                            if k < len(lines) and self.is_description(lines[k]):
                                desc_parts.append(lines[k])
                            elif (self.is_valid_name(lines[k]) or
                                  self.is_url(lines[k]) or
                                  len(desc_parts) > 3):
                                break
                        description = ' '.join(desc_parts).strip()
                        break

                # Filtrage (langue/keywords si besoin) + règle mini
                if self.matches_filters(line, description, config):
                    if website or len(description) > 15:
                        entry = self.create_entry(
                            name=line,
                            website=website,
                            description=description,
                            category=category,
                            source_url=source_url,
                            config=config
                        )
                        # Enrichissement : emails multiples, téléphones E.164, réseaux, langue, ville…
                        entry = self.enrich_contacts(entry, config)

                        # Dédup local
                        if not fuzzy_duplicate(entry, entries, threshold=90):
                            entries.append(entry)

            i += 1

        return entries

    # --------------------- Heuristiques & utilitaires ---------------------

    def is_valid_name(self, text):
        """Heuristique : ressemble à un nom d'organisation ?"""
        if not text or len(text) < 3 or len(text) > 250:
            return False

        # Exclusions navigation/footer
        exclusions = [
            'copyright', 'accueil', 'contact', 'mentions légales', 'top clics',
            'signaler un problème', 'modifier', 'visites depuis le', 'nouveautés',
            'nous contacter', 'proposer un site', 'filtrer les sites',
            'circuits sur mesure', 'prix hors vols', "mesure d'audience",
            'roi frequentation', 'voyage confidentiel'
        ]
        text_lower = text.lower()
        if any(excl in text_lower for excl in exclusions):
            return False

        # Doit contenir des lettres
        if not re.search(r'[a-zA-Z\u00C0-\u024F\u0E00-\u0E7F]', text):
            return False

        # Pas une URL
        if text_lower.startswith(('www.', 'http', 'https', 'ftp')):
            return False

        # Pas que des chiffres/symboles
        if re.match(r'^[0-9\s\-_.,;:!?]+$', text):
            return False

        return True

    def is_url(self, text):
        """Heuristique : ressemble à une URL ?"""
        if not text:
            return False
        text = text.strip().lower()
        return (
            text.startswith(('www.', 'http://', 'https://')) or
            any(ext in text for ext in ['.com', '.org', '.net', '.fr', '.th', '.co.uk', '.io', '.co'])
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
        """Vérifie les filtres (langue, etc.). Ici on reste permissif."""
        # Exemple : si config['language'] est précis, on pourrait filtrer,
        # mais par défaut on **accepte** pour maximiser la remontée.
        return True

    def country_to_region(self, country):
        """Convertit un nom de pays en code région (E.164)."""
        return self.COUNTRY_TO_REGION.get((country or '').strip(), 'TH')

    def http_get(self, url, timeout=10):
        """Récupère une page pour enrichissement (emails/téléphones/réseaux…)."""
        try:
            u = normalize_url(url)
            if not u:
                return ""
            r = requests.get(u, headers=self.headers, timeout=timeout)
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
            'name': name.strip(),
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
            # champ interne utile à la dédup
            'normalized_name': normalize_name(name)
        }

    def enrich_contacts(self, entry, config):
        """Renforce email/téléphone/réseaux/langue/ville à partir de la page du site + description."""

        texts = [entry.get('description', '')]

        # Récupérer le HTML de la home si un site est présent
        if entry.get('website'):
            html = self.http_get(entry['website'], timeout=8)
            if html:
                texts.append(html)

        blob = " ".join(texts)

        # ---- Emails (tous, dédupliqués) ----
        emails = set(extract_emails(blob))
        if entry.get('email'):
            emails.add(entry['email'])
        entry['email'] = "; ".join(sorted(emails)) if emails else None

        # ---- Téléphones (E.164 selon le pays) ----
        raw_phones = extract_phones(blob)
        if entry.get('phone'):
            raw_phones.append(entry['phone'])
        region = self.country_to_region(entry.get('country'))
        phones_norm = normalize_phone_list(raw_phones, default_region=region)
        entry['phone'] = "; ".join(phones_norm) if phones_norm else None

        # ---- Réseaux sociaux (prend le premier lien trouvé pour chaque type) ----
        socials = extract_socials(blob)
        for k, v in socials.items():
            if v and not entry.get(k):
                entry[k] = normalize_url(v[0])

        # ---- Langue (si inconnue) ----
        if not entry.get('language') or entry['language'] == 'unknown':
            entry['language'] = detect_lang(blob, entry.get('website')) or entry.get('language') or 'unknown'

        # ---- Ville (normalisation légère) ----
        if entry.get('city'):
            entry['city'] = normalize_location(entry['city'])

        # ---- Nom normalisé (utile à la dédup) ----
        entry['normalized_name'] = normalize_name(entry.get('name'))

        return entry

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
