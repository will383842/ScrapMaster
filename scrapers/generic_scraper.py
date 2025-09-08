# SCRAPER GÉNÉRIQUE - SCRAPMASTER
# Nom du fichier : generic_scraper.py

import os
import random
import time
import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from utils.ua import pick_user_agent
from utils.normalize import (
    normalize_url, extract_emails, extract_phones, normalize_phone_list,
    extract_socials, detect_language as detect_lang, normalize_location, normalize_name,
    extract_whatsapp, extract_line_id, extract_telegram, extract_wechat, find_contact_like_links
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
        """
        Scrape une catégorie/cible avec pagination adaptative.
        - Si c'est un annuaire paginé (URL finit par '/', ou contient /cat-, /category, /page/),
          on parcourt jusqu'à self.max_pages avec patron /{page}/index.html.
        - Sinon, on traite la cible telle quelle (1 page).
        """
        results = []

        # Heuristique: annuaire paginé vs. homepage/fiche
        is_directory = (
            base_url.endswith("/") or
            "/cat-" in base_url or "/category" in base_url or "/page/" in base_url
        )

        pages = range(1, self.max_pages + 1) if is_directory else range(1, 2)

        for page in pages:
            try:
                if is_directory:
                    # Patron paginé (comme Thailand Guide)
                    url = f"{base_url}{page}/index.html" if base_url.endswith("/") else f"{base_url}/{page}/index.html"
                else:
                    # Cible "simple": on ne touche pas à l'URL
                    url = base_url

                response = requests.get(url, headers=self.headers, timeout=10, proxies=self.proxies)
                if response.status_code == 404:
                    if page == 1:
                        print("    ⭕ Catégorie/cible vide")
                    break

                if response.status_code != 200:
                    print(f"    ⚠️ HTTP {response.status_code} pour {url}")
                    if not is_directory:
                        break
                    continue

                soup = BeautifulSoup(response.content, 'html.parser')
                page_results = self.extract_data_from_page(soup, url, category_name, config)
                if not page_results and not is_directory:
                    print("    ⭕ Rien d'extractible sur la cible")
                    break

                # Dédup au fil de l’eau
                for r in page_results:
                    if not fuzzy_duplicate(r, results, threshold=90):
                        results.append(r)

                print(f"    📄 Page {page}: {len(page_results)} entrées (total {len(results)})")
                time.sleep(self.delay_s + random.random()*0.4)

            except Exception as e:
                print(f"    ❌ Erreur page {page}: {str(e)[:120]}...")
                if not is_directory:
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

                # Filtrage (keywords optionnel) + règle mini
                if self.matches_filters(line, description, config):
                    keep_incomplete = bool(config.get("keep_incomplete", True))
                    if website or len(description) > 15 or keep_incomplete:
                        entry = self.create_entry(
                            name=line,
                            website=website or None,
                            description=description or None,
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
            # champ interne utile à la dédup
            'normalized_name': normalize_name(name or "")
        }

    def enrich_contacts(self, entry, config):
        """Renforce email/téléphone/réseaux/langue/ville à partir de la page du site + description."""
        texts = [entry.get('description', '')]

        html = ""
        # Récupérer le HTML de la home si un site est présent
        if entry.get('website'):
            html = self.http_get(entry['website'], timeout=8)
            if html:
                texts.append(html)

        blob = " ".join(texts)

        # ---- Nom de contact (heuristique) ----
        if not entry.get('contact_name'):
            cn = self.extract_contact_name(blob)
            if cn:
                entry['contact_name'] = cn

        # ---- Emails (tous, dédupliqués) ----
        emails = set(extract_emails(blob))
        if entry.get('email'):
            for e in str(entry['email']).replace(',', ';').split(';'):
                e = e.strip()
                if e:
                    emails.add(e)
        entry['email'] = "; ".join(sorted(emails)) if emails else None

        # ---- Téléphones (E.164 selon le pays) ----
        raw_phones = extract_phones(blob)
        if entry.get('phone'):
            raw_phones.extend([p.strip() for p in str(entry['phone']).replace(',', ';').split(';') if p.strip()])
        region = self.country_to_region(entry.get('country'))
        phones_norm = normalize_phone_list(raw_phones, default_region=region)
        entry['phone'] = "; ".join(phones_norm) if phones_norm else None

        # >>> LOG OPTIONNEL : active avec SCRAPMASTER_DEBUG_CONTACTS=1
        if os.getenv("SCRAPMASTER_DEBUG_CONTACTS", "0") == "1":
            if entry.get('email') or entry.get('phone'):
                print(f"    ✉️/📞 {entry.get('email') or '-'} | {entry.get('phone') or '-'}")

        # ---- Canaux directs additionnels (WhatsApp / Line ID / Telegram / WeChat) ----
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

        # ---- Réseaux sociaux (liens) ----
        socials = extract_socials(blob)
        if isinstance(socials, dict):
            for k, v in socials.items():
                if v and not entry.get(k):
                    link = v[0] if isinstance(v, (list, tuple)) and v else v
                    if link:
                        entry[k] = normalize_url(link)

        # ---- Suivre jusqu'à 5 pages "Contact/About/Legal" pour grappiller des contacts ----
        extra_htmls = []
        if entry.get('website') and html:
            try:
                for u in find_contact_like_links(html, entry['website'])[:5]:
                    h = self.http_get(u, timeout=8)
                    if h:
                        extra_htmls.append(h)
                    if len(extra_htmls) >= 5:
                        break
            except Exception:
                pass

        if extra_htmls:
            blob2 = " ".join(extra_htmls)

            # Emails supplémentaires (sans écraser les existants)
            more_emails = extract_emails(blob2)
            if more_emails:
                merged = set((entry.get('email') or "").replace(",", ";").split(";"))
                for e in more_emails:
                    merged.add(e)
                merged_clean = sorted(x.strip() for x in merged if x and x.strip())
                entry['email'] = "; ".join(merged_clean) if merged_clean else entry.get('email')

            # Phones supplémentaires
            more_phones = extract_phones(blob2)
            if more_phones:
                region = self.country_to_region(entry.get('country'))
                phones_norm2 = normalize_phone_list(more_phones, default_region=region)
                if phones_norm2:
                    merged = set((entry.get('phone') or "").replace(",", ";").split(";"))
                    for p in phones_norm2:
                        merged.add(p)
                    merged_clean = sorted(x.strip() for x in merged if x and x.strip())
                    entry['phone'] = "; ".join(merged_clean) if merged_clean else entry.get('phone')

            # Canaux directs si toujours vides
            if not entry.get('whatsapp'):
                wa2 = extract_whatsapp(blob2)
                if wa2:
                    entry['whatsapp'] = "; ".join(wa2)
            if not entry.get('line_id'):
                li2 = extract_line_id(blob2)
                if li2:
                    entry['line_id'] = "; ".join(li2)
            if not entry.get('telegram'):
                tg2 = extract_telegram(blob2)
                if tg2:
                    entry['telegram'] = "; ".join(tg2)
            if not entry.get('wechat'):
                wc2 = extract_wechat(blob2)
                if wc2:
                    entry['wechat'] = "; ".join(wc2)

        # ---- Langue (si inconnue) ----
        if not entry.get('language') or entry['language'] == 'unknown':
            entry['language'] = detect_lang(blob, entry.get('website')) or entry.get('language') or 'unknown'

        # ---- Ville (normalisation légère) ----
        if entry.get('city'):
            entry['city'] = normalize_location(entry['city'])

        # ---- Nom normalisé (utile à la dédup) ----
        entry['normalized_name'] = normalize_name(entry.get('name') or "")

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
