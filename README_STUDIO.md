
# ScrapMaster Studio — Patch UI (types/pays/langues + éditeur de scripts)

Ce patch ajoute à ton projet :
- Une page **/studio** avec filtres **Types / Pays / Langues** et un champ mots-clés.
- Un **éditeur de scripts** (CodeMirror) pour modifier les fichiers dans `ScrapMaster/scrapers` directement depuis l'interface.
- Des endpoints JSON pour lister/charger/sauver des scripts et **lancer un scraping** en tâche de fond.
- Un **export CSV** mappé à la trame Excel (colonnes attendues).

## Installation

1. Copie les fichiers de ce dossier dans ton projet, en respectant l'arborescence :
```
ScrapMaster/
  ui_blueprint.py
  templates/ui_studio.html
  static/js/   (si tu veux séparer le JS)
```
2. Dans `app.py`, en bas, enregistre le blueprint :
```python
from ui_blueprint import bp as studio_bp
app.register_blueprint(studio_bp)
```

3. (Optionnel) Configure l'emplacement de la DB et des scrapers via variables d'env :
```
export SCRAPMASTER_DB=/chemin/vers/ScrapMaster/database/scrapmaster.db
export SCRAPMASTER_SCRAPERS=/chemin/vers/ScrapMaster/scrapers
```

4. Démarre l'appli et va sur **/studio**.

## Notes

- L'endpoint `/studio/api/run` lance un thread qui invoque `ScrapingEngine.run(country=..., categories=[...], languages=[...], keywords=...)`. Adapte les noms d'arguments si ton `ScrapingEngine` diffère.
- `/studio/api/export` produit un CSV conforme à la trame Excel (colonnes manquantes remplies vides).
- L'éditeur sauvegarde un **backup `.bak`** à chaque enregistrement.
- Le tableau "Résultats récents" s'appuie sur un endpoint facultatif `/api/recent_results`. Si tu ne l'as pas, tout marche quand même ; utilise l'export CSV.

## Sécurité

- L'éditeur permet d'écrire des `.py` dans `scrapers/` : ne l'expose pas publiquement.
- Évite d'exécuter en production des scripts non audités.
- Ajoute un contrôle d'accès (auth simple/Flask-Login) si besoin.
