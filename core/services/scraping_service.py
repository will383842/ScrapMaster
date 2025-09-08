class ScrapingService:
    def __init__(self, db_path: str):
        self.db_path = db_path
        
    def start_project_scraping(self, project_id: int) -> str:
        """Démarre scraping, retourne job_id"""
        # Validation projet existe
        # Vérification statut (pas déjà en cours)
        # Démarrage thread avec gestion propre
        # Retour immédiat avec job_id
        
    def get_scraping_status(self, job_id: str) -> dict:
        """Statut d'un job de scraping"""