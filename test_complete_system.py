# test_complete_system.py
import sys
import os
import time
from datetime import datetime

# Ajouter le dossier racine au path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_semantic_search():
    """Test du moteur de recherche sémantique"""
    print("🧠 Test recherche sémantique...")
    
    try:
        from config.semantic_database import semantic_db
        
        # Test expansion profession
        expanded = semantic_db.expand_profession_keywords("avocat")
        print(f"✅ Expansion 'avocat': {len(expanded)} termes")
        print(f"   Exemples: {expanded[:5]}")
        
        # Test variations recherche
        variations = semantic_db.generate_search_variations("avocat", "thaïlande", "immigration")
        print(f"✅ Variations recherche: {len(variations)} requêtes")
        print(f"   Exemples: {variations[:3]}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur test sémantique: {e}")
        return False

def test_structured_extraction():
    """Test extraction données structurées"""
    print("📊 Test extraction données structurées...")
    
    try:
        from extractors.structured_data_extractor import structured_extractor
        
        # HTML de test avec JSON-LD
        test_html = '''
        <html>
        <script type="application/ld+json">
        {
            "@type": "Organization",
            "name": "Cabinet Test",
            "email": "contact@test.com",
            "telephone": "+33123456789",
            "address": {
                "streetAddress": "123 Test Street",
                "addressLocality": "Paris"
            }
        }
        </script>
        </html>
        '''
        
        results = structured_extractor.extract_all_structured_data(test_html, "http://test.com")
        print(f"✅ Extraction structurée: {len(results)} entrées")
        
        if results:
            print(f"   Exemple: {results[0].get('name')} - {results[0].get('email')}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur test extraction: {e}")
        return False

def test_enrichment():
    """Test enrichisseur multi-sources"""
    print("🔍 Test enrichissement multi-sources...")
    
    try:
        from enrichers.multi_source_enricher import multi_enricher
        
        # Entrée de test
        test_entry = {
            'name': 'Test Organization',
            'website': 'https://example.com',
            'description': 'Test legal services organization'
        }
        
        test_config = {
            'country': 'France',
            'profession': 'Avocat'
        }
        
        enriched = multi_enricher.enrich_entry_complete(test_entry, test_config)
        
        print(f"✅ Enrichissement: {len(enriched)} champs")
        quality = enriched.get('enrichment_quality', 0)
        print(f"   Score qualité: {quality}/10")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur test enrichissement: {e}")
        return False

def test_complete_scraping():
    """Test complet du scraping"""
    print("🚀 Test scraping complet...")
    
    try:
        from scraper_engine import ScrapingEngine
        
        # Configuration de test
        test_config = {
            'name': 'Test Scraping Complet',
            'profession': 'Association',
            'country': 'Thaïlande',
            'language': 'fr',
            'sources': {'keywords': 'expatrié, communauté'},
            'keep_incomplete': True
        }
        
        engine = ScrapingEngine()
        start_time = time.time()
        
        results = engine.run_scraping(test_config)
        
        duration = time.time() - start_time
        
        print(f"✅ Scraping terminé en {duration:.2f}s")
        print(f"   Résultats: {len(results)} entrées")
        
        if results:
            # Analyse qualité
            high_quality = [r for r in results if r.get('quality_score', 0) >= 7]
            enriched = [r for r in results if r.get('enrichment_quality', 0) > 0]
            
            print(f"   Haute qualité: {len(high_quality)}")
            print(f"   Enrichis: {len(enriched)}")
            
            # Exemple de résultat
            example = results[0]
            print(f"   Exemple: {example.get('name')} - {example.get('email', 'N/A')}")
        
        return len(results) > 0
        
    except Exception as e:
        print(f"❌ Erreur test scraping: {e}")
        return False

def test_database_integration():
    """Test intégration base de données"""
    print("💾 Test intégration base de données...")
    
    try:
        import sqlite3
        from app import DATABASE
        
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()
        
        # Vérifier tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        required_tables = ['projects', 'results', 'professions', 'countries', 'languages']
        existing_tables = [table[0] for table in tables]
        
        all_tables_exist = all(table in existing_tables for table in required_tables)
        
        print(f"✅ Tables DB: {len(existing_tables)} (requis: {all_tables_exist})")
        
        # Test insertion
        cursor.execute('''
            INSERT INTO projects (name, profession, country, language, sources, status)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', ('Test Project', 'Test', 'Test', 'fr', '{}', 'test'))
        
        test_project_id = cursor.lastrowid
        
        # Test résultat
        cursor.execute('''
            INSERT INTO results (project_id, name, email, phone, scraped_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (test_project_id, 'Test Result', 'test@example.com', '+33123456789'))
        
        # Nettoyage
        cursor.execute('DELETE FROM results WHERE project_id = ?', (test_project_id,))
        cursor.execute('DELETE FROM projects WHERE id = ?', (test_project_id,))
        
        conn.commit()
        conn.close()
        
        print("✅ Intégration DB: OK")
        return True
        
    except Exception as e:
        print(f"❌ Erreur test DB: {e}")
        return False

def main():
    """Test complet du système"""
    print("=" * 60)
    print("🧪 TESTS COMPLETS SCRAPMASTER AVANCÉ")
    print("=" * 60)
    
    tests = [
        ("Recherche sémantique", test_semantic_search),
        ("Extraction structurée", test_structured_extraction),
        ("Enrichissement multi-sources", test_enrichment),
        ("Intégration base de données", test_database_integration),
        ("Scraping complet", test_complete_scraping),
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        print(f"\n🔍 {test_name}...")
        try:
            success = test_func()
            results[test_name] = success
            status = "✅ PASS" if success else "❌ FAIL"
            print(f"   {status}")
        except Exception as e:
            results[test_name] = False
            print(f"   ❌ ERREUR: {e}")
    
    # Résumé
    print("\n" + "=" * 60)
    print("📊 RÉSUMÉ DES TESTS")
    print("=" * 60)
    
    total_tests = len(tests)
    passed_tests = sum(results.values())
    
    for test_name, success in results.items():
        status = "✅" if success else "❌"
        print(f"{status} {test_name}")
    
    print(f"\nRésultat: {passed_tests}/{total_tests} tests réussis")
    
    if passed_tests == total_tests:
        print("🎉 TOUS LES TESTS PASSENT - SYSTÈME OPÉRATIONNEL!")
    elif passed_tests >= total_tests * 0.8:
        print("⚠️ Système majoritairement fonctionnel")
    else:
        print("❌ Système nécessite des corrections")

if __name__ == "__main__":
    main()