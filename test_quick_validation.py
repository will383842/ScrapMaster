# test_quick_validation.py
import requests
import time
import json

def test_interface_simple():
    """Test l'interface simple avec de vraies données"""
    print("🧪 Test interface simple...")
    
    # Test création projet
    data = {
        'profession': 'Association',
        'country': 'Thaïlande',
        'language': 'fr',
        'keywords': 'expatrié français',
        'projectName': 'Test Interface Simple'
    }
    
    try:
        response = requests.post('http://127.0.0.1:5000/api/quick_search', 
                               json=data, timeout=10)
        
        if response.status_code == 200:
            result = response.json()
            if result.get('success'):
                project_id = result['project_id']
                print(f"✅ Projet créé: {project_id}")
                
                # Attendre un peu et vérifier le statut
                time.sleep(5)
                
                status_response = requests.get(f'http://127.0.0.1:5000/api/search_status/{project_id}')
                if status_response.status_code == 200:
                    status = status_response.json()
                    print(f"📊 Statut: {status.get('status')} - {status.get('total_results', 0)} résultats")
                    return True
                    
        print(f"❌ Erreur: {response.status_code}")
        return False
        
    except Exception as e:
        print(f"❌ Erreur test: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Démarrez d'abord l'application avec: python app.py")
    print("Puis dans un autre terminal lancez ce test")
    
    input("Appuyez sur Entrée quand l'app est démarrée...")
    
    success = test_interface_simple()
    
    if success:
        print("🎉 Interface simple fonctionne avec vraies données!")
    else:
        print("❌ Interface simple nécessite des corrections")