import json
import os

# --- CHARGEMENT CONFIGURATION ---
CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config.json')

try:
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        CONFIG = json.load(f)
except FileNotFoundError:
    # Fallback par défaut si config.json absent
    CONFIG = {
        "app_settings": {"sparql_endpoint": "", "main_namespace_uri": "", "name": "Sematheque"},
        "prefixes": {},
        "visualization": {"hidden_properties": [], "label_properties": ["http://www.w3.org/2000/01/rdf-schema#label"]},
        "manual_class_mapping": {}
    }

# --- PARAMETRES GLOBAUX ---
APP_SETTINGS = CONFIG.get('app_settings', {})
SPARQL_ENDPOINT_ACCESS = APP_SETTINGS.get('sparql_endpoint', '')
MAIN_NAMESPACE = APP_SETTINGS.get('main_namespace_uri', '')

# --- GESTION DES PREFIXES ---
# Export du dictionnaire pur pour utils.py (nettoyage des noms)
PREFIXES = CONFIG.get('prefixes', {})

# Construction de la chaîne pour les requêtes SPARQL
CUSTOM_PREFIX = ""
# Ajout des préfixes standards s'ils ne sont pas dans la config
if "xsd" not in PREFIXES: CUSTOM_PREFIX += "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n"
if "rdf" not in PREFIXES: CUSTOM_PREFIX += "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
if "rdfs" not in PREFIXES: CUSTOM_PREFIX += "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"

for prefix, uri in PREFIXES.items():
    CUSTOM_PREFIX += f"PREFIX {prefix}: <{uri}>\n"

# --- VISUALISATION & CLASSES ---
VISUALIZATION = CONFIG.get('visualization', {})
HIDDEN_PROPERTIES = VISUALIZATION.get('hidden_properties', [])
# Liste des propriétés utilisées pour trouver le label (par ordre de priorité)
LABEL_PROPERTIES = VISUALIZATION.get('label_properties', ["http://www.w3.org/2000/01/rdf-schema#label"])

# Mapping manuel des classes (prioritaire sur la découverte auto)
MANUAL_CLASSES = CONFIG.get('manual_class_mapping', {})
RESOURCE_TYPES = MANUAL_CLASSES # Alias pour compatibilité

# --- INFO PROJET & UI ---
PROJECT_INFO = {
    "name": APP_SETTINGS.get('name', 'Sematheque'),
    "domain": "sematheque.app", # Peut être déplacé en config si besoin
    "description": "Explorateur sémantique générique",
    "language": APP_SETTINGS.get('language', 'fr')
}

UI_CONFIG = {
    "theme_color": "#1f77b4",
    "secondary_color": "#ff7f0e",
    "text_color": "#2c3e50",
    "background_color": "#f8f9fa",
    "card_color": "#ffffff",
    "items_per_page_options": [5, 10, 20, 50, 100],
    "default_items_per_page": 10,
    "app_title": f"SPARQL Endpoint - {PROJECT_INFO['name']}",
    "app_icon": "fas fa-project-diagram"
}

# --- SPARQL UTILS ---
SPARQL_KEYWORDS = [
    'SELECT', 'WHERE', 'FILTER', 'OPTIONAL', 'UNION', 'ORDER BY',
    'GROUP BY', 'HAVING', 'LIMIT', 'OFFSET', 'DISTINCT', 'ASK', 'CONSTRUCT'
]

QUICK_INSERT_PREFIXES = [
    {"label": f"{p}:", "insert": f"PREFIX {p}: <{u}>"} 
    for p, u in PREFIXES.items()
]

# Ajout des standards manquant dans la liste d'insertion rapide
std_prefixes = ["rdf", "rdfs", "owl", "skos", "xsd"]
for p in std_prefixes:
    if p not in PREFIXES:
        # On ajoute les valeurs par défaut classiques si absentes
        uri = ""
        if p == "xsd": uri = "http://www.w3.org/2001/XMLSchema#"
        elif p == "rdfs": uri = "http://www.w3.org/2000/01/rdf-schema#"
        elif p == "rdf": uri = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        elif p == "owl": uri = "http://www.w3.org/2002/07/owl#"
        elif p == "skos": uri = "http://www.w3.org/2004/02/skos/core#"
        
        QUICK_INSERT_PREFIXES.append({"label": f"{p}:", "insert": f"PREFIX {p}: <{uri}>"})

QUICK_INSERT_CLASSES = [
    {"label": label, "insert": f"a <{uri}>"} 
    for label, uri in MANUAL_CLASSES.items()
]

QUERY_TEMPLATES = {
    "Tout voir (Limit 10)": "SELECT * WHERE { ?s ?p ?o } LIMIT 10",
    "Compter les classes": "SELECT ?type (COUNT(?s) as ?count) WHERE { ?s a ?type } GROUP BY ?type ORDER BY DESC(?count)",
    "Lister les propriétés": "SELECT DISTINCT ?p WHERE { ?s ?p ?o } LIMIT 50"
}