import json
import os

CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config.json')

try:
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        CONFIG = json.load(f)
except FileNotFoundError:
    CONFIG = {
        "app_settings": {"endpoints": [], "main_namespace_uri": "", "name": "Sematheque"},
        "prefixes": {},
        "visualization": {"hidden_properties": [], "label_properties": ["http://www.w3.org/2000/01/rdf-schema#label"]},
        "manual_class_mapping": {}
    }

APP_SETTINGS = CONFIG.get('app_settings') or {}

_eps = APP_SETTINGS.get('endpoints')
if _eps and isinstance(_eps, list) and len(_eps) > 0:
    ENDPOINTS = _eps
else:
    single_url = APP_SETTINGS.get('sparql_endpoint', '')
    ENDPOINTS = [{"name": "Default", "url": single_url}] if single_url else []

SPARQL_ENDPOINT_ACCESS = ENDPOINTS[0]['url'] if ENDPOINTS else ""

MAIN_NAMESPACE = APP_SETTINGS.get('main_namespace_uri', '')

PREFIXES = CONFIG.get('prefixes') or {}

CUSTOM_PREFIX = ""
if "xsd" not in PREFIXES: CUSTOM_PREFIX += "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n"
if "rdf" not in PREFIXES: CUSTOM_PREFIX += "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
if "rdfs" not in PREFIXES: CUSTOM_PREFIX += "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"

for prefix, uri in PREFIXES.items():
    CUSTOM_PREFIX += f"PREFIX {prefix}: <{uri}>\n"

VISUALIZATION = CONFIG.get('visualization') or {}
HIDDEN_PROPERTIES = VISUALIZATION.get('hidden_properties') or []
LABEL_PROPERTIES = VISUALIZATION.get('label_properties') or ["http://www.w3.org/2000/01/rdf-schema#label"]

MANUAL_CLASSES = CONFIG.get('manual_class_mapping') or {}
RESOURCE_TYPES = MANUAL_CLASSES 

PROJECT_INFO = {
    "name": APP_SETTINGS.get('name', 'Sematheque'),
    "domain": "sematheque.app",
    "description": "Explorateur sémantique fédéré",
    "language": APP_SETTINGS.get('language', 'fr'),
    "logo": APP_SETTINGS.get('logo', 'logo/logo.png')
}

UI_CONFIG = {
    "theme_color": "#1f77b4",
    "secondary_color": "#ff7f0e",
    "text_color": "#2c3e50",
    "background_color": "#f8f9fa",
    "card_color": "#ffffff",
    "items_per_page_options": [5, 10, 20, 50, 100],
    "default_items_per_page": 20,
    "app_title": f"SPARQL - {PROJECT_INFO['name']}",
    "app_icon": "fas fa-project-diagram"
}

SPARQL_KEYWORDS = [
    'SELECT', 'WHERE', 'FILTER', 'OPTIONAL', 'UNION', 'ORDER BY',
    'GROUP BY', 'HAVING', 'LIMIT', 'OFFSET', 'DISTINCT', 'ASK', 'CONSTRUCT'
]

QUICK_INSERT_PREFIXES = [
    {"label": f"{p}:", "insert": f"PREFIX {p}: <{u}>"} 
    for p, u in PREFIXES.items()
]

std_prefixes = ["rdf", "rdfs", "owl", "skos", "xsd"]
for p in std_prefixes:
    if p not in PREFIXES:
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