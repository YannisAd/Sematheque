import os
import time
import logging
import math
import uuid
import json
import urllib.parse
from functools import wraps
from datetime import datetime

from flask import Flask, render_template, request, redirect, url_for, jsonify, session
import pandas as pd

from Constants import (
    CONFIG, UI_CONFIG, PROJECT_INFO, SPARQL_KEYWORDS, 
    QUICK_INSERT_PREFIXES, QUICK_INSERT_CLASSES, QUERY_TEMPLATES
)
from sparql_queries import (
    get_classes, query_sparql, get_resources_by_type, search_resources,
    execute_raw_query, get_resource_metadata, get_properties, 
    get_unique_values, build_sparql_query, get_bulk_details,
    get_ontology_structure, get_graph_exploration
)
from utils import format_property_name, pivot_data_for_visualization

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'sematheque_generic_key_2025')
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024
app.config['MAX_JSON_LENGTH'] = 50 * 1024 * 1024
app.config['SESSION_TYPE'] = 'filesystem'

_cache = {}
visits = []

TEMP_VIS_DIR = os.path.join(os.getcwd(), 'temp_vis_data')
if not os.path.exists(TEMP_VIS_DIR):
    os.makedirs(TEMP_VIS_DIR)

def cached(timeout=3600):
    """Décorateur pour la mise en cache simple en mémoire."""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if request.args.get('q') or request.args.get('term'):
                return f(*args, **kwargs)
                
            cache_key = f.__name__ + str(args) + str(sorted(kwargs.items()))
            if cache_key in _cache:
                return _cache[cache_key]
            
            result = f(*args, **kwargs)
            _cache[cache_key] = result
            return result
        return decorated_function
    return decorator

@app.context_processor
def inject_global_vars():
    """Injecte les variables globales et les types disponibles dans les templates."""
    try:
        types = get_classes()
    except Exception:
        types = []
    return dict(ui_config=UI_CONFIG, project_info=PROJECT_INFO, global_available_types=types)

@cached(3600)
def cached_query_sparql(resource):
    """Version mise en cache de la requête de détails d'une ressource."""
    return query_sparql(resource)

@cached(3600)
def cached_get_resources_by_type(resource_type):
    """Version mise en cache de la récupération des ressources par type."""
    return get_resources_by_type(resource_type)

@app.route('/')
def index():
    """Page d'accueil et compteur de visites basique."""
    visits.append((request.remote_addr, datetime.now()))
    return render_template('index.html')

@app.route('/stats')
def stats():
    """Retourne les statistiques de connexion."""
    unique_ips = len(set(ip for ip, _ in visits))
    return {"total_connexions": len(visits), "utilisateurs_uniques": unique_ips}

@app.route('/about')
def about():
    """Page À propos."""
    return render_template('about.html')

@app.route('/structure')
def structure():
    """Page décrivant la structure des données."""
    return render_template('structure.html')

@app.route('/mention')
def mention():
    """Page des mentions légales."""
    return render_template('mention.html')

@app.route('/projet')
def equipe():
    """Page de présentation du projet/équipe."""
    return render_template('projet.html')

@app.route('/parcours', methods=['GET', 'POST'])
def parcours():
    """Gère la navigation par types de ressources."""
    available_classes = get_classes()
    default_label = available_classes[0]['label'] if available_classes else "Personne"
    preselected_uri = request.args.get('preselected_uri')
    preselected_label = request.args.get('preselected_label')
    preselected_type = request.args.get('preselected_type')
    
    if request.method == 'POST':
        resource_type = request.form.get('resource_type', default_label)
        selected_label = request.form.get('resource_search')
        if selected_label:
            resources = cached_get_resources_by_type(resource_type)
            res = next((r for r in resources if r['label'] == selected_label), None)
            if res:
                session['current_resource'] = f"<{res['uri']}>"
                return redirect(url_for('explore'))
        return redirect(url_for('parcours', resource_type=resource_type))
    
    resource_type = preselected_type or request.args.get('resource_type', default_label)
    resources = cached_get_resources_by_type(resource_type)
    resource_data = [{'uri': r['uri'], 'label': r['label']} for r in resources]
    
    return render_template(
        'parcours.html',
        resource_type=resource_type,
        resource_labels=json.dumps(resource_data),
        selected_resource_uri=preselected_uri,
        selected_resource_label=preselected_label,
        available_types=available_classes
    )

@app.route('/search', methods=['GET', 'POST'])
def search():
    """Page de recherche avancée."""
    if request.method == 'POST':
        text = request.form.get('query_text')
        return redirect(url_for('filter_results', query_text=text))
    return render_template('search.html')

@app.route('/filter_results', methods=['GET', 'POST'])
def filter_results():
    """Affiche les résultats de recherche avec pagination."""
    try:
        params = request.form if request.method == 'POST' else request.args
        query_text = params.get('query_text', '')
        filter_type = params.get('filter_type', '')
        try:
            page = int(params.get('page', 1))
            per_page = int(params.get('per_page', 20))
        except ValueError:
            page = 1
            per_page = 20
        
        if not query_text:
            return redirect(url_for('search'))
        
        results_df = search_resources(query_text, limit=500, resource_type=filter_type)
        all_results = results_df.to_dict('records')
        
        total_results = len(all_results)
        total_pages = math.ceil(total_results / per_page) if total_results > 0 else 1
        start = (page - 1) * per_page
        end = start + per_page
        paginated_results = all_results[start:end]
        all_types = sorted(list(set(r['type'] for r in all_results if r.get('type'))))
        
        return render_template(
            'search_results.html',
            query_text=query_text,
            filter_type=filter_type,
            results=paginated_results,
            all_types=all_types,
            current_page=page,
            total_pages=total_pages,
            total_results=total_results
        )
    except Exception as e:
        logger.error(f"Erreur filter_results: {e}")
        return render_template('search.html', error=str(e))

@app.route('/explore')
def explore():
    """Page d'exploration détaillée d'une ressource."""
    current_resource = session.get('current_resource')
    
    if not current_resource:
        all_classes = get_classes()
        if all_classes:
            first_class_uri = all_classes[0]['uri']
            q_init = f"SELECT ?s WHERE {{ ?s a <{first_class_uri}> }} LIMIT 1"
            df_init = execute_raw_query(q_init)
            if not df_init.empty:
                current_resource = f"<{df_init.iloc[0]['s']}>"
                session['current_resource'] = current_resource
            else:
                return render_template('explore.html', error="La classe par défaut ne contient aucune ressource.")
        else:
            return render_template('explore.html', error="Impossible de déterminer les classes.")

    df_initial = cached_query_sparql(current_resource)
    metadata = get_resource_metadata(current_resource)
    properties = []
    filters = {}
    
    if not df_initial.empty:
        for _, row in df_initial.iterrows():
            formatted = format_property_name(row['Property'])
            if formatted:
                if row['Property'] not in filters:
                    val_label = row.get('ValueLabel', row.get('Value', ''))
                    filters[row['Property']] = [(val_label, "=")] if val_label else []
                p_info = {'uri': row['Property'], 'label': formatted}
                if p_info not in properties:
                    properties.append(p_info)

    return render_template(
        'explore.html',
        current_resource=current_resource,
        properties=properties,
        filters=filters,
        metadata=metadata,
        df_initial=df_initial,
        available_types=get_classes()
    )

@app.route('/update_resource/<path:resource_uri>')
def update_resource(resource_uri):
    """Met à jour la ressource courante dans la session et redirige vers l'exploration."""
    resource_uri = urllib.parse.unquote(resource_uri)
    if not resource_uri.startswith('<'):
        resource_uri = f"<{resource_uri}>"
    session['current_resource'] = resource_uri
    return redirect(url_for('explore'))

@app.route('/get_properties')
def get_all_properties_api():
    """API retournant les propriétés disponibles pour les listes déroulantes (Select2)."""
    search_text = request.args.get('q', '') or request.args.get('term', '')
    properties = get_properties(search_text=search_text, limit=50)
    
    options = []
    for p in properties:
        if p['uri'] == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type':
            continue
        formatted = format_property_name(p['uri'])
        if formatted: 
            options.append({'id': p['uri'], 'text': formatted, 'label': formatted, 'uri': p['uri']})
            
    return jsonify({"results": options})

@app.route('/get_property_values')
def get_property_values_api():
    """API retournant les valeurs uniques pour une propriété donnée."""
    prop_uri = request.args.get('property_uri')
    search_text = request.args.get('value') or request.args.get('q')
    
    if not prop_uri:
        return jsonify({'success': False, 'values': []})
    
    prop_uri = urllib.parse.unquote(prop_uri)
    
    if prop_uri == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type':
        classes = get_classes()
        return jsonify({
            'success': True,
            'values': [{'value': c['label'], 'uri': c['uri'], 'is_uri': True} for c in classes]
        })

    values = get_unique_values(prop_uri, search_text=search_text, limit=50)
    if values is None:
        values = []
    return jsonify({'success': True, 'values': values})

@app.route('/execute_query', methods=['POST'])
def execute_query():
    """API exécutant une requête SPARQL construite via des filtres."""
    try:
        filters = request.json.get('filters', {})
        query = build_sparql_query(filters)
        df = execute_raw_query(query)
        results = []
        if not df.empty:
            rename_map = {'subject': 'SubjectURI', 'subjectLabel': 'SubjectLabel'}
            df.rename(columns=rename_map, inplace=True)
            results = df.to_dict('records')
        return jsonify({'success': True, 'results': results, 'query': query, 'count': len(results)})
    except Exception as e:
        logger.error(f"Execute Error: {e}")
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/resource_details')
def resource_details_api():
    """API retournant les détails complets d'une ressource."""
    uri = request.args.get('uri')
    if not uri:
        return jsonify({'success': False})
    try:
        df = cached_query_sparql(uri)
        results = []
        if not df.empty:
            df.rename(columns={'property': 'Property', 'value': 'Value', 'valueLabel': 'ValueLabel'}, inplace=True)
            for _, row in df.iterrows():
                prop_uri = row.get('Property', '')
                prop_label = format_property_name(prop_uri)
                if not prop_label:
                    prop_label = prop_uri.split('/')[-1].split('#')[-1]
                results.append({
                    'Property': prop_uri,
                    'PropertyLabel': prop_label,
                    'Value': row.get('Value', ''),
                    'ValueLabel': row.get('ValueLabel') or row.get('Value', '')
                })
        return jsonify({'success': True, 'details': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/prepare_visualization', methods=['POST'])
def prepare_visualization():
    """
    Prépare les données pour la visualisation, stocke le résultat et nettoie les vieux fichiers.
    Supprime les fichiers temporaires vieux de plus de 2 heures (7200 secondes).
    """
    try:
        # 1. Nettoyage automatique
        now = time.time()
        if os.path.exists(TEMP_VIS_DIR):
            for filename in os.listdir(TEMP_VIS_DIR):
                file_path = os.path.join(TEMP_VIS_DIR, filename)
                # Si le fichier est plus vieux que 2h
                if filename.endswith('.json') and os.path.isfile(file_path):
                    try:
                        if os.path.getmtime(file_path) < now - 7200:
                            os.remove(file_path)
                    except OSError:
                        pass # Fichier peut-être verrouillé ou déjà supprimé

        # 2. Traitement des données
        data = request.get_json()
        input_results = data.get('visualization_data', [])
        if not input_results:
            return jsonify({'success': False, 'message': 'Aucune donnée reçue'})
            
        uris = [r['SubjectURI'] for r in input_results if 'SubjectURI' in r]
        df_details = get_bulk_details(uris)
        
        if df_details.empty:
            final_data = input_results
        else:
            final_data = pivot_data_for_visualization(df_details)
        
        vis_id = str(uuid.uuid4())
        file_path = os.path.join(TEMP_VIS_DIR, f"{vis_id}.json")
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(final_data, f)
        session['vis_id'] = vis_id
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/visualize', methods=['GET', 'POST'])
def visualize():
    """Page de visualisation des données (tableau/graphe)."""
    initial_data = []
    vis_id = session.get('vis_id')
    
    if vis_id:
        file_path = os.path.join(TEMP_VIS_DIR, f"{vis_id}.json")
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    initial_data = json.load(f)
            except Exception:
                pass
    elif request.method == 'POST':
        try:
            if request.is_json:
                initial_data = request.json.get('visualization_data', [])
            else:
                initial_data = json.loads(request.form.get('visualization_data', '[]'))
        except Exception:
            pass
    
    columns = ['Label', 'URI']
    if initial_data and isinstance(initial_data, list) and len(initial_data) > 0 and isinstance(initial_data[0], dict):
         keys = set()
         for item in initial_data:
             keys.update(item.keys())
         columns = [k for k in list(keys) if k not in ['SubjectURI', 'URI', 'Value', 'Property']]
         
    return render_template(
        'visualization.html',
        initial_data=json.dumps(initial_data),
        columns=columns,
        timestamp=int(time.time())
    )

@app.route('/get_visualization_data', methods=['POST'])
def get_visualization_data():
    """API pour récupérer les données formatées pour la visualisation."""
    try:
        data = request.get_json()
        results = data.get('results', [])
        processed = []
        for res in results:
            processed.append({'URI': res.get('SubjectURI', ''), 'Label': res.get('SubjectLabel', '')})
        
        columns = list(set().union(*(d.keys() for d in processed)))
        if 'URI' in columns:
            columns.remove('URI')
        return jsonify({"success": True, "data": processed, "columns": columns})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/save_results_for_export', methods=['POST'])
def save_results_for_export():
    """Sauvegarde temporaire des résultats en session pour l'export."""
    session['results'] = request.json.get('results', [])
    return jsonify({"success": True})

@app.route('/export/<format>', methods=['POST'])
def export_data(format):
    """Exporte les données au format CSV ou JSON."""
    try:
        results = request.get_json().get('results', [])
        uris = [r['SubjectURI'] for r in results if 'SubjectURI' in r]
        df_details = get_bulk_details(uris)
        
        if not df_details.empty:
            df_pivot = pd.DataFrame(pivot_data_for_visualization(df_details))
        else:
            df_pivot = pd.DataFrame(results)
            
        if format == 'csv':
            return app.response_class(
                df_pivot.to_csv(index=False),
                mimetype='text/csv',
                headers={"Content-Disposition": "attachment;filename=export.csv"}
            )
        elif format == 'json':
            return app.response_class(
                df_pivot.to_json(orient='records'),
                mimetype='application/json',
                headers={"Content-Disposition": "attachment;filename=export.json"}
            )
        return jsonify({"error": "Format non supporté"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/visualizationNetwork')
def visualization_network():
    """Page de visualisation en réseau."""
    return render_template('visualizationNetwork.html')

@app.route('/api/network/data', methods=['GET', 'POST'])
def network_data_api():
    """API pour stocker/récupérer les données du graphe en session."""
    if request.method == 'POST':
        session['network_data'] = request.get_json()
        return jsonify({'success': True})
    return jsonify(session.get('network_data', {}))

@app.route('/ontology')
def ontology_view():
    """Page de visualisation de l'ontologie."""
    try:
        return render_template(
            'ontology.html',
            ontology_data=json.dumps(get_ontology_structure()),
            timestamp=int(time.time())
        )
    except Exception as e:
        return render_template('ontology.html', ontology_data="{}", error=str(e))

@app.route('/api/ontology/structure')
def ontology_structure_api():
    """API retournant la structure de l'ontologie."""
    return jsonify(get_ontology_structure())

@app.route('/api/graph/explore', methods=['POST'])
def graph_explore_api():
    """API d'exploration des voisins d'un nœud pour le graphe."""
    uri = request.form.get('uri')
    depth = int(request.form.get('depth', 2))
    if not uri:
        return jsonify({'error': 'URI manquante'}), 400
    return jsonify(get_graph_exploration(uri, depth))

@app.route('/api/resource_tree', methods=['POST'])
def resource_tree_api():
    """API stub pour l'arbre des ressources."""
    return jsonify({'success': False, 'message': "Arbre non implémenté"})

@app.route('/api/sparql/autocomplete', methods=['POST'])
def autocomplete_api():
    """API stub pour l'autocomplétion SPARQL."""
    return jsonify({'success': True, 'suggestions': []})

@app.route('/api/sparql/validate', methods=['POST'])
def validate_api():
    """API stub pour la validation SPARQL."""
    return jsonify({'success': True, 'valid': True})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)