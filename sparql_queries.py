import pandas as pd
import re
import time
import logging
import ssl
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
from SPARQLWrapper import SPARQLWrapper, JSON, POST
from Constants import (
    ENDPOINTS, CUSTOM_PREFIX, RESOURCE_TYPES, 
    HIDDEN_PROPERTIES, LABEL_PROPERTIES, MAIN_NAMESPACE
)

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def execute_single_query(query, endpoint_url):
    """Exécute une requête SPARQL sur un endpoint unique."""
    try:
        sparql = SPARQLWrapper(endpoint_url)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        sparql.setMethod(POST)
        sparql.setTimeout(45)
        
        results = sparql.query().convert()
        
        if 'results' in results and 'bindings' in results['results']:
            bindings = results['results']['bindings']
            if not bindings:
                return pd.DataFrame()
            
            vars_list = results['head']['vars']
            data = []
            for binding in bindings:
                row = {}
                for var in vars_list:
                    row[var] = binding[var]['value'] if var in binding else None
                data.append(row)
            return pd.DataFrame(data)
        return pd.DataFrame()
            
    except Exception as e:
        logger.warning(f"Timeout ou erreur sur {endpoint_url}: {e}")
        return pd.DataFrame()

def execute_raw_query(query, specific_endpoint=None):
    """Exécute la requête de manière fédérée et fusionne les résultats."""
    if specific_endpoint:
        return execute_single_query(query, specific_endpoint)

    dataframes = []
    with ThreadPoolExecutor(max_workers=len(ENDPOINTS) or 1) as executor:
        future_to_url = {executor.submit(execute_single_query, query, ep['url']): ep for ep in ENDPOINTS}
        for future in as_completed(future_to_url):
            try:
                df = future.result()
                if not df.empty:
                    dataframes.append(df)
            except Exception as e:
                logger.error(f"Erreur thread: {e}")

    if not dataframes:
        return pd.DataFrame()
        
    final_df = pd.concat(dataframes, ignore_index=True)
    final_df.drop_duplicates(inplace=True)
    return final_df

def build_label_selection(subject_var="?s", label_var="?label", suffix=""):
    """Clause OPTIONAL pour les labels."""
    if not subject_var.startswith('?') and not subject_var.startswith('<'): subject_var = f"?{subject_var}"
    if not label_var.startswith('?'): label_var = f"?{label_var}"

    effective_props = list(LABEL_PROPERTIES) if LABEL_PROPERTIES else ["http://www.w3.org/2000/01/rdf-schema#label"]
    if "http://www.w3.org/2000/01/rdf-schema#label" not in effective_props:
        effective_props.append("http://www.w3.org/2000/01/rdf-schema#label")

    optionals = []
    vars_list = []
    for i, prop in enumerate(effective_props):
        v = f"?l_{i}{suffix}"
        optionals.append(f"OPTIONAL {{ {subject_var} <{prop}> {v} }}")
        vars_list.append(v)
    
    coalesce = f"COALESCE({', '.join(vars_list)}, '')"
    return "\n".join(optionals), coalesce

def extract_label_from_uri(uri):
    if not uri or not isinstance(uri, str): return "Inconnu"
    if '#' in uri: return uri.split('#')[-1].replace('_', ' ')
    return uri.split('/')[-1].replace('_', ' ')

@lru_cache(maxsize=3600)
def get_classes():
    """Récupère les classes (Hybride : Config + Découverte légère)."""
    classes_list = []
    seen_uris = set()

    if RESOURCE_TYPES:
        for label, uri in RESOURCE_TYPES.items():
            if uri not in seen_uris:
                classes_list.append({"label": label, "uri": uri, "source": "config"})
                seen_uris.add(uri)

    query = f"""{CUSTOM_PREFIX} SELECT DISTINCT ?type WHERE {{ ?s a ?type }} LIMIT 50"""
    
    try:
        df = execute_raw_query(query)
        for _, row in df.iterrows():
            uri = row['type']
            if uri not in seen_uris:
                classes_list.append({"label": extract_label_from_uri(uri), "uri": uri, "source": "auto"})
                seen_uris.add(uri)
    except Exception as e:
        logger.warning(f"Erreur découverte classes: {e}")

    return sorted(classes_list, key=lambda x: x['label'])

def analyze_class_structure(class_uri):
    """Analyse approfondie (A-Box) pour détecter relations."""
    query = f"""
    {CUSTOM_PREFIX}
    SELECT DISTINCT ?p ?rangeType WHERE {{
        {{ SELECT ?s WHERE {{ ?s a <{class_uri}> }} LIMIT 20 }}
        ?s ?p ?o .
        FILTER(isIRI(?p))
        FILTER(?p != rdf:type)
        OPTIONAL {{ ?o a ?rangeType . }}
    }} LIMIT 200
    """
    return execute_raw_query(query)

def get_ontology_structure():
    """Construit le graphe (A-Box) de manière fédérée."""
    logger.info("Construction ontologie A-Box...")
    all_classes = get_classes()
    if not all_classes:
        return {"classes": [], "relations": []}

    classes_dict = {c['uri']: {"uri": c['uri'], "label": c['label'], "properties": [], "superClasses": []} for c in all_classes}
    relations_dict = {}

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_uri = {executor.submit(analyze_class_structure, c['uri']): c['uri'] for c in all_classes}
        
        for future in as_completed(future_to_uri):
            source_class_uri = future_to_uri[future]
            try:
                df = future.result()
                if df.empty: continue
                
                for _, row in df.iterrows():
                    p_uri = row['p']
                    range_type_uri = row.get('rangeType')
                    
                    if p_uri not in relations_dict:
                        relations_dict[p_uri] = {
                            "uri": p_uri,
                            "label": extract_label_from_uri(p_uri),
                            "domains": [],
                            "ranges": []
                        }
                    
                    if not any(d['uri'] == source_class_uri for d in relations_dict[p_uri]['domains']):
                        relations_dict[p_uri]['domains'].append({
                            "uri": source_class_uri, 
                            "label": classes_dict[source_class_uri]['label']
                        })
                    
                    if range_type_uri and range_type_uri in classes_dict:
                        if not any(r['uri'] == range_type_uri for r in relations_dict[p_uri]['ranges']):
                            relations_dict[p_uri]['ranges'].append({
                                "uri": range_type_uri, 
                                "label": classes_dict[range_type_uri]['label']
                            })
                    
                    prop_entry = {"uri": p_uri, "label": relations_dict[p_uri]['label']}
                    if not any(p['uri'] == p_uri for p in classes_dict[source_class_uri]['properties']):
                        classes_dict[source_class_uri]['properties'].append(prop_entry)
                        
            except Exception as e:
                logger.warning(f"Erreur analyse structure {source_class_uri}: {e}")

    final_relations = [r for r in relations_dict.values() if r['domains']]

    return {
        "classes": list(classes_dict.values()),
        "relations": final_relations
    }

@lru_cache(maxsize=3600)
def get_properties(search_text=None, limit=50):
    filter_clause = ""
    if search_text:
        safe_text = search_text.replace('"', '\\"')
        filter_clause = f'FILTER(CONTAINS(LCASE(STR(?property)), LCASE("{safe_text}")))'
    
    q = f"""{CUSTOM_PREFIX} SELECT DISTINCT ?property WHERE {{ ?s ?property ?o . FILTER(isIRI(?property)) {filter_clause} }} LIMIT {limit}"""
    
    df = execute_raw_query(q)
    props = []
    seen = set()
    for _, row in df.iterrows():
        uri = row['property']
        if uri not in HIDDEN_PROPERTIES and uri not in seen:
            seen.add(uri)
            props.append({'uri': uri, 'label': extract_label_from_uri(uri)})
    return sorted(props, key=lambda x: x['label'])

@lru_cache(maxsize=3600)
def get_unique_values(prop_uri, search_text=None, limit=50):
    opt_labels, coal_label = build_label_selection("?value", "?label", "_uniq")
    filter_clause = ""
    if search_text:
        safe_text = search_text.replace('"', '\\"')
        filter_clause = f'FILTER(CONTAINS(LCASE(?label), LCASE("{safe_text}")) || CONTAINS(LCASE(STR(?value)), LCASE("{safe_text}")))'

    q = f"""{CUSTOM_PREFIX} SELECT DISTINCT ?value ?label WHERE {{ ?s <{prop_uri}> ?value . {opt_labels} BIND({coal_label} AS ?label) {filter_clause} }} LIMIT {limit}"""
    
    df = execute_raw_query(q)
    res = []
    seen = set()
    for _, row in df.iterrows():
        v = row.get('value')
        if not v or v in seen: continue
        seen.add(v)
        res.append({"value": row.get('label', v) or v, "uri": v})
    return res

def build_sparql_query(filters_dict, logic="AND"):
    """Construit la requête SPARQL de filtrage avec logique ET/OU."""
    conditions = []
    var_counter = 0

    for prop_uri, filter_data in filters_dict.items():
        var_counter += 1
        val_var = f"?val{var_counter}"
        values = filter_data.get('values', [])
        
        if values:
            or_conds = []
            for val, op in values:
                safe_val = str(val).replace('"', '\\"')
                is_uri = str(val).startswith("http")
                
                clause = ""
                if op == "=":
                    if is_uri: clause = f"{val_var} = <{val}>"
                    else: clause = f"LCASE(STR({val_var})) = LCASE(\"{safe_val}\")"
                elif op == "contient":
                     clause = f"CONTAINS(LCASE(STR({val_var})), LCASE(\"{safe_val}\"))"
                elif op == "!=":
                     clause = f"LCASE(STR({val_var})) != LCASE(\"{safe_val}\")"
                elif op == ">":
                     clause = f"xsd:decimal({val_var}) > xsd:decimal(\"{safe_val}\")"
                elif op == "<":
                     clause = f"xsd:decimal({val_var}) < xsd:decimal(\"{safe_val}\")"
                
                if clause: or_conds.append(clause)
            
            if or_conds:
                conditions.append(f"?subject <{prop_uri}> {val_var} . FILTER({' || '.join(or_conds)})")

    if not conditions:
        opt_labels, coal_label = build_label_selection("?subject", "?subjectLabel", "_m")
        return f"""{CUSTOM_PREFIX} SELECT DISTINCT ?subject ?subjectLabel WHERE {{ ?subject a ?type . {opt_labels} BIND({coal_label} AS ?subjectLabel) }} LIMIT 100"""

    where_body = ""
    if logic == "OR":
        union_blocks = [f"{{ {cond} }}" for cond in conditions]
        where_body = " UNION ".join(union_blocks)
    else:
        where_body = "\n".join(conditions)

    opt_labels, coal_label = build_label_selection("?subject", "?subjectLabel", "_m")
    
    return f"""{CUSTOM_PREFIX} SELECT DISTINCT ?subject ?subjectLabel WHERE {{ {{ SELECT DISTINCT ?subject WHERE {{ ?subject a ?type . {where_body} }} LIMIT 1000 }} {opt_labels} BIND({coal_label} AS ?subjectLabel) }} LIMIT 1000"""

def get_graph_exploration(resource_uri, depth=2):
    if not resource_uri.startswith('<'): resource_uri = f"<{resource_uri}>"
    query = f"""{CUSTOM_PREFIX} SELECT DISTINCT ?start ?startLabel ?startType ?predicate ?predicateLabel ?end ?endLabel ?endType ?direction ?depth WHERE {{ BIND({resource_uri} AS ?central) {{ {{ ?central ?predicate ?end . BIND(?central AS ?start) BIND("descendant" AS ?direction) BIND(1 AS ?depth) }} UNION {{ ?start ?predicate ?central . BIND(?central AS ?end) BIND("ancestor" AS ?direction) BIND(1 AS ?depth) }} }} OPTIONAL {{ ?start rdfs:label ?startLabel }} OPTIONAL {{ ?end rdfs:label ?endLabel }} OPTIONAL {{ ?predicate rdfs:label ?predicateLabel }} FILTER(isIRI(?start) && isIRI(?end)) FILTER(?predicate != rdf:type) }} LIMIT 500"""
    
    try:
        df = execute_raw_query(query)
        bindings = []
        for _, row in df.iterrows():
            item = {}
            for col in df.columns: item[col] = {"type": "uri", "value": row[col]}
            bindings.append(item)
        return {"results": {"bindings": bindings}}
    except Exception: return {"results": {"bindings": []}}

def get_resources_by_type(rt):
    uri = RESOURCE_TYPES.get(rt)
    if not uri:
        for c in get_classes(): 
            if c['label'] == rt: uri = c['uri']; break
    if not uri and str(rt).startswith('http'): uri = rt
    if not uri: return []
    opt_labels, coal_label = build_label_selection("?r", "?l", "_t")
    q = f"""{CUSTOM_PREFIX} SELECT DISTINCT ?r ?l WHERE {{ ?r a <{uri}> . {opt_labels} BIND({coal_label} AS ?l) }} LIMIT 500"""
    df = execute_raw_query(q)
    return [{"uri": r['r'], "label": r.get('l', extract_label_from_uri(r['r']))} for _, r in df.iterrows()]

def query_sparql(uri):
    """
    Récupère les détails. Inclut la correction pour afficher les Valeurs
    quand les Labels sont vides.
    """
    if not uri.startswith('<'): uri = f"<{uri}>"
    opt_labels, coal_label = build_label_selection("?value", "?valueLabel", "_det")
    q = f"""{CUSTOM_PREFIX} SELECT ?property ?value ?valueLabel WHERE {{ {uri} ?property ?value . {opt_labels} BIND({coal_label} AS ?valueLabel) }} LIMIT 1000"""
    
    df = execute_raw_query(q)
    
    if not df.empty:
        # 1. Renommage normalisé des colonnes
        rename_map = {
            'property': 'Property', 'value': 'Value', 'valueLabel': 'ValueLabel',
            'Property': 'Property', 'Value': 'Value', 'ValueLabel': 'ValueLabel'
        }
        df.rename(columns=rename_map, inplace=True)
        
        # 2. FIX CRITIQUE : Remplissage des labels vides
        # Si 'ValueLabel' n'existe pas ou est vide, on prend 'Value'
        if 'Value' in df.columns:
            if 'ValueLabel' not in df.columns:
                df['ValueLabel'] = df['Value']
            else:
                # On remplace les NaN par chaine vide puis on applique le fallback
                df['ValueLabel'] = df['ValueLabel'].fillna('')
                df['ValueLabel'] = df.apply(
                    lambda x: x['ValueLabel'] if str(x['ValueLabel']).strip() != '' else x['Value'],
                    axis=1
                )
    
    return df

def get_bulk_details(uris):
    if not uris: return pd.DataFrame()
    clean_uris = [f"<{u}>" if not str(u).startswith('<') else u for u in uris]
    all_data = []
    chunk_size = 30
    opt_labels_sub, coal_label_sub = build_label_selection("?subject", "?subjectLabel", "_s")
    opt_labels_val, coal_label_val = build_label_selection("?value", "?valueLabel", "_v")
    
    for i in range(0, len(clean_uris), chunk_size):
        chunk = clean_uris[i:i + chunk_size]
        q = f"""{CUSTOM_PREFIX} SELECT DISTINCT ?subject ?subjectLabel ?property ?value ?valueLabel WHERE {{ VALUES ?subject {{ {" ".join(chunk)} }} ?subject ?property ?value . {opt_labels_sub} BIND({coal_label_sub} AS ?subjectLabel) {opt_labels_val} BIND({coal_label_val} AS ?valueLabel) }}"""
        df = execute_raw_query(q)
        if not df.empty: all_data.append(df)
    
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df.columns = [c[0].upper() + c[1:] for c in final_df.columns]
        return final_df
    return pd.DataFrame()

def search_resources(text, limit=20, resource_type=None):
    safe_text = text.replace('"', '\\"')
    limit = min(limit, 50)
    
    type_filter = ""
    if resource_type:
        if RESOURCE_TYPES and resource_type in RESOURCE_TYPES:
             type_filter = f"?subject a <{RESOURCE_TYPES[resource_type]}> ."
        elif str(resource_type).startswith('http'):
             type_filter = f"?subject a <{resource_type}> ."
         
    opt_labels, coal_label = build_label_selection("?subject", "?label", "_srch")
    q = f"""{CUSTOM_PREFIX} SELECT DISTINCT ?subject ?label ?type WHERE {{ {{ SELECT DISTINCT ?subject ?label WHERE {{ ?subject rdfs:label ?label . FILTER(CONTAINS(LCASE(?label), LCASE("{safe_text}"))) }} LIMIT {limit} }} {type_filter} OPTIONAL {{ ?subject a ?type }} BIND(?label as ?finalLabel) }}"""
    return execute_raw_query(q)

def get_resource_metadata(uri):
    if not uri.startswith('<'): uri = f"<{uri}>"
    opt_labels, coal_label = build_label_selection(uri, "?label", "_meta")
    q = f"""{CUSTOM_PREFIX} SELECT ?type ?label WHERE {{ {uri} a ?type . {opt_labels} BIND({coal_label} AS ?label) }} LIMIT 1"""
    df = execute_raw_query(q)
    meta = {"label": "Inconnu", "type": "Resource", "uri": uri.strip('<>')}
    if not df.empty:
        meta['label'] = df.iloc[0].get('label', meta['label'])
        if df.iloc[0].get('type'): meta['type'] = df.iloc[0]['type'].split('/')[-1]
    return meta