import pandas as pd
import re
import time
import logging
import ssl
from functools import lru_cache
from SPARQLWrapper import SPARQLWrapper, JSON, POST
from Constants import (
    SPARQL_ENDPOINT_ACCESS, CUSTOM_PREFIX, RESOURCE_TYPES, 
    HIDDEN_PROPERTIES, LABEL_PROPERTIES, MAIN_NAMESPACE
)

# Configuration SSL pour contextes non vérifiés (dev/test)
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def execute_raw_query(query):
    """
    Exécute une requête SPARQL brute et retourne un DataFrame Pandas.
    Gère les tentatives de reconnexion et les erreurs de syntaxe.
    """
    max_retries = 2
    last_error = None
    
    for attempt in range(max_retries):
        try:
            sparql = SPARQLWrapper(SPARQL_ENDPOINT_ACCESS)
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            sparql.setMethod(POST)
            sparql.setTimeout(60)
            
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
            last_error = e
            error_str = str(e).lower()
            if "syntax" in error_str or "parse error" in error_str:
                logger.error(f"Erreur Syntaxe SPARQL: {e}")
                break
            time.sleep(0.5)

    logger.error(f"Erreur SPARQL finale après {max_retries} tentatives: {last_error}")
    return pd.DataFrame()

def build_label_selection(subject_var="?s", label_var="?label", suffix=""):
    """
    Génère les clauses SPARQL OPTIONAL pour récupérer le label selon la configuration.
    
    Args:
        subject_var: Variable du sujet (ex: ?s)
        label_var: Variable de sortie pour le label (ex: ?label)
        suffix: Suffixe unique pour éviter les collisions de variables si appelée plusieurs fois (CORRECTIF)
    """
    if not subject_var.startswith('?') and not subject_var.startswith('<'):
        subject_var = f"?{subject_var}"
    if not label_var.startswith('?'):
        label_var = f"?{label_var}"

    effective_props = list(LABEL_PROPERTIES)
    # Ajout des propriétés de label standard si absentes
    for p in ["http://www.w3.org/2000/01/rdf-schema#label", "http://purl.org/dc/elements/1.1/title"]:
        if p not in effective_props:
            effective_props.append(p)

    optionals = []
    vars_list = []
    
    # Utilisation du suffixe pour rendre les variables uniques (ex: ?l_0_sub, ?l_0_val)
    for i, prop in enumerate(effective_props):
        v = f"?l_{i}{suffix}"
        optionals.append(f"OPTIONAL {{ {subject_var} <{prop}> {v} }}")
        vars_list.append(v)
    
    if not vars_list:
        v_def = f"?l_def{suffix}"
        optionals.append(f"OPTIONAL {{ {subject_var} rdfs:label {v_def} }}")
        vars_list.append(v_def)
        
    coalesce = f"COALESCE({', '.join(vars_list)}, '')"
    return "\n".join(optionals), coalesce

def extract_label_from_uri(uri):
    """Extrait un label lisible à partir d'une URI."""
    if not uri or not isinstance(uri, str):
        return "Inconnu"
    if '#' in uri:
        return uri.split('#')[-1].replace('_', ' ')
    return uri.split('/')[-1].replace('_', ' ')

@lru_cache(maxsize=3600)
def get_classes():
    """Récupère dynamiquement les classes utilisées dans la base."""
    if RESOURCE_TYPES:
        return sorted([{"label": k, "uri": v} for k, v in RESOURCE_TYPES.items()], key=lambda x: x['label'])

    query = f"""
    {CUSTOM_PREFIX}
    SELECT DISTINCT ?type ?label WHERE {{
        ?s a ?type .
        OPTIONAL {{ ?type rdfs:label ?label }}
        FILTER(isIRI(?type))
        FILTER(!STRSTARTS(STR(?type), "http://www.w3.org/1999/02/22-rdf-syntax-ns#"))
        FILTER(!STRSTARTS(STR(?type), "http://www.w3.org/2002/07/owl#"))
    }} LIMIT 1000
    """
    
    try:
        df = execute_raw_query(query)
        classes = []
        for _, row in df.iterrows():
            uri = row['type']
            label = row.get('label')
            if not label:
                label = extract_label_from_uri(uri)
            classes.append({"label": label, "uri": uri})
        return sorted(classes, key=lambda x: x['label'])
    except Exception:
        return []

@lru_cache(maxsize=3600)
def get_properties(search_text=None, limit=50):
    """Récupère les propriétés utilisées, avec filtre optionnel."""
    filter_clause = ""
    if search_text:
        safe_text = search_text.replace('"', '\\"')
        filter_clause = f'FILTER(CONTAINS(LCASE(STR(?label)), LCASE("{safe_text}")) || CONTAINS(LCASE(STR(?property)), LCASE("{safe_text}")))'
    
    q = f"""
    {CUSTOM_PREFIX} 
    SELECT DISTINCT ?property ?label 
    WHERE {{ 
        ?s ?property ?o . 
        OPTIONAL {{ ?property rdfs:label ?label }} 
        FILTER(isIRI(?property))
        {filter_clause}
    }} LIMIT {limit}
    """
    
    df = execute_raw_query(q)
    props = []
    for _, row in df.iterrows():
        uri = row['property']
        if uri in HIDDEN_PROPERTIES:
            continue
        l = row.get('label')
        if not l:
            l = extract_label_from_uri(uri)
        props.append({'uri': uri, 'label': l})
    
    return sorted(props, key=lambda x: x['label'])

@lru_cache(maxsize=3600)
def get_unique_values(prop_uri, search_text=None, limit=50):
    """Récupère les valeurs uniques pour une propriété donnée."""
    opt_labels, coal_label = build_label_selection("?value", "?label", "_uniq") # Suffixe ajouté
    
    filter_clause = ""
    if search_text:
        safe_text = search_text.replace('"', '\\"')
        filter_clause = f'FILTER(CONTAINS(LCASE(?label), LCASE("{safe_text}")) || CONTAINS(LCASE(STR(?value)), LCASE("{safe_text}")))'

    q = f"""
    {CUSTOM_PREFIX} 
    SELECT DISTINCT ?value ?label 
    WHERE {{ 
        ?s <{prop_uri}> ?value . 
        {opt_labels} 
        BIND({coal_label} AS ?label)
        {filter_clause}
    }} LIMIT {limit}
    """
    
    df = execute_raw_query(q)
    res = []
    for _, row in df.iterrows():
        v = row.get('value')
        if not v: continue
        l = row.get('label', v)
        if not l or str(l).strip() == "":
            l = v
        res.append({"value": l, "uri": v})
    return res

def build_sparql_query(filters_dict):
    """Construit dynamiquement une requête SPARQL complexe basée sur des filtres imbriqués."""
    primary_label_prop = LABEL_PROPERTIES[0] if LABEL_PROPERTIES else "http://www.w3.org/2000/01/rdf-schema#label"
    conditions = []
    var_counter = 0

    for prop_uri, filter_data in filters_dict.items():
        var_counter += 1
        val_var = f"?val{var_counter}"
        values_list = filter_data.get('values', [])
        nested_filters = filter_data.get('nestedFilters', {})
        
        if values_list:
            or_conditions = []
            for val_pair in values_list:
                value = val_pair[0] if isinstance(val_pair, list) else val_pair
                operator = val_pair[1] if isinstance(val_pair, list) and len(val_pair) > 1 else "="
                safe_val = str(value).replace('"', '\\"')
                is_uri = str(value).startswith("http://") or str(value).startswith("https://")
                
                if operator in ["=", "equals"]:
                    if is_uri:
                        or_conditions.append(f"{val_var} = <{value}>")
                    else:
                        or_conditions.append(f"""(LCASE(STR({val_var})) = LCASE("{safe_val}") || EXISTS {{ {val_var} <{primary_label_prop}> ?l_{var_counter} . FILTER(LCASE(STR(?l_{var_counter})) = LCASE("{safe_val}")) }})""")
                elif operator in ["contient", "contains"]:
                    or_conditions.append(f"""(CONTAINS(LCASE(STR({val_var})), LCASE("{safe_val}")) || EXISTS {{ {val_var} <{primary_label_prop}> ?l_{var_counter} . FILTER(CONTAINS(LCASE(STR(?l_{var_counter})), LCASE("{safe_val}"))) }})""")
                elif operator in ["!=", "different"]:
                     if is_uri: or_conditions.append(f"{val_var} != <{value}>")
                     else: or_conditions.append(f"""(LCASE(STR({val_var})) != LCASE("{safe_val}"))""")
                elif operator == ">":
                    or_conditions.append(f"xsd:decimal({val_var}) > xsd:decimal(\"{safe_val}\")")
                elif operator == "<":
                    or_conditions.append(f"xsd:decimal({val_var}) < xsd:decimal(\"{safe_val}\")")

            triple_pattern = f"?subject <{prop_uri}> {val_var} ."
            if or_conditions:
                conditions.append(f"{{ {triple_pattern}\nFILTER({' || '.join(or_conditions)}) }}")
            elif operator in ["non_null", "Existe"]:
                conditions.append(triple_pattern)

        if nested_filters:
            for nested_uri, nested_data in nested_filters.items():
                nested_var = f"?nested{var_counter}"
                link_triple = f"?subject <{prop_uri}> {nested_var} ."
                nested_vals = nested_data.get('values', [])
                if nested_vals:
                    nested_or = []
                    for n_val, n_op in nested_vals:
                        safe_n = str(n_val).replace('"', '\\"')
                        if n_op in ["=", "equals"]:
                             nested_or.append(f"""(LCASE(STR(?nv_{var_counter})) = LCASE("{safe_n}") || EXISTS {{ ?nv_{var_counter} <{primary_label_prop}> ?nl_{var_counter} . FILTER(LCASE(STR(?nl_{var_counter})) = LCASE("{safe_n}")) }})""")
                    if nested_or:
                        conditions.append(f"{{ {link_triple}\n{nested_var} <{nested_uri}> ?nv_{var_counter} .\nFILTER({' || '.join(nested_or)}) }}")

    where_body = "\n".join(conditions)
    opt_labels, coal_label = build_label_selection("?subject", "?subjectLabel", "_main") # Suffixe ajouté
    
    if not filters_dict:
        ns_filter = f'FILTER(REGEX(STR(?subject), "^{MAIN_NAMESPACE}"))' if MAIN_NAMESPACE else ""
        return f"""{CUSTOM_PREFIX} SELECT DISTINCT ?subject ?subjectLabel WHERE {{ ?subject a ?type . {ns_filter} {opt_labels} BIND({coal_label} AS ?subjectLabel) }} LIMIT 1000"""

    return f"""{CUSTOM_PREFIX} SELECT DISTINCT ?subject ?subjectLabel WHERE {{ {{ SELECT DISTINCT ?subject WHERE {{ {where_body} }} LIMIT 4000 }} {opt_labels} BIND({coal_label} AS ?subjectLabel) }} LIMIT 7000"""

def get_graph_exploration(resource_uri, depth=2):
    """Récupère les ancêtres et descendants pour la visualisation de graphe."""
    if not resource_uri.startswith('<'):
        resource_uri = f"<{resource_uri}>"
        
    query = f"""{CUSTOM_PREFIX} SELECT DISTINCT ?start ?startLabel ?startType ?predicate ?predicateLabel ?end ?endLabel ?endType ?direction ?depth WHERE {{ BIND({resource_uri} AS ?central) {{ {{ ?central ?predicate ?end . BIND(?central AS ?start) BIND("descendant" AS ?direction) BIND(1 AS ?depth) }} UNION {{ ?start ?predicate ?central . BIND(?central AS ?end) BIND("ancestor" AS ?direction) BIND(1 AS ?depth) }} }} { f''' UNION {{ {{ ?central ?p1 ?mid . ?mid ?predicate ?end . BIND(?mid AS ?start) BIND("descendant" AS ?direction) BIND(2 AS ?depth) }} UNION {{ ?start ?predicate ?mid . ?mid ?p1 ?central . BIND(?mid AS ?end) BIND("ancestor" AS ?direction) BIND(2 AS ?depth) }} }} ''' if depth >= 2 else '' } OPTIONAL {{ ?start rdfs:label ?startLabel }} OPTIONAL {{ ?start a ?startType }} OPTIONAL {{ ?end rdfs:label ?endLabel }} OPTIONAL {{ ?end a ?endType }} OPTIONAL {{ ?predicate rdfs:label ?predicateLabel }} FILTER(isIRI(?start) && isIRI(?end)) FILTER(?predicate != rdf:type) FILTER(!REGEX(STR(?predicate), "owl#")) }} LIMIT 5000"""
    
    try:
        sparql = SPARQLWrapper(SPARQL_ENDPOINT_ACCESS)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        sparql.setMethod(POST)
        return sparql.query().convert()
    except Exception:
        return {"results": {"bindings": []}}

def get_resources_by_type(rt):
    """Retourne toutes les ressources d'un type donné."""
    uri = RESOURCE_TYPES.get(rt)
    if not uri:
        all_classes = get_classes()
        for c in all_classes: 
            if c['label'] == rt:
                uri = c['uri']
                break
    if not uri and str(rt).startswith('http'):
        uri = rt
    if not uri:
        return []
        
    opt_labels, coal_label = build_label_selection("?r", "?l", "_type") # Suffixe ajouté
    q = f"""{CUSTOM_PREFIX} SELECT DISTINCT ?r ?l WHERE {{ ?r a <{uri}> . {opt_labels} BIND({coal_label} AS ?l) }} LIMIT 3000"""
    df = execute_raw_query(q)
    return [{"uri": r['r'], "label": r.get('l', extract_label_from_uri(r['r']))} for _, r in df.iterrows()]

def query_sparql(uri):
    """Récupère tous les détails d'une ressource spécifique (Format Long)."""
    if not uri.startswith('<'):
        uri = f"<{uri}>"
        
    opt_labels, coal_label = build_label_selection("?value", "?valueLabel", "_det") # Suffixe ajouté
    q = f"""{CUSTOM_PREFIX} SELECT ?property ?value ?valueLabel WHERE {{ {uri} ?property ?value . {opt_labels} BIND({coal_label} AS ?valueLabel) }} LIMIT 1000"""
    
    df = execute_raw_query(q)
    if df.empty:
        return pd.DataFrame(columns=['Property', 'Value', 'ValueLabel'])
    
    df.columns = [c[0].upper() + c[1:] for c in df.columns]
    if 'Value' in df.columns:
        if 'ValueLabel' not in df.columns:
            df['ValueLabel'] = df['Value']
        else:
            df['ValueLabel'] = [lbl if lbl and str(lbl).strip() != '' else val for lbl, val in zip(df['ValueLabel'], df['Value'])]
    return df

def get_bulk_details(resource_uris):
    """
    Récupération optimisée des détails pour une liste d'URIs (Format Large pour Visu/Export).
    Utilise des suffixes distincts pour éviter le mélange entre le label du sujet et le label de la valeur.
    """
    if not resource_uris:
        return pd.DataFrame()
        
    clean_uris = [f"<{str(u).strip()}>" if not str(u).strip().startswith('<') else str(u).strip() for u in resource_uris]
    chunk_size = 50
    all_data = []
    
    # Suffixes IMPORTANTS pour éviter la collision de variables (?l_0)
    opt_labels_sub, coal_label_sub = build_label_selection("?subject", "?subjectLabel", "_sub")
    opt_labels_val, coal_label_val = build_label_selection("?value", "?valueLabel", "_val")
    
    for i in range(0, len(clean_uris), chunk_size):
        chunk = clean_uris[i:i + chunk_size]
        values_clause = " ".join(chunk)
        query = f"""{CUSTOM_PREFIX} SELECT DISTINCT ?subject ?subjectLabel ?property ?value ?valueLabel WHERE {{ VALUES ?subject {{ {values_clause} }} ?subject ?property ?value . {opt_labels_sub} BIND({coal_label_sub} AS ?subjectLabel) {opt_labels_val} BIND({coal_label_val} AS ?valueLabel) }}"""
        df_chunk = execute_raw_query(query)
        if not df_chunk.empty:
            all_data.append(df_chunk)
            
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df.columns = [c[0].upper() + c[1:] for c in final_df.columns]
        return final_df
    return pd.DataFrame()

def search_resources(text, limit=20, resource_type=None):
    """Recherche plein texte simple."""
    tf = ""
    if resource_type:
        uri = RESOURCE_TYPES.get(resource_type)
        if not uri:
             for c in get_classes(): 
                 if c['label'] == resource_type:
                     uri = c['uri']
                     break
        if uri:
            tf = f"?subject a <{uri}> ."
    
    opt_labels, coal_label = build_label_selection("?subject", "?label", "_search") # Suffixe ajouté
    safe_text = text.replace('"', '\\"')
    
    q = f"""{CUSTOM_PREFIX} 
    SELECT DISTINCT ?subject ?label ?type WHERE {{ 
        ?subject a ?type . 
        {tf} 
        ?subject ?p ?val .
        FILTER(isLiteral(?val))
        FILTER(CONTAINS(LCASE(STR(?val)), LCASE("{safe_text}")))
        {opt_labels}
        BIND({coal_label} AS ?label)
    }} LIMIT {limit}"""
    
    return execute_raw_query(q)

def get_resource_metadata(uri):
    """Récupère les métadonnées de base."""
    if not uri.startswith('<'):
        uri = f"<{uri}>"
        
    opt_labels, coal_label = build_label_selection(uri, "?label", "_meta") # Suffixe ajouté
    q = f"""{CUSTOM_PREFIX} SELECT ?type ?label WHERE {{ {uri} a ?type . {opt_labels} BIND({coal_label} AS ?label) }} LIMIT 1"""
    df = execute_raw_query(q)
    
    meta = {"label": "Inconnu", "type": "Resource", "uri": uri.strip('<>')}
    if not df.empty:
        meta['label'] = df.iloc[0].get('label', meta['label'])
        if df.iloc[0].get('type'):
            meta['type'] = df.iloc[0]['type'].split('#')[-1]
    return meta

def get_ontology_structure():
    """Récupère la structure dynamique de l'ontologie."""
    logger.info("Récupération dynamique de la structure de l'ontologie")
    
    structure = {"classes": [], "relations": []}
    classes_dict = {} 
    relations_dict = {}

    try:
        q_classes = f"""
        {CUSTOM_PREFIX}
        SELECT DISTINCT ?type ?label WHERE {{
            ?s a ?type .
            OPTIONAL {{ ?type rdfs:label ?label }}
            FILTER(isIRI(?type))
            FILTER(!STRSTARTS(STR(?type), "http://www.w3.org/1999/02/22-rdf-syntax-ns#"))
            FILTER(!STRSTARTS(STR(?type), "http://www.w3.org/2002/07/owl#"))
        }} LIMIT 500
        """
        df_classes = execute_raw_query(q_classes)
        
        for _, row in df_classes.iterrows():
            uri = row['type']
            label = row.get('label')
            if not label: label = extract_label_from_uri(uri)
            
            classes_dict[uri] = {
                "uri": uri, "label": label,
                "superClasses": [], "properties": []
            }

        q_props = f"""
        {CUSTOM_PREFIX}
        SELECT DISTINCT ?p ?domainType ?rangeType WHERE {{
            ?s ?p ?o .
            ?s a ?domainType .
            OPTIONAL {{ ?o a ?rangeType }}
            FILTER(isIRI(?p))
            FILTER(?p != <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>)
        }} LIMIT 3000
        """
        df_props = execute_raw_query(q_props)
        
        for _, row in df_props.iterrows():
            p_uri = row['p']
            d_uri = row['domainType']
            r_uri = row.get('rangeType')
            
            if d_uri not in classes_dict: continue
            
            if p_uri not in relations_dict:
                relations_dict[p_uri] = {
                    "uri": p_uri,
                    "label": extract_label_from_uri(p_uri),
                    "domains": [],
                    "ranges": []
                }
            
            if not any(d['uri'] == d_uri for d in relations_dict[p_uri]['domains']):
                relations_dict[p_uri]['domains'].append({
                    "uri": d_uri, "label": classes_dict[d_uri]['label']
                })
                prop_entry = {"uri": p_uri, "label": relations_dict[p_uri]['label']}
                if not any(p['uri'] == p_uri for p in classes_dict[d_uri]['properties']):
                    classes_dict[d_uri]['properties'].append(prop_entry)
            
            if r_uri and r_uri in classes_dict:
                if not any(r['uri'] == r_uri for r in relations_dict[p_uri]['ranges']):
                    relations_dict[p_uri]['ranges'].append({
                        "uri": r_uri, "label": classes_dict[r_uri]['label']
                    })

        q_sub = f"""
        {CUSTOM_PREFIX}
        SELECT ?sub ?super WHERE {{
            ?sub rdfs:subClassOf ?super .
        }}
        """
        try:
            df_sub = execute_raw_query(q_sub)
            for _, row in df_sub.iterrows():
                sub = row['sub']
                sup = row['super']
                if sub in classes_dict and sup in classes_dict:
                    classes_dict[sub]['superClasses'].append({
                        "uri": sup, "label": classes_dict[sup]['label']
                    })
        except Exception:
            pass

        structure['classes'] = list(classes_dict.values())
        structure['relations'] = list(relations_dict.values())
        
        if not structure['classes']:
             return get_default_ontology_structure()
             
        return structure

    except Exception as e:
        logger.error(f"Erreur get_ontology_structure: {e}")
        return get_default_ontology_structure()

def get_default_ontology_structure():
    """Structure de secours."""
    return {
        "classes": [{"uri": "http://example.org/Thing", "label": "Chose", "properties": [], "superClasses": []}],
        "relations": []
    }