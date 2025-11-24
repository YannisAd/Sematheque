import re
import pandas as pd
from Constants import HIDDEN_PROPERTIES, PREFIXES

def extract_item_id(uri: str) -> str:
    """Extrait l'identifiant numérique d'une URI Omeka."""
    if not uri or not isinstance(uri, str):
        return None
    match = re.search(r'api/items/(\d+)', uri)
    return match.group(1) if match else None

def format_property_name(property_uri: str):
    """
    Nettoie et formate une URI de propriété en remplaçant les namespaces par leurs préfixes
    et les caractères spéciaux par des espaces.
    """
    if not property_uri or property_uri in HIDDEN_PROPERTIES:
        return None
    
    property_uri = property_uri.strip()
    formatted_label = None
    
    # Vérification des préfixes configurés (sensible à la casse)
    for prefix, base_uri in PREFIXES.items():
        if property_uri.startswith(base_uri):
            suffix = property_uri.replace(base_uri, "")
            formatted_label = f"{prefix}:{suffix}"
            break

    # Vérification des préfixes configurés (insensible à la casse)
    if not formatted_label:
        property_uri_lower = property_uri.lower()
        for prefix, base_uri in PREFIXES.items():
            if property_uri_lower.startswith(base_uri.lower()):
                suffix = property_uri[len(base_uri):]
                formatted_label = f"{prefix}:{suffix}"
                break

    # Fallback sur les namespaces standards
    if not formatted_label:
        standard_ns = {
            "http://www.w3.org/2000/01/rdf-schema#": "rdfs",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#": "rdf",
            "http://www.w3.org/2002/07/owl#": "owl",
            "http://www.w3.org/2004/02/skos/core#": "skos",
            "http://purl.org/dc/terms/": "dcterms",
            "http://omeka.org/s/vocabs/o#": "omeka"
        }
        for ns, prefix in standard_ns.items():
            if property_uri.lower().startswith(ns.lower()):
                suffix = property_uri[len(ns):]
                formatted_label = f"{prefix}:{suffix}"
                break

    # Fallback final : nom après le dernier séparateur
    if not formatted_label:
        formatted_label = property_uri.split('#')[-1].split('/')[-1]

    return formatted_label.replace("_", " ").replace("-", " ")

def format_value_with_link(value, value_label):
    """Génère un lien HTML si la valeur est une ressource interne, sinon retourne le label."""
    if not value or not isinstance(value, str):
        return value_label
    
    item_id = extract_item_id(value)
    if item_id and value.startswith("http"):
        return f'<a href="/update_resource/{value}" class="resource-link">{value_label}</a>'
    return value_label

def prepare_csv_data(results):
    """Transforme les résultats bruts SPARQL en DataFrame formaté pour l'export CSV."""
    if results.empty:
        return pd.DataFrame()
        
    data = []
    for _, row in results.iterrows():
        card_data = {'SubjectURI': row['SubjectURI'], 'SubjectLabel': row['SubjectLabel']}
        if 'Properties' in row:
            properties = row['Properties'].split(" | ")
            value_labels = row['ValueLabels'].split(" | ")
            for prop, val_label in zip(properties, value_labels):
                formatted_prop = format_property_name(prop)
                if formatted_prop:
                    if formatted_prop in card_data:
                        card_data[formatted_prop] += f"; {val_label}"
                    else:
                        card_data[formatted_prop] = val_label
        data.append(card_data)
    return pd.DataFrame(data)

def pivot_data_for_visualization(df_details):
    """Pivote les données détaillées pour regrouper les propriétés par sujet URI."""
    if df_details.empty:
        return []
        
    grouped_data = {}
    for _, row in df_details.iterrows():
        subj_uri = row.get('Subject', row.get('SubjectURI'))
        if not subj_uri:
            continue

        if subj_uri not in grouped_data:
            grouped_data[subj_uri] = {
                'URI': subj_uri, 
                'Label': row.get('SubjectLabel', 'Sans titre')
            }

        prop_uri = row['Property']
        prop_key = format_property_name(prop_uri)
        if not prop_key:
            continue

        val_label = row.get('ValueLabel')
        if not val_label or str(val_label).strip() == "":
            val_label = row.get('Value', '')

        if prop_key in grouped_data[subj_uri]:
            current_vals = grouped_data[subj_uri][prop_key].split(" | ")
            if val_label not in current_vals:
                grouped_data[subj_uri][prop_key] += f" | {val_label}"
        else:
            grouped_data[subj_uri][prop_key] = val_label

    return list(grouped_data.values())