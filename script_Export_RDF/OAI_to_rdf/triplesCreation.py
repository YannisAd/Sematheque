import logging
from rdflib import Graph, URIRef, Literal
from rdflib.namespace import RDF
from constants import *

def initializeRDFdatabase():
    """Initialise le graphe RDF avec les namespaces."""
    graph = Graph()
    for key, value in namespaces.items():
        graph.bind(key, value)
    return graph

def saveGraphToFile(graph, filename):
    """Sauvegarde le graphe dans le dossier courant."""
    file_path = FILES_REPOSITORY + filename
    logging.info(f"Sauvegarde du fichier : {file_path}")
    
    try:
        graph.serialize(destination=file_path, format=FORMAT)
    except:
        logging.exception(f"Erreur écriture fichier : {file_path}")

def createRecordsTriples(records_xml, graph):
    """Transforme les enregistrements XML OAI en triplets RDF avec toutes les métadonnées."""
    for record in records_xml:
        try:
            # Récupération Header
            header = record.find('oai:header', XML_NS)
            if header is None: continue
                
            if 'status' in header.attrib and header.attrib['status'] == 'deleted':
                continue

            # Création URI
            identifier = header.find('oai:identifier', XML_NS).text
            uri = URIRef(identifier)

            # Récupération Metadata
            metadata = record.find('oai:metadata', XML_NS)
            if metadata is None: continue
            
            oai_dc = metadata.find('oai_dc:dc', XML_NS)
            if oai_dc is None: continue

            # Boucle sur toutes les propriétés Dublin Core
            for element in oai_dc:
                # Nettoyage du tag pour enlever le namespace XML (ex: {http://...}title -> title)
                if '}' in element.tag:
                    tag_name = element.tag.split('}')[-1]
                else:
                    tag_name = element.tag
                
                # On crée dynamiquement le prédicat RDF (ex: dc:title, dc:creator...)
                predicate = namespaces['dc'][tag_name]
                
                # Ajout de la valeur si elle n'est pas vide
                if element.text and element.text.strip():
                    graph.add((uri, predicate, Literal(element.text.strip())))

            # Ajout du type Record
            graph.add((uri, RDF.type, namespaces['dc'].Record))

        except:
            logging.exception("Erreur traitement record")
            continue