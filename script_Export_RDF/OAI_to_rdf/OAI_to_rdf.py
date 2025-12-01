#!/usr/bin/python
# -*- coding: utf-8 -*-

import requests
import logging
import xml.etree.ElementTree as ET
from triplesCreation import *
from constants import *

def configureLogging():
    """Log simple dans un fichier local."""
    logging.basicConfig(
        filename='execution.log',
        filemode='w', # Écrase le log précédent à chaque lancement
        format='%(levelname)s - %(message)s',
        level=logging.INFO
    )
    # Ajout d'un handler pour voir les infos aussi dans le terminal
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger('').addHandler(console)

def harvestOAI():
    """Boucle de moissonnage OAI avec gestion du resumptionToken et limite de sécurité."""
    graph = initializeRDFdatabase()
    
    request_url = f"{OAI_ENDPOINT}?verb=ListRecords&metadataPrefix={METADATA_PREFIX}"
    token = None
    call_count = 0

    while True:
        # Vérification de la limite de batch
        if MAX_BATCHES and call_count >= MAX_BATCHES:
            logging.info(f"Limite de {MAX_BATCHES} batchs atteinte. Arrêt.")
            break

        logging.info(f"Récupération batch {call_count + 1}...")
        
        try:
            response = requests.get(request_url)
            response.raise_for_status()
            
            root = ET.fromstring(response.content)
            
            # Vérification erreurs OAI
            error = root.find('oai:error', XML_NS)
            if error is not None:
                logging.error(f"Erreur API OAI: {error.text}")
                break

            list_records = root.find('oai:ListRecords', XML_NS)
            if list_records is None: break
                
            records = list_records.findall('oai:record', XML_NS)
            if records:
                createRecordsTriples(records, graph)
                logging.info(f"{len(records)} items traités.")

            # Gestion pagination
            token_node = list_records.find('oai:resumptionToken', XML_NS)
            
            if token_node is not None and token_node.text:
                token = token_node.text
                request_url = f"{OAI_ENDPOINT}?verb=ListRecords&resumptionToken={token}"
                call_count += 1
            else:
                logging.info("Fin du moissonnage (plus de token).")
                break
                
        except Exception as e:
            logging.error(f"Arrêt sur erreur: {e}")
            break

    saveGraphToFile(graph, ITEMS_FILE)

#### Main ####

if __name__ == "__main__":
    configureLogging()
    logging.info('Démarrage import OAI...')
    harvestOAI()
    logging.info('Terminé.')