from rdflib import Namespace

# Configuration OAI-PMH
OAI_ENDPOINT = "https://bibnum.sciencespo.fr/oai"
METADATA_PREFIX = "oai_dc"
FORMAT = "turtle"

# Dossier courant
FILES_REPOSITORY = "./"
ITEMS_FILE = "items.ttl"


MAX_BATCHES = 1


# Namespaces
XML_NS = {
    'oai': 'http://www.openarchives.org/OAI/2.0/',
    'oai_dc': 'http://www.openarchives.org/OAI/2.0/oai_dc/',
    'dc': 'http://purl.org/dc/elements/1.1/'
}

DC = Namespace("http://purl.org/dc/elements/1.1/")
namespaces = {
    'dc': DC
}