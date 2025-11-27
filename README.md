# Sematheque - Explorateur Sémantique Générique

**Sematheque** est une interface web légère, modulaire et générique
conçue pour explorer, rechercher et visualiser des données issues de
n'importe quel **Endpoint SPARQL** (Wikidata, DBpedia, Nakala, ou un
corpus institutionnel personnalisé).

L'application agit comme une couche de présentation intelligente
au-dessus de vos données RDF, offrant des fonctionnalités de filtrage à
facettes, de visualisation graphique et d'analyse de réseau sans
nécessiter de compétences techniques avancées de la part de
l'utilisateur final.

## 🚀 Fonctionnalités Clés

-   **Exploration à Facettes :** Navigation intuitive dans les données
    via des filtres dynamiques (textuels, numériques, existence).
-   **Requêtes Imbriquées :** Construction visuelle de requêtes SPARQL
    complexes (ex: *Trouver les Auteurs nés dans une Ville située dans
    un Pays spécifique*).
-   **Performance Asynchrone :** Chargement des propriétés et valeurs
    via AJAX (Select2, Tagify) pour supporter des graphes massifs comme
    Wikidata.
-   **Visualisation de Données :** Génération automatique de graphiques
    (Barres, Camemberts, Lignes) et de graphes de réseaux (nœuds/liens).
-   **Recherche Textuelle :** Moteur de recherche full-text sur
    l'ensemble des littéraux du graphe.
-   **Export :** Exportation des résultats et des données pivotées aux
    formats CSV et JSON.
-   **Modulable :** Configuration complète via un simple fichier JSON
    (Préfixes, Classes, Propriétés masquées).

## 🛠 Prérequis

-   **Python 3.8+**
-   Un accès réseau à un **Endpoint SPARQL** (public ou privé). Test effectué avec un endpoint [Apache Fuseki](https://jena.apache.org/documentation/fuseki2/).

## 📥 Installation et Déploiement

Suivez ces étapes pour installer l'application localement ou sur un
serveur.


<details>
  <summary><h3><b>Démonstration d'une installation</b></h3></summary>
<video src="https://github.com/YannisAd/Sematheque/blob/main/videos/lauch.mp4" width="320" height="240" controls></video>

</details>


## 0.5. Export RDF depuis Omeka ou un entrepôt OAI (Optionnel)

- Export depuis Omeka : [repository](https://github.com/nlasolle/omekas2rdf)
- Export depuis entrepôt OAI : (prochainement) 


### 1. Cloner le dépôt

``` bash
git clone https://github.com/votre-utilisateur/sematheque.git
cd sematheque
```

### 2. Créer un environnement virtuel (Recommandé)

``` bash
# Windows
python -m venv venv
venv\Scripts\activate

# macOS / Linux
python3 -m venv venv
source venv/bin/activate
```

### 3. Installer les dépendances

Le fichier `requirements.txt` contient toutes les bibliothèques
nécessaires.

``` bash
pip install -r requirements.txt
```

### 4. Configuration 

``` bash
cp config.json.example config.json
```

Éditez le fichier `config.json` selon vos besoins.

### 5. Lancer l'application

``` bash
python app.py
```

L'application sera accessible à : `http://127.0.0.1:5001`

------------------------------------------------------------------------

## ⚙️ Configuration Avancée (`config.json`)

``` json
{
    "app_settings": {
        "name": "Nom de votre Projet",
        "sparql_endpoint": "URL_DE_VOTRE_ENDPOINT",
        "main_namespace_uri": "URI_DE_BASE_DE_VOS_DONNEES",
        "main_namespace_prefix": "prefixe_principal"
    },
    "prefixes": {
        "prefix1": "http://uri...",
        "prefix2": "http://uri..."
    },
    "visualization": {
        "hidden_properties": ["URI_A_CACHER", "URI_TYPE"],
        "label_properties": [
            "http://www.w3.org/2000/01/rdf-schema#label",
            "http://purl.org/dc/elements/1.1/title"
        ]
    },
    "manual_class_mapping": {
        "Nom Affiché": "URI_DE_LA_CLASSE_RDF"
    }
}
```

------------------------------------------------------------------------

## 📂 Architecture Technique

-   **app.py (Contrôleur)** : Gère les routes HTTP, sessions et cache.
-   **sparql_queries.py (Modèle)** : Génère les requêtes SPARQL.
-   **utils.py (Helpers)** : Nettoyage, formatage, pivot des données.
-   **Constants.py** : Chargement de `config.json`.
-   **templates/** : HTML + Jinja2 + Bootstrap 5.
-   `explore.html` : Filtres dynamiques + JS avancé.
-   `visualization.html` : Graphiques avec Chart.js.

## 🛡️ Notes de sécurité et Performance

-   **SSL** : Patch pour éviter `CERTIFICATE_VERIFY_FAILED`.
-   **Cache** : `lru_cache` + stockage temporaire.
-   **Injection** : Échappement automatique des valeurs utilisateurs.

## 📄 Licence

Projet distribué sous licence MIT.
