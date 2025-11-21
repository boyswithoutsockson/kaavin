import requests
import polars as pl
from tqdm import tqdm
from lxml import etree
from io import StringIO

def fetch_data():

    topics = []

    response = requests.get("https://api.finto.fi/rest/v1/yso/data?format=application%2Frdf%2Bxml&lang=fi")

    root = etree.fromstring(response.content)

    # Get namespaces
    NS = root.nsmap
    
    concepts = root.xpath(".//skos:Concept", namespaces=NS)

    for concept in concepts:
        term_node = concept.xpath('skos:prefLabel[@xml:lang="fi"]/text()', namespaces=NS)

        # Sometimes empty
        if not term_node:
            continue
        term = term_node[0]
        
        concept_id = concept.attrib[f'{{{NS["rdf"]}}}about'].split("/")[-1]
        topics.append({
            "concept_id": concept_id,
            "term": term
        })

    df = pl.DataFrame(topics)
    df.write_json("finto_topics.json")

if __name__ == "__main__":
    fetch_data()
