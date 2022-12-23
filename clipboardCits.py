import pandas as pd
import json
from rdflib.graph import Graph, URIRef, RDF, Namespace, Literal 
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from graphClasses import *

#cits_internal_id ={}
my_graph = Graph()

def uploadcits(filepath):
    with open(filepath, "r", encoding="utf-8") as file:
                    jsondata = json.load(file)

    cits = jsondata['references']        #<- temp data holder for references
    for key in cits:
        base_url = "https://FF.github.io/res/"
        local_id = "publication-" + str(key)
        subj = URIRef(base_url + local_id)
        for item in cits[key]:
            local_id2 = "publication-" + str(item)
            subj2 = URIRef(base_url + local_id2)
            my_graph.add((subj,cites,subj2))

    
    return my_graph