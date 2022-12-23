from cmath import nan
from sparql_dataframe import get

import pandas as pd
import json
from rdflib.graph import Graph, URIRef, RDF, Namespace, Literal 
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from graphClasses import *
import random

#filepath = "testData/graph_other_data.json"
keyCount = 0
#endpointURI = "http://127.0.0.1:9999/blazegraph/sparql"
#venue_checker = []
my_graph = Graph()

publisher_internal_id ={}
base_url = "https://FF.github.io/res/"


def fetchInternalpubliIDdict():
    return publisher_internal_id

# =============== PUBLISHER TRIPLES =============== 
def uploadpubli(filepath):
    with open(filepath, "r", encoding="utf-8") as file:
                    jsondata = json.load(file)

    publi_id = jsondata['publishers']        #<- temp data holder for publishers
    for key in publi_id:
        globals() ['keyCount'] += 1

        base_url = "https://FF.github.io/res/"
        local_id = "publisher-" +str(keyCount)
        subj = URIRef(base_url + local_id)
        publisher_internal_id[key] = subj  #we use the crossref as the key of the dict to store the unique URI of each publisher
        
        # here we are able to work directly with the values inside the dict
        publiid = publi_id[key]['id']
        publititle = publi_id[key]['name']
        my_graph.add((subj,Id,Literal(publiid)))
        my_graph.add((subj,title,Literal(publititle)))
        my_graph.add((subj,RDF.type,Organization))
    
    resultdata = {"triples":my_graph,"internalID":publisher_internal_id}
    return resultdata
