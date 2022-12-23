from cmath import nan
from re import sub
from types import TracebackType
from sparql_dataframe import get
import sqlite3 as sql3

import pandas as pd
import json
from rdflib.graph import Graph
from rdflib import DCTERMS, URIRef, RDF, Namespace, Literal

from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from graphClasses import *

from dataModelClasses import QueryProcessor

#filepath = "testData/graph_other_data.json"
keyCount = 0
long_checker = []
authChecker = []

#endpointURI = "http://127.0.0.1:9999/blazegraph/sparql"
my_graph = Graph()

auth_internal_id ={}
auth_internal_id2={}

base_url = "https://FF.github.io/res/"


def fetchInternalauthIDdict():
    return auth_internal_id

# =============== AUTHORS TRIPLES =============== 
# CAUTION!! --> authors can repeat, also multiple authors within same publication need different local id
def uploadauth(filepath):
    with open(filepath, "r", encoding="utf-8") as file:
                    jsondata = json.load(file)
    authors = jsondata['authors']        #<- temp data holder for authrs

    for key in authors:
        globals() ['keyCount'] += 1
        smallist = []
        local_id = "publication-" + str(keyCount)   # NOMERCLARUE FOR NAMING OUR PUBLICATION ENTITY -> ["publication" + "DOI"]
        subj1 = URIRef(base_url + local_id)
        
        #we make the internal ID here with DOI as the key and the author URI as the value
        for value in authors[key]:
            if value['orcid'] not in authChecker:
                local_id2 = "author-"+ str(value['orcid']) # NOMERCLARUE FOR NAMING OUR AUTHOR ENTITY -> ["author" + "ORCID"]
                subj = URIRef(base_url + local_id2)
                auth_internal_id[value['orcid']]= subj
    
                my_graph.add((subj,familyName,Literal(value['family']))) # add family name
                my_graph.add((subj,givenName,Literal(value['given']))) # add given name
                my_graph.add((subj,Id,Literal(value['orcid']))) # add orcid
                authChecker.append(value['orcid'])
                smallist.append(value['orcid'])
            else:
                smallist.append(value['orcid']) 
        
        auth_internal_id2.update({key:smallist})

    resultdata = {"triples":my_graph,"internalID":auth_internal_id,"internalID2":auth_internal_id2}
    return resultdata
