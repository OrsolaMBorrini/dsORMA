from cmath import nan
import imp
from itertools import count
from re import sub
from sparql_dataframe import get
import sqlite3 as sql3

import pandas as pd
import json
from rdflib.graph import Graph
from rdflib import DCTERMS, URIRef, RDF, Namespace, Literal

from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from graphClasses import *

from dataModelClasses import QueryProcessor

#Imported functions to fetch internal ids dicts
from clipboardPublications import fetchInternalpubIDdict
from clipboardAuthors import fetchInternalauthIDdict
from clipboardPublisher import fetchInternalpubliIDdict

#filepath = "testData/graph_other_data.json"
keyCount = 0

venue_checker = []
my_graph = Graph()

venue_internal_id ={}
venue_internal_id2 = {}
# BOTH ISSN AND ISBN AND ID for VENUES

def fetchInternalvenuIDdict():
    return venue_internal_id

# =============== VENUES TRIPLES =============== 
def uploadvenu(filepath):
    with open(filepath, "r", encoding="utf-8") as file:
                    jsondata = json.load(file)
    base_url = "https://FF.github.io/res/"
    venues_id = {}
    venues_id = jsondata["venues_id"]

    for key in venues_id:
        globals() ['keyCount'] += 1
        local_id = "venue-" +str(keyCount)
        subj = URIRef(base_url + local_id)
        unique = 1
        '''
        local_id1 = "publication-" + str(key)
        subj_DOI = URIRef(base_url + local_id1)
        internal_id1[key] = subj_DOI
        '''
        # we will add our venues for this doi now # for keeping it short<testing>
        for value in venues_id[key]: # here we are able to work directly with the values inside the dict
            # !!!! VENUES might repeat!!! so we need to check and make sure it is unique!!!
            
            if value not in venue_checker and unique == 1:
                venue_internal_id[value] = subj   # we strore the ISSN/ISBN as the key and the URI as the value
                venue_internal_id2[key] = subj
                my_graph.add((subj,Id,Literal(value))) 
                my_graph.add((subj,RDF.type,Venue,))  
                
                #THINGS TO DO WITH THE INTERNAL ID
                # NEED TO ADD THE VENUE TYPE --> Book / Journal / Proceedings when working on publications subgraph
                # NEED TO ADD THE VENUE TITLE --> when working on the publications subgraph
                # NEED TO ADD THE PUBLISER OF THIS VENUE - this need the internal ID and  the URI of the publisher

            venue_checker.append(value)
            unique = 0

    resultdata = {"triples":my_graph,"internalID":venue_internal_id,"internalID2":venue_internal_id2}
    return resultdata 


