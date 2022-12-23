from cmath import nan
from sparql_dataframe import get
import sqlite3 as sql3

import pandas as pd
import json
from rdflib.graph import Graph
from rdflib import DCTERMS, URIRef, RDF, Namespace, Literal

from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from graphClasses import *
from dataModelClasses import QueryProcessor

#filepath = "testData/graph_publications.csv"   #comes from user, thus will change
count = 0
long_checker = []

#endpointURI = "http://127.0.0.1:9999/blazegraph/sparql"
my_graph = Graph()

internal_id1 ={}
internal_id2 ={}
base_url = "https://FF.github.io/res/"

def fetchInternalpubIDdict():
    return internal_id1

def uploadpub(filepath):
    raw_publications = pd.read_csv(filepath)
    publications_df = pd.DataFrame({
        "id": raw_publications['id'].astype('str'),
        "title":raw_publications['title'].astype('str') ,
        "type": raw_publications['type'].astype('str'),
        "publication_year": raw_publications['publication_year'].astype('str'),
        "issue": raw_publications['issue'].astype('str'),
        "volume": raw_publications['volume'].astype('str'),
        "chapter": raw_publications['chapter'].astype('str'),
        "publication_venue": raw_publications['publication_venue'].astype('str'),
        "venue_type":raw_publications['venue_type'].astype('str'),
        "publisher_id":raw_publications['publisher'].astype('str'),
        "event":raw_publications['event']
    })
    publications_df.fillna('',inplace=True)

    for idx, row in publications_df.iterrows():
                local_id = "publication-" + str(row['id'])
                subj = URIRef(base_url + local_id)
                '''
                globals() ['count'] +=1
                print(count)
                print(subj)
                '''
                #we store our 1st internal id over here 
                internal_id1[row["id"]] = subj
                #we store our 2nd internal id over here - only useful for connecting to publishers graph
                internal_id2[row["publisher_id"]] = subj

                my_graph.add((subj, RDF.type, Publication))
                #we set the condtion for all the publications types
                if row["type"] == "journal-article":
                    my_graph.add((subj, RDF.type, JournalArticle)) #we make the JA exclusive triples here
                    #issue + volume triples
                elif row["type"] == "book-chapter":
                    my_graph.add((subj, RDF.type, BookChapter)) #we make the BC exclusive triples here
                    #add triple for book chapter here
                elif row["type"] == "proceedings-paper":
                    my_graph.add((subj, RDF.type, ProceedingsPaper)) #we make the PP exclusive triple here
                    # add event triple over here
                
                #now we make the general triples of publication type here
                my_graph.add((subj, Id, Literal(row["id"])))  #add triple for ID -> doi
                my_graph.add((subj, title, Literal(row["title"]))) #add triple for title -> publication title
                my_graph.add((subj, publicationYear, Literal(row["publication_year"]))) #add triple for year -> publication_year
                my_graph.add((subj, publicationVenue, Literal(row["publication_venue"])))  #add triple for venue -> publication venue name/title
                #we set the condition to make triples for venue type(we have 3 venue types!!)
                
                if row["venue_type"] == "journal":
                    my_graph.add((subj,DCTERMS.isPartOf,Journal)) #need to use A DIFFERENT PREDICATE <!!IMPORTANT> we need to fetch the title from JSON df
                    my_graph.add((subj,issue,Literal(row['issue'])))
                    my_graph.add((subj,volume,Literal(row['volume'])))
                elif row["venue_type"] == "book":
                    my_graph.add((subj,DCTERMS.isPartOf,Book))#need to use A DIFFERENT PREDICATE <!!IMPORTANT>
                    my_graph.add((subj,chapterNumber,Literal(row['chapter'])))
                elif row["venue_type"] == "proceedings": 
                    my_graph.add((subj,DCTERMS.isPartOf,Proceedings))#need to use A DIFFERENT PREDICATE <!!IMPORTANT>
                    my_graph.add((subj,event,Literal(row['event'])))
                
    resultdata = {"triples":my_graph,"internalID":internal_id1}
    return resultdata