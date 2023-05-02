from rdflib import Graph, URIRef, Literal, RDF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from sparql_dataframe import get

import pandas as pd
import json

from ModelClasses import QueryProcessor
from auxiliary import readCSV, readJSON ,dbupdater

# Establishing class object URIs 
JournalArticle = URIRef("https://schema.org/ScholarlyArticle")
BookChapter = URIRef("https://schema.org/Chapter")
ProceedingsPaper = URIRef("http://purl.org/spar/fabio/ProceedingsPaper")

Journal = URIRef("https://schema.org/Periodical")
Book = URIRef("https://schema.org/Book")
Proceedings = URIRef("https://schema.org/EventSeries")

publicationVenue = URIRef("https://schema.org/isPartOf")

publisher = URIRef("https://schema.org/publisher")


id = URIRef("https://schema.org/identifier") # Used for every identifier

# Attributes related to classes
publicationYear = URIRef("https://dbpedia.org/ontology/year")
title = URIRef("https://schema.org/name")
issue = URIRef("https://schema.org/issueNumber")
volume = URIRef("https://schema.org/volumeNumber")
chapter_num = URIRef("https://schema.org/numberedPosition")
event = URIRef("https://schema.org/event")


author = URIRef("https://schema.org/author")
name = URIRef("https://schema.org/givenName")
surname = URIRef("https://schema.org/familyName")
citation = URIRef("https://schema.org/citation")

# Global variables
df1_g = pd.DataFrame()
df2_g = pd.DataFrame()
df3_g = pd.DataFrame()
df4_g = pd.DataFrame()
df5_g = pd.DataFrame()
df6_g = pd.DataFrame()
df7_g = pd.DataFrame()
df8_g = pd.DataFrame()
df9_g = pd.DataFrame()
df10_g = pd.DataFrame()

class TriplestoreProcessor(object):
    def __init__(self):
        self.endpointUrl = ""

    def getEndpointUrl(self):
        if self.endpointUrl == "":
            return "endpointUrl is currently unset" + self.endpointUrl

    def setEndpointUrl(self, new_endpointUrl):
        if isinstance(new_endpointUrl,str):
        #if new_endpointUrl is str:
            self.endpointUrl = new_endpointUrl
            return True
        else:
            return False
        
# ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––

class TriplestoreDataProcessor(TriplestoreProcessor):
    def __init__(self):
        super().__init__()

    def uploadData(self, filepath):
        # Step-1 : read the data into pandas
        global df1_g, df2_g, df3_g, df4_g, df5_g, df6_g, df7_g, df8_g, df9_g, df10_g
        
        # ---------- CSV 
        if filepath.endswith(".csv"):
            #print("entered csvs")
            #df1 -> journal article         // columns = 'id', 'title', 'type', 'publication_year', 'issue', 'volume'
            #df2 -> book-chapter            // columns = 'id', 'title', 'type', 'publication_year', 'chapter'
            #df3 -> proceedings-paper       // columns = 'id', 'title', 'type', 'publication_year'
            #df4 -> Venue_book              // columns = 'id', 'publication_venue', 'venue_type', 'publisher'
            #df5 -> Venue_journal           // columns = 'id', 'publication_venue', 'venue_type', 'publisher'
            #df6 -> Venue_proceedings-event // columns = 'id', 'publication_venue', 'venue_type', 'publisher', 'event
            df1_g, df2_g, df3_g, df4_g, df5_g, df6_g = readCSV(filepath)
            #print("df1_g in the csv branch", df1_g)

            #REMEMBER TO CHANGE THE 'id' column to 'doi' to match the json column name
            
        # ---------- JSON 
        elif filepath.endswith(".json"):
            #df7  -> authors                // columns = 'doi', 'family', 'given', 'orcid'
            #df8  -> VenueIDs              // columns = 'doi', 'issn_isbn'
            #df9  -> citations             // columns = 'doi', 'cited_doi'
            #df10 -> publishers            // columns = 'crossref', 'publisher'
            df7_g, df8_g, df9_g, df10_g = readJSON(filepath)

        triples = Graph()

        base_url = "https://FF.github.io/res/"

        #example
        #subj = URIRef(base_url + local_id2)   #we have to make the subject a URI and only then can it be added to the triple
        #triples.add((subj,familyName,Literal(value['family'])))

        # Step-2 : iterate over the data to create triples
            # Avoid repetition
            #2.1 : store triples for publishers
            #2.2 : store triples for venues
            #2.3 : store triples for authors
            #2.4 : store triples for JA 
            #2.5 : store triples for BC 
            #2.6 : store triples for PP

        # Step-3 : open the connection to the DB and push the triples.

        dbupdater(triples,self.endpointUrl)

        return True

# ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––

class TriplestoreQueryProcessor(QueryProcessor,TriplestoreProcessor):
    def __init__(self):
        super().__init__()

    def getPublicationsPublishedInYear(self, year):
        QR_1 = pd.DataFrame()
        print("don the things here")
        return QR_1

    def getPublicationsByAuthorId(self, orcid):
        QR_2 = pd.DataFrame()
        print("don the things here")
        return QR_2
    
    def getMostCitedPublication(self):
        QR_3 = pd.DataFrame()
        print("don the things here")
        return QR_3
    
    def getMostCitedVenue(self):
        QR_4 = pd.DataFrame()
        print("don the things here")
        return QR_4
    
    def getVenuesByPublisherId(self, crossref):
        QR_5 = pd.DataFrame()
        print("don the things here")
        return QR_5
    
    def getPublicationInVenue(self, issn_isbn):
        QR_6 = pd.DataFrame()
        print("don the things here")
        return QR_6
    
    def getJournalArticlesInIssue(self, issue, volume, ja_id):
        QR_7 = pd.DataFrame()
        print("don the things here")
        return QR_7
    
    def getJournalArticlesInVolume(self, volume, ja_id):
        QR_8 = pd.DataFrame()
        print("don the things here")
        return QR_8
    
    def getJournalArticlesInJournal(self, ja_id):
        QR_9 = pd.DataFrame()
        print("don the things here")
        return QR_9
    
    def getProceedingsByEvent(self, eventName):
        QR_10 = pd.DataFrame()
        print("don the things here")
        return QR_10
    
    def getPublicationAuthors(self, doi):
        QR_11 = pd.DataFrame()
        print("don the things here")
        return QR_11
    
    def getPublicationsByAuthorName(self, authorName):
        QR_12 = pd.DataFrame()
        print("don the things here")
        return QR_12
    
    def getDistinctPublisherOfPublications(self, doi_list):
        QR_13 = pd.DataFrame()
        print("don the things here")
        return QR_13

# ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––

# TEST AREA
""" 
grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
grp_dp = TriplestoreDataProcessor()
grp_dp.setEndpointUrl(grp_endpoint)
grp_dp.uploadData("testData/graph_publications.csv")
grp_dp.uploadData("testData/graph_other_data.json")

# Checking the superclass is correct or not
# print(grp_dp.__bases__)

grp_qp = TriplestoreQueryProcessor()
grp_qp.setEndpointUrl(grp_endpoint)


# Checking the superclass is correct or not
# print(grp_qp.__bases__) """
