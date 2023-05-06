from rdflib import Graph, URIRef, Literal, RDF
import urllib.parse
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from sparql_dataframe import get

import pandas as pd
import json

from ModelClasses import QueryProcessor
from auxiliary import readCSV, readJSON ,dbupdater

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

# Establishing class object URIs 
Publication = URIRef("http://purl.org/spar/fabio/Expression")

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

pubURIs = dict()
venueURIs = dict()
authorURIs = dict()
publisherURIs = dict()

class TriplestoreProcessor(object):
    def __init__(self):
        self.endpointUrl = ""

    def getEndpointUrl(self):
        if self.endpointUrl == "":
            return "endpointUrl is currently unset" + self.endpointUrl
        else:
            return self.endpointUrl

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
        global df1_g, df2_g, df3_g, df4_g, df5_g, df6_g, df7_g, df8_g, df9_g, df10_g
        # Step-1 : read the data into pandas
        
        base_url = "https://FF.github.io/res/"
        triples = Graph()
        # ---------- CSV 
        if filepath.endswith(".csv"):
        
            # df1_g -> journal article         // columns = 'id_doi', 'title', 'type','publication_venue', 'publication_year', 'issue', 'volume'
            # df2_g -> book-chapter            // columns = 'id_doi', 'title', 'type','publication_venue', 'publication_year', 'chapter'
            # df3 -> proceedings-paper       // columns = 'id_doi', 'title', 'type','publication_venue', 'publication_year'
            # df4 -> Venue_book              // columns = 'id_doi', 'publication_venue', 'venue_type', 'id_crossref'
            # df5 -> Venue_journal           // columns = 'id_doi', 'publication_venue', 'venue_type', 'id_crossref'
            # df6 -> Venue_proceedings-event // columns = 'id_doi', 'publication_venue', 'venue_type', 'id_crossref', 'event'
            df1_g, df2_g, df3_g, df4_g, df5_g, df6_g = readCSV(filepath)

            # making VeJ triples
            for index, row in df5_g.iterrows():
                
                localID = "venue-" +str(row["publication_venue"]).replace(" ", "")
                subj = URIRef(base_url+urllib.parse.quote(localID))

                if row["publication_venue"] in venueURIs:
                    pass
                else: 
                    triples.add((subj,RDF.type,Journal))                                       # add rdf sub-type
                    triples.add((subj,title,Literal(row['publication_venue'])))                # add venue title

                    # add the URI to the URI dict
                    venueURIs.update({row["publication_venue"]:subj})

            # making VeB triples
            for index, row in df4_g.iterrows():

                localID = "venue-" +str(row["publication_venue"]).replace(" ", "")
                subj = URIRef(base_url+localID)

                if row["publication_venue"] in venueURIs:
                    pass
                else: 
                    triples.add((subj,RDF.type,Book))                                          # add rdf sub-type
                    triples.add((subj,title,Literal(row['publication_venue'])))                # add venue title

                    # add the URI to the URI dict
                    venueURIs.update({row["publication_venue"]:subj})

            # making VePE triples
            for index, row in df6_g.iterrows():

                localID = "venue-" +str(row["publication_venue"]).replace(" ", "")
                subj = URIRef(base_url+localID)

                if row["publication_venue"] in venueURIs:
                    pass
                else: 
                    triples.add((subj,RDF.type,Proceedings))                                   # add rdf sub-type
                    triples.add((subj,title,Literal(row['publication_venue'])))                # add venue title
                    triples.add((subj,event,Literal(row['event'])))                            # add venue event title

                    # add the URI to the URI dict
                    venueURIs.update({row["publication_venue"]:subj})   

            # making JA triples
            for index, row in df1_g.iterrows():

                localID = "publication-" +str(row["id_doi"])
                subj = URIRef(base_url+localID)

                if row["id_doi"] in pubURIs:
                    pass
                else: 
                    triples.add((subj,RDF.type,Publication))                                    # add rdf type
                    triples.add((subj,RDF.type,JournalArticle))                                 # add subclass type
                    triples.add((subj,id,Literal(row["id_doi"])))                               # add id_doi
                    triples.add((subj,title,Literal(row["title"])))                             # add title
                    triples.add((subj,publicationYear,Literal(row["publication_year"])))        # add publication year
                    triples.add((subj,issue,Literal(row["issue"])))                             # add issue number
                    triples.add((subj,volume,Literal(row["volume"])))                           # add issue number

                    #adding the relation to the venue
                    #need to match the doi to get the publication venue
                    if row["publication_venue"] in venueURIs:
                        triples.add((subj,publicationVenue,venueURIs[row["publication_venue"]])) # add venue relation
                    else:
                        pass

                    # add the URI to the URI dict
                    pubURIs.update({row["id_doi"]:subj})

            # making BC triples
            for index, row in df2_g.iterrows():

                localID = "publication-" +str(row["id_doi"])
                subj = URIRef(base_url+localID)

                if row["id_doi"] in pubURIs:
                    pass
                else: 
                    triples.add((subj,RDF.type,Publication))                                    # add rdf type
                    triples.add((subj,RDF.type,BookChapter))                                    # add subclass type
                    triples.add((subj,id,Literal(row["id_doi"])))                               # add id_doi
                    triples.add((subj,title,Literal(row["title"])))                             # add title
                    triples.add((subj,publicationYear,Literal(row["publication_year"])))        # add publication year
                    triples.add((subj,BookChapter,Literal(row["chapter"])))                     # add chapter


                    #adding the relation to the venue
                    if row["publication_venue"] in venueURIs:
                        triples.add((subj,publicationVenue,venueURIs[row["publication_venue"]])) # add venue relation
                    else:
                        pass
                    

                    # add the URI to the URI dict
                    pubURIs.update({row["id_doi"]:subj})
                    
            # making PP triples
            for index, row in df3_g.iterrows():

                localID = "publication-" +str(row["id_doi"])
                subj = URIRef(base_url+localID)

                if row["id_doi"] in pubURIs:
                    pass
                else: 
                    triples.add((subj,RDF.type,Publication))                                    # add rdf type
                    triples.add((subj,RDF.type,ProceedingsPaper))                               # add subclass type
                    triples.add((subj,id,Literal(row["id_doi"])))                               # add id_doi
                    triples.add((subj,title,Literal(row["title"])))                             # add title
                    triples.add((subj,publicationYear,Literal(row["publication_year"])))        # add publication year


                    #adding the relation to the venue
                    if row["publication_venue"] in venueURIs:
                        triples.add((subj,publicationVenue,venueURIs[row["publication_venue"]])) # add venue relation
                    else:
                        pass
                    

                    # add the URI to the URI dict
                    pubURIs.update({row["id_doi"]:subj})

       
        # ---------- JSON 
        elif filepath.endswith(".json"):
            #df7  -> authors                // columns = 'doi', 'family', 'given', 'orcid'
            #df8  -> VenueIDs              // columns = 'doi', 'issn_isbn'
            #df9  -> citations             // columns = 'doi', 'cited_doi'
            #df10 -> publishers            // columns = 'crossref', 'publisher'
            df7_g, df8_g, df9_g, df10_g = readJSON(filepath)

            # make publisher triples
            for index, row in df10_g.iterrows():

                localID = "publisher-" +str(row["crossref"])
                subj = URIRef(base_url+localID)

                if row["crossref"] in publisherURIs:
                    pass
                else: 
                    triples.add((subj,RDF.type,publisher))                                          # add rdf type
                    triples.add((subj,id,Literal(row["crossref"])))                                 # add id_doi
                    triples.add((subj,title,Literal(row["publisher"])))                             # add title
                    

                    # add the URI to the URI dict
                    publisherURIs.update({row["crossref"]:subj})

            # malke author triples
            for index, row in df7_g.iterrows():

                localID = "author-" +str(row["orcid"])
                subj = URIRef(base_url+localID)

                if row["orcid"] in authorURIs:
                    pass
                else: 
                    triples.add((subj,RDF.type,author))                                      # add rdf type
                    triples.add((subj,id,Literal(row["orcid"])))                             # add orcid
                    triples.add((subj,name,Literal(row["given"])))                           # add first name
                    triples.add((subj,surname,Literal(row["family"])))                       # add first name
                    

                    # add the URI to the URI dict
                    authorURIs.update({row["orcid"]:subj})

        if df10_g.empty is False and df1_g.empty is False:
            print("JSON + CSV is uploaded")

            # make publication-author relations and publication-citations relations
            for k in pubURIs:
                subjP = pubURIs[k]
                # group by DOI to get all the authors of 'k'
                authgrps = df7_g.groupby(['doi'])
                authdf = authgrps.get_group(k)
                for index,row in authdf.iterrows():
                    if row['orcid'] in authorURIs:
                        triples.add((subjP,author,authorURIs[row['orcid']]))

                # group by DOI to get all citations of 'k'
                citgrp = df9_g.groupby(['doi'])
                if k in citgrp.groups.keys():
                    citdf = citgrp.get_group(k)
                    for index,row in citdf.iterrows():
                        if row['cited_doi'] in pubURIs:
                            triples.add((subjP,citation,pubURIs[row['cited_doi']]))
                else:
                    continue

            # make venue-publisher relations
            for k in venueURIs:
                subjV = venueURIs[k]

                for index,row in df4_g.iterrows():
                    if row['publication_venue'] == k:
                        triples.add((subjV,publisher,publisherURIs[row['id_crossref']]))
                    else:
                        continue
                for index,row in df5_g.iterrows():
                    if row['publication_venue'] == k:
                        triples.add((subjV,publisher,publisherURIs[row['id_crossref']]))
                    else:
                        continue
                for index,row in df6_g.iterrows():
                    if row['publication_venue'] == k:
                        triples.add((subjV,publisher,publisherURIs[row['id_crossref']]))
                    else:
                        continue

                # make venue ids triples
                for index,row in df1_g.iterrows():
                    if row['publication_venue'] == k:
                        # make the grouped object by doi to get all possible issn/isbn values in a single df
                        issn_isbngrps = df8_g.groupby(['doi'])
                        VeIDS = issn_isbngrps.get_group(row['id_doi'])

                        for idx,row in VeIDS.iterrows():
                            triples.add((subjV,id,Literal(row['issn_isbn'])))
                
                for index,row in df2_g.iterrows():
                    if row['publication_venue'] == k:
                        # make the grouped object by doi to get all possible issn/isbn values in a single df
                        issn_isbngrps = df8_g.groupby(['doi'])
                        VeIDS = issn_isbngrps.get_group(row['id_doi'])

                        for idx,row in VeIDS.iterrows():
                            triples.add((subjV,id,Literal(row['issn_isbn'])))

                for index,row in df3_g.iterrows():
                    if row['publication_venue'] == k:
                        # make the grouped object by doi to get all possible issn/isbn values in a single df
                        issn_isbngrps = df8_g.groupby(['doi'])
                        VeIDS = issn_isbngrps.get_group(row['id_doi'])

                        for idx,row in VeIDS.iterrows():
                            triples.add((subjV,id,Literal(row['issn_isbn'])))

        else:
            pass
            
        

        #example

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
        if isinstance(year,int):
            QR_1 = pd.DataFrame()
            endpoint = self.getEndpointUrl()
            query = """
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <https://schema.org/>
            PREFIX fabio: <http://purl.org/spar/fabio/>
            PREFIX dbpedia: <https://dbpedia.org/ontology/>

            SELECT ?publication1 ?year
            WHERE {{
            ?publication1 rdf:type fabio:Expression;
                            dbpedia:year ?year.  
            FILTER (?year="{yearp}"^^xsd:int)
            }}
            """
            QR_1 = get(endpoint,query.format(yearp=year),True)
            return QR_1
        else:
            raise Exception("The input parameter is not an integer!")

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

grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
grp_dp = TriplestoreDataProcessor()
grp_dp.setEndpointUrl(grp_endpoint)
#grp_dp.uploadData("testData/graph_publications.csv")
#grp_dp.uploadData("testData/graph_other_data.json")

# Checking the superclass is correct or not
# print(grp_dp.__bases__)

grp_qp = TriplestoreQueryProcessor()
grp_qp.setEndpointUrl(grp_endpoint)

'''
for i in publisherURIs:
    print (i, publisherURIs[i])
'''

Q1 = grp_qp.getPublicationsPublishedInYear(2020)
print(Q1)