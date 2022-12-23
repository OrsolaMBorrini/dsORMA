from dataModelClasses import QueryProcessor
from graphClasses import *
from pubMemory_full import authorslist

from sparql_dataframe import get
import pandas as pd
import json
import rdflib as rdf
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore #this is used for pushing our graph to balzegraph

#importing the triplecreator func from other files
from clipboardPublications import uploadpub
from clipboardPublisher import uploadpubli
from clipboardAuthors import uploadauth
from clipboardVenues import uploadvenu
from clipboardCits import uploadcits
from pubMemory_full import authorslist

#MY INTERNAL ID VARIABLES -->>
paths=[]
auth_internal_id ={}
auth_internal_id2 = {}
publisher_internal_id ={}
venue_internal_id ={}
venue_internal_id2 = {} #stores ISSN/ISBN with URI
publication_internal_id ={}


def dbupdater(graphvariable,endpointURI):
    store = SPARQLUpdateStore()
    # The URL of the SPARQL endpoint is the same URL of the Blazegraph
    # instance + '/sparql'
    # It opens the connection with the SPARQL endpoint instance
    #endpointURI = "http://127.0.0.1:9999/blazegraph/sparql"  
    #endpointURI = endpointURI + '/sparql'
    store.open((endpointURI,endpointURI))

    for triple in graphvariable.triples((None, None, None)):
        store.add(triple)
            
    # close connection 
    store.close()

def entityconnector(paths,endpoint):
    # here we make the connections in between the publication, venues, authors and publisher entities
    myGraph = rdf.Graph()
    #for path in paths:
    path = "testData/graph_publications.csv"
    if path.endswith('.csv'):
        raw_publications = pd.read_csv(path)

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
        # 1 -- connect venue to the publisher:
        for id in venue_internal_id:
            for idx, row in publications_df.iterrows():
                if row['id'] == id:
                    crossref = row['publisher_id']
                    for key in venue_internal_id2:
                        if venue_internal_id2[key]==venue_internal_id[id]:
                            venueID = key
                    publisherURI = publisher_internal_id[crossref]
                    myGraph.add((venue_internal_id[id],publisher,publisherURI))
                    #we store our 1st internal id over here  

    # 2 -- connect publication to the venue:
        if publication_internal_id != {}:
            for id in publication_internal_id:
                for idx, row in publications_df.iterrows():
                    if row['id'] == id:
                        for key in venue_internal_id:
                            if key ==id:
                                venueURI = venue_internal_id[key]
                                publicationURI = publication_internal_id[id]
                                myGraph.add((publicationURI,publicationVenue,venue_internal_id[id]))
                                myGraph.add((venue_internal_id[id],title,rdf.Literal(row['publication_venue'])))
                        for key2 in auth_internal_id2:
                            if key2 ==id:
                                for item in auth_internal_id2[key2]:
                                    for orci in auth_internal_id:
                                        if item == orci:
                                            myGraph.add((publication_internal_id[id],author,auth_internal_id[orci]))
                    #we store our 1st internal id over here  

    # 3 -- connect publication to the author
    dbupdater(myGraph,endpoint)
    return

class TriplestoreProcessor(object):
    
    # bibliography: https://pynative.com/python-class-variables/#:~:text=In%20Python%2C%20Class%20variables%20are,all%20objects%20of%20the%20class.

    def __init__(self):
        self.endpointUrl = ""

    def getEndpointUrl(self):
        return self.endpointUrl

    def setEndpointUrl(self,endpointUrl):
        if isinstance(endpointUrl, str):
            self.endpointUrl = endpointUrl
            return True
        else:
            return False

class TriplestoreDataProcessor(TriplestoreProcessor):
    def __init__(self):
        super().__init__()

    def uploadData(self, path):
        if not isinstance(path, str):
            return False + "Please, retry inserting an input string correspondent to the file path."
        else:
            data = {}
            autherdata = {}
            publidata = {}
            venudata = {}
            # ===== BUILD TRIPLES FROM CSV ===== 
            if path.endswith('.csv'):
                paths.append(path)
                endpaoint = self.getEndpointUrl()
                #call the triple creator function, SHOULD return my_graph variables with the triples and the internal ID dict
                   #stores the triples and the internal ids from publications
                data = uploadpub(path)
                globals()['publication_internal_id'] = data['internalID']
                #call the function to push the triples to the  db i.e, dbupdater function

                dbupdater(data['triples'],self.getEndpointUrl())

                #call the connector function here
                entityconnector(path,endpaoint)
            # ===== BUILD TRIPLES FROM JSON ===== 
            elif path.endswith(".json"):
                authorslist(path)   #this adds the authors to the pub_memory
                paths.append(path)
                endpaoint = self.getEndpointUrl()
                #call the triple creator for authors
                
                autherdata = uploadauth(path) 
                globals()['auth_internal_id'] = autherdata['internalID']
                globals()['auth_internal_id2'] = autherdata['internalID2']
                
                #call the function to push the triples to the  db i.e, dbupdater function

                dbupdater(autherdata['triples'],self.getEndpointUrl())

                #call the triple creator for publishers
                
                publidata = uploadpubli(path)
                globals()['publisher_internal_id'] = publidata['internalID']
                #call the function to push the triples to the  db i.e, dbupdater function
                
                dbupdater(publidata['triples'],self.getEndpointUrl())

                #call the triple creator for the venues
                
                venudata = uploadvenu(path)
                globals()['venue_internal_id'] = venudata['internalID2']
                globals()['venue_internal_id2'] = venudata['internalID']
                #call the function to push the triples to the  db i.e, dbupdater function
                
                dbupdater(venudata['triples'],self.getEndpointUrl())

                citdata = uploadcits(path)
                dbupdater(citdata,self.getEndpointUrl())
            #call the connector function here
            entityconnector(path,endpaoint)
        return True

# ===== HERE YOU MUST IMPLEMENT THE TRIPLESTORE QUERY PROCESSOR (REMEMBER TO IMPORT THE QUERYPROCESSOR CLASS FROM A FILE)

class TriplestoreQueryProcessor(QueryProcessor, TriplestoreProcessor):
    def __init__(self):
        super().__init__()

    def getPublicationsPublishedInYear(self,year):
        endpoint = self.getEndpointUrl()
        query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>
        PREFIX fabio: <http://purl.org/spar/fabio/>
        
        SELECT ?id
        WHERE {{
        ?publication1 rdf:type fabio:Expression ;
                      schema:identifier ?id;
                    schema:datePublished "{yearp}" .
        }}
        """
        result = get(endpoint,query.format(yearp=year),True)
        return result
    
    def getPublicationsByAuthorId(self,id):
        endpoint = self.getEndpointUrl()
        query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>
        PREFIX fabio: <http://purl.org/spar/fabio/>
        PREFIX frbr: <http://purl.org/vocab/frbr/core#>
        
        SELECT ?publication
        WHERE {{
        ?publication rdf:type fabio:Expression ;
                    frbr:creator ?author .
        ?author schema:identifier "{authId}"
        }}
        """
        result = pd.DataFrame()
        result = get(endpoint,query.format(authId=id),True)
        #print(type(result))
        return result

    def getMostCitedPublication(self):
        endpoint = self.getEndpointUrl()
        #this gets the uri of the most cites one!
        query = """ 
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX frbr: <http://purl.org/vocab/frbr/core#>
        PREFIX schema: <https://schema.org/>
        PREFIX fabio: <http://purl.org/spar/fabio/>


        SELECT ?ref_doi (COUNT(?cited) AS ?num_citations) 
        WHERE { 
        ?publication schema:citation ?cited.
        ?cited schema:identifier ?ref_doi.
        }
        GROUP BY ?ref_doi
        ORDER BY desc(?num_citations)
        limit 1
        """
        result1 = pd.DataFrame()
        result1 = get(endpoint,query,True)
        '''
        for index,row in result1.iterrows():
            uri = row['citation']
            count = row['cited']

        query2 = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX frbr: <http://purl.org/vocab/frbr/core#>
        PREFIX schema: <https://schema.org/>
        PREFIX fabio: <http://purl.org/spar/fabio/>


        SELECT ?ref_doi
        WHERE{{
        ?publication ?p ?o;
                    schema:identifier ?ref_doi.
        FILTER(regex(str(?publication), "https://FF.github.io/res/publication-doi:10.1093/nar/gkz997"))
        }}
        LIMIT 1
        """
        result = get(endpoint,query2.format(uri=uri),True)
        countf = {"num_citations":[count]}
        countDF = pd.DataFrame(countf)
        c = pd.concat([result, countDF], axis=1, ignore_index=False)
        '''
        return result1

    def getMostCitedVenue(self):
        endpoint = self.getEndpointUrl()
        query = """ 
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX frbr: <http://purl.org/vocab/frbr/core#>
        PREFIX schema: <https://schema.org/>
        PREFIX fabio: <http://purl.org/spar/fabio/>

        SELECT ?name_venue ?issn_isbn ?publisher ?name_pub(COUNT(?venue) AS ?num_cit) 
        WHERE {  
                ?publication schema:citation ?citation;
                            schema:identifier ?id.
                ?citation frbr:embodimentOf ?venue.
                ?venue rdf:type fabio:Manifestation;
                    schema:name ?name_venue;
                    schema:identifier ?issn_isbn;
                    schema:publisher ?organisation.
                ?organisation schema:identifier ?publisher;
                        schema:name ?name_pub.
        
                    
        }
        GROUP BY ?venue ?name_venue ?issn_isbn ?publisher ?name_pub
        ORDER BY desc(?num_cit)
        LIMIT 1
        """
        result = get(endpoint,query,True)
        return result
        
    def getVenuesByPublisherId(self,id):
        endpoint = self.getEndpointUrl()
        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX frbr: <http://purl.org/vocab/frbr/core#>
        PREFIX schema: <https://schema.org/>
        PREFIX fabio: <http://purl.org/spar/fabio/>
        
        SELECT ?venue
        WHERE {{
        ?venue rdf:type fabio:Manifestation ;
                schema:publisher ?publisher .
        ?publisher schema:identifier "{pubId}"
        }}
        """

        result = get(endpoint,query.format(pubId=id),True)
        return result
        
    def getPublicationInVenue(self,venueId):
        endpoint = self.getEndpointUrl()
        query = """ 
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX frbr: <http://purl.org/vocab/frbr/core#>
        PREFIX schema: <https://schema.org/>
        PREFIX fabio: <http://purl.org/spar/fabio/>

        SELECT ?id
        WHERE {{
        ?publication rdf:type fabio:Expression ;
                    frbr:embodimentOf ?venue;
                    schema:identifier ?id.
        ?venue rdf:type fabio:Manifestation;
                schema:identifier "{issn}".
        }}
        """
        result = get(endpoint,query.format(issn=venueId),True)
        return result

    def getJournalArticlesInIssue(self,issue,volume,journalId):
        endpoint = self.getEndpointUrl()
        query = """ 
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX frbr: <http://purl.org/vocab/frbr/core#>
        PREFIX schema: <https://schema.org/>
        PREFIX fabio: <http://purl.org/spar/fabio/>

        SELECT ?id
        WHERE {{
        ?publication rdf:type fabio:Expression ;
                    rdf:type fabio:JournalArticle;
                    schema:identifier ?id;
                    schema:issueNumber "{issueN}";
                    schema:volumeNumber "{volumeN}";
                    frbr:embodimentOf ?venue.
        ?venue rdf:type fabio:Manifestation;
                schema:identifier "{issn}".             
  
        }}
        """
        result = get(endpoint,query.format(issueN=issue,volumeN=volume,issn=journalId),True)
        return result

    def getJournalArticlesInVolume(self,volume,journalId):
        endpoint = self.getEndpointUrl()
        query = """ 
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX frbr: <http://purl.org/vocab/frbr/core#>
        PREFIX schema: <https://schema.org/>
        PREFIX fabio: <http://purl.org/spar/fabio/>

        SELECT ?id
        WHERE {{
        ?publication rdf:type fabio:Expression ;
                    rdf:type fabio:JournalArticle;
                    schema:identifier ?id;
                    schema:volumeNumber "{volumeN}";
                    frbr:embodimentOf ?venue.
        ?venue rdf:type fabio:Manifestation;
                schema:identifier "{issnN}".  
        }}
        """
        result = get(endpoint,query.format(volumeN=volume,issnN=journalId),True)
        return result

    def getJournalArticlesInJournal(self,journalId):
        endpoint = self.getEndpointUrl()
        query = """ 
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX frbr: <http://purl.org/vocab/frbr/core#>
        PREFIX schema: <https://schema.org/>
        PREFIX fabio: <http://purl.org/spar/fabio/>

        SELECT ?id
        WHERE {{
        ?publication rdf:type fabio:Expression ;
                    rdf:type fabio:JournalArticle;
                    schema:identifier ?id;
                    frbr:embodimentOf ?venue.
        ?venue rdf:type fabio:Manifestation;
                schema:identifier "{issn}".
        }}
        """
        result = get(endpoint,query.format(issn=journalId),True)
        return result

    def getProceedingsByEvent(self,eventPartialName):
        endpoint = self.getEndpointUrl()
        query = """ 
                PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX frbr: <http://purl.org/vocab/frbr/core#>
                PREFIX schema: <https://schema.org/>
                PREFIX fabio: <http://purl.org/spar/fabio/>
                PREFIX dcterm: <http://purl.org/dc/terms/>
                select ?s ?n
                where{{
                ?s rdf:type fabio:ProceedingsPaper;
                    schema:event ?n.
                filter(regex(str(?n), '{partName}',"i"))  
                        }}
                """
        result = get(endpoint,query.format(partName=eventPartialName),True)
        return result

    def getPublicationAuthors(self,publicationId):
        endpoint = self.getEndpointUrl()
        query = """ 
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX frbr: <http://purl.org/vocab/frbr/core#>
        PREFIX schema: <https://schema.org/>
        PREFIX fabio: <http://purl.org/spar/fabio/>

        SELECT ?orcid ?firstName ?surName
        WHERE {{
        ?publication rdf:type fabio:Expression ;
                    rdf:type fabio:JournalArticle;
                    schema:identifier "{doi}";
                    frbr:creator ?author.
        ?author schema:identifier ?orcid;
                schema:givenName ?firstName;
                schema:familyName ?surName.
                       
        }}
        """
        result = get(endpoint,query.format(doi=publicationId),True)
        return result

    def getPublicationsByAuthorName(self,authorPartialName):
        endpoint = self.getEndpointUrl()
        query = """ 
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX frbr: <http://purl.org/vocab/frbr/core#>
        PREFIX schema: <https://schema.org/>
        PREFIX fabio: <http://purl.org/spar/fabio/>

        SELECT ?orcid ?firstName ?surName ?id
        WHERE {{
        ?publication rdf:type fabio:Expression ;
                    schema:identifier ?id;
                    frbr:creator ?author.
        ?author schema:identifier ?orcid;
                schema:givenName ?firstName;
                schema:familyName ?surName.
        filter(regex(str(?firstName), "{nome}","i") || regex(str(?surName), "{nome}","i"))                 
        }}
        """
        result = get(endpoint,query.format(nome=authorPartialName),True)
        return result

    def getDistinctPublishersOfPublications(self,pubIdList):
        result = pd.DataFrame()
        for item in pubIdList:
            endpoint = self.getEndpointUrl()
            query = """ 
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX frbr: <http://purl.org/vocab/frbr/core#>
            PREFIX schema: <https://schema.org/>
            PREFIX fabio: <http://purl.org/spar/fabio/>

            SELECT ?publisher ?crossref ?title
            WHERE {{
            ?publication rdf:type fabio:Expression ;
                        schema:identifier "{doi}";
                        frbr:embodimentOf ?venue.
            ?venue rdf:type fabio:Manifestation;
                    schema:publisher ?publisher.
            ?publisher schema:identifier ?crossref;
                        schema:name ?title.
                
            }}
            """
            result_q = get(endpoint,query.format(doi=item),True)
            result = pd.concat([result,result_q])
        return result

    # ADDITIONAL METHODS FOR MAKING THE DF-CREATOR TO WORK
    def getPublicationsFromDOI(self,DOI):   
        result = pd.DataFrame()
        for item in DOI:
            doi = item
            endpoint = self.getEndpointUrl()
            query = """ 
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX frbr: <http://purl.org/vocab/frbr/core#>
            PREFIX schema: <https://schema.org/>
            PREFIX fabio: <http://purl.org/spar/fabio/>

            SELECT ?publication_year ?title ?id ?issue ?volume ?chapter_number ?publication_venue ?ref_doi ?issn_isbn ?publisher ?name_pub
            WHERE {{
            ?publication rdf:type fabio:Expression ;
                        schema:identifier "{doi}";
                        schema:identifier ?id;
                        frbr:embodimentOf ?venue;
                        schema:datePublished ?publication_year;
                        schema:name ?title;
            OPTIONAL{{ ?publication schema:citation ?ref_pub.
                        ?ref_pub schema:identifier ?ref_doi.}}
            OPTIONAL{{ ?publication schema:issueNumber ?issue;
                                    schema:volumeNumber ?volume.}}
            OPTIONAL{{ ?publication schema:Integer ?chapter_number.}}
            
            OPTIONAL{{
            ?publication rdf:type fabio:Expression ;
                        frbr:embodimentOf ?venue.
            ?venue rdf:type fabio:Manifestation;
                    schema:publisher ?publisherURI;
                    schema:name ?publication_venue;
                    schema:identifier ?issn_isbn.
            ?publisherURI schema:identifier ?publisher;
                        schema:name ?name_pub.}}
            }}
            """
            result_q = get(endpoint,query.format(doi=item),True)
            result = pd.concat([result,result_q])
            result = result.fillna("")
        return result
