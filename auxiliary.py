import pandas as pd
import json as js
from ModelClasses import *
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from sparql_dataframe import get


def readJSON(path):
    # First, create empty dataframes one for each of the main json elements (authors, venues_id, references, publishers)
    authors_DF = pd.DataFrame(columns=["doi", "family", "given", "orcid"])
    venuesID_DF = pd.DataFrame(columns=["doi", "issn_isbn"])
    references_DF = pd.DataFrame(columns=["doi", "cited_doi"])
    publishers_DF = pd.DataFrame(columns=["crossref", "publisher"])

    # Read JSON file as python dictionary
    with open(path, "r", encoding="utf-8") as f:
        json_dict = js.load(f)

    # ---------------- AUTHORS DATAFRAME ----------------
    # Save the "authors" key of the JSON dict
    auth_key = list(json_dict.keys())[0]
    # Save the "authors" dictionary associated to the key
    auth_dict = json_dict[auth_key]

    # We scroll through each key/DOI of the "authors" dictionary
    for key in auth_dict:
        # Scroll through the authors of each DOI's group of authors
        for author in auth_dict[key]:
            # Add DOI, family, given and value to the new DF
            authors_DF.loc[len(authors_DF.index)] = [
                key, author['family'], author['given'], author['orcid']]
    # print(authors_DF.columns)

    # ---------------- VENUES ID DATAFRAME ----------------
    venues_key = list(json_dict.keys())[1]
    venues_dict = json_dict[venues_key]

    for key in venues_dict:
        for venue_id in venues_dict[key]:
            venuesID_DF.loc[len(venuesID_DF.index)] = [key, venue_id]
    # print(venuesID_DF.columns)

    # ---------------- REFERENCES DATAFRAME ----------------
    ref_key = list(json_dict.keys())[2]
    ref_dict = json_dict[ref_key]

    for key in ref_dict:
        for reference in ref_dict[key]:
            references_DF.loc[len(references_DF.index)] = [key, reference]
    # print(references_DF.columns)

    # ---------------- PUBLISHERS DATAFRAME ----------------
    pub_key = list(json_dict.keys())[3]
    pub_dict = json_dict[pub_key]

    for key in pub_dict:
        publishers_DF.loc[len(publishers_DF.index)] = [
            key, pub_dict[key]['name']]
    # print(publishers_DF.columns)

    return authors_DF, venuesID_DF, references_DF, publishers_DF

# !!!!! -------------------- CHANGE "id" columns to "doi" columns
def readCSV(path):
    D0 = pd.read_csv(path, header=0)

    # store JA_df
    filtered_df = D0.query("type == 'journal-article'")
    JA_df = filtered_df.drop(columns=['chapter', 'venue_type', 'publisher', 'event'])
    JA_df = JA_df.rename(columns={'id':'id_doi'})

    # store BC_df
    filtered_df = D0.query("type == 'book-chapter'")
    BC_df = filtered_df.drop(columns=['issue', 'volume', 'venue_type', 'publisher', 'event'])
    BC_df = BC_df.rename(columns={'id':'id_doi'})
    # print(BC_df.head(3))
    

    # store PP_df
    filtered_df = D0.query("type == 'proceedings-paper'")
    PP_df = filtered_df.drop(columns=['issue', 'volume', 'chapter', 'venue_type', 'publisher', 'event'])
    PP_df = PP_df.rename(columns={'id':'id_doi'})
    # print(PP_df.head(3))
    

    # store VeB_DF
    filtered_df = D0.query("venue_type == 'book'")
    VeB_df = filtered_df.drop(columns=['title', 'type', 'publication_year', 'issue', 'volume', 'chapter', 'event'])
    VeB_df = VeB_df.rename(columns={'id':'id_doi','publisher':'id_crossref'})
    

    # store VeJ_DF
    filtered_df = D0.query("venue_type == 'journal'")
    VeJ_df = filtered_df.drop(columns=['title', 'type', 'publication_year', 'issue', 'volume', 'chapter', 'event'])
    VeJ_df = VeJ_df.rename(columns={'id':'id_doi','publisher':'id_crossref'})
   

    # store VePE_DF
    filtered_df = D0.query("venue_type == 'proceedings'")
    VePE_df = filtered_df.drop(columns=['title', 'type', 'publication_year', 'issue', 'volume', 'chapter'])
    VePE_df = VePE_df.rename(columns={'id':'id_doi','publisher':'id_crossref'})
    

    return JA_df, BC_df, PP_df, VeB_df, VeJ_df, VePE_df


def dbupdater(graphvariable, endpointURI):
    store = SPARQLUpdateStore()
    # The URL of the SPARQL endpoint is the same URL of the Blazegraph
    # instance + '/sparql'
    # It opens the connection with the SPARQL endpoint instance
    #endpointURI = "http://127.0.0.1:9999/blazegraph/sparql"
    #endpointURI = endpointURI + '/sparql'
    store.open((endpointURI, endpointURI))

    for triple in graphvariable.triples((None, None, None)):
        store.add(triple)

    # close connection
    store.close()


def check_repetedDOI():
    return True

def createJournalArticleObj(doi,issue,volume):
    return True







# testing area

#df1,df2,df3,df4,df5,df6 = readCSV("testData/relational_publications.csv")
'''
print(df1.head(2))
print(df1.columns)
print('/n')
print(df2.head(2))
print(df2.columns)
print('/n')
print(df3.head(2))
print(df3.columns)
print('/n')
print(df4.head(2))
print(df4.columns)
print('/n')
print(df5.head(2))
print(df5.columns)
print('/n')
print(df6.head(2))
print(df6.columns)
'''

#df7,df8,df9,df10 = readJSON("testData/graph_other_data.json")
'''
print(df10.columns)
print(df10.head)
'''
'''
print(df7.columns)
print(df7.head)
'''


