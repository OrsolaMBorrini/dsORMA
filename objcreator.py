from relationalData_Manager import df1_r,df2_r,df3_r,df4_r,df5_r,df6_r,df7_r,df8_r,df9_r,df10_r
from graphData_Manager import df1_g,df2_g,df3_g,df4_g,df5_g,df6_g,df7_g,df8_g,df9_g,df10_g

import numpy as np
import pandas as pd
from ModelClasses import *

import sys

sys.setrecursionlimit(10**6)

#doi = "doi:10.1016/j.websem.2021.100655"
dflst_pub = [df1_g,df2_g,df3_g,df1_r,df2_r,df3_r]
dflst_ven = [df4_g,df5_g,df6_g,df4_r,df5_r,df6_r]

# need to join df4,df5,df6 with df8
concVEN_df = pd.concat([df4_g, df5_g, df6_g,df4_r,df5_r,df6_r])
concVEN_df = concVEN_df.rename(columns={'id_doi':'doi'})

concVEN2_df = pd.concat([df8_g,df8_r])
merged_df = pd.merge(concVEN_df, concVEN2_df, on='doi')


dflst_org = pd.concat([df10_g,df10_r])

dflst_aut = pd.concat([df7_g,df7_r])

dflst_cit = pd.concat([df9_g,df9_r])

# object dictioneries
authDICT = {}
venDICT = {}
orgDICT = {}
pubDICT = {}

JaDICT = {}
BcDICT = {}
PpDICT = {}

doi_data_dict = {}

def createAuthorObj(orcid):
    if  orcid in authDICT:
        return authDICT[orcid]
    for idx,row in dflst_aut.iterrows():
        if row['orcid'] == orcid:
            nome = row['given']
            conome = row['family']
            orcid = row['orcid']

            result_auth = Person(nome,conome,[orcid])
            authDICT.update({orcid:result_auth})
            return result_auth

def createPublisherObj(orgid):
            if orgid in orgDICT:
                return orgDICT[orgid]
    #for df in dflst_org:
        #if orgid in df['crossref'].values:
            for idx,row in dflst_org.iterrows():
                if row['crossref'] == orgid:
                    title = row['publisher']
                    id = row['crossref']

                    result_org = Organization(title,[id])
                    # we should save the created object in a dict for future use so we dont have to create an already created
                    # publisher object
                    orgDICT.update({orgid:result_org})
            return result_org

def createVenueObj(publication_venue,reqType):
    if publication_venue in venDICT and reqType == 'venue':
        return venDICT[publication_venue]

    grpOB = merged_df.groupby(['publication_venue'])
    req_Ven = grpOB.get_group(publication_venue)
    # there might be many issn_isbn for one single venue
    #print(req_Ven.head())
    ids = set()
    title = ""
    type = ""
    event = ""
    for idx,row in req_Ven.iterrows():
        ids.add(row['issn_isbn'])
        title = row['publication_venue']
        type = row['venue_type']
        orgid = row['id_crossref']
        if isinstance(row['event'],str):
            event = row['event']

    # make the publisher obj for this venue
    pub_org = createPublisherObj(orgid)
    if reqType == 'venue':
         result_ven = Venue(title,ids,pub_org)
         venDICT.update({publication_venue:result_ven})
         return result_ven
    
    if type == "journal":
        result_ven = Journal(title, ids, pub_org)
    elif type == "book":
        result_ven = Book(title, ids, pub_org)
    elif type == "proceedings":
        result_ven = Proceedings(event,title, ids, pub_org)

    return result_ven


# call this function to iterate over the doi_data dict
def dataIter(ogkey):
    self_cit = False

    def pubcreator(doi):
                # check if the DOI has already been created
                if doi in pubDICT:
                    return pubDICT[doi]
                
                # if not created before - create now.
                result = dataIter(doi)

                # append the pub object in the long term dict for later use
                pubDICT.update({doi:result})

                return result 
    
    for k in doi_data_dict:
        if k == ogkey:
            id_doi = k
            year = doi_data_dict[k][2]
            title = doi_data_dict[k][3]
            
            auths = doi_data_dict[k][4]     #already a list of AUTHOR objects
            
            venueOBJ = doi_data_dict[k][5]  #already a VENUE object OR NONE

            #print("This is the CITED DOIs of the OG doi - ",doi_data_dict[k][6])
            citedOBJ = []
            if not doi_data_dict[k][6]:
                pubDICT.update({ogkey:Publication(year,title,[id_doi],venueOBJ,auths,citedOBJ)})
                #title, ids, authors, cites,pubVenue pubYear,
                return Publication(title,[id_doi],auths,citedOBJ,venueOBJ,year)
            else:
                for item in doi_data_dict[k][6]:
                    if item == ogkey:
                        self_cit = True
                    else:
                        citedOBJ.append(pubcreator(item))
                tempPub = Publication(title,[id_doi],auths,citedOBJ,venueOBJ,year)
                citedOBJ.append(tempPub)
                pubDICT.update({ogkey:Publication(title,[id_doi],auths,citedOBJ,venueOBJ,year)})
                return Publication(title,[id_doi],auths,citedOBJ,venueOBJ,year)


# call this function to update the data of required DOIs to the dict
def update_nested_dictionary(keys, values,ogKey):
    nestedDict = {}
    for key, value in zip(keys, values):
        #print(key)
        #print(value)
        nestedDict.update({key:value})
    
    if ogKey not in doi_data_dict:
        doi_data_dict[ogKey] = nestedDict

    for item in nestedDict[6]:
        if item not in doi_data_dict:
            createPublicationObj(item)

    # iterate over the populated doi_data_dict to create objects
    returnedOBJECT = dataIter(ogKey)
    return returnedOBJECT

def createPublicationObj(doi):   
    for df in dflst_pub:
        for idx,row in df.iterrows():
            if row['id_doi'] == doi:
                id = row['id_doi']
                title = row['title']
                year = row['publication_year']

                # creating auth objects for the publication
                auths = []
                autgrp = dflst_aut.groupby(['doi'])
                slctauths = autgrp.get_group(doi)

                avoidauthRepetition = []
                for idx,rowA in slctauths.iterrows():
                    if rowA['orcid'] not in avoidauthRepetition:
                        avoidauthRepetition.append(rowA['orcid'])
                        auther = createAuthorObj(rowA['orcid'])
                        auths.append(auther)
                
                # creating venue object for the publication
                if (row['publication_venue']) == np.nan or np.NAN:
                        venueOBJ = None
                else:
                    venueOBJ = createVenueObj(row['publication_venue'],'venue')     # we need to specift what type of venue object we want

                # creating citated DOI list
                citedDOIS = []
                
                for idx,row in dflst_cit.iterrows():
                    if row['doi'] == doi:
                        citedDF = dflst_cit.groupby(['doi'])
                        slctDOI = citedDF.get_group(doi)
                        for idx,row in slctDOI.iterrows():
                            if row['cited_doi'] not in citedDOIS:
                                citedDOIS.append(row['cited_doi'])
                        else:
                            continue

                

                # send the data with keys to the dict updater
                keys = [1,2,3,4,5,6] 
                values = [id,title,year,auths,venueOBJ,citedDOIS]

                return update_nested_dictionary(keys,values,doi)

def createJournalArticleObj(doi):
    if doi in JaDICT:
        return JaDICT[doi]
    else:
        # check if a pub object of the same doi exists
        if doi in pubDICT:
            x = pubDICT[doi]
            id = x.getIds()
            year = x.getPublicationYear()
            title = x.getTitle()
            authrs = x.getAuthors()
            cited = x.getCitedPublications()
            venue = x.getPublicationVenue()

            for df in dflst_pub:
                for idx,row in df.iterrows():
                    if row['id_doi'] == doi:
                        if row['type'] == 'journal-article':
                            if (row['issue']) != np.nan or np.NAN:
                                issue_no = row['issue']
                            else:
                                issue_no = None
                            if (row['volume']) != np.nan or np.NAN:
                                vol_no = row['volume']
                            else:
                                vol_no = None
            
            result_JA = JournalArticle(title,id,authrs,cited,venue,year,issue_no,vol_no)
            return result_JA
        
        else:
            y = createPublicationObj(doi)
            x = pubDICT[doi]
            id = x.getIds()
            year = x.getPublicationYear()
            title = x.getTitle()
            authrs = x.getAuthors()
            cited = x.getCitedPublications()
            venue = x.getPublicationVenue()

            for df in dflst_pub:
                for idx,row in df.iterrows():
                    if row['id_doi'] == doi:
                        if row['type'] == 'journal-article':
                            if (row['issue']) != np.nan or np.NAN:
                                issue_no = row['issue']
                            else:
                                issue_no = None
                            if (row['volume']) != np.nan or np.NAN:
                                vol_no = row['volume']
                            else:
                                vol_no = None

            result_JA = JournalArticle(title,id,authrs,cited,venue,year,issue_no,vol_no)
            return result_JA
