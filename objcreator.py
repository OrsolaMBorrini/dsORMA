from relationalData_Manager import df1_r,df2_r,df3_r,df4_r,df5_r,df6_r,df7_r,df8_r,df9_r,df10_r
from graphData_Manager import df1_g,df2_g,df3_g,df4_g,df5_g,df6_g,df7_g,df8_g,df9_g,df10_g

import numpy as np
import pandas as pd
from ModelClasses import *

#doi = "doi:10.1016/j.websem.2021.100655"
dflst_pub = [df1_g,df2_g,df3_g,df1_r,df2_r,df3_r]
dflst_ven = [df4_g,df5_g,df6_g,df4_r,df5_r,df6_r]

# need to join df4,df5,df6 with df8
concVEN_df = pd.concat([df4_g, df5_g, df6_g,df4_r,df5_r,df6_r])
concVEN_df = concVEN_df.rename(columns={'id_doi':'doi'})

concVEN2_df = pd.concat([df8_g,df8_r])
merged_df = pd.merge(concVEN_df, concVEN2_df, on='doi')
#print(merged_df)
#print(merged_df.columns)

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

def createPublicationObj(doi):
    if doi in pubDICT:
        return pubDICT[doi]
    
    for df in dflst_pub:
        for idx,row in df.iterrows():
            if row['id_doi'] == doi:
                    id_doi = row['id_doi']
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

                    # creating venue objects for the publication
                    # some dois might not have any venue and are thus nan
                    # so we send an empty venue object for that
                    if (row['publication_venue']) == np.nan or np.NAN:
                        venueOBJ = []
                    else:
                        print(row['publication_venue'])
                        venueOBJ = createVenueObj(row['publication_venue'],'venue')     # we need to specift what type of venue object we want

                    # creating citated objects
                    cited = []
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
                    
                    if citedDOIS:
                        self_cit = 0
                        for item in citedDOIS:
                            if item == doi:
                                self_cit = 1
                            else:
                                cited.append(createPublicationObj(item)) 
                        result_pub = Publication(year,title,[id_doi],venueOBJ,auths,cited)
                        if self_cit == 1:
                            cited.append(result_pub)
                            result_pub = Publication(year,title,[id_doi],venueOBJ,auths,cited)

                        pubDICT.update({doi:result_pub})
                        return result_pub
                        
                    
                    
                     
                    # creating the publicatiob object as final result
                    result_pub = Publication(year,title,[id_doi],venueOBJ,auths,cited)
                    pubDICT.update({doi:result_pub})
                    return result_pub
          
        else:
            continue

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
                            issue_no = row['issue']
                            vol_no = row['volume']

            result_JA = JournalArticle(issue_no,vol_no,year,title,id,venue,authrs,cited)
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
                            issue_no = row['issue']
                            vol_no = row['volume']

            result_JA = JournalArticle(issue_no,vol_no,year,title,id,venue,authrs,cited)
            return result_JA

# needs to be removed later
def oldJA(doi):
    if doi in JaDICT:
        return JaDICT[doi]
    
    for df in dflst_pub:
        for idx,row in df.iterrows():
            if row['id_doi'] == doi:
                    id_doi = row['id_doi']
                    title = row['title']
                    year = row['publication_year']

                    if row['type'] == 'journal-article':
                        issue_no = row['issue']
                        vol_no = row['volume']

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

                    # creating venue objects for the publication
                    venueOBJ = createVenueObj(row['publication_venue'],'venue')     # we need to specift what type of venue object we want

                    # creating citated objects
                    cited = []
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
                    
                    if citedDOIS:
                        self_cit = 0
                        for item in citedDOIS:
                            if item == doi:
                                self_cit = 1
                            else:
                                cited.append(createPublicationObj(item))
                        result_ja = JournalArticle(issue_no,vol_no,year,title,[id_doi],venueOBJ,auths,cited)
                        result_pub = Publication(year,title,[id_doi],venueOBJ,auths,cited)
                        if self_cit == 1:
                            cited.append(result_pub)
                            result_ja = JournalArticle(issue_no,vol_no,year,title,[id_doi],venueOBJ,auths,cited)

                        JaDICT.update({doi:result_ja})
                        return result_ja
                        
                    
                    
                     
                    # creating the publicatiob object as final result
                    result_pub = Publication(year,title,[id_doi],venueOBJ,auths,cited)
                    pubDICT.update({doi:result_pub})
                    return result_pub
          
        else:
            continue

# works but both the dbs need to be created for the import to work in the appropriate way
#print(df1_g)
#print(df1_r)

#createPublicationObj(doi)

#ven1 = createVenueObj("Trip to the Pork",'venue')
#print(type(ven1))

#pers1 = createAuthorObj("0000-0002-3938-2064")
#print(type(pers1))
'''
pub1 = createPublicationObj('doi:10.1007/978-3-030-59621-7_2')
print(type(pub1))
print("This is the id of the publication \n",pub1.getIds())
print("This is the publication year of the publication\n",pub1.getPublicationYear())
print("This is the title of the publication",pub1.getTitle())
print("This is the cited publications of the publication", pub1.getCitedPublications())
print("This is the publication venue of the publication",pub1.getPublicationVenue())
print("This is the authors of the publication",pub1.getAuthors())
'''

# doi:10.1016/j.websem.2021.100655
# doi:10.1007/s10115-017-1100-y
# doi:10.1007/s10115-019-01401-x
# pub2 = createPublicationObj('doi:10.1016/j.websem.2021.100655')

'''
pub2 = createJournalArticleObj('doi:10.1016/j.websem.2021.100655')
print(type(pub2))
print("This is the id of the publication \n",pub2.getIds())
print("This is the publication year of the publication\n",pub2.getPublicationYear())
print("This is the title of the publication",pub2.getTitle())
print("This is the cited publications of the publication", pub2.getCitedPublications())
print("This is the publication venue of the publication",pub2.getPublicationVenue())
print("This is the authors of the publication",pub2.getAuthors()) 

print(df9_g)
print(df9_r)
print(dflst_cit)
'''