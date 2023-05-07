from relationalData_Manager import df1_r,df2_r,df3_r,df4_r,df5_r,df6_r,df7_r,df8_r,df9_r,df10_r
from graphData_Manager import df1_g,df2_g,df3_g,df4_g,df5_g,df6_g,df7_g,df8_g,df9_g,df10_g

import pandas as pd
from ModelClasses import *

doi = "doi:10.1016/j.websem.2021.100655"
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

def createAuthorObj(orcid):
    for idx,row in dflst_aut.iterrows():
        if row['orcid'] == orcid:
            nome = row['given']
            conome = row['family']
            orcid = row['orcid']

            result_auth = Person(nome,conome,[orcid])

            return result_auth

def createPublisherObj(orgid):
    #for df in dflst_org:
        #if orgid in df['crossref'].values:
            for idx,row in dflst_org.iterrows():
                if row['crossref'] == orgid:
                    title = row['publisher']
                    id = row['crossref']

                    result_org = Organization(title,[id])
                    # we should save the created object in a dict for future use so we dont have to create an already created
                    # publisher object
            return result_org

def createVenueObj(publication_venue,reqType):
    grpOB = merged_df.groupby(['publication_venue'])
    req_Ven = grpOB.get_group(publication_venue)
    # there might be many issn_isbn for one single venue
    print(req_Ven.head())
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
         return result_ven
    
    if type == "journal":
        result_ven = Journal(title, ids, pub_org)
    elif type == "book":
        result_ven = Book(title, ids, pub_org)
    elif type == "proceedings":
        result_ven = Proceedings(event,title, ids, pub_org)

    return result_ven

# WARNING - this function gave a none type error or something once, maybe needs more testing.
def createPublicationObj(doi):
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
                    venueOBJ = createVenueObj(row['publication_venue'],'venue')     # we need to specift what type of venue object we want

                    # creating citated objects
                    cited = []
                    citedDOIS = []
                    
                    for idx,row in dflst_cit.iterrows():
                         if row['doi'] == doi:
                            print("key is there")
                            citedDF = dflst_cit.groupby(['doi'])
                            slctDOI = citedDF.get_group(doi)
                            for idx,row in slctDOI.iterrows():
                                if row['cited_doi'] not in citedDOIS:
                                    citedDOIS.append(row['cited_doi'])
                         else:
                             continue
                    
                    if citedDOIS:
                        for item in citedDOIS:
                            cited.append(createPublicationObj(item)) 
                        result_pub = Publication(year,title,[id_doi],venueOBJ,auths,cited)
                        return result_pub
                        
                    
                    
                     
                    # creating the publicatiob object as final result
                    result_pub = Publication(year,title,[id_doi],venueOBJ,auths,cited)
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

#doi:10.1016/j.websem.2021.10065
pub2 = createPublicationObj('doi:10.1007/s10115-017-1100-y')
print(type(pub2))
print("This is the id of the publication \n",pub2.getIds())
print("This is the publication year of the publication\n",pub2.getPublicationYear())
print("This is the title of the publication",pub2.getTitle())
print("This is the cited publications of the publication", pub2.getCitedPublications())
print("This is the publication venue of the publication",pub2.getPublicationVenue())
print("This is the authors of the publication",pub2.getAuthors())
'''
print(df9_g)
print(df9_r)
print(dflst_cit)
'''