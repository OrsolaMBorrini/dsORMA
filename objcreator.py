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

def createPublicationObj(doi):
    for df in dflst_pub:
        if doi in df['id_doi'].values:
            print("the doi is in the this engine")
            id_doi = df['id_doi']
            title = df['title']
            year = df['publication_year']

        else:
            continue
        # data for general publications
        # use the id, publicationYear, title, identifiers, publicationVenue, author, cites
        # use the author constructor
    return True

# works but both the dbs need to be created for the import to work in the appropriate way
#print(df1_g)
#print(df1_r)

#createPublicationObj(doi)

ven1 = createVenueObj("Trip to the Pork",'venue')
print(type(ven1))