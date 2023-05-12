import ModelClasses as mdc
import pandas as pd
import numpy as np
import sys
import math

from relationalData_Manager import RelationalDataProcessor, RelationalQueryProcessor
from graphData_Manager import TriplestoreDataProcessor, TriplestoreQueryProcessor
from GenericQueryP import GenericQueryProcessor

# IMPORT DATA
from relationalData_Manager import df1_r,df2_r,df3_r,df4_r,df5_r,df6_r,df7_r,df8_r,df9_r,df10_r
from graphData_Manager import df1_g,df2_g,df3_g,df4_g,df5_g,df6_g,df7_g,df8_g,df9_g,df10_g

from objcreator import *
# Merge the corresponding dataframes from the two engines (= df1_r is concatenated to df1_g and so on)
    # ! They don't have the same columns so this could be an issue for the concat
    # As a result, we will get 10 different dataframes:
        # - JournalArticleDF
        # - BookChapterDF
        # - ProceedingsPaperDF
        # - BookDF
        # - JournalDF
        # - ProceedingsDF
        # - AuthorsDF
        # - VenuesIssnIsbnDF
        # - CitationsDF
        # - PublishersDF
        
# For each query, we iterate over the appropriate dataframe (that is, the dataframe that contains the value that we are supposed to pass as input -> e.g., getJournalArticlesInVolume query, we need to iterate the JournalArticleDF to save the values for the inputs 'volume' and 'journalId' that are under the columns 'volume' and 'issn_isbn')
    # ! But the 'issn_isbn' column is not part of the JournalArticleDF, but of the VenuesIssnIsbnDF ! So how do we solve this problem? For each query/type of query we would need to first create a bigger DF that contains all the needed input data so as to save the needed data and send it as input to the query
# We pass as input of the query the necessary input data

sys.setrecursionlimit(10**6)

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

faulty_DOI = []
faulty_ven = []
# creating pub objects for all DOIs
pubData = {}

# creating pub objects for all DOIs
orgData = {}

# storing ID_DOI -/- TITLE -/- PUBLICATION_VENUE -/- in a dict
for item in dflst_pub:
    for idx,row in item.iterrows():
        doi = row['id_doi']
        title = row['title']
        pub_venue = row['publication_venue']
        pubData.update({doi:{2:title,3:pub_venue}})

# printing if there are any DOI which are nan        
for k in pubData:
    if pd.isna(k):
        print("we have nan doi !!!! FUCKKKK")
# printing which ID_DOI has nan value for PUBLICATION_VENUE
for k in pubData:
    #print(pubData[k][3])
    #print(type(pubData[k][3]))
    if pd.isna(pubData[k][3]):

        print(k)
        print(" ")
# printing which ID_DOI has nan value for TITLE
for k in pubData:
    #print(pubData[k][3])
    #print(type(pubData[k][3]))
    if pd.isna(pubData[k][2]):

        print(k)
        print(" ")


# creating ven objects for all issn_isbn
for idx,row in merged_df.iterrows():
    x = row['publication_venue']
    if x not in venDICT:
        y = createVenueObj(x,'venue')
        title = y.getTitle()
        if title == None:   # also tested with np.nan and np.NAN
            faulty_ven.append(title)
        venDICT.update({x:y})

print(faulty_ven)

# UNDERSTANDING -> here we try to create a pub object that has NO venue data. What will happen?
# SURPRISE SURPRISE -> we create the pub_object by passing an empty list as a venue and thus it will return an empty list as the venue
# ERROR TYPE -> Attribute Error : 'list' object has no attribute 'getIds' OR 'list' object has no attribute 'getTitle'
'''
zeta = createPublicationObj("doi:10.4018/978-1-6684-3702-5.ch037")
zeta_v = zeta.getPublicationVenue()
print(zeta_v)
print(zeta_v.getIds())
print(zeta_v.getTitle())
'''

# storing all the org_data in a dict
for idx,row in dflst_org.iterrows():
    crossref = row['crossref']
    org_title = row['publisher']
    orgDICT.update({crossref:org_title})

for k in orgDICT:
    if pd.isna(orgDICT[k]):
        print("Surprise")

