import ModelClasses as mdc
import pandas as pd

from relationalData_Manager import RelationalDataProcessor, RelationalQueryProcessor
from graphData_Manager import TriplestoreDataProcessor, TriplestoreQueryProcessor
from GenericQueryP import GenericQueryProcessor

# IMPORT DATA
from relationalData_Manager import df1_r,df2_r,df3_r,df4_r,df5_r,df6_r,df7_r,df8_r,df9_r,df10_r
from graphData_Manager import df1_g,df2_g,df3_g,df4_g,df5_g,df6_g,df7_g,df8_g,df9_g,df10_g

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