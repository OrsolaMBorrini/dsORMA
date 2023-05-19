import pandas as pd
import numpy as np
import relationalData_Manager as rel
import graphData_Manager as grp
import ModelClasses as mdc
from objcreator import createPublicationObj, createVenueObj, createAuthorObj, createPublisherObj, createJournalArticleObj

class GenericQueryProcessor(object):
    # -- Constructor
    def __init__(self):
        self.queryProcessor = list()

    # -- Methods
    def cleanQueryProcessors(self):
        self.queryProcessor = []
        return True

    def addQueryProcessor(self, processor):
        if isinstance(processor,mdc.QueryProcessor):
            self.queryProcessor.append(processor)
            return True
        else:
            return "The processor added is not an instance of the class QueryProcessor", False

    # -- Queries
    # gq1 + merged DF is OK
    def getPublicationsPublishedInYear(self, year):
        if isinstance(year,int):
            complete_result = pd.DataFrame()
            for item in self.queryProcessor:
                # For every query processor added to the generic query processor, call the query
                partial_result = item.getPublicationsPublishedInYear(year)
                # Concatenate the result of this query (type DataFrame) to the empty df outside of the for-in cycle to *save* the result
                complete_result = pd.concat([complete_result,partial_result])
            
            # complete_result is now populated by all the results of the query for every query processor
            result = list()   # list[Publication]
            # Drop all duplicate values
            ids = set() # unordered collection of unique elements, no worries about duplicates
            # Scroll the complete_result dataframe
            for idx,row in complete_result.iterrows():
                ids.add(row["doi"])
            # Iterate over the cleaned set of DOIs and create a Publication object for each
            for item in ids:
                # Append the Publication object to the result list
                result.append(createPublicationObj(item))
            
            return result #list[Publication]

        else:
            raise Exception("The input parameter is not an integer!")

    # gq2 + merged DF is OK
    def getPublicationsByAuthorId(self, orcid):
        if isinstance(orcid,str):
            complete_result = pd.DataFrame()
            for item in self.queryProcessor:
                partial_result = item.getPublicationsByAuthorId(orcid)
                complete_result = pd.concat([complete_result,partial_result]) 
            
            result = list()   # list[Publication]
            # Drop duplicate ids
            ids = set()
            for idx,row in complete_result.iterrows():
                ids.add(row["doi"])
            for item in ids:
                result.append(createPublicationObj(item))
            
            return result #list[Publication]
        else:
            raise Exception("The input parameter is not a string!")

    # gq3 + merged DF checked in Sarzanello
    def getMostCitedPublication(self):
        complete_result = pd.DataFrame()
        for item in self.queryProcessor:
            partial_result = item.getPubCitationCount()
            complete_result = pd.concat([complete_result,partial_result])
        
        complete_result = complete_result.dropna()
        # Get set of unique cited_doi (column 'cited_doi')
        unique_citedDoi = set()
        for idx,row in complete_result.iterrows():
            unique_citedDoi.add(row['cited_doi'])
        
        # Group by column 'cited_doi'
        grouped_citdoi = complete_result.groupby(["cited_doi"])

        x = 0
        mostcitedPub = ""

        for doi in unique_citedDoi:
            df = grouped_citdoi.get_group(doi)
            df = df.drop_duplicates()
            # Count number of rows for each df (= number of citations for each doi)
            y = len(df.index)
            if y > x:
                x = y
                mostcitedPub = doi
        
        return createPublicationObj(mostcitedPub)
    
    # gq4 + merged DF checked in Sarzanello
    def getMostCitedVenue(self):
        complete_result = pd.DataFrame()
        for item in self.queryProcessor:
            partial_result = item.getVenCitationCount()
            complete_result = pd.concat([complete_result,partial_result])
        
        # Get set of unique venue_name (column 'venue_name')
        unique_venueName = set()
        for idx,row in complete_result.iterrows():
            unique_venueName.add(row['venue_name'])
        
        # Group by 'venue_name'
        grouped_venueName = complete_result.groupby(["venue_name"])

        x = 0
        mostcitedVen = ""

        for name in unique_venueName:
            df = grouped_venueName.get_group(name)
            df = df.drop_duplicates()
            # Count number of rows for each df ( = number of citations for each venue_name)
            y = len(df.index)
            if y > x:
                x = y
                mostcitedVen = name
        
        return createVenueObj(mostcitedVen,"venue") # Venue
        
    # gq5 + merged DF is OK
    def getVenuesByPublisherId(self, crossref):
        if isinstance(crossref,str):
            complete_result = pd.DataFrame()
            for item in self.queryProcessor:
                partial_result = item.getVenuesByPublisherId(crossref)
                complete_result = pd.concat([complete_result,partial_result])
            
            # complete_result is now populated by all the results of the query for every query processor
            result = list()   # list[Publication]
            # Drop all duplicate values
            ven_name = set() # unordered collection of unique elements, no worries about duplicates
            # Scroll the complete_result dataframe
            for idx,row in complete_result.iterrows():
                ven_name.add(row["publication_venue"])
            # Iterate over the cleaned set of publication venue names and create a Venue object for each
            for item in ven_name:
                # Append the Publication object to the result list
                result.append(createVenueObj(item,"venue"))
            
            return result #list[Venue]

        else:
            raise Exception("The input parameter is not a string!")

    # gq6 + merged DF is OK
    def getPublicationInVenue(self, issn_isbn):
        if isinstance(issn_isbn,str):
            complete_result = pd.DataFrame()
            for item in self.queryProcessor:
                partial_result = item.getPublicationInVenue(issn_isbn)
                complete_result = pd.concat([complete_result,partial_result]) 
            
            result = list()   # list[Publication]
            # Drop duplicate ids
            ids = set()
            for idx,row in complete_result.iterrows():
                ids.add(row["doi"])
            for item in ids:
                result.append(createPublicationObj(item))
            
            return result #list[Publication]
        else:
            raise Exception("The input parameter is not a string!")
        
    # gq7 + merged DF is OK
    def getJournalArticlesInIssue(self, issue, volume, journalId):
        if isinstance(issue,str) and isinstance(volume,str) and isinstance(journalId,str):
            complete_result = pd.DataFrame()
            for item in self.queryProcessor:
                partial_result = item.getJournalArticlesInIssue(issue,volume,journalId)
                complete_result = pd.concat([complete_result,partial_result])
            
            result = list()
            ids = set()
            for idx,row in complete_result.iterrows():
                ids.add(row["doi"])

            for item in ids:
                result.append(createJournalArticleObj(item))
            
            return result # list[JournalArticle]
        else:
            raise Exception("All or some of the input parameters are not strings!")
    
    # gq8 + merged DF is OK
    def getJournalArticlesInVolume(self, volume, journalId):
        if isinstance(volume,str) and isinstance(journalId,str):
            complete_result = pd.DataFrame()
            for item in self.queryProcessor:
                partial_result = item.getJournalArticlesInVolume(volume,journalId)
                complete_result = pd.concat([complete_result,partial_result])
            
            result = list()
            ids = set()
            for idx,row in complete_result.iterrows():
                ids.add(row["doi"])

            for item in ids:
                result.append(createJournalArticleObj(item))
            
            return result # list[JournalArticle]
        else:
            raise Exception("All or some of the input parameters are not strings!")
        
    # gq9 + merged DF is OK
    def getJournalArticlesInJournal(self, journalId):
        if isinstance(journalId,str):
            complete_result = pd.DataFrame()
            for item in self.queryProcessor:
                partial_result = item.getJournalArticlesInJournal(journalId)
                complete_result = pd.concat([complete_result,partial_result])
            
            result = list()
            ids = set()
            for idx,row in complete_result.iterrows():
                ids.add(row["doi"])

            for item in ids:
                result.append(createJournalArticleObj(item))
            
            return result # list[JournalArticle]
        else:
            raise Exception("The input parameter is not a string!")
        
    # gq10 + merged DF is OK
    def getProceedingsByEvent(self, eventPartialName):
        if isinstance(eventPartialName,str):
            complete_result = pd.DataFrame()
            for item in self.queryProcessor:
                partial_result = item.getProceedingsByEvent(eventPartialName)
                complete_result = pd.concat([complete_result,partial_result])

            result = list()
            
            ven_name = set()
            for idx,row in complete_result.iterrows():
                ven_name.add(row["publication_venue"])
            for item in ven_name:
                result.append(createVenueObj(item,"proceedings"))

            return result # list[Proceeding]
        
        else:
            raise Exception("The input parameter is not a string!")
                
    # gq11 + merged DF is OK
    def getPublicationAuthors(self, doi):
        if isinstance(doi,str):
            complete_result = pd.DataFrame()
            for item in self.queryProcessor:
                partial_result = item.getPublicationAuthors(doi)
                complete_result = pd.concat([complete_result,partial_result])

            result = list()
            orcid = set()
            for idx,row in complete_result.iterrows():
                orcid.add(row["orcid"])
            for item in orcid:
                result.append(createAuthorObj(item))
            
            return result # llist[Person]

        else:
            raise Exception("The input parameter is not a string!")
        
    # gq12 + merged DF is OK
    def getPublicationsByAuthorName(self, authorPartialName):
        if isinstance(authorPartialName,str):
            complete_result = pd.DataFrame()
            for item in self.queryProcessor:
                partial_result = item.getPublicationsByAuthorName(authorPartialName)
                complete_result = pd.concat([complete_result,partial_result])

            result = list()
            ids = set()
            for idx,row in complete_result.iterrows():
                ids.add(row["doi"])
            for item in ids:
                result.append(createPublicationObj(item))

            return result # list[Publication]
        
        else:
            raise Exception("The input parameter is not a string!")
        
    # gq13 + merged DF is OK
    def getDistinctPublishersOfPublications(self, doiList):
        if all(isinstance(n, str) for n in doiList):
            complete_result = pd.DataFrame()
            for item in self.queryProcessor:
                partial_result = item.getDistinctPublishersOfPublications(doiList)
                complete_result = pd.concat([complete_result,partial_result])
            
            result = list()
            crossrefs = set()
            for idx,row in complete_result.iterrows():
                crossrefs.add(row["crossref"])
            for item in crossrefs:
                result.append(createPublisherObj(item))

            return result # list[Organisation]
        else:
            raise Exception("The input parameter is not a list or it is not a list of strings!")


