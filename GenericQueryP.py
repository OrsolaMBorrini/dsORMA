import pandas as pd
import relationalData_Manager as rel
import graphData_Manager as grp
import ModelClasses as mdc

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
    # g11
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

    # gq2
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

    # gq3 ---- TO DO 
    def getMostCitedPublication(self):
        return True # Publication
    
    # gq4 ---- TO DO 
    def getMostCitedVenue(self):
        return True # Venue
        
    # gq5 ---- TO DO 
    def getVenuesByPublisherId(self, crossref):
        if isinstance(crossref,str):
            complete_result = pd.DataFrame()
            for item in self.queryProcessor:
                partial_result = item.getVenuesByPublisherId(crossref)
                complete_result = pd.concat([complete_result,partial_result])
            
            # Should we group-by the title of the venue? and then pass "each" group-by to the createVenueObj() function?

            # Calling the appropriate function to create the list of Venue objects to return as result of this query
            result = createVenueObj()    # list[Venue]
            
            return result
        
        else:
            raise Exception("The input parameter is not a string!")

    # gq6
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
        
    # gq7 ---- TO DO 
    def getJournalArticlesInIssue(self, issue, volume, journalId):
        return True # list[JournalArticle]
    
    # gq8 ---- TO DO 
    def getJournalArticlesInVolume(self, volume, journalId):
        return True # list[JournalArticle]
        
    # gq9 ---- TO DO 
    def getJournalArticlesInJournal(self, journalId):
        return True # list[JournalArticle]
        
    # gq10 ---- TO DO 
    def getProceedingsByEvent(self, eventPartialName):
        return True # list[Proceeding]
        
    # gq11 ---- TO DO 
    def getPublicationAuthors(self, doi):
        return True # list[Person]
        
    # gq12 ---- TO DO 
    def getPublicationsByAuthorName(self, authorPartialName):
        return True # list[Publication]
        
    # gq13 ---- TO DO 
    def getDistinctPublishersOfPublications(self, doiList):
        return True # list[Organisation]
