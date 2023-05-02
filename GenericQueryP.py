import pandas as pd
import relationalData_Manager as rel
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
    def getPublicationsPublishedInYear(self, year):
        return True # list[Publication]

    def getPublicationsByAuthorId(self, orcid):
        return True # list[Publication]

    def getMostCitedPublication(self):
        return True # Publication
    
    def getMostCitedVenue(self):
        return True # Venue
    
    def getVenuesByPublisherId(self, crossref):
        return True # list[Venue]
    
    def getPublicationInVenue(self, issn_isbn):
        return True # list[Publication]
    
    def getJournalArticlesInIssue(self, issue, volume, journalId):
        return True # list[JournalArticle]

    def getJournalArticlesInVolume(self, volume, journalId):
        return True # list[JournalArticle]
    
    def getJournalArticlesInJournal(self, journalId):
        return True # list[JournalArticle]
    
    def getProceedingsByEvent(self, eventPartialName):
        return True # list[Proceeding]
    
    def getPublicationAuthors(self, doi):
        return True # list[Person]
    
    def getPublicationsByAuthorName(self, authorPartialName):
        return True # list[Publication]
    
    def getDistinctPublishersOfPublications(self, doiList):
        return True # list[Organisation]
