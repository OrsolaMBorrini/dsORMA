# ==== NEW GENERIC QUERY PROCESSOR (2/7) ====

import pandas as pd
import dataModelClasses as dm
from pubMemory_full import *

class GenericQueryProcessor(object):
    def __init__(self):
        self.queryProcessor = list()   

    def cleanQueryProcessors(self):
        self.queryProcessor = []
        return True

    def addQueryProcessor(self,processor):
        pClass = type(processor)
        if issubclass (pClass,dm.QueryProcessor):
            self.queryProcessor.append(processor)
            df_creator(processor)
            return True
        else:
            return False


    def getPublicationsPublishedInYear(self,year):
        final_DF = pd.DataFrame()
        for item in self.queryProcessor:
            result_DF = item.getPublicationsPublishedInYear(year)
            final_DF = pd.concat([final_DF,result_DF])

        result = list() # This is the list[Publication] to be returned at the end of the query (see: UML)

        # Since in the result_DF there will be duplicates for the dois (under the column "id"), I first need to get rid of these duplicate values and so I create a set and populate it with the dois (the string values under the column "id" in the final_DF)
        ids = set()
        for idx,row in final_DF.iterrows(): # I iterate over the final_DF (which will contain all the information needed to build each Publication object)
            ids.add(row["id"])  # For each row I add the value under the column "id" (which will be a doi string) to the set
            # As the set is an unordered collection of unique elements, I don't have to worry about duplicates: if the doi is already contained in the set it will NOT be added to it again 
        
        # I iterate over the set and, for each item of the set, I call the additional method and create a Publication object (and add this Publication object to the result list)
        for item in ids:
            result.append(creatPubobj(item))
        return result
        #list[Publication]
    

    def getPublicationsByAuthorId(self,id):
        # The id is going to be a orcid
        final_DF = pd.DataFrame()
            
        for item in self.queryProcessor:
            result_DF = item.getPublicationsByAuthorId(id)
            final_DF = pd.concat([final_DF,result_DF])
        
        result = list()

        ids = set()
        for idx,row in final_DF.iterrows():
            ids.add(row["id"])

        for item in ids:
            result.append(creatPubobj(item))
        return result
        #list[Publication]


    def getMostCitedPublication(self):
        # We save all the results to this query (coming from both RelationalQueryProcessors and GraphQueryProcessors) in a final_DF
        final_DF = pd.DataFrame()
        for item in self.queryProcessor:
            result_DF = item.getMostCitedPublication()
            final_DF = pd.concat([final_DF,result_DF])
        
        # We order the final_DF based on the values under the column num_citations
        final_DF.sort_values(by=["ref_doi","num_citations"],ascending=False)

        # Select only the doi with the max number of citations (= the doi of the first row of the final_DF)
        max_doi = final_DF.iloc[0]["ref_doi"]
        return creatPubobj(max_doi)
        #Publication


    def getMostCitedVenue(self):
        final_DF = pd.DataFrame()
        
        for item in self.queryProcessor:
            result_DF = item.getMostCitedVenue()
            final_DF = pd.concat([final_DF,result_DF])
        
        # We order in a descending order the final_DF
        final_DF.sort_values(by=["num_cit"],ascending=False)

        # Now we need to save all the information needed to CREATE an object of class Venue:
            # ["ids"](=issn_isbn), "title"(of the Venue), ["ids"](=crossref), "name"(of the Organisation)

        max_title = final_DF.iloc[0]["name_venue"]
        max_ids_lst = list()
        for idx,row in final_DF.iterrows():
            title = row["name_venue"]
            if title == max_title:
                # We are in the same Venue, that has more than one issn_isbn 
                max_issn_isbn = row["issn_isbn"]
                max_ids_lst.append(max_issn_isbn)
        max_crossref_lst = list()
        for idx1,row1 in final_DF.iterrows():
            title1 = row1["name_venue"]
            if title1 == max_title:
                max_crossref = row1["publisher"]
                max_crossref_lst.append(max_crossref)
        max_name_org = final_DF.iloc[0]["name_pub"]
        pub_obj = dm.Organisation(max_crossref_lst,max_name_org)
        venue_obj = dm.Venue(max_ids_lst,max_title,pub_obj)
        return venue_obj
        #Venue
        

    def getVenuesByPublisherId(self,id):
        # The id is going to be a crossref
        final_DF = pd.DataFrame()
        
        for item in self.queryProcessor:
            result_DF = item.getVenuesByPublisherId(id)
            final_DF = pd.concat([final_DF,result_DF])

        result = list()
        for idx,row in final_DF.iterrows():
            title = row["name_venue"] # -> "title"(of the Venue)
            ids_list = list() # -> ["ids"](=issn_isbn)
            for idx2,row2 in final_DF.iterrows():
                title2 = row2["name_venue"]
                if title2 == title:
                    # We are in the same Venue!
                    issn_isbn = row2["issn_isbn"]
                    ids_list.append(issn_isbn)
            crossref_list = list() # -> ["ids"](=crossref)
            for idx3,row3 in final_DF.iterrows():
                title3 = row3["name_venue"]
                if title3 == title:
                    crossref = row3["publisher"]
                    crossref_list.append(crossref)
            name_org = row["name_pub"]
            publ_obj = dm.Organisation(crossref_list,name_org)
            venue_obj = dm.Venue(ids_list,title,publ_obj)
            result.append(venue_obj)
        return result
        #list[Venue]
        

    def getPublicationInVenue(self,venueId):
        # The id is going to be a issn_isbn
        final_DF = pd.DataFrame()
            
        for item in self.queryProcessor:
            result_DF = item.getPublicationInVenue(venueId)
            final_DF = pd.concat([final_DF,result_DF])
        
        result = list()

        ids = set()
        for idx,row in final_DF.iterrows():
            ids.add(row["id"])
            
        for item in ids:
            result.append(creatPubobj(item))
        return result
        #list[Publication]


    def getJournalArticlesInIssue(self,issue,volume,journalId):
        # The id is going to be a doi
        #list[JournalArticle]
        final_DF = pd.DataFrame()
            
        for item in self.queryProcessor:
            result_DF = item.getJournalArticlesInIssue(issue,volume,journalId)
            final_DF = pd.concat([final_DF,result_DF])
        
        result = list() # This is the list[Publication] to be returned at the end of the query (see: UML)

        # Since in the result_DF there will be duplicates for the dois (under the column "id"), I first need to get rid of these duplicate values and so I create a set and populate it with the dois (the string values under the column "id" in the final_DF)
        ids = set()
        for idx,row in final_DF.iterrows(): # I iterate over the final_DF (which will contain all the information needed to build each Publication object)
            ids.add(row["id"])  # For each row I add the value under the column "id" (which will be a doi string) to the set
            # As the set is an unordered collection of unique elements, I don't have to worry about duplicates: if the doi is already contained in the set it will NOT be added to it again 
        
        # I iterate over the set and, for each item of the set, I call the additional method and create a Publication object (and add this Publication object to the result list)
        for item in ids:
            result.append(creatJAobj(item))
        return result
        #list[Publication]


    def getJournalArticlesInVolume(self,volume,journalId):
        # The id is going to be a doi
        final_DF = pd.DataFrame()
            
        for item in self.queryProcessor:
            result_DF = item.getJournalArticlesInVolume(volume,journalId)
            final_DF = pd.concat([final_DF,result_DF])
        
        result = list() # This is the list[Publication] to be returned at the end of the query (see: UML)
        # Since in the result_DF there will be duplicates for the dois (under the column "id"), I first need to get rid of these duplicate values and so I create a set and populate it with the dois (the string values under the column "id" in the final_DF)
        ids = set()
        for idx,row in final_DF.iterrows(): # I iterate over the final_DF (which will contain all the information needed to build each Publication object)
            ids.add(row["id"])  # For each row I add the value under the column "id" (which will be a doi string) to the set
            # As the set is an unordered collection of unique elements, I don't have to worry about duplicates: if the doi is already contained in the set it will NOT be added to it again 
        
        # I iterate over the set and, for each item of the set, I call the additional method and create a Publication object (and add this Publication object to the result list)
        for item in ids:
            result.append(creatJAobj(item))
        return result
        #list[JournalArticle]


    def getJournalArticlesInJournal(self,journalId):
        # The id is going to be a doi
        final_DF = pd.DataFrame()
            
        for item in self.queryProcessor:
            result_DF = item.getJournalArticlesInJournal(journalId)
            final_DF = pd.concat([final_DF,result_DF])
        
        result = list() # This is the list[Publication] to be returned at the end of the query (see: UML)

        # Since in the result_DF there will be duplicates for the dois (under the column "id"), I first need to get rid of these duplicate values and so I create a set and populate it with the dois (the string values under the column "id" in the final_DF)
        ids = set()
        for idx,row in final_DF.iterrows(): # I iterate over the final_DF (which will contain all the information needed to build each Publication object)
            ids.add(row["id"])  # For each row I add the value under the column "id" (which will be a doi string) to the set
            # As the set is an unordered collection of unique elements, I don't have to worry about duplicates: if the doi is already contained in the set it will NOT be added to it again 
        
        # I iterate over the set and, for each item of the set, I call the additional method and create a Publication object (and add this Publication object to the result list)
        for item in ids:
            result.append(creatJAobj(item))
        return result
        #list[JournalArticle]


    def getProceedingsByEvent(self,eventPartialName):
        final_DF = pd.DataFrame()
        
        for item in self.queryProcessor:
            result_DF = item.getProceedingsByEvent(eventPartialName)
            final_DF = pd.concat([final_DF,result_DF])

        result = list()
        for idx,row in final_DF.iterrows():
            event = row["event"] # -> additional info "event" str
            title = row["name_venue"] # -> "title"(of the Venue)
            ids_list = list() # -> ["ids"](=issn_isbn)
            for idx2,row2 in final_DF.iterrows():
                title2 = row2["name_venue"]
                if title2 == title:
                    # We are in the same Venue!
                    issn_isbn = row2["issn_isbn"]
                    ids_list.append(issn_isbn)
            crossref_list = list() # -> ["ids"](=crossref)
            for idx3,row3 in final_DF.iterrows():
                title3 = row3["name_venue"]
                if title3 == title:
                    crossref = row3["publisher"]
                    crossref_list.append(crossref)
            name_org = row["name_pub"]
            publ_obj = dm.Organisation(crossref_list,name_org)
            proc_obj = dm.Proceedings(ids_list,title,publ_obj,event)
            result.append(proc_obj)
        return result
        #list[Proceeding]
   
        
    def getPublicationAuthors(self,publicationId):
        # The id is going to be a doi
        final_DF = pd.DataFrame()
        
        for item in self.queryProcessor:
            result_DF = item.getPublicationAuthors(publicationId)
            final_DF = pd.concat([final_DF,result_DF])

        result = list()
        for idx,row in final_DF.iterrows():
            #["ids"](=orcid), "givenName", "familyName"
            doi = row["doi_authors"]
            ids_list = list() # -> ["ids"](=orcid)
            for idx2,row2 in final_DF.iterrows():
                doi2 = row2["doi_authors"]
                if doi2 == doi:
                    # We are in the same publication
                    id = row2["orcid"]
                    ids_list.append(id)
            given = row["given"] # -> "givenName"
            family = row["family"] # -> "familyName"
            pers_obj = dm.Person(ids_list,given,family)
            result.append(pers_obj)
        return result
        #list[Person]


    def getPublicationsByAuthorName(self,authorPartialName):
        final_DF = pd.DataFrame()
            
        for item in self.queryProcessor:
            result_DF = item.getPublicationsByAuthorName(authorPartialName)
            final_DF = pd.concat([final_DF,result_DF])
        
        result = list()

        ids = set()
        for idx,row in final_DF.iterrows():
            ids.add(row["id"])
            
        for item in ids:
            result.append(creatPubobj(item))
        return result
        #list[Publication]


    def getDistinctPublishersOfPublications(self,pubIdList):
        # The ids are going to be dois
        final_DF = pd.DataFrame()
        
        for item in self.queryProcessor:
            result_DF = item.getDistinctPublishersOfPublications(pubIdList)
            final_DF = pd.concat([final_DF,result_DF])

        result = list()
        for idx,row in final_DF.iterrows():
            #["ids"](=crossref), "name"(of the Organisation)
            doi = row["id_venue"]
            ids_list = list() # -> ["ids"](=crossref)
            for idx2,row2 in final_DF.iterrows():
                doi2 = row2["id_venue"]
                if doi2 == doi:
                    # We are in the same publication
                    id = row2["crossref"]
                    ids_list.append(id)
            name = row["name_pub"] # -> "name"(of the Organisation)
            pub_obj = dm.Organisation(ids_list,name)
            result.append(pub_obj)
        return result
        #list[Organisation]
