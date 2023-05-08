import pandas as pd
import sqlite3 as sql3
import json

from ModelClasses import QueryProcessor
from auxiliary import readCSV, readJSON


# Global variables
df1_r = pd.DataFrame()
df2_r = pd.DataFrame()
df3_r = pd.DataFrame()
df4_r = pd.DataFrame()
df5_r = pd.DataFrame()
df6_r = pd.DataFrame()
df7_r = pd.DataFrame()
df8_r = pd.DataFrame()
df9_r = pd.DataFrame()
df10_r = pd.DataFrame()


class RelationalProcessor(object):
    # -- Constructor
    def __init__(self):
        # 'dbPath' is name we use for the database path
        self.dbPath = ""

    # -- Methods
    def getDbPath(self):
        if self.dbPath == "":
            return "DbPath is currently unset" + self.dbPath
        else:
            return self.dbPath

    def setDbPath(self, new_dbPath):
        if isinstance(new_dbPath, str):
            self.dbPath = new_dbPath
            return True
        else:
            return False

# ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––


class RelationalDataProcessor(RelationalProcessor):
    # -- Constructor
    def __init__(self):
        super().__init__()

    # -- Methods
    def uploadData(self, filepath):
        # Step-1 : read the data into pandas
        global df1_r, df2_r, df3_r, df4_r, df5_r, df6_r, df7_r, df8_r, df9_r, df10_r

        # ---------- CSV
        if filepath.endswith(".csv"):
            # df1 -> journal article         // columns = 'id_doi', 'title', 'type', 'publication_year', 'issue', 'volume'
            # df2 -> book-chapter            // columns = 'id_doi', 'title', 'type', 'publication_year', 'chapter'
            # df3 -> proceedings-paper       // columns = 'id_doi', 'title', 'type', 'publication_year'
            # df4 -> Venue_book              // columns = 'id_doi', 'publication_venue', 'venue_type', 'id_crossref'
            # df5 -> Venue_journal           // columns = 'id_doi', 'publication_venue', 'venue_type', 'id_crossref'
            # df6 -> Venue_proceedings-event // columns = 'id_doi', 'publication_venue', 'venue_type', 'id_crossref', 'event'
            df1_r, df2_r, df3_r, df4_r, df5_r, df6_r = readCSV(filepath)

            # ----- DATABASE CONNECTION
            with sql3.connect(self.dbPath) as rdb:
                df1_r.to_sql('JournalArticleTable', rdb, if_exists='replace', index=False)
                df2_r.to_sql('BookChapterTable', rdb, if_exists='replace', index=False)
                df3_r.to_sql('ProceedingsPaperTable', rdb, if_exists='replace', index=False)
                df4_r.to_sql('BookTable', rdb, if_exists='replace', index=False)
                df5_r.to_sql('JournalTable', rdb, if_exists='replace', index=False)
                df6_r.to_sql('ProceedingsTable', rdb, if_exists='replace', index=False)

                rdb.commit()

        # ---------- JSON
        elif filepath.endswith(".json"):
            # df7  -> authors                // columns = 'doi', 'family', 'given', 'orcid'
            # df8  -> VenueIDs              // columns = 'doi', 'issn_isbn'
            # df9  -> citations             // columns = 'doi', 'cited_doi'
            # df10 -> publishers            // columns = 'crossref', 'publisher'
            df7_r, df8_r, df9_r, df10_r = readJSON(filepath)

            # ----- DATABASE CONNECTION
            with sql3.connect(self.dbPath) as rdb:
                df7_r.to_sql('AuthorsTable', rdb, if_exists='replace', index=False)
                df8_r.to_sql('VenuesIDTable', rdb, if_exists='replace', index=False)
                df9_r.to_sql('CitationsTable', rdb, if_exists='replace', index=False)
                df10_r.to_sql('PublishersTable', rdb, if_exists='replace', index=False)

                rdb.commit()

        return True

# ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––


class RelationalQueryProcessor(QueryProcessor, RelationalProcessor):
    # -- Constructor
    def __init__(self):
        super().__init__()

    # -- Queries

    # q1
    def getPublicationsPublishedInYear(self, year):
        if isinstance(year,int):
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                query = "SELECT id_doi, type FROM JournalArticleTable WHERE publication_year='{year}' UNION SELECT id_doi, type FROM BookChapterTable WHERE publication_year='{year}' UNION SELECT id_doi, type FROM ProceedingsPaperTable WHERE publication_year='{year}'".format(year=year)
                cur.execute(query)
                result = cur.fetchall()
                qrdb.commit()
            return pd.DataFrame(data=result,columns=["doi","pub_type"])
        else:
            raise Exception("The input parameter is not an integer!")
    
    # q2
    def getPublicationsByAuthorId(self, orcid):
        if isinstance(orcid,str):
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                query = "SELECT id_doi, type, orcid FROM JournalArticleTable LEFT JOIN AuthorsTable ON JournalArticleTable.id_doi==AuthorsTable.doi WHERE orcid = '{orcid}' UNION SELECT id_doi, type, orcid FROM BookChapterTable LEFT JOIN AuthorsTable ON BookChapterTable.id_doi==AuthorsTable.doi WHERE orcid = '{orcid}' UNION SELECT id_doi, type, orcid FROM ProceedingsPaperTable LEFT JOIN AuthorsTable ON ProceedingsPaperTable.id_doi==AuthorsTable.doi WHERE orcid = '{orcid}'".format(orcid=orcid)
                cur.execute(query)
                result = cur.fetchall()
                qrdb.commit()
            return pd.DataFrame(data=result,columns=["doi","type","orcid"])
        else:
            raise Exception("The input parameter is not a string!")
        
    # q3
    def getMostCitedPublication(self):
        with sql3.connect(self.getDbPath()) as qrdb:
            cur = qrdb.cursor()
            query = "SELECT doi, cited_doi FROM CitationsTable"
            cur.execute(query)
            result = cur.fetchall() 
            qrdb.commit()
        return pd.DataFrame(data=result,columns=["doi","cited_doi"])

    # q4
    def getMostCitedVenue(self):
        with sql3.connect(self.getDbPath()) as qrdb:
            cur = qrdb.cursor()
            query = "SELECT doi, id_doi, publication_venue FROM JournalTable LEFT JOIN CitationsTable ON JournalTable.id_doi==CitationsTable.cited_doi WHERE cited_doi IS NOT NULL UNION SELECT doi, id_doi, publication_venue FROM BookTable LEFT JOIN CitationsTable ON BookTable.id_doi==CitationsTable.cited_doi WHERE cited_doi IS NOT NULL UNION SELECT doi, id_doi, publication_venue FROM ProceedingsTable LEFT JOIN CitationsTable ON ProceedingsTable.id_doi==CitationsTable.cited_doi WHERE cited_doi IS NOT NULL"
            cur.execute(query)
            result = cur.fetchall()            
            qrdb.commit()
        return pd.DataFrame(data=result,columns=["doi","cited_doi","venue_name"])

    # q5
    def getVenuesByPublisherId(self, crossref):
        if isinstance(crossref, str):
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                query = "SELECT issn_isbn, publication_venue, venue_type, id_crossref FROM JournalTable LEFT JOIN VenuesIDTable on JournalTable.id_doi == VenuesIDTable.doi WHERE id_crossref = '{crossref}' UNION SELECT issn_isbn, publication_venue, venue_type, id_crossref FROM BookTable LEFT JOIN VenuesIDTable on BookTable.id_doi == VenuesIDTable.doi WHERE id_crossref = '{crossref}' UNION SELECT issn_isbn, publication_venue, venue_type, id_crossref FROM ProceedingsTable LEFT JOIN VenuesIDTable on ProceedingsTable.id_doi == VenuesIDTable.doi WHERE id_crossref = '{crossref}'".format(crossref=crossref)
                cur.execute(query)
                result = cur.fetchall()
                qrdb.commit
            return pd.DataFrame(data=result,columns=["issn_isbn","publication_venue","venue_type","publisher"])
        else:
            raise Exception("The input parameter is not a string!")

    # q6
    def getPublicationInVenue(self, issn_isbn):
        if isinstance(issn_isbn, str):
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                query = "SELECT id_doi, type FROM JournalArticleTable LEFT JOIN VenuesIDTable on JournalArticleTable.id_doi == VenuesIDTable.doi WHERE issn_isbn = '{issn_isbn}' UNION SELECT id_doi, type FROM BookChapterTable LEFT JOIN VenuesIDTable on BookChapterTable.id_doi == VenuesIDTable.doi WHERE issn_isbn = '{issn_isbn}' UNION SELECT id_doi, type FROM ProceedingsPaperTable LEFT JOIN VenuesIDTable on ProceedingsPaperTable.id_doi == VenuesIDTable.doi WHERE issn_isbn = '{issn_isbn}'".format(issn_isbn=issn_isbn)
                cur.execute(query)
                result = cur.fetchall()
                qrdb.commit
            return pd.DataFrame(data=result,columns=["doi","type"])
        else:
            raise Exception("The input parameter is not a string!")

    # q7
    def getJournalArticlesInIssue(self, issue, volume, journalId):
        if isinstance(issue,str) and isinstance(volume,str) and isinstance(journalId,str):
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                query = "SELECT id_doi, type, issue, volume, issn_isbn FROM JournalArticleTable LEFT JOIN VenuesIDTable ON JournalArticleTable.id_doi == VenuesIDTable.doi WHERE issue = '{issue}' AND volume = '{volume}' AND issn_isbn = '{issn_isbn}'".format(issue=issue, volume=volume, issn_isbn=journalId)
                cur.execute(query)
                result = cur.fetchall()
                qrdb.commit
            return pd.DataFrame(data=result,columns=["doi","type","issue","volume","issn_isbn"])
        else:
            raise Exception("All or some of the input parameters are not strings!")

    # q8
    def getJournalArticlesInVolume(self, volume, journalId):
        if isinstance(volume,str) and isinstance(journalId,str):
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                query = "SELECT id_doi, type, issue, volume, issn_isbn FROM JournalArticleTable LEFT JOIN VenuesIDTable ON JournalArticleTable.id_doi == VenuesIDTable.doi WHERE volume = '{volume}' AND issn_isbn = '{issn_isbn}'".format(volume=volume, issn_isbn=journalId)
                cur.execute(query)
                result = cur.fetchall()
                qrdb.commit
            return pd.DataFrame(data=result,columns=["doi","type","issue","volume","issn_isbn"])
        else:
            raise Exception("All or some of the input parameters are not strings!")

    # q9
    def getJournalArticlesInJournal(self, journalId):
        if isinstance(journalId,str):
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                query = "SELECT id_doi, type, issue, volume, issn_isbn FROM JournalArticleTable LEFT JOIN VenuesIDTable ON JournalArticleTable.id_doi == VenuesIDTable.doi WHERE issn_isbn = '{issn_isbn}'".format(issn_isbn=journalId)
                cur.execute(query)
                result = cur.fetchall()
                qrdb.commit
            return pd.DataFrame(data=result,columns=["doi","type","issue","volume","issn_isbn"])
        else:
            raise Exception("The input parameter is not a string!")

    # q10
    def getProceedingsByEvent(self, eventPartialName):
        if isinstance(eventPartialName,str):
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                query = "SELECT publication_venue, issn_isbn, event FROM ProceedingsTable LEFT JOIN VenuesIDTable ON ProceedingsTable.id_doi == VenuesIDTable.doi WHERE event LIKE '%{event}%'".format(event=eventPartialName)
                cur.execute(query)
                result = cur.fetchall()
                qrdb.commit
            return pd.DataFrame(data=result,columns=["publication_venue","issn_isbn","event"])
        else:
            raise Exception("The input parameter is not a string!")

    # q11
    def getPublicationAuthors(self, doi):
        if isinstance(doi,str):
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                query = "SELECT doi, family, given, orcid FROM AuthorsTable WHERE doi='{doi}'".format(doi=doi)
                cur.execute(query)
                result = cur.fetchall()
                qrdb.commit
            return pd.DataFrame(data=result,columns=["doi","family","given","orcid"])
        else:
            raise Exception("The input parameter is not a string!")

    # q12
    def getPublicationsByAuthorName(self, authorPartialName):
        if isinstance(authorPartialName,str):
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                query = "SELECT doi, type, issue, volume, NULL AS chapter, NULL AS event FROM JournalArticleTable LEFT JOIN AuthorsTable ON JournalArticleTable.id_doi == AuthorsTable.doi WHERE family LIKE '%{family}%' OR given LIKE '%{given}%' UNION SELECT doi, type, NULL AS issue, NULL AS volume, chapter, NULL AS event FROM BookChapterTable LEFT JOIN AuthorsTable ON BookChapterTable.id_doi == AuthorsTable.doi WHERE family LIKE '%{family}%' OR given LIKE '%{given}%' UNION SELECT doi, type, NULL AS issue, NULL AS volume, NULL AS chapter, NULL AS event FROM ProceedingsPaperTable LEFT JOIN AuthorsTable ON ProceedingsPaperTable.id_doi == AuthorsTable.doi WHERE family LIKE '%{family}%' OR given LIKE '%{given}%'".format(family=authorPartialName,given=authorPartialName)
                cur.execute(query)
                result = cur.fetchall()
                qrdb.commit
            return pd.DataFrame(data=result,columns=["doi","type","issue","volume","chapter","event"])
        else:
            raise Exception("The input parameter is not a string!")

    # q13
    def getDistinctPublisherOfPublications(self, doiList):
        if isinstance(doiList,list) and all(isinstance(n, str) for n in doiList):
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                result = list()
                for item in doiList:
                    query = "SELECT publisher, crossref FROM JournalTable LEFT JOIN PublishersTable ON JournalTable.id_crossref == PublishersTable.crossref WHERE id_doi = '{doi}' UNION SELECT publisher, crossref FROM BookTable LEFT JOIN PublishersTable ON BookTable.id_crossref==PublishersTable.crossref WHERE id_doi = '{doi}' UNION SELECT publisher, crossref FROM ProceedingsTable LEFT JOIN PublishersTable ON ProceedingsTable.id_crossref==PublishersTable.crossref WHERE id_doi = '{doi}'".format(doi = item)
                    cur.execute(query)
                    result_q = cur.fetchall()
                    result.extend(result_q)
                qrdb.commit()
            return pd.DataFrame(data=result,columns=["publisher","crossref"])
        else:
            raise Exception("The input parameter is not a list or it is not a list of strings!")

        
# ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––


# TEST AREA
rel_path = "relational.db"
rel_dp = RelationalDataProcessor()
rel_dp.setDbPath(rel_path)
rel_dp.uploadData("testData/new_relational_publications.csv")
rel_dp.uploadData("testData/new_relational_other_data.json")

# Checking the superclass is correct or not
# print(rel_dp.__bases__)

rel_qp = RelationalQueryProcessor()
rel_qp.setDbPath(rel_path)

""" q1 = rel_qp.getPublicationsPublishedInYear(2020)
q2 = rel_qp.getPublicationsByAuthorId("0000-0003-0530-4305")
q3 = rel_qp.getMostCitedPublication()
q4 = rel_qp.getMostCitedVenue()
q5 = rel_qp.getVenuesByPublisherId("crossref:301")
q6 = rel_qp.getPublicationInVenue("issn:2641-3337")
q7 = rel_qp.getJournalArticlesInIssue("10","126","issn:0138-9130")
q8 = rel_qp.getJournalArticlesInVolume("126","issn:0138-9130")
q9 = rel_qp.getJournalArticlesInJournal("issn:0138-9130")
q10 = rel_qp.getProceedingsByEvent("arz")
q11 = rel_qp.getPublicationAuthors("doi:10.1007/s11192-021-04097-5")
q12 = rel_qp.getPublicationsByAuthorName("Per")
q13 = rel_qp.getDistinctPublisherOfPublications(["doi:10.1007/978-3-030-61244-3_16","doi:10.1371/journal.pbio.3000385","doi:10.1007/s11192-018-2796-5"])
 """

#print(q5)
#print(q5.groupby(['publication_venue']))

# Checking the superclass is correct or not
# print(rel_qp.__bases__) """