import pandas as pd
import sqlite3 as sql3
import json

from ModelClasses import QueryProcessor
from auxiliary import readCSV, readJSON


# Global variables
df1 = pd.DataFrame()
df2 = pd.DataFrame()
df3 = pd.DataFrame()
df4 = pd.DataFrame()
df5 = pd.DataFrame()
df6 = pd.DataFrame()
df7 = pd.DataFrame()
df8 = pd.DataFrame()
df9 = pd.DataFrame()
df10 = pd.DataFrame()


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
        global df1, df2, df3, df4, df5, df6, df7, df8, df9, df10

        # ---------- CSV
        if filepath.endswith(".csv"):
            # df1 -> journal article         // columns = 'id', 'title', 'type', 'publication_year', 'issue', 'volume'
            # df2 -> book-chapter            // columns = 'id', 'title', 'type', 'publication_year', 'chapter'
            # df3 -> proceedings-paper       // columns = 'id', 'title', 'type', 'publication_year'
            # df4 -> Venue_book              // columns = 'id', 'publication_venue', 'venue_type', 'publisher'
            # df5 -> Venue_journal           // columns = 'id', 'publication_venue', 'venue_type', 'publisher'
            # df6 -> Venue_proceedings-event // columns = 'id', 'publication_venue', 'venue_type', 'publisher', 'event
            df1, df2, df3, df4, df5, df6 = readCSV(filepath)

            # ----- DATABASE CONNECTION
            with sql3.connect(self.dbPath) as rdb:
                df1.to_sql('JournalArticleTable', rdb, if_exists='replace', index=False)
                df2.to_sql('BookChapterTable', rdb, if_exists='replace', index=False)
                df3.to_sql('ProceedingsPaperTable', rdb, if_exists='replace', index=False)
                df4.to_sql('BookTable', rdb, if_exists='replace', index=False)
                df5.to_sql('JournalTable', rdb, if_exists='replace', index=False)
                df6.to_sql('ProceedingsTable', rdb, if_exists='replace', index=False)

                rdb.commit()

        # ---------- JSON
        elif filepath.endswith(".json"):
            # df7  -> authors                // columns = 'doi', 'family', 'given', 'orcid'
            # df8  -> VenueIDs              // columns = 'doi', 'issn_isbn'
            # df9  -> citations             // columns = 'doi', 'cited_doi'
            # df10 -> publishers            // columns = 'crossref', 'publisher'
            df7, df8, df9, df10 = readJSON(filepath)

            # ----- DATABASE CONNECTION
            with sql3.connect(self.dbPath) as rdb:
                df7.to_sql('AuthorsTable', rdb, if_exists='replace', index=False)
                df8.to_sql('VenuesIDTable', rdb, if_exists='replace', index=False)
                df9.to_sql('CitationsTable', rdb, if_exists='replace', index=False)
                df10.to_sql('PublishersTable', rdb, if_exists='replace', index=False)

                rdb.commit()

        return True

# ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––


class RelationalQueryProcessor(QueryProcessor, RelationalProcessor):
    # -- Constructor
    def __init__(self):
        super().__init__()

    # -- Queries
    def getPublicationsPublishedInYear(self, year):
        if type(year) == int:
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                query = "SELECT id, type FROM JournalArticleTable WHERE publication_year='{year}' UNION SELECT id, type FROM BookChapterTable WHERE publication_year='{year}' UNION SELECT id, type FROM ProceedingsPaperTable WHERE publication_year='{year}'".format(year=year)
                cur.execute(query)
                result = cur.fetchall()
                qrdb.commit()
            return pd.DataFrame(data=result,columns=["doi","pub_type"])
        else:
            raise Exception("The input parameter is not an integer!")

    def getPublicationsByAuthorId(self, orcid):
        QR_2 = pd.DataFrame()
        print("don the things here")
        return QR_2

    def getMostCitedPublication(self):
        QR_3 = pd.DataFrame()
        print("don the things here")
        return QR_3

    def getMostCitedVenue(self):
        QR_4 = pd.DataFrame()
        print("don the things here")
        return QR_4

    def getVenuesByPublisherId(self, crossref):
        QR_5 = pd.DataFrame()
        print("don the things here")
        return QR_5

    def getPublicationInVenue(self, issn_isbn):
        QR_6 = pd.DataFrame()
        print("don the things here")
        return QR_6

    def getJournalArticlesInIssue(self, issue, volume, journalId):
        QR_7 = pd.DataFrame()
        print("don the things here")
        return QR_7

    def getJournalArticlesInVolume(self, volume, journalId):
        QR_8 = pd.DataFrame()
        print("don the things here")
        return QR_8

    def getJournalArticlesInJournal(self, journalId):
        QR_9 = pd.DataFrame()
        print("don the things here")
        return QR_9

    def getProceedingsByEvent(self, eventPartialName):
        QR_10 = pd.DataFrame()
        print("don the things here")
        return QR_10

    def getPublicationAuthors(self, doi):
        QR_11 = pd.DataFrame()
        print("don the things here")
        return QR_11

    def getPublicationsByAuthorName(self, authorPartialName):
        QR_12 = pd.DataFrame()
        print("don the things here")
        return QR_12

    def getDistinctPublisherOfPublications(self, doiList):
        QR_13 = pd.DataFrame()
        print("don the things here")
        return QR_13

# ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––


# TEST AREA
rel_path = "relational.db"
rel_dp = RelationalDataProcessor()
rel_dp.setDbPath(rel_path)
rel_dp.uploadData("testData/relational_publications.csv")
rel_dp.uploadData("testData/relational_other_data.json")

# Checking the superclass is correct or not
# print(rel_dp.__bases__)

rel_qp = RelationalQueryProcessor()
rel_qp.setDbPath(rel_path)

q1 = rel_qp.getPublicationsPublishedInYear(2020)

# Checking the superclass is correct or not
# print(rel_qp.__bases__)
