from dataclasses import *
from graphClasses import *
from extraGraphClasses import TriplestoreDataProcessor,TriplestoreQueryProcessor
from extraRelationalClasses import RelationalDataProcessor,RelationalQueryProcessor
from genericQueryProcessor import *
from pubMemory_full import *

rel_path = "relational.db"
rel_dp = RelationalDataProcessor()
rel_dp.setDbPath(rel_path)

rel_dp.uploadData("testData/new_relational_publications.csv") 
rel_dp.uploadData("testData/new_relational_other_data.json")
print("uploaded relational data")

# Then, create the RDF triplestore (remember first to run the
# Blazegraph instance) using the related source data
grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
grp_dp = TriplestoreDataProcessor()
grp_dp.setEndpointUrl(grp_endpoint)

grp_dp.uploadData("testData/new_graph_publications.csv")
grp_dp.uploadData("testData/new_graph_other_data.json")
print("ALL THE UPLOAD IS DONE")

# In the next passage, create the query processors for both the databases, using the related classes
rel_qp = RelationalQueryProcessor()
rel_qp.setDbPath(rel_path)

grp_qp = TriplestoreQueryProcessor()
grp_qp.setEndpointUrl(grp_endpoint)

# Finally, create a generic query processor for asking about data
generic = GenericQueryProcessor()
generic.addQueryProcessor(rel_qp)
generic.addQueryProcessor(grp_qp)



#NOTES - ther are edge cases where the publication might end up citing itself and in that case it goes into endless recursion

print("=====\n START OF THE QUERIES \n =====\n")

q1 = generic.getPublicationsPublishedInYear(2014)
print("getPublicationsPublishedInYear Query\n",q1)
# === TESTING THE QUERIES FOR THE SPECIFIC OBJECTS
for pub in q1:
  print("getIds:\t",pub.getIds())
  print("getPublicationYear:\t",pub.getPublicationYear())
  print("getTitle:\t",pub.getTitle())
  print("getCitedPublications:\t",pub.getCitedPublications())
  print("getPublicationVenue:\t",pub.getPublicationVenue())
  print("getAuthors:\t",pub.getAuthors())
  if type(pub) == JournalArticle:
    print("The Publication is a JournalArticle")
    print("getIssue:\t",pub.getIssue())
    print("getVolume:\t",pub.getVolume())
  elif type(pub) == BookChapter:
    print("The Publication is a BookChapter")
    print("getChapterNumber:\t",pub.getChapterNumber())

q2 = generic.getPublicationsByAuthorId("0000-0001-9857-1511")
print("getPublicationsByAuthorId Query\n",q2)

q3 = generic.getMostCitedPublication()
print("getMostCitedPublication Query\n",q3)

q4 = generic.getMostCitedVenue()
print("getMostCitedVenue Query\n",q4)

q5 = generic.getVenuesByPublisherId("crossref:5")
print("getVenuesByPublisherId Query\n",q5)
# === TESTING THE QUERIES FOR THE SPECIFIC OBJECTS
for ven in q5:
  print("getIds:\t",ven.getIds())
  print("getPublisher:\t",ven.getPublisher())
  print("getTitle:\t",ven.getTitle())
  if type(ven) == Proceedings:
    print("The Venue is a Proceedings")
    print("getEvent:\t",ven.getEvent())

q6 = generic.getPublicationInVenue("issn:0219-1377")
print("getPublicationInVenue Query\n",q6)

q7 = generic.getJournalArticlesInIssue("9","13","issn:1999-4893")
print("getJournalArticleInIssue Query\n",q7)

q8 = generic.getJournalArticlesInVolume("17","issn:2164-5515")
print("getJournalArticleInVolume Query\n",q8)

q9 = generic.getJournalArticlesInJournal("issn:2641-3337")
print("getJournalArticleInJournal Query\n",q9)

q10 = generic.getProceedingsByEvent("ducks")
print("getProceedingsByEvent Query\n",q10)

q11 = generic.getPublicationAuthors("doi:00.0000/69")
print("getPublicationAuthors Query\n",q11)
# === TESTING THE QUERIES FOR THE SPECIFIC OBJECTS
for pers in q11:
  print("getIds:\t",pers.getIds())
  print("getGivenName:\t",pers.getGivenName())
  print("getFamilyName:\t",pers.getFamilyName())

q12 = generic.getPublicationsByAuthorName("riesco")
print("getPublicationsByAuthorName Query\n",q12)

q13 = generic.getDistinctPublishersOfPublications(["doi:10.1080/21645515.2021.1910000","doi:10.3390/ijfs9030035"])
print("getDistinctPublisherOfPublications Query\n",q13)
# === TESTING THE QUERIES FOR THE SPECIFIC OBJECTS
for org in q13:
  print("getIds:\t",org.getIds())
  print("getName:\t",org.getName())


