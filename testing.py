import ModelClasses as mdc
import pandas as pd

# 1) Importing all the classes for handling the relational database
from relationalData_Manager import RelationalDataProcessor, RelationalQueryProcessor

# 2) Importing all the classes for handling RDF database
from graphData_Manager import TriplestoreDataProcessor, TriplestoreQueryProcessor

# 3) Importing the class for dealing with generic queries
from GenericQueryP import GenericQueryProcessor

# Once all the classes are imported, first create the relational
# database using the related source data
rel_path = "relational.db"
rel_dp = RelationalDataProcessor()
rel_dp.setDbPath(rel_path)
#rel_dp.uploadData("testData/relational_publications.csv")
#rel_dp.uploadData("testData/relational_other_data.json")


# Then, create the RDF triplestore (remember first to run the
# Blazegraph instance) using the related source data
grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
grp_dp = TriplestoreDataProcessor()
grp_dp.setEndpointUrl(grp_endpoint)
#grp_dp.uploadData("testData/graph_publications.csv")
#grp_dp.uploadData("testData/graph_other_data.json")


# In the next passage, create the query processors for both
# the databases, using the related classes
rel_qp = RelationalQueryProcessor()
rel_qp.setDbPath(rel_path)

grp_qp = TriplestoreQueryProcessor()
grp_qp.setEndpointUrl(grp_endpoint)

# Finally, create a generic query processor for asking
# about data
generic = GenericQueryProcessor()
generic.addQueryProcessor(rel_qp)
generic.addQueryProcessor(grp_qp)



# --------- TESTING ON SOME VARIABLES THE GENERIC QUERIES

result_q1 = generic.getPublicationsPublishedInYear(2020)
""" 
print("Result of query getPublicationsPublishedInYear:\n",result_q1)    # list[Publication]
print("Type:\n",type(result_q1))    #  <class 'list'>
for item in result_q1:
    print("getPublicationYear:\n",item.getPublicationYear())    # 2020
    print("getTitle:\n",item.getTitle())    # Tension Between Leadership Archetypes: Systematic Review To Inform Construction Research And Practice
    print("getCitedPublications:\n",item.getCitedPublications())    # []
    print("getPublicationVenue:\n",item.getPublicationVenue())  # <ModelClasses.Venue object at 0x000001D7B42D5D50>
    print("getAuthors:\n",item.getAuthors())     # {<ModelClasses.Person object at 0x000001D7B42D48E0>}
    print("getIds:\n",item.getIds())    # ['doi:10.1061/(asce)me.1943-5479.0000722']
    # ---- Additional second-level queries
    for pub in item.getCitedPublications():
        print("getCitedPublications().getPublicationYear():\n",pub.getPublicationYear())
        print("getCitedPublications().getTitle():\n",pub.getTitle())
        print("getCitedPublications().getCitedPublications():\n",pub.getCitedPublications())
        print("getCitedPublications().getPublicationVenue():\n",pub.getPublicationVenue())
        print("getCitedPublications().getAuthors():\n",pub.getAuthors())
        break
    print("getPublicationVenue().getTitle()",item.getPublicationVenue().getTitle()) # str
    print("getPublicationVenue().getPublisher()",item.getPublicationVenue().getPublisher()) # <ModelClasses.Organization object at 0x000001570D0357E0> 
    print("getPublicationVenue().getIds()",item.getPublicationVenue().getIds()) # ['issn:1471-244X']
    for author in item.getAuthors():
        print("getAuthors().getGivenName()",author.getGivenName())  # str
        print("getAuthors().getFamilyName()",author.getFamilyName())    # str
        print("getAuthors().getIds()",author.getIds())  # ['0000-0001-7412-4776']
        break
    break
"""

result_q2 = generic.getPublicationsByAuthorId("0000-0001-9857-1511")
"""
print("Result of query getPublicationsByAuthorId:\n",result_q2)
print("Type:\n",type(result_q2))
for item in result_q2:
    print("getPublicationYear:\n",item.getPublicationYear())
    print("getTitle:\n",item.getTitle())
    print("getCitedPublications:\n",item.getCitedPublications())
    print("getPublicationVenue:\n",item.getPublicationVenue())
    print("getAuthors:\n",item.getAuthors())
    print("getIds:\n",item.getIds())
    # ---- Additional second-level queries
    for pub in item.getCitedPublications():
        print("getCitedPublications().getPublicationYear():\n",pub.getPublicationYear())
        print("getCitedPublications().getTitle():\n",pub.getTitle())
        print("getCitedPublications().getCitedPublications():\n",pub.getCitedPublications())
        print("getCitedPublications().getPublicationVenue():\n",pub.getPublicationVenue())
        print("getCitedPublications().getAuthors():\n",pub.getAuthors())
        break
    print("getPublicationVenue().getTitle()",item.getPublicationVenue().getTitle())
    print("getPublicationVenue().getPublisher()",item.getPublicationVenue().getPublisher())
    print("getPublicationVenue().getIds()",item.getPublicationVenue().getIds())
    for author in item.getAuthors():
        print("getAuthors().getGivenName()",author.getGivenName())
        print("getAuthors().getFamilyName()",author.getFamilyName())
        print("getAuthors().getIds()",author.getIds())
        break
    break
"""

result_q3 = generic.getMostCitedPublication()
""" print("Result of query getMostCitedPublication:\n",result_q3)
print("Type:\n",type(result_q3))
print("getPublicationYear:\n",result_q3.getPublicationYear())
print("getTitle:\n",result_q3.getTitle())
print("getCitedPublications:\n",result_q3.getCitedPublications())
print("getPublicationVenue:\n",result_q3.getPublicationVenue())
print("getAuthors:\n",result_q3.getAuthors())
print("getIds:\n",result_q3.getIds())
# ---- Additional second-level queries
for pub in result_q3.getCitedPublications():
    print("getCitedPublications().getPublicationYear():\n",pub.getPublicationYear())
    print("getCitedPublications().getTitle():\n",pub.getTitle())
    print("getCitedPublications().getCitedPublications():\n",pub.getCitedPublications())
    print("getCitedPublications().getPublicationVenue():\n",pub.getPublicationVenue())
    print("getCitedPublications().getAuthors():\n",pub.getAuthors())
    break
print("getPublicationVenue().getTitle()",result_q3.getPublicationVenue().getTitle())
print("getPublicationVenue().getPublisher()",result_q3.getPublicationVenue().getPublisher())
print("getPublicationVenue().getIds()",result_q3.getPublicationVenue().getIds())
for author in result_q3.getAuthors():
    print("getAuthors().getGivenName()",author.getGivenName())
    print("getAuthors().getFamilyName()",author.getFamilyName())
    print("getAuthors().getIds()",author.getIds())
    break
"""


result_q4 = generic.getMostCitedVenue()
"""
print("Result of query getMostCitedVenue:\n",result_q4)
print("Type:\n",type(result_q4))
print("getTitle:\n",result_q4.getTitle())
print("getPublisher:\n",result_q4.getPublisher())
print("getIds:\n",result_q4.getIds())
# ---- Additional second-level queries
print("getPublisher().getIds():\n",result_q4.getPublisher().getIds())
print("getPublisher().getName():\n",result_q4.getPublisher().getName())
"""


result_q5 = generic.getVenuesByPublisherId("crossref:286")
""" 
print("Result of query getVenuesByPublisherId:\n",result_q5)   # list[Venue]
print("Type:\n",type(result_q5))   #  <class 'list'>
for item in result_q5:
    print("getTitle:\n",item.getTitle())
    print("getPublisher:\n",item.getPublisher())
    print("getIds:\n",item.getIds())
    # ---- Additional second-level queries
    print("getPublisher().getIds():\n",item.getPublisher().getIds())
    print("getPublisher().getName():\n",item.getPublisher().getName())
    break
"""

result_q6 = generic.getPublicationInVenue("issn:0138-9130")
"""
print("Result of query getPublicationInVenue:\n",result_q6)
print("Type:\n",type(result_q6))
for item in result_q6:
    print("getPublicationYear:\n",item.getPublicationYear())
    print("getTitle:\n",item.getTitle())
    print("getCitedPublications:\n",item.getCitedPublications())
    print("getPublicationVenue:\n",item.getPublicationVenue())
    print("getAuthors:\n",item.getAuthors())
    print("getIds:\n",item.getIds())
    # ---- Additional second-level queries
    for pub in item.getCitedPublications():
        print("getCitedPublications().getPublicationYear():\n",pub.getPublicationYear())
        print("getCitedPublications().getTitle():\n",pub.getTitle())
        print("getCitedPublications().getCitedPublications():\n",pub.getCitedPublications())
        print("getCitedPublications().getPublicationVenue():\n",pub.getPublicationVenue())
        print("getCitedPublications().getAuthors():\n",pub.getAuthors())
        break
    print("getPublicationVenue().getTitle()",item.getPublicationVenue().getTitle())
    print("getPublicationVenue().getPublisher()",item.getPublicationVenue().getPublisher())
    print("getPublicationVenue().getIds()",item.getPublicationVenue().getIds())
    for author in item.getAuthors():
        print("getAuthors().getGivenName()",author.getGivenName())
        print("getAuthors().getFamilyName()",author.getFamilyName())
        print("getAuthors().getIds()",author.getIds())
        break
    break
 """

result_q7 = generic.getJournalArticlesInIssue("1","1","issn:2641-3337")
"""
print("Result of query getJournalArticlesInIssue:\n",result_q7)
print("Type:\n",type(result_q7))
for item in result_q7:
    print("getPublicationYear:\n",item.getPublicationYear())
    print("getTitle:\n",item.getTitle())
    print("getCitedPublications:\n",item.getCitedPublications())
    print("getPublicationVenue:\n",item.getPublicationVenue())
    print("getAuthors:\n",item.getAuthors())
    print("getIds:\n",item.getIds())
    print("getIssue:\n",item.getIssue())
    print("getVolume:\n",item.getVolume())
    # ---- Additional second-level queries
    for pub in item.getCitedPublications():
        print("getCitedPublications().getPublicationYear():\n",pub.getPublicationYear())
        print("getCitedPublications().getTitle():\n",pub.getTitle())
        print("getCitedPublications().getCitedPublications():\n",pub.getCitedPublications())
        print("getCitedPublications().getPublicationVenue():\n",pub.getPublicationVenue())
        print("getCitedPublications().getAuthors():\n",pub.getAuthors())
        break
    print("getPublicationVenue().getTitle()",item.getPublicationVenue().getTitle())
    print("getPublicationVenue().getPublisher()",item.getPublicationVenue().getPublisher())
    print("getPublicationVenue().getIds()",item.getPublicationVenue().getIds())
    for author in item.getAuthors():
        print("getAuthors().getGivenName()",author.getGivenName())
        print("getAuthors().getFamilyName()",author.getFamilyName())
        print("getAuthors().getIds()",author.getIds())
        break
    break
"""

result_q8 = generic.getJournalArticlesInVolume("72","issn:1022-2588")
""" print("Result of query getJournalArticlesInVolume:\n",result_q8)    # [<ModelClasses.JournalArticle object at 0x000001EC2A027B80>]
print("Type:\n",type(result_q8))
for item in result_q8:
    print("This is the type of the ITEM:\n",item)   # <ModelClasses.JournalArticle object at 0x000001EC2A027B80>
    print("getPublicationYear:\n",item.getPublicationYear())
    print("getTitle:\n",item.getTitle())
    print("getCitedPublications:\n",item.getCitedPublications())
    print("getPublicationVenue:\n",item.getPublicationVenue())
    print("getAuthors:\n",item.getAuthors())
    print("getIds:\n",item.getIds())
    print("getIssue:\n",item.getIssue())
    print("getVolume:\n",item.getVolume())
    # ---- Additional second-level queries
    for pub in item.getCitedPublications():
        print("getCitedPublications().getPublicationYear():\n",pub.getPublicationYear())
        print("getCitedPublications().getTitle():\n",pub.getTitle())
        print("getCitedPublications().getCitedPublications():\n",pub.getCitedPublications())
        print("getCitedPublications().getPublicationVenue():\n",pub.getPublicationVenue())
        print("getCitedPublications().getAuthors():\n",pub.getAuthors())
        break
    print("getPublicationVenue().getTitle()",item.getPublicationVenue().getTitle())
    print("getPublicationVenue().getPublisher()",item.getPublicationVenue().getPublisher())
    print("getPublicationVenue().getIds()",item.getPublicationVenue().getIds())
    for author in item.getAuthors():
        print("getAuthors().getGivenName()",author.getGivenName())
        print("getAuthors().getFamilyName()",author.getFamilyName())
        print("getAuthors().getIds()",author.getIds())
        break
    break
 """

# MANU's update - is fixed now, but please test again with some other JA id to be sure
result_q9 = generic.getJournalArticlesInJournal("issn:0138-9130")
"""
print("Result of query getJournalArticlesInJournal:\n",result_q9)   #  [<ModelClasses.Publication object at 0x0000021A7651EDD0>]
print("Type:\n",type(result_q9))   
for item in result_q9:
    print("This is the type of the ITEM:\n",item)   # <ModelClasses.Publication object at 0x0000021A7651EDD0>
    print("getPublicationYear:\n",item.getPublicationYear())
    print("getTitle:\n",item.getTitle())
    print("getCitedPublications:\n",item.getCitedPublications())
    print("getPublicationVenue:\n",item.getPublicationVenue())
    print("getAuthors:\n",item.getAuthors())
    print("getIds:\n",item.getIds())
    print("getIssue:\n",item.getIssue())    # AttributeError: 'Publication' object has no attribute 'getIssue'
    print("getVolume:\n",item.getVolume())
    # ---- Additional second-level queries
    for pub in item.getCitedPublications():
        print("getCitedPublications().getPublicationYear():\n",pub.getPublicationYear())
        print("getCitedPublications().getTitle():\n",pub.getTitle())
        print("getCitedPublications().getCitedPublications():\n",pub.getCitedPublications())
        print("getCitedPublications().getPublicationVenue():\n",pub.getPublicationVenue())
        print("getCitedPublications().getAuthors():\n",pub.getAuthors())
        break
    print("getPublicationVenue().getTitle()",item.getPublicationVenue().getTitle())
    print("getPublicationVenue().getPublisher()",item.getPublicationVenue().getPublisher())
    print("getPublicationVenue().getIds()",item.getPublicationVenue().getIds())
    for author in item.getAuthors():
        print("getAuthors().getGivenName()",author.getGivenName())
        print("getAuthors().getFamilyName()",author.getFamilyName())
        print("getAuthors().getIds()",author.getIds())
        break
    break
"""

result_q10 = generic.getProceedingsByEvent("ffic")
""" print("Result of query getProceedingsByEvent:\n",result_q10)
print("Type:\n",type(result_q10))
for item in result_q10:
    print("getEvent:\n",item.getEvent())
    print("getTitle:\n",item.getTitle())
    print("getPublisher:\n",item.getPublisher())
    print("getIds:\n",item.getIds())
    # ---- Additional second-level queries
    print("getPublisher().getIds():\n",item.getPublisher().getIds())
    print("getPublisher().getName():\n",item.getPublisher().getName())
    break
 """


result_q11 = generic.getPublicationAuthors("doi:10.1007/978-3-030-71903-6_32")
"""
print("Result of query getPublicationAuthors:\n",result_q11)
print("Type:\n",type(result_q11))
for item in result_q11:
    print("getGivenName:\n",item.getGivenName())
    print("getFamilyName:\n",item.getFamilyName())
    print("getIds:\n",item.getIds())
    break
"""

#  Still can get stuck at max recursion !!
result_q12 = generic.getPublicationsByAuthorName("Dennis")
"""
print("Result of query getPublicationsByAuthorName:\n",result_q12)
print("Type:\n",type(result_q12))
for item in result_q12:
    print("getPublicationYear:\n",item.getPublicationYear())
    print("getTitle:\n",item.getTitle())
    print("getCitedPublications:\n",item.getCitedPublications())
    print("getPublicationVenue:\n",item.getPublicationVenue())
    print("getAuthors:\n",item.getAuthors())
    print("getIds:\n",item.getIds())
    # ---- Additional second-level queries
    for pub in item.getCitedPublications():
        print("getCitedPublications().getPublicationYear():\n",pub.getPublicationYear())
        print("getCitedPublications().getTitle():\n",pub.getTitle())
        print("getCitedPublications().getCitedPublications():\n",pub.getCitedPublications())
        print("getCitedPublications().getPublicationVenue():\n",pub.getPublicationVenue())
        print("getCitedPublications().getAuthors():\n",pub.getAuthors())
        break
    print("getPublicationVenue().getTitle()",item.getPublicationVenue().getTitle())
    print("getPublicationVenue().getPublisher()",item.getPublicationVenue().getPublisher())
    print("getPublicationVenue().getIds()",item.getPublicationVenue().getIds())
    for author in item.getAuthors():
        print("getAuthors().getGivenName()",author.getGivenName())
        print("getAuthors().getFamilyName()",author.getFamilyName())
        print("getAuthors().getIds()",author.getIds())
        break
    break
"""

result_q13 = generic.getDistinctPublishersOfPublications(["doi:10.1007/978-3-030-71903-6_32","doi:10.1007/s11192-016-2215-8"])
""" print("Result of query getDistinctPublishersOfPublications:\n",result_q13)
print("Type:\n",type(result_q13))
for item in result_q13:
    print("getName:\n",item.getName())
    print("getIds:\n",item.getIds())
    break
 """