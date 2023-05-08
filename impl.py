import ModelClasses as mdc
import pandas as pd

# 1) Importing all the classes for handling the relational database
from relationalData_Manager import RelationalDataProcessor, RelationalQueryProcessor, df1_r, df10_r

# 2) Importing all the classes for handling RDF database
from graphData_Manager import df1_g,df10_g
from graphData_Manager import TriplestoreDataProcessor, TriplestoreQueryProcessor
#import graphData_Manager as gdp

# 3) Importing the class for dealing with generic queries
from GenericQueryP import GenericQueryProcessor

# Once all the classes are imported, first create the relational
# database using the related source data
rel_path = "relational.db"
rel_dp = RelationalDataProcessor()
rel_dp.setDbPath(rel_path)
rel_dp.uploadData("testData/relational_publications.csv")
rel_dp.uploadData("testData/relational_other_data.json")

#print(df1_r.head())
#print(df10_r.head())

# Then, create the RDF triplestore (remember first to run the
# Blazegraph instance) using the related source data
grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
grp_dp = TriplestoreDataProcessor()
grp_dp.setEndpointUrl(grp_endpoint)
grp_dp.uploadData("testData/graph_publications.csv")
grp_dp.uploadData("testData/graph_other_data.json")

#print(df1_g)
#print(df10_g)

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


print(generic.getMostCitedPublication())