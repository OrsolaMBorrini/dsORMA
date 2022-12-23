from ast import Pass
from cmath import nan
from multiprocessing import dummy
from typing import Final, final
import pandas as pd
import json
import dataModelClasses as dMC

#IMPORTANT CASES - 
#objs citing themselves
finalSUMdf = pd.DataFrame()
mem = {} # all the dicts(objs-variable) of each publication
doi_mem = [] #for check for not creating circular loops  #maybe global
             #mem will contain all the values for creating a pub obj
tutti_author = {}

def authorslist(filepath):    # Call this function with upload of json
    if filepath.endswith(".json"):
        with open(filepath, "r", encoding="utf-8") as file:
            jsondata = json.load(file)
        tutti_author.update(jsondata['authors'])
    

# Concatenating all dfs into one big one — called in the generic addition of queryprocs
def df_creator(z):
    sumDF = pd.DataFrame()
    for key in tutti_author:
        singDF1 = z.getPublicationsFromDOI([key])
        sumDF = pd.concat([singDF1,sumDF])
    caller(sumDF)
    return

def caller(sumDF):
    globals()['finalSUMdf'] = pd.concat([sumDF,finalSUMdf])
    createPubobjsdict(finalSUMdf) #instead of calling this here immediately after the first qp, we can send the list of dfs

def createPubobjsdict(df):     # This DF contains all our publication data for one all our databases.
    for idx, item in df.iterrows():
        if item['id'] in mem:  #checks if we have alreay dealt with the the DOI in the df argument
            pass
        else:
            #need to add proceedings as well as book chapter data!
            obj = {
                    "doi": [item['id']],
                    "title": item['title'],
                    "year":item['publication_year'],
                    "issue":item['issue'],
                    "volume":item['volume']
            }
            # ADDING AUTH LIST CREATED IN THE AUTHORLIST FUNCTION
            if item['id'] in tutti_author:    #check key
                auth_list = tutti_author[item['id']]  # json might have incomplete data
                obj.update({"authors":auth_list}) #adding dict of authors for this obj
            else:
                auth_list = {
                            "family": "No Data available",
                            "given": "No Data available",
                            "orcid": "No Data available"}
                obj.update({"authors":auth_list}) #adding dict of authors for this obj

            # CREATING AND ADDING VENUE AND PUBLISHER OBJECT
            if item['publication_venue'] == None:
                empt_publisher = {"id":"none//data nt available","title":"none//data nt available"}
                empt_venue = {"id":"none/data nt available", "title":"none/data nt available","publisher":empt_publisher}
                obj.update({'venue': empt_venue })
            else:
                org_data = {"id":[item["publisher"]],"title":item["name_pub"]}
                ven_data = {"id":[item['issn_isbn']],"title": item['publication_venue'], "publisher": org_data}
                obj.update({"venue":ven_data})
            mem.update({item['id']:obj})   #here we add the incomple obj to the mem dict
            
            #creating cites list
            cites_list = []
            cites_list.append(item['ref_doi'])
            obj.update({'cites': cites_list })
    
citlist = set()

def MANUalrecursion(doilist):
    for item in doilist:
        if item == None or item == "":
            Noauth = dMC.Person("NO-Orcid","NO-Givenname","No-Familyname")
            autlist = set()
            autlist.add(Noauth)
            NoVenue = dMC.Venue("NO-VenueId","NO-VenueTitle",dMC.Organisation("NO-OrganisationID","NO-OrganisationTitle"))
            ender = dMC.Publication("NO-PubID","NO-PubYear","NO-Title",autlist,NoVenue,set())
            citlist.add(ender)
            pass
        else:
            citlist.add(creatPubobj(item))
    return citlist

def creatPubobj(DOI):   #creates Publication class objects 
    Final_obj = {}
    Final_obj.update(mem[DOI])
    #now we use the Final_obj dict to create our class object
    #first we create dependency objects like person object, venue object, etc
    # 1 - author object by iterating over the dict in the Final_obj dict
    aut = []
    for item in Final_obj['authors']:
        person = dMC.Person(item['orcid'],item['given'],item['family'])
        aut.append(person)
    if Final_obj['venue']['id'] == None:
        pass
    else:
        orgdata = {}
        orgdata.update(Final_obj['venue']['publisher'])
        publisherobj = dMC.Organisation(orgdata['id'],orgdata['title'])
        venueobj = dMC.Venue(Final_obj['venue']['id'],Final_obj['venue']['title'],publisherobj)
    
    if Final_obj['cites'] == None:
        pub_obj = dMC.Publication(Final_obj['doi'],Final_obj['year'],Final_obj['title'],aut,venueobj,set())
    else:
        pub_obj = dMC.Publication(Final_obj['doi'],Final_obj['year'],Final_obj['title'],aut,venueobj,MANUalrecursion(Final_obj['cites']))

    return pub_obj     

def creatJAobj(DOI):   #creates JA class objects 
    Final_obj = {}
    Final_obj.update(mem[DOI])
    #now we use the Final_obj dict to create our class object
    #first we create dependency objects like person object, venue object, etc
    # 1 - author object by iterating over the dict in the Final_obj dict
    aut = []
    for item in Final_obj['authors']:
        person = dMC.Person(item['orcid'],item['given'],item['family'])
        aut.append(person)
    if Final_obj['venue']['id'] == None:
        pass
    else:
        orgdata = {}
        orgdata.update(Final_obj['venue']['publisher'])
        publisherobj = dMC.Organisation(orgdata['id'],orgdata['title'])
        venueobj = dMC.Venue(Final_obj['venue']['id'],Final_obj['venue']['title'],publisherobj)
    
    if Final_obj['cites'] == None:
        ja_obj = dMC.JournalArticle(Final_obj['doi'],Final_obj['year'],Final_obj['title'],aut,venueobj,set(),Final_obj['issue'],Final_obj['volume'])
    else:
        ja_obj = dMC.JournalArticle(Final_obj['doi'],Final_obj['year'],Final_obj['title'],aut,venueobj,MANUalrecursion(Final_obj['cites']),Final_obj['issue'],Final_obj['volume'])
    
    return ja_obj