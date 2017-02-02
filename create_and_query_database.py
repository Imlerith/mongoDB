#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb  2 11:34:11 2017

@author: nasekins
"""

import pprint
import os

os.chdir('/Users/nasekins/Documents/python_scripts/mongoDB')
     
        

######################MongoDB data import and querying#########################

#create the database first ( mongo shell: use osm_london )
#create the collection first ( mongo shell: db.createCollection("london_city") )
#import the data into the collection first ( shell (in the JSON file location): mongoimport -d osm_london -c london_city --file london.json )
       
db_name           = 'osm_london'
collection_name   = 'london_city'


# a MongoDB database class
class MDBDatabase():
    
    def __init__(self, db_name):
        self.db_name = db_name
        from pymongo import MongoClient
        client = MongoClient('localhost:27017')
        self.db = client[self.db_name]

        
    def get_collection(self, collection_name):
        self.collection = self.db[collection_name]
        return self.collection
           
    
    def run_query(self, pipeline, collection_name):
        self.collection = self.get_collection(collection_name)
        self.result = [doc for doc in self.collection.aggregate(pipeline)]
        return self.result

    
    def show_query_results(self, pipeline, collection_name):
        self.result = self.run_query(pipeline, collection_name)
        pprint.pprint(self.result)


##############Create a MongoDB database object to run queries##################           
dbase = MDBDatabase(db_name)
# get the collection
london_collection = dbase.get_collection(collection_name)        


###########################Simple query examples###############################
query1 = {'created.user': 'Tom Chance', 'amenity': {'$exists': 1}}
query2 = {'amenity': {'$exists': 1}, 'cuisine': 'italian'}
query3 = {'amenity': {'$exists': 1}, 'cuisine': 'italian', 'name': 'Piccolo Diavolo'}
for doc in london_collection.find(query1):
    pprint.pprint(doc)    

# how many contributions by a user are there which include information on amenities 
london_collection.find(query1).count()  

# print the names of Italian restaurants
for doc in london_collection.find(query2):
    try:
        pprint.pprint(doc['name']) 
    except KeyError:
        pass
    
# print out information about a restaurant  
for doc in london_collection.find(query3):
    pprint.pprint(doc)  
    

#######################Get 5 top contributors##################################
pipeline1 = [{'$match': {'created.user': {'$exists': 1}}}, 
             {'$group': {'_id': '$created.user', 'count': {'$sum': 1}}}, 
             {'$project': {'_id': 0, 'User': '$_id', 'Count': '$count'}},
             {'$sort': {'Count': -1}}, 
             {'$limit': 5}] 
            
dbase.run_query(pipeline1,collection_name)

#db.london_city.ensureIndex({'created.user': 1})


######################Get 10 most popular amenities############################
pipeline2 = [{'$match': {'amenity': {'$exists': 1}}}, 
             {'$group': {'_id': '$amenity', 'count': {'$sum': 1}}}, 
             {'$project': {'_id': 0, 'Amenity': '$_id', 'Count': '$count'}},
             {'$sort': {'Count': -1}}, 
             {'$limit': 10}] 
    
dbase.run_query(pipeline2,collection_name) #apparently, postboxes, benches, bicycle parkings and restaurants are the most important amenities



######################Get 10 most popular cuisines##########################
pipeline3 = [{ '$match': {'$and': [{'amenity': {'$exists': 1}}, {'amenity': 'restaurant'}], 'cuisine': {'$exists': 1} } }, 
             {'$group': {'_id': '$cuisine', 'count': {'$sum': 1}}}, 
             {'$project': {'_id': 0, 'Cuisine': '$_id', 'Count': '$count'}},
             {'$sort': {'Count': -1}}, 
             {'$limit': 10}]     

dbase.run_query(pipeline3,collection_name) #italian, indian and chinese cuisines are the most popular ones


london_collection.find(query1).count() 
dbase.run_query(pipeline1,collection_name)



        