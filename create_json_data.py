#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb  2 12:57:11 2017

@author: nasekins
"""

import xml.etree.cElementTree as ET
#import pprint
import re
import codecs
import json
import os
from collections import defaultdict

os.chdir('/Users/nasekins/Documents/python_scripts/mongoDB')
filename = 'london.osm'

# regular expression to search for "normal" tag names
lower = re.compile(r'^([a-z]|_)*$')
# regular expression to search for tag names with colons
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
# regular expression to search for tag names with "irregular" characters
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
# regular expression for street names
street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)

CREATED = [ "version", "changeset", "timestamp", "user", "uid"]

# a mapping to correct street names
mapping = { "St": "Street",
            "St.": "Street",
            "Steet": "Street",
            "Sq": "Square",
            "Park,": "Park",
            "Rd": "Road",
            "Rd.": "Road",
            "Road)": "Road",
            "road": "Road",
            "Ave": "Avenue",
            "Ave.": "Avenue",
            "HIll": "Hill",
            "Picadilly": "Piccadilly"
            }
            
# "good" street names            
expected = ["Street", "Avenue", "Boulevard", "Drive", "Court",
"Place", "Square", "Lane", "Road", "Trail", "Parkway", "Commons",
"Highway", "Loop", "Circle", "Walk", "Way", "Southwest", "Northeast",
"Southeast", "Northwest"]
 
# this function updates streetnames         
def update_name(name, mapping):
    m = street_type_re.search(name)
    if ( m and ( m.group() in mapping.keys() )):
        name = street_type_re.sub(mapping[m.group()], name)
    return name
    
    
# this function processes elements of the XML iterparser   
# input: XML iterparser element
# output: an Open Street Map document (dictionary) 
def shape_element(element):
    node = {}
    if element.tag == "node" or element.tag == "way" :
        try:
            node['id'] = element.attrib['id']
        except KeyError:
            pass
        
        try:
            node['visible'] = element.attrib['visible']
        except KeyError:
            pass
        
        created = {}
        for key in CREATED:
            try:
                created[key] = element.attrib[key]
            except KeyError:
                pass
        node['created'] = created
        if element.tag == "node":
            node['type'] = 'node'
            pos = [float(element.attrib['lat']),float(element.attrib['lon'])]
            node['pos'] = pos
        else:
            node['type'] = 'way'
            node_refs = []
            for nd in element.iter('nd'):
                node_refs.append(nd.attrib['ref'])
            if ( len(node_refs)>0 ):
                node['node_refs'] = node_refs
            
        #Fill in the information in "tag" tags
        address = defaultdict(None)
        for tag in element.iter('tag'):
            if ( (tag.attrib['k'][0:5] == 'addr:') and (len([x for x in tag.attrib['k'] if x == ':']) == 1) ):
                k_key = tag.attrib['k'][5:]
                name_improved = update_name(tag.attrib['v'], mapping).lower().title()
                address[k_key] = name_improved
            elif ( (tag.attrib['k'][0:5] != 'addr:') and ( ':' in tag.attrib['k'] ) ):
                continue
            elif ( len([x for x in tag.attrib['k'] if x == ':']) > 1 ):
                continue
            elif (problemchars.search(tag.attrib['k']) is not None):
                continue
            else:
                k_key = tag.attrib['k']
                node[k_key] = tag.attrib['v']
            
        # check if dictionary not empty
        if ( bool(address) ):        
            node['address'] = dict(address)
      
        return node
    else:
        return None
        
# this is the main XML data processing function
# it creates a list of dictionaries out of the XML input        
# then it creates a JSON file out of the list of dictionaries  
# with the same name as the original XML file   

# input: XML file name, output formatting detail
# output: list of Open Street Map documents (dictionaries)       
def process_map(file_in, pretty = False):
    #take every thing before the dot
    bdot_re = re.compile(r'\b[^.]+', re.IGNORECASE)
    m = bdot_re.search(filename)
    file_out = "{0}.json".format(m.group())
    data = []
    with codecs.open(file_out, "w") as fo:
        for _, element in ET.iterparse(file_in):
            el = shape_element(element)
            if el:
                data.append(el)
                if pretty:
                    fo.write(json.dumps(el, indent=2)+"\n")
                else:
                    fo.write(json.dumps(el) + "\n")
    return data
              

def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected:
            street_types[street_type].add(street_name)


def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")


def audit(osmfile):
    with codecs.open(osmfile, 'r', 'utf8', errors='replace') as osm_file:
        street_types = defaultdict(set)
        for event, elem in ET.iterparse(osm_file, events=("start",)):
            if elem.tag == "node" or elem.tag == "way":
                for tag in elem.iter("tag"):
                    if is_street_name(tag):
                        try:
                            audit_street_type(street_types, tag.attrib['v'])
                        except:
                            print('Some error occurred')
                            continue
    return street_types
    
# create the data    
data = process_map(filename, True)     
