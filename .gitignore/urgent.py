#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 29 11:31:37 2018

@author: polo
"""

import re
import time

import numpy as np

from fuzzywuzzy import fuzz

import pandas as pd

import os
import urllib
import requests
from xml.etree import ElementTree as ET

#_______________________________________________________________

location = 'Yazid'
location = 'ggllllggg'

user_name = 'panos.kostakos'
fuzzy_score = '0.8'


requestURL = 'http://api.geonames.org/search?' \
            + 'q=' + location \
            + '&fuzzy=' + fuzzy_score \
            + '&username=' + user_name

resp = requests.get(requestURL)
msg = resp.content

tree = ET.fromstring(msg)

root = ET.parse(urllib.request.urlopen(requestURL)).getroot()

item = root.find('geoname')
if item:
    print(item.find('name').text + ', ' + item.find('countryName').text + ' [' + item.find('lat').text + ',' + item.find('lng').text + '] [id:' + item.find('geonameId').text + ']')

#for item in items:
#    print(item.find('name').text + ', ' + item.find('countryName').text + ' [' + item.find('lat').text + ',' + item.find('lng').text + '] [id:' + item.find('geonameId').text + ']')


#_______________________________________________________________

def split_uppercase(value):
    S = re.sub(r'([A-Z][a-z]+)', r' \1', value)
    return re.sub(r'([A-Z]+)', r' \1', S)

#_______________________________________________________________

def LOAD_NETHERLANDS_DATA():
    
    start_time = time.time()
    
    #df = pd.read_csv('SubSetHashTags500.tsv',delimiter='\t',encoding='utf-8')
    df = pd.read_csv('SubSetHashTags10000.tsv',delimiter='\t',encoding='utf-8')

    df['Hashtag'] = df['Hashtag'].str.replace('[0-9]','')
    
    df['Hashtag'] = df.Hashtag.astype(str)
    df['Hashtag'] = df['Hashtag'].apply(split_uppercase)    
        
    df['Hashtag'] = df['Hashtag'].str.replace(' +',' ', regex = True)
    df['Hashtag'] = df['Hashtag'].apply(lambda x: x.strip())
    #df['Hashtag'] = df['Hashtag'].str.findall('[A-Z][a-z]*').apply(' '.join)
    df['Hashtag'] = df['Hashtag'].apply(lambda x: x.lower())
    df['Hashtag'] = df['Hashtag'].str.replace(' ','_')     
    end_time = time.time()

    print('...........DATAFRME HAS BEEN LOADED AFTER = %s SECONDS',end_time - start_time,'...........')

    return df

#_______________________________________________________________

def HASHTAG_LOCATION_MATCHING(df):
    i=0
    for hashtag in df.Hashtag.tolist():
        hashtag = re.sub(r'[^a-zA-Z]+_', "", hashtag)
        requestURL = 'http://api.geonames.org/search?q='+hashtag+'&fuzzy=0.9&username=panos.kostakos'

        resp = requests.get(requestURL)
        msg = resp.content

        tree = ET.fromstring(msg)
        root = ET.parse(urllib.request.urlopen(requestURL)).getroot()
            
        item = root.find('geoname')
        if item:
           S = item.find('name').text + ', ' + item.find('countryName').text + ' [' + item.find('lat').text + ',' + item.find('lng').text + '] [id:' + item.find('geonameId').text + ']'
           df.at[i,'Loction'] = S
        print (i)
        i+=1
        

    df.to_csv('GEO LOC good.tsv',sep='\t')
#print(split_uppercase('GoodMorningYAZID'))

df = LOAD_NETHERLANDS_DATA()

HASHTAG_LOCATION_MATCHING(df)
