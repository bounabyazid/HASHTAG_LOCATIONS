#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 23 07:51:26 2018

@author: polo
"""

import re
import time
import random

import numpy as np
import editdistance

from fuzzywuzzy import fuzz

import pandas as pd

import spacy

from sklearn.metrics import accuracy_score

#_______________________________________________________________
    
def LOAD_HASHTAGS_JASON_DATA():
    
    start_time = time.time()
    #df = pd.read_csv('has.csv',delimiter=',',encoding='utf-8',names = ["ID", "Hashtags"])
    df = pd.read_csv('has.csv',delimiter=',',encoding='utf-8')

    df['Hashtags'] = df['Hashtags'].str.replace('[[]]','')
    df['Hashtags'] = df['Hashtags'].str.replace('\"\[','')
    df['Hashtags'] = df['Hashtags'].str.replace('\]\"','')
    df['Hashtags'] = df['Hashtags'].str.replace('\"','')
    df['Hashtags'] = df['Hashtags'].str.replace('\"','')
    df['Hashtags'] = df['Hashtags'].str.replace(',indices:[[0-9]+,[0-9]+]','', regex = True)
    df['Hashtags'] = df['Hashtags'].str.replace('text:','')
    df['Hashtags'] = df['Hashtags'].str.replace('[0-9]','')
    df['Hashtags'] = df['Hashtags'].str.replace('[{}]','')
    df['Hashtags'] = df['Hashtags'].str.replace(',',' ')
    #df['Hashtags'] = df['Hashtags'].str.replace('^[0-9]','')
    
    df3 = pd.DataFrame(columns = ['ID', 'Hashtag', 'FuzzyWuzzy', 'EditDist', 'Is Loc'])
    i = 0
    for Hashtag in df.Hashtags.tolist():
        if Hashtag:
           ListHahtag = [s.strip() for s in Hashtag.split(' ')]
           for H in ListHahtag:
               df3.at[len(df3)] = [i,H,'','',0]
        #print (i)
        i+=1
        
    #__________________TO KNOW THE NON ALPHA CHARS_____________________________
    #text = ' '.join(df2.Locations.tolist())
    #text = re.sub('[A-Za-z]|[0-9]|[\n\t]|[£€$%& ]','',text)

    #SymbList = list(dict.fromkeys(text).keys())
    #print(SymbList)
    #__________________________________________________________________________
    df2 = pd.read_csv('Gazetteer_nl.tsv',delimiter='\t',encoding='utf-8',names = ["Locations", "Lat", "Long"])

    df2['Locations'] = df2['Locations'].str.replace('[0-9]','')
    df2['Locations'] = df2['Locations'].str.replace('[’.(),-]',' ')
    #df2['Locations'] = df2['Locations'].str.replace('-',' ')
    
    df2['Locations'] = df2['Locations'].apply(lambda x: x.strip())
    df2['Locations'] = df2['Locations'].str.replace(' +',' ', regex = True)
    df2['Locations'] = df2['Locations'].apply(lambda x: x.lower())
    
    df3.to_csv('Indexed_HashTags.tsv',sep='\t')
    
    end_time = time.time()

    print('...........DATAFRME HAS BEEN LOADED AFTER = %s SECONDS',end_time - start_time,'...........')
    
    return df,df2,df3

#_______________________________________________________________

def split_uppercase(value):
    S = re.sub(r'([A-Z][a-z]+)', r' \1', value)
    return re.sub(r'([A-Z]+)', r' \1', S)

#_______________________________________________________________

def LOAD_NETHERLANDS_DATA():
    
    start_time = time.time()
    
    #df = pd.read_csv('SubSetHashTags500.tsv',delimiter='\t',encoding='utf-8')
    df = pd.read_csv('Indexed_HashTags.tsv',delimiter='\t',encoding='utf-8')

    df['Hashtag'] = df['Hashtag'].str.replace('[0-9]','')
    
    df['Hashtag'] = df.Hashtag.astype(str)
    df['Hashtag'] = df['Hashtag'].apply(split_uppercase)    
    df['Hashtag'] = df['Hashtag'].str.replace(' +',' ', regex = True)
    df['Hashtag'] = df['Hashtag'].apply(lambda x: x.strip())
    #df['Hashtag'] = df['Hashtag'].str.findall('[A-Z][a-z]*').apply(' '.join)
    df['Hashtag'] = df['Hashtag'].apply(lambda x: x.lower())
        
    df2 = pd.read_csv('Gazetteer_nl.tsv',delimiter='\t',encoding='utf-8',names = ["Locations", "Lat", "Long"])
    df2['Locations'] = df2['Locations'].str.replace('[0-9]','')
    df2['Locations'] = df2['Locations'].str.replace(' +',' ', regex = True)
    df2['Locations'] = df2['Locations'].str.replace('-',' ')
    df2['Locations'] = df2['Locations'].apply(lambda x: x.lower())
    
    end_time = time.time()

    print('...........DATAFRME HAS BEEN LOADED AFTER = %s SECONDS',end_time - start_time,'...........')

    return df,df2
#_______________________________________________________________
    
def LOAD_DATA():
    
    start_time = time.time()
    
    df = pd.read_csv('SubSetHashTags10000.tsv',delimiter='\t',encoding='utf-8')

    df['Hashtag'] = df['Hashtag'].str.replace('-',' ')
    df['Hashtag'] = df['Hashtag'].str.replace(' +',' ', regex = True)
    df['Hashtag'] = df['Hashtag'].apply(lambda x: x.strip())
    
    index = [i for i in range(df.shape[0])]
    random.shuffle(index)
    df.set_index([index]).sort_index()

    df2 = pd.read_csv('Gazetteer_nl.tsv',delimiter='\t',encoding='utf-8',names = ["Locations", "Lat", "Long"])
    df2['Locations'] = df2['Locations'].str.replace('[0-9]','')
    df2['Locations'] = df2['Locations'].str.replace(' +',' ', regex = True)
    df2['Locations'] = df2['Locations'].str.replace('-',' ')
    df2['Locations'] = df2['Locations'].apply(lambda x: x.lower())
    
    end_time = time.time()

    print('...........DATAFRME HAS BEEN LOADED AFTER = %s SECONDS',end_time - start_time,'...........')

    return df,df2
#_______________________________________________________________

def GenerateSubSet(N):
    df,df2 = LOAD_NETHERLANDS_DATA()
    #newDF = pd.DataFrame(columns = ['Hashtag', 'FuzzyWuzzy', 'EditDist'])
    
    hashtags = []
    IDs = []
    IsLoc = []

    for x in range(N):
        i = random.randint(1,df.shape[0])
        hashtags.append(df.at[i,'Hashtag'])
        IDs.append(df.at[i,'ID'])
        IsLoc.append('1')
    newDF = pd.DataFrame({'ID': IDs, 'Hashtag': hashtags, 'IsLoc': IsLoc, 'FuzzyWuzzy': 'NA', 'EditDist': 'NA','Loca': 'NA','Lat' : 'NA', 'Long' : 'NA'})

    newDF.to_csv('SubSetHashTags.tsv',sep='\t')
#_______________________________________________________________

def GenerateSubSet():
    df,df2 = LOAD_NETHERLANDS_DATA()
    #newDF = pd.DataFrame(columns = ['Hashtag', 'FuzzyWuzzy', 'EditDist'])
    
    hashtags = []
    IDs = []
    k = 0
    
    while k<= 5000:
        i = random.randint(1,df.shape[0])
        for loc in df2.Locations.tolist():
            FW = fuzz.ratio(df.at[i,'Hashtag'], loc)
            if FW >= 90:
               hashtags.append(df.at[i,'Hashtag'])
               IDs.append(df.at[i,'ID'])
               k+=1
               print(k)
               break
    k = 0
        
    while k<= 5000:
        i = random.randint(1,df.shape[0])
        for loc in df2.Locations.tolist():
            FW = fuzz.ratio(df.at[i,'Hashtag'], loc)
            if FW < 70:
               hashtags.append(df.at[i,'Hashtag'])
               IDs.append(df.at[i,'ID'])
               k+=1
               print(k)
               break

    newDF = pd.DataFrame({'ID': IDs, 'Hashtag': hashtags, 'IsLoc': '0', 'FuzzyWuzzy': 'NA', 'EditDist': 'NA','Loca': 'NA','Lat' : 'NA', 'Long' : 'NA', 'ED0' : '0', 'ED1' : '0', 'ED2' : '0'})
    newDF.to_csv('SubSetHashTags10000.tsv',sep='\t')
    return newDF

#_______________________________________________________________

def HASHTAG_LOCATION_MATCHING(df,df2):
    i=0
    for hashtag in df.Hashtag.tolist(): 
        #__________________________________________________
        EDsMatch = []
        EDs = []
            
        FWs_Match = []
        FWs = []
        j = 0
        for loc in df2.Locations.tolist():
        #__________________EDIT DISTANCE___________________
            ED = editdistance.eval(hashtag,loc)
            if ED <= 2:
               EDs.append(ED)
               EDsMatch.append([ED,hashtag,loc, df2.at[j,'Lat'], df2.at[j,'Long']])
        #_____________FUZZYWUZZY WITHOUT NER_______________
            FW = fuzz.ratio(hashtag, loc)
            
            if FW >= 90:
               FWs.append(FW)
               FWs_Match.append([FW,hashtag,loc, df2.at[j,'Lat'], df2.at[j,'Long']])
        #______________FUZZYWUZZY WITH NER_________________                                
        j+= 1
        #__________________________________________________
        if len(EDs)>0:
           #index_min = np.argmin(EDs)
           df.at[i,'EditDist'] = 1#min(EDs)
        else:
            df.at[i,'EditDist'] = 0
        #__________________________________________________
        if len(FWs)>0:
           #index_max = np.argmax(FWs)
           df.at[i,'FuzzyWuzzy'] = 1#max(FWs)
        else:
            df.at[i,'FuzzyWuzzy'] = 0
        i+=1
        

    df.to_csv('GEO LOC good.tsv',sep='\t')

#________________________-__ACCUARACY___________________________

def Accuracy(df):
    y_true = np.asarray(df.IsLoc.tolist())

    y_EditDist = np.asarray(df.EditDist.tolist())
    acc_EditDist = accuracy_score(y_true, y_EditDist)

    y_FuzzyWuzzy = np.asarray(df.FuzzyWuzzy.tolist())
    acc_FuzzyWuzzy = accuracy_score(y_true, y_FuzzyWuzzy)

    l = list(df.IsLoc.tolist())
    T = l.count(1)
    F = l.count(0)

    l = list(df.EditDist.tolist())
    EDTP = l.count(1)
    EDFP = l.count(0)

    l = list(df.FuzzyWuzzy.tolist())
    FWDTP = l.count(1)
    FWDFP = l.count(0)

    print('True = ',T,' False = ',F)
    print('TP ED = ',EDTP,' FP ED = ',EDFP)
    print('TP FW = ',FWDTP,' FP FW = ',FWDFP)

    print ('acc_EditDist = ',acc_EditDist)
    print ('acc_FuzzyWuzzy = ',acc_FuzzyWuzzy)    

#_______________________________________________________________

#GenerateSubSet(500)
#df = GenerateSubSet()
#df,df2 = LOAD_NETHERLANDS_DATA()
    
df,df2 = LOAD_DATA()
start_time = time.time()
HASHTAG_LOCATION_MATCHING(df,df2)
Accuracy(df)
end_time = time.time()


print('...........TIME NEEDED TO RUN = %s SECONDS',end_time - start_time,'...........')