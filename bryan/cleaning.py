# -*- coding: utf-8 -*-
"""
Created on Sat May  1 20:29:02 2021

@author: bodonohoe
"""
#!/usr/bin/env python3
import pandas as pd
import numpy as np
import pycountry
import pymongo
import dns
#import dnspython
import pycountry_convert as pc
from pymongo import MongoClient
from pprint import pprint



client = pymongo.MongoClient("mongodb+srv://bryanodonohoe:mongomongo@cluster0.laxah.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")

db = client.bryan_db1
cursor = db.fifa_player_ratings.find({})

#Set mongoDB to dataframe
df =  pd.DataFrame(list(cursor))




#Set blank club to "Unsigned"
df["Club"] = df["Club"].fillna("Unsigned")

#Set blank position to Unk
df["Position"] = df["Position"].fillna("Unk")

#Set blank releaseclause to 0
df["Release Clause"] = df["Release Clause"].fillna("0M")

#clean the position ratings
#goalies are blank so let's set these values to negative 1 so they can be identified later
#aso need to remove the "potential upside" from those ratings too

#list of position variables
positions = ["LS", "ST", "RS", "LW", "LF", "CF", "RF", "RW", "LAM", "CAM", "RAM", "LM", "LCM", "CM", "RCM", "RM", "LWB", "LDM",
             "CDM", "RDM","RWB", "LB", "LCB", "CB", "RCB", "RB", "GK"]

for value in positions:
    df[value] = df[value].fillna("-1+0")

    
attributes = ["Crossing","Finishing","Heading Accuracy","Short Passing","Volleys","Dribbling","Curve","FK Accuracy",
              "Long Passing","Ball Control","Acceleration","Sprint Speed","Agility","Reactions","Balance","Shot Power",
              "Jumping","Stamina","Strength","Long Shots","Aggression","Interceptions","Positioning","Vision","Penalties",
              "Composure","Defensive Awareness","Standing Tackle","Sliding Tackle","GK Diving","GK Handling","GK Kicking",
              "GK Positioning","GK Reflexes"]

for value in attributes:
    df[value] = df[value].fillna(-1)
    
    
 #clean height and transform to centimetres
df["height_in_cm"] = df["Height"].apply(lambda x: ((int(x.split("'")[0]))*12 + int(x.split("'")[1]))*2.54 )
   

 #weight needs to be cleaned to remove "lbs"
df["weight_in_lbs"] = df["Weight"].apply(lambda x: int(x.split("lbs")[0])) 


#Some variables saved down as special characters (e.g. weak foot is "3 â˜…" instead of 3 stars
#convert these to numeric (1-5)

df["weak_foot_cleaned"] = df["Weak Foot"].apply(lambda x: int(x.split(" ")[0]))
df["skill_moves_cleaned"] = df["Skill Moves"].apply(lambda x: int(x.split(" ")[0]))
df["int_rep_cleaned"] = df["International Reputation"].apply(lambda x: int(x.split(" ")[0]))

df["Height"].apply(lambda x: ((int(x.split("'")[0]))*12 + int(x.split("'")[1]))*2.54 )


# Release clause currently in the format of "€1.2M" or "€300K"
# Two functions created to transform this into numeric format

def release_clause1 (value):
    try:
        return value.split("€")[1]
    except:
        return value

def release_clause2 (value):
    try:
        return float(value.split("M")[0])*1000
    except:
        return float(value.split("K")[0])
    
df["release_clause_cleaned_k"] = df["Release Clause"].apply(lambda x: release_clause1(x)).apply(lambda x: release_clause2(x)) 

country_cleaning_dict = {"Republic of Ireland": "Ireland",
                    "Korea Republic": "Korea",
                    "Scotland": "United Kingdom",
                    "Wales": "United Kingdom",
                    "Northern Ireland": "United Kingdom",
                    "England": "United Kingdom",
                    "China PR": "China "}

def country_cleaning (country):
    try:
        return country_cleaning_dict[country]
    except:
        return country


df["Nationality"] = df["Nationality"].apply(lambda x: country_cleaning(x))


#Position
#Let's band for Goalkeepers, Defenders, Midfielders and Attackers

position_banding = [("CAM","M"), ("CB","D"), ("CDM","M"), ("CF","A"), ("CM","M"), ("GK","GK"), ("LAM","M"),
("LB","D"), ("LCB","D"), ("LCM","M"), ("LDM","M"), ("LF","A"), ("LM","M"), ("LS","A"), ("LW","M"), ("LWB","D"),
("RAM","M"), ("RB","D"), ("RCB","D"), ("RCM","M"), ("RDM","M"), ("RF","A"), ("RM","M"), ("RS","A"), ("RW","M"),
("RWB","D"), ("ST","A"), ("Unk", "Unk")]

high_level_position = dict(position_banding)

def position_banding (value):
    return high_level_position[value]
    
df["position_banded"] = df["Position"].apply(lambda x: position_banding(x))  













    