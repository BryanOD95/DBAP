#!/usr/bin/env python
# coding: utf-8

# # Import Packages

# In[15]:


import pandas as pd
import numpy as np
import pycountry
import pymongo
import dns
#import dnspython
import pycountry_convert as pc
from pymongo import MongoClient
from pprint import pprint


# In[16]:


#pip install dnspython


# ## Import Data

# In[17]:


client = pymongo.MongoClient("mongodb+srv://bryanodonohoe:mongomongo@cluster0.laxah.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")

db = client.bryan_db1
cursor = db.fifa_player_ratings.find({})

#Set mongoDB to dataframe
df =  pd.DataFrame(list(cursor))
df


# ## Data Cleaning

# In[18]:


##get summary of NaN
for col in df.columns:
    print(col, ": ", df[col].isnull().sum())


# In[49]:


#1802 missing is 99% goalkeepers, rest is past players (e.g. David Beckham)


# In[50]:


df.describe()


# In[20]:


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


# In[21]:


df["LS"]


# In[22]:


#clean height and transform to centimetres
df["height_in_cm"] = df["Height"].apply(lambda x: ((int(x.split("'")[0]))*12 + int(x.split("'")[1]))*2.54 )
df[["Height", "height_in_cm"]].head()


# In[23]:


#weight needs to be cleaned to remove "lbs"
df["weight_in_lbs"] = df["Weight"].apply(lambda x: int(x.split("lbs")[0])) 


# In[24]:


#lets try and impute body type from height and weight
#9 useful body types that can be calculated from height and weight (roughly)
# Lean (170-)
# Lean (170-185)
# Lean (185+)
# Normal (170-)
# Normal (170-185)
# Normal (185+)
# Stocky (170-)
# Stocky (170-185)
# Stocky (185+)


#first get height band
bands =  ["170_minus","170_185","185_plus"]
bins_height = [0, 170, 185]
height_band = dict(enumerate(bands,1))
df["height_band"] = np.vectorize(height_band.get)(np.digitize(df["height_in_cm"],bins_height))



bodytype = df.loc[df['Body Type'].isin(["Lean (170-)", "Lean (170-185)", "Lean (185+)", "Normal (170-)", "Normal (170-185)",
                                        "Normal (185+)", "Stocky (170-)", "Stocky (170-185)", "Stocky (185+)"])]

avgweight = bodytype[['Body Type', "weight_in_lbs", "height_band"]]
avgweight_summary = avgweight.groupby(['Body Type'])["weight_in_lbs"].mean().reset_index()
#income_summary.columns = ['ConsumerID', "reported_month", "reported_year","avg_inc"]
avgweight_summary["height"] = avgweight_summary.apply(lambda x: "170_minus" if x["Body Type"].split("(")[1] == "170-)"  else "170_185" if x["Body Type"].split("(")[1] == "170-185)" else "185_plus", axis = 1)

avgweight_summary


# In[25]:


#df = df.merge(avgweight_summary[["Body Type",'weight_in_lbs']], on='weight_in_lbs', how = "left")
df


# In[48]:


# avgweight_summary['height'] = df['Body Type'].map(body_dict)

# avgweight_summary


# In[28]:


#Some variables saved down as special characters (e.g. weak foot is "3 â˜…" instead of 3 stars
#convert these to numeric (1-5)

df["weak_foot_cleaned"] = df["Weak Foot"].apply(lambda x: int(x.split(" ")[0]))
df["skill_moves_cleaned"] = df["Skill Moves"].apply(lambda x: int(x.split(" ")[0]))
df["int_rep_cleaned"] = df["International Reputation"].apply(lambda x: int(x.split(" ")[0]))
df


# In[29]:


df["Height"].apply(lambda x: ((int(x.split("'")[0]))*12 + int(x.split("'")[1]))*2.54 )


# In[54]:


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


# In[31]:


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
df["Nationality"]


# In[ ]:


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


# In[32]:


#df["Nationality"].to_csv('country.csv') 


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# ## Feature Engineering 

# In[33]:


#continent
data = df

# sorting by Nationality
data.sort_values("Nationality", inplace = True)
  
# dropping ALL duplicate values
data = data.drop_duplicates(subset =["Nationality"])


#return list of exceptions where country doesn't correctly map to continent

for i in data["Nationality"]:
    try:
        pc.country_alpha2_to_continent_code(pc.country_name_to_country_alpha2(i, cn_name_format="default"))
    except:
        print(i)


# In[34]:


#no match for 17 countries, so need to be manually adjusted
unmatched_countires = [
("Antigua & Barbuda", "NA"),
("Bosnia Herzegovina", "EU"),
("China", "AS"),
("Chinese Taipei", "AS"),
("Curacao", "SA"),
("DR Congo", "AF"),
("England", "EU"),
("Guinea Bissau", "AF"),
("Korea DPR", "AS"),
("Korea Republic", "AS"),
("Kosovo", "EU"),
("Northern Ireland", "EU"),
("Republic of Ireland", "EU"),
("Scotland", "EU"),
("São Tomé & Príncipe", "AF"),
("Trinidad & Tobago", "SA"),
("Wales", "EU")]

# Convert list of tuple to dictionary
unmatched_dict = dict(unmatched_countires)
unmatched_dict


# In[35]:


# def country_to_cont (value):
#     try:
#         return pc.country_alpha2_to_continent_code(pc.country_name_to_country_alpha2(value, 
#                                                                     cn_name_format="default"))
#     except:
#         return unmatched_dict[value]

# df["continent"] = df["Nationality"].apply(lambda x: country_to_cont(x))   

# df[["continent", "Nationality"]].head()


# In[36]:





# ## Cleaned data to MongoDB 

# In[56]:


#Send to MongoDB
client = pymongo.MongoClient("mongodb+srv://bryanodonohoe:mongomongo@cluster0.laxah.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
 

db = client.bryan_db1
print(db)
coll = db.cleaned_fifa_details

coll.insert_many(df.apply(lambda x: x.to_dict(), axis=1).to_list())


# In[37]:


#df.to_csv('dataframe.csv')  


# ## Summary for Merging 

# In[38]:


#create dataframe with policy level identifier and country code
country_count = df[['Nationality']]

country_summary = country_count.value_counts().reset_index()
country_summary.columns = ['country', "count"]
country_summary.set_index('country')

df.groupby('Nationality').agg(pd.Series.mode)


# In[39]:


FIFA_count = df.groupby('Nationality').count()

#FIFA_count["count"] =  FIFA_count["ID"]
#FIFA_count2 = FIFA_count[['Nationality', "count"]]

#FIFA_count2 = FIFA_count[['Nationality', "ID"]]
FIFA_count2 = pd.DataFrame()
FIFA_count2["count"] = FIFA_count["ID"]

FIFA_count2


# In[40]:


FIFA_mean = df[["Age","weak_foot_cleaned","skill_moves_cleaned","release_clause_cleaned_k","Potential",
                "Overall","Nationality","int_rep_cleaned","height_in_cm","weight_in_lbs"]].groupby('Nationality').mean().round()

FIFA_mean_pos = df[["Nationality","Crossing","Finishing","Heading Accuracy","Short Passing","Volleys","Dribbling",
                    "Curve","FK Accuracy","Long Passing","Ball Control","Acceleration","Sprint Speed","Agility",
                    "Reactions","Balance","Shot Power","Jumping","Stamina","Strength","Long Shots","Aggression",
                    "Interceptions","Positioning","Vision","Penalties","Composure","Defensive Awareness",
                    "Standing Tackle","Sliding Tackle"]].loc[df['position_banded'] != "GK"].groupby('Nationality').mean().round()

FIFA_mean_gk = df[["Nationality","GK Diving","GK Handling","GK Kicking",
                   "GK Positioning","GK Reflexes"]].loc[df['position_banded'] == "GK"].groupby('Nationality').mean().round()


# In[41]:



# def label_variables(dataset, description):
            
#     for col in dataset.columns:
#         dataset.rename(columns={col:col.lower() + "_" +description},inplace=True)

# label_variables(FIFA_mean, "mean")


# In[42]:


FIFA_std = df[["Age","weak_foot_cleaned","skill_moves_cleaned","release_clause_cleaned_k","Potential",
                "Overall","Nationality","int_rep_cleaned","height_in_cm","weight_in_lbs"]].groupby('Nationality').std().round()

FIFA_std_pos = df[["Nationality","Crossing","Finishing","Heading Accuracy","Short Passing","Volleys","Dribbling",
                    "Curve","FK Accuracy","Long Passing","Ball Control","Acceleration","Sprint Speed","Agility",
                    "Reactions","Balance","Shot Power","Jumping","Stamina","Strength","Long Shots","Aggression",
                    "Interceptions","Positioning","Vision","Penalties","Composure","Defensive Awareness",
                    "Standing Tackle","Sliding Tackle"]].loc[df['position_banded'] != "GK"].groupby('Nationality').std().round()

FIFA_std_gk = df[["Nationality","GK Diving","GK Handling","GK Kicking",
                   "GK Positioning","GK Reflexes"]].loc[df['position_banded'] == "GK"].groupby('Nationality').std().round()


# In[43]:


FIFA_quantile_low = df[["Age","weak_foot_cleaned","skill_moves_cleaned","release_clause_cleaned_k","Potential",
                "Overall","Nationality","int_rep_cleaned","height_in_cm","weight_in_lbs"]].groupby('Nationality').quantile(0.25).round()

FIFA_quantile_low_pos = df[["Nationality","Crossing","Finishing","Heading Accuracy","Short Passing","Volleys","Dribbling",
                    "Curve","FK Accuracy","Long Passing","Ball Control","Acceleration","Sprint Speed","Agility",
                    "Reactions","Balance","Shot Power","Jumping","Stamina","Strength","Long Shots","Aggression",
                    "Interceptions","Positioning","Vision","Penalties","Composure","Defensive Awareness",
                    "Standing Tackle","Sliding Tackle"]].loc[df['position_banded'] != "GK"].groupby('Nationality').quantile(0.25).round()

FIFA_quantile_low_gk = df[["Nationality","GK Diving","GK Handling","GK Kicking",
                   "GK Positioning","GK Reflexes"]].loc[df['position_banded'] == "GK"].groupby('Nationality').quantile(0.25).round()


FIFA_quantile_high = df[["Age","weak_foot_cleaned","skill_moves_cleaned","release_clause_cleaned_k","Potential",
                "Overall","Nationality","int_rep_cleaned","height_in_cm","weight_in_lbs"]].groupby('Nationality').quantile(0.75).round()

FIFA_quantile_high_pos = df[["Nationality","Crossing","Finishing","Heading Accuracy","Short Passing","Volleys","Dribbling",
                    "Curve","FK Accuracy","Long Passing","Ball Control","Acceleration","Sprint Speed","Agility",
                    "Reactions","Balance","Shot Power","Jumping","Stamina","Strength","Long Shots","Aggression",
                    "Interceptions","Positioning","Vision","Penalties","Composure","Defensive Awareness",
                    "Standing Tackle","Sliding Tackle"]].loc[df['position_banded'] != "GK"].groupby('Nationality').quantile(0.75).round()

FIFA_quantile_high_gk = df[["Nationality","GK Diving","GK Handling","GK Kicking",
                   "GK Positioning","GK Reflexes"]].loc[df['position_banded'] == "GK"].groupby('Nationality').quantile(0.75).round()


# In[44]:


def label_variables(dataset, description):            
    for col in dataset.columns:
        dataset.rename(columns={col:col.lower() + "_" +description},inplace=True)

label_variables(FIFA_mean, "mean")
label_variables(FIFA_mean_pos, "mean")
label_variables(FIFA_mean_gk, "mean")

label_variables(FIFA_std, "std_dev")
label_variables(FIFA_std_pos, "std_dev")
label_variables(FIFA_std_gk, "std_dev")

label_variables(FIFA_quantile_low, "qnt_low")
label_variables(FIFA_quantile_low_pos, "qnt_low")
label_variables(FIFA_quantile_low_gk, "qnt_low")

label_variables(FIFA_quantile_high, "qnt_high")
label_variables(FIFA_quantile_high_pos, "qnt_high")
label_variables(FIFA_quantile_high_gk, "qnt_high")


# In[45]:


mergeddataframe = pd.merge(FIFA_count2,
                 FIFA_mean,
                 on='Nationality', 
                 how='left')

def merging(dataset):
    global mergeddataframe
    mergeddataframe = pd.merge(mergeddataframe,
                 dataset,
                 on='Nationality', 
                 how='left')



merging(FIFA_mean_pos)
merging(FIFA_mean_gk)
merging(FIFA_std)
merging(FIFA_std_pos)
merging(FIFA_std_gk)
merging(FIFA_quantile_low)
merging(FIFA_quantile_low_pos)
merging(FIFA_quantile_low_gk)
merging(FIFA_quantile_high)
merging(FIFA_quantile_high_pos)
merging(FIFA_quantile_high_gk)

mergeddataframe


# In[ ]:


countries_to_keep = ["Austria", "Australia", "Belgium", "Canada", "Chile", "Colombia", "Czech Republic", "Denmark", "Estonia",
                     "Finland", "France", "Germany", "Greece", "Hungary", "Iceland", "Ireland", "Israel", "Italy", "Japan", 
                     "Korea", "Latvia", "Lithuania", "Luxembourg", "Mexico", "Netherlands", "New Zealand", "Norway", "Poland", 
                     "Portugal", "Slovakia", "Slovenia", "Spain", "Sweden", "Switzerland", "Turkey", "United Kingdom", 
                     "United States", "China ", "India"]

mergeddataframe = mergeddataframe.loc[["Austria", "Australia", "Belgium", "Canada", "Chile", "Colombia", "Czech Republic", 
                                       "Denmark", "Estonia", "Finland", "France", "Germany", "Greece", "Hungary", "Iceland", 
                                       "Ireland", "Israel", "Italy", "Japan", "Korea", "Latvia", "Lithuania", "Luxembourg", 
                                       "Mexico", "Netherlands", "New Zealand", "Norway", "Poland", "Portugal", "Slovakia", 
                                       "Slovenia", "Spain", "Sweden", "Switzerland", "Turkey", "United Kingdom",
                                       "United States", "China ", "India"]]


mergeddataframe = mergeddataframe.fillna(-1)
    
mergeddataframe = mergeddataframe[["count", "potential_mean", "weight_in_lbs_mean",
                                                  "weight_in_lbs_std_dev", "weight_in_lbs_qnt_low", "weight_in_lbs_qnt_high"]]

mergeddataframe


# In[46]:


mergeddataframe.to_csv('merged.csv') 



# In[414]:


from sqlalchemy import create_engine
import psycopg2


alchemyEngine  = create_engine('postgresql+psycopg2://bryan:bryan123@postgresql-28021-0.cloudclusters.net:28035/dap', pool_recycle=3600);
postgreSQLConnection    = alchemyEngine.connect();
postgreSQLTable         = "FIFA_summary";
try:
    frame = mergeddataframe.to_sql(postgreSQLTable, postgreSQLConnection, if_exists='fail');
except ValueError as vx:
    print(vx)
except Exception as ex:  
    print(ex)
else:

    print(f"PostgreSQL Table {postgreSQLTable}  has been created successfully");
finally:
    postgreSQLConnection.close();


# In[47]:


from sqlalchemy import create_engine
import psycopg2


alchemyEngine  = create_engine('postgresql+psycopg2://bryan:bryan123@postgresql-28021-0.cloudclusters.net:28035/dap', pool_recycle=3600);
postgreSQLConnection    = alchemyEngine.connect();
postgreSQLTable         = "FIFA_ALL";
try:
    frame = mergeddataframe.to_sql(postgreSQLTable, postgreSQLConnection, if_exists='fail');
except ValueError as vx:
    print(vx)
except Exception as ex:  
    print(ex)
else:

    print(f"PostgreSQL Table {postgreSQLTable}  has been created successfully");
finally:
    postgreSQLConnection.close();


# In[ ]:





# In[ ]:





# ## Clustering 

# In[11]:


import matplotlib.pyplot as plt
from kneed import KneeLocator
from sklearn.datasets import make_blobs
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler


# In[10]:


pip install kneed


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[4]:


import pandas as pd
import geopandas as gpd

shapefile = 'ne_110m_admin_0_countries.shp'
#datafile = 'data/obesity.csv'

gdf = gpd.read_file(shapefile)[['ADMIN', 'ADM0_A3', 'geometry']]
gdf.columns = ['country', 'country_code', 'geometry']
gdf.head()


# In[8]:


conda install -c conda-forge geopandas


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[198]:


FIFA_std = df.groupby('Nationality').std()


# In[204]:


FIFA_quantile_low = df.groupby('Nationality').quantile(0.25)
FIFA_quantile_high = df.groupby('Nationality').quantile(0.75)


# In[205]:


FIFA_quantile_high


# In[187]:


country_summary


# In[192]:


from sqlalchemy import create_engine
import psycopg2


alchemyEngine  = create_engine('postgresql+psycopg2://bryan:bryan123@postgresql-28021-0.cloudclusters.net:28035/dap', pool_recycle=3600);
postgreSQLConnection    = alchemyEngine.connect();
postgreSQLTable         = "FIFA_count";
try:
    frame = country_summary.to_sql(postgreSQLTable, postgreSQLConnection, if_exists='fail');
except ValueError as vx:
    print(vx)
except Exception as ex:  
    print(ex)
else:

    print(f"PostgreSQL Table {postgreSQLTable}  has been created successfully");
finally:
    postgreSQLConnection.close();


# In[195]:


alchemyEngine  = create_engine('postgresql+psycopg2://bryan:bryan123@postgresql-28021-0.cloudclusters.net:28035/dap', pool_recycle=3600);
postgreSQLConnection    = alchemyEngine.connect();
postgreSQLTable         = "FIFA_mean";
try:
    frame = FIFA_mean.to_sql(postgreSQLTable, postgreSQLConnection, if_exists='fail');
except ValueError as vx:
    print(vx)
except Exception as ex:  
    print(ex)
else:

    print(f"PostgreSQL Table {postgreSQLTable}  has been created successfully");
finally:
    postgreSQLConnection.close();


# In[199]:


alchemyEngine  = create_engine('postgresql+psycopg2://bryan:bryan123@postgresql-28021-0.cloudclusters.net:28035/dap', pool_recycle=3600);
postgreSQLConnection    = alchemyEngine.connect();
postgreSQLTable         = "FIFA_std";
try:
    frame = FIFA_std.to_sql(postgreSQLTable, postgreSQLConnection, if_exists='fail');
except ValueError as vx:
    print(vx)
except Exception as ex:  
    print(ex)
else:

    print(f"PostgreSQL Table {postgreSQLTable}  has been created successfully");
finally:
    postgreSQLConnection.close();


# In[206]:



alchemyEngine  = create_engine('postgresql+psycopg2://bryan:bryan123@postgresql-28021-0.cloudclusters.net:28035/dap', pool_recycle=3600);
postgreSQLConnection    = alchemyEngine.connect();
postgreSQLTable         = "FIFA_quantile_low";
try:
    frame = FIFA_quantile_low.to_sql(postgreSQLTable, postgreSQLConnection, if_exists='fail');
except ValueError as vx:
    print(vx)
except Exception as ex:  
    print(ex)
else:

    print(f"PostgreSQL Table {postgreSQLTable}  has been created successfully");
finally:
    postgreSQLConnection.close();
    


    


# In[208]:


alchemyEngine  = create_engine('postgresql+psycopg2://bryan:bryan123@postgresql-28021-0.cloudclusters.net:28035/dap', pool_recycle=3600);
postgreSQLConnection    = alchemyEngine.connect();
postgreSQLTable         = "FIFA_quantile_high";
try:
    frame = FIFA_quantile_high.to_sql(postgreSQLTable, postgreSQLConnection, if_exists='fail');
except ValueError as vx:
    print(vx)
except Exception as ex:  
    print(ex)
else:

    print(f"PostgreSQL Table {postgreSQLTable}  has been created successfully");
finally:
    postgreSQLConnection.close();


# In[193]:


#read from DB

alchemyEngine  = create_engine('postgresql+psycopg2://bryan:bryan123@postgresql-28021-0.cloudclusters.net:28035/dap', pool_recycle=3600);
postgreSQLConnection    = alchemyEngine.connect();
postgreSQLTable         = "FIFA_count";
# Read data from PostgreSQL database table and load into a DataFrame instance

#dataFrame       = pds.read_sql("select \"Country\"  from \"Covid_BMI\" ", postgreSQLConnection);
dataFrame       = pd.read_sql("select *  from \"FIFA_count\" ", postgreSQLConnection);
print(dataFrame);


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[134]:


df


# In[42]:


test = df

test.loc[(test.continent == "OC")]


# In[ ]:





# In[20]:


#for i in df["Nationality"]:
try:
    df["continent"] = pc.country_alpha2_to_continent_code(pc.country_name_to_country_alpha2(df["Nationality"], 
                                                                cn_name_format="default"))
except:
    df["continent"] = df["Nationality"]


# In[26]:


for i in df["Nationality"]:
    try:
        print(pc.country_alpha2_to_continent_code(pc.country_name_to_country_alpha2(i, 
                                                                    cn_name_format="default")))
    except:
        print("exception")


# In[29]:


def country_to_cont (value):
    try:
        return pc.country_alpha2_to_continent_code(pc.country_name_to_country_alpha2(value, 
                                                                    cn_name_format="default"))
    except:
        return value


# In[30]:


df["continent"] = df["Nationality"].apply(lambda x: country_to_cont(x))


# In[31]:


df


# In[ ]:





# In[28]:


df["continent"] = df["Nationality"].apply(lambda x: pc.country_alpha2_to_continent_code(pc.country_name_to_country_alpha2(x, 
                                                                    cn_name_format="default")))


# In[57]:


# Manual Correction for the following
# Antigua & Barbuda: NA
# Bosnia Herzegovina: EU
# China PR: AS
# Chinese Taipei: AS
# Curacao: SA
# DR Congo: AF
# England: EU
# Guinea Bissau: AF
# Korea DPR: AS
# Korea Republic: AS
# Kosovo: EU
# Northern Ireland: EU
# Republic of Ireland: EU
# Scotland: EU
# São Tomé & Príncipe: AF
# Trinidad & Tobago: SA
# Wales: EU


# In[ ]:


Antigua & Barbuda
Bosnia Herzegovina
China PR
Chinese Taipei
Curacao
DR Congo
England
Guinea Bissau
Korea DPR
Korea Republic
Kosovo
Northern Ireland
Republic of Ireland
Scotland
São Tomé & Príncipe
Trinidad & Tobago
Wales

