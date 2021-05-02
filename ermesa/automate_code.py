#!/usr/bin python3

# import packages that we will use
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
import matplotlib
plt.style.use('ggplot')
from matplotlib.pyplot import figure

#%matplotlib inline
matplotlib.rcParams['figure.figsize'] = (12,8)

pd.options.mode.chained_assignment = None


import pymongo
import pandas as pd
from pymongo import MongoClient

client = pymongo.MongoClient("mongodb+srv://mongo:mongo@cluster0.laxah.mongodb.net/mynewdb?retryWrites=true&w=majority")
db = client.ermesa_db
coll = db.meat_data
df1_mongo = pd.DataFrame(list(coll.find({}, {'_id':0})))

coll = db.meat_historic_data
df2_mongo = pd.DataFrame(list(coll.find({}, {'_id':0})))


#because we will have to transform our data is better to  copy the data to a new dataframe called df1 and df2 
df1=df1_mongo.copy(deep=True)

df2=df2_mongo.copy(deep=True)

df1=df1[(df1['Item'] =='Meat') | (df1['Item'] =='Bovine Meat') | (df1['Item'] =='Mutton & Goat Meat') | (df1['Item'] =='Pigmeat') | (df1['Item'] =='Poultry Meat') | (df1['Item'] =='Meat, Other')]
df1=df1[(df1['Element'] =='Production') | (df1['Element'] =='Import Quantity') | (df1['Element'] =='Export Quantity') | (df1['Element'] =='Food supply quantity (kg/capita/yr)')]
#df1=df1[(df1['Element'] =='Production') | (df1['Element'] =='Import Quantity') | (df1['Element'] =='Export Quantity')]

df1.shape

df2=df2[(df2['Item'] =='Meat') | (df2['Item'] =='Bovine Meat') | (df2['Item'] =='Mutton & Goat Meat') | (df2['Item'] =='Pigmeat') | (df2['Item'] =='Poultry Meat') | (df2['Item'] =='Meat, Other')]
df2=df2[(df2['Element'] =='Production') | (df2['Element'] =='Import Quantity') | (df2['Element'] =='Export Quantity') | (df2['Element'] =='Food supply quantity (kg/capita/yr)')]

df2.shape

test = df1['Unit'].isin(['1000 tonnes','kg']).all()

#search for missing data
cols = df1.columns[:] # columns
#colours = ['#000099', '#ffff00'] # specify the colours - yellow is missing. blue is not missing.
#sns.heatmap(df1[cols].isnull(), cmap=sns.color_palette(colours))
cmap = sns.cubehelix_palette(as_cmap=True, light=.9)
sns.heatmap(df1[cols].isnull(), cmap=cmap)

#for example let's see the raw 6811 that should have missing data that are hsown as NaN
df1.loc[1020:1030,:]

#search for missing data
cols = df2.columns[:] # columns
colours = ['#000099', '#ffff00'] # specify the colours - yellow is missing. blue is not missing.
cmap = sns.cubehelix_palette(as_cmap=True, light=.9)
sns.heatmap(df2[cols].isnull(), cmap=cmap)
#this historic data dataset has many more missing data

#When there are many features in the dataset, we can make a list of missing data % for each feature.
# if it's a larger dataset and the visualization takes too long can do this.
# % of missing.
for col in df1.columns:
    pct_missing = np.mean(df1[col].isnull())
    print('{} - {}%'.format(col, round(pct_missing*100)))
    
    dfm=df1
dfm['Y2014'] = dfm.groupby(['Area','Item'])[['Y2014','Area','Item']].fillna(method = 'ffill').groupby(['Area','Item'])[['Y2014','Area','Item']].fillna(method = 'bfill')
dfm['Y2015'] = dfm.groupby(['Area','Item'])[['Y2015','Area','Item']].fillna(method = 'ffill').groupby(['Area','Item'])[['Y2015','Area','Item']].fillna(method = 'bfill')
dfm['Y2016'] = dfm.groupby(['Area','Item'])[['Y2016','Area','Item']].fillna(method = 'ffill').groupby(['Area','Item'])[['Y2016','Area','Item']].fillna(method = 'bfill')
dfm['Y2017'] = dfm.groupby(['Area','Item'])[['Y2017','Area','Item']].fillna(method = 'ffill').groupby(['Area','Item'])[['Y2017','Area','Item']].fillna(method = 'bfill')
dfm['Y2018'] = dfm.groupby(['Area','Item'])[['Y2018','Area','Item']].fillna(method = 'ffill').groupby(['Area','Item'])[['Y2018','Area','Item']].fillna(method = 'bfill')

for col in dfm.columns:
    pct_missing = np.mean(dfm[col].isnull())
    print('{} - {}%'.format(col, round(pct_missing*100)))
    
dfm.head()

dfm1=df2
for col in dfm1.columns[7:]:
    
    dfm1[col]=dfm1.groupby(['Area','Item'])[col].apply(lambda x:x.fillna(0.0))

for col in dfm1.columns:
    pct_missing = np.mean(dfm1[col].isnull())

df1.loc[(df1['Area'] == 'United Kingdom of Great Britain and Northern Ireland'),'Area'] = 'United Kingdom'
a = df1.groupby('Area',sort=True).Item.nunique()>=0
a1 = df2.groupby('Area',sort=True).Item.nunique()>=0
b1=a1[a1].index.tolist()
b=a[a].index.tolist()

bb=set(b1)-set(b)
bb1=set(b)-set(b1)

a=bb1
df1 = df1[~df1['Area'].isin(a)]
df1.shape

a=bb
df2 = df2[~df2['Area'].isin(a)]
df2.shape
a=bb1
df1 = df1[~df1['Area'].isin(a)]
df1.shape

a=bb
df2 = df2[~df2['Area'].isin(a)]
df2.shape

a = df2.groupby('Item',sort=True).Item.nunique()>0
b=a[a].index.tolist()
len(b)
print(b)

a = df1.groupby('Element',sort=True).Item.nunique()>0
b=a[a].index.tolist()
len(b)
print(b)

a = df2.groupby('Element',sort=True).Item.nunique()>0
b=a[a].index.tolist()
len(b)
print(b)

# we know that column 'id' is unique, but what if we drop it?
df_dedupped = df1.drop(df1.columns[0],axis=1).drop_duplicates()
# there were duplicate rows
print(df1.shape)
print(df_dedupped.shape)

# we know that column 'id' is unique, but what if we drop it?
df_dedupped = df2.drop(df2.columns[0],axis=1).drop_duplicates()
# there were duplicate rows
print(df2.shape)
print(df_dedupped.shape)


ups1 = df1.groupby(['Area'])['Item']\
         .agg(['size', 'sum']).reset_index()

ups1

ups2 = df2.groupby(['Area'])['Item']\
         .agg(['size', 'sum']).reset_index()
ups2

f1=ups1[ups1['size']<24]
area=f1['Area']
print(list(area))

df1 = df1[~df1['Area'].isin(area)]
df1.shape

df2 = df2[~df2['Area'].isin(area)]
df2.shape

df1 = df1[~df1['Area'].isin(area)]
df1.shape

df2 = df2[~df2['Area'].isin(area)]
df2.shape

f2=ups2[ups2['size']<24]
area=f2['Area']
print(list(area))
df1 = df1[~df1['Area'].isin(area)]
df1.shape
df2 = df2[~df2['Area'].isin(area)]
df2.shape

#Finally the dataset have same lenght
a = df2.groupby('Area',sort=True).Item.nunique()>0
b=a[a].index.tolist()

print(b)

len(b)


df2.sort_values(['Area', 'Item',], ascending=[True, True])
#df2[df2["Area"]=="United Kingdom"]

final = df2.merge(df1, on=["Area","Item","Element","Area Code", "Item Code", "Element Code", "Unit"])
a = final.groupby('Area',sort=True).Item.nunique()>0
b=a[a].index.tolist()
print(b)
#save locally as check point to not have to connect again to mongodb 
final.to_csv('final.csv', index=False)  
final = pd.read_csv('final.csv')

final.shape
final.head()

final.loc[final["Element"]=="Food supply quantity (kg/capita/yr)",'Element']="Consumption"
final.loc[final["Area"]=="Czechia",'Area']="Czech Republic"
final.loc[final["Area"]=="United States of America",'Area']="United States"
final.head()
#final[final["Area"]=="Czech Republic"]

a = final.groupby('Item',sort=True).Item.nunique()>0
b=a[a].index.tolist()
len(b)
print(b)

final.drop(final.loc[final['Item']=="Meat"].index, inplace=True)
a = final.groupby('Item',sort=True).Item.nunique()>0
b=a[a].index.tolist()
len(b)
print(b)

import pandas as pd

a = final.groupby('Item',sort=True).Item.nunique()>0
b=a[a].index.tolist()
print(b)
result = pd.melt(final, id_vars=['Area Code','Area','Item Code','Item','Element Code','Element','Unit'])

result.shape

a = result.groupby('Item',sort=True).Item.nunique()>=0
b=a[a].index.tolist()
len(b)
print(b)

#replase all thhe value Y.. dropping the frst letter

result = result.rename(columns = {'variable': 'Year', 'value': 'Quantity'}, inplace = False)

result['Year'] = result['Year'].str[1:]

result.set_index(['Area','Item','Element','Area Code','Item Code','Unit'], append=True)

pivot_table_df = pd.pivot_table(
    result,
    index=['Year','Area','Area Code','Element'],
    columns='Item',
    values='Quantity',
    aggfunc='sum',
    margins=True
)

pivot_table_df

meat_data=pivot_table_df.sort_values(by=['Area',"Year","Element"], ascending=True)
df = pd.DataFrame(data=meat_data)
df.head()

df = pd.DataFrame(df.to_records())

df = df.rename(columns = {'All': 'Meat Tot', 'Meat, Other': 'Other Meat'}, inplace = False)
column_names = ["Year", "Area", "Area Code","Element", "Bovine Meat", "Pigmeat","Poultry Meat", "Mutton & Goat Meat", "Other Meat","Meat Tot"]

df = df.reindex(columns=column_names)
df = df.rename(columns = {'Area Code': 'Area_Code', 'Mutton & Goat Meat': 'Mutton_Goat','Poultry Meat': 'Poultry','Bovine Meat': 'Bovine','Other Meat': 'Other_Meat','Meat Tot': 'Meat_Tot'}, inplace = False)
df.head()


df.to_csv('cleandata.csv', index=False) 

print('data cleanining completed... data available in file cleandata.csv')

