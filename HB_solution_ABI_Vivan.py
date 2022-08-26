# importing necessary libraries
import pandas as pd
import json
from sqlalchemy import create_engine
import mysql.connector

# First part: creating mysql db and inserting into it tables "Genes to down regulate" and Genes to up regulate
# for each patient

# importing benchling_entries.json to python
with open('benchling_entries.json', 'r') as f:
    bench_entries = json.load(f)

pat = {} # dictionary where I am going to put all the data

for patients in bench_entries['entries']: # looping over all patients
    genes = {} # for each patient a separate dictionary is create with keys "GDR" - "Genes to down regulate"
    # and "GUR" - "Genes to up regulate"
    name = patients['fields']['a. Patient ID']['displayValue'] # selecting the name of a patient
    days = patients['days'] # selecting 'days' list
    for d in days: # looping over all days
        for note in d['notes']: # looping over all notes
            if 'table' in note and (note['table']['name'] == 'Genes to down regulate'): # selecting GDR table
                gen_list = [] # genes list
                if 'Hs' in note['table']['columnLabels'][0]: # checking if a table has a columns with 'Hs' in its name
                    for row in note['table']['rows']: # looping over all rows corresponding to 'Hs' gene
                        data = row['cells'][0]['text'].replace(" ", "") # getting the name of the gene
                        # (additionally, replacing any space in it)
                        if 'Hs-' in data: # appending the value under 'text' key only if it contains 'Hs-'
                            gen_list.append(data)
                if 'Dm' in note['table']['columnLabels'][1]: # checking if a table has a columns with 'Dm' in its name
                    for row in note['table']['rows']:  # looping over all rows corresponding to 'Dm' gene
                        data = row['cells'][1]['text'].replace(" ", "") # getting the name of the gene
                        # (additionally, replacing any space in it)
                        if 'Dm-' in data: # appending the value under 'text' key only if it contains 'Dm-'
                            gen_list.append(data)
                genes['GDR'] = gen_list # putting gen_list under 'GDR' key
            if 'table' in note and (note['table']['name'] == 'Genes to up regulate'): # selecting GUP table
                gen_list = [] # genes list
                if 'Hs' in note['table']['columnLabels'][0]: # checking if a table has a columns with 'Hs' in its name
                    for row in note['table']['rows']: # looping over all rows corresponding to 'Hs' gene
                        data = row['cells'][0]['text'].replace(" ", "") # getting the name of the gene
                        # (additionally, replacing any space in it)
                        if 'Hs-' in data: # appending the value under 'text' key only if it contains 'Hs-'
                            gen_list.append(data)
                if 'Dm' in note['table']['columnLabels'][1]: # checking if a table has a columns with 'Dm' in its name
                    for row in note['table']['rows']: # looping over all rows corresponding to 'Dm' gene
                        data = row['cells'][1]['text'].replace(" ", "") # getting the name of the gene
                        # (additionally, replacing any space in it)
                        if 'Dm-' in data: # appending the value under 'text' key only if it contains 'Dm-'
                            gen_list.append(data)
                genes['GUR'] = gen_list
    pat[name] = genes # putting genes dictionary under the name of a particular patient

# printing all patient genes
for pat_name in pat.keys():
    print(f"For patient {pat_name} data is\n", pd.DataFrame.from_dict(pat[pat_name], orient='index').transpose())

# Credentials to database connection
hostname = "localhost"
dbname = "patients_db"
uname = "hb_user"
pwd = "ABI_Vivan"


# creating an MySQL database 

patients_db = mysql.connector.connect(
  host=hostname,
  user=uname,
  password=pwd
)

mycursor = patients_db.cursor()

mycursor.execute("DROP DATABASE patients_db")
mycursor.execute("CREATE DATABASE patients_db")

# Create SQLAlchemy engine to connect to MySQL Database
engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=hostname, db=dbname, user=uname, pw=pwd))

# Inserting all patient tables into patients_db
for k in pat.keys():
    df = pd.DataFrame.from_dict(pat[k], orient='index').transpose()
    df.to_sql(k.lower(), engine, index=False, if_exists='replace')

