# importing necessary libraries
import pandas as pd
import json
from sqlalchemy import create_engine
import mysql.connector
import numpy as np

print("\nFirst part: creating mysql db and inserting into it tables **Genes to down regulate** and **Genes to up "
      "regulate** for each patient.")

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

print('Printing all patient genes.\n')
for pat_name in pat.keys():
    print(f"For patient {pat_name} data is\n", pd.DataFrame.from_dict(pat[pat_name], orient='index').transpose())

# Credentials for database connection
hostname = "localhost"
dbname = "patients_db"
uname = "hb_user"
pwd = "ABI_Vivan"

print("\nWe can see that for patiend 'Pat012' there is no data for GUR. We will check this in MySQL too.")
print(f"\nFor patient Pat012 data is\n", pd.DataFrame.from_dict(pat["Pat012"], orient='index').transpose())

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

print("\nSecond part: processing 'cnv_procerssed.txt' and inserting it into a 'cnv' table of the patients_db database.")

# reading 'cnv_processed.txt' as a pandas dataframe
cnv = pd.read_csv('cnv_processed.txt', sep='\t', lineterminator='\r', low_memory=False);

# Visualization of the dataframe
cnv.head()

# printing all patient names in 'cnv_processed.txt'
print("\nPatient names present in the 'cnv_processed.txt':", cnv.Patient_ID.unique())

print("\nChecking if 'cnv_processed.txt' has empty strings in it.")
print(np.where(cnv.applymap(lambda x: x == '')))
print("It seems there is no such patient.")

print("\nChecking if there is a patient with non NaN in any row.")
nonNaNpatients = cnv.dropna().Patient_ID.unique()
print("Patient(s) with no NaN in any row:", np.ravel(nonNaNpatients))
print(f"It seems the only {np.ravel(nonNaNpatients)} has rows of data with no missing values (NaNs). "
      f"We will check this in MySQL too.")

# Inserting cnv as a table into patients_db.
cnv.to_sql("cnv", engine, index=True, if_exists='replace');
