# importing necessary libraries
import pandas as pd
import json
from sqlalchemy import create_engine
import mysql.connector
import numpy as np

########################################################################################################################
print("\nFirst part: creating mysql db and inserting into it tables **Genes to down regulate** and **Genes to up "
      "regulate** for each patient.")

# importing benchling_entries.json to python
with open('benchling_entries.json', 'r') as f:
    bench_entries = json.load(f)

# dictionary where I am going to put all the data
pat = {}

for patients in bench_entries['entries']:  # looping over all patients
    genes = {}  # for each patient a separate dictionary is created with keys "GDR" - "Genes to down regulate"
    # and "GUR" - "Genes to up regulate"
    name = patients['fields']['a. Patient ID']['displayValue']  # selecting the name of a patient
    days = patients['days']  # selecting 'days' list
    for d in days:  # looping over all days
        for note in d['notes']:  # looping over all notes
            if 'table' in note and (note['table']['name'] == 'Genes to down regulate'):  # selecting GDR table
                gen_list = []  # genes list
                if 'Hs' in note['table']['columnLabels'][0]:  # checking if a table has a columns with 'Hs' in its name
                    for row in note['table']['rows']:  # looping over all rows corresponding to 'Hs' gene
                        data = row['cells'][0]['text'].replace(" ", "")  # getting the name of the gene
                        # (additionally, replacing any space in it)
                        if 'Hs-' in data:  # appending the value under 'text' key only if it contains 'Hs-'
                            gen_list.append(data)
                if 'Dm' in note['table']['columnLabels'][1]:  # checking if a table has a columns with 'Dm' in its name
                    for row in note['table']['rows']:  # looping over all rows corresponding to 'Dm' gene
                        data = row['cells'][1]['text'].replace(" ", "")  # getting the name of the gene
                        # (additionally, replacing any space in it)
                        if 'Dm-' in data:  # appending the value under 'text' key only if it contains 'Dm-'
                            gen_list.append(data)
                genes['GDR'] = gen_list  # putting gen_list under 'GDR' key
            if 'table' in note and (note['table']['name'] == 'Genes to up regulate'):  # selecting GUP table
                gen_list = []  # genes list
                if 'Hs' in note['table']['columnLabels'][0]:  # checking if a table has a columns with 'Hs' in its name
                    for row in note['table']['rows']:  # looping over all rows corresponding to 'Hs' gene
                        data = row['cells'][0]['text'].replace(" ", "")  # getting the name of the gene
                        # (additionally, replacing any space in it)
                        if 'Hs-' in data:  # appending the value under 'text' key only if it contains 'Hs-'
                            gen_list.append(data)
                if 'Dm' in note['table']['columnLabels'][1]:  # checking if a table has a columns with 'Dm' in its name
                    for row in note['table']['rows']:  # looping over all rows corresponding to 'Dm' gene
                        data = row['cells'][1]['text'].replace(" ", "")  # getting the name of the gene
                        # (additionally, replacing any space in it)
                        if 'Dm-' in data:  # appending the value under 'text' key only if it contains 'Dm-'
                            gen_list.append(data)
                genes['GUR'] = gen_list
    pat[name] = genes  # putting genes dictionary under the name of a particular patient

print('Printing all patient genes.\n')
for pat_name in pat.keys():
    print(f"For patient {pat_name} data is\n", pd.DataFrame.from_dict(pat[pat_name], orient='index').transpose())

print("\nWe can see that for patient 'Pat012' there is no data for GUR. We will check this in MySQL too.")
print(f"\nFor patient Pat012 data is\n", pd.DataFrame.from_dict(pat["Pat012"], orient='index').transpose())

# Credentials for database connection
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
mycursor.execute("DROP DATABASE IF EXISTS patients_db")
mycursor.execute("CREATE DATABASE patients_db")

# Create SQLAlchemy engine to connect to MySQL Database
engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=hostname, db=dbname, user=uname, pw=pwd))

# Inserting all patient tables into patients_db
for k in pat.keys():
    df = pd.DataFrame.from_dict(pat[k], orient='index').transpose()
    df.to_sql(k.lower(), engine, index=False, if_exists='replace')

########################################################################################################################

print("\nSecond part: processing 'cnv_procerssed.txt' and inserting it into a 'cnv' table of the patients_db database.")

# reading 'cnv_processed.txt' as a pandas dataframe
cnv = pd.read_csv('cnv_processed.txt', sep='\t', lineterminator='\r', low_memory=False)

# Visualization of the dataframe
cnv.head()

# printing all patient names in 'cnv_processed.txt'
print("\nPatient names present in the 'cnv_processed.txt':", cnv.Patient_ID.unique())
print("\nChecking if 'cnv_processed.txt' has empty strings in it.")
print(np.where(cnv.applymap(lambda x: x == '')))
print("It seems there is no such patient.")

# Inserting cnv as a table into patients_db.
cnv.to_sql("cnv", engine, index=True, if_exists='replace')

########################################################################################################################
# connecting to patients_db
cn = mysql.connector.connect(user=uname, password=pwd, host=hostname, database='patients_db')
cursor = cn.cursor()
cursor.execute("Show tables")
tables = cursor.fetchall()

print("\nQUESTION (a): Number of patients in Benchling with information for genes to up/down regulate.")
print("ANSWER (a): Patients")  # we will also create a dataframe with patient names with information in Benchling and
# insert a table "pat_name_bench" from that dataframe into patients_db
nonnullpat_bench = []  # a list for patient names with information in Benchling
counter = 0  # to count number of patients
for table in tables[1:]:  # looping over all tables except cnv
    cursor.execute(f"SELECT * FROM {table[0]} WHERE GDR IS NOT NULL AND GUR IS NOT NULL;")
    rows = cursor.fetchall()
    if len(rows) != 0:  # if there is no row with non-NULL values than rows = 0
        nnpat = table[0].capitalize()
        print(nnpat, sep='')
        nonnullpat_bench.append(nnpat)
        counter += 1
print("have data both for genes to up regulate and genes to down regulate.")
print("The total number of patients in Benchling with information for genes to "
      "up/down regulate is ", counter, ".", sep="")

df_patname_bench = pd.DataFrame.from_dict(dict(zip(range(len(nonnullpat_bench)), nonnullpat_bench)), orient='index',
                                          columns=["pat_name"])
df_patname_bench.to_sql("pat_name_bench", engine, index=True, if_exists='replace')
cn.commit()

########################################################################################################################

print("\nQUESTION (b): Number of patients with information for copy number variation.")
# we will also create a dataframe with patient names with information in cnv and
# insert a table "pat_name_cnv" from that dataframe into the database patients_db
pat_cnv = []  # a list for patient names
cursor.execute(f"SELECT DISTINCT Patient_ID FROM cnv;")
rows = cursor.fetchall()
print(f"There are {len(rows)} patients with information for copy number variation.\n "
      f"Those patients are:")
for patname in rows:
    print(patname[0])
    pat_cnv.append(patname[0])
df_patname_cnv = pd.DataFrame.from_dict(dict(zip(range(len(pat_cnv)), pat_cnv)), orient='index',
                                        columns=["pat_name"])
df_patname_cnv.to_sql("pat_name_cnv", engine, index=True, if_exists='replace')
cn.commit()  # updating the database

########################################################################################################################

print("\nQuestion (c): Identify which patients have both information.")
# For this we would compare "pat_name" columns in "pat_name_cnv" and "pat_name_bench" tables
cursor.execute("SELECT pat_name_bench.pat_name as pat_name_bench, pat_name_cnv.pat_name as pat_name_cnv "
               "FROM pat_name_bench, pat_name_cnv WHERE pat_name_bench.pat_name = pat_name_cnv.pat_name;")
rows_bench = cursor.fetchall()
print(rows_bench[0][0], rows[1][0])

########################################################################################################################

# For this I created 2 tables for Pat007 and Pat027 that contain all Hs genes
print("\nQuestion (d): For each patient found in “c” list the genes present in Benchling and having a copy number "
      "variation found with the tool “pipeline_name: sequenza_vivan”.")
queries = ["CREATE TABLE pat007_genes (genes TEXT);", "CREATE TABLE pat027_genes (genes TEXT);",
           "INSERT INTO pat007_genes (genes) SELECT (GUR) FROM pat007 AS genes WHERE "
           "(GUR IS NOT NULL AND GUR LIKE 'Hs%');",
           "INSERT INTO pat007_genes (genes) SELECT (GDR) FROM pat007 AS genes "
           "WHERE (GDR IS NOT NULL AND GDR LIKE 'Hs%');",
           "INSERT INTO pat027_genes (genes) SELECT (GUR) FROM pat027 AS genes "
           "WHERE (GUR IS NOT NULL AND GUR LIKE 'Hs%');",
           "INSERT INTO pat027_genes (genes) SELECT (GDR) FROM pat027 AS genes "
           "WHERE (GDR IS NOT NULL AND GDR LIKE 'Hs%');",
           "SET SQL_SAFE_UPDATES=0;", "UPDATE pat007_genes SET genes = SUBSTRING(genes, 4);",
           "UPDATE pat027_genes SET genes = SUBSTRING(genes, 4);", "SET SQL_SAFE_UPDATES=1;"]
[cursor.execute(q) for q in queries]
cn.commit()

for p in np.array(rows_bench).T[0]:
    cursor.execute(f"SELECT DISTINCT l.symbol AS cnv_bench_seq_viv FROM cnv AS l JOIN {p.lower()}_genes AS r ON "
                   f"(l.symbol = r.genes) WHERE (l.pipeline_name = 'sequenza_vivan');")
    rows = cursor.fetchall()
    print(f"For patient {p} those genes are:")
    [print(i[0]) for i in rows]
