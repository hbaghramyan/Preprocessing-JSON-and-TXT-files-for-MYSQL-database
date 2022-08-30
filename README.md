### Preprocessing the JSON file:

I used *json* package to import json files into python as native dictionaries. Then, using for loops and if statemens I extracted the genes for each patient and put it into a dested dictionary with keys as patient names. After that, the dictionary for each patiend was converted into *pandas* dataframe. Then I used *mysql* package to create a MYSQL **patients_db** database. Subsequently, I used  create_engine from sqlalchemy to connect to **patients_db** and pandas *to_sql* to insert each table into **patients_db**.

### Preprocessing the TXT (tabulated) file:

I did it using pandas.read_csv.

#### QUESTION (a): Number of patients in Benchling with information for genes to up/down regulate.
ANSWER (a): Patients
Pat002
Pat004
Pat007
Pat009
Pat027
Pat032
Pat044
Pat046
Pat057
have data both for genes to up regulate and genes to down regulate.
The total number of patients in Benchling with information for genes to up/down regulate is 9.

#### QUESTION (b): Number of patients with information for copy number variation.
There are 2 patients with information for copy number variation.
 Those patients are:
Pat027
Pat007

#### Question (c): Identify which patients have both information.
Pat007 Pat007

#### Question (d): For each patient found in “c” list the genes present in Benchling and having a copy number variation found with the tool “pipeline_name: sequenza_vivan”.
For patient Pat007 those genes are:
STAG1
PTEN
KMT2D
MYO1C
SMARCA4
CEBPA
SMC1A
KDM6A
For patient Pat027 those genes are:
KMT2C
TP53
CCNE1
TSHZ3
TSHZ2
AURKA
GNAS



