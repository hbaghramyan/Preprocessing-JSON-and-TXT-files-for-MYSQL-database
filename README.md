### Preprocessing the JSON file:

I used *json* package to import json files into python as native dictionaries. Then, using for loops and if statemens I extracted the genes for each patient and put it into a dested dictionary with keys as patient names. After that, the dictionary for each patiend was converted into *pandas* dataframe. Then I used *mysql* package to create a MYSQL **patients_db** database. Subsequently, I used  create_engine from sqlalchemy to connect to **patients_db** and pandas *to_sql* to insert each table into **patients_db**.

### Preprocessing the TXT (tabulated) file:

I did it using pandas.read_csv.
