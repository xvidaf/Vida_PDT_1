PDT Zadanie 1 Filip Vida

The main script is written in python and located in .\src. 
The folders are pre-created and will hold the respective table .csv files.

The required libraries are:
  - pandas: for parsing source data.
  - datetime: for getting the current time and date
  - psycopg3: for connecting to the database and executing queries
  - csv: for writing the run time to a csv.
  
Before running the script:
  - Specify the location of the source twitter data, while calling the prepareAuthors and prepareConversations functions.
  - Enter the database credentials for the psycopg3 connect, in the respective functions.
  
The script will at first parse the provided data source, and create csv files that are equivalent to the tables that are to be created. The script will then create the tables and copy the created csv. files into the database.
