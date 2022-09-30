#### Database Description
The database warehouse_project for Sparkify is designed to keep track of what songs users play. The database is build as a star schema and consists of the following tables:
* songplays (Fact Table) - records associated with song plays, users and artists etc. 
* users (Dimension Table) - users in the app
* songs (Dimension Table) - songs in music database
* artists (Dimension Table) - artists in music database
* time (Dimension Table) - timestamps of records in songplays broken down into specific units

In addition, warehouse_project has staging_tables events_staging, songs_staging that serve to process data from S3 and laod to final tables pointed above.
The database sparkifydb allows to query all neccesary data about users, songs, artists, duration etc. and build reports based on it

#### How to run Python scripts
In order to create the database and launch the ETL process you need to run 2 py. scripts in the follwing order:
1. create_tables.py
2. etl.py
To run a python (py.) script open a terminal (File->New->Terminal) and make sure you are in the right directory by using a command *ls*. If you are in the directory with the script that you want to run then type *Python3 create_tables.py* or *Python3 etl.py*
Please note, it may take several minutes to execute etl.py script depending on your cluster performance. 

#### Files in the repo
-  *create_tables.py* drops and creates tables.
- *etl.py* launches sql scripts to processes files from song_data and log_data and loads them into final tables.
- *sql_queries.py* contains sql queries to for the ETL process.
- *dwh.cfg* contains project credentials
- *data_review.ipynb* allows to review a sample data from one JSON to better understand data structure
- *cluster_creation.ipynb* creates/deletes redshift cluster programmatically 
- *README.md* provides discussion on the project.