# Sparkify Data
## Overview
Sparkify, a music streaming startup, has grown their user base and song database and needs processes and data in the cloud. Their data has been moved to S3 from on prem, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
This project creates an ETL pipline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.
## How to run the python script
### Install Dependencies
To run the scripts in the project first 
1. Create an AWS account.
2. create an AWS user that can be used to connect to AWS programmatically.
3. Store the SECRETCODE and KEY in a save place.
4. Create a role and attach AmazonS3ReadOnlyAccess policy to it.
5. Store the name of the role and input it in dwh2.cg

1. complete the dwh2.cfg file by replacing the text in **astericks**
[AWS]
KEY= **Key of the user with programtic access**
SECRET=**secret key of programatic access**

[CLUSTER]
CLUSTER_NODE_TYPE=dc2.large
CLUSTER_TYPE=multi-node
CLUSTER_IDENTIFIER=sparkifyCluster
CLUSTER_NUM_NODES=4
DB_NAME=**dbname**
DB_USER=**db_user**
DB_PASSWORD=**db_password**
DB_PORT=5439

2. Run the cells in the infrastructure_setup.ipynb until the mark down  ***Delete AWS Resources***
3. Take note of the DWH_ENDPOINT and DWH_ROLE_ARN
4. Go to dwh.cfg and complete the HOST and ARN with the DWH_ENDPOINT and DWH_ROLE_ARN respectively.
5. Fill in the DB_NAME, DB_USER, DB_PASSWORD with the same value as in dwh2.cfg

ETL
1. Run the create_tables.py to create the staging tables, fact table, and dimensional tables
2. Run etl.py file to perform the ETL
3. confirm on AWS Redshift that the data is now in redshift


### Repository Files

#### DATA
The data folder contains two other folders that contains sample song data and data collected from users that use the sparkify application.

- data folder: contains all data that will be used in this project.
- Create_tables.py: this is a script that connects to the database in the configuration and creates five tables as specified in the script sql_queries.py </br>
The tables created are songplay, users, songs, artists, and time_info.
The create table will first drops all table with the same name that are in the connected database then creates the tables.

- sql_queries.py -: this script contains template of drop, create or select queries that are used in the project

- etl.py: the etl.py script connects to the database and inserts data into the five tables created.
- etl.ipynb: this is a notebook of the etl.py
- erd.pgerd : this is the entity relationship diagram of the table created by the create table file.

### Entity relationship Diagram of the database created

The ERD of the database
<img src="erd.png" alt="ERD" style="float: left; margin-right: 10px;" />
