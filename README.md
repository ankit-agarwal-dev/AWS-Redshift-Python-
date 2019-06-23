# Redshift Tutorial

Redshift Tutorial is collection of scripts that explains in simple steps about various ETL operations on a Redshift DB using Python on AWS. This tutorial also briefly explains about Data Warehousing concepts and its implementation in a star schema (Fact and Dimensions).
Following concepts have been covered in this tutorial
* Creation of redshift cluster
* Dropping tables (handling existence/non-existence)
* Creating tables (handling existence/non-existence). This also emphasize on constraints and its implementation.
* Normal and Bulk insertion into tables ensuring no violation of constraints.
* Joining of two tables for fetching some values.

     
## Installation

```python
pip install create_tables
pip install etl
pip install redshift_cluster
```

## Usage

### Importing the libraries
```python
import create_tables
import etl
import redshift_cluster

```
### Executing the scripts
```python
pyhton create_tables.py
python etl.py
```
#### create_tables.py
**Pre Requisite**  
* Redhift cluster should already be running with configured (as per configuration file) DB in available and healthy state. 

**Usage**  
This script will drop Fact and Dimensions table (if exist) and recreate them. It is important to note that any pre-data will be lost from all the tables unless backed-up upon execution of this script.

#### etl.py
**Pre Requisite**  
* Redhift cluster should already be running with configured (as per configuration file) DB in available and healthy state.
* Fact and Dimensions tables should be available in Redshift Database.

**Usage**  
This script will will first load data from Json files into staging tables (using bulk copy command) and then will load data from staging to Fact and Dimensions tables.
Importance of various perfomance mesures is also explained in the tutorial. Usage of distribution style is explained for the same. 

Following Fact and Dimensions table is used in this tutorial.  

**Fact Table**
* songs_plays_fact

**Dimension Tables**
* users_dim
* songs_dim
* artists_dim
* time_dim

***Additional Script***

* redshift_cluster.py  
This script is used to start a redshift clutser, create an IAM role with appropriate permissions and delete a cluster (whenever not required). 
* dwh.cfg  
Configuration files for AWS, Redshift cluster and Database. Please note that this has been intentionally kept blank. Please fill it with required values before starting the tutorial.

## Contributing
Any suggestions are welcome. For major changes, please open an issue first to discuss what you would like to change.   

## License
Applied