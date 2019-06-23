"""Load data into Redshift's sparkifydb database Fact and Dimensions tables
This script is used to process and load data into sparkifydb Database 
tables. 

This file can also be imported as a module and contains the following
functions:

    * load_staging_tables - Processing Songs and Events Json file and loading 
      data into staging tables.  
    * insert_tables - Insert Data into Fact and Diomensions table from staging 
      tables. 
    * main - the main function of the script
"""

# Importing system libraries
import configparser
import psycopg2

# Importing user libraries
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Processing Song & Event data file and loading data into staging tables. 
    
    Parameters
    ___________
        cur  : psycopg2 cursor object
              Cursor object for sparkifydb
        conn : psycopg2 connection object
              Connection object for sparkifydb
    
    Returns
    ___________
        None
    """
    
    # Looping for loading each file into the tables 
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Processing data into Facts and Dimension tables. 
    
    Parameters
    ___________
        cur  : psycopg2 cursor object
              Cursor object for sparkifydb
        conn : psycopg2 connection object
              Connection object for sparkifydb
    
    Returns
    ___________
        None
    """
    
    # Looping for populating Fact and Dimensions 

    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    # Loading Configurations
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # creating sparkify DB connection
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                            .format(config.get('DB','HOST'),
                                    config.get('DB','DB_NAME'), 
                                    config.get('DB','DB_USER'),
                                    config.get('DB','DB_PASSWORD'),
                                    config.get('DB','DB_PORT'),
                                   )
                           )
    
    conn.set_session(autocommit=True)
    
    # Opening Cursor object
    cur = conn.cursor()
    
    # Calling function for populating staging tables
    try:
        load_staging_tables(cur, conn)
    except Exception as e:
        print(e)   
    
    # Calling function for populating Fact and Dimension tables
    try:
        insert_tables(cur, conn)
    except Exception as e:
        print(e)
    
    # closing the connection
    conn.close()

if __name__ == "__main__":
    main()