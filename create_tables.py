""" Drop and recreate tables in redshift Database 
This script forst delete table (if exists) and then recreates the tables.
This file can also be imported as a module and contains the following
functions:

    * drop_tables - Dropping Tables
    * create_tables - Creating Tables
    * main - the main function of the script
"""

# Importing system libraries
import configparser
import psycopg2

# Importing user defined libraries
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """Dropping tables from Database. 
    
    Parameters 
    ___________
        cur : psycopg2 cursor object
            Cursor object for DB
        conn : psycopg2 connection object
            Connection object for DB
    
    Returns
    ___________
        None
    """
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """Creating tables in Database. 
    
    Parameters 
    ___________
        cur : psycopg2 cursor object
            Cursor object for DB
        conn : psycopg2 connection object
            Connection object for DB
    
    Returns 
    ___________
        None
    """
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    
    # Intialising and loading config
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # Opening DB connecion
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                            .format(config.get('DB','HOST'),
                                    config.get('DB','DB_NAME'), 
                                    config.get('DB','DB_USER'),
                                    config.get('DB','DB_PASSWORD'),
                                    config.get('DB','DB_PORT'),
                                   )
                           )
    
    conn.set_session(autocommit=True)
    
    cur = conn.cursor()
    
    # Dropping existing tables 
    try:
        drop_tables(cur, conn)
    except Exception as e:
        print("Error while Creating tables; error message " + str(e))
        cur.close()
        conn.close()
        sys.exit(1)
    
    # Creating tables
    try:
        create_tables(cur, conn)
    except Exception as e:
        print("Error while Dropping tables; error message " + str(e))
        cur.close()
        conn.close()
        sys.exit(1)
    
    # Closing the connection
    conn.close()

if __name__ == "__main__":
    main()