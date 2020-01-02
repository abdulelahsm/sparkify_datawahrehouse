import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Loads JSON input data from Amazon S3 and inserts it into staging_events and staging songs tables
    Params:
    ** cur - reference to connected db.
    ** conn- parameters (host, dbname, user, password, port)
    Output:
    * log_data in staging_events table.
    * song_data in staging_songs table.
    '''
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data from staging tables into the analytics tables:
        ** Fact table: songplays
        ** Dimensions tables: users, songs, artists, time
    Inputs:
        ** cur -   refers to connected database.
        ** conn - (host, dbname, user, password, port) params used to connect the database.
    Outputs:
        ** Data inserted from staging tables to dimension tables.
        ** Data inserted from staging tables to fact table.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Connects to the database and calls the following:
        ** to load_staging_tables to load data from JSON files
            (song_data and log_data in S3) to staging tables
    args:
        * None
    Outputs:
        * All input data processed in the database tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
    
    
