import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """  
    - Run SQL queries to copy data from S3 to staging tables
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """  
    - Run SQL queries to insert into final tables from staging tables
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """  
    - Parse config and log into Amazon Redshift and run above two functions
    - Close connection after finished
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
