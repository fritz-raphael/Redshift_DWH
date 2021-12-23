import configparser
import psycopg2
import time
from sql_queries import *

def truncate_tables(cur, conn):
    """Truncate all tables."""
    for query in truncate_table_queries:
        table = query.split(" ")[2]
        print(f"Do you want to truncate table '{table}'?")
        decision = input("[y/n] > ")
        if decision.lower() == "y":
            cur.execute(query)
            conn.commit()
            print(f"Table '{table}' truncated.")
        else:
            continue
    print()


def load_staging_tables(cur, conn):
    print("4.1 Copying data to the staging tables.")
    for query in copy_table_queries:
        table = query.split(" ")[1]
        print(f"\nCopying data into '{table}' table.")
        cur.execute(query)
        conn.commit()
        print(f"Data copied.")
    print("\nAll tables copied.\n")


def insert_tables(cur, conn):
    print("4.2 Inserting data into star schema.")
    for query in insert_table_queries:
        table = query.split(" ")[2]
        print(f"\nInserting data into '{table}' table.")
        cur.execute(query)
        conn.commit()
        print("Insert complete.")
    print("\nAll data has been inserted to star schema.\n")


def clean_data(cur, conn):
    """Set year-column in songs-table to NULL where '0'."""
    cur.execute(detect_year_zero)
    results = cur.fetchall()
    if results:
        print("In the 'songs'-table there are some records with 'year' = '0'.\n"
              "Should we set those fields to 'NULL'?")
        decision = input("[y/n] > ")
        if decision.lower() == "y":
            cur.execute(set_year_null)
            conn.commit()
            print("Done.\n")
        else:
            print("Leaving year=0 as it is.\n")


def check_for_duplicates(cur, conn):
    """Check each star schema table for duplicates."""
    tables = {0: "users",
              1: "songs",
              2: "artists",
              3: "time",
              4: "songplays"}
    print("5.1 Checking for duplicates.\n")
    for index, query in enumerate(check_duplicates_queries):
        table = tables.get(index, 'None')
        print(f"Checking for duplicates in table '{table}'.")
        cur.execute(query)
        results = cur.fetchall()
        if results:
            print(f"Table '{table}' has duplicates.")
            kick_duplicates(cur, conn, table)
        else:
            print(f"Table '{table}' has no duplicates.\n")
            continue


def kick_duplicates(cur, conn, tablename):
    """Identify and remove duplicates from table."""
    if tablename == "artists":
        print(f"5.2 Removing duplicates from {tablename} table.")
        cur.execute(artists_remove_duplicates)
        conn.commit()
        print("Duplicates removed.\n")
    else:
        print(f"There is no query for {tablename} yet. "
               "Go and write one.\n")


def drop_staging_tables(cur, conn):
    "Drop both staging tables."
    print("6.1 Do you want to drop both staging_tables?")
    decision = input("[y / n] > ")
    if decision.lower() == "y":
        for query in drop_table_queries:
            table = query.split(r"\s")[4]
            print(f"Dropping table '{table}'.")
            cur.execute(query)
            conn.commit()
            print("Table dropped.")
    else:
        print("Both staging_tables stay intact.")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"\
                            .format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Run 'truncate_tables' function if data was copied
    # but not inserted correctly.
    # truncate_tables(cur, conn)
    load_staging_tables(cur, conn)

    insert_tables(cur, conn)
    check_for_duplicates(cur, conn)
    clean_data(cur, conn)
    drop_staging_tables(cur, conn)
    conn.close()

if __name__ == "__main__":
    main()
