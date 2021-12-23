import configparser
import psycopg2
from sql_queries import *

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"\
                            .format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    cur.execute(songplays_per_artist)
    results = cur.fetchall()
    for row in results:
        print(f"{row}")

    conn.close()


if __name__ == "__main__":
    main()
