#!/usr/bin/env python
from __future__ import absolute_import, print_function, unicode_literals

import sys
import psycopg2
import psycopg2.extras


class PGDBException(Exception):
    pass

class PGDB:
    def __init__(self, db_name, table_name, user, password, host, port):
        self.pg_db = db_name
        self.pg_table = table_name
        self.default_db = "postgres"
        self.pg_user = user
        self.pg_pass = password
        self.pg_host = host
        self.pg_port = port
        self.conn = None
        self.cur = None

    def open_db(self):
        connect_str = "dbname=" + self.pg_db
        connect_str += " user=" + self.pg_user
        connect_str += " password=" + self.pg_pass
        connect_str += " host=" + self.pg_host
        connect_str += " port=" + self.pg_port
        conn = psycopg2.connect(dsn=connect_str) 

        try:
            cur = conn.cursor()
        except:
            conn.close()
            raise

        try:
            self.conn = conn
            self.cur = cur
        except:
            cur.close()
            conn.close()
            self.conn = None
            self.cur = None

    def read_table(self, table, fields, matcher):
    
        querysql = "SELECT " + fields[0][0]
        for t in fields[1:]:
            querysql += ", " + t[0]
        querysql += " FROM "
        querysql += table

        if not matcher is None:
            querysql += " WHERE"
            arglist = []
            for i, t in enumerate(matcher):
                if i == 0:
                    querysql += " " + t[0] + "=%s"
                    arglist.append(t[2])
                else:
                    querysql += " AND " + t[0] + "=%s"
                    arglist.append(t[2])
            querysql += ";"
            argsql = tuple(arglist)
        else:
            querysql += ";"
            argsql = None

        try:
            if argsql is None:
                self.cur.execute(querysql)
            else:
                self.cur.execute(querysql, argsql)
        except:
            self.conn.rollback()
            raise

        try:
            result_list = []
            while True:
                row = self.cur.fetchone()
                if row is None:
                    break
                result_fields = []
                for i, t in enumerate(fields):
                    result_fields.append(row[i])
                result_list.append(result_fields)
        except:
            self.conn.rollback()
            raise

        try:
            self.conn.commit()
        except:
            self.conn.rollback()
            raise

        return result_list

    def close_db(self):

        # Check if we opened the db in the first place
        if self.conn is None:
            raise PGDBException()
        self.cur.close()
        self.conn.close()
        self.conn = None
        self.cur = None

DB = "tcount"
TABLE = "tweetwordcount"

def print_interval(k1, k2):
    try:
        pg_db = PGDB(DB, TABLE, "postgres", "pass", "localhost", "5432")
        pg_db.open_db()
        fields = [
                   ("word", "varchar", None),
                   ("count", "int", None)
                 ]
        matcher = None
        results = pg_db.read_table(TABLE, fields, matcher)
    except:
        print("Error accessing database " + DB + " or table " + TABLE)
        return

    if len(results) == 0:
        print("No words found in database")
        return

    sorted_results = sorted(results, key=lambda row: row[0])

    banner = True
    for row in sorted_results:
        if row[1] < k1 or row[1] > k2:
            continue
        if banner == True:
            banner = False
        print(row[0], str(row[1]))

    if banner == True:
        print("There are no words in the database whose count satisfies: " + str(k1) + " <= count <= " + str(k2))


if __name__ == "__main__":

    if len(sys.argv) == 2:
        interval = sys.argv[1]
        try:
            interval = interval.strip()
            tokens = interval.split(",")
        except:
            print("Interval specification must be: k1,k2  where k1 and k2 are integers")
            sys.exit(1)
         
        if len(tokens) != 2:
            print("Interval specification must have exatly 2 integers separated by comma")
            sys.exit(1)

        try:
            k1 = int(tokens[0])
            k2 = int(tokens[1])
        except:
            print("One or more tokens in the interval specification is not an integer")
            sys.exit(1)

        if k1 > k2:
            print("Interval specification is invalid. k1 is larger than k2")
            sys.exit(1)

        if k1 < 0 or k2 < 0:             
            print("Interval specification is invalid. Atleast one of k1 or k2 is negative")
            sys.exit(1)
        print_interval(k1,k2)
    else:
        print("Invalid number of arguments.")
        print("Usage: :" + sys.argv[0] + " k1,k2")
        print("\twhere k1 and k2 are integers and k1 <= k2. k1 and k2 should be comma separated with no space between.")
        print("\tThis script prints all words in the database whose count satisfies:  k1 <= count <= k2")
        sys.exit(1)

sys.exit(0)
