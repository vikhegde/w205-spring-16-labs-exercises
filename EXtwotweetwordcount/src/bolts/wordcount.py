from __future__ import absolute_import, print_function, unicode_literals

from collections import Counter
from streamparse.bolt import Bolt
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

    def create_db(self):
        # The database may not exist yet. First connect to the
        # postgres database that always exists
        connect_str = "dbname=" + self.default_db
        connect_str += " user=" + self.pg_user
        connect_str += " password=" + self.pg_pass
        connect_str += " host=" + self.pg_host
        connect_str += " port=" + self.pg_port
        conn = psycopg2.connect(connect_str)

        # Catch exceptions to close the connection on exit
        try:
            # Cannot create database inside a transaction block, so
            # temporarily set auto-commit
            level = psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
            conn.set_isolation_level(level)
        except:
            conn.close()
            raise

        try:
            cur = conn.cursor()

        except:
            conn.close()
            raise

        cerror = False

        # Catch exceptions to detect errors creating the database
        try:
            createsql = "CREATE DATABASE " + self.pg_db + ";"
            cur.execute(createsql) 
        #except psycopg2.ProgrammingError as e:
        #    # Only possible programming error is database already exists
        #    pass
        except:
            # The database probably already exists
            pass
            #cerror = True

        try:
            # Remove auto-commit
            level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
            conn.set_isolation_level(level)
        except:
            cur.close()
            conn.close()
            raise
 
        try:
            cur.close()
        except:
            conn.close()
            raise
    
        conn.close()

        #if cerror:
        #    raise PGDBException()

        connect_str = "dbname=" + self.pg_db
        connect_str += " user=" + self.pg_user
        connect_str += " password=" + self.pg_pass
        connect_str += " host=" + self.pg_host
        connect_str += " port=" + self.pg_port
        conn = psycopg2.connect(dsn=connect_str)
        conn.close()

    def create_table(self, table, fields, constraint):
        # IF NOT EXISTS is available only from Postgresql version 9.1 onwards
        tablesql = "CREATE TABLE "
        tablesql += table + " "
        tablesql += "(" + fields[0][0] + " " + fields[0][1]
        for t in fields[1:]:
            tablesql += ", " + t[0] + " " + t[1]
        if len(constraint) == 0:
            tablesql += ");"
        else:
            tablesql += ", " + constraint + ");"


        try:
            self.cur.execute(tablesql)
            self.conn.commit()
        #except psycopg2.ProgrammingError as e:
        #    # Probably the table already exists
        #    self.conn.rollback()
        except:
            # probably the table already exists
            self.conn.rollback()
            #raise

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

    # No native upsert statement in Postgres until 9.5, so define our own
    # upsert() routine in case we are running a version below that
    def upsert(self, table, fields, matcher):

        if matcher is None:
            PGDBException()

        insertsql = "INSERT INTO " + table + " "
        insertsql += "("
        for i, t in enumerate(fields):
            if i == 0:
                insertsql += t[0]
            else:
                insertsql += ", " + t[0]
        insertsql += ") VALUES ("
        arglist = []
        for i, t in enumerate(fields):
            if i == 0:
                insertsql += "%s"
            else:
                insertsql += ",%s"
            arglist.append(t[2])
        insertsql += ");"
         
        # Argument must be a list or tuple
        argsql = tuple(arglist)
        
        # Catch exceptions to rollback transaction on error
        try:
            self.cur.execute(insertsql, argsql)
            self.conn.commit()
        except psycopg2.IntegrityError:
            # The row probably already exists, so we need to do an update
            self.conn.rollback()
            # The rest of the code i.e. the update SQL, follows this try-except
        except:
            self.conn.rollback()
            raise
        else:
            return

        # Insert failed, try an update. Yes this is not an atomic upsert
        try:
            updatesql = "UPDATE " + table + " SET "
            arglist = []
            for i, t in enumerate(fields):
                if i == 0:
                    updatesql += t[0] + "=%s"
                else:
                    updatesql += ", " + t[0] + "=%s"
                if t[2] is None:
                    raise PGDBEXception()
                else:
                    arglist.append(t[2])
            updatesql += " WHERE"
            for i, t in enumerate(matcher):
                if i == 0:
                    updatesql += " " + t[0] + "=%s"
                else:
                    updatesql += " AND " + t[0] + "=%s"
                if t[2] is None:
                    raise PGDBException()
                else:
                    arglist.append(t[2])
            updatesql += ";"
            # Argument must be list or tuple
            argsql = tuple(arglist)
            self.cur.execute(updatesql, argsql)
            self.conn.commit()
        except:
            self.conn.rollback()
            raise

    def close_db(self):

        # Check if we opened the db in the first place
        if self.conn is None:
            raise PGDBException()
        self.cur.close()
        self.conn.close()
        self.conn = None
        self.cur = None

class WordCounter(Bolt):

    def initialize(self, conf, ctx):
        self.counts = Counter()
        #self.redis = StrictRedis()
        self.db_name = "tcount"
        self.table_name = "tweetwordcount"

        # Uncomment the following create-db, create-table code if DB/table are
        # not present
        pg_db = PGDB(self.db_name, self.table_name, "postgres", "pass",
            "localhost", "5432")
        pg_db.create_db()
        pg_db.open_db()
        fields = []
        fields.append(("word", "varchar", None))
        fields.append(("count","int", None))
        constraint = 'CONSTRAINT pkey_word PRIMARY KEY (word)'
        pg_db.create_table(self.table_name, fields, constraint)
        pg_db.close_db()
        

    def process(self, tup):
        word = tup.values[0]

        # Write codes to increment the word count in Postgres
        # Use psycopg to interact with Postgres
        # Database name: Tcount 
        # Table name: Tweetwordcount 
        # you need to create both the database and the table in advance.
        new_stored_count = 0
        old_stored_count = 0
        try:
            pg_db = PGDB(self.db_name, self.table_name, "postgres", "pass",
                "localhost", "5432")
            pg_db.open_db()
        except KeyboardInterrupt as e:
            self.log('Interrupted: open DB: %s: %d' % (word, self.counts[word]))
            raise
        except:
            self.log('ERROR open DB: %s: %d' % (word, self.counts[word]))
        else:
            try:
                fields = []
                fields.append(('count', 'int', None)) 
                matcher = []
                matcher.append(('word', 'varchar', word)) 
                row_list = pg_db.read_table(self.table_name, fields, matcher)
                nrows = len(row_list)
        
                # We can have zero or one row per word
                if nrows == 1:
                    row = row_list[0]
                    old_stored_count = row[0]
                elif nrows == 0:
                    old_stored_count = 0
                else:
                    raise PGDBException()
            except KeyboardInterrupt as e:
                self.log('Interrupted: old count: %s: %d' % (word, self.counts[word]))
                raise
            except:
                self.log('ERROR old count: %s: %d' % (word, self.counts[word]))
            else:
                new_stored_count = old_stored_count + 1
                try: 
                    fields = []
                    fields.append(('word', 'varchar', word)) 
                    fields.append(('count', 'int', new_stored_count)) 
                    matcher = []
                    matcher.append(('word', 'varchar', word)) 
                    pg_db.upsert(self.table_name, fields, matcher)
                except KeyboardInterrupt as e:
                    self.log('Interrupted: upsert: %s: %d' % (word, self.counts[word]))
                    raise
                except:
                    self.log('ERROR upsert: %s: %d' % (word, new_stored_count))
            # Close DB
            pg_db.close_db()

        # Increment the local count
        self.counts[word] += 1
        self.emit([word, self.counts[word]])

        # Log the count - just to see the topology running
        self.log('OK: %s: %d, %d' % (word, self.counts[word], new_stored_count))
