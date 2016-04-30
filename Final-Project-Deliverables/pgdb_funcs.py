#!/usr/bin/env python

import sys
import datetime
import psycopg2
import psycopg2.extras

class PGDBLibException(Exception):
    pass

class PGDBLib:
    def __init__(self, password=None, host="localhost", port="5432"):
        self.pg_def_db = "postgres"
        self.pg_def_user = "postgres"
        self.pg_def_pass = password
        self.pg_ml_db = "ml_db"
        self.pg_ml_user = "postgres"
        self.pg_ml_pass = self.pg_def_pass
        self.pg_host = host
        self.pg_port = port
        self.conn = None
        self.cur = None

    def create_db(self):

        connect_str = "dbname=" + self.pg_def_db + " user=" + self.pg_def_user
        connect_str += " password=" + self.pg_def_pass
        # The machine learning database may not exist yet. So first
        # connect to the postgres database (which always exists)
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

        # Catch exceptions to close the connection on exit
        try:
            cur = conn.cursor()
        except:
            conn.close()
            raise

        cerror = False

        # Catch exceptions to detect errors creating the database
        try:
            createsql = "CREATE DATABASE " + self.pg_ml_db + ";"
            cur.execute(createsql)  
        except psycopg2.ProgrammingError as e:
            # Only possible programing error is database already exists 
            #msg = traceback.print_exc(file=sys.stderr) + ': %s'
            #glogger.exception(fn()+msg, conn)
            pass
        except:
            cerror = True

        # Catch exceptions to close cursor and connection
        try:
	    # Remove auto-commit
            level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
            conn.set_isolation_level(level)
        except:
            cur.close()
            conn.close()
            raise

        # Catch exceptions to close connection on exit
        try:
            cur.close()
        except:
            conn.close()
            raise

        conn.close()

        if cerror:
            raise PGDBLibException()

        connect_str = "dbname=" + self.pg_ml_db
        connect_str += " user=" + self.pg_ml_user
        connect_str += " password=" + self.pg_ml_pass
        conn = psycopg2.connect(dsn=connect_str)
        conn.close()

    def open_db(self, timeout):

        connect_str = "dbname=" + self.pg_ml_db
        connect_str += " user=" + self.pg_ml_user
        if not self.pg_ml_pass is None:
            connect_str += " password=" + self.pg_ml_pass
        if not self.pg_host is None:
            connect_str += " host=" + self.pg_host
            connect_str += " port=" + self.pg_port
        connect_str += " options='-c statement_timeout=" + str(timeout) +"'"

        conn = psycopg2.connect(dsn=connect_str)

        # Catch exceptions to close connection on exit
        try:
            cur = conn.cursor()
        except:
            conn.close()
            raise

        # Catch exceptions to close connection and cursor on exit
        try:
            self.conn =  conn
            self.cur = cur
        except:
            cur.close()
            conn.close()
            self.conn = None
            self.cur = None
            raise

    def create_table(self, table, fields, constraint):

        tablesql = "CREATE TABLE "
        tablesql += table + " "
        tablesql += "(id serial, tstamp timestamptz"
        for t in fields:
            tablesql += ", " + t[0] + " " + t[1]
        if len(constraint) == 0:
            tablesql += ");"
        else:
            tablesql += ", " + constraint + ");"

        # An exception indicates table already exists, just return
        try:
            self.cur.execute(tablesql)
            self.conn.commit()
        except:
            self.conn.commit()
            return
    def drop_table(self, table):
        try:
            tablesql = "DROP TABLE "
            tablesql += table + ";"
            self.cur.execute(tablesql)
            self.conn.commit()
        except:
            self.conn.commit()
            raise

    def get_rows(self, table, fields, matcher):

        if fields is None:
            raise PGDBLibException()

	if len(fields) == 0:
            querysql = "SELECT *"
        else:
            querysql = "SELECT id, tstamp"
            for t in fields:
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

        # Catch exceptions to rollback transaction on error
        try:
            if argsql is None:
                self.cur.execute(querysql)
            else:
                self.cur.execute(querysql,argsql)
        except:
            self.conn.rollback()
            raise

        # Catch exceptions to rollback transaction on error
        try:
            result_list = []
            while True:
                row = self.cur.fetchone()
                if row is None:
                    break
                result_fields = [row[0], row[1]]
                for i, t in enumerate(fields):
                    result_fields.append(row[i + 2])
                result_list.append(result_fields)
        except:
            self.conn.rollback()
            raise

        # Catch exceptions to rollback transaction on error
        try:
            self.conn.commit()
        except:
            self.conn.rollback()
            raise

        return result_list

    # This replaces save_replace() above. No native upsert statement in Postgres
    # until 9.5 and we are running a version below that (9.3 at last check)
    def upsert(self, table, fields, matcher):

        if matcher is None:
            raise PGDBLibException()

        insertsql = "INSERT INTO " + table + " "
        insertsql += "(tstamp"
        for t in fields:
            insertsql += ", " + t[0]
        insertsql += ") VALUES (%s"
        arglist = [datetime.datetime.now()]
        for t in fields:
            insertsql += ",%s"
            if t[1] == 'json':
                arglist.append(psycopg2.extras.Json(t[2]))
            else:
                arglist.append(t[2])
        insertsql += ");"
        # Argument must be a list or tuple
        argsql = tuple(arglist)

        # Catch exceptions to rollback transaction on error
        try:
            self.cur.execute(insertsql,argsql)
            self.conn.commit()
        except psycopg2.IntegrityError:
            # Probably the row already exists so we need to do an update
            self.conn.rollback()
            # Rest of the code runs after all of the other cases (see #-cont-1)
        except:
            self.conn.rollback()
            raise
        else:
            return

        #-cont-1
        try:
            updatesql = "UPDATE " + table + " SET "
            updatesql += "tstamp=%s"
            arglist = [datetime.datetime.now()]
            for t in fields:
                if t[2] is None:
                    raise PGDBLibException() 
                updatesql += ", " + t[0] + "=%s"
                if t[1] == 'json':
                    arglist.append(psycopg2.extras.Json(t[2]))
                else:
                    arglist.append(t[2])
            updatesql += " WHERE"
            for i, t in enumerate(matcher):
                if i == 0:
                    updatesql += " " + t[0] + "=%s"
                    arglist.append(t[2])
                else:
                    updatesql += " AND " + t[0] + "=%s"
                    arglist.append(t[2])
            updatesql += ";"
            # Argument must be a list or tuple
            argsql = tuple(arglist)
            self.cur.execute(updatesql,argsql)
            self.conn.commit()
        except:
            self.conn.rollback()
            raise

    def update_fields(self, table, all_fields, matcher, limit):

        querysql = "SELECT id, tstamp"
        for t in all_fields:
            querysql += ", " + t[0]
        querysql += " FROM "
        querysql += table
        argsql = None
        arglist = []
        if not matcher is None:
            querysql += " WHERE"
            for i, t in enumerate(matcher):
                if i == 0:
                    querysql += " " + t[0] + "=%s"
                    arglist.append(t[2])
                else:
                    querysql += " AND " + t[0] + "=%s"
                    arglist.append(t[2])

        if not limit is None:
            querysql += " LIMIT %s"
            arglist.append(str(limit))

        querysql += ";"
        if len(arglist) != 0:
            argsql = tuple(arglist)

        # Catch exceptions to rollback transaction on error
        try:
            if argsql is None:
                self.cur.execute(querysql)
            else:
                self.cur.execute(querysql,argsql)
        except:
            self.conn.rollback()
            raise

        # Catch exceptions to rollback transaction on error
        try:
            result_list = []
            while True:
                row = self.cur.fetchone()
                if row is None:
                    break
                result_fields = [row[0], row[1]]
                for i, t in enumerate(all_fields):
                    result_fields.append(row[i + 2])
                result_list.append(result_fields)
        except:
            self.conn.rollback()
            raise

        if len(result_list) == 0:
            self.conn.commit()
            return []

        # Catch exceptions to rollback transaction on error
        try:
            for r in result_list:
                updatesql = "UPDATE " + table + " SET "
                updatesql += "tstamp=%s"
                arglist = [datetime.datetime.now()]
                for t in all_fields:
                    if t[2] is None:
                        continue 
                    updatesql += ", " + t[0] + "=%s"
                    if t[1] == 'json':
                        arglist.append(psycopg2.extras.Json(t[2]))
                    else:
                        arglist.append(t[2])
                updatesql += " WHERE id=%s;"
                arglist.append(r[0])
                # Argument must be a list or tuple
                argsql = tuple(arglist)
                self.cur.execute(updatesql,argsql)
        except:
            self.conn.rollback()
            raise

        # Catch exceptions to rollback transaction on error
        try:
            self.conn.commit()
        except:
            self.conn.rollback()
            raise

        return result_list

    def delete_rows(self, table, matcher):

        if matcher is None:
            raise PGDBLibException()

        if len(matcher) == 0:
            deletesql = "DELETE FROM " + table + ";"
            # Catch exceptions to rollback transaction on error
            try:
                self.cur.execute(deletesql)
                self.conn.commit() 
            except:
                self.conn.rollback() 
                raise
            return

        deletesql = "DELETE FROM " + table
        deletesql += " WHERE"
        arglist = []
        for i, t in enumerate(matcher):
            if i == 0:
                deletesql += " " + t[0] + "=%s"
                arglist.append(t[2])
            else:
                deletesql += " AND " + t[0] + "=%s"
                arglist.append(t[2])
        deletesql += ";"
        # Argument must be a list or tuple
        argsql = tuple(arglist)

        # Catch exceptions to rollback transaction on error
        try:
            self.cur.execute(deletesql,argsql)
            self.conn.commit() 
        except:
            self.conn.rollback()
            raise

    def close_db(self):

        # Check if we opened the DB in the first place
        if self.conn is None:
            raise PGDBLibException()

        self.cur.close()
        self.conn.close()
        self.conn =  None
        self.cur = None

